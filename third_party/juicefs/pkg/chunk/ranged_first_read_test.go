package chunk

// JuiceMount: a small cold read at the start of a block is served by a
// ranged GET, and the prefetcher fills the whole block in the background
// (patch 0018).

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

// gatedStorage records every Get. A whole-object Get (limit < 0) waits
// until the gate is open, so a test can prove a read did not wait for it.
type gatedStorage struct {
	object.ObjectStorage
	mu     sync.Mutex
	ranged []int64 // limits of ranged Gets
	full   atomic.Int32
	gate   chan struct{}
}

func (g *gatedStorage) Get(key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	if limit < 0 {
		g.full.Add(1)
		<-g.gate
	} else {
		g.mu.Lock()
		g.ranged = append(g.ranged, limit)
		g.mu.Unlock()
	}
	return g.ObjectStorage.Get(key, off, limit, getters...)
}

func (g *gatedStorage) rangedGets() []int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]int64(nil), g.ranged...)
}

func rangedConf(t *testing.T, prefetch int) Config {
	conf := defaultConf
	// The memory cache: a disk cache's background goroutines outlive the
	// test and race (upstream) when its folder is removed. This change does
	// not touch how either cache stores or verifies a block.
	conf.CacheDir = "memory"
	conf.CacheFullBlock = true
	conf.Prefetch = prefetch
	return conf
}

// writeBlocks stores one slice of n full blocks through a separate store,
// so the reading store starts with an empty cache.
func writeBlocks(t *testing.T, mem object.ObjectStorage, id uint64, n int) []byte {
	t.Helper()
	conf := rangedConf(t, 0)
	conf.CacheSize = 0 // nothing cached by the writer
	w := NewCachedStore(mem, conf, nil)
	data := make([]byte, n*conf.BlockSize)
	rand.New(rand.NewSource(int64(id))).Read(data)
	sw := w.NewWriter(id)
	if _, err := sw.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := sw.Finish(len(data)); err != nil {
		t.Fatal(err)
	}
	return data
}

func waitCached(t *testing.T, s *cachedStore, key string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := s.bcache.exist(key); ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("block %s was never filled", key)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func readAt(t *testing.T, r Reader, off, n int) []byte {
	t.Helper()
	p := NewPage(make([]byte, n))
	got, err := r.ReadAt(context.Background(), p, off)
	if err != nil || got != n {
		t.Fatalf("read %d at %d: got %d, %v", n, off, got, err)
	}
	return p.Data
}

func TestJMRangedReadAtBlockStartDoesNotWaitForBlock(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	data := writeBlocks(t, mem, 101, 2)
	g := &gatedStorage{ObjectStorage: mem, gate: make(chan struct{})}
	store := NewCachedStore(g, rangedConf(t, 1), nil).(*cachedStore)
	r := store.NewReader(101, len(data))
	bs := store.conf.BlockSize

	// The whole-block GET is held, yet the read at offset 0 returns.
	done := make(chan []byte)
	go func() { done <- readAt(t, r, 0, 64<<10) }()
	select {
	case got := <-done:
		if !bytes.Equal(got, data[:64<<10]) {
			t.Fatal("offset 0 read returned wrong bytes")
		}
	case <-time.After(5 * time.Second):
		close(g.gate)
		t.Fatal("read at offset 0 waited for the whole block")
	}
	if rg := g.rangedGets(); len(rg) != 1 || rg[0] != 64<<10 {
		t.Fatalf("ranged GETs %v, want one of 64 KiB", rg)
	}

	// The prefetcher fills the block in the background.
	close(g.gate)
	key := sliceForRead(101, len(data), store).key(0)
	waitCached(t, store, key)
	if n := g.full.Load(); n != 1 {
		t.Fatalf("whole-block GETs %d, want 1", n)
	}

	// The rest of the block comes from the cache.
	before := len(g.rangedGets())
	if got := readAt(t, r, 512<<10, 256<<10); !bytes.Equal(got, data[512<<10:768<<10]) {
		t.Fatal("cached read returned wrong bytes")
	}
	if got := readAt(t, r, bs-4096, 4096); !bytes.Equal(got, data[bs-4096:bs]) {
		t.Fatal("cached tail read returned wrong bytes")
	}
	if len(g.rangedGets()) != before || g.full.Load() != 1 {
		t.Fatal("reads after the fill went to the object store")
	}
}

func TestJMBlockStartReadWithoutPrefetcherReadsWholeBlock(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	data := writeBlocks(t, mem, 102, 1)
	g := &gatedStorage{ObjectStorage: mem, gate: make(chan struct{})}
	close(g.gate)
	store := NewCachedStore(g, rangedConf(t, 0), nil).(*cachedStore)
	r := store.NewReader(102, len(data))
	if got := readAt(t, r, 0, 64<<10); !bytes.Equal(got, data[:64<<10]) {
		t.Fatal("wrong bytes")
	}
	if rg := g.rangedGets(); len(rg) != 0 || g.full.Load() != 1 {
		t.Fatalf("ranged %v full %d, want 0 ranged and 1 whole-block GET", rg, g.full.Load())
	}
	// A large read at offset 0 still reads the whole block.
	store2 := NewCachedStore(g, rangedConf(t, 1), nil).(*cachedStore)
	r2 := store2.NewReader(102, len(data))
	bs := store2.conf.BlockSize
	if got := readAt(t, r2, 0, bs/2); !bytes.Equal(got, data[:bs/2]) {
		t.Fatal("wrong bytes")
	}
	if rg := g.rangedGets(); len(rg) != 0 || g.full.Load() != 2 {
		t.Fatalf("ranged %v full %d after a large read", rg, g.full.Load())
	}
}

func TestJMPrefetchSkipsBlockAlreadyCached(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	data := writeBlocks(t, mem, 103, 1)
	g := &gatedStorage{ObjectStorage: mem, gate: make(chan struct{})}
	close(g.gate)
	store := NewCachedStore(g, rangedConf(t, 1), nil).(*cachedStore)
	r := store.NewReader(103, len(data))
	readAt(t, r, 0, len(data)) // whole block, cached by the read itself
	key := sliceForRead(103, len(data), store).key(0)
	waitCached(t, store, key)
	if g.full.Load() != 1 {
		t.Fatalf("whole-block GETs %d, want 1", g.full.Load())
	}
	store.fetcher.fetch(key)
	deadline := time.Now().Add(5 * time.Second)
	for {
		store.fetcher.Lock()
		idle := len(store.fetcher.busy) == 0
		store.fetcher.Unlock()
		if idle {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("prefetch never finished")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if g.full.Load() != 1 {
		t.Fatalf("prefetch downloaded a cached block again (%d whole-block GETs)", g.full.Load())
	}
}

func TestJMConcurrentReadersOfOneColdBlock(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	data := writeBlocks(t, mem, 104, 2)
	g := &gatedStorage{ObjectStorage: mem, gate: make(chan struct{})}
	store := NewCachedStore(g, rangedConf(t, 1), nil).(*cachedStore)
	bs := store.conf.BlockSize
	var wg sync.WaitGroup
	errs := make(chan string, 64)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := store.NewReader(104, len(data))
			off := (i % 2) * bs           // both blocks at offset 0
			off += (i / 2 % 4) * (bs / 4) // and at each quarter
			p := NewPage(make([]byte, 32<<10))
			n, err := r.ReadAt(context.Background(), p, off)
			if err != nil || n != len(p.Data) || !bytes.Equal(p.Data, data[off:off+n]) {
				errs <- "wrong read"
			}
		}(i)
		if i == 8 {
			close(g.gate) // some readers find a fill in flight, some do not
		}
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		waitCached(t, store, sliceForRead(104, len(data), store).key(i))
	}
	if n := g.full.Load(); n > 2 {
		t.Fatalf("whole-block GETs %d for 2 blocks", n)
	}
	r := store.NewReader(104, len(data))
	if got := readAt(t, r, 0, 2*bs); !bytes.Equal(got, data) {
		t.Fatal("cached blocks differ from the object")
	}
}

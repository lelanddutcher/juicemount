package chunk

import (
	"bytes"
	"errors"

	"github.com/juicedata/juicefs/pkg/object"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// spanObject is a fake object store for one block: byte i is byte(i*7+off).
func spanBlock(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/1000)
	}
	return b
}

type spanFake struct {
	block   []byte
	calls   atomic.Int32
	mu      sync.Mutex
	offs    []int
	gate    chan struct{} // when set, every fetch waits for it
	fail    map[int]bool
	running atomic.Int32
	maxRun  atomic.Int32
}

func (f *spanFake) fetch(sp *spanPart) ([]byte, error) {
	f.calls.Add(1)
	r := f.running.Add(1)
	for {
		m := f.maxRun.Load()
		if r <= m || f.maxRun.CompareAndSwap(m, r) {
			break
		}
	}
	defer f.running.Add(-1)
	f.mu.Lock()
	f.offs = append(f.offs, sp.off)
	fail := f.fail[sp.off]
	f.mu.Unlock()
	if f.gate != nil {
		<-f.gate
	}
	if fail {
		return nil, errors.New("injected")
	}
	return append([]byte(nil), f.block[sp.off:sp.off+sp.n]...), nil
}

func newSpanTest(bsize int) (*spanCache, *spanFake) {
	f := &spanFake{block: spanBlock(bsize), fail: map[int]bool{}}
	c := newSpanCache(spanMemMax, spanTTL)
	c.fetch = f.fetch
	return c, f
}

func TestJMSpanReadFetchesSpanOnceAndServesTheRest(t *testing.T) {
	const bs = 4 << 20
	c, f := newSpanTest(bs)
	// A 1 MiB jump at 1 MiB, read as eight 128 KiB FUSE reads in order.
	for i := 0; i < 8; i++ {
		off := 1<<20 + i*(128<<10)
		p := make([]byte, 128<<10)
		n, started, ok := c.read("k", off, p, bs)
		if !ok || n != len(p) || !bytes.Equal(p, f.block[off:off+n]) {
			t.Fatalf("read %d: n=%d ok=%v bytes match=%v", i, n, ok, bytes.Equal(p[:n], f.block[off:off+n]))
		}
		if want := map[bool]int{true: 4, false: 0}[i == 0]; started != want {
			t.Fatalf("read %d started %d parts, want %d", i, started, want)
		}
	}
	if got := f.calls.Load(); got != 4 {
		t.Fatalf("GETs = %d, want 4 (one per 256 KiB part of the jump)", got)
	}
}

func TestJMSpanPartsRunInParallel(t *testing.T) {
	const bs = 4 << 20
	c, f := newSpanTest(bs)
	f.gate = make(chan struct{})
	done := make(chan bool)
	go func() {
		p := make([]byte, 128<<10)
		_, _, ok := c.read("k", 0, p, bs)
		done <- ok
	}()
	deadline := time.Now().Add(5 * time.Second)
	for f.running.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := f.running.Load(); got != 4 {
		t.Fatalf("parts running at once = %d, want 4", got)
	}
	close(f.gate)
	if !<-done {
		t.Fatal("read failed")
	}
}

func TestJMSpanConcurrentReadsShareParts(t *testing.T) {
	const bs = 4 << 20
	c, f := newSpanTest(bs)
	f.gate = make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			off := i * (128 << 10)
			p := make([]byte, 128<<10)
			n, _, ok := c.read("k", off, p, bs)
			if !ok || !bytes.Equal(p[:n], f.block[off:off+n]) || n != len(p) {
				t.Errorf("read at %d: n=%d ok=%v", off, n, ok)
			}
		}(i)
	}
	time.Sleep(50 * time.Millisecond)
	close(f.gate)
	wg.Wait()
	// Reads that arrive out of order may start windows at different parts,
	// but no part is ever fetched twice.
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := map[int]bool{}
	for _, o := range f.offs {
		if seen[o] {
			t.Fatalf("part %d fetched twice", o)
		}
		seen[o] = true
	}
}

func TestJMSpanReadAcrossPartsAndBlockEnd(t *testing.T) {
	const bs = 1<<20 + 100<<10 // a short last block: 4 full parts and one of 100 KiB
	c, f := newSpanTest(bs)
	// A read that crosses the 256 KiB boundary waits for both parts.
	p := make([]byte, 128<<10)
	off := 200 << 10
	n, _, ok := c.read("k", off, p, bs)
	if !ok || n != len(p) || !bytes.Equal(p, f.block[off:off+n]) {
		t.Fatalf("cross read: n=%d ok=%v", n, ok)
	}
	// A read at the end of the block is clipped to it.
	off = bs - 50<<10
	n, _, ok = c.read("k", off, p, bs)
	if !ok || n != 50<<10 || !bytes.Equal(p[:n], f.block[off:]) {
		t.Fatalf("tail read: n=%d ok=%v", n, ok)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, o := range f.offs {
		if o%spanPartSize != 0 || o >= bs {
			t.Fatalf("part offset %d not aligned or past the block", o)
		}
	}
}

func TestJMSpanFailedPartIsRetriedAndReported(t *testing.T) {
	const bs = 4 << 20
	c, f := newSpanTest(bs)
	f.fail[256<<10] = true
	p := make([]byte, 128<<10)
	if _, _, ok := c.read("k", 0, p, bs); !ok {
		t.Fatal("a read of a good part failed because another part failed")
	}
	if _, _, ok := c.read("k", 256<<10, p, bs); ok {
		t.Fatal("a read of a failed part reported success")
	}
	f.mu.Lock()
	f.fail[256<<10] = false
	f.mu.Unlock()
	before := f.calls.Load()
	n, _, ok := c.read("k", 256<<10, p, bs)
	if !ok || !bytes.Equal(p[:n], f.block[256<<10:256<<10+n]) {
		t.Fatal("retry of the failed part did not fetch it again")
	}
	if f.calls.Load() == before {
		t.Fatal("no new GET for the failed part")
	}
}

func TestJMSpanWholeBlockGoesToDiskCacheOnce(t *testing.T) {
	const bs = 4 << 20
	c, f := newSpanTest(bs)
	var got [][]byte
	var mu sync.Mutex
	c.whole = func(key string, parts [][]byte, size int) {
		block := bytes.Join(parts, nil)
		if len(block) != size {
			t.Errorf("whole: %d bytes, size %d", len(block), size)
		}
		mu.Lock()
		got = append(got, block)
		mu.Unlock()
	}
	p := make([]byte, 128<<10)
	for off := 0; off < bs; off += 128 << 10 {
		if _, _, ok := c.read("k", off, p, bs); !ok {
			t.Fatal("read failed")
		}
	}
	// whole runs after the last part closes; give it a moment.
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || !bytes.Equal(got[0], f.block) {
		t.Fatalf("whole calls = %d (want 1 with the block's bytes)", len(got))
	}
}

func TestJMSpanMemoryBoundAndExpiry(t *testing.T) {
	const bs = 4 << 20
	f := &spanFake{block: spanBlock(bs), fail: map[int]bool{}}
	c := newSpanCache(2*spanPartSize, 50*time.Millisecond)
	c.fetch = f.fetch
	p := make([]byte, 128<<10)
	c.read("k", 0, p, bs) // 4 parts; only 2 kept
	c.mu.Lock()
	if c.bytes > 2*spanPartSize || c.lru.Len() > 2 {
		t.Fatalf("kept %d bytes in %d parts, bound is 2 parts", c.bytes, c.lru.Len())
	}
	c.mu.Unlock()
	time.Sleep(80 * time.Millisecond)
	before := f.calls.Load()
	c.read("k", 3*spanPartSize, p, bs)
	if f.calls.Load() == before {
		t.Fatal("an expired part was served from memory")
	}
}

// Through the store: a scrub jump (eight 128 KiB reads of 1 MiB inside a
// block) makes four 256 KiB ranged GETs and no whole-block GET; reading a
// whole block through leaves it in the block cache, still without one.
func TestJMSpanStoreScrubJumpAndSequentialBlock(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	data := writeBlocks(t, mem, 301, 2)
	d := &demandStorage{ObjectStorage: mem}
	store := NewCachedStore(d, demandConf(t, true), nil).(*cachedStore)
	if !store.spansOn() {
		t.Fatal("spans are off with --demand-first on a plain object store")
	}
	r := store.NewReader(301, len(data))
	bs := store.conf.BlockSize
	jump, jumpLen := bs, min(bs, spanSize) // 1 MiB (or the block) at block 1
	for off := jump; off < jump+jumpLen; off += 128 << 10 {
		if got := readAt(t, r, off, 128<<10); !bytes.Equal(got, data[off:off+128<<10]) {
			t.Fatalf("jump read at %d returned wrong bytes", off)
		}
	}
	jumpParts := int32(jumpLen / spanPartSize)
	if n := d.ranged.Load(); n != jumpParts {
		t.Fatalf("ranged GETs for the jump %d, want %d", n, jumpParts)
	}
	for off := 0; off < bs; off += 128 << 10 {
		if got := readAt(t, r, off, 128<<10); !bytes.Equal(got, data[off:off+128<<10]) {
			t.Fatalf("block 0 read at %d returned wrong bytes", off)
		}
	}
	waitCached(t, store, sliceForRead(301, len(data), store).key(0))
	if n := d.full.Load(); n != 0 {
		t.Fatalf("whole-block GETs %d, want 0", n)
	}
	if n := d.ranged.Load(); n != jumpParts+int32(bs/spanPartSize) {
		t.Fatalf("ranged GETs %d, want %d", n, jumpParts+int32(bs/spanPartSize))
	}
}

// Only the parts a read waits for count as demand reads (patch 0020's
// signal); the rest of the span is fetched without being counted.
func TestJMSpanDemandCountsOnlyAwaitedParts(t *testing.T) {
	const bs = 4 << 20
	c, f := newSpanTest(bs)
	f.gate = make(chan struct{})
	var mu sync.Mutex
	counted := map[string]int{}
	inflight := 0
	c.demand = func(key string) func() {
		mu.Lock()
		counted[key]++
		inflight++
		mu.Unlock()
		return func() { mu.Lock(); inflight--; mu.Unlock() }
	}
	done := make(chan bool)
	go func() {
		_, _, ok := c.read("k", 0, make([]byte, 128<<10), bs)
		done <- ok
	}()
	deadline := time.Now().Add(5 * time.Second)
	for f.running.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	if inflight != 1 || counted["k"] != 1 {
		mu.Unlock()
		t.Fatalf("demand in flight %d (counted %d) while 4 parts run, want 1: only the awaited part", inflight, counted["k"])
	}
	mu.Unlock()
	close(f.gate)
	if !<-done {
		t.Fatal("read failed")
	}
	mu.Lock()
	defer mu.Unlock()
	if inflight != 0 {
		t.Fatalf("demand left in flight: %d", inflight)
	}
	// A read served from a finished part is not counted.
	if _, _, ok := c.read("k", 256<<10, make([]byte, 128<<10), bs); !ok || counted["k"] != 1 {
		t.Fatalf("a hit counted as a demand read (count %d)", counted["k"])
	}
}

// Finished parts are dropped after ttl even when no read comes.
func TestJMSpanSweepWithoutReads(t *testing.T) {
	const bs = 4 << 20
	f := &spanFake{block: spanBlock(bs), fail: map[int]bool{}}
	c := newSpanCache(spanMemMax, 30*time.Millisecond)
	c.fetch = f.fetch
	c.read("k", 0, make([]byte, 128<<10), bs)
	deadline := time.Now().Add(3 * time.Second)
	for {
		c.mu.Lock()
		kept, parts := c.bytes, len(c.parts)
		c.mu.Unlock()
		if kept == 0 && parts == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d bytes in %d keys still kept after the ttl with no reads", kept, parts)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

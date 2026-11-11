package chunk

// JuiceMount: with --demand-first, a read that waits gets its bytes before
// the prefetcher (patch 0020).

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

// demandStorage holds whole-object Gets on fullGate and ranged Gets on
// rangedGate (nil: not held).
type demandStorage struct {
	object.ObjectStorage
	full, ranged atomic.Int32
	fullGate     chan struct{}
	rangedGate   chan struct{}
}

func (d *demandStorage) Get(key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	if limit < 0 {
		d.full.Add(1)
		if d.fullGate != nil {
			<-d.fullGate
		}
	} else {
		d.ranged.Add(1)
		if d.rangedGate != nil {
			<-d.rangedGate
		}
	}
	return d.ObjectStorage.Get(key, off, limit, getters...)
}

// noSpans turns off demand spans (patch 0021), so a test sees the patch 0020
// ranged read, piggyback and prefetch defer that spans replace.
func noSpans(store *cachedStore) *cachedStore {
	store.spans = nil
	return store
}

func demandConf(t *testing.T, demandFirst bool) Config {
	conf := rangedConf(t, 1)
	conf.DemandFirst = demandFirst
	return conf
}

// goRead reads in a goroutine and sends the bytes, or nil on any error (a
// goroutine must not call t.Fatal).
func goRead(r Reader, off, n int) chan []byte {
	done := make(chan []byte, 1)
	go func() {
		p := NewPage(make([]byte, n))
		got, err := r.ReadAt(context.Background(), p, off)
		if err != nil || got != n {
			done <- nil
			return
		}
		done <- p.Data
	}()
	return done
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// The lab shape: the first read at offset 0 queues the block for the
// prefetcher; a second read a quarter into the same block arrives while that
// whole-block download is in flight. Upstream piggybacks it on the download,
// so it waits for all 4 MiB. With --demand-first it sends its own ranged GET.
func TestJMDemandFirstSecondReadInBlockDoesNotWaitForPrefetch(t *testing.T) {
	for _, demandFirst := range []bool{true, false} {
		name := map[bool]string{true: "demand-first", false: "upstream piggyback"}[demandFirst]
		t.Run(name, func(t *testing.T) {
			mem, _ := object.CreateStorage("mem", "", "", "", "")
			data := writeBlocks(t, mem, 201, 1)
			d := &demandStorage{ObjectStorage: mem, fullGate: make(chan struct{})}
			store := noSpans(NewCachedStore(d, demandConf(t, demandFirst), nil).(*cachedStore))
			r := store.NewReader(201, len(data))
			bs := store.conf.BlockSize
			off, n := bs/4, bs/8 // a second read a quarter into the block
			if got := readAt(t, r, 0, 64<<10); !bytes.Equal(got, data[:64<<10]) {
				t.Fatal("first read returned wrong bytes")
			}
			waitFor(t, "the prefetch download to start", func() bool { return d.full.Load() == 1 })

			done := goRead(r, off, n)
			select {
			case got := <-done:
				if !demandFirst {
					close(d.fullGate)
					t.Fatal("control: the read did not wait for the in-flight block (test is not exercising piggyback)")
				}
				if !bytes.Equal(got, data[off:off+n]) {
					t.Fatal("second read returned wrong bytes")
				}
				if n := d.ranged.Load(); n != 2 {
					t.Fatalf("ranged GETs %d, want 2", n)
				}
				close(d.fullGate)
			case <-time.After(300 * time.Millisecond):
				close(d.fullGate)
				if demandFirst {
					t.Fatal("the second read waited for the whole-block download")
				}
				if got := <-done; !bytes.Equal(got, data[off:off+n]) {
					t.Fatal("piggybacked read returned wrong bytes")
				}
			}
			waitCached(t, store, sliceForRead(201, len(data), store).key(0))
			if n := d.full.Load(); n != 1 {
				t.Fatalf("whole-block GETs %d, want 1", n)
			}
		})
	}
}

// The prefetcher does not start a whole-block download while a ranged demand
// GET is in flight; it starts when the demand read ends.
func TestJMDemandFirstPrefetchWaitsForDemandRead(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	data := writeBlocks(t, mem, 202, 2)
	d := &demandStorage{ObjectStorage: mem, rangedGate: make(chan struct{})}
	store := noSpans(NewCachedStore(d, demandConf(t, true), nil).(*cachedStore))
	r := store.NewReader(202, len(data))
	bs := store.conf.BlockSize

	done := goRead(r, bs+4096, 64<<10) // block 1, held
	waitFor(t, "the ranged demand GET", func() bool { return d.ranged.Load() == 1 })

	key0 := sliceForRead(202, len(data), store).key(0)
	store.fetcher.fetch(key0) // a prefetch of another block
	time.Sleep(200 * time.Millisecond)
	if n := d.full.Load(); n != 0 {
		t.Fatalf("the prefetch downloaded %d block(s) while a demand read was in flight", n)
	}
	close(d.rangedGate)
	if got := <-done; !bytes.Equal(got, data[bs+4096:bs+4096+64<<10]) {
		t.Fatal("demand read returned wrong bytes")
	}
	waitCached(t, store, key0)
	waitCached(t, store, sliceForRead(202, len(data), store).key(1))
}

// A stream of demand reads cannot hold the prefetcher off for longer than
// deferMax.
func TestJMDemandFirstPrefetchDeferIsBounded(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	data := writeBlocks(t, mem, 203, 2)
	d := &demandStorage{ObjectStorage: mem, rangedGate: make(chan struct{})}
	store := noSpans(NewCachedStore(d, demandConf(t, true), nil).(*cachedStore))
	store.demand.deferMax = 150 * time.Millisecond
	r := store.NewReader(203, len(data))
	bs := store.conf.BlockSize
	done := goRead(r, bs+4096, 64<<10)
	waitFor(t, "the ranged demand GET", func() bool { return d.ranged.Load() == 1 })
	key0 := sliceForRead(203, len(data), store).key(0)
	start := time.Now()
	store.fetcher.fetch(key0)
	waitCached(t, store, key0) // the demand read is still held
	if waited := time.Since(start); waited < 100*time.Millisecond {
		t.Fatalf("the prefetch did not wait for the demand read (%s)", waited)
	}
	close(d.rangedGate)
	<-done
}

func TestJMDemandFirstPiggybackOnlyWhenDownloadNearlyDone(t *testing.T) {
	const bs = 4 << 20
	off := newDemandFirst(false, bs)
	if !off.piggyback("k", 64<<10) {
		t.Fatal("without --demand-first a read must piggyback as upstream does")
	}
	var nilDF *demandFirst
	if !nilDF.piggyback("k", 64<<10) {
		t.Fatal("a store without the hook must piggyback as upstream does")
	}

	df := newDemandFirst(true, bs)
	end := df.beginFull("k")
	if df.piggyback("k", 64<<10) {
		t.Fatal("with no measurements a read must send its own GET")
	}
	df.fullNS.Store(int64(time.Second))
	df.rangedNSPerKB.Store(int64(100 * time.Millisecond / 64)) // 64 KiB in 100 ms
	if df.piggyback("k", 64<<10) {
		t.Fatal("a download with ~1 s left must not hold a read whose GET takes 100 ms")
	}
	setStart := func(key string, ago time.Duration) {
		df.mu.Lock()
		df.fullAt[key] = &fullDownload{start: time.Now().Add(-ago)}
		df.mu.Unlock()
	}
	setStart("late", 900*time.Millisecond) // ~100 ms left
	if !df.piggyback("late", 64<<10) {
		t.Fatal("a download about to finish should be shared")
	}
	// A 1 MiB GET takes ~1.6 s at this rate, so ~100 ms left is well within it.
	if !df.piggyback("late", 1<<20) {
		t.Fatal("the per-KiB estimate must scale with the read size")
	}
	if df.piggyback("other", 64<<10) {
		t.Fatal("an untracked download must not hold a read")
	}
	end(nil)
	if df.piggyback("k", 64<<10) {
		t.Fatal("a finished download is no longer tracked")
	}
	if got := time.Duration(df.fullNS.Load()); got >= time.Second { // a ~0 s sample pulls it down
		t.Fatalf("the finished download did not update the estimate (%s)", got)
	}
}

// A download that overran its smoothed time is late, not nearly done: reads
// must not wait for it (that brought back the 53 s case on lossy links).
func TestJMDemandFirstDoesNotWaitForOverrunDownload(t *testing.T) {
	df := newDemandFirst(true, 4<<20)
	df.fullNS.Store(int64(time.Second))
	df.rangedNSPerKB.Store(int64(time.Millisecond))
	df.mu.Lock()
	df.fullAt["slow"] = &fullDownload{start: time.Now().Add(-30 * time.Second)}
	df.mu.Unlock()
	if df.piggyback("slow", 64<<10) {
		t.Fatal("a read waited for a download 29 s past its expected end")
	}
}

// The re-review's case (M-2): 1 MiB reads spend the quarter-block budget of a
// 4 MiB block after one read. Against a late download the third read (and
// every later one) must still send its own GET, never wait for it.
func TestJMDemandFirstLateDownloadIgnoresBudget(t *testing.T) {
	df := newDemandFirst(true, 4<<20)
	df.fullNS.Store(int64(time.Second))
	df.rangedNSPerKB.Store(int64(time.Millisecond))
	df.mu.Lock()
	df.fullAt["late"] = &fullDownload{start: time.Now().Add(-30 * time.Second)}
	df.mu.Unlock()
	for i := 1; i <= 3; i++ {
		if df.piggyback("late", 1<<20) {
			t.Fatalf("1 MiB read %d waited for a download 29 s past its expected end", i)
		}
	}
}

// Reads that bypass one download total at most a quarter block; later reads
// in that block wait for it, so a sequential reader does not fetch the block
// twice.
func TestJMDemandFirstBypassBudgetPerDownload(t *testing.T) {
	const bs = 4 << 20
	df := newDemandFirst(true, bs)
	end := df.beginFull("k")
	defer end(nil)
	n := 128 << 10
	for got := 0; got+n <= bs/4; got += n {
		if df.piggyback("k", n) {
			t.Fatalf("read at %d KiB of bypass waited; the budget is %d KiB", got>>10, bs/4>>10)
		}
	}
	if !df.piggyback("k", n) {
		t.Fatal("a read past the quarter-block budget must wait for the download")
	}
	end2 := df.beginFull("k2") // a new download has its own budget
	defer end2(nil)
	if df.piggyback("k2", n) {
		t.Fatal("another block's download must start with a fresh budget")
	}
}

func TestJMDemandFirstBeginDemandIdle(t *testing.T) {
	df := newDemandFirst(true, 4<<20)
	if w := df.waitIdle("k"); w != 0 {
		t.Fatalf("waitIdle with nothing in flight waited %s", w)
	}
	end1, end2 := df.beginDemand("a"), df.beginDemand("a")
	end1()
	end1() // a second call is ignored
	if w := df.waitIdle("a"); w != 0 {
		t.Fatalf("demand reads of the same block held its prefetch for %s", w)
	}
	df.deferMax = 50 * time.Millisecond
	if w := df.waitIdle("k"); w < 40*time.Millisecond {
		t.Fatalf("waitIdle returned after %s with a demand read of another block in flight", w)
	}
	df.deferMax = 5 * time.Second
	go func() { time.Sleep(20 * time.Millisecond); end2() }()
	if w := df.waitIdle("k"); w <= 0 || w > 2*time.Second {
		t.Fatalf("waitIdle did not end when the last demand read ended (%s)", w)
	}
	if w := df.waitIdle("k"); w != 0 {
		t.Fatalf("waitIdle waited %s after every demand read ended", w)
	}
}

// A sequential reader: the prefetch of the block being read is not held by
// demand reads of that same block (its download is what serves them).
func TestJMDemandFirstPrefetchNotHeldBySameBlockDemand(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	data := writeBlocks(t, mem, 205, 1)
	d := &demandStorage{ObjectStorage: mem, rangedGate: make(chan struct{})}
	store := noSpans(NewCachedStore(d, demandConf(t, true), nil).(*cachedStore))
	r := store.NewReader(205, len(data))
	bs := store.conf.BlockSize
	done := goRead(r, bs/2, 64<<10) // block 0, held
	waitFor(t, "the ranged demand GET", func() bool { return d.ranged.Load() == 1 })
	key0 := sliceForRead(205, len(data), store).key(0)
	store.fetcher.fetch(key0)
	waitCached(t, store, key0) // filled while the same-block demand read is held
	close(d.rangedGate)
	if got := <-done; !bytes.Equal(got, data[bs/2:bs/2+64<<10]) {
		t.Fatal("demand read returned wrong bytes")
	}
}

// --demand-first changes nothing on an encrypted volume: no ranged read, one
// whole-object GET per block.
func TestJMDemandFirstEncryptedStaysWholeBlock(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := object.NewDataEncryptor(object.NewRSAEncryptor(key), object.AES256GCM_RSA)
	if err != nil {
		t.Fatal(err)
	}
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	data := writeBlocks(t, object.NewEncrypted(mem, enc), 204, 1)
	d := &demandStorage{ObjectStorage: mem}
	conf := demandConf(t, true)
	conf.Encrypted = true
	store := NewCachedStore(object.NewEncrypted(d, enc), conf, nil).(*cachedStore)
	r := store.NewReader(204, len(data))
	bs := store.conf.BlockSize
	for _, off := range []int{0, bs / 4, 3 * bs / 4} {
		if got := readAt(t, r, off, 64<<10); !bytes.Equal(got, data[off:off+64<<10]) {
			t.Fatalf("wrong bytes at %d", off)
		}
	}
	waitCached(t, store, sliceForRead(204, len(data), store).key(0))
	time.Sleep(50 * time.Millisecond) // a stray prefetch would show up here
	if n, rg := d.full.Load(), d.ranged.Load(); n != 1 || rg != 0 {
		t.Fatalf("whole-object GETs %d, ranged %d, want 1 and 0", n, rg)
	}
}

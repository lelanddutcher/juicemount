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
			store := NewCachedStore(d, demandConf(t, demandFirst), nil).(*cachedStore)
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
	store := NewCachedStore(d, demandConf(t, true), nil).(*cachedStore)
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
	store := NewCachedStore(d, demandConf(t, true), nil).(*cachedStore)
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
	off := newDemandFirst(false)
	if !off.piggyback("k") {
		t.Fatal("without --demand-first a read must piggyback as upstream does")
	}
	var nilDF *demandFirst
	if !nilDF.piggyback("k") {
		t.Fatal("a store without the hook must piggyback as upstream does")
	}

	df := newDemandFirst(true)
	end := df.beginFull("k")
	if df.piggyback("k") {
		t.Fatal("with no measurements a read must send its own GET")
	}
	df.fullNS.Store(int64(time.Second))
	df.rangedNS.Store(int64(100 * time.Millisecond))
	if df.piggyback("k") {
		t.Fatal("a download with ~1 s left must not hold a read whose GET takes 100 ms")
	}
	df.mu.Lock()
	df.fullAt["late"] = time.Now().Add(-900 * time.Millisecond) // started 0.9 s ago
	df.mu.Unlock()
	if !df.piggyback("late") {
		t.Fatal("a download about to finish should be shared")
	}
	if df.piggyback("other") {
		t.Fatal("an untracked download must not hold a read")
	}
	end(nil)
	if df.piggyback("k") {
		t.Fatal("a finished download is no longer tracked")
	}
	if got := time.Duration(df.fullNS.Load()); got >= time.Second { // a ~0 s sample pulls it down
		t.Fatalf("the finished download did not update the estimate (%s)", got)
	}
}

func TestJMDemandFirstBeginDemandIdle(t *testing.T) {
	df := newDemandFirst(true)
	if w := df.waitIdle(); w != 0 {
		t.Fatalf("waitIdle with nothing in flight waited %s", w)
	}
	end1, end2 := df.beginDemand(), df.beginDemand()
	end1()
	end1() // a second call is ignored
	df.deferMax = 50 * time.Millisecond
	if w := df.waitIdle(); w < 40*time.Millisecond {
		t.Fatalf("waitIdle returned after %s with one demand read in flight", w)
	}
	end2()
	if w := df.waitIdle(); w != 0 {
		t.Fatalf("waitIdle waited %s after every demand read ended", w)
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

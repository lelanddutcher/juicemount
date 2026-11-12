package chunk

// JuiceMount: the small-upload lane (patch 0024, jm_upload_lane.go).

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

// laneStorage counts concurrent PUTs by the logical size in the key and can
// hold large (whole 4 MiB block) PUTs on a gate until the test opens it.
type laneStorage struct {
	object.ObjectStorage
	largeGate chan struct{} // nil: large PUTs are not held
	smallGate chan struct{} // nil: small PUTs are not held
	failSmall atomic.Bool

	mu                  sync.Mutex
	small, large        int
	maxSmall, maxLarge  int
	maxTotal            int
	largeDone, smallPut atomic.Int32
}

func (l *laneStorage) Put(key string, in io.Reader, getters ...object.AttrGetter) error {
	small := parseObjOrigSize(key) <= SmallUploadBytes
	l.mu.Lock()
	if small {
		l.small++
		l.maxSmall = max(l.maxSmall, l.small)
	} else {
		l.large++
		l.maxLarge = max(l.maxLarge, l.large)
	}
	l.maxTotal = max(l.maxTotal, l.small+l.large)
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		if small {
			l.small--
		} else {
			l.large--
		}
		l.mu.Unlock()
	}()
	if small {
		if l.smallGate != nil {
			<-l.smallGate
		}
		l.smallPut.Add(1)
		if l.failSmall.Load() {
			return errors.New("test small PUT failure")
		}
	} else if l.largeGate != nil {
		<-l.largeGate
	}
	err := l.ObjectStorage.Put(key, in, getters...)
	if !small {
		l.largeDone.Add(1)
	}
	return err
}

func (l *laneStorage) counts() (maxSmall, maxLarge, maxTotal int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.maxSmall, l.maxLarge, l.maxTotal
}

func (l *laneStorage) inFlight() (small, large int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.small, l.large
}

// laneConf is the app's mount on a slow link: 4 MiB blocks, --max-uploads 4,
// no writeback, and a PUT deadline far above the test's holds.
func laneConf(smallUploads int) Config {
	conf := defaultConf
	conf.BlockSize = 4 << 20
	conf.CacheDir = "memory"
	conf.MaxUpload = 4
	conf.SmallUploads = smallUploads
	conf.PutTimeout = time.Minute
	conf.MaxRetries = 1
	conf.BufferSize = 64 << 20
	return conf
}

func laneBytes(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i*7)
	}
	return b
}

// writeSlice writes data into a new slice and finishes it in a goroutine;
// the channel gets Finish's result. A slice of at most 1 MiB is marked as
// pkg/vfs marks an fsync or close of a small file.
func writeSlice(store ChunkStore, id uint64, data []byte) chan error {
	return writeSliceMarked(store, id, data, len(data) <= SmallUploadBytes)
}

// writeSliceMarked is writeSlice with the mark chosen by the caller: false
// is a background flush (a slice older than 5 s or idle for 1 s).
func writeSliceMarked(store ChunkStore, id uint64, data []byte, smallSave bool) chan error {
	done := make(chan error, 1)
	go func() {
		w := store.NewWriter(id)
		if _, err := w.WriteAt(data, 0); err != nil {
			done <- err
			return
		}
		if smallSave {
			w.(interface{ SetSmallSave() }).SetSmallSave()
		}
		done <- w.Finish(len(data))
	}()
	return done
}

func readSlice(t *testing.T, store ChunkStore, id uint64, n int) []byte {
	t.Helper()
	p := NewPage(make([]byte, n))
	defer p.Release()
	got, err := store.NewReader(id, n).ReadAt(context.Background(), p, 0)
	if err != nil || got != n {
		t.Fatalf("read slice %d: %d bytes, %v", id, got, err)
	}
	return append([]byte(nil), p.Data...)
}

// The lab shape: a render holds every shared token with whole 4 MiB blocks
// and has more queued. A small save's fsync (Finish) must return while those
// PUTs are still running. Without the lane (control) it waits behind them.
func TestJMUploadLaneSmallSaveNotBehindLargeBlocks(t *testing.T) {
	for _, lane := range []int{2, 0} {
		t.Run(fmt.Sprintf("small-uploads=%d", lane), func(t *testing.T) {
			mem, _ := object.CreateStorage("mem", "", "", "", "")
			ls := &laneStorage{ObjectStorage: mem, largeGate: make(chan struct{})}
			store := NewCachedStore(ls, laneConf(lane), nil)
			large := laneBytes(6*4<<20, 1) // 6 blocks: 4 run, 2 queue
			largeDone := writeSlice(store, 1, large)
			waitFor(t, "four large PUTs", func() bool { _, l := ls.inFlight(); return l == 4 })

			smallData := laneBytes(300<<10, 9)
			smallDone := writeSlice(store, 2, smallData)
			if lane > 0 {
				select {
				case err := <-smallDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("small save waited behind the large blocks")
				}
				if _, l := ls.inFlight(); l != 4 || ls.largeDone.Load() != 0 {
					t.Fatalf("large PUTs in flight %d, done %d: want 4 and 0", l, ls.largeDone.Load())
				}
			} else {
				select {
				case <-smallDone:
					t.Fatal("control: without the lane the small save must queue behind the large blocks")
				case <-time.After(200 * time.Millisecond):
				}
			}
			close(ls.largeGate)
			if err := <-largeDone; err != nil {
				t.Fatal(err)
			}
			if lane == 0 {
				if err := <-smallDone; err != nil {
					t.Fatal(err)
				}
			}
			if got := readSlice(t, store, 2, len(smallData)); !bytes.Equal(got, smallData) {
				t.Fatal("small slice read back wrong")
			}
			if got := readSlice(t, store, 1, len(large)); !bytes.Equal(got, large) {
				t.Fatal("large slice read back wrong")
			}
		})
	}
}

// Finish returns only after the small block's PUT: while the PUT is held,
// Finish waits; a failed PUT fails Finish.
func TestJMUploadLaneFinishWaitsForThePut(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	ls := &laneStorage{ObjectStorage: mem, smallGate: make(chan struct{})}
	store := NewCachedStore(ls, laneConf(2), nil)
	done := writeSlice(store, 3, laneBytes(64<<10, 3))
	waitFor(t, "small PUT", func() bool { s, _ := ls.inFlight(); return s == 1 })
	select {
	case <-done:
		t.Fatal("Finish returned before the PUT")
	case <-time.After(100 * time.Millisecond):
	}
	close(ls.smallGate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := mem.Head(sliceForRead(3, 64<<10, store.(*cachedStore)).key(0)); err != nil {
		t.Fatalf("object missing after Finish: %v", err)
	}

	ls2 := &laneStorage{ObjectStorage: mem}
	ls2.failSmall.Store(true)
	store2 := NewCachedStore(ls2, laneConf(2), nil)
	if err := <-writeSlice(store2, 4, laneBytes(64<<10, 4)); err == nil {
		t.Fatal("a failed small PUT must fail Finish")
	}
}

// Bounds under load: large blocks never hold more than MaxUpload PUTs, and
// all PUTs together never exceed MaxUpload + SmallUploads. Every slice reads
// back correctly. Large slices finish although small ones keep arriving.
func TestJMUploadLaneBoundsUnderMixedLoad(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	ls := &laneStorage{ObjectStorage: &slowStorage{ObjectStorage: mem, delay: 2 * time.Millisecond}}
	conf := laneConf(2)
	store := NewCachedStore(ls, conf, nil)
	type job struct {
		id   uint64
		data []byte
		done chan error
	}
	var jobs []job
	for i := 0; i < 3; i++ {
		id := uint64(100 + i)
		data := laneBytes(9*4<<20, byte(i))
		jobs = append(jobs, job{id, data, writeSlice(store, id, data)})
	}
	for i := 0; i < 40; i++ {
		id := uint64(200 + i)
		data := laneBytes(1+i*25<<10, byte(i))
		jobs = append(jobs, job{id, data, writeSlice(store, id, data)})
	}
	for _, j := range jobs {
		select {
		case err := <-j.done:
			if err != nil {
				t.Fatalf("slice %d: %v", j.id, err)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("slice %d did not finish", j.id)
		}
	}
	maxSmall, maxLarge, maxTotal := ls.counts()
	if maxLarge > conf.MaxUpload || maxTotal > conf.MaxUpload+conf.SmallUploads {
		t.Fatalf("concurrent PUTs: large %d (max %d), total %d (max %d)",
			maxLarge, conf.MaxUpload, maxTotal, conf.MaxUpload+conf.SmallUploads)
	}
	if maxSmall == 0 || maxLarge == 0 {
		t.Fatalf("no overlap measured: small %d, large %d", maxSmall, maxLarge)
	}
	for _, j := range jobs {
		if got := readSlice(t, store, j.id, len(j.data)); !bytes.Equal(got, j.data) {
			t.Fatalf("slice %d read back wrong", j.id)
		}
	}
	if n := store.(*cachedStore).uploadsInFlight(); n != 0 {
		t.Fatalf("%d upload tokens still held", n)
	}
}

type slowStorage struct {
	object.ObjectStorage
	delay time.Duration
}

func (s *slowStorage) Put(key string, in io.Reader, getters ...object.AttrGetter) error {
	time.Sleep(s.delay)
	return s.ObjectStorage.Put(key, in, getters...)
}

// Only a marked slice of at most 1 MiB takes the lane: not an unmarked one
// (a background flush), and not a marked one larger than 1 MiB.
func TestJMUploadLaneOnlyMarkedSmallSlices(t *testing.T) {
	store := NewCachedStore(&laneStorage{ObjectStorage: nil}, laneConf(2), nil).(*cachedStore)
	cases := []struct {
		length int
		marked bool
		want   bool
	}{
		{1, true, true},
		{SmallUploadBytes, true, true},
		{SmallUploadBytes + 1, true, false},
		{4<<20 + 1000, true, false},
		{1, false, false},
		{SmallUploadBytes, false, false},
	}
	for _, c := range cases {
		s := sliceForWrite(1, store)
		s.length = c.length
		if c.marked {
			s.SetSmallSave()
		}
		if got := s.smallLane(); got != c.want {
			t.Fatalf("length %d marked %v: lane %v, want %v", c.length, c.marked, got, c.want)
		}
	}
	off := NewCachedStore(&laneStorage{ObjectStorage: nil}, laneConf(0), nil).(*cachedStore)
	s := sliceForWrite(1, off)
	s.length = 1
	s.SetSmallSave()
	if s.smallLane() {
		t.Fatal("--small-uploads 0 must turn the lane off")
	}
}

// The review case: a large file written slower than 1 MiB per 5 s is flushed
// by the background timers in slices under 1 MiB. Those are not marked, so
// they wait for the shared tokens and never use the lane: a render on a thin
// link keeps to --max-uploads PUTs.
func TestJMUploadLaneUnmarkedSmallSlicesStayShared(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	ls := &laneStorage{ObjectStorage: mem, largeGate: make(chan struct{}), smallGate: make(chan struct{})}
	store := NewCachedStore(ls, laneConf(2), nil)
	large := laneBytes(4*4<<20, 1)
	largeDone := writeSlice(store, 1, large)
	waitFor(t, "four large PUTs", func() bool { _, l := ls.inFlight(); return l == 4 })
	var timer []chan error
	for i := 0; i < 3; i++ {
		timer = append(timer, writeSliceMarked(store, uint64(10+i), laneBytes(560<<10, byte(i)), false))
	}
	time.Sleep(100 * time.Millisecond)
	if s, _ := ls.inFlight(); s != 0 {
		t.Fatalf("%d unmarked small slices took the lane", s)
	}
	cs := store.(*cachedStore)
	if n := len(cs.smallUpload); n != 0 {
		t.Fatalf("lane tokens held: %d", n)
	}
	close(ls.smallGate)
	close(ls.largeGate)
	for _, d := range append(timer, largeDone) {
		if err := <-d; err != nil {
			t.Fatal(err)
		}
	}
	if _, maxLarge, maxTotal := ls.counts(); maxTotal > 4 || maxLarge > 4 {
		t.Fatalf("PUTs in flight: large %d total %d, want at most 4", maxLarge, maxTotal)
	}
}

// A small block takes a free lane token before a shared one, so it leaves the
// shared tokens to large blocks; with the lane full it takes a free shared one.
func TestJMUploadLanePrefersLaneTokens(t *testing.T) {
	store := NewCachedStore(&laneStorage{ObjectStorage: nil}, laneConf(2), nil).(*cachedStore)
	r1 := store.acquireUpload(true)
	r2 := store.acquireUpload(true)
	if len(store.smallUpload) != 2 || len(store.currentUpload) != 0 {
		t.Fatalf("lane %d shared %d, want 2 and 0", len(store.smallUpload), len(store.currentUpload))
	}
	r3 := store.acquireUpload(true)
	if len(store.currentUpload) != 1 {
		t.Fatal("with the lane full a small block must take a free shared token")
	}
	r4 := store.acquireUpload(false)
	if len(store.currentUpload) != 2 || len(store.smallUpload) != 2 {
		t.Fatal("a large block must take a shared token only")
	}
	for _, r := range []func(){r1, r2, r3, r4} {
		r()
	}
	if store.uploadsInFlight() != 0 {
		t.Fatal("tokens leaked")
	}
}

// Encrypted and compressed volumes: the lane changes only which token a PUT
// takes, so a small save still bypasses held large PUTs and reads back right.
func TestJMUploadLaneEncryptedCompressed(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := object.NewDataEncryptor(object.NewRSAEncryptor(key), object.AES256GCM_RSA)
	if err != nil {
		t.Fatal(err)
	}
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	ls := &laneStorage{ObjectStorage: mem, largeGate: make(chan struct{})}
	conf := laneConf(2)
	conf.Compress = "lz4"
	conf.Encrypted = true
	store := NewCachedStore(object.NewEncrypted(ls, enc), conf, nil)
	large := laneBytes(5*4<<20, 5)
	largeDone := writeSlice(store, 10, large)
	waitFor(t, "four large PUTs", func() bool { _, l := ls.inFlight(); return l == 4 })
	smallData := laneBytes(700<<10, 6)
	select {
	case err := <-writeSlice(store, 11, smallData):
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("small save waited behind the large blocks")
	}
	close(ls.largeGate)
	if err := <-largeDone; err != nil {
		t.Fatal(err)
	}
	// A fresh store (empty cache) reads from the object store.
	fresh := NewCachedStore(object.NewEncrypted(mem, enc), conf, nil)
	if got := readSlice(t, fresh, 11, len(smallData)); !bytes.Equal(got, smallData) {
		t.Fatal("small slice read back wrong")
	}
	if got := readSlice(t, fresh, 10, len(large)); !bytes.Equal(got, large) {
		t.Fatal("large slice read back wrong")
	}
}

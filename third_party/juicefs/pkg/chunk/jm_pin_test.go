package chunk

// JuiceMount: pinned blocks (patch 0026, jm_pin.go).

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

// pinStorage counts whole-object and ranged GETs and can fail the first GET
// of each key.
type pinStorage struct {
	object.ObjectStorage
	full, ranged atomic.Int32
	failFirst    bool
	mu           sync.Mutex
	failed       map[string]bool
}

func (s *pinStorage) Get(key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	if limit < 0 {
		s.full.Add(1)
	} else {
		s.ranged.Add(1)
	}
	if s.failFirst {
		s.mu.Lock()
		first := !s.failed[key]
		s.failed[key] = true
		s.mu.Unlock()
		if first {
			return nil, errors.New("test GET failure")
		}
	}
	return s.ObjectStorage.Get(key, off, limit, getters...)
}

// pinConf is a disk cache of cacheMiB in its own folder. The folder is not
// removed: the disk cache's background goroutines outlive the test.
func pinConf(t *testing.T, cacheMiB int) Config {
	t.Helper()
	dir, err := os.MkdirTemp("", "jmpin")
	if err != nil {
		t.Fatal(err)
	}
	conf := defaultConf
	conf.CacheDir = dir
	conf.CacheSize = uint64(cacheMiB) << 20
	conf.FreeSpace = 0.0001
	conf.CacheFullBlock = true
	conf.Prefetch = 1
	return conf
}

func pinStoreFor(t *testing.T, storage object.ObjectStorage, cacheMiB int) *cachedStore {
	t.Helper()
	return NewCachedStore(storage, pinConf(t, cacheMiB), nil).(*cachedStore)
}

func whole(id uint64, data []byte) []JMPinSlice {
	return []JMPinSlice{{ID: id, Size: uint32(len(data)), Len: uint32(len(data))}}
}

func onDisk(s *cachedStore, key string) bool {
	return s.bcache.(*cacheManager).jmOnDisk(key)
}

func TestJMPinFillDownloadsWholeBlocksToDisk(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	data := writeBlocks(t, mem, 301, 6)
	ps := &pinStorage{ObjectStorage: mem}
	store := pinStoreFor(t, ps, 64)
	var prog JMPinProgress
	if err := store.JMPinFill(context.Background(), 7, whole(301, data), JMPinAttr{}, 0, &prog); err != nil {
		t.Fatal(err)
	}
	keys := sliceForRead(301, len(data), store).keys()
	for _, k := range keys {
		if !onDisk(store, k) {
			t.Fatalf("block %s is not a cache file after the fill returned", k)
		}
	}
	if ps.full.Load() != 6 || ps.ranged.Load() != 0 {
		t.Fatalf("GETs: %d whole, %d ranged; want 6 whole, 0 ranged", ps.full.Load(), ps.ranged.Load())
	}
	if prog.Total.Load() != int64(len(data)) || prog.Done.Load() != prog.Total.Load() || prog.Fetched.Load() != prog.Total.Load() {
		t.Fatalf("progress total %d done %d fetched %d, want %d each", prog.Total.Load(), prog.Done.Load(), prog.Fetched.Load(), len(data))
	}
	// The pinned blocks serve reads without the object store.
	r := store.NewReader(301, len(data))
	if got := readAt(t, r, 3<<20, 128<<10); !bytes.Equal(got, data[3<<20:3<<20+128<<10]) {
		t.Fatal("wrong bytes from a pinned block")
	}
	// A second fill of present blocks downloads nothing.
	var again JMPinProgress
	if err := store.JMPinFill(context.Background(), 7, whole(301, data), JMPinAttr{}, 0, &again); err != nil {
		t.Fatal(err)
	}
	if ps.full.Load() != 6 || again.Fetched.Load() != 0 || again.Done.Load() != int64(len(data)) {
		t.Fatalf("refill fetched %d (GETs %d), want 0", again.Fetched.Load(), ps.full.Load())
	}
	if known, total, missing, _, err := store.JMPinCheck(7); err != nil || !known || total != int64(len(data)) || missing != 0 {
		t.Fatalf("check: known %v total %d missing %d err %v", known, total, missing, err)
	}
}

func TestJMPinEvictionSkipsPinnedBlocks(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	pinned := writeBlocks(t, mem, 311, 4)
	other := writeBlocks(t, mem, 312, 12)
	store := pinStoreFor(t, mem, 10) // 10 MiB cache, 1 MiB blocks
	m := store.bcache.(*cacheManager)
	if err := store.JMPinFill(context.Background(), 1, whole(311, pinned), JMPinAttr{}, 0, nil); err != nil {
		t.Fatal(err)
	}
	pkeys := sliceForRead(311, len(pinned), store).keys()
	// Make the pinned blocks the oldest, so plain LRU would evict them first.
	s := m.stores[0]
	for _, k := range pkeys {
		s.add(k, int32(parseObjOrigSize(k)), uint32(time.Now().Unix())-3600)
	}
	// 12 MiB of unpinned blocks through a 10 MiB cache.
	okeys := sliceForRead(312, len(other), store).keys()
	for _, k := range okeys {
		p := NewOffPage(parseObjOrigSize(k))
		if err := store.load(k, p, false, false); err != nil {
			t.Fatal(err)
		}
		if err := m.jmCacheSync(k, p); err != nil {
			t.Fatal(err)
		}
		p.Release()
	}
	for _, k := range pkeys {
		if !onDisk(store, k) {
			t.Fatalf("pinned block %s was evicted", k)
		}
	}
	evicted := 0
	for _, k := range okeys {
		if !onDisk(store, k) {
			evicted++
		}
	}
	if evicted == 0 {
		t.Fatal("no unpinned block was evicted: the cache never filled")
	}
	// Control: once unpinned, the same old blocks are evicted by pressure.
	store.JMPinDrop(1)
	for _, k := range pkeys {
		s.add(k, int32(parseObjOrigSize(k)), uint32(time.Now().Unix())-7200)
	}
	for _, k := range okeys {
		if onDisk(store, k) {
			continue
		}
		p := NewOffPage(parseObjOrigSize(k))
		if err := store.load(k, p, false, false); err != nil {
			t.Fatal(err)
		}
		_ = m.jmCacheSync(k, p)
		p.Release()
	}
	gone := 0
	for _, k := range pkeys {
		if !onDisk(store, k) {
			gone++
		}
	}
	if gone == 0 {
		t.Fatal("control: unpinned old blocks survived the same pressure")
	}
}

func TestJMPinCapRefusesAndKeepsRoomForReads(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	a := writeBlocks(t, mem, 321, 8)
	b := writeBlocks(t, mem, 322, 2)
	ps := &pinStorage{ObjectStorage: mem}
	store := pinStoreFor(t, ps, 10) // cap 9 MiB
	if err := store.JMPinFill(context.Background(), 1, whole(321, a), JMPinAttr{}, 0, nil); err != nil {
		t.Fatal(err)
	}
	// Pinning the same file again does not count its blocks twice.
	if err := store.JMPinFill(context.Background(), 1, whole(321, a), JMPinAttr{}, 0, nil); err != nil {
		t.Fatalf("refill of the same set: %v", err)
	}
	before := ps.full.Load()
	err := store.JMPinFill(context.Background(), 2, whole(322, b), JMPinAttr{}, 0, nil)
	if !errors.Is(err, ErrJMPinFull) {
		t.Fatalf("fill past the cap: %v, want ErrJMPinFull", err)
	}
	if ps.full.Load() != before {
		t.Fatal("a refused fill downloaded blocks")
	}
	if known, _, _, _, _ := store.JMPinCheck(2); known {
		t.Fatal("a refused fill left pins")
	}
	pinnedBytes, limit := store.JMPinUsage()
	if pinnedBytes > limit || limit != int64(float64(10<<20)*JMPinShare) {
		t.Fatalf("usage %d of %d", pinnedBytes, limit)
	}
	// Unpinning the first file makes room.
	store.JMPinDrop(1)
	if err := store.JMPinFill(context.Background(), 2, whole(322, b), JMPinAttr{}, 0, nil); err != nil {
		t.Fatal(err)
	}
}

func TestJMPinSharedSliceStaysPinnedUntilLastOwner(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	d := writeBlocks(t, mem, 331, 2)
	store := pinStoreFor(t, mem, 64)
	sl := whole(331, d)
	for _, owner := range []uint64{1, 2} {
		if err := store.JMPinFill(context.Background(), owner, sl, JMPinAttr{}, 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	m := store.bcache.(*cacheManager)
	k := jmCacheKey(sliceForRead(331, len(d), store).key(0))
	store.JMPinDrop(1)
	if !m.pins.has(k) {
		t.Fatal("dropping one owner unpinned a block another owner pins")
	}
	if p, _ := store.JMPinUsage(); p != 2*(1<<20+4096) {
		t.Fatalf("pinned bytes %d, want two blocks once", p)
	}
	store.JMPinDrop(2)
	if m.pins.has(k) {
		t.Fatal("block still pinned after the last owner dropped it")
	}
	if p, _ := store.JMPinUsage(); p != 0 {
		t.Fatalf("pinned bytes %d after every drop", p)
	}
}

func TestJMPinCheckFindsMissingBlocks(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	d := writeBlocks(t, mem, 341, 3)
	store := pinStoreFor(t, mem, 64)
	if known, _, _, _, _ := store.JMPinCheck(9); known {
		t.Fatal("an owner never pinned is known")
	}
	if err := store.JMPinFill(context.Background(), 9, whole(341, d), JMPinAttr{}, 0, nil); err != nil {
		t.Fatal(err)
	}
	k := sliceForRead(341, len(d), store).key(1)
	m := store.bcache.(*cacheManager)
	if err := os.Remove(m.getStore(k).cachePath(k)); err != nil {
		t.Fatal(err)
	}
	known, total, missing, _, err := store.JMPinCheck(9)
	if err != nil || !known || total != int64(len(d)) || missing != 1<<20 {
		t.Fatalf("check after a lost block: known %v total %d missing %d err %v", known, total, missing, err)
	}
	// A refill downloads only the lost block.
	var prog JMPinProgress
	if err := store.JMPinFill(context.Background(), 9, whole(341, d), JMPinAttr{}, 0, &prog); err != nil {
		t.Fatal(err)
	}
	if prog.Fetched.Load() != 1<<20 || !onDisk(store, k) {
		t.Fatalf("refill fetched %d", prog.Fetched.Load())
	}
}

func TestJMPinRetriesFailedGET(t *testing.T) {
	old := jmPinRetryWait
	jmPinRetryWait = time.Millisecond
	defer func() { jmPinRetryWait = old }()
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	d := writeBlocks(t, mem, 351, 2)
	ps := &pinStorage{ObjectStorage: mem, failFirst: true, failed: map[string]bool{}}
	store := pinStoreFor(t, ps, 64)
	if err := store.JMPinFill(context.Background(), 1, whole(351, d), JMPinAttr{}, 0, nil); err != nil {
		t.Fatalf("fill with one failed GET per block: %v", err)
	}
	if ps.full.Load() != 4 {
		t.Fatalf("whole GETs %d, want 2 failed + 2 retried", ps.full.Load())
	}
}

func TestJMPinEncryptedVolumeWholeObjects(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := object.NewDataEncryptor(object.NewRSAEncryptor(key), object.AES256GCM_RSA)
	if err != nil {
		t.Fatal(err)
	}
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	data := writeBlocks(t, object.NewEncrypted(mem, enc), 361, 3)
	ps := &pinStorage{ObjectStorage: mem}
	conf := pinConf(t, 64)
	conf.Encrypted = true
	store := NewCachedStore(object.NewEncrypted(ps, enc), conf, nil).(*cachedStore)
	if err := store.JMPinFill(context.Background(), 1, whole(361, data), JMPinAttr{}, 0, nil); err != nil {
		t.Fatal(err)
	}
	if ps.full.Load() != 3 || ps.ranged.Load() != 0 {
		t.Fatalf("GETs %d whole, %d ranged; want 3 whole", ps.full.Load(), ps.ranged.Load())
	}
	r := store.NewReader(361, len(data))
	if got := readAt(t, r, 2<<20+4096, 64<<10); !bytes.Equal(got, data[2<<20+4096:2<<20+4096+64<<10]) || ps.full.Load() != 3 {
		t.Fatal("pinned encrypted block read wrong or from the store")
	}
	// The cache file holds the decrypted block.
	k := sliceForRead(361, len(data), store).key(0)
	raw, err := os.ReadFile(store.bcache.(*cacheManager).getStore(k).cachePath(k))
	if err != nil || !bytes.Equal(raw[:1<<20], data[:1<<20]) {
		t.Fatalf("cache file: %v", err)
	}
}

func TestJMPinMemoryCacheUnsupported(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	conf := rangedConf(t, 1)
	store := NewCachedStore(mem, conf, nil).(*cachedStore)
	if err := store.JMPinFill(context.Background(), 1, []JMPinSlice{{ID: 1, Size: 10, Len: 10}}, JMPinAttr{}, 0, nil); !errors.Is(err, ErrJMPinUnsupported) {
		t.Fatalf("memory cache fill: %v", err)
	}
}

// C1: a cache dir that goes down (removeStore sets its slot to nil) must not
// panic the pin paths.
func TestJMPinSurvivesRemovedCacheStore(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	d := writeBlocks(t, mem, 371, 2)
	store := pinStoreFor(t, mem, 64)
	if err := store.JMPinFill(context.Background(), 1, whole(371, d), JMPinAttr{}, 0, nil); err != nil {
		t.Fatal(err)
	}
	m := store.bcache.(*cacheManager)
	m.removeStore(m.stores[0].id)
	if p, lim := store.JMPinUsage(); p != 0 || lim != 0 { // no live disk holds them
		t.Fatalf("usage with no live store: %d of %d", p, lim)
	}
	if known, _, missing, _, err := store.JMPinCheck(1); err != nil || !known || missing != int64(len(d)) {
		t.Fatalf("check with no live store: known %v missing %d err %v", known, missing, err)
	}
	if err := store.JMPinFill(context.Background(), 2, whole(371, d), JMPinAttr{}, 0, nil); err == nil {
		t.Fatal("a fill with no live store succeeded")
	}
	store.JMPinSetCapacity(1 << 30)
	store.JMPinDrop(1)
}

// C2: a fill never writes below the cache's free-space floor.
func TestJMPinRefusesBelowFreeSpaceFloor(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	d := writeBlocks(t, mem, 381, 2)
	ps := &pinStorage{ObjectStorage: mem}
	store := pinStoreFor(t, ps, 64)
	old := jmDiskRoom
	defer func() { jmDiskRoom = old }()
	jmDiskRoom = func(*cacheStore) (int64, int64) { return 1 << 20, 10 << 20 } // below the floor
	err := store.JMPinFill(context.Background(), 1, whole(381, d), JMPinAttr{}, 0, nil)
	if !errors.Is(err, ErrJMPinNoSpace) {
		t.Fatalf("fill at the floor: %v, want ErrJMPinNoSpace", err)
	}
	if ps.full.Load() != 0 {
		t.Fatal("a fill refused for disk space downloaded")
	}
	// Room for the pin set but not for one more block on the disk.
	jmDiskRoom = func(*cacheStore) (int64, int64) { return 100 << 20, 99 << 20 }
	if err := store.JMPinFill(context.Background(), 1, whole(381, d), JMPinAttr{}, 0, nil); !errors.Is(err, ErrJMPinNoSpace) || ps.full.Load() != 0 {
		t.Fatalf("block below the floor: %v, GETs %d", err, ps.full.Load())
	}
	jmDiskRoom = old
	// jmRoomFor itself: a store marked full refuses every block.
	ok := pinStoreFor(t, mem, 64)
	m := ok.bcache.(*cacheManager)
	k := sliceForRead(381, len(d), ok).key(0)
	if _, err := m.jmRoomFor(k, 1<<20); err != nil {
		t.Fatalf("room on a roomy disk: %v", err)
	}
	m.stores[0].rawFull.Store(true)
	if _, err := m.jmRoomFor(k, 1<<20); !errors.Is(err, ErrJMPinNoSpace) {
		t.Fatalf("room on a full store: %v", err)
	}
}

// H1: pins are persisted and loaded before the store scans or evicts, and
// CHECK needs no metadata.
func TestJMPinSurvivesRestart(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	d := writeBlocks(t, mem, 391, 4)
	other := writeBlocks(t, mem, 392, 12)
	conf := pinConf(t, 10)
	first := NewCachedStore(mem, conf, nil).(*cachedStore)
	attr := JMPinAttr{Length: uint64(len(d)), MtimeNS: 1234}
	if err := first.JMPinFill(context.Background(), 5, whole(391, d), attr, 0, nil); err != nil {
		t.Fatal(err)
	}
	keys := sliceForRead(391, len(d), first).keys()
	// The process restarts: a new store on the same cache dir.
	ps := &pinStorage{ObjectStorage: mem}
	second := NewCachedStore(ps, conf, nil).(*cachedStore)
	m := second.bcache.(*cacheManager)
	if m.stores[0].jmPins != m.pins || !m.pins.has(jmCacheKey(keys[0])) {
		t.Fatal("the store started without the persisted pins")
	}
	known, total, missing, got, err := second.JMPinCheck(5)
	if err != nil || !known || total != int64(len(d)) || missing != 0 || got != attr {
		t.Fatalf("check after restart: known %v total %d missing %d attr %+v err %v", known, total, missing, got, err)
	}
	if b := m.pins.storeBytes(m.stores[0].id); b != 4*(1<<20+4096) {
		t.Fatalf("restored pins not bound to their store: %d", b)
	}
	for _, k := range keys {
		m.stores[0].add(k, int32(parseObjOrigSize(k)), uint32(time.Now().Unix())-3600)
	}
	for _, k := range sliceForRead(392, len(other), second).keys() {
		p := NewOffPage(parseObjOrigSize(k))
		if err := second.load(k, p, false, false); err != nil {
			t.Fatal(err)
		}
		_ = m.jmCacheSync(k, p)
		p.Release()
	}
	for _, k := range keys {
		if !onDisk(second, k) {
			t.Fatalf("restored pinned block %s was evicted", k)
		}
	}
	// Dropping removes the persisted record too.
	second.JMPinDrop(5)
	third := NewCachedStore(mem, conf, nil).(*cachedStore)
	if known, _, _, _, _ := third.JMPinCheck(5); known {
		t.Fatal("a dropped pin came back after a restart")
	}
}

// M3: only blocks that hold visible data are pinned.
func TestJMPinOnlyVisibleBlocks(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	d := writeBlocks(t, mem, 401, 6)
	ps := &pinStorage{ObjectStorage: mem}
	store := pinStoreFor(t, ps, 64)
	// The file shows 1.5 MiB starting 2.5 MiB into the slice: blocks 2 and 3.
	sl := []JMPinSlice{{ID: 401, Size: uint32(len(d)), Off: 5 << 19, Len: 3 << 19}}
	var prog JMPinProgress
	if err := store.JMPinFill(context.Background(), 1, sl, JMPinAttr{}, 0, &prog); err != nil {
		t.Fatal(err)
	}
	if ps.full.Load() != 2 || prog.Total.Load() != 2<<20 {
		t.Fatalf("GETs %d total %d; want blocks 2 and 3 only", ps.full.Load(), prog.Total.Load())
	}
	rs := sliceForRead(401, len(d), store)
	if onDisk(store, rs.key(0)) || !onDisk(store, rs.key(2)) || !onDisk(store, rs.key(3)) || onDisk(store, rs.key(4)) {
		t.Fatal("wrong blocks pinned")
	}
}

// M1: VERIFY reports owners pinned from another version of the file.
func TestJMPinVerifyReasons(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	d := writeBlocks(t, mem, 411, 2)
	store := pinStoreFor(t, mem, 64)
	a := JMPinAttr{Length: uint64(len(d)), MtimeNS: 99}
	for _, o := range []uint64{1, 2, 3} {
		if err := store.JMPinFill(context.Background(), o, whole(411, d), a, 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	k := sliceForRead(411, len(d), store).key(1)
	m := store.bcache.(*cacheManager)
	res, err := store.JMPinVerify([]JMPinVerifyItem{{Owner: 1, Attr: a}, {Owner: 2, Attr: JMPinAttr{Length: a.Length, MtimeNS: 100}}, {Owner: 9, Attr: a}})
	if err != nil || len(res) != 2 || res[0].Owner != 2 || res[0].Reason != JMPinChanged || res[1].Owner != 9 || res[1].Reason != JMPinUnknown {
		t.Fatalf("verify: %+v %v", res, err)
	}
	_ = os.Remove(m.getStore(k).cachePath(k))
	res, _ = store.JMPinVerify([]JMPinVerifyItem{{Owner: 3, Attr: a}})
	if len(res) != 1 || res[0].Reason != JMPinIncomplete || res[0].Missing != 1<<20 {
		t.Fatalf("verify after a lost block: %+v", res)
	}
}

// H2: capacity can grow at run time, raising the pin cap.
func TestJMPinSetCapacityRaisesCap(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	a := writeBlocks(t, mem, 421, 12)
	store := pinStoreFor(t, mem, 10)
	if err := store.JMPinFill(context.Background(), 1, whole(421, a), JMPinAttr{}, 0, nil); !errors.Is(err, ErrJMPinFull) {
		t.Fatalf("12 MiB into a 10 MiB cache: %v", err)
	}
	if got := store.JMPinSetCapacity(20 << 20); got != 20<<20 {
		t.Fatalf("capacity %d", got)
	}
	if err := store.JMPinFill(context.Background(), 1, whole(421, a), JMPinAttr{}, 0, nil); err != nil {
		t.Fatalf("after growing the cache: %v", err)
	}
	if got := store.JMPinSetCapacity(1 << 20); got != 10<<20 {
		t.Fatalf("capacity fell below --cache-size: %d", got)
	}
}

// H5: a fill and a whole-block read of the same block share one download.
func TestJMPinSharesDownloadWithReads(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	d := writeBlocks(t, mem, 431, 1)
	g := &gatedStorage{ObjectStorage: mem, gate: make(chan struct{})}
	store := pinStoreFor(t, g, 64)
	done := make(chan error, 1)
	go func() {
		done <- store.JMPinFill(context.Background(), 1, whole(431, d), JMPinAttr{}, 0, nil)
	}()
	for g.full.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	key := sliceForRead(431, len(d), store).key(0)
	read := make(chan *Page, 1)
	go func() {
		p, _ := store.group.TryPiggyback(key)
		read <- p
	}()
	time.Sleep(20 * time.Millisecond)
	close(g.gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	p := <-read
	if p == nil || !bytes.Equal(p.Data, d) || g.full.Load() != 1 {
		t.Fatalf("read did not share the fill's download (GETs %d)", g.full.Load())
	}
	p.Release()
}

// loadBlocks reads each block of a slice and writes it to the disk cache,
// as a demand read with --cache-full-block does.
func loadBlocks(t *testing.T, store *cachedStore, id uint64, n int) []string {
	t.Helper()
	m := store.bcache.(*cacheManager)
	keys := sliceForRead(id, n<<20, store).keys()
	for _, k := range keys {
		p := NewOffPage(parseObjOrigSize(k))
		if err := store.load(k, p, false, false); err != nil {
			t.Fatal(err)
		}
		if err := m.jmCacheSync(k, p); err != nil {
			t.Fatal(err)
		}
		p.Release()
	}
	return keys
}

// Re-review HIGH-1: pinned keys that never reached the disk (a failed or
// canceled fill) do not stop eviction from freeing the rest of the cache.
func TestJMPinCleanupIgnoresMissingPinnedBlocks(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	other := writeBlocks(t, mem, 431, 16)
	store := pinStoreFor(t, mem, 10) // 10 MiB cache
	m := store.bcache.(*cacheManager)
	s := m.stores[0]
	// 12 MiB registered for owner 1, none of it downloaded.
	missing := sliceForRead(432, 12<<20, store).keys()
	ids := make([]string, len(missing))
	for i := range ids {
		ids[i] = s.id
	}
	if err := m.pins.replace(context.Background(), 1, missing, ids, JMPinAttr{}, "", map[string]jmLimit{s.id: {bytes: 1 << 40}}); err != nil {
		t.Fatal(err)
	}
	if m.pins.storeBytes(s.id) <= s.capacity {
		t.Fatal("setup: registered pins should exceed the cache")
	}
	loadBlocks(t, store, 431, len(other)>>20)
	s.Lock()
	used := s.used
	s.Unlock()
	if used > s.capacity+(1<<20+4096) {
		t.Fatalf("cache holds %d bytes for a %d byte capacity: registered but missing pins blocked eviction", used, s.capacity)
	}
	if pinned, _ := store.JMPinUsage(); pinned != 0 {
		t.Fatalf("pinned bytes on disk %d, want 0", pinned)
	}
}

// Re-review MEDIUM-1: at the free-space floor, a pinned block evicts
// unpinned cached blocks instead of failing the fill.
func TestJMPinRoomForEvictsUnpinnedAtFloor(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	other := writeBlocks(t, mem, 441, 8)
	d := writeBlocks(t, mem, 442, 2)
	store := pinStoreFor(t, mem, 64)
	m := store.bcache.(*cacheManager)
	s := m.stores[0]
	okeys := loadBlocks(t, store, 441, len(other)>>20)
	s.Lock()
	diskTotal := s.used + 4<<20 // free == floor now
	s.Unlock()
	old := jmDiskRoom
	defer func() { jmDiskRoom = old }()
	jmDiskRoom = func(c *cacheStore) (int64, int64) {
		c.Lock()
		defer c.Unlock()
		return diskTotal - c.used, 4 << 20
	}
	if err := store.JMPinFill(context.Background(), 1, whole(442, d), JMPinAttr{}, 0, nil); err != nil {
		t.Fatalf("fill at the floor with evictable cache: %v", err)
	}
	evicted := 0
	for _, k := range okeys {
		if !onDisk(store, k) {
			evicted++
		}
	}
	if evicted < 2 {
		t.Fatalf("%d unpinned blocks evicted, want at least 2", evicted)
	}
	// Only pinned blocks left to evict: the fill is refused.
	store.JMPinDrop(1)
	for _, k := range okeys {
		if onDisk(store, k) {
			_ = os.Remove(s.cachePath(k))
			s.Lock()
			delete(s.keys, jmCacheKey(k))
			s.Unlock()
		}
	}
	if err := store.JMPinFill(context.Background(), 2, whole(441, other), JMPinAttr{}, 0, nil); !errors.Is(err, ErrJMPinNoSpace) {
		t.Fatalf("fill with nothing evictable: %v, want ErrJMPinNoSpace", err)
	}
}

// Re-review MEDIUM-4: a canceled fill never overwrites the owner's keys.
func TestJMPinCanceledFillKeepsNewerKeys(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	a := writeBlocks(t, mem, 451, 1)
	b := writeBlocks(t, mem, 452, 2)
	store := pinStoreFor(t, mem, 64)
	m := store.bcache.(*cacheManager)
	if err := store.JMPinFill(context.Background(), 1, whole(452, b), JMPinAttr{Length: 2}, 0, nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.JMPinFill(ctx, 1, whole(451, a), JMPinAttr{Length: 1}, 0, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled fill: %v", err)
	}
	rec, _ := m.pins.record(1)
	if rec.Length != 2 || len(rec.Keys) != 2 {
		t.Fatalf("a canceled fill replaced the newer keys: %+v", rec)
	}
}

// Re-review MEDIUM-5: VERIFY with the file's current slices reports a file
// whose slices changed (compaction) with the same length and mtime.
func TestJMPinVerifyComparesSlices(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	d := writeBlocks(t, mem, 461, 1)
	store := pinStoreFor(t, mem, 64)
	a := JMPinAttr{Length: uint64(len(d)), MtimeNS: 7}
	sl := whole(461, d)
	if err := store.JMPinFill(context.Background(), 1, sl, a, 0, nil); err != nil {
		t.Fatal(err)
	}
	res, err := store.JMPinVerify([]JMPinVerifyItem{{Owner: 1, Attr: a}, {Owner: 1, Attr: a, Slices: sl}})
	if err != nil || len(res) != 0 {
		t.Fatalf("same slices: %+v %v", res, err)
	}
	compacted := []JMPinSlice{{ID: 462, Size: sl[0].Size, Len: sl[0].Len}}
	res, _ = store.JMPinVerify([]JMPinVerifyItem{{Owner: 1, Attr: a, Slices: compacted}})
	if len(res) != 1 || res[0].Reason != JMPinChanged {
		t.Fatalf("compacted slices: %+v", res)
	}
}

// Re-review LOW: a pin file with a key listed twice counts the block once,
// and dropping the owner unpins it.
func TestJMPinLoadDedupesKeys(t *testing.T) {
	conf := pinConf(t, 64)
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	first := NewCachedStore(mem, conf, nil).(*cachedStore)
	k := sliceForRead(471, 1<<20, first).key(0)
	dir := first.bcache.(*cacheManager).pins.dir
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/9.json", []byte(`{"keys":["`+k+`","`+k+`"],"length":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	second := NewCachedStore(mem, conf, nil).(*cachedStore)
	m := second.bcache.(*cacheManager)
	if got := m.pins.storeBytes(m.stores[0].id); got != 1<<20+4096 {
		t.Fatalf("pinned bytes %d, want one block", got)
	}
	second.JMPinDrop(9)
	if m.pins.has(jmCacheKey(k)) || m.pins.total() != 0 {
		t.Fatal("a duplicated key stayed pinned after the drop")
	}
}

// Re-review LOW: waitIdleCtx returns when the fill is canceled.
func TestJMPinWaitIdleReturnsOnCancel(t *testing.T) {
	d := newDemandFirst(true, 1<<20)
	d.deferMax = time.Hour
	end := d.beginDemand("other")
	defer end()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.waitIdleCtx(ctx, "mine"); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("waitIdleCtx did not return on cancel")
	}
}

// peakStorage records the most whole-object GETs in flight at once.
type peakStorage struct {
	object.ObjectStorage
	mu        sync.Mutex
	cur, peak int
}

func (s *peakStorage) Get(key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	s.mu.Lock()
	s.cur++
	s.peak = max(s.peak, s.cur)
	s.mu.Unlock()
	time.Sleep(30 * time.Millisecond)
	s.mu.Lock()
	s.cur--
	s.mu.Unlock()
	return s.ObjectStorage.Get(key, off, limit, getters...)
}

func (s *peakStorage) takePeak() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.peak
	s.peak = 0
	return p
}

// Re-review MEDIUM-3: one worker per fill (the app's slow or metered link)
// means one pin download at a time across all fills of the process.
func TestJMPinOneWorkerLimitsAllFills(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	a := writeBlocks(t, mem, 481, 3)
	b := writeBlocks(t, mem, 482, 3)
	c := writeBlocks(t, mem, 483, 4)
	ss := &peakStorage{ObjectStorage: mem}
	store := pinStoreFor(t, ss, 64)
	defer jmPinDownloads.setLimit(jmPinMaxDownloads)
	var wg sync.WaitGroup
	for i, d := range [][]byte{a, b} {
		wg.Add(1)
		go func(owner uint64, id uint64, d []byte) {
			defer wg.Done()
			if err := store.JMPinFill(context.Background(), owner, whole(id, d), JMPinAttr{}, 1, nil); err != nil {
				t.Error(err)
			}
		}(uint64(i+1), uint64(481+i), d)
	}
	wg.Wait()
	if p := ss.takePeak(); p != 1 {
		t.Fatalf("two one-worker fills downloaded %d blocks at once, want 1", p)
	}
	if err := store.JMPinFill(context.Background(), 3, whole(483, c), JMPinAttr{}, 4, nil); err != nil {
		t.Fatal(err)
	}
	if p := ss.takePeak(); p < 2 {
		t.Fatalf("a four-worker fill downloaded %d at a time", p)
	}
}

// PR #90 review MEDIUM-2: with demand-first on, only a one-worker fill (the
// app's slow or metered link) waits for demand reads of other blocks. A
// four-worker fill (lan, fast) does not wait while unpinned playback runs.
func TestJMPinWaitsForDemandReadsOnlyWithOneWorker(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	wide := writeBlocks(t, mem, 491, 2)
	narrow := writeBlocks(t, mem, 492, 2)
	conf := pinConf(t, 64)
	conf.DemandFirst = true
	store := NewCachedStore(mem, conf, nil).(*cachedStore)
	defer jmPinDownloads.setLimit(jmPinMaxDownloads)
	store.demand.deferMax = time.Hour
	end := store.demand.beginDemand("a block an editor plays")
	defer end()

	done := make(chan error, 1)
	go func() { done <- store.JMPinFill(context.Background(), 1, whole(491, wide), JMPinAttr{}, 4, nil) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a four-worker fill waited for a demand read of another block")
	}

	go func() { done <- store.JMPinFill(context.Background(), 2, whole(492, narrow), JMPinAttr{}, 1, nil) }()
	select {
	case <-done:
		t.Fatal("a one-worker fill did not wait for the demand read")
	case <-time.After(300 * time.Millisecond):
	}
	end()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the one-worker fill did not resume after the demand read ended")
	}
}

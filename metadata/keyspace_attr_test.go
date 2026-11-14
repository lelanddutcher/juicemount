package metadata

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestParseFileInodeFromChannel(t *testing.T) {
	const prefix = "__keyspace@1__:"
	for _, tc := range []struct {
		ch   string
		want uint64
		ok   bool
	}{
		{prefix + "i42", 42, true},
		{prefix + "i", 0, false},
		{prefix + "d42", 0, false},
		{prefix + "iabc", 0, false},
		{"__keyspace@2__:i42", 0, false},
	} {
		got, ok := parseFileInodeFromChannel(tc.ch, prefix)
		if got != tc.want || ok != tc.ok {
			t.Errorf("%q = %d, %v; want %d, %v", tc.ch, got, ok, tc.want, tc.ok)
		}
	}
}

func TestKeyspaceAttrNotifySufficient(t *testing.T) {
	for flags, want := range map[string]bool{
		"KEA": true, "Kghx$": true, "K$": true, "Kghx": false, "E$": false, "": false,
	} {
		if got := keyspaceAttrNotifySufficient(flags); got != want {
			t.Errorf("%q = %v, want %v", flags, got, want)
		}
	}
}

// A peer's append or overwrite changes only the file's i-key. The attr push
// must refresh size and mtime in the mirror and fire the scan content hook
// (which the NFS layer uses to reopen its pooled read descriptor), once per
// real change, and never for a file with a pending local write.
func TestReconcileFileAttrsRefreshesChangedFile(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	value := make([]byte, 9)
	value[0] = jfsTypeFile
	binary.BigEndian.PutUint32(value[5:], 42)
	var mu sync.Mutex
	attr := encodeTestAttr(1700000000, 4096)
	var reads [][]string
	rc := &RedisClient{
		store: store, stopCh: make(chan struct{}),
		testReadDirRaw: func(uint64) (map[string]string, error) {
			return map[string]string{"media.mov": string(value)}, nil
		},
		testReadAttrs: func(keys []string) ([]interface{}, error) {
			mu.Lock()
			defer mu.Unlock()
			reads = append(reads, keys)
			out := make([]interface{}, len(keys))
			for i := range keys {
				out[i] = string(attr)
			}
			return out, nil
		},
	}
	defer close(rc.stopCh)
	if err := rc.reconcileDir(1); err != nil {
		t.Fatal(err)
	}
	var fired []string
	store.SetOnScanContentInvalidated(func(p string, _ uint64) { fired = append(fired, p) })
	gen, _, _ := rc.backendAdmission()

	// Unchanged attr (our own write's echo): one MGET, no upsert, no hook.
	reads = nil
	if retry, err := rc.reconcileFileAttrs([]uint64{42, 7}, gen); err != nil || retry != nil {
		t.Fatalf("unchanged: retry %v, err %v", retry, err)
	}
	if len(fired) != 0 || len(reads) != 1 || len(reads[0]) != 1 || reads[0][0] != "i42" {
		t.Fatalf("unchanged: fired %v, reads %v (unknown inode 7 must cost nothing)", fired, reads)
	}

	// The peer appended: size and mtime move, the hook fires once.
	mu.Lock()
	attr = encodeTestAttr(1700000100, 8192)
	mu.Unlock()
	if retry, err := rc.reconcileFileAttrs([]uint64{42}, gen); err != nil || retry != nil {
		t.Fatalf("changed: retry %v, err %v", retry, err)
	}
	got := store.LookupByPath("media.mov")
	if got == nil || got.Size != 8192 || !got.Mtime.Equal(time.Unix(1700000100, 0)) || got.Inode != 42 {
		t.Fatalf("mirror row = %+v", got)
	}
	if len(fired) != 1 || fired[0] != "media.mov" {
		t.Fatalf("hook fired %v, want [media.mov]", fired)
	}

	// A pending local write: this client is the authority; no read at all.
	rc.SetSpoolGuard(func(p string) bool { return p == "media.mov" })
	mu.Lock()
	attr = encodeTestAttr(1700000200, 1)
	mu.Unlock()
	reads = nil
	if _, err := rc.reconcileFileAttrs([]uint64{42}, gen); err != nil {
		t.Fatal(err)
	}
	if len(reads) != 0 || store.LookupByPath("media.mov").Size != 8192 {
		t.Fatalf("spool-pending file was refreshed: reads %v", reads)
	}
}

// The attr coalescer reuses the folder coalescer's debounce but applies its
// own batch, once per window, with the attr window.
func TestAttrCoalescerBatches(t *testing.T) {
	rc := &RedisClient{stopCh: make(chan struct{})}
	defer close(rc.stopCh)
	c := newInodeCoalescer(rc)
	c.tuningFn = func() coalescerTuning {
		return coalescerTuning{debounce: 20 * time.Millisecond, maxWait: time.Second, burstCeiling: 1}
	}
	got := make(chan []uint64, 4)
	c.apply = func(b []uint64) { got <- b }
	defer c.stop()
	for _, ino := range []uint64{5, 6, 5} {
		c.add(ino)
	}
	select {
	case b := <-got:
		if len(b) != 2 {
			t.Fatalf("batch %v, want 2 distinct inodes (burstCeiling must not promote)", b)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no flush")
	}
}

// Review MEDIUM-1: an inode whose attr read keeps failing must stop after
// attrMaxRetries retries, with backoff. The old code cleared the count for
// the whole batch, so the retry (alone in its own batch) never counted and
// the inode retried about 10 times per second forever.
func TestAttrRetryHonoursLimit(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	value := make([]byte, 9)
	value[0] = jfsTypeFile
	binary.BigEndian.PutUint32(value[5:], 42)
	var mu sync.Mutex
	failing := false
	mgets := 0
	rc := &RedisClient{
		store: store, stopCh: make(chan struct{}),
		testReadDirRaw: func(uint64) (map[string]string, error) {
			return map[string]string{"media.mov": string(value)}, nil
		},
		testReadAttrs: func(keys []string) ([]interface{}, error) {
			mu.Lock()
			defer mu.Unlock()
			if failing {
				mgets++
				return nil, errors.New("injected MGET failure")
			}
			out := make([]interface{}, len(keys))
			for i := range keys {
				out[i] = string(encodeTestAttr(1700000000, 4096))
			}
			return out, nil
		},
	}
	defer close(rc.stopCh)
	if err := rc.reconcileDir(1); err != nil {
		t.Fatal(err)
	}
	saved := attrRetryBase
	attrRetryBase = 10 * time.Millisecond
	defer func() { attrRetryBase = saved }()
	gen, _, _ := rc.backendAdmission()
	c := rc.newAttrCoalescer(gen)
	c.tuningFn = func() coalescerTuning {
		return coalescerTuning{debounce: 2 * time.Millisecond, maxWait: 20 * time.Millisecond, burstCeiling: attrBurstCeiling}
	}
	defer c.stop()
	mu.Lock()
	failing = true
	mu.Unlock()
	c.add(42)
	// Retries wait 10, 20 and 40 ms; well past them nothing more may come.
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	got := mgets
	mu.Unlock()
	if got != 1+attrMaxRetries {
		t.Fatalf("MGETs for a permanently failing inode = %d, want %d (1 + %d retries)", got, 1+attrMaxRetries, attrMaxRetries)
	}
	// A new event for the inode starts a fresh count; a success then ends it.
	mu.Lock()
	failing = false
	mu.Unlock()
	c.add(42)
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	failing = true
	mgets = 0
	mu.Unlock()
	c.add(42)
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	got = mgets
	mu.Unlock()
	if got != 1+attrMaxRetries {
		t.Fatalf("after a success, MGETs = %d, want %d", got, 1+attrMaxRetries)
	}
}

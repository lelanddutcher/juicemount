package metadata

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testTime(sec int64) time.Time { return time.Unix(1700000000+sec, 0) }

// Laptop B1: while the NFS layer's in-place WRITE of a file is still syncing
// abroad, the attr push must not publish that file (the WRITE's completion
// CAS would fail). It defers the inode, without an MGET or a retry-budget
// failure, and refreshes it once the owner is gone.
func TestReconcileFileAttrsDefersForegroundOwnedFile(t *testing.T) {
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
	reads := 0
	rc := &RedisClient{
		store: store, stopCh: make(chan struct{}),
		testReadDirRaw: func(uint64) (map[string]string, error) {
			return map[string]string{"b0.prproj": string(value)}, nil
		},
		testReadAttrs: func(keys []string) ([]interface{}, error) {
			mu.Lock()
			defer mu.Unlock()
			reads++
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
	gen, _, _ := rc.backendAdmission()
	before := store.LookupByPath("b0.prproj").Version

	var owner sync.Mutex
	owned := true
	store.SetForegroundOwner(func(p string) bool {
		owner.Lock()
		defer owner.Unlock()
		return owned && p == "b0.prproj"
	})
	mu.Lock()
	attr = encodeTestAttr(1700000100, 8192) // the WRITE's own echo
	reads = 0
	mu.Unlock()
	retry, deferred, err := rc.reconcileFileAttrsOwned([]uint64{42}, gen)
	if err != nil || len(retry) != 0 || len(deferred) != 1 || deferred[0] != 42 {
		t.Fatalf("owned: retry %v, deferred %v, err %v; want deferred [42]", retry, deferred, err)
	}
	if reads != 0 || store.LookupByPath("b0.prproj").Version != before {
		t.Fatalf("owned file was read (%d MGETs) or republished", reads)
	}

	// The owner finished: the deferred inode refreshes normally.
	owner.Lock()
	owned = false
	owner.Unlock()
	retry, deferred, err = rc.reconcileFileAttrsOwned([]uint64{42}, gen)
	if err != nil || len(retry) != 0 || len(deferred) != 0 {
		t.Fatalf("released: retry %v, deferred %v, err %v", retry, deferred, err)
	}
	if got := store.LookupByPath("b0.prproj"); got.Size != 8192 {
		t.Fatalf("after release size = %d, want 8192", got.Size)
	}
}

// The Store's own check is the atomic one: a row the owner holds is skipped
// by the guarded batch even when the caller's pre-check missed it, and the
// pub/sub feed writes nothing.
func TestStoreBackgroundPublicationSkipsForegroundOwned(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Insert(MakeEntry("w/b0.prproj", false, 4096, testTime(1), 42)); err != nil {
		t.Fatal(err)
	}
	cur := store.LookupByPath("w/b0.prproj")
	store.SetForegroundOwner(func(p string) bool { return p == "w/b0.prproj" })

	echo := MakeEntry("w/b0.prproj", false, 8192, testTime(2), 42)
	epoch, err := store.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	_, owned, err := store.ReconcileUpsertBatchOwned([]*Entry{echo, MakeEntry("w/c0.prproj", false, 1, testTime(2), 43)}, epoch)
	if err != nil || len(owned) != 1 || owned[0] != "w/b0.prproj" {
		t.Fatalf("batch: owned %v, err %v", owned, err)
	}
	if store.LookupByPath("w/c0.prproj") == nil {
		t.Fatal("an unowned sibling in the same batch was not published")
	}
	if applied, err := store.InsertFromFeed(MakeEntry("w/b0.prproj", false, 8192, testTime(3), 42)); err != nil || applied {
		t.Fatalf("feed: applied %v, err %v", applied, err)
	}
	if got := store.LookupByPath("w/b0.prproj"); got.Version != cur.Version || got.Size != 4096 {
		t.Fatalf("owned row changed: %+v", got)
	}
	if err := store.DeletePathsConditional([]*Entry{cur}); !errors.Is(err, ErrForegroundOwned) || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("prune = %v, want ErrForegroundOwned", err)
	}
	store.SetForegroundOwner(nil)
	if applied, err := store.InsertFromFeed(MakeEntry("w/b0.prproj", false, 8192, testTime(3), 42)); err != nil || !applied {
		t.Fatalf("unowned feed: applied %v, err %v", applied, err)
	}
}

//go:build h8aacceptance

package metadata

import (
	"context"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func TestH8AStaleScanUpsertDoesNotInvalidateNewerWinner(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Insert(MakeEntry("media.mov", false, 4096, time.Unix(1700000000, 0), 42)); err != nil {
		t.Fatal(err)
	}
	var callbacks []uint64
	store.SetOnContentInvalidated(func(path string, isDir bool) {
		if path == "media.mov" && !isDir {
			if old := store.LookupByPath(path); old != nil {
				callbacks = append(callbacks, old.Inode)
			}
		}
	})
	store.SetOnScanContentInvalidated(func(path string, oldInode uint64) {
		if path == "media.mov" {
			callbacks = append(callbacks, oldInode)
		}
	})

	rc := &RedisClient{store: store, stopCh: make(chan struct{})}
	rc.testScanBatch = func(_ context.Context, cursor, _ string) ([]string, error) {
		if cursor != "0" {
			t.Fatalf("unexpected SCAN cursor %q", cursor)
		}
		return []string{"0", "1:1:1700000100:8192:43:1:media.mov"}, nil
	}
	rc.testAfterScanTokenCheck = func() {
		if err := store.Insert(MakeEntry("media.mov", false, 16384, time.Unix(1700000200, 0), 44)); err != nil {
			t.Errorf("insert foreground winner: %v", err)
		}
	}
	if err := rc.SyncOnce(); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if len(callbacks) != 0 {
		t.Fatalf("stale SCAN upsert invalidated content for observed inodes %v; want no callback", callbacks)
	}
	if got := store.LookupByPath("media.mov"); got == nil || got.Inode != 44 || got.Size != 16384 {
		t.Fatalf("stale SCAN upsert evicted foreground winner: %+v", got)
	}
}

func TestH8AStaleScanPruneDoesNotInvalidateNewerWinner(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Insert(MakeEntry("media.mov", false, 4096, time.Unix(1700000000, 0), 42)); err != nil {
		t.Fatal(err)
	}
	var callbacks []uint64
	store.SetOnContentInvalidated(func(path string, isDir bool) {
		if path == "media.mov" && !isDir {
			if old := store.LookupByPath(path); old != nil {
				callbacks = append(callbacks, old.Inode)
			}
		}
	})
	store.SetOnScanContentInvalidated(func(path string, oldInode uint64) {
		if path == "media.mov" {
			callbacks = append(callbacks, oldInode)
		}
	})
	rc := &RedisClient{
		store:       store,
		stopCh:      make(chan struct{}),
		pruneAbsent: map[string]int{"media.mov": PruneThreshold - 1},
	}
	rc.mu.Lock()
	rc.connected = true
	rc.mu.Unlock()
	rc.testScanBatch = func(_ context.Context, cursor, _ string) ([]string, error) {
		if cursor != "0" {
			t.Fatalf("unexpected SCAN cursor %q", cursor)
		}
		return []string{"0"}, nil
	}
	rc.testAfterScanTokenCheck = func() {
		if err := store.Insert(MakeEntry("media.mov", false, 16384, time.Unix(1700000200, 0), 44)); err != nil {
			t.Errorf("insert foreground winner: %v", err)
		}
	}
	if err := rc.SyncOnce(); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if got := store.LookupByPath("media.mov"); got == nil || got.Inode != 44 || got.Size != 16384 {
		t.Fatalf("stale SCAN prune evicted foreground winner: %+v", got)
	}
	if len(callbacks) != 0 {
		t.Fatalf("stale SCAN prune invalidated content for observed inodes %v; want no callback", callbacks)
	}
}

func TestH8AAdmittedScanReplacementInvalidatesExactOldInode(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Insert(MakeEntry("media.mov", false, 4096, time.Unix(1700000000, 0), 42)); err != nil {
		t.Fatal(err)
	}
	var observed []uint64
	store.SetOnContentInvalidated(func(path string, isDir bool) {
		if path == "media.mov" && !isDir {
			if old := store.LookupByPath(path); old != nil {
				observed = append(observed, old.Inode)
			}
		}
	})
	store.SetOnScanContentInvalidated(func(path string, oldInode uint64) {
		if path == "media.mov" {
			observed = append(observed, oldInode)
		}
	})
	rc := &RedisClient{store: store, stopCh: make(chan struct{})}
	rc.testScanBatch = func(_ context.Context, cursor, _ string) ([]string, error) {
		if cursor != "0" {
			t.Fatalf("unexpected SCAN cursor %q", cursor)
		}
		return []string{"0", "1:1:1700000100:8192:43:1:media.mov"}, nil
	}
	if err := rc.SyncOnce(); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if len(observed) != 1 || observed[0] != 42 {
		t.Fatalf("admitted SCAN replacement callback observed inodes %v; want exactly [42]", observed)
	}
	if got := store.LookupByPath("media.mov"); got == nil || got.Inode != 43 || got.Size != 8192 {
		t.Fatalf("admitted SCAN replacement missing: %+v", got)
	}
}

func TestH8AScanInvalidatesCacheOverlayAndDurablePreimages(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stamp := time.Unix(1700000000, 0)
	if err := store.Insert(MakeEntry("media.mov", false, 4096, stamp, 42)); err != nil {
		t.Fatal(err)
	}
	store.InsertToCache(MakeEntry("media.mov", false, 8192, stamp.Add(time.Second), 44))
	epoch, err := store.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	var observed []uint64
	store.SetOnScanContentInvalidated(func(path string, oldInode uint64) {
		if path == "media.mov" {
			observed = append(observed, oldInode)
		}
	})
	next := MakeEntry("media.mov", false, 16384, stamp.Add(2*time.Second), 43)
	if _, err := store.ScanUpsertBatch([]*Entry{next}, epoch, []string{"media.mov"}); err != nil {
		t.Fatal(err)
	}
	sort.Slice(observed, func(i, j int) bool { return observed[i] < observed[j] })
	if len(observed) != 2 || observed[0] != 42 || observed[1] != 44 {
		t.Fatalf("invalidated preimage inodes %v, want durable 42 and served cache 44", observed)
	}
	if got := store.LookupByPath("media.mov"); got == nil || got.Inode != 43 {
		t.Fatalf("SCAN replacement missing: %+v", got)
	}
}

func TestH8AScanPruneInvalidatesCacheOverlayAndDurablePreimages(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stamp := time.Unix(1700000000, 0)
	if err := store.Insert(MakeEntry("media.mov", false, 4096, stamp, 42)); err != nil {
		t.Fatal(err)
	}
	store.InsertToCache(MakeEntry("media.mov", false, 8192, stamp.Add(time.Second), 44))
	epoch, err := store.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	var observed []uint64
	store.SetOnScanContentInvalidated(func(path string, oldInode uint64) {
		if path == "media.mov" {
			observed = append(observed, oldInode)
		}
	})
	if _, err := store.ScanDeletePathsBatch([]string{"media.mov"}, epoch); err != nil {
		t.Fatal(err)
	}
	sort.Slice(observed, func(i, j int) bool { return observed[i] < observed[j] })
	if len(observed) != 2 || observed[0] != 42 || observed[1] != 44 {
		t.Fatalf("invalidated prune preimage inodes %v, want durable 42 and served cache 44", observed)
	}
	if got := store.LookupByPath("media.mov"); got != nil {
		t.Fatalf("SCAN prune retained row: %+v", got)
	}
}

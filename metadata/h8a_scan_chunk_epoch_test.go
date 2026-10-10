//go:build h8aacceptance

package metadata

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestH8AScanRejectsForegroundInsertBetweenUpsertChunks(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rc := &RedisClient{store: store, stopCh: make(chan struct{})}
	raws := []string{"0"}
	for i := 0; i < 501; i++ {
		raws = append(raws, fmt.Sprintf("1:1:1700000000:4096:%d:1:file-%03d.mov", i+100, i))
	}
	rc.testScanBatch = func(_ context.Context, cursor, _ string) ([]string, error) {
		if cursor != "0" {
			t.Fatalf("unexpected SCAN cursor %q", cursor)
		}
		return raws, nil
	}
	chunks := 0
	rc.testAfterScanUpsertChunk = func() {
		chunks++
		if chunks == 1 {
			e := MakeEntry("file-500.mov", false, 8192, time.Unix(1700000100, 0), 999)
			if err := store.Insert(e); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := rc.SyncOnce(); err != nil {
		t.Fatal(err)
	}
	if chunks != 1 {
		t.Fatalf("committed %d SCAN chunks, want only the first", chunks)
	}
	if got := store.LookupByPath("file-000.mov"); got == nil || got.Inode != 100 {
		t.Fatalf("first committed SCAN chunk missing: %+v", got)
	}
	if got := store.LookupByPath("file-500.mov"); got == nil || got.Inode != 999 || got.Size != 8192 {
		t.Fatalf("foreground row clobbered by later SCAN chunk: %+v", got)
	}
	if !rc.baselineIncomplete() {
		t.Fatal("conflicted SCAN certified a complete baseline")
	}
}

func TestH8AScanBatchRejectsCacheOnlyPublication(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	epoch, err := store.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	store.InsertToCache(MakeEntry("media.mov", false, 8192, time.Unix(1700000100, 0), 43))
	stale := MakeEntry("media.mov", false, 4096, time.Unix(1700000000, 0), 42)
	if _, err := store.ScanUpsertBatch([]*Entry{stale}, epoch, nil); err != ErrMirrorConflict {
		t.Fatalf("stale SCAN cache-only conflict = %v, want ErrMirrorConflict", err)
	}
	if got := store.LookupByPath("media.mov"); got == nil || got.Inode != 43 {
		t.Fatalf("cache-only foreground row clobbered: %+v", got)
	}
}

func TestH8AScanInvalidatesOldContentBeforeUpsert(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Insert(MakeEntry("media.mov", false, 4096, time.Unix(1700000000, 0), 42)); err != nil {
		t.Fatal(err)
	}
	var observed uint64
	store.SetOnScanContentInvalidated(func(p string, oldInode uint64) {
		if p == "media.mov" {
			observed = oldInode
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
		t.Fatal(err)
	}
	if observed != 42 {
		t.Fatalf("content invalidation observed inode %d, want previous inode 42", observed)
	}
	if got := store.LookupByPath("media.mov"); got == nil || got.Inode != 43 {
		t.Fatalf("SCAN replacement missing after invalidation: %+v", got)
	}
}

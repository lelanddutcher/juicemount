//go:build h8aacceptance

package metadata

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestH8ANoopScanDoesNotCertifyAfterForegroundStoreMutation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	observed := MakeEntry("stable.mov", false, 4096, time.Unix(1700000000, 0), 42)
	if err := store.Insert(observed); err != nil {
		t.Fatal(err)
	}
	before, err := store.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}

	rc := &RedisClient{store: store, stopCh: make(chan struct{})}
	rc.testScanBatch = func(_ context.Context, cursor, _ string) ([]string, error) {
		if cursor != "0" {
			t.Fatalf("unexpected SCAN cursor %q", cursor)
		}
		return []string{"0", "1:1:1700000000:4096:42:1:stable.mov"}, nil
	}
	rc.testAfterScanTokenCheck = func() {
		// The Redis observation is complete and already matches the mirror.
		// This unrelated foreground write changes the Store epoch without
		// creating any SCAN upsert or prune operation.
		if err := store.Insert(MakeEntry("foreground.mov", false, 8192, time.Unix(1700000100, 0), 43)); err != nil {
			t.Errorf("insert foreground row after SCAN observation: %v", err)
		}
	}

	if err := rc.SyncOnce(); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	after, err := store.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatalf("foreground mutation did not change Store epoch: before=%+v after=%+v", before, after)
	}
	if got := store.LookupByPath("foreground.mov"); got == nil || got.Inode != 43 {
		t.Fatalf("foreground row did not survive: %+v", got)
	}

	rc.mu.RLock()
	completedAt := rc.lastSyncTime
	rc.mu.RUnlock()
	if !completedAt.IsZero() {
		t.Fatalf("RED: no-op SCAN certified at %s after Store epoch changed from %+v to %+v", completedAt, before, after)
	}
}

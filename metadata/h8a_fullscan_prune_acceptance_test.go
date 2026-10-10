//go:build h8aacceptance

package metadata

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestH8AFullScanPrunePreservesPostObservationReplacement(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	old := MakeEntry("media.mov", false, 4096, time.Unix(1700000000, 0), 42)
	if err := store.Insert(old); err != nil {
		t.Fatal(err)
	}

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
		return []string{"0"}, nil // complete authoritative SCAN: media.mov absent
	}

	replaced := false
	rc.testAfterScanTokenCheck = func() {
		replacement := MakeEntry("media.mov", false, 8192, time.Unix(1700000100, 0), 43)
		if err := store.Insert(replacement); err != nil {
			t.Errorf("insert post-observation replacement: %v", err)
			return
		}
		replaced = true
	}
	if err := rc.SyncOnce(); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if !replaced {
		t.Fatal("replacement hook did not run after completed SCAN absence observation")
	}

	ram := store.LookupByPath("media.mov")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	durable := store.LookupByPath("media.mov")
	if ram == nil || ram.Inode != 43 || ram.Size != 8192 || durable == nil || durable.Inode != 43 || durable.Size != 8192 {
		t.Fatalf("full-SCAN prune deleted replacement published after absence observation: RAM=%+v reopened_SQL=%+v, want inode=43 size=8192 in both", ram, durable)
	}
}

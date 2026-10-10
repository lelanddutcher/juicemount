//go:build h8aacceptance

package metadata

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestH8AForegroundInsertSurvivesObservedScanCommit(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	rc := &RedisClient{store: store, stopCh: make(chan struct{})}

	checked := make(chan struct{})
	release := make(chan struct{})
	rc.testScanBatch = func(_ context.Context, cursor, _ string) ([]string, error) {
		if cursor != "0" {
			t.Fatalf("unexpected SCAN cursor %q", cursor)
		}
		return []string{"0", "1:1:1700000000:4096:42:1:media.mov"}, nil
	}
	rc.testAfterScanTokenCheck = func() {
		close(checked)
		<-release
	}

	scanDone := make(chan error, 1)
	go func() { scanDone <- rc.SyncOnce() }()
	select {
	case <-checked:
	case <-time.After(3 * time.Second):
		close(release)
		_ = store.Close()
		t.Fatal("SCAN did not reach the post-token-check pause")
	}

	newer := MakeEntry("media.mov", false, 8192, time.Unix(1700000100, 0), 43)
	if err := store.Insert(newer); err != nil {
		close(release)
		_ = store.Close()
		t.Fatal(err)
	}
	close(release)
	if err := <-scanDone; err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if !rc.baselineIncomplete() {
		_ = store.Close()
		t.Fatal("conflicted SCAN certified a complete baseline")
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
	sql := store.LookupByPath("media.mov")
	if ram == nil || ram.Inode != 43 || ram.Size != 8192 || sql == nil || sql.Inode != 43 || sql.Size != 8192 {
		t.Fatalf("stale observed SCAN clobbered foreground row: RAM=%+v reopened_SQL=%+v", ram, sql)
	}
}

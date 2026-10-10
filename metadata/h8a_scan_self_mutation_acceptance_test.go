//go:build h8aacceptance

package metadata

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

func h8aSelfMutationClient(t *testing.T, store *Store) *RedisClient {
	t.Helper()
	SetClassSignals(nil, func() bool { return true })
	t.Cleanup(func() { SetClassSignals(nil, nil) })
	pin.ResetFUSEIdentityForTest()
	t.Cleanup(pin.ResetFUSEIdentityForTest)
	rc := &RedisClient{store: store, stopCh: make(chan struct{})}
	rc.mu.Lock()
	rc.connected = true
	rc.mu.Unlock()
	return rc
}

func h8aCompleteEmptyScan(rc *RedisClient) {
	rc.testScanBatch = func(_ context.Context, cursor, _ string) ([]string, error) {
		if cursor != "0" {
			return nil, nil
		}
		return []string{"0"}, nil
	}
}

func h8aAssertBaselineActivated(t *testing.T, rc *RedisClient) {
	t.Helper()
	if rc.baselineIncomplete() {
		t.Fatal("complete SCAN did not clear incomplete-baseline debt")
	}
	if _, ok, err := rc.store.GetMeta(metaKeyLastSyncTime); err != nil || !ok {
		t.Fatalf("complete SCAN did not publish last-sync time: present=%v err=%v", ok, err)
	}
}

func TestH8AScanLocalOnlyClearDoesNotInvalidateItsOwnBaseline(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	local := MakeEntry("media.mov", false, 4096, time.Unix(1700000000, 0), 42)
	local.LocalOnly = true
	if err := store.Insert(local); err != nil {
		t.Fatal(err)
	}
	rc := h8aSelfMutationClient(t, store)
	rc.testScanBatch = func(_ context.Context, cursor, _ string) ([]string, error) {
		if cursor != "0" {
			return nil, nil
		}
		return []string{"0", "1:1:1700000000:4096:42:1:media.mov"}, nil
	}

	if err := rc.SyncOnce(); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	got := store.LookupByPath("media.mov")
	if got == nil || got.LocalOnly {
		t.Fatalf("SCAN did not clear local_only on observed row: %+v", got)
	}
	h8aAssertBaselineActivated(t, rc)
}

func TestH8AScanPruneDoesNotInvalidateItsOwnBaseline(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	stale := MakeEntry("gone.mov", false, 4096, time.Unix(1700000000, 0), 43)
	if err := store.Insert(stale); err != nil {
		t.Fatal(err)
	}
	rc := h8aSelfMutationClient(t, store)
	rc.pruneAbsent = map[string]int{"gone.mov": PruneThreshold - 1}
	h8aCompleteEmptyScan(rc)

	if err := rc.SyncOnce(); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if got := store.LookupByPath("gone.mov"); got != nil {
		t.Fatalf("threshold-qualified SCAN prune did not delete stale row: %+v", got)
	}
	h8aAssertBaselineActivated(t, rc)
}

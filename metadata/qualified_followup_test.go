package metadata

import (
	"context"
	"testing"
	"time"
)

// Boot-offline recovery (laptop lab 2026-11-11, after-8): the reachability
// monitor fired TriggerSync the moment the backend answered, before the cache
// session re-qualified it. That reconcile was refused ("metadata backend is not
// qualified") and the failure backoff put the next one 60 s out, so the volume
// stayed empty for a minute after the backend came back. Qualification must
// replay the refused reconcile at once, through the backoff floor.
func TestQualificationReplaysRefusedReconcile(t *testing.T) {
	farLink(t) // non-zero flap debounce: a plain flap trigger would be suppressed
	s := openTestStore(t)
	rc, err := NewRedisClientDeferred("redis://127.0.0.1:1/1", s)
	if err != nil {
		t.Fatal(err)
	}
	rc.SetBackendQualified(false)
	rc.testScanBatch = func(_ context.Context, _, _ string) ([]string, error) {
		return []string{"0", "1:1:1700000100:8192:43:1:foreign.mov"}, nil
	}
	done := make(chan struct{})
	go func() { defer close(done); rc.reconcileLoop() }()
	t.Cleanup(func() { close(rc.stopCh); <-done })

	// Backend answers; the network-change reconcile is refused locally.
	rc.TriggerSync()
	waitForQualified(t, 3*time.Second, "refused reconcile recorded", rc.unqualifiedSyncSkipped.Load)
	if s.LookupByPath("foreign.mov") != nil {
		t.Fatal("an unqualified backend published entries")
	}

	// The cache session qualifies the backend: the refused reconcile replays.
	start := time.Now()
	rc.SetBackendQualified(true)
	waitForQualified(t, 5*time.Second, "mirror refreshed after qualification", func() bool {
		return s.LookupByPath("foreign.mov") != nil
	})
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("refresh took %v; the failure backoff (60 s) must not apply", took)
	}
	if rc.unqualifiedSyncSkipped.Load() || rc.qualifiedFollowUp.Load() {
		t.Fatal("follow-up flags must be consumed after the replay")
	}
}

// A normal online boot qualifies without any refused reconcile; it must not
// pay an extra full SCAN for the follow-up.
func TestQualificationWithoutRefusalSchedulesNothing(t *testing.T) {
	s := openTestStore(t)
	rc, err := NewRedisClientDeferred("redis://127.0.0.1:1/1", s)
	if err != nil {
		t.Fatal(err)
	}
	rc.SetBackendQualified(false)
	rc.SetBackendQualified(true)
	if rc.qualifiedFollowUp.Load() {
		t.Fatal("qualification without a refused reconcile scheduled a follow-up")
	}
	select {
	case <-rc.syncNowCh:
		t.Fatal("qualification without a refused reconcile triggered a sync")
	default:
	}
}

func waitForQualified(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for: %s", limit, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStoreHasEntries(t *testing.T) {
	s := openTestStore(t)
	if has, err := s.HasEntries(); err != nil || has {
		t.Fatalf("empty store HasEntries = %v, %v; want false", has, err)
	}
	if err := s.BulkInsert([]*Entry{{Path: "a.mov", Name: "a.mov", ParentPath: ".", Inode: 7, Mtime: time.Unix(1_700_000_000, 0)}}, 10); err != nil {
		t.Fatal(err)
	}
	if has, err := s.HasEntries(); err != nil || !has {
		t.Fatalf("populated store HasEntries = %v, %v; want true", has, err)
	}
}

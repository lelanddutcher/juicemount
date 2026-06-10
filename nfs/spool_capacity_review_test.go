package nfs

// Phase-2 adversarial-review regression tests (docs/LAUNCH_PLAN.md ledger).
//
// BUG 1: failPermanent released capacity for rows it no longer owned
// (deleted by CancelForDelete mid-drain, or replaced by a rename-requeue),
// double-releasing the reservation. The CAS floor-at-zero MASKS the bug in
// any single-row test — `used` clamps to 0 whether released once or twice —
// so these tests hold a SECOND outstanding reservation and assert EXACT
// values throughout.
//
// BUG 3: RetryFailed could requeue a failed row whose path has a NEWER row,
// clobbering fresh bytes with the stale spool file.

import (
	"os"
	"path/filepath"
	"testing"
)

// TestFailPermanentAfterCancelDoesNotDoubleRelease: claim → user deletes
// (CancelForDelete releases + removes the row) → the drain worker fails out.
// failPermanent must NOT release again: a second live entry's reservation
// must survive exactly.
func TestFailPermanentAfterCancelDoesNotDoubleRelease(t *testing.T) {
	jfs, spool, drainer, _ := newSpoolWiredHandlerNoDrain(t)

	payloadA := patternedPayload(64<<10, 31) // the row that gets cancelled
	payloadB := patternedPayload(32<<10, 33) // the bystander reservation
	simulateNFSCreateThenWrite(t, jfs, "cancelme.bin", payloadA, 32<<10)
	simulateNFSCreateThenWrite(t, jfs, "bystander.bin", payloadB, 16<<10)
	if n := spool.sweepOnce(0); n != 2 {
		t.Fatalf("sweepOnce finalized %d entries, want 2", n)
	}
	wantBoth := int64(len(payloadA) + len(payloadB))
	if used, _ := spool.Capacity(); used != wantBoth {
		t.Fatalf("pre: used=%d want %d", used, wantBoth)
	}

	rowA, err := spool.Meta().LookupByPath("cancelme.bin")
	if err != nil {
		t.Fatalf("lookup cancelme: %v", err)
	}
	if claimed, _ := spool.Meta().MarkDraining(rowA.ID); !claimed {
		t.Fatalf("claim failed")
	}
	// Re-read post-claim, as the real worker does (Phase-1 BUG A fix).
	rowA, err = spool.Meta().Get(rowA.ID)
	if err != nil {
		t.Fatalf("post-claim get: %v", err)
	}

	// User deletes the file mid-drain: QA-37 cancel path releases A's bytes
	// and removes the row + spool file.
	spool.CancelForDelete("cancelme.bin")
	if used, _ := spool.Capacity(); used != int64(len(payloadB)) {
		t.Fatalf("post-cancel: used=%d want %d (B only)", used, len(payloadB))
	}

	// The worker, oblivious, fails the drain (e.g. spool file ENOENT).
	drainer.failPermanent(rowA, "test: spool file missing after cancel")

	// Without the ownership guard, failPermanent's release drops B's
	// reservation (clamped at the floor): used becomes 0 and the spool
	// over-admits by 32KiB forever. With the guard, B's bytes survive.
	if used, _ := spool.Capacity(); used != int64(len(payloadB)) {
		t.Fatalf("post-failPermanent: used=%d want %d — double-release detected", used, int64(len(payloadB)))
	}
}

// TestCapacityExactThroughFailRetryDoneCycle: one row's reservation through
// fail → retry-failed → drain-done, with a bystander reservation held the
// whole time so every transition asserts an exact non-zero value.
func TestCapacityExactThroughFailRetryDoneCycle(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandlerNoDrain(t)

	payloadC := patternedPayload(16<<10, 41)
	payloadB := patternedPayload(8<<10, 43) // bystander
	simulateNFSCreateThenWrite(t, jfs, "cycle.bin", payloadC, 8<<10)
	if n := spool.sweepOnce(0); n != 1 {
		t.Fatalf("sweepOnce finalized %d, want 1", n)
	}
	// Bystander written AFTER the sweep: it stays in `writing` state, so the
	// drainer never touches it and its reservation is held through the whole
	// cycle — every assertion below is against an exact non-zero baseline.
	simulateNFSCreateThenWrite(t, jfs, "hold.bin", payloadB, 8<<10)
	cBytes, bBytes := int64(len(payloadC)), int64(len(payloadB))
	if used, _ := spool.Capacity(); used != cBytes+bBytes {
		t.Fatalf("pre: used=%d want %d", used, cBytes+bBytes)
	}

	rowC, err := spool.Meta().LookupByPath("cycle.bin")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if claimed, _ := spool.Meta().MarkDraining(rowC.ID); !claimed {
		t.Fatalf("claim failed")
	}
	rowC, _ = spool.Meta().Get(rowC.ID)

	// Terminal failure: row owns its reservation → release happens once.
	drainer.failPermanent(rowC, "test: simulated permanent failure")
	if used, _ := spool.Capacity(); used != bBytes {
		t.Fatalf("post-fail: used=%d want %d (bystander only)", used, bBytes)
	}

	// Operator retry: re-reserves exactly C's bytes.
	n, err := spool.RetryFailed()
	if err != nil || n != 1 {
		t.Fatalf("RetryFailed: n=%d err=%v, want 1", n, err)
	}
	if used, _ := spool.Capacity(); used != cBytes+bBytes {
		t.Fatalf("post-retry: used=%d want %d", used, cBytes+bBytes)
	}

	// Drain completes: C's reservation releases exactly once; the index
	// entry for the retried row was never re-created, which is fine — the
	// drain path releases via MarkDrainComplete's row-ownership check.
	drainAllForTest(t, drainer)
	if used, _ := spool.Capacity(); used != bBytes {
		t.Fatalf("post-drain: used=%d want %d (bystander only)", used, bBytes)
	}
	got, err := os.ReadFile(filepath.Join(fuseRoot, "cycle.bin"))
	if err != nil || len(got) != int(cBytes) {
		t.Fatalf("drained file: err=%v len=%d want %d", err, len(got), cBytes)
	}
}

// TestRetryFailedSkipsRowsWithNewerRowForPath: a failed row must not be
// requeued when ANY newer row exists for the same path — the stale spool
// file would clobber the newer bytes (review BUG 3).
func TestRetryFailedSkipsRowsWithNewerRowForPath(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandlerNoDrain(t)

	stale := patternedPayload(12<<10, 51)
	simulateNFSCreateThenWrite(t, jfs, "doc.bin", stale, 6<<10)
	if n := spool.sweepOnce(0); n != 1 {
		t.Fatalf("sweep1: %d", n)
	}
	rowOld, err := spool.Meta().LookupByPath("doc.bin")
	if err != nil {
		t.Fatalf("lookup old: %v", err)
	}
	if claimed, _ := spool.Meta().MarkDraining(rowOld.ID); !claimed {
		t.Fatalf("claim failed")
	}
	rowOld, _ = spool.Meta().Get(rowOld.ID)
	drainer.failPermanent(rowOld, "test: NAS unreachable")

	// User deletes the failed file in Finder (evicts the zombie index entry;
	// the FAILED SQL row survives — it's not "active"), then re-copies it:
	// fresh bytes, NEWER row for the same path.
	if err := jfs.Remove("doc.bin"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	fresh := patternedPayload(20<<10, 53)
	simulateNFSCreateThenWrite(t, jfs, "doc.bin", fresh, 10<<10)
	if n := spool.sweepOnce(0); n != 1 {
		t.Fatalf("sweep2: %d", n)
	}
	drainAllForTest(t, drainer) // fresh bytes land on FUSE

	// Retry must SKIP the stale failed row (newer row exists).
	n, err := spool.RetryFailed()
	if err != nil {
		t.Fatalf("RetryFailed: %v", err)
	}
	if n != 0 {
		t.Fatalf("RetryFailed requeued %d rows, want 0 (stale row must be skipped)", n)
	}
	row, err := spool.Meta().Get(rowOld.ID)
	if err != nil {
		t.Fatalf("get old row: %v", err)
	}
	if string(row.DrainState) != "failed" {
		t.Fatalf("stale row state=%s, want failed (untouched)", row.DrainState)
	}
	// And the fresh bytes survive on FUSE.
	got, err := os.ReadFile(filepath.Join(fuseRoot, "doc.bin"))
	if err != nil || len(got) != len(fresh) {
		t.Fatalf("fresh content: err=%v len=%d want %d", err, len(got), len(fresh))
	}
}

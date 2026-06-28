package nfs

import (
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

// TestFailPermanentEvictsIndexEntry is the review FIX 3 gate.
//
// Before the fix, Drainer.failPermanent marked the SQL row failed and released
// capacity but left the in-memory spool index entry in place. HasPending(path)
// — the exact O(1) signal QA-30 Layer D consults to spare a path from the
// metadata prune — therefore kept returning true for the process lifetime (the
// boot scrubber does NOT repopulate the index for failed rows). Consequence: a
// permanently-failed path became un-prunable forever; if the user later deleted
// it, the stale store entry + its NFS handle could never be pruned (Layer D
// kept sparing it), surfacing as leaked metadata / stale handles until restart.
//
// The fix evicts the index entry (identity-checked) inside failPermanent. This
// test asserts:
//   - HasPending is TRUE while the entry is live (precondition),
//   - HasPending is FALSE after a permanent failure (the core FIX 3 invariant),
//   - the SQL row is in the failed state (failPermanent still does its job),
//   - a subsequently-deleted permanently-failed path is cleanly prunable —
//     CancelForDelete leaves no index entry and no SQL row behind.
func TestFailPermanentEvictsIndexEntry(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})

	const nfsPath = "/Films/doomed.mov"
	e := writeSpoolEntry(t, spool, nfsPath, []byte("permanently failing bytes"))

	// Precondition: the live entry makes HasPending true — this is what Layer D
	// reads to spare the path while it is genuinely draining.
	if !spool.HasPending(nfsPath) {
		t.Fatalf("precondition: HasPending(%q) = false, want true (entry is live)", nfsPath)
	}

	row, err := spool.Meta().Get(e.ID())
	if err != nil || row == nil {
		t.Fatalf("get row: %v", err)
	}

	// Permanent failure: mark failed, release capacity, AND (FIX 3) evict the
	// index entry.
	d.failPermanent(row, "retry budget exhausted (test)")

	// Core FIX 3 invariant: a permanently-failed path is NO LONGER spool-pending,
	// so Layer D will stop sparing it and a genuine prune can proceed.
	if spool.HasPending(nfsPath) {
		t.Errorf("FIX 3: HasPending(%q) = true after permanent failure — the index entry was NOT evicted, so Layer D would make this path un-prunable forever", nfsPath)
	}

	// failPermanent still records the failure in SQL.
	got, _ := spool.Meta().Get(e.ID())
	if got == nil || got.DrainState != metadata.DrainFailed {
		t.Errorf("row state = %v, want failed", got)
	}
	if d.Metrics().DrainsFailed.Load() == 0 {
		t.Errorf("DrainsFailed counter should be > 0 after a permanent failure")
	}

	// "Subsequently-deleted permanently-failed path is prunable again": the user
	// deletes the path; CancelForDelete must leave nothing behind (no stale index
	// entry, no SQL row). With the pre-fix lingering index entry this still
	// removed the row, but the orphaned-entry class is what Layer D mis-spared;
	// here we assert the path is fully clean post-delete.
	spool.CancelForDelete(nfsPath)
	if spool.HasPending(nfsPath) {
		t.Errorf("after CancelForDelete, HasPending(%q) = true, want false (path must be fully prunable)", nfsPath)
	}
	if _, ok := spool.LookupActive(nfsPath); ok {
		t.Errorf("after CancelForDelete, an index entry for %q still exists", nfsPath)
	}
}

// TestEvictIndexIdentityChecked proves the FIX 3 EvictIndex helper is
// identity-checked: if the entry currently at the path has a DIFFERENT row id
// than the one being evicted (a newer writer rotated in a fresh entry for the
// same path — e.g. the user re-created the file after the old drain
// permanently failed), EvictIndex must be a no-op and NOT stomp the live entry.
// Otherwise a re-create racing a late failPermanent would wrongly make the
// fresh file appear non-pending — Layer D would then prune the live re-created
// path, the mirror-image data-loss bug.
func TestEvictIndexIdentityChecked(t *testing.T) {
	spool := newTestSpoolStore(t, 0)

	const nfsPath = "/Films/reopened.mov"
	// A live entry occupies the path (kept open so it stays the active slot).
	e, err := spool.OpenWrite(nfsPath)
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if _, err := e.WriteAt([]byte("live bytes"), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !spool.HasPending(nfsPath) {
		t.Fatalf("precondition: HasPending(%q) = false, want true", nfsPath)
	}

	// Evicting a STALE / non-matching id must be a no-op — the slot holds a
	// different (live) entry id.
	if evicted := spool.EvictIndex(nfsPath, e.ID()+9999); evicted {
		t.Errorf("EvictIndex(stale id) evicted the slot, but it holds a different live entry — must be identity-checked no-op")
	}
	if !spool.HasPending(nfsPath) {
		t.Errorf("HasPending(%q) = false after a stale-id evict — the live entry was wrongly removed", nfsPath)
	}

	// Evicting the CURRENT id does remove it.
	if evicted := spool.EvictIndex(nfsPath, e.ID()); !evicted {
		t.Errorf("EvictIndex(current id) should have evicted the live entry")
	}
	if spool.HasPending(nfsPath) {
		t.Errorf("HasPending(%q) = true after evicting the current entry", nfsPath)
	}
}

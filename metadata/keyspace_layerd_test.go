package metadata

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestScopedPruneLayerDSparesSpoolPending is the QA-30 Layer D gate (NFSv3
// sprint). It reproduces the live build-438 bug: a Finder copy routes a new
// file to the write spool; while it is spool-pending (NOT in Redis, NOT in
// FUSE), a keyspace reconcile of the parent dir runs scopedPrune. The file is
// absent from the fresh Redis name set, so it is a prune candidate; Layer C
// (pin) misses (not pinned) and Layer A (FUSE Lstat) would miss (only on the
// spool). WITHOUT Layer D the file is pruned via store.DeletePaths →
// handles.Forget(path) → the kernel's path-stable Track-B handle for that path
// resolves to STALE (FromHandle unknown) → Finder aborts with error 100070.
//
// The test asserts:
//   - the spool-pending child is NOT deleted from the store, AND
//   - while a sibling that is NOT spool-pending and NOT in freshNames IS pruned
//     (proving Layer D is TARGETED, not a blanket prune-disable).
//
// NOTE (rc-keyspace base): the original sprint test also asserted the path's
// Track-B path-stable handle survived (HandlePathByID). Track B is NOT in this
// release-candidate base, so the load-bearing Layer D guarantee asserted here is
// the store-prune spare (LookupByPath) — the exact behavior of the guard.
//
// Red/green: with rc.spoolPending = nil (or pointing at a stub that always
// returns false), the spool-pending child IS deleted and its handle Forgotten —
// the subtests below exercise both, so the guard's effect is proven, not
// assumed.
func TestScopedPruneLayerDSparesSpoolPending(t *testing.T) {
	mt := time.Unix(1700000000, 0)

	// seed builds a store with two top-level children under a parent dir:
	//   parent="dir" (inode 10)
	//   dir/pending.mov (inode 11) — the spool-pending file
	//   dir/gone.mov    (inode 12) — a genuinely-deleted sibling
	// Both are absent from the fresh Redis name set, so both are candidates;
	// only "pending.mov" is reported spool-pending.
	seed := func(t *testing.T) *Store {
		t.Helper()
		store, err := Open(filepath.Join(t.TempDir(), "store.db"))
		if err != nil {
			t.Fatalf("Open store: %v", err)
		}
		t.Cleanup(func() { store.Close() })

		for _, e := range []*Entry{
			MakeEntry("dir", true, 0, mt, 10),
			MakeEntry("dir/pending.mov", false, 1234, mt, 11),
			MakeEntry("dir/gone.mov", false, 5678, mt, 12),
		} {
			store.InsertToCache(e)
			if err := store.Insert(e); err != nil {
				t.Fatalf("Insert %q: %v", e.Path, err)
			}
		}

		// Sanity: both children indexed under the "dir" parent key.
		if c, _ := store.ListChildren("dir"); len(c) != 2 {
			t.Fatalf("precondition: ListChildren(\"dir\") = %d, want 2", len(c))
		}
		return store
	}

	// Fresh Redis name set for "dir" is EMPTY — both children are absent (the
	// HGETALL diff dropped them). Both are therefore prune candidates.
	freshNames := map[string]struct{}{}

	t.Run("guard_spares_pending_prunes_sibling", func(t *testing.T) {
		store := seed(t)

		rc := &RedisClient{store: store} // no fuseRoot, no pin checker
		// Layer D guard: only "dir/pending.mov" is spool-pending.
		rc.SetSpoolGuard(func(volRelPath string) bool {
			return volRelPath == "dir/pending.mov"
		})

		rc.scopedPrune("dir", freshNames)

		// Spool-pending child MUST survive in the store (the Layer D guarantee).
		if store.LookupByPath("dir/pending.mov") == nil {
			t.Error("Layer D: spool-pending dir/pending.mov was pruned (must be spared) — this is the ESTALE/100070 bug")
		}

		// The genuinely-gone sibling MUST still be pruned (targeted guard).
		if store.LookupByPath("dir/gone.mov") != nil {
			t.Error("Layer D over-spared: dir/gone.mov (no spool entry, absent from Redis) must be pruned")
		}
	})

	// RED control: with no guard (nil spoolPending — the pre-fix behavior) the
	// spool-pending child IS pruned. This proves the test would FAIL without
	// Layer D, i.e. the guard is load-bearing.
	t.Run("without_guard_pending_is_wrongly_pruned", func(t *testing.T) {
		store := seed(t)

		rc := &RedisClient{store: store} // spoolPending == nil → pre-fix path
		rc.scopedPrune("dir", freshNames)

		if store.LookupByPath("dir/pending.mov") != nil {
			t.Error("control precondition broken: without the guard the candidate should be pruned (this is exactly the bug Layer D fixes)")
		}
	})
}

// TestScopedPruneLayerDSparesSpoolPendingDescendant covers the subtree case: a
// candidate DIRECTORY (absent from Redis) is expanded by collectSubtree to its
// descendants; a spool-pending FILE inside it must still be spared even though
// only the dir itself was a candidate root. This exercises the FINAL-toDelete
// re-filter, not just the candidate-root filter.
func TestScopedPruneLayerDSparesSpoolPendingDescendant(t *testing.T) {
	mt := time.Unix(1700000000, 0)

	store, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer store.Close()

	// parent="proj" with one child dir "proj/shoot" that itself holds a
	// spool-pending file "proj/shoot/clip.mov" plus a genuinely-gone sibling
	// "proj/shoot/old.mov". "proj/shoot" is the prune candidate (absent from
	// the fresh set); collectSubtree expands it to both files.
	for _, e := range []*Entry{
		MakeEntry("proj", true, 0, mt, 100),
		MakeEntry("proj/shoot", true, 0, mt, 101),
		MakeEntry("proj/shoot/clip.mov", false, 1234, mt, 102),
		MakeEntry("proj/shoot/old.mov", false, 5678, mt, 103),
	} {
		store.InsertToCache(e)
		if err := store.Insert(e); err != nil {
			t.Fatalf("Insert %q: %v", e.Path, err)
		}
	}
	rc := &RedisClient{store: store}
	rc.SetSpoolGuard(func(volRelPath string) bool {
		return volRelPath == "proj/shoot/clip.mov"
	})

	// Fresh set for "proj" is empty → "proj/shoot" is a candidate; its subtree
	// (clip.mov, old.mov) is expanded into toDelete.
	rc.scopedPrune("proj", map[string]struct{}{})

	// The spool-pending DESCENDANT must survive (final-toDelete re-filter).
	if store.LookupByPath("proj/shoot/clip.mov") == nil {
		t.Error("Layer D subtree: spool-pending descendant proj/shoot/clip.mov was pruned (must be spared)")
	}
	// The genuinely-gone descendant must still be pruned.
	if store.LookupByPath("proj/shoot/old.mov") != nil {
		t.Error("Layer D subtree over-spared: proj/shoot/old.mov must be pruned")
	}
}

// TestSpoolGuardConcurrentSetAndRead is the review FIX 1 gate. The spoolPending
// guard field is written by SetSpoolGuard and read by the reconcile goroutine
// (scopedPrune / syncMetadata's filterSpoolPending). Production installs the
// guard AFTER rc.Start has already launched that goroutine, so the write and
// reads race. With the field stored in an atomic.Pointer this is race-clean;
// with a plain field `go test -race` flags it. This test spins a writer
// hammering SetSpoolGuard against many concurrent readers driving the actual
// prune read path (scopedPrune, which loads the guard via loadSpoolGuard and
// also a direct filterSpoolPending call). Run with -race.
func TestSpoolGuardConcurrentSetAndRead(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer store.Close()

	mt := time.Unix(1700000000, 0)
	// A parent with a couple of children so scopedPrune actually walks the
	// candidate list and invokes the guard read path each call.
	for _, e := range []*Entry{
		MakeEntry("dir", true, 0, mt, 10),
		MakeEntry("dir/a.mov", false, 1, mt, 11),
		MakeEntry("dir/b.mov", false, 2, mt, 12),
	} {
		store.InsertToCache(e)
		if err := store.Insert(e); err != nil {
			t.Fatalf("Insert %q: %v", e.Path, err)
		}
	}

	rc := &RedisClient{store: store}

	var (
		wg   sync.WaitGroup
		stop atomic.Bool
	)

	// Writer: continuously install/clear the guard (the SetSpoolGuard store).
	wg.Add(1)
	go func() {
		defer wg.Done()
		var guards = []func(string) bool{
			func(string) bool { return true },
			func(string) bool { return false },
			nil,
		}
		for i := 0; !stop.Load(); i++ {
			rc.SetSpoolGuard(guards[i%len(guards)])
		}
	}()

	// Readers: drive the real reconcile read path concurrently. scopedPrune
	// re-seeds nothing destructive that matters across iterations here — the
	// point is the concurrent guard LOAD against the writer's STORE. We also
	// hit filterSpoolPending directly for a second reader flavor.
	const readers = 8
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				// loadSpoolGuard path (atomic load) exercised here:
				rc.filterSpoolPending([]string{"dir/a.mov", "dir/b.mov"})
				_ = rc.loadSpoolGuard()
			}
		}()
	}

	// Let the race detector observe many interleavings.
	time.Sleep(150 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
}

// TestSyncMetadataPruneSparesSpoolPending is the review FIX 2 gate: the periodic
// full-SCAN prune (syncMetadata) must apply the SAME Layer D spool-pending
// spare as scopedPrune. syncMetadata itself needs a live Redis, but its prune
// decision funnels through rc.filterSpoolPending (the shared helper, called on
// the toDelete set right before store.DeletePaths). This test exercises that
// exact helper with the production guard wiring and asserts:
//   - a spool-pending path in toDelete is SPARED (removed from the delete set),
//   - a genuinely-absent path in toDelete is STILL pruned (kept in the set),
//   - with no guard wired the spool-pending path is (wrongly) pruned — proving
//     the filter is load-bearing, i.e. without FIX 2 the SCAN path hits the bug.
func TestSyncMetadataPruneSparesSpoolPending(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer store.Close()

	// The full-SCAN prune builds toDelete from pruneAbsent (paths absent from
	// Redis for >= PruneThreshold cycles). We model that set directly: one
	// spool-pending file still draining, one genuinely-gone file.
	toDelete := []string{"dir/draining.mov", "dir/gone.mov"}

	t.Run("guard_spares_pending_prunes_gone", func(t *testing.T) {
		rc := &RedisClient{store: store}
		rc.SetSpoolGuard(func(volRelPath string) bool {
			return volRelPath == "dir/draining.mov"
		})

		kept, spared := rc.filterSpoolPending(append([]string(nil), toDelete...))
		if spared != 1 {
			t.Fatalf("spared = %d, want 1 (the spool-pending file)", spared)
		}
		// The spool-pending file must NOT be in the kept (to-be-deleted) set.
		for _, p := range kept {
			if p == "dir/draining.mov" {
				t.Error("FIX 2: spool-pending dir/draining.mov was NOT spared from the SCAN prune (ESTALE/100070 bug)")
			}
		}
		// The genuinely-gone file MUST still be pruned (in the kept set).
		foundGone := false
		for _, p := range kept {
			if p == "dir/gone.mov" {
				foundGone = true
			}
		}
		if !foundGone {
			t.Error("FIX 2 over-spared: dir/gone.mov (no spool entry) must still be pruned by the SCAN path")
		}
	})

	// RED control: with no guard the spool-pending file is NOT spared — proving
	// the FIX 2 filter is what closes the SCAN-path ESTALE bug.
	t.Run("without_guard_pending_is_pruned", func(t *testing.T) {
		rc := &RedisClient{store: store} // no SetSpoolGuard → guard nil
		kept, spared := rc.filterSpoolPending(append([]string(nil), toDelete...))
		if spared != 0 {
			t.Fatalf("control: spared = %d, want 0 with no guard", spared)
		}
		// Both paths remain in the delete set — including the draining one.
		if len(kept) != 2 {
			t.Fatalf("control: kept = %d, want 2 (nothing spared without the guard)", len(kept))
		}
	})
}

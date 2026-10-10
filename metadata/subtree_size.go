package metadata

import (
	"os"
	"path"
)

// INSTANT-NAV #2: incremental SUBTREE-SIZE aggregation.
//
// Answers "total bytes + file count under directory P" in O(1) from RAM so the
// app/OpenLoupe can show folder sizes instantly instead of du-walking the tree
// (Finder's own size column still walks — this is the control-plane/app
// surface, exposed via GET /du on the metrics listener).
//
// DESIGN
//
//   - Two per-directory aggregate maps on Store — subtreeBytes / subtreeFiles,
//     keyed by directory path ("." = volume root, matching the childrenIdx["."]
//     storeParent convention) — guarded by the SAME s.mu as pathCache.
//   - INVARIANT: for every dir P, subtreeBytes[P] == Σ Size over all non-dir
//     entries CURRENTLY IN pathCache whose path is strictly under P (any
//     depth), and subtreeFiles[P] == their count. Directory entries themselves
//     contribute 0 bytes / 0 files. Keys whose both aggregates reach exactly 0
//     are removed, so the maps stay bounded by "dirs with content".
//   - Maintenance is incremental: every mutation that changes a file's Size or
//     its presence in pathCache walks the ParentPath chain to the root adding
//     the delta — O(depth) map ops while the write lock is ALREADY held
//     (Insert/InsertToCache, BulkInsert/BulkInsertAbsent, Delete/
//     DeleteFromCache/DeletePaths, UpdateSize, BatchDrainComplete,
//     evictInodeOrphanLocked's rename-orphan sweep, evictOldest's drops,
//     RecoverShadow). Wholesale cache rebuilds (rebuildCaches at Open,
//     evictOldest already covered per-drop) recompute in one O(N×depth) pass.
//   - Anchoring the invariant on pathCache (not SQLite, not the backend) is
//     what makes every delta exact: an upsert that displaces an old entry
//     applies (new−old), a re-insert after eviction re-adds exactly once, and
//     the aggregates can never double-count. In production the RAM cache IS
//     the full mirror (maxCacheSize 500k ≫ ~300k entries; evictOldest is a
//     no-op), so "Σ over pathCache" == "Σ over the mirror".
//
// HOT-PATH DISCIPLINE: no read accessor consults the aggregate maps. RAM
// lookups and child listings only take the existing cache lock and create the
// immutable snapshots required to keep callers race-free. All aggregate
// maintenance rides existing s.mu.Lock() WRITE sections, adding O(depth) map
// ops to paths already dominated by SQLite work.
//
// GATE: JM_SUBTREE_SIZES, DEFAULT ON. Off (=0): the maps stay nil, every
// maintenance helper returns before touching them (zero map writes on any
// mutation — byte-identical mutation behavior to before this feature), and
// SubtreeSize reports ok=false.

// subtreeSizesEnabled reports whether the subtree-size aggregation is on.
// Read once at Open (s.subtreeOn), matching the ftsDefer snapshot pattern.
// DEFAULT ON; JM_SUBTREE_SIZES=0 disables (see docs/TUNING conventions).
func subtreeSizesEnabled() bool {
	return os.Getenv("JM_SUBTREE_SIZES") != "0"
}

// SubtreeSize returns the recursive total bytes and file count under the
// directory dirPath, O(1) under RLock. "" is treated as "." (volume root, the
// storeParent convention). ok is false when the feature is gated off
// (JM_SUBTREE_SIZES=0); a directory that is empty, unknown, or all-dirs
// returns (0, 0, true) — existence is the caller's question (LookupByPath).
func (s *Store) SubtreeSize(dirPath string) (bytes int64, files int64, ok bool) {
	if !s.subtreeOn {
		return 0, 0, false
	}
	if dirPath == "" {
		dirPath = "."
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.subtreeBytes[dirPath], s.subtreeFiles[dirPath], true
}

// subtreeContribution returns the (bytes, files) a single entry contributes to
// its ancestors' aggregates. Directories contribute nothing themselves — only
// the files beneath them count. nil-safe.
func subtreeContribution(e *Entry) (int64, int64) {
	if e == nil || e.IsDir {
		return 0, 0
	}
	return e.Size, 1
}

// applySubtreeDeltaLocked adds (bytes, files) to every directory on the chain
// from parentPath up to and including the root "." — O(depth) map ops. Caller
// must hold s.mu for writing AND have checked s.subtreeOn (every caller below
// does; the maps are only allocated when the gate is on).
//
// Termination: the mirror keys are volume-relative ("a/b"; root children under
// "."), so path.Dir strictly shortens the path until "."; the "/"/""/fixpoint
// guards make pathological inputs (absolute or already-root paths) safe too.
func (s *Store) applySubtreeDeltaLocked(parentPath string, bytes, files int64) {
	if bytes == 0 && files == 0 {
		return
	}
	p := parentPath
	if p == "" {
		p = "."
	}
	for {
		nb := s.subtreeBytes[p] + bytes
		nf := s.subtreeFiles[p] + files
		if nb == 0 && nf == 0 {
			// Exact zero on both — drop the key so the maps stay bounded by
			// dirs-with-content (a fully-emptied subtree leaves no residue).
			delete(s.subtreeBytes, p)
			delete(s.subtreeFiles, p)
		} else {
			s.subtreeBytes[p] = nb
			s.subtreeFiles[p] = nf
		}
		if p == "." || p == "/" {
			return
		}
		parent := path.Dir(p)
		if parent == p {
			return // defensive: cannot ascend further
		}
		p = parent
	}
}

// subtreeUpsertLocked applies the aggregate delta for writing e into pathCache
// where old was pathCache[e.Path] at the same instant (nil when the path is
// new). A size-change upsert therefore applies exactly (new−old) — never a
// double count. Caller must hold s.mu for writing.
func (s *Store) subtreeUpsertLocked(old, e *Entry) {
	if !s.subtreeOn {
		return
	}
	ob, of := subtreeContribution(old)
	nb, nf := subtreeContribution(e)
	if old == nil || old.ParentPath == e.ParentPath {
		// Common case (same path ⇒ same parent): one merged walk.
		s.applySubtreeDeltaLocked(e.ParentPath, nb-ob, nf-of)
		return
	}
	// Defensive: displaced entry recorded a different parent spelling — undo
	// its contribution on ITS chain, add e's on e's chain.
	s.applySubtreeDeltaLocked(old.ParentPath, -ob, -of)
	s.applySubtreeDeltaLocked(e.ParentPath, nb, nf)
}

// subtreeRemoveLocked undoes e's contribution when e leaves pathCache
// (Delete/DeleteFromCache/DeletePaths, evictInodeOrphanLocked's rename-orphan
// sweep, evictOldest's memory-pressure drops). nil-safe. Caller must hold
// s.mu for writing.
func (s *Store) subtreeRemoveLocked(e *Entry) {
	if !s.subtreeOn || e == nil {
		return
	}
	b, f := subtreeContribution(e)
	s.applySubtreeDeltaLocked(e.ParentPath, -b, -f)
}

// subtreeResizeLocked applies the delta for an IN-PLACE e.Size change (the
// UpdateSize / BatchDrainComplete MAX-bump paths mutate the cached Entry
// directly, no pathCache map write). Call AFTER e.Size is updated, passing the
// pre-update size; the no-change case (MAX semantics rejected a shrink) is a
// zero delta and returns immediately. Caller must hold s.mu for writing.
func (s *Store) subtreeResizeLocked(e *Entry, oldSize int64) {
	if !s.subtreeOn || e == nil || e.IsDir {
		return
	}
	s.applySubtreeDeltaLocked(e.ParentPath, e.Size-oldSize, 0)
}

// recomputeSubtreeAggregatesLocked rebuilds both aggregate maps from the
// CURRENT pathCache in one O(N×depth) pass — the boot/wholesale-rebuild
// counterpart to the incremental deltas (called by rebuildCaches after the
// caches are swapped in and evictOldest has trimmed them, so the pass sees
// exactly the final serving state). ~300k entries recompute in well under 1s
// (see BenchmarkSubtreeRecompute_300k). Caller must hold s.mu for writing.
func (s *Store) recomputeSubtreeAggregatesLocked() {
	if !s.subtreeOn {
		return
	}
	s.subtreeBytes = make(map[string]int64, len(s.childrenIdx))
	s.subtreeFiles = make(map[string]int64, len(s.childrenIdx))
	for _, e := range s.pathCache {
		b, f := subtreeContribution(e)
		s.applySubtreeDeltaLocked(e.ParentPath, b, f)
	}
}

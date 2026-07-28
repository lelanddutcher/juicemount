package metadata

import (
	"path"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// RenameSubtree re-keys every DESCENDANT of a renamed directory from
// oldDir/... to newDir/... (#8 / task #109: a folder rename previously
// re-keyed only the directory's own entry — its children kept their old
// paths in pathCache/childrenIdx/SQLite, so the moved folder listed EMPTY
// until a remount or full SCAN rebuilt the mirror).
//
// The caller (nfs Rename) is responsible for the renamed directory's OWN
// entry (the existing delete-old/insert-new flow); this handles strictly the
// subtree beneath it.
//
// Two phases, mirroring the existing rename's cache-sync/SQLite-async split:
//
//  1. RAM (synchronous, chunked): pathCache re-key, childrenIdx re-bucket,
//     Entry.Path/ParentPath rewrite in place (inodeCache is inode-keyed and
//     holds the same pointers — no re-key needed), and the subtree-size
//     aggregates move (per-dir keys re-keyed; the subtree's total subtracted
//     from the old ancestor chain and added to the new). Chunked at
//     renameChunk mutations per lock hold (the S5/BulkClear discipline) so a
//     huge subtree can't stall readers; a reader during the window sees a
//     transiently split tree — strictly better than the fully-orphaned
//     children of the old behavior, and healed within the same call.
//
//  2. Durable (async, like the existing rename's SQLite goroutine): the old
//     rows are removed and the re-keyed entries re-inserted via the existing
//     DeletePaths + BulkInsert paths — both already maintain the
//     external-content FTS correctly, which a raw UPDATE would not.
//
// Returns the number of descendants re-keyed (0 for an empty/unknown dir).
func (s *Store) RenameSubtree(oldDir, newDir string) int {
	// Serving-path integrity (2026-07-28): tell the NFS layer to drop every
	// pooled FUSE fd under BOTH ends of this move. The pool is keyed by path
	// alone, so after the caller's os.Rename each descendant's cached fd still
	// points at the pre-rename inode — a later read of the same name is served
	// the OLD file's bytes (right length, wrong content, no error) and a later
	// in-place write lands inside the moved-away file.
	//
	// Deferred so it also fires on the len(oldPaths)==0 early return: the
	// mirror having no descendants does NOT mean the POOL has none (a mirror
	// entry can be evicted while its fd is still pooled). Fired outside every
	// lock this function takes. Ordering is not load-bearing — the caller has
	// already executed the FUSE rename, so any re-open after this point
	// resolves the new, correct identity.
	defer s.fireSubtreeRenamed(oldDir, newDir)

	oldPrefix := oldDir + "/"

	// Snapshot the descendant set under a read lock (keys only — the
	// entries are re-fetched under the write lock chunk by chunk so a
	// concurrent mutation between snapshot and re-key can't act on a stale
	// pointer).
	s.mu.RLock()
	oldPaths := make([]string, 0, 64)
	for p := range s.pathCache {
		if strings.HasPrefix(p, oldPrefix) {
			oldPaths = append(oldPaths, p)
		}
	}
	s.mu.RUnlock()
	if len(oldPaths) == 0 {
		return 0
	}

	renamed := make([]*Entry, 0, len(oldPaths))
	deleted := make([]string, 0, len(oldPaths))

	// Phase 1a: move the subtree-size TOTAL between the ancestor chains and
	// re-key the per-dir aggregate keys, then re-key the entries themselves —
	// all chunked under the write lock.
	s.mu.Lock()
	if subtreeSizesEnabled() && s.subtreeBytes != nil {
		totB := s.subtreeBytes[oldDir]
		totF := s.subtreeFiles[oldDir]
		if totB != 0 || totF != 0 {
			// Subtract from the OLD ancestor chain (starting at oldDir's
			// parent) and add to the NEW one. The oldDir/newDir keys
			// themselves are re-keyed below with the other per-dir keys.
			s.applySubtreeDeltaLocked(parentKeyOf(oldDir), -totB, -totF)
			s.applySubtreeDeltaLocked(parentKeyOf(newDir), totB, totF)
			delete(s.subtreeBytes, oldDir)
			delete(s.subtreeFiles, oldDir)
			s.subtreeBytes[newDir] = totB
			s.subtreeFiles[newDir] = totF
		}
		for _, op := range oldPaths {
			if b, ok := s.subtreeBytes[op]; ok {
				np := newDir + op[len(oldDir):]
				delete(s.subtreeBytes, op)
				s.subtreeBytes[np] = b
			}
			if f, ok := s.subtreeFiles[op]; ok {
				np := newDir + op[len(oldDir):]
				delete(s.subtreeFiles, op)
				s.subtreeFiles[np] = f
			}
		}
	}
	s.mu.Unlock()

	// Phase 1b: re-key the entries, chunked.
	for start := 0; start < len(oldPaths); start += renameChunk {
		end := start + renameChunk
		if end > len(oldPaths) {
			end = len(oldPaths)
		}
		s.mu.Lock()
		for _, op := range oldPaths[start:end] {
			e, ok := s.pathCache[op]
			if !ok {
				continue // deleted concurrently — nothing to move
			}
			np := newDir + op[len(oldDir):]
			s.removeFromChildrenIdx(e)
			delete(s.pathCache, op)
			e.Path = np
			e.ParentPath = path.Dir(np)
			s.pathCache[np] = e
			s.addToChildrenIdx(e)
			renamed = append(renamed, e)
			deleted = append(deleted, op)
		}
		s.mu.Unlock()
	}

	// Phase 2: durable re-key, async (the existing rename's SQLite pattern).
	// DeletePaths + BulkInsert both maintain FTS correctly; a raw path UPDATE
	// would silently corrupt the external-content index.
	go func(dead []string, live []*Entry) {
		t0 := time.Now()
		if err := s.DeletePaths(dead); err != nil {
			jmlog.Warn("rename subtree: durable delete failed (mirror cache is correct; SCAN heals SQLite)",
				"old_dir", oldDir, "error", err.Error())
		}
		if err := s.BulkInsert(live, 500); err != nil {
			jmlog.Warn("rename subtree: durable insert failed (mirror cache is correct; SCAN heals SQLite)",
				"new_dir", newDir, "error", err.Error())
		}
		jmlog.Info("rename subtree: durable re-key complete",
			"old_dir", oldDir, "new_dir", newDir,
			"descendants", len(live), "ms", time.Since(t0).Milliseconds())
	}(deleted, renamed)

	return len(renamed)
}

// renameChunk bounds mutations per write-lock hold during a subtree re-key
// (same discipline as cacheMutationChunk for bulk clears).
const renameChunk = 2048

// parentKeyOf returns the childrenIdx/aggregate key of a path's parent —
// "." for a top-level entry, matching the storeParent convention.
func parentKeyOf(p string) string {
	d := path.Dir(p)
	if d == "" || d == "/" {
		return "."
	}
	return d
}

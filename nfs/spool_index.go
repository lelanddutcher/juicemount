package nfs

import (
	"sync"
)

// SpoolIndex is a path → *SpoolEntry in-memory lookup table.
//
// Used by the NFS read path (slice D) to answer "is this file currently
// in the spool?" in O(1) without an SQLite round-trip. The QA-35 perf-
// discipline gate explicitly requires the read-path hot lookup to be
// in-memory only — any SQL probe in Stat/Lstat/OpenFile would be a
// regression.
//
// Coherence with the SQLite index (metadata.SpoolStore) is maintained by
// the writer side: SpoolStore.OpenWrite inserts here AFTER inserting the
// SQL row, and SpoolStore.markDoneLocked deletes from here AFTER updating
// the SQL row. Boot-time recovery rebuilds the in-memory index from
// metadata.SpoolStore.ListAll filtered to in-flight states.
type SpoolIndex struct {
	mu     sync.RWMutex
	byPath map[string]*SpoolEntry
}

// NewSpoolIndex returns an empty index.
func NewSpoolIndex() *SpoolIndex {
	return &SpoolIndex{
		byPath: make(map[string]*SpoolEntry),
	}
}

// Insert adds or replaces the entry for path. Replacement semantics are
// chosen intentionally: a second OpenWrite on the same path overrides the
// first (the caller is responsible for closing the previous entry first,
// or accepting that it becomes orphaned in the SQL index).
func (idx *SpoolIndex) Insert(path string, e *SpoolEntry) {
	idx.mu.Lock()
	idx.byPath[path] = e
	idx.mu.Unlock()
}

// Lookup returns the entry for path and ok=true if one exists. O(1).
// This is the hot read-path call; the lock is RLock to allow concurrent
// readers.
func (idx *SpoolIndex) Lookup(path string) (*SpoolEntry, bool) {
	idx.mu.RLock()
	e, ok := idx.byPath[path]
	idx.mu.RUnlock()
	return e, ok
}

// Delete removes path from the index unconditionally. Called when the
// caller is certain it owns the entry at this path. Prefer
// DeleteIfMatches when there's any chance another writer has rotated in
// a new entry for the same path.
func (idx *SpoolIndex) Delete(path string) {
	idx.mu.Lock()
	delete(idx.byPath, path)
	idx.mu.Unlock()
}

// DeleteIfMatches removes path only if the current entry at that key is
// `want`. Returns true if the delete happened, false if the slot held a
// different entry (or no entry) — in which case the caller's entry was
// already replaced by a newer writer and is now an orphan from the
// index's perspective.
//
// Why this exists: CloseAndDelete used to call Delete unconditionally,
// which would stomp a freshly-inserted entry for the same path if a
// scrubber raced with a re-open. With this method, callers cleaning up a
// specific entry can never harm a live entry for the same path.
func (idx *SpoolIndex) DeleteIfMatches(path string, want *SpoolEntry) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if cur, ok := idx.byPath[path]; ok && cur == want {
		delete(idx.byPath, path)
		return true
	}
	return false
}

// Len returns the current number of entries. O(1).
func (idx *SpoolIndex) Len() int {
	idx.mu.RLock()
	n := len(idx.byPath)
	idx.mu.RUnlock()
	return n
}

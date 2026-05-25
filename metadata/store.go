package metadata

import (
	"database/sql"
	"fmt"
	"io/fs"
	"log"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS entries (
    path        TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    parent_path TEXT NOT NULL,
    is_dir      INTEGER NOT NULL,
    size        INTEGER NOT NULL,
    mtime       INTEGER NOT NULL,
    inode       INTEGER NOT NULL,
    mode        INTEGER NOT NULL,
    local_only  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_parent ON entries(parent_path);
CREATE INDEX IF NOT EXISTS idx_inode ON entries(inode);
CREATE INDEX IF NOT EXISTS idx_name ON entries(name COLLATE NOCASE);
`

// ftsSchema creates a full-text search virtual table for instant filename search.
// Uses FTS5 with a trigram tokenizer so partial matches work (e.g. "explosion" matches
// "Big_Explosion_4K.mov"). NO triggers — FTS is rebuilt manually after bulk operations
// and updated incrementally for individual inserts/deletes. This avoids the massive
// overhead of per-row trigram indexing during BulkInsert (131K entries).
const ftsSchema = `
CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(
    name,
    path,
    content='entries',
    content_rowid='rowid',
    tokenize='trigram'
);
`

// dropFTSTriggers removes any legacy FTS triggers from previous versions.
const dropFTSTriggers = `
DROP TRIGGER IF EXISTS entries_ai;
DROP TRIGGER IF EXISTS entries_ad;
DROP TRIGGER IF EXISTS entries_au;
`

const pragmas = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -8000;
PRAGMA busy_timeout = 30000;
`

// DefaultMaxCacheSize is the default maximum number of entries in the in-memory caches.
const DefaultMaxCacheSize = 500_000

// PinChecker is the minimal interface the metadata layer needs from the pin
// store. QA-30 (2026-05-25): pinned paths must NEVER be pruned from the
// metadata caches — pinning is an explicit user contract that the file
// remains offline-accessible, and dropping its cache entry causes kernel-
// cached NFS handles to surface as ESTALE (observed: DaVinci Resolve
// treating fully-cached media as offline mid-edit). Both syncMetadata's
// transient-SCAN-miss prune and Store.evictOldest's memory-pressure eviction
// consult this to filter pinned paths from any deletion candidate set.
//
// The interface is intentionally tiny and pull-based: callers fetch the
// pinned set once per pass and check against it in memory. Backed by a
// single SELECT against pinned_files; pin counts are small (<1000) so the
// cost is negligible per cycle.
//
// IMPORTANT: an error return from PinnedPaths is a FAIL-SAFE signal. The
// caller does not know what's pinned and MUST NOT proceed with any
// destructive operation (prune, eviction) that relies on the protection.
// Returning an empty set on error would unprotect every pinned file and
// re-introduce the exact ESTALE-on-pinned-media bug QA-30 closes.
type PinChecker interface {
	PinnedPaths() (map[string]struct{}, error)
}

// Store is a SQLite-backed metadata store with in-memory caches.
type Store struct {
	db           *sql.DB
	writeMu      sync.Mutex   // serializes all SQLite write operations (eliminates SQLITE_BUSY)
	mu           sync.RWMutex // protects in-memory caches
	inodeCache   map[uint64]*Entry
	pathCache    map[string]*Entry
	childrenIdx  map[string]map[string]*Entry // parentPath → {path → *Entry}
	maxCacheSize int
	pinChecker   PinChecker // optional (QA-30); see PinChecker docstring

	// QA-30 Layer B (2026-05-25): recently-evicted shadow map. When an entry
	// is removed from pathCache+inodeCache via Delete/DeleteFromCache/
	// DeletePaths or via evictOldest's rebuild, a copy of its scalar
	// metadata is parked here keyed by inode with a TTL. FromHandle in the
	// NFS handler consults this on cache-miss BEFORE returning ESTALE:
	// if the recently-evicted shadow has the inode AND FUSE confirms the
	// path still exists, the entry is re-inserted into pathCache+inodeCache
	// and the request is served normally. This is the safety net for any
	// stale-handle bug class Layers A+C don't catch.
	//
	// Bounded by ShadowTTL × eviction rate; pruned opportunistically when
	// new entries are added. Guarded by mu (same lock as pathCache).
	recentlyEvicted map[uint64]evictedShadow

	// QA-30 Layer B HIGH-2 (2026-05-25): pre-fetched pinned set for the
	// next evictOldest call. Set by stagePinnedForEviction BEFORE the
	// caller acquires mu; consumed and cleared by evictOldest. Guarded
	// by mu (write only happens after stagePinnedForEviction returns).
	// pinnedSetErr non-nil → evictOldest skips this cycle (fail-safe).
	pinnedSetForEviction map[string]struct{}
	pinnedSetErr         error
}

// stagePinnedForEviction is called by BulkInsert/rebuildCaches BEFORE they
// take s.mu, so the pin-store SQL query doesn't run while the NFS hot-path
// lock is held. The result is consumed by evictOldest on its next call
// from this caller (caller must call evictOldest while still holding mu).
//
// QA-30 Layer B HIGH-2 fix: previously evictOldest called pinnedSetLocked
// with mu held, blocking all NFS LookupByInode/LookupByPath until SQLite
// returned — up to 30 s under SQLite busy contention.
func (s *Store) stagePinnedForEviction() {
	pinned, err := s.pinnedSetPublic() // takes RLock briefly, NOT mu.Lock
	s.mu.Lock()
	s.pinnedSetForEviction = pinned
	s.pinnedSetErr = err
	s.mu.Unlock()
}

// ShadowTTL is the lifetime of recently-evicted shadow entries. 5 min is
// chosen to comfortably outlive any reasonable NFS-client handle-cache
// retry window — macOS NFS hangs onto handles for ~3 min by default,
// DaVinci's retry storms last seconds. After ShadowTTL the entry is
// dropped; if the kernel still uses the handle then, ESTALE fires
// (correct NFSv3 behavior, prompts client to re-LOOKUP).
const ShadowTTL = 5 * time.Minute

// evictedShadow captures just enough metadata to rebuild an Entry's cache
// presence if Layer B confirms the file is still on disk. Value-copy of
// scalars only — never aliases the original *Entry.
type evictedShadow struct {
	Path      string
	Name      string
	ParentPath string
	Mode      fs.FileMode
	Size      int64
	Mtime     time.Time
	IsDir     bool
	ExpiresAt time.Time
}

// shadowEvictedLocked parks a scalar copy of an entry being removed from
// the caches, keyed by inode, with a TTL. Caller must hold s.mu.Lock.
// Opportunistically expires any other shadow entries that are past TTL —
// keeps the map size bounded without a separate sweep goroutine.
//
// nil-safe: nil entry is a no-op (legitimate for Delete calls where the
// pre-fetch found nothing).
func (s *Store) shadowEvictedLocked(e *Entry) {
	if e == nil {
		return
	}
	if s.recentlyEvicted == nil {
		s.recentlyEvicted = make(map[uint64]evictedShadow, 64)
	}
	now := time.Now()
	// Opportunistic TTL sweep (cheap, bounded by map size).
	if len(s.recentlyEvicted) > 1024 {
		for k, v := range s.recentlyEvicted {
			if v.ExpiresAt.Before(now) {
				delete(s.recentlyEvicted, k)
			}
		}
	}
	s.recentlyEvicted[e.Inode] = evictedShadow{
		Path:       e.Path,
		Name:       e.Name,
		ParentPath: e.ParentPath,
		Mode:       e.Mode,
		Size:       e.Size,
		Mtime:      e.Mtime,
		IsDir:      e.IsDir,
		ExpiresAt:  now.Add(ShadowTTL),
	}
}

// LookupRecentlyEvicted returns the shadow record for `inode` if one
// exists and has not yet expired. Used by NFS handler's FromHandle on
// cache-miss to attempt recovery before returning ESTALE.
func (s *Store) LookupRecentlyEvicted(inode uint64) (evictedShadow, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.recentlyEvicted[inode]
	if !ok {
		return evictedShadow{}, false
	}
	if rec.ExpiresAt.Before(time.Now()) {
		return evictedShadow{}, false
	}
	return rec, true
}

// EvictedShadow is the public type alias for the shadow record. Renamed
// from evictedShadow without changing fields so external callers (NFS
// handler) can name the type by exported identifier.
type EvictedShadow = evictedShadow

// RecoverShadow promotes a recently-evicted shadow back into the
// pathCache + inodeCache. Used by FromHandle once it has confirmed via
// FUSE Lstat that the file still exists. Builds a new Entry from the
// shadow's scalar copy.
func (s *Store) RecoverShadow(rec evictedShadow, inode uint64) *Entry {
	e := &Entry{
		Path:       rec.Path,
		Name:       rec.Name,
		ParentPath: rec.ParentPath,
		IsDir:      rec.IsDir,
		Size:       rec.Size,
		Mtime:      rec.Mtime,
		Inode:      inode,
		Mode:       rec.Mode,
	}
	s.mu.Lock()
	if old, ok := s.pathCache[e.Path]; ok {
		s.removeFromChildrenIdx(old)
		s.evictPathOrphanLocked(old, e)
	}
	s.evictInodeOrphanLocked(e)
	s.inodeCache[e.Inode] = e
	s.pathCache[e.Path] = e
	s.addToChildrenIdx(e)
	// Clear the shadow now that the entry is live again.
	delete(s.recentlyEvicted, inode)
	s.mu.Unlock()
	return e
}

// SetPinChecker installs the pin-store backed pin checker. Nil-safe: callers
// (and tests) may pass nil, in which case the metadata layer falls back to
// "no pinned files" — same behavior as before QA-30.
//
// Concurrency (QA-30 code review HIGH-1): takes s.mu.Lock so we don't race
// with concurrent readers. The bridge wires this on the start path which
// runs after RedisClient.Start has already launched its reconcile goroutine,
// so the very first sync tick can otherwise interleave with this write.
func (s *Store) SetPinChecker(pc PinChecker) {
	s.mu.Lock()
	s.pinChecker = pc
	s.mu.Unlock()
}

// pinnedSetLocked returns the current pinned-path set under s.mu (caller
// already holds the lock, e.g. evictOldest). Returns nil + error to caller
// on DB failure; the caller MUST fail-safe (skip eviction this cycle).
func (s *Store) pinnedSetLocked() (map[string]struct{}, error) {
	pc := s.pinChecker
	if pc == nil {
		return map[string]struct{}{}, nil
	}
	return pc.PinnedPaths()
}

// pinnedSetPublic is the lock-free entry for in-package callers
// (RedisClient.syncMetadata) that do NOT already hold s.mu. Takes RLock to
// snapshot the pinChecker pointer safely, then queries outside the lock to
// avoid holding s.mu across a SQL call. Returns nil + error on failure;
// callers MUST fail-safe.
func (s *Store) pinnedSetPublic() (map[string]struct{}, error) {
	s.mu.RLock()
	pc := s.pinChecker
	s.mu.RUnlock()
	if pc == nil {
		return map[string]struct{}{}, nil
	}
	return pc.PinnedPaths()
}

// isPinned returns true if `p` is in the pinned-set map. Tiny helper to
// keep eviction/prune call sites readable.
func isPinned(set map[string]struct{}, p string) bool {
	_, ok := set[p]
	return ok
}

// Open creates or opens a SQLite metadata database at the given path.
// Use ":memory:" for an in-memory database (tests).
func Open(dbPath string) (*Store, error) {
	return OpenWithMaxCacheSize(dbPath, DefaultMaxCacheSize)
}

// OpenWithMaxCacheSize creates or opens a SQLite metadata database with a
// configurable maximum cache size. When the in-memory caches exceed this
// limit, the oldest entries (by mtime) are evicted.
func OpenWithMaxCacheSize(dbPath string, maxCacheSize int) (*Store, error) {
	// For in-memory databases with multiple connections, use shared cache
	// so all connections see the same database.
	if dbPath == ":memory:" {
		dbPath = "file::memory:?mode=memory&cache=shared"
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	db.SetMaxOpenConns(8)

	if _, err := db.Exec(pragmas); err != nil {
		db.Close()
		return nil, fmt.Errorf("set pragmas: %w", err)
	}

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}

	if _, err := db.Exec(ftsSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create FTS schema: %w", err)
	}

	// Drop legacy triggers (FTS is maintained manually for performance)
	if _, err := db.Exec(dropFTSTriggers); err != nil {
		db.Close()
		return nil, fmt.Errorf("drop FTS triggers: %w", err)
	}

	if maxCacheSize <= 0 {
		maxCacheSize = DefaultMaxCacheSize
	}

	s := &Store{
		db:           db,
		inodeCache:   make(map[uint64]*Entry),
		pathCache:    make(map[string]*Entry),
		childrenIdx:  make(map[string]map[string]*Entry),
		maxCacheSize: maxCacheSize,
	}

	if err := s.rebuildCaches(); err != nil {
		db.Close()
		return nil, fmt.Errorf("rebuild caches: %w", err)
	}

	// Rebuild FTS index on startup to ensure consistency with the entries table.
	// This is fast (<1s for 131K entries) and only runs once.
	if err := s.RebuildFTS(); err != nil {
		db.Close()
		return nil, fmt.Errorf("rebuild FTS: %w", err)
	}

	return s, nil
}

// Close closes the database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// Insert adds or replaces an entry in the store.
func (s *Store) Insert(e *Entry) error {
	s.writeMu.Lock()
	// Cast inode to int64 for SQLite compatibility (modernc.org/sqlite
	// rejects uint64 values with the high bit set).
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO entries (path, name, parent_path, is_dir, size, mtime, inode, mode, local_only)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.Path, e.Name, e.ParentPath,
		boolToInt(e.IsDir), e.Size, e.Mtime.Unix(),
		int64(e.Inode), uint32(e.Mode), boolToInt(e.LocalOnly),
	)
	s.writeMu.Unlock()

	if err != nil {
		return fmt.Errorf("insert %q: %w", e.Path, err)
	}

	s.mu.Lock()
	// Remove old entry from children index if path existed with different parent
	if old, ok := s.pathCache[e.Path]; ok {
		s.removeFromChildrenIdx(old)
		// QA-27 (2026-05-21): if the displaced entry had a different inode,
		// clean up its now-orphaned inodeCache mapping. See evictPathOrphanLocked.
		s.evictPathOrphanLocked(old, e)
	}
	// QA-25 (2026-05-20): juicefs preserves inode across rename. If another
	// path previously owned this inode, evict the stale orphan now so
	// pathCache/childrenIdx don't carry a ghost entry until reconcile prunes
	// it (60s window). Without this, ListChildren returns ghosts AND
	// evictOldest can rebuild inodeCache from the stale pathCache pointer,
	// re-creating the QA-25 stale-handle bug.
	s.evictInodeOrphanLocked(e)
	s.inodeCache[e.Inode] = e
	s.pathCache[e.Path] = e
	s.addToChildrenIdx(e)
	s.mu.Unlock()

	return nil
}

// Delete removes an entry by path.
func (s *Store) Delete(entryPath string) error {
	s.mu.RLock()
	e := s.pathCache[entryPath]
	s.mu.RUnlock()

	s.writeMu.Lock()
	_, err := s.db.Exec(`DELETE FROM entries WHERE path = ?`, entryPath)
	s.writeMu.Unlock()

	if err != nil {
		return fmt.Errorf("delete %q: %w", entryPath, err)
	}

	s.mu.Lock()
	if e != nil {
		// QA-25 (2026-05-20): only drop the inode mapping if it still points
		// to THIS entry. A concurrent rename (juicefs preserves inode) may
		// have re-assigned inodeCache[e.Inode] to a new-path entry between
		// our RLock pre-fetch and now; dropping it would orphan that new
		// path and make every cached NFS handle for it go STALE.
		if cur, ok := s.inodeCache[e.Inode]; ok && cur == e {
			delete(s.inodeCache, e.Inode)
		}
		s.removeFromChildrenIdx(e)
		// QA-30 Layer B: park a shadow so FromHandle can recover if FUSE
		// later proves the file actually exists. Cheap; bounded TTL.
		s.shadowEvictedLocked(e)
	}
	delete(s.pathCache, entryPath)
	s.mu.Unlock()

	return nil
}

// InsertToCache adds an entry to the in-memory cache without touching SQLite.
// This makes the entry immediately visible to NFS LOOKUP/GETATTR while the
// SQLite write may be blocked by a concurrent BulkInsert transaction.
func (s *Store) InsertToCache(e *Entry) {
	s.mu.Lock()
	if old, ok := s.pathCache[e.Path]; ok {
		s.removeFromChildrenIdx(old)
		// QA-27: see Insert.
		s.evictPathOrphanLocked(old, e)
	}
	// QA-25: see Insert.
	s.evictInodeOrphanLocked(e)
	s.inodeCache[e.Inode] = e
	s.pathCache[e.Path] = e
	s.addToChildrenIdx(e)
	s.mu.Unlock()
}

// DeleteFromCache removes an entry from the in-memory cache without touching SQLite.
func (s *Store) DeleteFromCache(entryPath string) {
	s.mu.Lock()
	if e, ok := s.pathCache[entryPath]; ok {
		// QA-25: see Delete.
		if cur, inodeOk := s.inodeCache[e.Inode]; inodeOk && cur == e {
			delete(s.inodeCache, e.Inode)
		}
		s.removeFromChildrenIdx(e)
		// QA-30 Layer B: shadow for possible recovery (see Delete).
		s.shadowEvictedLocked(e)
		delete(s.pathCache, entryPath)
	}
	s.mu.Unlock()
}

// evictInodeOrphanLocked removes a stale path entry that owned `new.Inode`
// under a different path. Caller must hold s.mu.Lock(). This is the QA-25
// proactive-cleanup half: when a rename inserts a new path entry whose inode
// already maps to a stale old-path entry, sweep the old path out of
// pathCache + childrenIdx in the same critical section so callers never see
// the ghost. Pairs with the pointer-equality guard in Delete*.
func (s *Store) evictInodeOrphanLocked(new *Entry) {
	prev, ok := s.inodeCache[new.Inode]
	if !ok || prev == nil || prev.Path == new.Path {
		return
	}
	s.removeFromChildrenIdx(prev)
	delete(s.pathCache, prev.Path)
}

// evictPathOrphanLocked is the symmetric counterpart to
// evictInodeOrphanLocked. Addresses two related bugs:
//
//   - QA-27 (2026-05-21): when a path gets reused with a different inode
//     (Finder delete+recreate of `._xxx` AppleDouble sidecars during a
//     folder copy; JuiceFS assigns a fresh inode each cycle), the old
//     entry's inodeCache mapping was left orphaned. 3,806 leaked entries
//     observed in one Editor Resource Vault copy.
//
//   - QA-28 (2026-05-21): a naive "delete the orphan" fix to QA-27
//     caused error code 100070 (ESTALE) mid-copy. Reason: synthetic
//     inodes created by ToHandle's fallback are handed to the kernel
//     as NFS file handles BEFORE the metadata sync replaces them with
//     real juicefs inodes. Deleting the synthetic mapping when the
//     real one arrives broke every in-flight kernel-cached handle —
//     Finder surfaces this as "operation can't be completed (100070)".
//
// Resolution: REDIRECT the old inode's mapping to the new entry rather
// than deleting it. The kernel's cached handle still resolves to the
// correct logical file. pathCache stays clean (caller overwrites it
// with `new` in the next line). Cost: inodeCache may carry alias
// entries for displaced inodes; bounded by the rate of inode-change
// events on shared paths and far below the 500k cache limit in
// realistic use. evictOldest rebuilds from pathCache so aliases
// eventually drop after enough churn — at which point any kernel
// handles still pointing at them will get a fresh ESTALE and the
// client will re-LOOKUP, which is the correct NFSv3 protocol behavior.
//
// Caller must hold s.mu.Lock(). Called BEFORE the new entry is written
// to pathCache; `old` here is `pathCache[new.Path]` at call time.
func (s *Store) evictPathOrphanLocked(old, new *Entry) {
	if old == nil || old.Inode == new.Inode {
		return
	}
	// Redirect only if the old entry is still the authoritative owner of
	// its inode — otherwise another path has already taken over and we
	// shouldn't overwrite that mapping.
	if cur, ok := s.inodeCache[old.Inode]; ok && cur == old {
		s.inodeCache[old.Inode] = new
	}
}

// LookupByInode returns the entry with the given inode, or nil.
func (s *Store) LookupByInode(inode uint64) *Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inodeCache[inode]
}

// LookupByPath returns the entry at the given path, or nil.
func (s *Store) LookupByPath(entryPath string) *Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pathCache[entryPath]
}

// ListChildren returns all entries whose parent_path matches the given path.
// Uses the children index for O(children) lookup instead of O(total entries).
func (s *Store) ListChildren(parentPath string) ([]*Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	children := s.childrenIdx[parentPath]
	if len(children) == 0 {
		return nil, nil
	}
	entries := make([]*Entry, 0, len(children))
	for _, e := range children {
		entries = append(entries, e)
	}
	return entries, nil
}

// BulkInsert inserts entries in batches within transactions.
// batchSize controls how many entries per batch. Defaults to 500 if <= 0.
//
// Locking note: each batch is its own writeMu acquisition. This lets
// concurrent NFS write-back paths (UpdateSize, Insert, applyEvent) and
// pin-store writes get their turn between batches. Previously the entire
// multi-batch loop held writeMu — on a 131K-entry initial sync that meant
// ~seconds of blocked NFS writes, which the user perceived as a frozen
// mount during the "just started" window. With per-batch locking the
// worst-case block is one batch's worth of SQL exec (~ms).
//
// Cache + FTS rebuild done at the very end is left holding mu.Lock for the
// full iteration — that's an in-memory operation, no I/O, and it only
// blocks readers (NFS LOOKUP/GETATTR) not writers, so it's a different
// failure mode that's mitigated by the cache rebuild itself being fast.
func (s *Store) BulkInsert(entries []*Entry, batchSize int) error {
	if batchSize <= 0 {
		batchSize = 500
	}

	for i := 0; i < len(entries); i += batchSize {
		end := i + batchSize
		if end > len(entries) {
			end = len(entries)
		}
		batch := entries[i:end]

		if err := s.bulkInsertBatch(batch); err != nil {
			return err
		}
	}

	// QA-30 Layer B HIGH-2: pre-fetch the pinned set BEFORE taking mu so
	// the SQLite query on pin.db doesn't block NFS LookupByInode while
	// the write lock is held.
	s.stagePinnedForEviction()

	// Rebuild caches after all batches insert
	s.mu.Lock()
	for _, e := range entries {
		if old, ok := s.pathCache[e.Path]; ok {
			s.removeFromChildrenIdx(old)
			// QA-27: see Insert.
			s.evictPathOrphanLocked(old, e)
		}
		// QA-25: see Insert.
		s.evictInodeOrphanLocked(e)
		s.inodeCache[e.Inode] = e
		s.pathCache[e.Path] = e
		s.addToChildrenIdx(e)
	}
	s.evictOldest()
	s.mu.Unlock()

	// Rebuild FTS index after bulk operation. This is much faster than
	// per-row trigger updates (~1s for 131K entries vs minutes with triggers).
	if err := s.RebuildFTS(); err != nil {
		// Stale FTS is recoverable, lost data isn't — the caller's rows
		// are already committed by the batch loop above. Log loudly so
		// the user (or a future debugger) can see the search index drifted
		// from the row store.
		log.Printf("[metadata] BulkInsert: RebuildFTS failed: %v", err)
	}

	return nil
}

// bulkInsertBatch runs one transaction holding writeMu only for its own
// duration. Extracted so the outer loop can release between batches.
func (s *Store) bulkInsertBatch(batch []*Entry) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	stmt, err := tx.Prepare(
		`INSERT OR REPLACE INTO entries (path, name, parent_path, is_dir, size, mtime, inode, mode, local_only)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare: %w", err)
	}

	for _, e := range batch {
		_, err := stmt.Exec(
			e.Path, e.Name, e.ParentPath,
			boolToInt(e.IsDir), e.Size, e.Mtime.Unix(),
			int64(e.Inode), uint32(e.Mode), boolToInt(e.LocalOnly),
		)
		if err != nil {
			stmt.Close()
			tx.Rollback()
			return fmt.Errorf("exec %q: %w", e.Path, err)
		}
	}

	stmt.Close()
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// UpdateSize updates the size and mtime for an entry identified by path.
//
// QA-16 fix (2026-05-17): MAX semantics on size. Under concurrent NFS
// WRITE RPC dispatch, multiple writeFile.Close() calls land here for
// the same path with each RPC's view of the in-flight high-water mark.
// Without MAX, a Close that observes a stale (lower) value and runs
// last would shrink the stored size, even though earlier RPCs already
// wrote past it. SQLite's metadata then mis-reports the file as
// smaller and NFS readbacks/stats are truncated. mtime is always
// updated to the most-recent call so "newest modification wins" still
// holds.
//
// Truncate(2) does not go through this code path (writeFile.Close is
// the only caller); explicit size shrinks come from a SETATTR path
// that resets the SQLite row directly. So MAX here is safe for the
// real-world write workflow.
func (s *Store) UpdateSize(entryPath string, size int64, mtime time.Time) error {
	s.writeMu.Lock()
	_, err := s.db.Exec(
		`UPDATE entries SET size = MAX(size, ?), mtime = ? WHERE path = ?`,
		size, mtime.Unix(), entryPath,
	)
	s.writeMu.Unlock()

	if err != nil {
		return fmt.Errorf("update size %q: %w", entryPath, err)
	}

	s.mu.Lock()
	if e, ok := s.pathCache[entryPath]; ok {
		if size > e.Size {
			e.Size = size
		}
		e.Mtime = mtime
		e.PreSerializedGetAttr = nil // [JM5] invalidate cached XDR bytes
	}
	s.mu.Unlock()

	return nil
}

// ClearLocalOnly marks an entry as no longer local-only (confirmed in Redis).
func (s *Store) ClearLocalOnly(entryPath string) error {
	s.writeMu.Lock()
	_, err := s.db.Exec(
		`UPDATE entries SET local_only = 0 WHERE path = ?`, entryPath,
	)
	s.writeMu.Unlock()

	if err != nil {
		return fmt.Errorf("clear local_only %q: %w", entryPath, err)
	}

	s.mu.Lock()
	if e, ok := s.pathCache[entryPath]; ok {
		e.LocalOnly = false
	}
	s.mu.Unlock()

	return nil
}

// BulkClearLocalOnly clears the local_only flag for all given paths in a
// single transaction. Much faster than calling ClearLocalOnly individually
// for each path (e.g. 146K entries after a full reconciliation).
func (s *Store) BulkClearLocalOnly(paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	batchSize := 500
	s.writeMu.Lock()
	for i := 0; i < len(paths); i += batchSize {
		end := i + batchSize
		if end > len(paths) {
			end = len(paths)
		}
		batch := paths[i:end]

		tx, err := s.db.Begin()
		if err != nil {
			s.writeMu.Unlock()
			return fmt.Errorf("begin tx: %w", err)
		}

		stmt, err := tx.Prepare(`UPDATE entries SET local_only = 0 WHERE path = ?`)
		if err != nil {
			tx.Rollback()
			s.writeMu.Unlock()
			return fmt.Errorf("prepare: %w", err)
		}

		for _, p := range batch {
			if _, err := stmt.Exec(p); err != nil {
				stmt.Close()
				tx.Rollback()
				s.writeMu.Unlock()
				return fmt.Errorf("exec clear local_only %q: %w", p, err)
			}
		}

		stmt.Close()
		if err := tx.Commit(); err != nil {
			s.writeMu.Unlock()
			return fmt.Errorf("commit: %w", err)
		}
	}
	s.writeMu.Unlock()

	// Update in-memory cache
	s.mu.Lock()
	for _, p := range paths {
		if e, ok := s.pathCache[p]; ok {
			e.LocalOnly = false
		}
	}
	s.mu.Unlock()

	return nil
}

// LocalOnlyEntries returns all entries marked as local_only.
func (s *Store) LocalOnlyEntries() ([]*Entry, error) {
	rows, err := s.db.Query(
		`SELECT path, name, parent_path, is_dir, size, mtime, inode, mode, local_only
		 FROM entries WHERE local_only = 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*Entry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// Count returns the total number of entries.
func (s *Store) Count() (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM entries`).Scan(&count)
	return count, err
}

// AllPaths returns every path in the store (used for reconciliation diffing).
func (s *Store) AllPaths() (map[string]struct{}, error) {
	rows, err := s.db.Query(`SELECT path FROM entries WHERE local_only = 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	paths := make(map[string]struct{})
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		paths[p] = struct{}{}
	}
	return paths, rows.Err()
}

// SearchResult is a single search hit with a relevance rank.
type SearchResult struct {
	Entry *Entry
	Rank  float64 // FTS5 rank (lower = more relevant)
}

// Search performs a full-text search on filenames using the FTS5 trigram index.
// Returns up to `limit` results ordered by relevance. The query matches partial
// substrings (e.g. "explo" matches "Big_Explosion_4K.mov").
// Pass an empty parentPath to search the entire tree, or a path to scope results
// to a subtree (e.g. "SFX/Impacts").
func (s *Store) Search(query string, limit int, parentPath string) ([]SearchResult, error) {
	if query == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 50
	}

	// FTS5 trigram tokenizer supports substring matching with double quotes.
	// Escape any existing double quotes in the query.
	ftsQuery := `"` + strings.ReplaceAll(query, `"`, `""`) + `"`

	var rows *sql.Rows
	var err error

	if parentPath != "" {
		// Scoped search: only entries under parentPath
		rows, err = s.db.Query(
			`SELECT e.path, e.name, e.parent_path, e.is_dir, e.size, e.mtime, e.inode, e.mode, e.local_only, rank
			 FROM entries_fts fts
			 JOIN entries e ON e.rowid = fts.rowid
			 WHERE entries_fts MATCH ?
			   AND e.path LIKE ?
			 ORDER BY rank
			 LIMIT ?`,
			ftsQuery, parentPath+"/%", limit,
		)
	} else {
		rows, err = s.db.Query(
			`SELECT e.path, e.name, e.parent_path, e.is_dir, e.size, e.mtime, e.inode, e.mode, e.local_only, rank
			 FROM entries_fts fts
			 JOIN entries e ON e.rowid = fts.rowid
			 WHERE entries_fts MATCH ?
			 ORDER BY rank
			 LIMIT ?`,
			ftsQuery, limit,
		)
	}

	if err != nil {
		return nil, fmt.Errorf("search %q: %w", query, err)
	}
	defer rows.Close()

	var results []SearchResult
	for rows.Next() {
		var (
			e         Entry
			isDir     int
			mtimeUnix int64
			inodeRaw  int64
			mode      uint32
			localOnly int
			rank      float64
		)
		err := rows.Scan(&e.Path, &e.Name, &e.ParentPath, &isDir, &e.Size, &mtimeUnix, &inodeRaw, &mode, &localOnly, &rank)
		if err != nil {
			return nil, err
		}
		e.Inode = uint64(inodeRaw)
		e.IsDir = isDir != 0
		e.Mtime = time.Unix(mtimeUnix, 0)
		e.Mode = fs.FileMode(mode)
		if e.IsDir {
			e.Mode |= fs.ModeDir
		}
		e.LocalOnly = localOnly != 0
		results = append(results, SearchResult{Entry: &e, Rank: rank})
	}
	return results, rows.Err()
}

// RebuildFTS rebuilds the FTS5 index from the entries table.
// Call this after a bulk data load where triggers may not have fired
// (e.g. initial startup with a pre-existing database).
func (s *Store) RebuildFTS() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	// Delete all FTS content, then re-insert from entries table
	if _, err := s.db.Exec(`INSERT INTO entries_fts(entries_fts) VALUES('delete-all')`); err != nil {
		return fmt.Errorf("fts delete-all: %w", err)
	}
	if _, err := s.db.Exec(`INSERT INTO entries_fts(rowid, name, path) SELECT rowid, name, path FROM entries`); err != nil {
		return fmt.Errorf("fts rebuild: %w", err)
	}
	return nil
}

// DeletePaths removes multiple entries by path in a single transaction.
func (s *Store) DeletePaths(paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	s.writeMu.Lock()
	tx, err := s.db.Begin()
	if err != nil {
		s.writeMu.Unlock()
		return err
	}

	stmt, err := tx.Prepare(`DELETE FROM entries WHERE path = ?`)
	if err != nil {
		tx.Rollback()
		s.writeMu.Unlock()
		return err
	}

	for _, p := range paths {
		if _, err := stmt.Exec(p); err != nil {
			stmt.Close()
			tx.Rollback()
			s.writeMu.Unlock()
			return err
		}
	}
	stmt.Close()

	if err := tx.Commit(); err != nil {
		s.writeMu.Unlock()
		return err
	}
	s.writeMu.Unlock()

	s.mu.Lock()
	for _, p := range paths {
		if e, ok := s.pathCache[p]; ok {
			// QA-25: see Delete.
			if cur, inodeOk := s.inodeCache[e.Inode]; inodeOk && cur == e {
				delete(s.inodeCache, e.Inode)
			}
			s.removeFromChildrenIdx(e)
			// QA-30 Layer B: shadow for possible recovery (see Delete).
			s.shadowEvictedLocked(e)
			delete(s.pathCache, p)
		}
	}
	s.mu.Unlock()

	return nil
}

// CacheStats returns the current size of the path and inode caches.
func (s *Store) CacheStats() (pathCacheSize, inodeCacheSize int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.pathCache), len(s.inodeCache)
}

// evictOldest trims the caches to maxCacheSize by removing the entries
// with the oldest mtime. Must be called with s.mu held for writing.
//
// QA-18 defense (2026-05-17): directories are always retained. Losing
// a file entry to LRU just means the next access fabricates a fresh
// synthetic handle via ToHandle; harmless. Losing a DIRECTORY entry
// is high-impact: ToHandle's fallback historically hardcoded IsDir=false
// (now Lstat-corrected, but still slower), and any in-flight LOOKUP
// for the dir in the eviction window can race with the re-fabrication.
// Dir count is typically O(thousands) vs file count O(millions), so
// the memory cost of pinning all dirs is negligible.
func (s *Store) evictOldest() {
	if s.maxCacheSize <= 0 || len(s.pathCache) <= s.maxCacheSize {
		return
	}

	// Split entries: directories always retained, files sorted by mtime.
	// QA-30 (2026-05-25): pinned files are also always retained, regardless
	// of mtime budget. Evicting a pinned file's entry causes its kernel-
	// cached NFS handle to surface as ESTALE on next access — DaVinci
	// treats the media as offline mid-edit. Pinning is the user's explicit
	// contract that the file stay available; honoring it through eviction
	// is non-negotiable.
	//
	// QA-30 Layer B code review HIGH-2 (2026-05-25): the pinned set is
	// PRE-FETCHED by the caller (BulkInsert/rebuildCaches) BEFORE the
	// caller acquired s.mu — passed in via s.pinnedSetForEviction. Holding
	// s.mu (the hot NFS-cache lock) across a SQLite query against the
	// pin store could stall every concurrent LookupByInode/LookupByPath
	// up to 30 s (SQLite busy_timeout). The pre-fetch sidesteps that.
	//
	// Fail-safe on pin-checker error: if the pre-fetch failed (caller set
	// pinnedSetErr), SKIP eviction entirely rather than risk evicting
	// pinned entries. The cache will exceed maxCacheSize for one cycle;
	// the next BulkInsert/rebuild retries.
	if s.pinnedSetErr != nil {
		log.Printf("[metadata] evictOldest: pin-checker error, skipping eviction this cycle: %v", s.pinnedSetErr)
		s.pinnedSetForEviction = nil
		s.pinnedSetErr = nil
		return
	}
	pinned := s.pinnedSetForEviction
	if pinned == nil {
		// Caller didn't pre-fetch (legacy path or test). Fail-safe.
		pinned = map[string]struct{}{}
	}
	s.pinnedSetForEviction = nil
	dirs := make([]*Entry, 0, len(s.pathCache)/16)
	pinnedFiles := make([]*Entry, 0, len(pinned))
	files := make([]*Entry, 0, len(s.pathCache))
	for _, e := range s.pathCache {
		switch {
		case e.IsDir:
			dirs = append(dirs, e)
		case isPinned(pinned, e.Path):
			pinnedFiles = append(pinnedFiles, e)
		default:
			files = append(files, e)
		}
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Mtime.After(files[j].Mtime)
	})

	// Budget: keep all dirs + all pinned files + as many newest non-pinned
	// files as fit. If dirs+pinned alone exceed maxCacheSize (pathological),
	// keep them all anyway (the alternative — evicting them — re-introduces
	// the bug we're trying to prevent).
	mandatory := len(dirs) + len(pinnedFiles)
	fileBudget := s.maxCacheSize - mandatory
	if fileBudget < 0 {
		fileBudget = 0
	}
	if fileBudget > len(files) {
		fileBudget = len(files)
	}

	newPathCache := make(map[string]*Entry, mandatory+fileBudget)
	newInodeCache := make(map[uint64]*Entry, mandatory+fileBudget)
	newChildrenIdx := make(map[string]map[string]*Entry)
	addEntry := func(e *Entry) {
		newPathCache[e.Path] = e
		newInodeCache[e.Inode] = e
		children, ok := newChildrenIdx[e.ParentPath]
		if !ok {
			children = make(map[string]*Entry)
			newChildrenIdx[e.ParentPath] = children
		}
		children[e.Path] = e
	}
	for _, e := range dirs {
		addEntry(e)
	}
	for _, e := range pinnedFiles {
		addEntry(e)
	}
	for i := 0; i < fileBudget; i++ {
		addEntry(files[i])
	}

	// QA-30 Layer B: shadow every entry that was dropped by this eviction
	// so FromHandle can recover if FUSE proves they still exist. Iterate
	// the OLD pathCache (still in scope until we replace it) and shadow
	// anything that didn't make it into newPathCache.
	for path, e := range s.pathCache {
		if _, kept := newPathCache[path]; !kept {
			s.shadowEvictedLocked(e)
		}
	}

	s.pathCache = newPathCache
	s.inodeCache = newInodeCache
	s.childrenIdx = newChildrenIdx
}

// rebuildCaches loads all entries from SQLite into the in-memory caches.
func (s *Store) rebuildCaches() error {
	rows, err := s.db.Query(
		`SELECT path, name, parent_path, is_dir, size, mtime, inode, mode, local_only FROM entries`)
	if err != nil {
		return err
	}
	defer rows.Close()

	iCache := make(map[uint64]*Entry)
	pCache := make(map[string]*Entry)
	cIdx := make(map[string]map[string]*Entry)

	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return err
		}
		iCache[e.Inode] = e
		pCache[e.Path] = e
		children, ok := cIdx[e.ParentPath]
		if !ok {
			children = make(map[string]*Entry)
			cIdx[e.ParentPath] = children
		}
		children[e.Path] = e
	}

	// QA-30 Layer B HIGH-2: pre-fetch pinned set outside the lock.
	s.stagePinnedForEviction()

	s.mu.Lock()
	s.inodeCache = iCache
	s.pathCache = pCache
	s.childrenIdx = cIdx
	s.evictOldest()
	s.mu.Unlock()

	return rows.Err()
}

// MakeEntry is a convenience constructor.
func MakeEntry(entryPath string, isDir bool, size int64, mtime time.Time, inode uint64) *Entry {
	var mode fs.FileMode = 0644
	if isDir {
		mode = 0755 | fs.ModeDir
	}
	return &Entry{
		Path:       entryPath,
		Name:       path.Base(entryPath),
		ParentPath: path.Dir(entryPath),
		IsDir:      isDir,
		Size:       size,
		Mtime:      mtime,
		Inode:      inode,
		Mode:       mode,
	}
}

type scanner interface {
	Scan(dest ...any) error
}

func scanEntry(s scanner) (*Entry, error) {
	var (
		e         Entry
		isDir     int
		mtimeUnix int64
		inodeRaw  int64 // SQLite stores uint64 as int64; high-bit inodes become negative
		mode      uint32
		localOnly int
	)
	err := s.Scan(&e.Path, &e.Name, &e.ParentPath, &isDir, &e.Size, &mtimeUnix, &inodeRaw, &mode, &localOnly)
	if err != nil {
		return nil, err
	}
	e.Inode = uint64(inodeRaw) // reinterpret bits — negative int64 → high-bit uint64
	e.IsDir = isDir != 0
	e.Mtime = time.Unix(mtimeUnix, 0)
	e.Mode = fs.FileMode(mode)
	if e.IsDir {
		e.Mode |= fs.ModeDir
	}
	e.LocalOnly = localOnly != 0
	return &e, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// addToChildrenIdx adds an entry to the children index. Must be called with s.mu held for writing.
func (s *Store) addToChildrenIdx(e *Entry) {
	children, ok := s.childrenIdx[e.ParentPath]
	if !ok {
		children = make(map[string]*Entry)
		s.childrenIdx[e.ParentPath] = children
	}
	children[e.Path] = e
}

// removeFromChildrenIdx removes an entry from the children index. Must be called with s.mu held for writing.
func (s *Store) removeFromChildrenIdx(e *Entry) {
	if children, ok := s.childrenIdx[e.ParentPath]; ok {
		delete(children, e.Path)
		if len(children) == 0 {
			delete(s.childrenIdx, e.ParentPath)
		}
	}
}

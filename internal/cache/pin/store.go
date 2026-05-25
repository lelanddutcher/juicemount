// Package pin manages the pinned-paths registry and the prefetcher that
// keeps those paths warm in JuiceFS's local SSD cache.
//
// Mental model:
//   - User calls Pin("/Volumes/zpool/Project_Foo") — recursive.
//   - We persist every individual file path to SQLite as "pinned".
//   - The Prefetcher reads each pinned path through the FUSE mount, which
//     causes JuiceFS to populate its own LRU cache.
//   - A periodic re-warmup worker re-reads pinned paths every ~6 hours so
//     they don't fall off the LRU under cache pressure.
//   - Unpin removes the registry entry and lets natural eviction happen.
//
// We do NOT modify JuiceFS's cache directory directly. We're a layer on top.
package pin

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Status describes where a pinned path is in its lifecycle.
type Status int

const (
	StatusUnknown   Status = iota
	StatusPending          // pinned but not yet prefetched
	StatusPrefetching      // currently being read into cache
	StatusReady            // bytes confirmed cached
	StatusFailed           // last prefetch attempt failed
	StatusUnpinned         // user removed; eligible for eviction
)

func (s Status) String() string {
	switch s {
	case StatusPending:     return "pending"
	case StatusPrefetching: return "prefetching"
	case StatusReady:       return "ready"
	case StatusFailed:      return "failed"
	case StatusUnpinned:    return "unpinned"
	default:                return "unknown"
	}
}

// Entry is a single pinned file's record.
type Entry struct {
	Path           string
	Size           int64
	Status         Status
	BytesCached    int64
	LastPrefetched time.Time
	LastError      string
	PinnedAt       time.Time
	PinRoot        string // the directory the user pinned (so Unpin can find siblings)
}

const schema = `
CREATE TABLE IF NOT EXISTS pinned_files (
    path             TEXT PRIMARY KEY,
    size             INTEGER NOT NULL,
    status           INTEGER NOT NULL,
    bytes_cached     INTEGER NOT NULL DEFAULT 0,
    last_prefetched  INTEGER NOT NULL DEFAULT 0,
    last_error       TEXT NOT NULL DEFAULT '',
    pinned_at        INTEGER NOT NULL,
    pin_root         TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_pinned_status ON pinned_files(status);
CREATE INDEX IF NOT EXISTS idx_pinned_root ON pinned_files(pin_root);
`

// Store is a thread-safe SQLite-backed pinned-paths registry.
type Store struct {
	db    *sql.DB
	mu    sync.RWMutex
}

// Open creates or opens the pin store at the given path. Use ":memory:" for
// tests. The DB file can be the same one as the metadata store or a separate
// file; pinned_files is its own table.
func Open(dbPath string) (*Store, error) {
	if dbPath == ":memory:" {
		dbPath = "file::memory:?mode=memory&cache=shared"
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// busy_timeout matches the metadata store. Without it, a concurrent
	// IsPinnedReady on the hot offline-gate path returns "database is
	// locked" the instant a Pin/UpdateStatus transaction begins. With
	// 30 000 ms the reader waits for the writer to finish (typically
	// <50 ms) instead of erroring up to the NFS handler, which returns
	// EIO to the kernel and looks like "the mount is broken."
	if _, err := db.Exec(`PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 30000;`); err != nil {
		db.Close()
		return nil, fmt.Errorf("set pragmas: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Pin adds a single file to the registry. Idempotent; existing entries with
// the same path are updated to Pending status (re-queue them for prefetch).
func (s *Store) Pin(path string, size int64, root string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	_, err := s.db.Exec(`
		INSERT INTO pinned_files (path, size, status, pinned_at, pin_root)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET
		    size = excluded.size,
		    status = ?,
		    pin_root = excluded.pin_root`,
		path, size, int(StatusPending), now, root,
		int(StatusPending),
	)
	if err != nil {
		return fmt.Errorf("pin %q: %w", path, err)
	}
	return nil
}

// PinMany pins a batch of paths in a single transaction. Much faster than
// looping Pin() when adding hundreds/thousands of files at once.
func (s *Store) PinMany(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`
		INSERT INTO pinned_files (path, size, status, pinned_at, pin_root)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET
		    size = excluded.size,
		    status = ?,
		    pin_root = excluded.pin_root`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	now := time.Now().UnixMilli()
	for _, e := range entries {
		_, err := stmt.Exec(
			e.Path, e.Size, int(StatusPending), now, e.PinRoot,
			int(StatusPending),
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("pin %q: %w", e.Path, err)
		}
	}
	return tx.Commit()
}

// Unpin removes paths under root (everything pinned with this root or
// matching it as a prefix). Returns count removed.
func (s *Store) Unpin(root string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.Exec(`DELETE FROM pinned_files WHERE pin_root = ? OR path = ?`,
		root, root)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// UnpinPath removes a single path entry.
func (s *Store) UnpinPath(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`DELETE FROM pinned_files WHERE path = ?`, path)
	return err
}

// UpdateStatus is called by the prefetcher to record progress.
func (s *Store) UpdateStatus(path string, status Status, bytesCached int64, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	_, err := s.db.Exec(`
		UPDATE pinned_files
		SET status = ?, bytes_cached = ?, last_prefetched = ?, last_error = ?
		WHERE path = ?`,
		int(status), bytesCached, now, errMsg, path)
	return err
}

// IsPinnedReady checks whether a single path is "good enough" to be
// served while offline mode is on. A path qualifies if either:
//
//   1. status = Ready, OR
//   2. status = Prefetching AND bytes_cached >= size — i.e. the prefetcher
//      has finished pulling bytes but hasn't flipped the status row yet.
//      This eliminates a small (~1s) window where a fully-cached file
//      would otherwise be refused after the user toggles offline ON.
//
// Hot-path-friendly — uses the path primary key. Called on every NFS
// OpenFile when offline mode is on.
//
// Note: Pending files are intentionally refused — the user explicitly
// asked for fail-fast, and Pending means we know we don't have the bytes.
// Promoting Pending here would re-introduce the very freeze (FUSE →
// backend → timeout) the offline gate exists to prevent.
func (s *Store) IsPinnedReady(path string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var statusInt int
	var size, bytesCached int64
	err := s.db.QueryRow(
		`SELECT status, size, bytes_cached FROM pinned_files WHERE path = ?`,
		path,
	).Scan(&statusInt, &size, &bytesCached)
	if err != nil {
		return false
	}
	st := Status(statusInt)
	if st == StatusReady {
		return true
	}
	// Late-Ready window: prefetcher has the bytes but hasn't updated status yet.
	if st == StatusPrefetching && size > 0 && bytesCached >= size {
		return true
	}
	return false
}

// PinnedPaths returns the set of all paths in the pin store, regardless of
// status (Pending, Prefetching, Ready, Failed). Used by the metadata layer's
// prune and eviction logic to enforce the invariant that a pinned path is
// NEVER pruned from the metadata caches — pinning is an explicit user
// contract that the file should remain offline-accessible. Bounded by the
// number of pins (typically <1000); cheap to fetch on demand.
//
// Returns an explicit error on DB failure (QA-30 code review HIGH-2): callers
// MUST treat an error as "I don't know what's pinned" and fail-safe — i.e.
// SKIP any prune or eviction pass that depended on this set. Silently
// returning an empty map would re-introduce the very ESTALE-on-pinned-media
// bug QA-30 was created to close (a SQLite hiccup would unprotect every
// pinned file).
func (s *Store) PinnedPaths() (map[string]struct{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query(`SELECT path FROM pinned_files`)
	if err != nil {
		return nil, fmt.Errorf("pin: query pinned paths: %w", err)
	}
	defer rows.Close()
	out := make(map[string]struct{}, 256)
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, fmt.Errorf("pin: scan pinned path: %w", err)
		}
		out[p] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pin: iterate pinned paths: %w", err)
	}
	return out, nil
}

// Pending returns up to limit entries waiting for prefetch.
func (s *Store) Pending(limit int) ([]Entry, error) {
	return s.queryStatus(StatusPending, limit)
}

// Stale returns entries whose last_prefetched is older than ttl, suitable
// for re-warmup. Used by the periodic re-warmer to prevent eviction.
func (s *Store) Stale(ttl time.Duration, limit int) ([]Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cutoff := time.Now().Add(-ttl).UnixMilli()
	rows, err := s.db.Query(`
		SELECT path, size, status, bytes_cached, last_prefetched, last_error, pinned_at, pin_root
		FROM pinned_files
		WHERE status = ? AND last_prefetched < ?
		ORDER BY last_prefetched ASC
		LIMIT ?`,
		int(StatusReady), cutoff, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEntries(rows)
}

// AllPinnedForRepair returns every entry the user has pinned that we
// might want to re-prefetch — Ready, Prefetching, AND Failed. Pending is
// excluded (the worker pool is already going to pick it up). The verify
// flow uses this to re-attempt files that errored on a previous run
// (commonly: FUSE was momentarily unmounted right after a restart, every
// open() returned ENOENT, all the entries got marked Failed, and the
// user has no obvious way to retry them short of unpinning and re-pinning).
func (s *Store) AllPinnedForRepair(limit int) ([]Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query(`
		SELECT path, size, status, bytes_cached, last_prefetched, last_error, pinned_at, pin_root
		FROM pinned_files
		WHERE status IN (?, ?, ?)
		ORDER BY last_prefetched ASC
		LIMIT ?`,
		int(StatusReady), int(StatusPrefetching), int(StatusFailed), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEntries(rows)
}

func (s *Store) queryStatus(status Status, limit int) ([]Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query(`
		SELECT path, size, status, bytes_cached, last_prefetched, last_error, pinned_at, pin_root
		FROM pinned_files
		WHERE status = ?
		ORDER BY pinned_at ASC
		LIMIT ?`,
		int(status), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEntries(rows)
}

// All returns every pinned entry. Use for status reports; not the hot path.
func (s *Store) All() ([]Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query(`
		SELECT path, size, status, bytes_cached, last_prefetched, last_error, pinned_at, pin_root
		FROM pinned_files
		ORDER BY pin_root, path`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEntries(rows)
}

// PinRoots returns the distinct pin roots and aggregate stats per root.
type RootSummary struct {
	Root         string
	TotalFiles   int
	ReadyFiles   int
	PendingFiles int
	FailedFiles  int
	TotalBytes   int64
	CachedBytes  int64
}

func (s *Store) PinRoots() ([]RootSummary, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query(`
		SELECT pin_root,
		    COUNT(*) AS total,
		    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) AS ready,
		    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) AS pending,
		    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) AS failed,
		    SUM(size) AS total_bytes,
		    SUM(bytes_cached) AS cached_bytes
		FROM pinned_files
		WHERE pin_root != ''
		GROUP BY pin_root
		ORDER BY pin_root`,
		int(StatusReady), int(StatusPending), int(StatusFailed))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RootSummary
	for rows.Next() {
		var r RootSummary
		var total, ready, pending, failed sql.NullInt64
		var totalBytes, cachedBytes sql.NullInt64
		if err := rows.Scan(&r.Root, &total, &ready, &pending, &failed, &totalBytes, &cachedBytes); err != nil {
			return nil, err
		}
		r.TotalFiles = int(total.Int64)
		r.ReadyFiles = int(ready.Int64)
		r.PendingFiles = int(pending.Int64)
		r.FailedFiles = int(failed.Int64)
		r.TotalBytes = totalBytes.Int64
		r.CachedBytes = cachedBytes.Int64
		out = append(out, r)
	}
	return out, rows.Err()
}

// AggregateStats returns whole-database counters.
type AggregateStats struct {
	TotalFiles    int
	ReadyFiles    int
	PendingFiles  int
	FailedFiles   int
	TotalBytes    int64
	CachedBytes   int64
}

func (s *Store) AggregateStats() (AggregateStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var a AggregateStats
	row := s.db.QueryRow(`
		SELECT COUNT(*),
		    COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
		    COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
		    COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
		    COALESCE(SUM(size), 0),
		    COALESCE(SUM(bytes_cached), 0)
		FROM pinned_files`,
		int(StatusReady), int(StatusPending), int(StatusFailed))
	if err := row.Scan(&a.TotalFiles, &a.ReadyFiles, &a.PendingFiles, &a.FailedFiles, &a.TotalBytes, &a.CachedBytes); err != nil {
		return a, err
	}
	return a, nil
}

func scanEntries(rows *sql.Rows) ([]Entry, error) {
	var out []Entry
	for rows.Next() {
		var e Entry
		var statusInt int
		var lastPre int64
		var pinnedAt int64
		if err := rows.Scan(&e.Path, &e.Size, &statusInt, &e.BytesCached, &lastPre, &e.LastError, &pinnedAt, &e.PinRoot); err != nil {
			return nil, err
		}
		e.Status = Status(statusInt)
		e.LastPrefetched = time.UnixMilli(lastPre)
		e.PinnedAt = time.UnixMilli(pinnedAt)
		out = append(out, e)
	}
	return out, rows.Err()
}

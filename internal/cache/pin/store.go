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
	"bytes"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Status describes where a pinned path is in its lifecycle.
type Status int

const (
	StatusUnknown     Status = iota
	StatusPending            // pinned but not yet prefetched
	StatusPrefetching        // currently being read into cache
	StatusReady              // bytes confirmed cached
	StatusFailed             // last prefetch attempt failed
	StatusUnpinned           // user removed; eligible for eviction
)

func (s Status) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusPrefetching:
		return "prefetching"
	case StatusReady:
		return "ready"
	case StatusFailed:
		return "failed"
	case StatusUnpinned:
		return "unpinned"
	default:
		return "unknown"
	}
}

// Entry is a single pinned file's record.
type Entry struct {
	Path           string
	Generation     string // durable random incarnation; never inherited by a repin
	Size           int64
	Status         Status
	BytesCached    int64
	LastPrefetched time.Time
	LastError      string
	PinnedAt       time.Time
	PinRoot        string // the directory the user pinned (so Unpin can find siblings)
	Authority      []byte // opaque durable producer witness; never inferred from Status alone
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
    pin_root         TEXT NOT NULL DEFAULT '',
    pin_generation   TEXT NOT NULL DEFAULT '',
    authority        BLOB NOT NULL DEFAULT X''
);
CREATE INDEX IF NOT EXISTS idx_pinned_status ON pinned_files(status);
CREATE INDEX IF NOT EXISTS idx_pinned_root ON pinned_files(pin_root);
`

// Store is a thread-safe SQLite-backed pinned-paths registry.
type Store struct {
	db *sql.DB
	mu sync.RWMutex

	// Only producer-finalized or locally restored witnesses enter this RAM
	// index. SQLite StatusReady alone and old Prefetching byte counts cannot
	// authorize an NFS open. IsPinnedReady performs no SQL or remote I/O.
	readyMu        sync.RWMutex
	ready          map[string]Entry     // admitted generation and witness, never a raw SQL Ready row
	readyValidator func(Entry) bool     // immutable once installed; RAM-only and no Redis/SQL
	readyRevoker   func(string, string) // exact-generation provider cleanup, after readyMu unlock
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
	if err := migratePinGenerations(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func newPinGeneration() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

// Existing pre-incarnation rows cannot authorize a still-running old worker.
// Give each a new durable token and conservatively queue it for recheck.
func migratePinGenerations(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(pinned_files)`)
	if err != nil {
		return err
	}
	hasGeneration := false
	hasAuthority := false
	for rows.Next() {
		var id, notNull, pk int
		var name, kind string
		var defaultValue any
		if err := rows.Scan(&id, &name, &kind, &notNull, &defaultValue, &pk); err != nil {
			rows.Close()
			return err
		}
		if name == "pin_generation" {
			hasGeneration = true
		}
		if name == "authority" {
			hasAuthority = true
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if !hasGeneration {
		if _, err := db.Exec(`ALTER TABLE pinned_files ADD COLUMN pin_generation TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	if !hasAuthority {
		if _, err := db.Exec(`ALTER TABLE pinned_files ADD COLUMN authority BLOB NOT NULL DEFAULT X''`); err != nil {
			return err
		}
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err = tx.Query(`SELECT path FROM pinned_files WHERE pin_generation = ''`)
	if err != nil {
		return err
	}
	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			rows.Close()
			return err
		}
		paths = append(paths, path)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, path := range paths {
		generation, err := newPinGeneration()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE pinned_files SET pin_generation=?,status=?,bytes_cached=0,last_prefetched=0,last_error='',authority=X'' WHERE path=? AND pin_generation=''`, generation, int(StatusPending), path); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// Earlier builds used StatusReady for bytes-only FUSE hydration. Those
	// rows have no authority witness and must be reacquired, not stranded.
	if _, err := db.Exec(`UPDATE pinned_files SET status=?,bytes_cached=0,last_prefetched=0,last_error='' WHERE status=? AND length(authority)=0`, int(StatusPending), int(StatusReady)); err != nil {
		return err
	}
	if _, err := db.Exec(`UPDATE pinned_files SET status=?,bytes_cached=0,last_error='',authority=X'' WHERE status=?`, int(StatusPending), int(StatusPrefetching)); err != nil {
		return err
	}
	_, err = db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_pinned_generation ON pinned_files(pin_generation) WHERE pin_generation != ''`)
	return err
}

func (s *Store) Close() error { return s.db.Close() }

// Pin adds a single file to the registry. Idempotent; existing entries with
// the same path are updated to Pending status (re-queue them for prefetch).
func (s *Store) Pin(path string, size int64, root string) error {
	generation, err := newPinGeneration()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	_, err = s.db.Exec(`
		INSERT INTO pinned_files (path, size, status, pinned_at, pin_root, pin_generation)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET
		    size = excluded.size,
		    status = ?,
		    pinned_at = excluded.pinned_at,
		    pin_root = excluded.pin_root,
		    pin_generation = excluded.pin_generation,
		    bytes_cached = 0, last_prefetched = 0, last_error = '', authority = X''`,
		path, size, int(StatusPending), now, root, generation,
		int(StatusPending),
	)
	if err != nil {
		return fmt.Errorf("pin %q: %w", path, err)
	}
	s.RevokeReady(path)
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
		INSERT INTO pinned_files (path, size, status, pinned_at, pin_root, pin_generation)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET
		    size = excluded.size,
		    status = ?,
		    pinned_at = excluded.pinned_at,
		    pin_root = excluded.pin_root,
		    pin_generation = excluded.pin_generation,
		    bytes_cached = 0, last_prefetched = 0, last_error = '', authority = X''`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	now := time.Now().UnixMilli()
	for _, e := range entries {
		generation, genErr := newPinGeneration()
		if genErr != nil {
			tx.Rollback()
			return genErr
		}
		_, err := stmt.Exec(
			e.Path, e.Size, int(StatusPending), now, e.PinRoot, generation,
			int(StatusPending),
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("pin %q: %w", e.Path, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	for _, e := range entries {
		s.RevokeReady(e.Path)
	}
	return nil
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
	s.readyMu.Lock()
	var removed []Entry
	for path, entry := range s.ready {
		if path == root || entry.PinRoot == root {
			removed = append(removed, entry)
			delete(s.ready, path)
		}
	}
	revoke := s.readyRevoker
	s.readyMu.Unlock()
	if revoke != nil {
		for _, entry := range removed {
			revoke(entry.Path, entry.Generation)
		}
	}
	return int(n), nil
}

// UnpinPath removes a single path entry.
func (s *Store) UnpinPath(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`DELETE FROM pinned_files WHERE path = ?`, path)
	if err == nil {
		s.RevokeReady(path)
	}
	return err
}

// UpdateStatus is retained for synchronous legacy callers. Asynchronous
// prefetch work must use UpdateStatusForEntry to fence superseded attempts.
func (s *Store) UpdateStatus(path string, status Status, bytesCached int64, errMsg string) error {
	if status == StatusReady {
		return fmt.Errorf("pin Ready requires durable authority witness")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	_, err := s.db.Exec(`
		UPDATE pinned_files
		SET status = ?, bytes_cached = ?, last_prefetched = ?, last_error = ?, authority = X''
		WHERE path = ?`,
		int(status), bytesCached, now, errMsg, path)
	if err == nil {
		s.RevokeReady(path)
	}
	return err
}

// UpdateStatusForEntry is the only status publication path for an asynchronous
// prefetch job. A deleted/recreated or repinned path has a different generation.
func (s *Store) UpdateStatusForEntry(e Entry, status Status, bytesCached int64, errMsg string) (bool, error) {
	if status == StatusReady {
		return false, fmt.Errorf("pin Ready requires durable authority witness")
	}
	if e.Generation == "" {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.Exec(`UPDATE pinned_files SET status=?,bytes_cached=?,last_prefetched=?,last_error=?,authority=X'' WHERE path=? AND pin_generation=?`,
		int(status), bytesCached, time.Now().UnixMilli(), errMsg, e.Path, e.Generation)
	if err != nil {
		return false, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if changed != 0 {
		s.RevokeReady(e.Path)
	}
	return changed == 1, nil
}

// CompleteReady commits the opaque, already-verified producer witness last.
// Publication remains unavailable until AdmitReady validates local state.
func (s *Store) CompleteReady(e Entry, authority []byte) (bool, error) {
	if e.Generation == "" || len(authority) == 0 || e.Size < 0 {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.Exec(`UPDATE pinned_files SET status=?,bytes_cached=size,last_prefetched=?,last_error='',authority=? WHERE path=? AND pin_generation=? AND status=?`,
		int(StatusReady), time.Now().UnixMilli(), authority, e.Path, e.Generation, int(StatusPrefetching))
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}

// ClaimPrefetch atomically gives one queued attempt its own durable generation.
// A duplicate queue item or a later repair cannot inherit this worker's token.
func (s *Store) ClaimPrefetch(e Entry) (Entry, bool, error) {
	if e.Generation == "" || e.Status == StatusUnknown {
		return Entry{}, false, nil
	}
	generation, err := newPinGeneration()
	if err != nil {
		return Entry{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	status := StatusPrefetching
	bytesCached := int64(0)
	result, err := s.db.Exec(`UPDATE pinned_files SET pin_generation=?,status=?,bytes_cached=?,last_prefetched=?,authority=X'' WHERE path=? AND pin_generation=? AND status=?`,
		generation, int(status), bytesCached, time.Now().UnixMilli(), e.Path, e.Generation, int(e.Status))
	if err != nil {
		return Entry{}, false, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Entry{}, false, err
	}
	if changed != 1 {
		return Entry{}, false, nil
	}
	s.RevokeReady(e.Path)
	e.Generation, e.Status, e.BytesCached, e.Authority = generation, status, bytesCached, nil
	return e, true, nil
}

// RequeueForRepair revokes any earlier worker before Pending becomes visible.
func (s *Store) RequeueForRepair(e Entry) (bool, error) {
	if e.Generation == "" {
		return false, nil
	}
	generation, err := newPinGeneration()
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.Exec(`UPDATE pinned_files SET pin_generation=?,status=?,bytes_cached=0,last_prefetched=0,last_error='',authority=X'' WHERE path=? AND pin_generation=?`,
		generation, int(StatusPending), e.Path, e.Generation)
	if err != nil {
		return false, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if changed != 0 {
		s.RevokeReady(e.Path)
	}
	return changed == 1, nil
}

// SetReadyValidator installs the immutable, RAM-only local authority check.
// Production wires it before any Ready admission or NFS listener publication.
func (s *Store) SetReadyValidator(validate func(Entry) bool) error {
	if validate == nil {
		return fmt.Errorf("pin ready validator is nil")
	}
	s.readyMu.Lock()
	defer s.readyMu.Unlock()
	if s.readyValidator != nil {
		return fmt.Errorf("pin ready validator is already configured")
	}
	s.readyValidator = validate
	s.ready = make(map[string]Entry)
	return nil
}

// SetReadyRevoker installs a local-only cleanup hook before workers start.
// The hook must not call back into Store; mutations can hold s.mu.
func (s *Store) SetReadyRevoker(revoke func(string, string)) error {
	if revoke == nil {
		return fmt.Errorf("nil pin ready revoker")
	}
	s.readyMu.Lock()
	defer s.readyMu.Unlock()
	if s.readyRevoker != nil {
		return fmt.Errorf("pin ready revoker already configured")
	}
	s.readyRevoker = revoke
	return nil
}

// ReadyAdmission is a pure RAM check. StatusReady in SQLite alone never
// authorizes a read, including a legacy row or a crash before Finalize.
func (s *Store) ReadyAdmission(path string) (Entry, bool) {
	s.readyMu.RLock()
	entry, ok := s.ready[path]
	validate := s.readyValidator
	s.readyMu.RUnlock()
	if !ok || validate == nil || !validate(entry) {
		return Entry{}, false
	}
	s.readyMu.RLock()
	current, still := s.ready[path]
	s.readyMu.RUnlock()
	return entry, still && current.Generation == entry.Generation
}

func (s *Store) IsPinnedReady(path string) bool {
	_, ok := s.ReadyAdmission(path)
	return ok
}

// AdmitReady is called only after producer Finalize or locally verified boot
// restore. The SQL check and RAM install are serialized with pin mutations.
func (s *Store) AdmitReady(e Entry) (bool, error) {
	if e.Generation == "" || len(e.Authority) == 0 {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var status int
	var authority []byte
	err := s.db.QueryRow(`SELECT status,authority FROM pinned_files WHERE path=? AND pin_generation=?`, e.Path, e.Generation).Scan(&status, &authority)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if Status(status) != StatusReady || !bytes.Equal(authority, e.Authority) {
		return false, nil
	}
	s.readyMu.Lock()
	if s.ready == nil {
		s.ready = make(map[string]Entry)
	}
	s.ready[e.Path] = e
	s.readyMu.Unlock()
	return true, nil
}

// TouchReady records a completed local revalidation without rotating its
// generation or revoking a still-valid witness. A superseding pin/repair
// cannot inherit the refresh timestamp.
func (s *Store) TouchReady(e Entry) (bool, error) {
	if e.Generation == "" || len(e.Authority) == 0 {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.Exec(`UPDATE pinned_files SET last_prefetched=? WHERE path=? AND pin_generation=? AND status=? AND authority=?`,
		time.Now().UnixMilli(), e.Path, e.Generation, int(StatusReady), e.Authority)
	if err != nil {
		return false, err
	}
	changed, err := result.RowsAffected()
	return changed == 1, err
}

// RevokeReady removes one admission immediately after a local mutation.
func (s *Store) RevokeReady(path string) {
	s.readyMu.Lock()
	old, existed := s.ready[path]
	delete(s.ready, path)
	revoke := s.readyRevoker
	s.readyMu.Unlock()
	if existed && revoke != nil {
		revoke(path, old.Generation)
	}
}

func (s *Store) RevokeReadyIfGeneration(path, generation string) {
	s.readyMu.Lock()
	revoked := false
	if current, ok := s.ready[path]; ok && current.Generation == generation {
		delete(s.ready, path)
		revoked = true
	}
	revoke := s.readyRevoker
	s.readyMu.Unlock()
	if revoked && revoke != nil {
		revoke(path, generation)
	}
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
		SELECT path, size, status, bytes_cached, last_prefetched, last_error, pinned_at, pin_root, pin_generation, authority
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
		SELECT path, size, status, bytes_cached, last_prefetched, last_error, pinned_at, pin_root, pin_generation, authority
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
		SELECT path, size, status, bytes_cached, last_prefetched, last_error, pinned_at, pin_root, pin_generation, authority
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
		SELECT path, size, status, bytes_cached, last_prefetched, last_error, pinned_at, pin_root, pin_generation, authority
		FROM pinned_files
		ORDER BY pin_root, path`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEntries(rows)
}

// Get returns the pin row for exactly this path, or (nil, false) when the path
// has no pinned_files row. Unlike IsPinnedReady (a boolean offline-gate check),
// Get exposes the raw durable scheduler row. Its Ready status and byte counter
// are not current residency until ReadyAdmission validates this generation.
// Not a hot path; one indexed lookup on the path primary key.
func (s *Store) Get(path string) (*Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query(`
		SELECT path, size, status, bytes_cached, last_prefetched, last_error, pinned_at, pin_root, pin_generation, authority
		FROM pinned_files
		WHERE path = ?
		LIMIT 1`, path)
	if err != nil {
		return nil, false
	}
	defer rows.Close()
	entries, err := scanEntries(rows)
	if err != nil || len(entries) == 0 {
		return nil, false
	}
	e := entries[0]
	return &e, true
}

// PinRoots returns the distinct pin roots and aggregate stats per root.
type RootSummary struct {
	Root         string `json:"root"`
	TotalFiles   int    `json:"total_files"`
	ReadyFiles   int    `json:"ready_files"`
	PendingFiles int    `json:"pending_files"`
	FailedFiles  int    `json:"failed_files"`
	TotalBytes   int64  `json:"total_bytes"`
	CachedBytes  int64  `json:"cached_bytes"`
}

func (s *Store) PinRoots() ([]RootSummary, error) {
	// All closes SQL rows and releases Store.mu before the per-use validator is
	// called. Reporting cannot turn a durable witness into RAM admission.
	entries, err := s.All()
	if err != nil {
		return nil, err
	}
	var out []RootSummary
	for _, e := range entries {
		if e.PinRoot == "" {
			continue
		}
		if len(out) == 0 || out[len(out)-1].Root != e.PinRoot {
			out = append(out, RootSummary{Root: e.PinRoot})
		}
		r := &out[len(out)-1]
		r.TotalFiles++
		r.TotalBytes += e.Size
		status, cached := s.reportedStatus(e)
		r.CachedBytes += cached
		switch status {
		case StatusReady:
			r.ReadyFiles++
		case StatusPending:
			r.PendingFiles++
		case StatusFailed:
			r.FailedFiles++
		}
	}
	return out, nil
}

// AggregateStats returns whole-database counters.
type AggregateStats struct {
	TotalFiles   int   `json:"total_files"`
	ReadyFiles   int   `json:"ready_files"`
	PendingFiles int   `json:"pending_files"`
	FailedFiles  int   `json:"failed_files"`
	TotalBytes   int64 `json:"total_bytes"`
	CachedBytes  int64 `json:"cached_bytes"`
}

func (s *Store) AggregateStats() (AggregateStats, error) {
	var a AggregateStats
	entries, err := s.All()
	if err != nil {
		return a, err
	}
	for _, e := range entries {
		a.TotalFiles++
		a.TotalBytes += e.Size
		status, cached := s.reportedStatus(e)
		a.CachedBytes += cached
		switch status {
		case StatusReady:
			a.ReadyFiles++
		case StatusPending:
			a.PendingFiles++
		case StatusFailed:
			a.FailedFiles++
		}
	}
	return a, nil
}

// reportedStatus projects durable scheduler state through this process's
// exact-generation local admission. It never changes the row or its witness.
func (s *Store) reportedStatus(e Entry) (Status, int64) {
	if e.Status == StatusFailed {
		return StatusFailed, 0
	}
	if e.Status != StatusReady {
		return e.Status, e.BytesCached // Preserve active prefetch progress.
	}
	admitted, ok := s.ReadyAdmission(e.Path)
	if !ok || admitted.Generation != e.Generation || admitted.Size != e.Size ||
		!bytes.Equal(admitted.Authority, e.Authority) {
		return StatusPending, 0
	}
	return StatusReady, e.BytesCached
}

func scanEntries(rows *sql.Rows) ([]Entry, error) {
	var out []Entry
	for rows.Next() {
		var e Entry
		var statusInt int
		var lastPre int64
		var pinnedAt int64
		if err := rows.Scan(&e.Path, &e.Size, &statusInt, &e.BytesCached, &lastPre, &e.LastError, &pinnedAt, &e.PinRoot, &e.Generation, &e.Authority); err != nil {
			return nil, err
		}
		e.Status = Status(statusInt)
		e.LastPrefetched = time.UnixMilli(lastPre)
		e.PinnedAt = time.UnixMilli(pinnedAt)
		out = append(out, e)
	}
	return out, rows.Err()
}

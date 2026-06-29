package metadata

import (
	"database/sql"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"
)

// DrainState is the spool entry lifecycle state.
type DrainState string

const (
	DrainWriting  DrainState = "writing"
	DrainReady    DrainState = "ready"
	DrainDraining DrainState = "draining"
	DrainDone     DrainState = "done"
	DrainFailed   DrainState = "failed"
)

// SpoolRow is the persisted shape of one spool entry. Maps 1:1 to the
// spool_entries table. The NFS handler layer wraps this with file-handle
// state in nfs.SpoolEntry.
type SpoolRow struct {
	ID            int64
	NFSPath       string
	SpoolFile     string
	Size          int64
	SHA256        []byte
	DrainState    DrainState
	DrainAttempts int
	LastError     string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// SpoolStore is the SQLite-backed CRUD layer for spool_entries.
// Shares the underlying *sql.DB with Store but takes its own write mutex
// so spool churn doesn't contend with metadata-cache writes any more than
// strictly necessary at the connection-pool level. (The DB itself
// serializes writes via WAL; the mutex is a contention-tracking aid.)
type SpoolStore struct {
	db      *sql.DB
	writeMu sync.Mutex
}

// NewSpoolStore returns a SpoolStore backed by db. The caller is responsible
// for having already called InitSpoolSchema.
func NewSpoolStore(db *sql.DB) *SpoolStore {
	return &SpoolStore{db: db}
}

// Insert creates a new spool_entries row in `writing` state and returns
// the assigned id. Caller is expected to track id alongside the local
// file-handle state.
func (s *SpoolStore) Insert(nfsPath, spoolFile string) (int64, error) {
	now := time.Now().Unix()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(
		`INSERT INTO spool_entries (nfs_path, spool_file, size, drain_state, drain_attempts, created_at, updated_at)
		 VALUES (?, ?, 0, ?, 0, ?, ?)`,
		nfsPath, spoolFile, DrainWriting, now, now,
	)
	if err != nil {
		return 0, fmt.Errorf("spool insert %q: %w", nfsPath, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("spool insert lastid %q: %w", nfsPath, err)
	}
	return id, nil
}

// MarkReady transitions id from writing→ready and persists final size + sha.
// Only succeeds if the current state is writing — protects against races
// where a scrubber has already marked the row failed.
func (s *SpoolStore) MarkReady(id int64, size int64, sha []byte) error {
	now := time.Now().Unix()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(
		`UPDATE spool_entries SET drain_state=?, size=?, sha256=?, updated_at=?
		 WHERE id=? AND drain_state=?`,
		DrainReady, size, sha, now, id, DrainWriting,
	)
	if err != nil {
		return fmt.Errorf("spool mark ready %d: %w", id, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("spool mark ready %d: row not in writing state", id)
	}
	return nil
}

// MarkDraining transitions ready→draining. Used by the drainer to claim
// an entry. Returns false if the row was not in ready state (e.g. another
// drainer worker already claimed it).
func (s *SpoolStore) MarkDraining(id int64) (bool, error) {
	now := time.Now().Unix()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(
		`UPDATE spool_entries SET drain_state=?, updated_at=?
		 WHERE id=? AND drain_state=?`,
		DrainDraining, now, id, DrainReady,
	)
	if err != nil {
		return false, fmt.Errorf("spool mark draining %d: %w", id, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// MarkDone transitions a row to done and reports whether a row was actually
// updated. The WHERE clause is by id only, so any row that still exists (in
// any state) is marked done and reported true. The one case that returns
// false is a row DELETED out from under the drainer: the NFS delete path
// cancels in-flight spool entries (QA-37, DeleteActiveByPath), and the drainer
// uses a false return to know it must UNDO its FUSE write rather than
// resurrect a file the user already deleted.
func (s *SpoolStore) MarkDone(id int64) (bool, error) {
	now := time.Now().Unix()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(
		`UPDATE spool_entries SET drain_state=?, updated_at=? WHERE id=?`,
		DrainDone, now, id,
	)
	if err != nil {
		return false, fmt.Errorf("spool mark done %d: %w", id, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// DeleteActiveByPath removes every non-terminal (writing/ready/draining) row
// for nfsPath and returns the rows it removed, so the caller can clean up the
// associated spool files + capacity reservations. Terminal rows (done/failed)
// are left for the audit trail / GC.
//
// This is the SQL half of the QA-37 delete↔drain fix. Every spool state
// transition (MarkDraining, MarkDone, …) takes writeMu, so holding it across
// the SELECT+DELETE makes the cancel atomic with respect to an in-flight
// drain: a concurrent MarkDraining/MarkDone either committed before this call
// (and is observed by the SELECT) or blocks until after the DELETE (and then
// finds 0 rows). No row can be half-claimed across the cancel.
func (s *SpoolStore) DeleteActiveByPath(nfsPath string) ([]*SpoolRow, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	rows, err := s.db.Query(
		`SELECT id, nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, created_at, updated_at
		 FROM spool_entries WHERE nfs_path=? AND drain_state IN ('writing','ready','draining')`,
		nfsPath,
	)
	if err != nil {
		return nil, fmt.Errorf("spool delete-active-by-path select %q: %w", nfsPath, err)
	}
	var out []*SpoolRow
	for rows.Next() {
		r, scanErr := scanSpoolRow(rows.Scan)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		out = append(out, r)
	}
	errRows := rows.Err()
	rows.Close()
	if errRows != nil {
		return nil, errRows
	}
	if len(out) == 0 {
		return nil, nil
	}
	if _, err := s.db.Exec(
		`DELETE FROM spool_entries WHERE nfs_path=? AND drain_state IN ('writing','ready','draining')`,
		nfsPath,
	); err != nil {
		return out, fmt.Errorf("spool delete-active-by-path delete %q: %w", nfsPath, err)
	}
	return out, nil
}

// SpoolPathMigration describes one row moved by MigrateActivePaths.
type SpoolPathMigration struct {
	OldID    int64
	NewID    int64 // == OldID unless Requeued
	OldPath  string
	NewPath  string
	Requeued bool // draining row was cancelled + reinserted as a fresh ready row
}

// MigrateActivePaths is the SQL half of the rename↔spool fix (Phase-1
// BUG 1). It re-points every active row whose nfs_path is oldPath — or
// lives under oldPath+"/" (directory rename) — at the corresponding path
// under newPath:
//
//	writing/ready → UPDATE nfs_path in place (drain not yet claimed; the
//	                queued copy will target the new path)
//	draining      → DELETE + INSERT a fresh `ready` row at the new path
//	                sharing the same spool file. The in-flight drain then
//	                observes done=false at MarkDone (rows-affected 0) and
//	                undoes its FUSE write — the QA-37 cancel contract —
//	                while the requeued row re-drains to the new target.
//
// Holding writeMu across the SELECT + mutations makes the migration atomic
// with respect to MarkDraining/MarkDone/DeleteActiveByPath, exactly like
// DeleteActiveByPath: a concurrent drain transition either committed before
// (and is observed by the SELECT) or blocks until after (and then sees the
// migrated/deleted state). The statements run in one transaction so a crash
// can't split a directory rename's children across both prefixes.
//
// Prefix matching uses substr(), not LIKE, so paths containing '%' or '_'
// can't over-match.
func (s *SpoolStore) MigrateActivePaths(oldPath, newPath string) ([]SpoolPathMigration, error) {
	if oldPath == "" || oldPath == newPath {
		return nil, nil
	}
	now := time.Now().Unix()
	prefix := oldPath + "/"

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("spool migrate-paths begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op after Commit

	rows, err := tx.Query(
		`SELECT id, nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, created_at, updated_at
		 FROM spool_entries
		 WHERE drain_state IN ('writing','ready','draining')
		   AND (nfs_path = ? OR substr(nfs_path, 1, ?) = ?)`,
		// substr counts CHARACTERS, len() counts BYTES (adversarial-review
		// BUG B): a multi-byte dir name ("vidéos/") made the byte length
		// overshoot the rune count, so the prefix never matched and a
		// unicode directory rename silently migrated zero children.
		oldPath, utf8.RuneCountInString(prefix), prefix,
	)
	if err != nil {
		return nil, fmt.Errorf("spool migrate-paths select %q: %w", oldPath, err)
	}
	var active []*SpoolRow
	for rows.Next() {
		r, scanErr := scanSpoolRow(rows.Scan)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		active = append(active, r)
	}
	errRows := rows.Err()
	rows.Close()
	if errRows != nil {
		return nil, errRows
	}
	if len(active) == 0 {
		return nil, nil
	}

	out := make([]SpoolPathMigration, 0, len(active))
	for _, r := range active {
		newRowPath := newPath + r.NFSPath[len(oldPath):]
		m := SpoolPathMigration{OldID: r.ID, NewID: r.ID, OldPath: r.NFSPath, NewPath: newRowPath}

		switch r.DrainState {
		case DrainDraining:
			if _, err := tx.Exec(`DELETE FROM spool_entries WHERE id=?`, r.ID); err != nil {
				return nil, fmt.Errorf("spool migrate-paths cancel draining %d: %w", r.ID, err)
			}
			// created_at preserved so the requeued row keeps its FIFO
			// position in ListReady; attempts carried (the cancelled
			// attempt wasn't a data failure, but the budget shouldn't
			// reset on rename either).
			res, err := tx.Exec(
				`INSERT INTO spool_entries (nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, created_at, updated_at)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				newRowPath, r.SpoolFile, r.Size, r.SHA256, DrainReady, r.DrainAttempts, r.LastError, r.CreatedAt.Unix(), now,
			)
			if err != nil {
				return nil, fmt.Errorf("spool migrate-paths requeue %d: %w", r.ID, err)
			}
			newID, err := res.LastInsertId()
			if err != nil {
				return nil, fmt.Errorf("spool migrate-paths requeue lastid %d: %w", r.ID, err)
			}
			m.NewID = newID
			m.Requeued = true

		default: // writing, ready
			if _, err := tx.Exec(
				`UPDATE spool_entries SET nfs_path=?, updated_at=? WHERE id=?`,
				newRowPath, now, r.ID,
			); err != nil {
				return nil, fmt.Errorf("spool migrate-paths update %d: %w", r.ID, err)
			}
		}
		out = append(out, m)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("spool migrate-paths commit: %w", err)
	}
	return out, nil
}

// MarkFailed transitions any state → failed with an attempt-counter bump
// and an error message. Drainer uses this on retry exhaustion or SHA
// mismatch; scrubber uses it for orphan recovery.
// MarkFailed transitions a row to failed and reports whether a row was
// actually updated. The bool matters for capacity accounting (adversarial-
// review BUG 1): a row DELETED out from under the worker (CancelForDelete,
// or a rename-requeue's DELETE+INSERT) has already had its reservation
// released by whoever deleted it — releasing again on a 0-row UPDATE
// double-releases, the cap under-counts, and the spool SSD can over-admit.
// Callers must release capacity ONLY when this returns true.
func (s *SpoolStore) MarkFailed(id int64, errMsg string) (bool, error) {
	now := time.Now().Unix()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(
		`UPDATE spool_entries SET drain_state=?, drain_attempts=drain_attempts+1, last_error=?, updated_at=?
		 WHERE id=?`,
		DrainFailed, errMsg, now, id,
	)
	if err != nil {
		return false, fmt.Errorf("spool mark failed %d: %w", id, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// HasNewerRowForPath reports whether any row exists for nfsPath with an id
// greater than afterID, in ANY state. RetryFailed uses this as its staleness
// guard (adversarial-review BUG 3): a failed row whose path has a NEWER row
// must not be requeued — the newer row's bytes (drained, draining, or queued)
// would be clobbered by the older spool file, silently replacing fresh
// content with stale bytes. A newer row in any state means the user acted on
// the path after the failure; the failed row is history, not work.
func (s *SpoolStore) HasNewerRowForPath(nfsPath string, afterID int64) (bool, error) {
	var exists bool
	err := s.db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM spool_entries WHERE nfs_path=? AND id>?)`,
		nfsPath, afterID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("spool has-newer-row %q: %w", nfsPath, err)
	}
	return exists, nil
}

// IncrementAttempts bumps the drain_attempts counter without changing state.
// Used by the drainer on a retryable failure (it will mark the row back to
// ready for retry, after backing off).
func (s *SpoolStore) IncrementAttempts(id int64, errMsg string) error {
	now := time.Now().Unix()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.Exec(
		`UPDATE spool_entries SET drain_attempts=drain_attempts+1, last_error=?, updated_at=?
		 WHERE id=?`,
		errMsg, now, id,
	)
	if err != nil {
		return fmt.Errorf("spool incr attempts %d: %w", id, err)
	}
	return nil
}

// ResetToReady transitions a row back to ready. Used by the drainer after
// backoff or by the boot scrubber to resurrect interrupted drain rows.
func (s *SpoolStore) ResetToReady(id int64) error {
	now := time.Now().Unix()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.Exec(
		`UPDATE spool_entries SET drain_state=?, updated_at=? WHERE id=?`,
		DrainReady, now, id,
	)
	if err != nil {
		return fmt.Errorf("spool reset ready %d: %w", id, err)
	}
	return nil
}

// ResetForRetry transitions a FAILED row back to ready with a fresh
// attempt budget. Used by the operator-facing retry path
// (/spool-recover?action=retry-failed): without zeroing drain_attempts
// the drainer's next claim would immediately re-fail the row on its
// "retry budget exhausted" check, making retry a no-op. last_error is
// preserved for the audit trail until the next attempt overwrites it.
// The WHERE guards on drain_state=failed so a concurrent drain
// transition can't be clobbered; returns false if the row was not in
// failed state.
func (s *SpoolStore) ResetForRetry(id int64) (bool, error) {
	now := time.Now().Unix()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(
		`UPDATE spool_entries SET drain_state=?, drain_attempts=0, updated_at=?
		 WHERE id=? AND drain_state=?`,
		DrainReady, now, id, DrainFailed,
	)
	if err != nil {
		return false, fmt.Errorf("spool reset for retry %d: %w", id, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// Get returns the row for id, or sql.ErrNoRows if it doesn't exist.
func (s *SpoolStore) Get(id int64) (*SpoolRow, error) {
	row := s.db.QueryRow(
		`SELECT id, nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, created_at, updated_at
		 FROM spool_entries WHERE id=?`,
		id,
	)
	return scanSpoolRow(row.Scan)
}

// LookupByPath returns the most recent (highest id) row for nfsPath that
// is in writing or ready state, or sql.ErrNoRows if none exists.
// Reads of in-progress writes consult this to serve from spool.
func (s *SpoolStore) LookupByPath(nfsPath string) (*SpoolRow, error) {
	row := s.db.QueryRow(
		`SELECT id, nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, created_at, updated_at
		 FROM spool_entries
		 WHERE nfs_path=? AND drain_state IN ('writing','ready','draining')
		 ORDER BY id DESC LIMIT 1`,
		nfsPath,
	)
	return scanSpoolRow(row.Scan)
}

// ListReady returns up to limit rows in ready state ordered by created_at.
// Used by the drainer to fetch its next batch.
func (s *SpoolStore) ListReady(limit int) ([]*SpoolRow, error) {
	rows, err := s.db.Query(
		`SELECT id, nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, created_at, updated_at
		 FROM spool_entries WHERE drain_state=? ORDER BY created_at ASC LIMIT ?`,
		DrainReady, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("spool list ready: %w", err)
	}
	defer rows.Close()
	var out []*SpoolRow
	for rows.Next() {
		r, err := scanSpoolRow(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListAll returns ALL spool rows. Used by the boot scrubber to reconcile
// disk state against the index. Intentionally unfiltered.
func (s *SpoolStore) ListAll() ([]*SpoolRow, error) {
	rows, err := s.db.Query(
		`SELECT id, nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, created_at, updated_at
		 FROM spool_entries`,
	)
	if err != nil {
		return nil, fmt.Errorf("spool list all: %w", err)
	}
	defer rows.Close()
	var out []*SpoolRow
	for rows.Next() {
		r, err := scanSpoolRow(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListForStatus returns only the rows the /spool status view needs: every
// non-done row (writing/ready/draining/failed) plus done rows updated at or
// after doneSince. BuildSpoolStatus discards older done rows anyway, so this
// is output-identical to ListAll for the status view while scanning a handful
// of rows instead of the whole table.
//
// QA-38 (2026-06-13): the table had grown to 44k+ rows because DeleteDone was
// defined but never scheduled. ListAll on every /spool status poll then
// fetched + allocated all 44k rows; the Swift menu-bar UI polls continuously,
// so the bridge burned multiple cores (database/sql row iteration + GC) and
// held SQLite locks, starving the NFS request handlers until the FUSE mount
// went readdir-unresponsive and writes blew the soft-mount budget — surfacing
// to Finder as "operation can't be completed (error 100060)" mid-copy.
func (s *SpoolStore) ListForStatus(doneSince time.Time) ([]*SpoolRow, error) {
	rows, err := s.db.Query(
		`SELECT id, nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, created_at, updated_at
		 FROM spool_entries
		 WHERE drain_state != ? OR updated_at >= ?
		 ORDER BY id`,
		DrainDone, doneSince.Unix(),
	)
	if err != nil {
		return nil, fmt.Errorf("spool list for status: %w", err)
	}
	defer rows.Close()
	var out []*SpoolRow
	for rows.Next() {
		r, err := scanSpoolRow(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// PendingStats returns counts of writing+ready+draining rows and their
// total size. Used by the manager UI overview tile.
func (s *SpoolStore) PendingStats() (pendingFiles int, pendingBytes int64, err error) {
	row := s.db.QueryRow(
		`SELECT COUNT(*), COALESCE(SUM(size), 0) FROM spool_entries
		 WHERE drain_state IN ('writing','ready','draining')`,
	)
	if err := row.Scan(&pendingFiles, &pendingBytes); err != nil {
		return 0, 0, fmt.Errorf("spool pending stats: %w", err)
	}
	return pendingFiles, pendingBytes, nil
}

// Delete removes a row outright. Used by the rollback path when
// OpenFile-on-disk fails immediately after Insert — there's no in-flight
// state to preserve, so leaving a `failed` row would just churn the
// table on every retry of the same path. Caller is responsible for any
// downstream cleanup; this is purely the SQL row delete.
func (s *SpoolStore) Delete(id int64) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := s.db.Exec(`DELETE FROM spool_entries WHERE id=?`, id); err != nil {
		return fmt.Errorf("spool delete %d: %w", id, err)
	}
	return nil
}

// DeleteDone removes done rows older than cutoff. Garbage collection for
// the audit trail; safe to call on a timer.
func (s *SpoolStore) DeleteDone(cutoff time.Time) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(
		`DELETE FROM spool_entries WHERE drain_state=? AND updated_at < ?`,
		DrainDone, cutoff.Unix(),
	)
	if err != nil {
		return 0, fmt.Errorf("spool delete done: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// PendingSymlink is one persisted offline-created symlink awaiting
// materialization on the FUSE mount at reconnect. Maps 1:1 to a
// pending_symlinks row.
type PendingSymlink struct {
	ID        int64
	LinkPath  string // in-mount path of the link (UNIQUE key)
	Target    string // verbatim target string (relative or absolute)
	CreatedAt time.Time
}

// PutPendingSymlink records (linkPath, target) so an offline-created symlink
// survives an app restart and gets materialized on the FUSE mount when the
// backend is reachable again. It is an UPSERT keyed on link_path: if the same
// link is re-created (e.g. a copy re-issued after a cancel), the latest target
// wins and no duplicate row accumulates — matching os.Symlink's "one link per
// path" semantics. created_at is refreshed on conflict so the audit ordering
// reflects the most recent intent.
func (s *SpoolStore) PutPendingSymlink(linkPath, target string) error {
	now := time.Now().Unix()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := s.db.Exec(
		`INSERT INTO pending_symlinks (link_path, target, created_at)
		 VALUES (?, ?, ?)
		 ON CONFLICT(link_path) DO UPDATE SET target=excluded.target, created_at=excluded.created_at`,
		linkPath, target, now,
	); err != nil {
		return fmt.Errorf("put pending symlink %q: %w", linkPath, err)
	}
	return nil
}

// GetPendingSymlink returns the persisted target for linkPath, or sql.ErrNoRows
// if none is pending. The offline Readlink path uses this to serve the target
// for a not-yet-materialized symlink (os.Readlink on FUSE would ENOENT offline).
func (s *SpoolStore) GetPendingSymlink(linkPath string) (string, error) {
	var target string
	err := s.db.QueryRow(
		`SELECT target FROM pending_symlinks WHERE link_path=?`, linkPath,
	).Scan(&target)
	if err != nil {
		return "", err
	}
	return target, nil
}

// ListPendingSymlinks returns every pending symlink, oldest first, so the
// drainer's reconnect hook materializes them in creation order. Unfiltered:
// the table only ever holds links not yet materialized (each is deleted on
// success), so its size is bounded by the offline working set.
func (s *SpoolStore) ListPendingSymlinks() ([]PendingSymlink, error) {
	rows, err := s.db.Query(
		`SELECT id, link_path, target, created_at FROM pending_symlinks ORDER BY created_at ASC, id ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("list pending symlinks: %w", err)
	}
	defer rows.Close()
	var out []PendingSymlink
	for rows.Next() {
		var p PendingSymlink
		var createdAt int64
		if err := rows.Scan(&p.ID, &p.LinkPath, &p.Target, &createdAt); err != nil {
			return nil, err
		}
		p.CreatedAt = time.Unix(createdAt, 0)
		out = append(out, p)
	}
	return out, rows.Err()
}

// DeletePendingSymlink removes the row for linkPath once it has been
// materialized on FUSE (or is otherwise no longer pending — e.g. the link's
// path was deleted before reconnect). Idempotent: deleting an absent row is a
// no-op, so a crash between os.Symlink and this delete just re-materializes
// harmlessly on the next reconnect.
func (s *SpoolStore) DeletePendingSymlink(linkPath string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := s.db.Exec(`DELETE FROM pending_symlinks WHERE link_path=?`, linkPath); err != nil {
		return fmt.Errorf("delete pending symlink %q: %w", linkPath, err)
	}
	return nil
}

// scanSpoolRow is the Scan callback adapter used by Get/ListReady/etc.
func scanSpoolRow(scan func(...any) error) (*SpoolRow, error) {
	var r SpoolRow
	var state string
	var lastErr sql.NullString
	var createdAt, updatedAt int64
	if err := scan(&r.ID, &r.NFSPath, &r.SpoolFile, &r.Size, &r.SHA256, &state, &r.DrainAttempts, &lastErr, &createdAt, &updatedAt); err != nil {
		return nil, err
	}
	r.DrainState = DrainState(state)
	if lastErr.Valid {
		r.LastError = lastErr.String
	}
	r.CreatedAt = time.Unix(createdAt, 0)
	r.UpdatedAt = time.Unix(updatedAt, 0)
	return &r, nil
}

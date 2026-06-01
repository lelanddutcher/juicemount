package metadata

import (
	"database/sql"
	"fmt"
	"sync"
	"time"
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

// MarkFailed transitions any state → failed with an attempt-counter bump
// and an error message. Drainer uses this on retry exhaustion or SHA
// mismatch; scrubber uses it for orphan recovery.
func (s *SpoolStore) MarkFailed(id int64, errMsg string) error {
	now := time.Now().Unix()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.Exec(
		`UPDATE spool_entries SET drain_state=?, drain_attempts=drain_attempts+1, last_error=?, updated_at=?
		 WHERE id=?`,
		DrainFailed, errMsg, now, id,
	)
	if err != nil {
		return fmt.Errorf("spool mark failed %d: %w", id, err)
	}
	return nil
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

package metadata

import (
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
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
	ID                int64
	NFSPath           string
	SpoolFile         string
	Size              int64
	SHA256            []byte
	DrainState        DrainState
	DrainAttempts     int
	LastError         string
	PauseSince        time.Time // durable first failed publish of this row
	RetryAfter        time.Time // next eligible claim; zero means immediately due
	QualificationHold string    // unverified saved image; no automatic or operator replay
	RetiredBy         int64     // a later accepted save permanently superseded this history
	// SuspectZeroTail is the #104 zero-tail-detection marker. Empty = clean.
	// Non-empty = compact JSON detail {detected_at,size,contiguous,holes}
	// written by MarkSuspectZeroTail when the entry finalized with unwritten
	// hole(s) below its written size (the interrupted preallocate-then-write
	// download signature — the file drains FULL-SIZE with a zero tail).
	// Surfacing only: nothing gates a drain on this field.
	SuspectZeroTail string
	// PunchedEnd is the end of the prefix the streaming drain has copied to the
	// backend AND punched out of the spool file. Zero on every row a
	// non-streaming build ever wrote, which is the correct default: nothing
	// punched.
	//
	// A NON-ZERO VALUE MEANS THE SPOOL FILE IS NOT SELF-SUFFICIENT. Its first
	// PunchedEnd bytes read as zeros, and because F_PUNCHHOLE leaves the logical
	// size untouched, nothing about the file on disk reveals that. Recovery MUST
	// consult this before treating a size match as proof the copy survived — see
	// SetPunchedEnd and nfs/spool.go's failed→ready branch.
	PunchedEnd int64
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// SpoolStore is the SQLite-backed CRUD layer for spool_entries.
// Shares the underlying *sql.DB with Store but takes its own write mutex
// so spool churn doesn't contend with metadata-cache writes any more than
// strictly necessary at the connection-pool level. (The DB itself
// serializes writes via WAL; the mutex is a contention-tracking aid.)
type SpoolStore struct {
	db      *sql.DB
	writeMu sync.Mutex
	owner   atomic.Pointer[Store]
	// Test-only reply-loss seam after a real READY COMMIT, before admission.
	continuationPostCommit  func() error
	shutdownReadyPostCommit func() error // test-only reply loss after durable finalization
}

// NewSpoolStore returns a SpoolStore backed by db. The caller is responsible
// for having already called InitSpoolSchema.
func NewSpoolStore(db *sql.DB) *SpoolStore {
	return &SpoolStore{db: db}
}

// CheckReplacementUnder refuses generic namespace mutations that would cancel
// or move a locally accepted replacement carrier. The caller must check this
// before any physical destination cancellation; the mutation methods below
// repeat the check while holding writeMu for their own SQL changes.
func (s *SpoolStore) CheckReplacementUnder(paths ...string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.checkReplacementLocked(0, paths...)
}

// checkReplacementLocked is shared by the path and row-ID guards. A non-NULL
// witness is fenced even if malformed: generic paths may not discard evidence
// whose canonicality they have not proved. Caller holds writeMu.
func (s *SpoolStore) checkReplacementLocked(id int64, paths ...string) error {
	if id > 0 {
		var blocked bool
		if err := s.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM spool_entries
 WHERE id=? AND retired_by=0 AND replacement_witness IS NOT NULL)`, id).Scan(&blocked); err != nil {
			return err
		}
		if blocked {
			return fmt.Errorf("accepted replacement row %d: %w", id, ErrMirrorConflict)
		}
	}
	for _, name := range paths {
		var rows *sql.Rows
		var err error
		if name == "" || name == "." {
			rows, err = s.db.Query(`SELECT id FROM spool_entries WHERE retired_by=0 AND replacement_witness IS NOT NULL`)
		} else {
			prefix := name + "/"
			rows, err = s.db.Query(`SELECT id FROM spool_entries WHERE retired_by=0 AND replacement_witness IS NOT NULL AND (nfs_path=? OR substr(nfs_path,1,?)=?)`, name, utf8.RuneCountInString(prefix), prefix)
		}
		if err != nil {
			return err
		}
		var ids []int64
		for rows.Next() {
			var candidate int64
			if err := rows.Scan(&candidate); err != nil {
				_ = rows.Close()
				return err
			}
			ids = append(ids, candidate)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, candidate := range ids {
			p, _, _, err := readReplacementPublication(s.db, candidate)
			if err != nil || p == nil || p.Phase != ReplacementPublicationCleaned {
				return fmt.Errorf("accepted replacement under %q: %w", name, ErrMirrorConflict)
			}
		}
	}
	return nil
}

// deleteActiveByPathSelectHook, when non-nil, is called by DeleteActiveByPath
// after its SELECT has collected rows to delete but BEFORE its DELETE runs —
// the precise "decision made, not yet executed" point the QA-37 cancel↔drain
// race turns on. Test-only (set by the concurrent-interleaving regression
// test); nil in production, so the hot path pays only a nil check.
var deleteActiveByPathSelectHook func(nfsPath string)

// Insert creates a new spool_entries row in `writing` state and returns
// the assigned id. Caller is expected to track id alongside the local
// file-handle state.
//
// Insert records an UNKNOWN base identity. Production shadow-opens use
// InsertWithBase so the drainer can tell a save of the file the user saw from
// a file another writer put there (H-8B).
func (s *SpoolStore) Insert(nfsPath, spoolFile string) (int64, error) {
	return s.InsertWithBase(nfsPath, spoolFile, SpoolBase{}, 0)
}

// InsertWithBase is Insert plus the base identity of the backend file this
// write replaces (or SpoolBaseAbsent for a new name) and the project-lease
// epoch the write was accepted under (0 = no lease held).
func (s *SpoolStore) InsertWithBase(nfsPath, spoolFile string, base SpoolBase, acceptedEpoch uint64) (int64, error) {
	now := time.Now().Unix()
	ino, mt, ct, ln := base.columns()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := rejectCompletionPath(s.db, nfsPath, false); err != nil {
		return 0, err
	}
	var active bool
	if err := s.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM spool_entries WHERE nfs_path=?
	 AND drain_state IN ('writing','ready','draining') AND retired_by=0)`, nfsPath).Scan(&active); err != nil {
		return 0, err
	}
	if active {
		return 0, ErrMirrorConflict
	}
	res, err := s.db.Exec(
		`INSERT INTO spool_entries (nfs_path, spool_file, size, drain_state, drain_attempts, created_at, updated_at,
		 base_ino, base_mtime_ns, base_ctime_ns, base_len, accepted_epoch)
		 VALUES (?, ?, 0, ?, 0, ?, ?, ?, ?, ?, ?, ?)`,
		nfsPath, spoolFile, DrainWriting, now, now, ino, mt, ct, ln, int64(acceptedEpoch),
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
	if err := s.checkReplacementLocked(id); err != nil {
		return err
	}
	if err := rejectCompletionHold(s.db, id); err != nil {
		return err
	}
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

// ReopenReady transitions an unclaimed, self-contained row from ready back to
// writing. It is the inverse of MarkReady for a continuation WRITE that arrives
// after the idle sweeper finalized a file but before the drainer claimed it.
//
// The punched_end predicate is part of the CAS, not a caller-side check. A row
// with a streamed/punched prefix is no longer self-contained in its spool file;
// reopening that file as an ordinary writer could replace already-drained bytes
// with holes. Such rows keep the normal ready->draining path and return false.
// A concurrent drainer wins by changing ready->draining first, in which case
// this CAS also returns false and the caller waits/defers to the backend path.
func (s *SpoolStore) ReopenReady(id int64) (bool, error) {
	now := time.Now().Unix()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.checkReplacementLocked(id); err != nil {
		return false, err
	}
	if err := rejectCompletionHold(s.db, id); err != nil {
		return false, err
	}
	res, err := s.db.Exec(
		`UPDATE spool_entries SET drain_state=?, updated_at=?
		 WHERE id=? AND drain_state=? AND punched_end=0
		 AND retired_by=0 AND NOT EXISTS (SELECT 1 FROM spool_bindings WHERE spool_id=?)`,
		DrainWriting, now, id, DrainReady, id,
	)
	if err != nil {
		return false, fmt.Errorf("spool reopen ready %d: %w", id, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// MarkDraining transitions a self-contained ready row to draining. A punched
// source needs its operation-owned stage and ClaimPunchedSpool instead. A row
// held as copied from another Mac (clone_hold) is never claimed.
func (s *SpoolStore) MarkDraining(id int64) (bool, error) {
	return s.markDraining(id, false)
}

// MarkDrainingWithoutProjectLease atomically clears a stale project wait while
// claiming a ready row. Only a dispatcher that freshly observed no lease
// client may call it; the worker still checks the live lease gate before any
// backend write. All other ordinary claim guards are shared with MarkDraining.
func (s *SpoolStore) MarkDrainingWithoutProjectLease(id int64) (bool, error) {
	return s.markDraining(id, true)
}

func (s *SpoolStore) markDraining(id int64, noProjectLease bool) (bool, error) {
	now := time.Now().Unix()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.checkReplacementLocked(id); err != nil {
		return false, err
	}
	if err := rejectCompletionHold(s.db, id); err != nil {
		return false, err
	}
	setClause := "drain_state=?, updated_at=?"
	waitGuard := " AND wait_project=''"
	if noProjectLease {
		setClause = "drain_state=?, wait_project='', updated_at=?"
		waitGuard = ""
	}
	res, err := s.db.Exec(
		`UPDATE spool_entries SET `+setClause+`
		 WHERE id=? AND drain_state=? AND punched_end=0 AND clone_hold='' AND qualification_hold=''`+waitGuard+`
		 AND retired_by=0 AND retry_after_ns<=?
		 AND NOT EXISTS (SELECT 1 FROM spool_entries newer WHERE newer.nfs_path=spool_entries.nfs_path AND newer.id>spool_entries.id AND newer.retired_by=0)`,
		DrainDraining, now, id, DrainReady, time.Now().UnixNano(),
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
// SetPunchedEnd durably records how much of a spool file has been drained to the
// backend and punched away.
//
// THIS MUST COMMIT BEFORE THE CORRESPONDING PUNCH. F_PUNCHHOLE leaves the file's
// logical size unchanged, so a half-punched spool file is indistinguishable on
// disk from a complete one — and boot recovery decides intactness by comparing
// sizes. Punch first and crash, and the next boot re-drains the holey file,
// uploading its zero prefix over real bytes already durable at the backend:
// silent, total loss of that prefix.
//
// The two failure directions are deliberately asymmetric. Persisted-but-not-
// punched costs a redundant re-copy on the next boot and is completely safe;
// punched-but-not-persisted is unrecoverable. So this is ordered first, and its
// failure aborts the advance.
//
// MONOTONIC via MAX: punching cannot be undone, so a lower value must never
// overwrite a higher one. A retry that recomputes a smaller boundary (or an
// out-of-order write under concurrency) would otherwise re-expose a range that
// is already a hole. Same reasoning as UpdateSize's MAX, and unlike that case
// there is no legitimate shrink at all.
func (s *SpoolStore) SetPunchedEnd(id int64, end int64) error {
	if end < 0 {
		return fmt.Errorf("spool set punched_end %d: negative end %d", id, end)
	}
	now := time.Now().Unix()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.checkReplacementLocked(id); err != nil {
		return err
	}
	if err := rejectCompletionHold(s.db, id); err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("spool set punched_end %d=%d begin: %w", id, end, err)
	}
	defer tx.Rollback()
	res, err := tx.Exec(
		`UPDATE spool_entries SET punched_end=MAX(punched_end, ?), updated_at=? WHERE id=?`,
		end, now, id,
	)
	if err != nil {
		return fmt.Errorf("spool set punched_end %d=%d: %w", id, end, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("spool set punched_end %d=%d rows affected: %w", id, end, err)
	}
	if n != 1 {
		return fmt.Errorf("spool set punched_end %d=%d: updated %d rows, want 1", id, end, n)
	}
	var persisted int64
	if err := tx.QueryRow(`SELECT punched_end FROM spool_entries WHERE id=?`, id).Scan(&persisted); err != nil {
		return fmt.Errorf("spool set punched_end %d=%d verify: %w", id, end, err)
	}
	if persisted < end {
		return fmt.Errorf("spool set punched_end %d=%d: persisted boundary %d is too low", id, end, persisted)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("spool set punched_end %d=%d commit: %w", id, end, err)
	}
	return nil
}

// PunchedEnd reads the persisted punched boundary for one row.
func (s *SpoolStore) PunchedEnd(id int64) (int64, error) {
	var end int64
	err := s.db.QueryRow(`SELECT punched_end FROM spool_entries WHERE id=?`, id).Scan(&end)
	if err != nil {
		return 0, fmt.Errorf("spool get punched_end %d: %w", id, err)
	}
	return end, nil
}

func (s *SpoolStore) MarkDone(id int64) (bool, error) {
	now := time.Now().Unix()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.checkReplacementLocked(id); err != nil {
		return false, err
	}
	if err := rejectCompletionHold(s.db, id); err != nil {
		return false, err
	}
	if err := rejectBoundSpool(s.db, id); err != nil {
		return false, err
	}
	res, err := s.db.Exec(
		`UPDATE spool_entries SET drain_state=?, updated_at=? WHERE id=? AND retired_by=0`,
		DrainDone, now, id,
	)
	if err != nil {
		return false, fmt.Errorf("spool mark done %d: %w", id, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// batchMarkDoneTx conditionally completes the current unpunched draining row.
// The caller holds s.writeMu and opened tx after acquiring that lock, so cancel
// and requeue cannot interleave with qualification or metadata publication.
// Results are provisional until the caller commits the cross-table transaction.
func (s *SpoolStore) batchMarkDoneTx(tx *sql.Tx, items []DrainCommitItem) ([]DrainCommitResult, error) {
	results := make([]DrainCommitResult, len(items))
	for i, it := range items {
		res, err := tx.Exec(
			`UPDATE spool_entries SET drain_state=?, updated_at=?
			 WHERE id=? AND nfs_path=? AND spool_file=? AND size=? AND drain_state=? AND punched_end=0 AND retired_by=0`,
			DrainDone, it.Mtime.Unix(), it.SpoolID, it.NFSPath, it.SpoolFile, it.Size, DrainDraining,
		)
		if err != nil {
			return nil, fmt.Errorf("batch mark done %d: %w", it.SpoolID, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("batch mark done rows affected %d: %w", it.SpoolID, err)
		}
		results[i] = DrainCommitResult{SpoolID: it.SpoolID, Done: n > 0}
	}
	return results, nil
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
// finds 0 rows). No row can be half-claimed across the cancel. The batched
// mark-done (Store.BatchDrainComplete → batchMarkDoneTx) also runs under this
// same writeMu, so it is likewise serialized against this cancel.
func (s *SpoolStore) DeleteActiveByPath(nfsPath string) ([]*SpoolRow, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.checkReplacementLocked(0, nfsPath); err != nil {
		return nil, err
	}
	if err := s.refuseLatestFailedUnder(nfsPath, true); err != nil {
		return nil, err
	}
	if err := rejectCompletionPath(s.db, nfsPath, false); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(
		`SELECT id, nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, suspect_zero_tail, punched_end, created_at, updated_at, pause_since_ns, retry_after_ns, qualification_hold, retired_by
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
	// Test-only sync point between the SELECT (decision-to-delete) and the
	// DELETE, used by the QA-37 concurrent-interleaving regression test to force
	// the exact bug window. nil in production (zero cost). Fires only when the
	// SELECT actually collected rows — i.e. the cancel has decided to delete.
	if hook := deleteActiveByPathSelectHook; hook != nil {
		hook(nfsPath)
	}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// A canceled accepted save cannot make an older failed image current.
	if _, err := tx.Exec(`UPDATE spool_entries SET retired_by=(SELECT MAX(id) FROM spool_entries
	 WHERE nfs_path=? AND drain_state IN ('writing','ready','draining'))
	 WHERE nfs_path=? AND retired_by=0 AND id<(SELECT MAX(id) FROM spool_entries
	 WHERE nfs_path=? AND drain_state IN ('writing','ready','draining'))`, nfsPath, nfsPath, nfsPath); err != nil {
		return nil, fmt.Errorf("spool delete-active retire history %q: %w", nfsPath, err)
	}
	if _, err := tx.Exec(
		`DELETE FROM spool_entries WHERE nfs_path=? AND drain_state IN ('writing','ready','draining')`,
		nfsPath,
	); err != nil {
		return out, fmt.Errorf("spool delete-active-by-path delete %q: %w", nfsPath, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
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
	return s.MigrateActivePathsWithBase(oldPath, newPath, nil)
}

// MigrateActivePathsWithBase is MigrateActivePaths that also re-derives each
// migrated row's base identity for its NEW path (H-8B). resolve receives the
// row's new path and its current base and accepted epoch, and returns the
// values to store. It runs under the spool write lock and must not block
// (callers pass precomputed values). A nil resolve keeps the row's base.
// Every migrated row also leaves any project wait: its project may have
// changed with its path.
func (s *SpoolStore) MigrateActivePathsWithBase(oldPath, newPath string, resolve func(newRowPath string, old SpoolBase, oldEpoch uint64) (SpoolBase, uint64)) ([]SpoolPathMigration, error) {
	if oldPath == "" || oldPath == newPath {
		return nil, nil
	}
	now := time.Now().Unix()
	prefix := oldPath + "/"

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.checkReplacementLocked(0, oldPath, newPath); err != nil {
		return nil, err
	}
	if err := s.refuseLatestFailedUnder(oldPath, true); err != nil {
		return nil, err
	}
	if err := s.refuseLatestFailedUnder(newPath, true); err != nil {
		return nil, err
	}
	if err := rejectCompletionPath(s.db, oldPath, true); err != nil {
		return nil, err
	}
	if err := rejectCompletionPath(s.db, newPath, true); err != nil {
		return nil, err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("spool migrate-paths begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op after Commit

	rows, err := tx.Query(
		`SELECT id, nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, suspect_zero_tail, punched_end, created_at, updated_at, pause_since_ns, retry_after_ns, qualification_hold, retired_by
		 FROM spool_entries
			 WHERE drain_state IN ('writing','ready','draining') AND retired_by=0
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
	// Birth paths and files are immutable custody. A rename of any member of
	// this cohort would either detach the row from its birth tuple or requeue
	// a draining row with an empty tuple. Refuse the whole transaction first.
	for _, r := range active {
		var birthMarked int
		if err := tx.QueryRow(`SELECT count(*) FROM spool_bindings WHERE spool_id=?
		 AND (birth_id<>'' OR birth_path<>'' OR birth_file<>'' OR birth_identity<>'')`, r.ID).Scan(&birthMarked); err != nil {
			return nil, err
		}
		if birthMarked != 0 {
			return nil, ErrMirrorConflict
		}
	}

	out := make([]SpoolPathMigration, 0, len(active))
	for _, r := range active {
		var phase string
		bindingErr := tx.QueryRow(`SELECT phase FROM spool_bindings WHERE spool_id=?`, r.ID).Scan(&phase)
		if bindingErr != nil && bindingErr != sql.ErrNoRows {
			return nil, bindingErr
		}
		if bindingErr == nil && phase != "bound" {
			return nil, ErrMirrorConflict
		}
		newRowPath := newPath + r.NFSPath[len(oldPath):]
		var destinationActive bool
		if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM spool_entries WHERE nfs_path=?
		 AND retired_by=0 AND drain_state IN ('writing','ready','draining'))`, newRowPath).Scan(&destinationActive); err != nil {
			return nil, err
		}
		if destinationActive {
			return nil, ErrMirrorConflict
		}
		// The rename is a namespace event. Retire retained source and target
		// histories before moving the winner, in this same transaction.
		if _, err := tx.Exec(`UPDATE spool_entries SET retired_by=? WHERE retired_by=0
		 AND ((nfs_path=? AND id<?) OR nfs_path=?)`,
			r.ID, r.NFSPath, r.ID, newRowPath); err != nil {
			return nil, err
		}
		m := SpoolPathMigration{OldID: r.ID, NewID: r.ID, OldPath: r.NFSPath, NewPath: newRowPath}
		var oIno, oMt, oCt, oLen, oEpoch int64
		var oHold string
		if err := tx.QueryRow(`SELECT base_ino, base_mtime_ns, base_ctime_ns, base_len, accepted_epoch, clone_hold
		 FROM spool_entries WHERE id=?`, r.ID).Scan(&oIno, &oMt, &oCt, &oLen, &oEpoch, &oHold); err != nil {
			return nil, fmt.Errorf("spool migrate-paths base %d: %w", r.ID, err)
		}
		if oEpoch < 0 {
			oEpoch = 0
		}
		newBase, newEpoch := spoolBaseFromColumns(oIno, oMt, oCt, oLen), uint64(oEpoch)
		if resolve != nil {
			newBase, newEpoch = resolve(newRowPath, newBase, newEpoch)
		}
		bIno, bMt, bCt, bLen := newBase.columns()

		switch r.DrainState {
		case DrainDraining:
			var pauseNS, retryNS int64
			if !r.PauseSince.IsZero() {
				pauseNS = r.PauseSince.UnixNano()
			}
			if !r.RetryAfter.IsZero() {
				retryNS = r.RetryAfter.UnixNano()
			}
			deleted, err := tx.Exec(`DELETE FROM spool_entries WHERE id=?`, r.ID)
			if err != nil {
				return nil, fmt.Errorf("spool migrate-paths cancel draining %d: %w", r.ID, err)
			}
			if err := exactlyOneRow(deleted); err != nil {
				return nil, err
			}
			// created_at preserved so the requeued row keeps its FIFO
			// position in ListReady; attempts carried (the cancelled
			// attempt wasn't a data failure, but the budget shouldn't
			// reset on rename either). Both suspect_zero_tail and punched_end
			// follow the SAME spool file. Dropping punched_end would make its
			// holes look like a complete local copy after requeue.
			res, err := tx.Exec(
				`INSERT INTO spool_entries (nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, suspect_zero_tail, punched_end, created_at, updated_at,
				 base_ino, base_mtime_ns, base_ctime_ns, base_len, accepted_epoch, clone_hold, pause_since_ns, retry_after_ns, qualification_hold, retired_by)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				newRowPath, r.SpoolFile, r.Size, r.SHA256, DrainReady, r.DrainAttempts, r.LastError, r.SuspectZeroTail, r.PunchedEnd, r.CreatedAt.Unix(), now,
				bIno, bMt, bCt, bLen, int64(newEpoch), oHold, pauseNS, retryNS, r.QualificationHold, r.RetiredBy,
			)
			if err != nil {
				return nil, fmt.Errorf("spool migrate-paths requeue %d: %w", r.ID, err)
			}
			if err := exactlyOneRow(res); err != nil {
				return nil, err
			}
			newID, err := res.LastInsertId()
			if err != nil {
				return nil, fmt.Errorf("spool migrate-paths requeue lastid %d: %w", r.ID, err)
			}
			m.NewID = newID
			m.Requeued = true
			if bindingErr == nil {
				// Preserve the old ID as a tombstone: a stale worker must never
				// become eligible for path-only legacy completion after rename.
				res, err := tx.Exec(`INSERT INTO spool_bindings(spool_id,incarnation)
 SELECT ?,incarnation FROM spool_bindings WHERE spool_id=? AND phase='bound'`, newID, r.ID)
				if err != nil {
					return nil, err
				}
				if err := exactlyOneRow(res); err != nil {
					return nil, err
				}
				res, err = tx.Exec(`UPDATE spool_bindings SET phase='retired' WHERE spool_id=? AND phase='bound'`, r.ID)
				if err != nil {
					return nil, err
				}
				if err := exactlyOneRow(res); err != nil {
					return nil, err
				}
			}

		default: // writing, ready
			res, err := tx.Exec(
				`UPDATE spool_entries SET nfs_path=?, updated_at=?, base_ino=?, base_mtime_ns=?, base_ctime_ns=?,
				 base_len=?, accepted_epoch=?, wait_project='' WHERE id=?`,
				newRowPath, now, bIno, bMt, bCt, bLen, int64(newEpoch), r.ID,
			)
			if err != nil {
				return nil, fmt.Errorf("spool migrate-paths update %d: %w", r.ID, err)
			}
			if err := exactlyOneRow(res); err != nil {
				return nil, err
			}
		}
		out = append(out, m)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("spool migrate-paths commit: %w", err)
	}
	return out, nil
}

// Called with writeMu. A terminal save owns its pathname until an explicit
// recovery decision; checking here closes the RAM-check/SQL-mutation race.
func (s *SpoolStore) refuseLatestFailedUnder(path string, subtree bool) error {
	prefix := path + "/"
	var blocked bool
	query := `SELECT EXISTS(SELECT 1 FROM spool_entries f WHERE f.drain_state='failed' AND f.retired_by=0
	 AND f.nfs_path=? AND f.id=(SELECT MAX(x.id) FROM spool_entries x WHERE x.nfs_path=f.nfs_path AND x.retired_by=0))`
	args := []any{path}
	if subtree {
		query = `SELECT EXISTS(SELECT 1 FROM spool_entries f WHERE f.drain_state='failed' AND f.retired_by=0
	 AND (f.nfs_path=? OR substr(f.nfs_path,1,?)=?)
	 AND f.id=(SELECT MAX(x.id) FROM spool_entries x WHERE x.nfs_path=f.nfs_path AND x.retired_by=0))`
		args = []any{path, utf8.RuneCountInString(prefix), prefix}
	}
	if err := s.db.QueryRow(query, args...).Scan(&blocked); err != nil {
		return err
	}
	if blocked {
		return fmt.Errorf("unresolved failed local save under %q: %w", path, syscall.EIO)
	}
	return nil
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
	if err := s.checkReplacementLocked(id); err != nil {
		return false, err
	}
	if err := rejectCompletionHold(s.db, id); err != nil {
		return false, err
	}
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

// FailClaimedDrain changes only the exact draining source the worker claimed.
// Local quarantine movement may run while writeMu is held, so a retry, delete
// or rename cannot take the same spool file between the CAS and its move.
func (s *SpoolStore) FailClaimedDrain(id int64, path, file, reason string, integrityFailure bool, moveLocal func() error) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.checkReplacementLocked(id); err != nil {
		return false, err
	}
	if err := rejectCompletionHold(s.db, id); err != nil {
		return false, err
	}
	hold := ""
	if integrityFailure {
		hold = reason
	}
	res, err := s.db.Exec(`UPDATE spool_entries SET drain_state=?, drain_attempts=drain_attempts+1,
	 last_error=?, qualification_hold=?, updated_at=? WHERE id=? AND nfs_path=? AND spool_file=? AND drain_state=? AND retired_by=0`,
		DrainFailed, reason, hold, time.Now().Unix(), id, path, file, DrainDraining)
	if err != nil {
		return false, fmt.Errorf("fail claimed drain %d: %w", id, err)
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return false, nil
	}
	if moveLocal != nil {
		return true, moveLocal()
	}
	return true, nil
}

// HoldUnqualified preserves a source whose complete saved-byte image cannot
// be proved. Neither ordinary dispatch nor RetryFailed may release this hold.
func (s *SpoolStore) HoldUnqualified(id int64, path, file, reason string) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.checkReplacementLocked(id); err != nil {
		return false, err
	}
	if err := rejectCompletionHold(s.db, id); err != nil {
		return false, err
	}
	res, err := s.db.Exec(`UPDATE spool_entries SET drain_state=?, qualification_hold=?, last_error=?, updated_at=?
	 WHERE id=? AND nfs_path=? AND spool_file=? AND drain_state IN ('writing','ready','failed')`,
		DrainFailed, reason, reason, time.Now().Unix(), id, path, file)
	if err != nil {
		return false, fmt.Errorf("spool hold unqualified %d: %w", id, err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// RequeueAfter records the failure, pause authority and retry eligibility in
// one durable transition. A wake from another row cannot claim this row early.
func (s *SpoolStore) RequeueAfter(id int64, reason string, pauseSince, retryAfter time.Time) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.checkReplacementLocked(id); err != nil {
		return err
	}
	if err := rejectCompletionHold(s.db, id); err != nil {
		return err
	}
	var since, due int64
	if !pauseSince.IsZero() {
		since = pauseSince.UnixNano()
	}
	if !retryAfter.IsZero() {
		due = retryAfter.UnixNano()
	}
	res, err := s.db.Exec(`UPDATE spool_entries SET drain_state=?, drain_attempts=drain_attempts+1,
	 last_error=?, pause_since_ns=CASE WHEN pause_since_ns=0 THEN ? ELSE pause_since_ns END,
	 retry_after_ns=?, updated_at=? WHERE id=? AND drain_state=?`,
		DrainReady, reason, since, due, time.Now().Unix(), id, DrainDraining)
	if err != nil {
		return fmt.Errorf("spool requeue after %d: %w", id, err)
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("spool requeue after %d: row no longer draining", id)
	}
	return nil
}

// MarkSuspectZeroTail records the #104 zero-tail-detection marker on a row.
// detail is the compact JSON blob {detected_at,size,contiguous,holes} the
// finalize-time detector assembled. State-independent by design (UPDATE by id
// only, drain_state untouched, updated_at untouched): the marking is pure
// surfacing — the row's lifecycle (ready→draining→done) proceeds exactly as
// if the marker were absent, and a marking must never reset the age/staleness
// signals derived from updated_at. Called between finalize and MarkReady, so
// the common case marks a `writing` row; a scrubber/cancel race that removed
// the row first makes this affect 0 rows, which is fine — detection-only.
func (s *SpoolStore) MarkSuspectZeroTail(id int64, detail string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.Exec(
		`UPDATE spool_entries SET suspect_zero_tail=? WHERE id=?`,
		detail, id,
	)
	if err != nil {
		return fmt.Errorf("spool mark suspect-zero-tail %d: %w", id, err)
	}
	return nil
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
		`SELECT EXISTS(SELECT 1 FROM spool_entries WHERE nfs_path=? AND id>? AND retired_by=0)`,
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
	if err := s.checkReplacementLocked(id); err != nil {
		return err
	}
	if err := rejectCompletionHold(s.db, id); err != nil {
		return err
	}
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
	if err := s.checkReplacementLocked(id); err != nil {
		return err
	}
	if err := rejectCompletionHold(s.db, id); err != nil {
		return err
	}
	_, err := s.db.Exec(
		`UPDATE spool_entries SET drain_state=?, updated_at=? WHERE id=?`,
		DrainReady, now, id,
	)
	if err != nil {
		return fmt.Errorf("spool reset ready %d: %w", id, err)
	}
	return nil
}

// DeferReady parks a claimed row until the durable per-row due time. It does
// not consume an attempt for an unavailable mount or offline project.
func (s *SpoolStore) DeferReady(id int64, due time.Time) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := rejectCompletionHold(s.db, id); err != nil {
		return err
	}
	_, err := s.db.Exec(`UPDATE spool_entries SET drain_state=?, retry_after_ns=?, updated_at=?
	 WHERE id=? AND drain_state=?`, DrainReady, due.UnixNano(), time.Now().Unix(), id, DrainDraining)
	if err != nil {
		return fmt.Errorf("spool defer ready %d: %w", id, err)
	}
	return nil
}

// RequeueUnadmittedDrain is only for a path lease refused before completion
// SQL. It cannot move a held, bound, replaced, or no-longer-draining row.
func (s *SpoolStore) RequeueUnadmittedDrain(item DrainCommitItem) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(`UPDATE spool_entries SET drain_state='ready',updated_at=?
 WHERE id=? AND nfs_path=? AND spool_file=? AND size=? AND drain_state='draining' AND punched_end=0
 AND NOT EXISTS (SELECT 1 FROM spool_completion_holds h WHERE h.spool_id=spool_entries.id)
 AND NOT EXISTS (SELECT 1 FROM spool_bindings b WHERE b.spool_id=spool_entries.id)`,
		time.Now().Unix(), item.SpoolID, item.NFSPath, item.SpoolFile, item.Size)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
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
	if err := s.checkReplacementLocked(id); err != nil {
		return false, err
	}
	if err := rejectCompletionHold(s.db, id); err != nil {
		return false, err
	}
	res, err := s.db.Exec(
		`UPDATE spool_entries SET drain_state=?, drain_attempts=0, pause_since_ns=0, retry_after_ns=0, updated_at=?
		 WHERE id=? AND drain_state=? AND qualification_hold='' AND retired_by=0
		 AND NOT EXISTS (SELECT 1 FROM spool_entries newer WHERE newer.nfs_path=spool_entries.nfs_path AND newer.id>spool_entries.id AND newer.retired_by=0)`,
		DrainReady, now, id, DrainFailed,
	)
	if err != nil {
		return false, fmt.Errorf("spool reset for retry %d: %w", id, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ResumeFailedPaused preserves the original risk deadline and attempt count.
// Boot recovery may re-drive a paused row before its cap, but may not grant a
// new 24-hour window merely because the process restarted.
func (s *SpoolStore) ResumeFailedPaused(id int64) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.checkReplacementLocked(id); err != nil {
		return false, err
	}
	if err := rejectCompletionHold(s.db, id); err != nil {
		return false, err
	}
	res, err := s.db.Exec(`UPDATE spool_entries SET drain_state=?, updated_at=?
	 WHERE id=? AND drain_state=? AND pause_since_ns>0 AND qualification_hold='' AND retired_by=0
	 AND NOT EXISTS (SELECT 1 FROM spool_entries newer WHERE newer.nfs_path=spool_entries.nfs_path AND newer.id>spool_entries.id AND newer.retired_by=0)`,
		DrainReady, time.Now().Unix(), id, DrainFailed)
	if err != nil {
		return false, fmt.Errorf("spool resume paused %d: %w", id, err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// Get returns the row for id, or sql.ErrNoRows if it doesn't exist.
func (s *SpoolStore) Get(id int64) (*SpoolRow, error) {
	row := s.db.QueryRow(
		`SELECT id, nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, suspect_zero_tail, punched_end, created_at, updated_at, pause_since_ns, retry_after_ns, qualification_hold, retired_by
		 FROM spool_entries WHERE id=?`,
		id,
	)
	return scanSpoolRow(row.Scan)
}

// LookupByPath returns the most recent (highest id) active row for nfsPath,
// or sql.ErrNoRows if none exists.
// Reads of in-progress writes consult this to serve from spool.
func (s *SpoolStore) LookupByPath(nfsPath string) (*SpoolRow, error) {
	row := s.db.QueryRow(
		`SELECT id, nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, suspect_zero_tail, punched_end, created_at, updated_at, pause_since_ns, retry_after_ns, qualification_hold, retired_by
		 FROM spool_entries
			 WHERE nfs_path=? AND drain_state IN ('writing','ready','draining') AND retired_by=0
		 ORDER BY id DESC LIMIT 1`,
		nfsPath,
	)
	return scanSpoolRow(row.Scan)
}

// LookupUndrainedByPath includes failed rows, whose bytes may still be only
// in the spool. Residency uses this while NFS reads use LookupByPath.
func (s *SpoolStore) LookupUndrainedByPath(nfsPath string) (*SpoolRow, error) {
	row := s.db.QueryRow(
		`SELECT id, nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, suspect_zero_tail, punched_end, created_at, updated_at, pause_since_ns, retry_after_ns, qualification_hold, retired_by
		 FROM spool_entries
		 WHERE nfs_path=? AND drain_state IN ('writing','ready','draining','failed')
		 ORDER BY id DESC LIMIT 1`,
		nfsPath,
	)
	return scanSpoolRow(row.Scan)
}

// ListReady returns self-contained ready rows ordered by created_at. Punched
// sources are excluded because ordinary whole-file drain would copy holes.
// Rows waiting for a project lease (non-empty wait_project) are excluded: they stay
// ready (so reads, deletes and renames treat them as live) but are not
// dispatched until ReleaseProjectHold clears the wait.
func (s *SpoolStore) ListReady(limit int) ([]*SpoolRow, error) {
	return s.listReady(limit, false)
}

// ListReadyIncludingWaiting is ListReady that also returns rows waiting for a
// project lease. The drainer uses it whenever no lease client is wired, so a
// wait can never strand a row.
func (s *SpoolStore) ListReadyIncludingWaiting(limit int) ([]*SpoolRow, error) {
	return s.listReady(limit, true)
}

// Rows held as copied from another Mac are excluded from both, always: unlike
// a project wait, nothing releases them but an explicit decision
// (ReleaseCloneHeld / DeleteCloneHeld).
func (s *SpoolStore) listReady(limit int, includeWaiting bool) ([]*SpoolRow, error) {
	waitClause := " AND wait_project=''"
	if includeWaiting {
		waitClause = ""
	}
	rows, err := s.db.Query(
		`SELECT id, nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, suspect_zero_tail, punched_end, created_at, updated_at, pause_since_ns, retry_after_ns, qualification_hold, retired_by
			 FROM spool_entries WHERE drain_state=? AND punched_end=0 AND clone_hold='' AND qualification_hold='' AND retired_by=0 AND replacement_witness IS NULL AND retry_after_ns<=?
			 AND NOT EXISTS (SELECT 1 FROM spool_entries newer WHERE newer.nfs_path=spool_entries.nfs_path AND newer.id>spool_entries.id AND newer.retired_by=0)`+waitClause+` ORDER BY created_at ASC LIMIT ?`,
		DrainReady, time.Now().UnixNano(), limit,
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
		`SELECT id, nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, suspect_zero_tail, punched_end, created_at, updated_at, pause_since_ns, retry_after_ns, qualification_hold, retired_by
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
		`SELECT id, nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, suspect_zero_tail, punched_end, created_at, updated_at, pause_since_ns, retry_after_ns, qualification_hold, retired_by
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
	if err := s.checkReplacementLocked(id); err != nil {
		return err
	}
	if err := rejectCompletionHold(s.db, id); err != nil {
		return err
	}
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
		`DELETE FROM spool_entries WHERE drain_state=? AND updated_at < ?
 AND replacement_witness IS NULL
 AND NOT EXISTS (SELECT 1 FROM spool_completion_holds h WHERE h.spool_id=spool_entries.id)`,
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

// CountPendingSymlinks is the cheap status-poll counterpart to the ordered
// materialization listing. An unreadable table is not an empty queue.
func (s *SpoolStore) CountPendingSymlinks() (int, error) {
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM pending_symlinks`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count pending symlinks: %w", err)
	}
	return count, nil
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
	var lastErr, suspect sql.NullString
	var createdAt, updatedAt, pauseSince, retryAfter int64
	if err := scan(&r.ID, &r.NFSPath, &r.SpoolFile, &r.Size, &r.SHA256, &state, &r.DrainAttempts, &lastErr, &suspect, &r.PunchedEnd, &createdAt, &updatedAt, &pauseSince, &retryAfter, &r.QualificationHold, &r.RetiredBy); err != nil {
		return nil, err
	}
	r.DrainState = DrainState(state)
	if lastErr.Valid {
		r.LastError = lastErr.String
	}
	if suspect.Valid {
		r.SuspectZeroTail = suspect.String
	}
	r.CreatedAt = time.Unix(createdAt, 0)
	r.UpdatedAt = time.Unix(updatedAt, 0)
	if pauseSince > 0 {
		r.PauseSince = time.Unix(0, pauseSince)
	}
	if retryAfter > 0 {
		r.RetryAfter = time.Unix(0, retryAfter)
	}
	return &r, nil
}

// ---------------------------------------------------------------------------
// H-8B: base identity and project-lease state per spool row.
// ---------------------------------------------------------------------------

// SpoolBaseKind says what a row knew about the backend file at its name when
// the write opened.
type SpoolBaseKind uint8

const (
	// SpoolBaseUnknown: the row predates H-8B (or was created by a path that
	// records nothing). The drain keeps the pre-0.5 behaviour for it.
	SpoolBaseUnknown SpoolBaseKind = iota
	// SpoolBaseAbsent: the name did not exist from this Mac's view when the
	// write opened. A file found there at drain time belongs to someone else.
	SpoolBaseAbsent
	// SpoolBasePresent: the exact backend identity (inode, mtime, ctime and
	// length, nanoseconds) of the file this write replaces.
	SpoolBasePresent
	// SpoolBaseWeak: only the mirror's length and whole-second mtime were
	// known (the Mac was offline, or the backend lookup timed out).
	SpoolBaseWeak
	// SpoolBaseClaimed: a drain attempt of THIS row already wrote inode Ino
	// (in place, or by installing a new name). A retry must recognise its own
	// partial or installed file instead of treating it as another writer's.
	SpoolBaseClaimed
	// SpoolBaseMoved: a present base whose inode was then renamed with the
	// pending row. JuiceFS sets the moved inode's ctime to the rename time
	// (doRename: iattr.Ctime = now), so ctime cannot be carried; inode,
	// mtime (ns) and length are unchanged by a rename and are still compared.
	SpoolBaseMoved
	// SpoolBaseReplace: a spool-only file was renamed over an existing
	// backend file (an atomic save: write a temp, rename it over the
	// original). Ino, mtime (ns) and length identify the file it replaces.
	// The drain does not truncate that file: it stages the new bytes and
	// installs them with one replacing rename, only while the destination is
	// still that file, so other Macs see old -> new and never a missing name.
	// ctime is not compared (as for moved): a metadata-only change to the
	// old file (xattr, chmod) must not turn every such save into a conflict.
	SpoolBaseReplace
	// SpoolBaseAtomicConflict marks a temp-file rename whose destination
	// identity could not be proved. It must stage and keep any old destination;
	// a collision publishes a conflict copy. This intent survives restart.
	SpoolBaseAtomicConflict
)

func (k SpoolBaseKind) String() string {
	switch k {
	case SpoolBaseAbsent:
		return "absent"
	case SpoolBasePresent:
		return "present"
	case SpoolBaseWeak:
		return "weak"
	case SpoolBaseClaimed:
		return "claimed"
	case SpoolBaseMoved:
		return "moved"
	case SpoolBaseReplace:
		return "replace"
	case SpoolBaseAtomicConflict:
		return "atomic-conflict"
	}
	return "unknown"
}

// SpoolBase is the base identity recorded per spool row.
//
// Column encoding (all INTEGER NOT NULL DEFAULT 0, so a migrated legacy row
// is UNKNOWN by construction):
//
//	unknown  base_ino = 0
//	absent   base_ino = -1
//	weak     base_ino = -2, base_mtime_ns, base_len
//	present  base_ino > 0, base_ctime_ns >= 0, all four fields
//	claimed  base_ino > 0, base_ctime_ns = -1
//	moved    base_ino > 0, base_ctime_ns = -2, base_mtime_ns, base_len
//	replace  base_ino > 0, base_ctime_ns = -3, base_mtime_ns, base_len
//
// A build that predates "replace" reads it as present with ctime -3, which
// never matches a real file: it publishes a conflict copy (the safe side).
type SpoolBase struct {
	Kind    SpoolBaseKind
	Ino     uint64
	MtimeNs int64
	CtimeNs int64
	Len     int64
}

const (
	spoolBaseInoAbsent           = -1
	spoolBaseInoWeak             = -2
	spoolBaseCtimeAtomicConflict = -4
	spoolBaseCtimeClaim          = -1
	spoolBaseCtimeMoved          = -2
	spoolBaseCtimeRepl           = -3
)

func (b SpoolBase) columns() (ino, mtimeNs, ctimeNs, length int64) {
	switch b.Kind {
	case SpoolBaseAbsent:
		return spoolBaseInoAbsent, 0, 0, 0
	case SpoolBaseAtomicConflict:
		return spoolBaseInoAbsent, 0, spoolBaseCtimeAtomicConflict, 0
	case SpoolBaseWeak:
		return spoolBaseInoWeak, b.MtimeNs, 0, b.Len
	case SpoolBasePresent:
		if b.Ino == 0 || b.Ino > 1<<62 || b.CtimeNs < 0 {
			return 0, 0, 0, 0
		}
		return int64(b.Ino), b.MtimeNs, b.CtimeNs, b.Len
	case SpoolBaseClaimed:
		if b.Ino == 0 || b.Ino > 1<<62 {
			return 0, 0, 0, 0
		}
		return int64(b.Ino), 0, spoolBaseCtimeClaim, 0
	case SpoolBaseMoved:
		if b.Ino == 0 || b.Ino > 1<<62 {
			return 0, 0, 0, 0
		}
		return int64(b.Ino), b.MtimeNs, spoolBaseCtimeMoved, b.Len
	case SpoolBaseReplace:
		if b.Ino == 0 || b.Ino > 1<<62 {
			return 0, 0, 0, 0
		}
		return int64(b.Ino), b.MtimeNs, spoolBaseCtimeRepl, b.Len
	}
	return 0, 0, 0, 0
}

func spoolBaseFromColumns(ino, mtimeNs, ctimeNs, length int64) SpoolBase {
	switch {
	case ino == spoolBaseInoAbsent && ctimeNs == spoolBaseCtimeAtomicConflict:
		return SpoolBase{Kind: SpoolBaseAtomicConflict}
	case ino == spoolBaseInoAbsent:
		return SpoolBase{Kind: SpoolBaseAbsent}
	case ino == spoolBaseInoWeak:
		return SpoolBase{Kind: SpoolBaseWeak, MtimeNs: mtimeNs, Len: length}
	case ino > 0 && ctimeNs == spoolBaseCtimeClaim:
		return SpoolBase{Kind: SpoolBaseClaimed, Ino: uint64(ino)}
	case ino > 0 && ctimeNs == spoolBaseCtimeMoved:
		return SpoolBase{Kind: SpoolBaseMoved, Ino: uint64(ino), MtimeNs: mtimeNs, Len: length}
	case ino > 0 && ctimeNs == spoolBaseCtimeRepl:
		return SpoolBase{Kind: SpoolBaseReplace, Ino: uint64(ino), MtimeNs: mtimeNs, Len: length}
	case ino > 0:
		return SpoolBase{Kind: SpoolBasePresent, Ino: uint64(ino), MtimeNs: mtimeNs, CtimeNs: ctimeNs, Len: length}
	}
	return SpoolBase{}
}

// Owner returns the metadata Store this spool store is wired to (nil before
// Store.SetSpoolStore). The drainer reads the mirror through it to learn
// what this Mac saw at a name when a write opened.
func (s *SpoolStore) Owner() *Store { return s.owner.Load() }

// DrainBase returns the row's base identity and accepted lease epoch.
func (s *SpoolStore) DrainBase(id int64) (SpoolBase, uint64, error) {
	var ino, mt, ct, ln, epoch int64
	err := s.db.QueryRow(`SELECT base_ino, base_mtime_ns, base_ctime_ns, base_len, accepted_epoch
	 FROM spool_entries WHERE id=?`, id).Scan(&ino, &mt, &ct, &ln, &epoch)
	if err != nil {
		return SpoolBase{}, 0, err
	}
	if epoch < 0 {
		epoch = 0
	}
	return spoolBaseFromColumns(ino, mt, ct, ln), uint64(epoch), nil
}

// ClaimDrainBase records that a drain of row id is about to write inode ino
// at the row's name. It only touches a row that is still draining.
func (s *SpoolStore) ClaimDrainBase(id int64, ino uint64) error {
	cIno, _, cCt, _ := SpoolBase{Kind: SpoolBaseClaimed, Ino: ino}.columns()
	if cIno <= 0 {
		return fmt.Errorf("spool claim base %d: invalid inode %d", id, ino)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.Exec(`UPDATE spool_entries SET base_ino=?, base_mtime_ns=0, base_ctime_ns=?, base_len=0
	 WHERE id=? AND drain_state=?`, cIno, cCt, id, DrainDraining)
	if err != nil {
		return fmt.Errorf("spool claim base %d: %w", id, err)
	}
	return nil
}

// HoldForProject returns a draining row to ready and parks it until the
// project lease frees (state "waiting_project"). The attempt budget is not
// touched: a refused write is not a failure of the file. Returns false when
// the row is no longer draining (canceled, renamed or already finished).
func (s *SpoolStore) HoldForProject(id int64, project, reason string) (bool, error) {
	if project == "" {
		return false, fmt.Errorf("spool hold %d: empty project", id)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := rejectCompletionHold(s.db, id); err != nil {
		return false, err
	}
	res, err := s.db.Exec(`UPDATE spool_entries SET drain_state=?, wait_project=?, last_error=?, updated_at=?
	 WHERE id=? AND drain_state=?`, DrainReady, project, reason, time.Now().Unix(), id, DrainDraining)
	if err != nil {
		return false, fmt.Errorf("spool hold %d: %w", id, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ReleaseProjectHold makes every row waiting on project dispatchable again.
func (s *SpoolStore) ReleaseProjectHold(project string) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(`UPDATE spool_entries SET wait_project='', updated_at=? WHERE wait_project=?`,
		time.Now().Unix(), project)
	if err != nil {
		return 0, fmt.Errorf("spool release hold %q: %w", project, err)
	}
	return res.RowsAffected()
}

// ReleaseAllProjectHolds clears every project wait. The drainer calls it when
// it runs without a lease client: nothing else would ever release them.
func (s *SpoolStore) ReleaseAllProjectHolds() (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(`UPDATE spool_entries SET wait_project='', updated_at=? WHERE wait_project<>''`,
		time.Now().Unix())
	if err != nil {
		return 0, fmt.Errorf("spool release all holds: %w", err)
	}
	return res.RowsAffected()
}

// WaitingProjects counts active rows waiting per project.
func (s *SpoolStore) WaitingProjects() (map[string]int, error) {
	rows, err := s.db.Query(`SELECT wait_project, count(*) FROM spool_entries
	 WHERE wait_project<>'' AND drain_state IN ('writing','ready','draining') GROUP BY wait_project`)
	if err != nil {
		return nil, fmt.Errorf("spool waiting projects: %w", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var p string
		var n int
		if err := rows.Scan(&p, &n); err != nil {
			return nil, err
		}
		out[p] = n
	}
	return out, rows.Err()
}

// WaitProject returns the project row id waits on ("" = not waiting).
func (s *SpoolStore) WaitProject(id int64) (string, error) {
	var p string
	err := s.db.QueryRow(`SELECT wait_project FROM spool_entries WHERE id=?`, id).Scan(&p)
	return p, err
}

// ActivePaths returns the distinct paths of every active (writing, ready or
// draining) row. The lease heartbeat derives "projects with pending spool"
// from it.
func (s *SpoolStore) ActivePaths() ([]string, error) {
	rows, err := s.db.Query(`SELECT DISTINCT nfs_path FROM spool_entries
	 WHERE drain_state IN ('writing','ready','draining')`)
	if err != nil {
		return nil, fmt.Errorf("spool active paths: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Clone hold: rows copied from another Mac.
// ---------------------------------------------------------------------------

// SpoolCloneHoldReason marks rows found in a spool that was copied from
// another Mac (Migration Assistant, Time Machine restore, cloned disk).
const SpoolCloneHoldReason = "copied_from_other_mac"

// HoldRowsCopiedFromOtherMac marks every row that is not done as held with
// SpoolCloneHoldReason. It runs when the spool instance rotates because the
// spool was created on another Mac, BEFORE the new instance ID is persisted.
//
// Why: those rows are the other Mac's pending writes. That Mac may still
// drain them itself, or may have edited the same files since. Draining the
// copy here would upload stale bytes over its later edits (a row whose base is
// unknown and whose project has no foreign holder is rewritten in place). So
// a held row is never claimed (MarkDraining, ListReady) and never deleted by
// any automatic path; it keeps its drain_state, so it still counts as pending
// (PendingStats, ListForStatus, the Quit guard) until the user decides.
func (s *SpoolStore) HoldRowsCopiedFromOtherMac() (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(`UPDATE spool_entries SET clone_hold=?, updated_at=?
	 WHERE drain_state<>? AND clone_hold=''`, SpoolCloneHoldReason, time.Now().Unix(), DrainDone)
	if err != nil {
		return 0, fmt.Errorf("spool clone hold: %w", err)
	}
	return res.RowsAffected()
}

// ListCloneHeld returns the rows held as copied from another Mac.
func (s *SpoolStore) ListCloneHeld() ([]*SpoolRow, error) {
	rows, err := s.db.Query(
		`SELECT id, nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, suspect_zero_tail, punched_end, created_at, updated_at, pause_since_ns, retry_after_ns, qualification_hold, retired_by
		 FROM spool_entries WHERE clone_hold<>'' ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("spool list clone-held: %w", err)
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

// CloneHold returns the row's clone-hold reason ("" = not held).
func (s *SpoolStore) CloneHold(id int64) (string, error) {
	var h string
	err := s.db.QueryRow(`SELECT clone_hold FROM spool_entries WHERE id=?`, id).Scan(&h)
	return h, err
}

// ReleaseCloneHeld is the user's "upload them" decision: every held row
// becomes dispatchable again under this Mac's identity.
func (s *SpoolStore) ReleaseCloneHeld() (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(`UPDATE spool_entries SET clone_hold='', updated_at=? WHERE clone_hold<>''`, time.Now().Unix())
	if err != nil {
		return 0, fmt.Errorf("spool release clone holds: %w", err)
	}
	return res.RowsAffected()
}

// DeleteCloneHeld is the user's "discard them" decision: it deletes every
// held row except those under a completion hold, and returns the deleted
// rows so the caller removes their spool files and releases capacity.
func (s *SpoolStore) DeleteCloneHeld() ([]*SpoolRow, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("spool discard clone-held begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.Query(
		`SELECT id, nfs_path, spool_file, size, sha256, drain_state, drain_attempts, last_error, suspect_zero_tail, punched_end, created_at, updated_at, pause_since_ns, retry_after_ns, qualification_hold, retired_by
		 FROM spool_entries WHERE clone_hold<>''
		 AND NOT EXISTS (SELECT 1 FROM spool_completion_holds h WHERE h.spool_id=spool_entries.id) ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("spool discard clone-held list: %w", err)
	}
	var out []*SpoolRow
	for rows.Next() {
		r, err := scanSpoolRow(rows.Scan)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for _, r := range out {
		if _, err := tx.Exec(`DELETE FROM spool_entries WHERE id=? AND clone_hold<>''`, r.ID); err != nil {
			return nil, fmt.Errorf("spool discard clone-held %d: %w", r.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("spool discard clone-held commit: %w", err)
	}
	return out, nil
}

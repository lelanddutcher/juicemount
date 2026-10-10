package metadata

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"
)

// InsertBoundSpool creates a fresh writer and its ownership record in one
// transaction. There is intentionally no API to attach a binding to an old
// spool row using the incarnation currently occupying its pathname.
func (s *Store) InsertBoundSpool(expected MirrorToken, spoolFile string) (int64, error) {
	if expected.Incarnation == "" || s.spoolStore == nil || spoolFile == "" {
		return 0, ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if err := validateExpected(tx, expected); err != nil {
		return 0, err
	}
	now := time.Now().Unix()
	res, err := tx.Exec(`INSERT INTO spool_entries(nfs_path,spool_file,size,drain_state,drain_attempts,created_at,updated_at)
 SELECT ?,?,0,'writing',0,?,? WHERE NOT EXISTS
 (SELECT 1 FROM spool_entries WHERE nfs_path=? AND drain_state IN ('writing','ready','draining'))
 AND NOT EXISTS (SELECT 1 FROM spool_bindings WHERE publish_path=? AND phase='publishing')`,
		expected.Path, spoolFile, now, now, expected.Path, expected.Path)
	if err != nil {
		return 0, err
	}
	if err := exactlyOneRow(res); err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	res, err = tx.Exec(`INSERT INTO spool_bindings(spool_id,incarnation) VALUES (?,?)`, id, expected.Incarnation)
	if err != nil {
		return 0, err
	}
	if err := exactlyOneRow(res); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

// CaptureSpool validates a persisted binding. Revisions may advance while the
// same writer changes metadata; publication uses the exact returned revision.
func (s *Store) CaptureSpool(row *SpoolRow) (MirrorToken, error) {
	if row == nil {
		return MirrorToken{}, ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.RLock()
	defer s.mu.RUnlock()
	var incarnation, phase, name, file, state string
	var size, punched int64
	err := s.db.QueryRow(`SELECT b.incarnation,b.phase,r.nfs_path,r.spool_file,r.drain_state,r.size,r.punched_end
 FROM spool_bindings b JOIN spool_entries r ON r.id=b.spool_id WHERE r.id=?`, row.ID).
		Scan(&incarnation, &phase, &name, &file, &state, &size, &punched)
	if err != nil {
		return MirrorToken{}, err
	}
	if phase != "bound" || name != row.NFSPath || file != row.SpoolFile || state != string(DrainDraining) || size != row.Size || punched != 0 {
		return MirrorToken{}, ErrMirrorConflict
	}
	v, err := captureToken(s.db, name)
	if err != nil {
		return MirrorToken{}, err
	}
	if v.Incarnation != incarnation {
		return MirrorToken{}, ErrMirrorConflict
	}
	if e := s.pathCache[name]; e != nil && e.Version != v {
		return MirrorToken{}, ErrMirrorConflict
	}
	return v, nil
}

// BeginSpoolPublish leaves a durable refusal marker before the final rename.
// FULL is connection-scoped: use the same dedicated connection for the intent
// transaction. Restart must reconcile a publishing row, never automatically
// re-copy it onto a possibly replaced destination.
func (s *Store) BeginSpoolPublish(row *SpoolRow, expected MirrorToken, stage string) error {
	if row == nil || s.spoolStore == nil || stage == "" || expected.Incarnation == "" || expected.Path != row.NFSPath {
		return ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	tx, closeConn, err := s.beginSpoolDurableTx()
	if err != nil {
		return err
	}
	defer closeConn()
	defer tx.Rollback()
	if err := rejectCompletionHold(tx, row.ID); err != nil {
		return err
	}
	if err := validateExpected(tx, expected); err != nil {
		return err
	}
	res, err := tx.Exec(`UPDATE spool_bindings SET phase='publishing',stage_path=?,publish_path=?
 WHERE spool_id=? AND incarnation=? AND phase='bound' AND EXISTS
 (SELECT 1 FROM spool_entries WHERE id=? AND nfs_path=? AND spool_file=? AND size=? AND punched_end=0 AND drain_state='draining' AND retired_by=0)`,
		stage, row.NFSPath, row.ID, expected.Incarnation, row.ID, row.NFSPath, row.SpoolFile, row.Size)
	if err != nil {
		return err
	}
	if err := exactlyOneRow(res); err != nil {
		return err
	}
	return tx.Commit()
}

// CompleteSpool atomically publishes the exact mirror size and retires the
// bound draining row. RAM and custody cleanup follow only a successful commit.
func (s *Store) CompleteSpool(row *SpoolRow, expected MirrorToken, mtime time.Time) error {
	if row == nil || s.spoolStore == nil || expected.Incarnation == "" || expected.Path != row.NFSPath || row.Size < 0 {
		return ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.pathCache[expected.Path]; e != nil && e.Version != expected {
		return ErrMirrorConflict
	}
	tx, closeConn, err := s.beginSpoolDurableTx()
	if err != nil {
		return err
	}
	defer closeConn()
	defer tx.Rollback()
	res, err := tx.Exec(`UPDATE spool_bindings SET phase='completed' WHERE spool_id=? AND incarnation=? AND phase='publishing' AND publish_path=?`, row.ID, expected.Incarnation, expected.Path)
	if err != nil {
		return err
	}
	if err := exactlyOneRow(res); err != nil {
		return err
	}
	res, err = tx.Exec(`UPDATE spool_entries SET drain_state='done',updated_at=? WHERE id=? AND nfs_path=? AND spool_file=? AND size=? AND punched_end=0 AND drain_state='draining' AND retired_by=0`, mtime.Unix(), row.ID, row.NFSPath, row.SpoolFile, row.Size)
	if err != nil {
		return err
	}
	if err := exactlyOneRow(res); err != nil {
		return err
	}
	res, err = tx.Exec(`UPDATE entries SET size=?,mtime=?,mtime_nsec=NULL,revision=revision+1 WHERE path=? AND incarnation=? AND revision=?`, row.Size, mtime.Unix(), expected.Path, expected.Incarnation, expected.Revision)
	if err != nil {
		return err
	}
	if err := exactlyOneRow(res); err != nil {
		return err
	}
	e, err := scanEntry(tx.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, expected.Path))
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.publishEntryLocked(e)
	return nil
}

func (s *Store) UnresolvedSpoolPublications() ([]string, error) {
	rows, err := s.db.Query(`SELECT publish_path FROM spool_bindings WHERE phase='publishing'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name == "" {
			return nil, fmt.Errorf("spool publication has no path")
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// SpoolBound distinguishes a persisted operation from a legacy row, including
// after cancellation has removed its spool_entries row.
func (s *SpoolStore) SpoolBound(id int64) (bool, error) {
	var n int
	err := s.db.QueryRow(`SELECT count(*) FROM spool_bindings WHERE spool_id=?`, id).Scan(&n)
	return n != 0, err
}

func (s *Store) beginSpoolDurableTx() (*sql.Tx, func(), error) {
	tx, release, err := s.beginSpoolDurableTxWithDiscard()
	if err != nil {
		return nil, nil, err
	}
	return tx, func() { release(false) }, nil
}

// The birth API uses the discard path only if an explicit pre-COMMIT rollback
// fails. database/sql calls the driver's Rollback and can otherwise return
// the same physical connection to its pool after a non-BadConn error.
func (s *Store) beginSpoolDurableTxWithDiscard() (*sql.Tx, func(bool), error) {
	conn, err := s.db.Conn(context.Background())
	if err != nil {
		return nil, nil, err
	}
	closeConn := func(discard bool) {
		if discard {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
	}
	if _, err := conn.ExecContext(context.Background(), `PRAGMA synchronous=FULL`); err != nil {
		closeConn(false)
		return nil, nil, err
	}
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		// A driver that began a transaction before reporting an error cannot
		// put that connection back into the pool as clean.
		closeConn(true)
		return nil, nil, err
	}
	return tx, closeConn, nil
}

// PendingSpoolPublication is a known ambiguous backing mutation. The caller
// must stop startup before recovery, drain/sweep workers or an NFS listener.
// Ordinary database errors are deliberately not converted to this type: a
// later startup may retry the query without installing a permanent fence.
type PendingSpoolPublication struct{ Paths []string }

func (e *PendingSpoolPublication) Error() string {
	return fmt.Sprintf("spool publication requires reconciliation for %d path(s)", len(e.Paths))
}

// PendingPunchedSpool identifies private bytes that a hole-punched source
// cannot reconstruct. Neither boot recovery nor path-derived stage cleanup may
// proceed until publication has been reconciled.
type PendingPunchedSpool struct{ Rows []PunchedSpoolRow }

// PendingSpoolBirthRecovery fences every unretired durable birth binding,
// including a missing spool row or a later phase. A boot scrub cannot infer
// that a child received the COMMIT result or that source cleanup is safe.
type PendingSpoolBirthRecovery struct{ Births []SpoolBirthRecoveryRow }

type SpoolBirthRecoveryRow struct {
	BirthID, Path, SpoolFile, FileIdentity, Phase string
	SpoolID                                       int64
	DrainState                                    sql.NullString
}

func (e *PendingSpoolBirthRecovery) Error() string {
	return fmt.Sprintf("spool birth requires reconciliation for %d binding(s)", len(e.Births))
}

// CheckSpoolBirthRecovery is read-only and also called by direct boot scrub,
// not just the bridge preflight. Refuse every nonempty birth marker until a
// future cohesive recovery flow has explicitly reconciled it. Legacy bindings
// have an empty birth_id and retain their previous recovery behavior.
func (s *SpoolStore) CheckSpoolBirthRecovery() error {
	rows, err := s.db.Query(`SELECT b.birth_id,b.birth_path,b.birth_file,b.birth_identity,b.phase,b.spool_id,r.drain_state
 FROM spool_bindings b LEFT JOIN spool_entries r ON r.id=b.spool_id WHERE b.birth_id<>'' ORDER BY b.spool_id`)
	if err != nil {
		return fmt.Errorf("inspect spool births: %w", err)
	}
	defer rows.Close()
	var births []SpoolBirthRecoveryRow
	for rows.Next() {
		var b SpoolBirthRecoveryRow
		if err := rows.Scan(&b.BirthID, &b.Path, &b.SpoolFile, &b.FileIdentity, &b.Phase, &b.SpoolID, &b.DrainState); err != nil {
			return fmt.Errorf("inspect spool births: %w", err)
		}
		births = append(births, b)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("inspect spool births: %w", err)
	}
	if len(births) != 0 {
		return &PendingSpoolBirthRecovery{Births: births}
	}
	return nil
}

type PunchedSpoolRow struct {
	ID         int64
	Path       string
	SpoolFile  string
	PunchedEnd int64
}

func (e *PendingPunchedSpool) Error() string {
	return fmt.Sprintf("punched spool requires reconciliation for %d row(s)", len(e.Rows))
}

// CheckPunchedSpoolRecovery includes legacy rows with no custody ledger. A
// published custody phase alone does not retire the row or prove cleanup, so
// every non-done punched source remains a boot refusal.
func (s *SpoolStore) CheckPunchedSpoolRecovery() error {
	rows, err := s.db.Query(`SELECT r.id,r.nfs_path,r.spool_file,r.punched_end
 FROM spool_entries r WHERE r.punched_end>0 AND r.drain_state<>'done'
 ORDER BY r.id`)
	if err != nil {
		return fmt.Errorf("inspect punched spool: %w", err)
	}
	defer rows.Close()
	var pending []PunchedSpoolRow
	for rows.Next() {
		var row PunchedSpoolRow
		if err := rows.Scan(&row.ID, &row.Path, &row.SpoolFile, &row.PunchedEnd); err != nil {
			return fmt.Errorf("inspect punched spool: %w", err)
		}
		pending = append(pending, row)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("inspect punched spool: %w", err)
	}
	if len(pending) > 0 {
		return &PendingPunchedSpool{Rows: pending}
	}
	return nil
}

func (s *Store) CheckSpoolRecovery() error {
	if err := NewSpoolStore(s.db).CheckBoundEffectsRecovery(); err != nil {
		return err
	}
	if err := NewSpoolStore(s.db).CheckSpoolBirthRecovery(); err != nil {
		return err
	}
	names, err := s.UnresolvedSpoolPublications()
	if err != nil {
		return fmt.Errorf("check spool recovery: %w", err)
	}
	if len(names) != 0 {
		return &PendingSpoolPublication{Paths: names}
	}
	if err := s.CheckSpoolCustodyRecovery(); err != nil {
		return err
	}
	if err := NewSpoolStore(s.db).CheckPunchedSpoolRecovery(); err != nil {
		return err
	}
	return NewSpoolStore(s.db).CheckDrainCompletionRecovery()
}

// No legacy completion interface may retire an operation whose filesystem
// publication is governed by this protocol.
func rejectBoundSpool(q versionQuerier, id int64) error {
	var bound int64
	err := q.QueryRow(`SELECT spool_id FROM spool_bindings WHERE spool_id=?`, id).Scan(&bound)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("bound spool %d requires CompleteSpool: %w", id, ErrMirrorConflict)
}

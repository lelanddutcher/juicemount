package metadata

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var ErrSpoolContinuationConflict = errors.New("spool continuation custody conflict")

// PendingSpoolContinuation means the READY transition may have committed.
// The caller must retain its private source and must not expose a writer.
type PendingSpoolContinuation struct{ Cause error }

func (e *PendingSpoolContinuation) Error() string {
	return fmt.Sprintf("spool continuation requires reconciliation: %v", e.Cause)
}
func (e *PendingSpoolContinuation) Unwrap() error { return e.Cause }

// SpoolContinuationSnapshot is a read-only observation. No pathname occupant
// can create or repair its persisted birth tuple.
type SpoolContinuationSnapshot struct {
	Legacy                                                 bool
	SpoolID                                                int64
	Path, File, State, BirthID, BirthIdentity, Incarnation string
	Size, PunchedEnd, Revision                             int64
}

func (s *SpoolStore) inspectContinuation(id int64, path, file string) (SpoolContinuationSnapshot, error) {
	var c SpoolContinuationSnapshot
	var rowPath, rowFile, state string
	var size, punched int64
	var incarnation, birthID, birthPath, birthFile, birthIdentity, phase sql.NullString
	err := s.db.QueryRow(`SELECT r.nfs_path,r.spool_file,r.drain_state,r.size,r.punched_end,
 b.incarnation,b.birth_id,b.birth_path,b.birth_file,b.birth_identity,b.phase
 FROM spool_entries r LEFT JOIN spool_bindings b ON b.spool_id=r.id WHERE r.id=?`, id).
		Scan(&rowPath, &rowFile, &state, &size, &punched, &incarnation, &birthID, &birthPath, &birthFile, &birthIdentity, &phase)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return c, ErrSpoolContinuationConflict
		}
		return c, err
	}
	if id <= 0 || rowPath != path || rowFile != file {
		return c, ErrSpoolContinuationConflict
	}
	c = SpoolContinuationSnapshot{SpoolID: id, Path: path, File: file, State: state, Size: size, PunchedEnd: punched}
	if !incarnation.Valid {
		var tombstones int
		if err := s.db.QueryRow(`SELECT count(*) FROM spool_bindings WHERE spool_id<>? AND birth_id<>'' AND (birth_path=? OR birth_file=?)`, id, path, file).Scan(&tombstones); err != nil {
			return c, err
		}
		if tombstones != 0 {
			return c, ErrSpoolContinuationConflict
		}
		c.Legacy = true
		return c, nil
	}
	if punched != 0 || (state != string(DrainWriting) && state != string(DrainReady)) {
		return c, ErrSpoolContinuationConflict
	}
	var mirrorInc sql.NullString
	var mirrorRev sql.NullInt64
	if err := s.db.QueryRow(`SELECT incarnation,revision FROM entries WHERE path=?`, path).Scan(&mirrorInc, &mirrorRev); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return c, ErrSpoolContinuationConflict
		}
		return c, err
	}
	if !birthID.Valid || !birthPath.Valid || !birthFile.Valid || !birthIdentity.Valid || !phase.Valid ||
		!validCustodyID(birthID.String) || !validCustodyID(incarnation.String) || !validIdentity(birthIdentity.String) ||
		birthPath.String != path || birthFile.String != file || phase.String != "bound" ||
		!mirrorInc.Valid || mirrorInc.String != incarnation.String || !mirrorRev.Valid || mirrorRev.Int64 <= 0 {
		return c, ErrSpoolContinuationConflict
	}
	var holds int
	if err := s.db.QueryRow(`SELECT count(*) FROM spool_completion_holds WHERE spool_id=?`, id).Scan(&holds); err != nil {
		return c, err
	}
	if holds != 0 {
		return c, ErrSpoolContinuationConflict
	}
	c.BirthID, c.BirthIdentity, c.Incarnation, c.Revision = birthID.String, birthIdentity.String, incarnation.String, mirrorRev.Int64
	return c, nil
}

func (s *SpoolStore) checkContinuationRAM(c SpoolContinuationSnapshot) error {
	owner := s.owner.Load()
	if owner == nil || owner.db != s.db || owner.spoolStore != s {
		return ErrSpoolContinuationConflict
	}
	owner.mu.RLock()
	conflict := owner.preparedCreateConflictLocked(c.Path)
	if e := owner.pathCache[c.Path]; e != nil && (e.Version.Incarnation != c.Incarnation || e.Version.Revision != c.Revision) {
		conflict = true
	}
	owner.mu.RUnlock()
	if conflict {
		return ErrSpoolContinuationConflict
	}
	return nil
}

// InspectSpoolContinuation distinguishes no binding (legacy) from every
// present binding, including incomplete historical tuples. Bound rows require
// the linked mirror Store so cache-only and prepared successors are visible.
func (s *SpoolStore) InspectSpoolContinuation(id int64, path, file string) (SpoolContinuationSnapshot, error) {
	owner := s.owner.Load()
	if owner != nil {
		owner.writeMu.Lock()
		defer owner.writeMu.Unlock()
		owner.cachePublicationMu.Lock()
		defer owner.cachePublicationMu.Unlock()
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	c, err := s.inspectContinuation(id, path, file)
	if err != nil || c.Legacy {
		return c, err
	}
	return c, s.checkContinuationRAM(c)
}

func sameContinuation(a, b SpoolContinuationSnapshot) bool { return a == b }

// AdmitLegacyActive protects the no-binding branch against a binding inserted
// after initial classification. admit only changes process-local entry state.
func (s *SpoolStore) AdmitLegacyActive(c SpoolContinuationSnapshot, admit func()) error {
	if !c.Legacy || admit == nil {
		return ErrSpoolContinuationConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now, err := s.inspectContinuation(c.SpoolID, c.Path, c.File)
	if err != nil {
		return err
	}
	if !now.Legacy {
		return ErrSpoolContinuationConflict
	}
	admit()
	return nil
}

// AdmitBoundActive rechecks SQL and RAM while holding the Store publication
// locks through the no-I/O writer admission callback.
func (s *SpoolStore) AdmitBoundActive(c SpoolContinuationSnapshot, admit func()) error {
	if c.Legacy || admit == nil {
		return ErrSpoolContinuationConflict
	}
	owner := s.owner.Load()
	if owner == nil {
		return ErrSpoolContinuationConflict
	}
	owner.writeMu.Lock()
	defer owner.writeMu.Unlock()
	owner.cachePublicationMu.Lock()
	defer owner.cachePublicationMu.Unlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now, err := s.inspectContinuation(c.SpoolID, c.Path, c.File)
	if err != nil {
		return err
	}
	if !sameContinuation(now, c) || now.State != string(DrainWriting) {
		return ErrSpoolContinuationConflict
	}
	if err := s.checkContinuationRAM(c); err != nil {
		return err
	}
	admit()
	return nil
}

// ReopenBoundReady performs the exact READY→writing transition and admits an
// already pinned FD under the same Store publication locks. false means only
// that a drainer already won READY→draining. No filesystem I/O occurs here.
func (s *SpoolStore) ReopenBoundReady(c SpoolContinuationSnapshot, admit func()) (reopened bool, retErr error) {
	if c.Legacy || c.State != string(DrainReady) || admit == nil {
		return false, ErrSpoolContinuationConflict
	}
	owner := s.owner.Load()
	if owner == nil {
		return false, ErrSpoolContinuationConflict
	}
	owner.writeMu.Lock()
	defer owner.writeMu.Unlock()
	owner.cachePublicationMu.Lock()
	defer owner.cachePublicationMu.Unlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now, err := s.inspectContinuation(c.SpoolID, c.Path, c.File)
	if err != nil {
		// A drainer can win between the read-only capture and this admission.
		var state string
		if qerr := s.db.QueryRow(`SELECT drain_state FROM spool_entries WHERE id=?`, c.SpoolID).Scan(&state); qerr == nil && state == string(DrainDraining) {
			return false, nil
		}
		return false, err
	}
	if !sameContinuation(now, c) {
		return false, ErrSpoolContinuationConflict
	}
	if err := s.checkContinuationRAM(c); err != nil {
		return false, err
	}
	tx, release, err := owner.beginSpoolDurableTxWithDiscard()
	if err != nil {
		return false, err
	}
	discard := false
	defer func() { release(discard) }()
	commitStarted := false
	defer func() {
		if !commitStarted {
			if rb := tx.Rollback(); rb != nil {
				discard = true
				retErr = &PendingSpoolContinuation{Cause: errors.Join(retErr, rb)}
			}
		}
	}()
	res, err := tx.Exec(`UPDATE spool_entries SET drain_state='writing',updated_at=? WHERE id=? AND nfs_path=? AND spool_file=? AND size=?
 AND drain_state='ready' AND punched_end=0 AND EXISTS
 (SELECT 1 FROM spool_bindings b WHERE b.spool_id=spool_entries.id AND b.incarnation=? AND b.birth_id=? AND b.birth_path=?
 AND b.birth_file=? AND b.birth_identity=? AND b.phase='bound') AND EXISTS
 (SELECT 1 FROM entries e WHERE e.path=spool_entries.nfs_path AND e.incarnation=? AND e.revision=?)
 AND NOT EXISTS (SELECT 1 FROM spool_completion_holds h WHERE h.spool_id=spool_entries.id)`,
		time.Now().Unix(), c.SpoolID, c.Path, c.File, c.Size, c.Incarnation, c.BirthID, c.Path, c.File, c.BirthIdentity, c.Incarnation, c.Revision)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n != 1 {
		return false, ErrSpoolContinuationConflict
	}
	commitStarted = true
	if err := tx.Commit(); err != nil {
		discard = true
		return false, &PendingSpoolContinuation{Cause: err}
	}
	if s.continuationPostCommit != nil {
		if err := s.continuationPostCommit(); err != nil {
			return false, &PendingSpoolContinuation{Cause: err}
		}
	}
	admit()
	return true, nil
}

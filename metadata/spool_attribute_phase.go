package metadata

import (
	"errors"
	"fmt"
	"io/fs"
)

// BoundModeObservation is a durable, exact bound-row and mirror observation.
// It is not authority to mutate a backing pathname.
type BoundModeObservation struct {
	SpoolID                                int64
	BirthID, Path, SpoolFile, FileIdentity string
	Token                                  MirrorToken
	Inode                                  uint64
	Mode                                   fs.FileMode
	FullSize                               int64
}

type PendingBoundMode struct {
	Observation BoundModeObservation
	Cause       error
}

func (e *PendingBoundMode) Error() string {
	return fmt.Sprintf("bound mode admission requires reconciliation: %v", e.Cause)
}
func (e *PendingBoundMode) Unwrap() error { return e.Cause }

func (s *Store) boundModeLocked(id int64, birthID, path, file, identity string) (BoundModeObservation, error) {
	var out BoundModeObservation
	if s.spoolStore == nil || s.spoolStore.db != s.db || id <= 0 || !validCustodyID(birthID) || !validCustodyPath(path) || !validPrivatePath(file) || !validIdentity(identity) {
		return out, ErrMirrorConflict
	}
	if err := rejectCompletionHold(s.db, id); err != nil {
		return out, err
	}
	var count int
	err := s.db.QueryRow(`SELECT count(*) FROM spool_entries r JOIN spool_bindings b ON b.spool_id=r.id
	 WHERE r.id=? AND r.nfs_path=? AND r.spool_file=? AND r.drain_state='draining' AND r.punched_end=0
	 AND b.birth_id=? AND b.birth_path=? AND b.birth_file=? AND b.birth_identity=? AND b.phase='bound'`,
		id, path, file, birthID, path, file, identity).Scan(&count)
	if err != nil {
		return out, err
	}
	if count != 1 {
		return out, ErrMirrorConflict
	}
	var fullSize int64
	if err := s.db.QueryRow(`SELECT size FROM spool_entries WHERE id=?`, id).Scan(&fullSize); err != nil {
		return out, err
	}
	e, err := scanEntry(s.db.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, path))
	if err != nil {
		return out, err
	}
	if e.IsDir || e.Version.Incarnation == "" {
		return out, ErrMirrorConflict
	}
	var incarnation string
	if err := s.db.QueryRow(`SELECT incarnation FROM spool_bindings WHERE spool_id=?`, id).Scan(&incarnation); err != nil {
		return out, err
	}
	if e.Version.Incarnation != incarnation {
		return out, ErrMirrorConflict
	}
	s.mu.RLock()
	conflict := s.preparedCreateConflictLocked(path)
	if cached := s.pathCache[path]; cached != nil && (cached.Version != e.Version || cached.Mode != e.Mode || cached.Inode != e.Inode) {
		conflict = true
	}
	s.mu.RUnlock()
	if conflict {
		return out, ErrMirrorConflict
	}
	return BoundModeObservation{SpoolID: id, BirthID: birthID, Path: path, SpoolFile: file, FileIdentity: identity,
		Token: e.Version, Inode: e.Inode, Mode: e.Mode, FullSize: fullSize}, nil
}

// CaptureBoundMode atomically observes the exact born draining row and current
// durable/RAM mirror mode under Store→publication→spool lock order.
func (s *Store) CaptureBoundMode(id int64, birthID, path, file, identity string) (BoundModeObservation, error) {
	if s.spoolStore == nil {
		return BoundModeObservation{}, ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	return s.boundModeLocked(id, birthID, path, file, identity)
}

// ApplyBoundMode admits an acknowledged SETATTR only for the exact current
// observation. The caller's path lease must remain held until its predecessor
// FD has the mode; a successful metadata CAS alone is not SETATTR success.
func (s *Store) ApplyBoundMode(expected BoundModeObservation, mode fs.FileMode) (BoundModeObservation, error) {
	return s.applyBoundMode(expected, mode, nil)
}

// postCommit is a test-only reply-loss seam after actual FULL COMMIT.
func (s *Store) applyBoundMode(expected BoundModeObservation, mode fs.FileMode, postCommit func() error) (out BoundModeObservation, retErr error) {
	if s.spoolStore == nil || expected.Token.Path != expected.Path || expected.Token.Incarnation == "" || expected.Token.Revision <= 0 {
		return out, ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	current, err := s.boundModeLocked(expected.SpoolID, expected.BirthID, expected.Path, expected.SpoolFile, expected.FileIdentity)
	if err != nil {
		return out, err
	}
	if current != expected {
		return out, ErrMirrorConflict
	}
	tx, release, err := s.beginSpoolDurableTxWithDiscard()
	if err != nil {
		return out, err
	}
	discard, started := false, false
	defer func() {
		if !started {
			if rb := tx.Rollback(); rb != nil {
				discard = true
				retErr = &PendingBoundMode{Observation: expected, Cause: errors.Join(retErr, rb)}
			}
		}
		release(discard)
	}()
	perm := uint32(mode & fs.ModePerm)
	res, err := tx.Exec(`UPDATE entries SET mode=(mode & ~511)|?,revision=revision+1 WHERE path=? AND incarnation=? AND revision=?
	 AND EXISTS (SELECT 1 FROM spool_entries r JOIN spool_bindings b ON b.spool_id=r.id
	 WHERE r.id=? AND r.nfs_path=entries.path AND r.spool_file=? AND r.drain_state='draining' AND r.punched_end=0
	 AND b.birth_id=? AND b.birth_path=? AND b.birth_file=? AND b.birth_identity=? AND b.phase='bound' AND b.incarnation=entries.incarnation)`,
		perm, expected.Path, expected.Token.Incarnation, expected.Token.Revision, expected.SpoolID, expected.SpoolFile,
		expected.BirthID, expected.Path, expected.SpoolFile, expected.FileIdentity)
	if err != nil {
		return out, err
	}
	if err := exactlyOneRow(res); err != nil {
		return out, err
	}
	e, err := scanEntry(tx.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, expected.Path))
	if err != nil {
		return out, err
	}
	if e.Version.Revision != expected.Token.Revision+1 || e.Version.Incarnation != expected.Token.Incarnation || e.Inode != expected.Inode {
		return out, ErrMirrorConflict
	}
	started = true
	if err := tx.Commit(); err != nil {
		discard = true
		return out, &PendingBoundMode{Observation: expected, Cause: err}
	}
	s.mu.Lock()
	s.publishEntryLocked(e)
	s.mu.Unlock()
	out = BoundModeObservation{SpoolID: expected.SpoolID, BirthID: expected.BirthID, Path: expected.Path, SpoolFile: expected.SpoolFile,
		FileIdentity: expected.FileIdentity, Token: e.Version, Inode: e.Inode, Mode: e.Mode, FullSize: expected.FullSize}
	if postCommit != nil {
		if err := postCommit(); err != nil {
			return BoundModeObservation{}, &PendingBoundMode{Observation: out, Cause: err}
		}
	}
	return out, nil
}

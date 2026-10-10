package metadata

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"time"
)

// PendingSpoolFinalization is a lost durable reply. The caller must preserve
// the image and stop admitting writes; boot reconciles the actual row state.
type PendingSpoolFinalization struct{ Cause error }

func (e *PendingSpoolFinalization) Error() string {
	return fmt.Sprintf("spool finalization requires reconciliation: %v", e.Cause)
}
func (e *PendingSpoolFinalization) Unwrap() error { return e.Cause }

// QuiescentSpoolSnapshot binds a live legacy row to the current durable mirror.
// It never installs a birth binding or authorizes recovery of a writing row.
type QuiescentSpoolSnapshot struct {
	SpoolContinuationSnapshot
	Inode uint64
}

func (s *SpoolStore) inspectQuiescent(id int64, path, file string, inode uint64) (QuiescentSpoolSnapshot, error) {
	var q QuiescentSpoolSnapshot
	c, err := s.inspectContinuation(id, path, file)
	if err != nil {
		return q, err
	}
	if !c.Legacy || c.State != string(DrainWriting) || c.PunchedEnd != 0 || inode == 0 {
		return q, ErrSpoolContinuationConflict
	}
	var actual int64
	var isDir bool
	if err := s.db.QueryRow(`SELECT inode,is_dir,incarnation,revision FROM entries WHERE path=?`, path).Scan(&actual, &isDir, &c.Incarnation, &c.Revision); err != nil {
		return q, err
	}
	if isDir || uint64(actual) != inode || c.Incarnation == "" || c.Revision <= 0 {
		return q, ErrSpoolContinuationConflict
	}
	if err := s.checkContinuationRAM(c); err != nil {
		return q, err
	}
	owner := s.owner.Load()
	owner.mu.RLock()
	cached := owner.pathCache[path]
	conflict := cached != nil && (cached.IsDir || cached.Inode != inode)
	owner.mu.RUnlock()
	if conflict {
		return q, ErrSpoolContinuationConflict
	}
	return QuiescentSpoolSnapshot{c, inode}, nil
}

func (s *SpoolStore) InspectQuiescentSpool(id int64, path, file string, inode uint64) (QuiescentSpoolSnapshot, error) {
	owner := s.owner.Load()
	if owner == nil {
		return QuiescentSpoolSnapshot{}, ErrSpoolContinuationConflict
	}
	owner.writeMu.Lock()
	defer owner.writeMu.Unlock()
	owner.cachePublicationMu.Lock()
	defer owner.cachePublicationMu.Unlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.inspectQuiescent(id, path, file, inode)
}

// MarkQuiescentReady records a complete currently received image, not client
// EOF. The caller owns the sealed, fsynced original descriptor and proves its
// contiguous coverage/hash. Bound, historical or remotely staged rows refuse.
func (s *SpoolStore) MarkQuiescentReady(c QuiescentSpoolSnapshot, size int64, digest []byte) (retErr error) {
	if !c.Legacy || c.State != string(DrainWriting) || size < 0 || len(digest) != sha256.Size {
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
	now, err := s.inspectQuiescent(c.SpoolID, c.Path, c.File, c.Inode)
	if err != nil {
		return err
	}
	if now != c {
		return ErrSpoolContinuationConflict
	}
	tx, release, err := owner.beginSpoolDurableTxWithDiscard()
	if err != nil {
		return err
	}
	discard, committing := false, false
	defer func() { release(discard) }()
	defer func() {
		if !committing {
			if err := tx.Rollback(); err != nil {
				discard = true
				retErr = &PendingSpoolFinalization{Cause: errors.Join(retErr, err)}
			}
		}
	}()
	res, err := tx.Exec(`UPDATE spool_entries SET drain_state='ready',size=?,sha256=?,updated_at=?
 WHERE id=? AND nfs_path=? AND spool_file=? AND size=? AND drain_state='writing'
 AND punched_end=0 AND retired_by=0 AND clone_hold='' AND qualification_hold=''
 AND replacement_witness IS NULL AND replacement_publication_required=0
 AND NOT EXISTS (SELECT 1 FROM spool_entries r WHERE r.nfs_path=spool_entries.nfs_path AND r.id>spool_entries.id)
 AND NOT EXISTS (SELECT 1 FROM spool_completion_holds h WHERE h.spool_id=spool_entries.id)
 AND NOT EXISTS (SELECT 1 FROM spool_custody k WHERE k.spool_id=spool_entries.id)
 AND NOT EXISTS (SELECT 1 FROM spool_replacement_publications p WHERE p.spool_id=spool_entries.id)
 AND NOT EXISTS (SELECT 1 FROM spool_bindings b WHERE b.spool_id=spool_entries.id OR
 (b.birth_id<>'' AND (b.birth_path=spool_entries.nfs_path OR b.birth_file=spool_entries.spool_file)))
 AND EXISTS (SELECT 1 FROM entries e WHERE e.path=spool_entries.nfs_path AND e.is_dir=0 AND e.inode=? AND e.incarnation=? AND e.revision=?)`,
		size, digest, time.Now().Unix(), c.SpoolID, c.Path, c.File, c.Size, int64(c.Inode), c.Incarnation, c.Revision)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrSpoolContinuationConflict
	}
	committing = true
	if err := tx.Commit(); err != nil {
		discard = true
		return &PendingSpoolFinalization{Cause: err}
	}
	if s.shutdownReadyPostCommit != nil {
		if err := s.shutdownReadyPostCommit(); err != nil {
			return &PendingSpoolFinalization{Cause: err}
		}
	}
	return nil
}

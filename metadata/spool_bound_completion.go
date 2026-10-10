package metadata

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"syscall"
	"time"
)

// BoundDrainCommitItem names a previously born operation and a caller-retained
// private FD. BackingIdentity is an observation for a later FD-bound effects
// phase, not authority to mutate a pathname or clean up a source.
type BoundDrainCommitItem struct {
	SpoolID                                                 int64
	BirthID, Path, SpoolFile, FileIdentity, BackingIdentity string
	Source                                                  *os.File
	FullSize                                                int64
	Expected                                                MirrorToken
	Mtime                                                   time.Time
}

// BoundDrainCommitResult is only a committed mirror/row receipt. Its mutable
// fields alone never authorize cleanup or a backend effect.
type BoundDrainCommitResult struct {
	CompletionID                                            string
	SpoolID                                                 int64
	BirthID, Path, SpoolFile, FileIdentity, BackingIdentity string
	FullSize                                                int64
	Published                                               MirrorToken
	OriginalInode                                           uint64
	OriginalMode                                            fs.FileMode
}

// PendingBoundDrainCompletion means the FULL hold transaction or later exact
// completion may have committed. The caller keeps its pinned FD and source.
type PendingBoundDrainCompletion struct {
	SpoolIDs []int64
	Cause    error
}

func (e *PendingBoundDrainCompletion) Error() string {
	return fmt.Sprintf("bound drain completion requires reconciliation for %d source(s): %v", len(e.SpoolIDs), e.Cause)
}
func (e *PendingBoundDrainCompletion) Unwrap() error { return e.Cause }

type boundImage struct {
	item      BoundDrainCommitItem
	sha       [32]byte
	id        string
	original  *Entry
	published *Entry
}

func preflightBoundImage(it BoundDrainCommitItem) (boundImage, error) {
	var x boundImage
	if it.SpoolID <= 0 || !validCustodyID(it.BirthID) || !validCustodyPath(it.Path) ||
		!validPrivatePath(it.SpoolFile) || !validIdentity(it.FileIdentity) || !validIdentity(it.BackingIdentity) ||
		it.FullSize < 0 || it.Source == nil || it.Expected.Path != it.Path || it.Expected.Incarnation == "" || it.Expected.Revision <= 0 {
		return x, ErrMirrorConflict
	}
	fi, err := it.Source.Stat()
	if err != nil || !fi.Mode().IsRegular() || fi.Size() != it.FullSize {
		return x, ErrMirrorConflict
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || fmt.Sprintf("%d:%d", uint64(st.Dev), st.Ino) != it.FileIdentity {
		return x, ErrMirrorConflict
	}
	pathID, pathSize, err := privateBirthFileState(it.SpoolFile)
	if err != nil || pathID != it.FileIdentity || pathSize != it.FullSize {
		return x, ErrMirrorConflict
	}
	h := sha256.New()
	if n, err := io.Copy(h, io.NewSectionReader(it.Source, 0, it.FullSize)); err != nil || n != it.FullSize {
		return x, ErrMirrorConflict
	}
	copy(x.sha[:], h.Sum(nil))
	x.item = it
	return x, nil
}

// CompleteBoundSpool is the one-item form of the exact all-or-none batch.
func (s *Store) CompleteBoundSpool(item BoundDrainCommitItem) (BoundDrainCommitResult, error) {
	r, err := s.BatchCompleteBoundSpools([]BoundDrainCommitItem{item})
	if err != nil {
		return BoundDrainCommitResult{}, err
	}
	return r[0], nil
}

// BatchCompleteBoundSpools rejects the entire cohort if any member is stale.
// The FULL hold commit precedes a separate exact size+Done commit. No caller
// may dispose a source or publish effects until later reconciliation/cleanup.
func (s *Store) BatchCompleteBoundSpools(items []BoundDrainCommitItem) ([]BoundDrainCommitResult, error) {
	return s.completeBoundSpools(items, nil, nil)
}

// postHold/postDone are test-only lost-return seams after actual commits.
func (s *Store) completeBoundSpools(items []BoundDrainCommitItem, postHold, postDone func() error) ([]BoundDrainCommitResult, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if s.spoolStore == nil || s.spoolStore.db != s.db {
		return nil, ErrMirrorConflict
	}
	xs := make([]boundImage, len(items))
	ids := make([]int64, len(items))
	seenIDs := make(map[int64]bool, len(items))
	seenPaths := make(map[string]bool, len(items))
	seenFiles := make(map[string]bool, len(items))
	for i, it := range items {
		if seenIDs[it.SpoolID] || seenPaths[it.Path] || seenFiles[it.SpoolFile] {
			return nil, ErrMirrorConflict
		}
		seenIDs[it.SpoolID], seenPaths[it.Path], seenFiles[it.SpoolFile] = true, true, true
		var err error
		if xs[i], err = preflightBoundImage(it); err != nil {
			return nil, err
		}
		ids[i] = it.SpoolID
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	s.mu.RLock()
	for _, x := range xs {
		if s.preparedCreateConflictLocked(x.item.Path) || (s.pathCache[x.item.Path] != nil && s.pathCache[x.item.Path].Version != x.item.Expected) {
			s.mu.RUnlock()
			return nil, ErrMirrorConflict
		}
	}
	s.mu.RUnlock()
	if err := s.prepareBoundHolds(xs, ids); err != nil {
		return nil, err
	}
	if postHold != nil {
		if err := postHold(); err != nil {
			return nil, &PendingBoundDrainCompletion{SpoolIDs: ids, Cause: err}
		}
	}
	if err := s.commitBoundDone(xs, ids); err != nil {
		return nil, &PendingBoundDrainCompletion{SpoolIDs: ids, Cause: err}
	}
	s.mu.Lock()
	for i := range xs {
		s.publishEntryLocked(xs[i].published)
	}
	s.mu.Unlock()
	if postDone != nil {
		if err := postDone(); err != nil {
			return nil, &PendingBoundDrainCompletion{SpoolIDs: ids, Cause: err}
		}
	}
	results := make([]BoundDrainCommitResult, len(xs))
	for i, x := range xs {
		results[i] = BoundDrainCommitResult{CompletionID: x.id, SpoolID: x.item.SpoolID, BirthID: x.item.BirthID,
			Path: x.item.Path, SpoolFile: x.item.SpoolFile, FileIdentity: x.item.FileIdentity,
			BackingIdentity: x.item.BackingIdentity, FullSize: x.item.FullSize,
			Published: x.published.Version, OriginalInode: x.original.Inode, OriginalMode: x.original.Mode}
	}
	return results, nil
}

func (s *Store) prepareBoundHolds(xs []boundImage, ids []int64) (retErr error) {
	tx, release, err := s.beginSpoolDurableTxWithDiscard()
	if err != nil {
		return err
	}
	discard, committed := false, false
	defer func() {
		if !committed {
			if rb := tx.Rollback(); rb != nil {
				discard = true
				retErr = &PendingBoundDrainCompletion{SpoolIDs: ids, Cause: errors.Join(retErr, rb)}
			}
		}
		release(discard)
	}()
	for i := range xs {
		x := &xs[i]
		it := x.item
		if err := validateExpected(tx, it.Expected); err != nil {
			return err
		}
		if err := rejectCompletionHold(tx, it.SpoolID); err != nil {
			return err
		}
		var count int
		err := tx.QueryRow(`SELECT count(*) FROM spool_entries r JOIN spool_bindings b ON b.spool_id=r.id
		 JOIN entries e ON e.path=r.nfs_path WHERE r.id=? AND r.nfs_path=? AND r.spool_file=? AND r.size=?
		 AND r.drain_state='draining' AND r.punched_end=0 AND r.sha256=?
		 AND b.phase='bound' AND b.birth_id=? AND b.birth_path=? AND b.birth_file=? AND b.birth_identity=?
		 AND b.incarnation=? AND e.incarnation=? AND e.revision=?`,
			it.SpoolID, it.Path, it.SpoolFile, it.FullSize, x.sha[:], it.BirthID, it.Path, it.SpoolFile, it.FileIdentity,
			it.Expected.Incarnation, it.Expected.Incarnation, it.Expected.Revision).Scan(&count)
		if err != nil {
			return err
		}
		if count != 1 {
			return ErrMirrorConflict
		}
		x.original, err = scanEntry(tx.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, it.Path))
		if err != nil {
			return err
		}
		if x.original.Version != it.Expected {
			return ErrMirrorConflict
		}
		x.id, err = NewSpoolCustodyID()
		if err != nil {
			return err
		}
		res, err := tx.Exec(`INSERT INTO spool_completion_holds
		 (completion_id,spool_id,nfs_path,spool_file,size,kind,birth_id,birth_identity,incarnation,expected_revision,
		 backing_identity,source_sha256,original_inode,original_mode)
		 VALUES (?,?,?,?,?,'bound',?,?,?,?,?,?,?,?)`,
			x.id, it.SpoolID, it.Path, it.SpoolFile, it.FullSize, it.BirthID, it.FileIdentity, it.Expected.Incarnation,
			it.Expected.Revision, it.BackingIdentity, x.sha[:], int64(x.original.Inode), uint32(x.original.Mode))
		if err != nil {
			return err
		}
		if err := exactlyOneRow(res); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		discard = true
		committed = true
		return &PendingBoundDrainCompletion{SpoolIDs: ids, Cause: err}
	}
	committed = true
	return nil
}

func (s *Store) commitBoundDone(xs []boundImage, ids []int64) (retErr error) {
	tx, release, err := s.beginSpoolDurableTxWithDiscard()
	if err != nil {
		return err
	}
	discard, committed := false, false
	defer func() {
		if !committed {
			if rb := tx.Rollback(); rb != nil {
				discard = true
				retErr = errors.Join(retErr, rb)
			}
		}
		release(discard)
	}()
	for i := range xs {
		x := &xs[i]
		it := x.item
		res, err := tx.Exec(`UPDATE spool_entries SET drain_state='done',updated_at=? WHERE id=? AND nfs_path=? AND spool_file=?
		 AND size=? AND sha256=? AND drain_state='draining' AND punched_end=0 AND EXISTS
		 (SELECT 1 FROM spool_bindings b WHERE b.spool_id=spool_entries.id AND b.phase='bound' AND b.birth_id=?
		 AND b.birth_path=? AND b.birth_file=? AND b.birth_identity=? AND b.incarnation=?) AND EXISTS
		 (SELECT 1 FROM spool_completion_holds h WHERE h.spool_id=spool_entries.id AND h.completion_id=? AND h.kind='bound'
		 AND h.birth_id=? AND h.birth_identity=? AND h.incarnation=? AND h.expected_revision=? AND h.source_sha256=?)`,
			it.Mtime.Unix(), it.SpoolID, it.Path, it.SpoolFile, it.FullSize, x.sha[:], it.BirthID, it.Path, it.SpoolFile,
			it.FileIdentity, it.Expected.Incarnation, x.id, it.BirthID, it.FileIdentity, it.Expected.Incarnation, it.Expected.Revision, x.sha[:])
		if err != nil {
			return err
		}
		if err := exactlyOneRow(res); err != nil {
			return err
		}
		res, err = tx.Exec(`UPDATE entries SET size=?,mtime=?,mtime_nsec=NULL,revision=revision+1
		 WHERE path=? AND incarnation=? AND revision=?`, it.FullSize, it.Mtime.Unix(), it.Path, it.Expected.Incarnation, it.Expected.Revision)
		if err != nil {
			return err
		}
		if err := exactlyOneRow(res); err != nil {
			return err
		}
		x.published, err = scanEntry(tx.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, it.Path))
		if err != nil {
			return err
		}
		if x.published.Version.Incarnation != it.Expected.Incarnation || x.published.Version.Revision != it.Expected.Revision+1 || x.published.Size != it.FullSize {
			return ErrMirrorConflict
		}
		res, err = tx.Exec(`UPDATE spool_completion_holds SET published_revision=? WHERE completion_id=? AND spool_id=?
		 AND kind='bound' AND birth_id=? AND birth_identity=? AND expected_revision=? AND published_revision=0`,
			x.published.Version.Revision, x.id, it.SpoolID, it.BirthID, it.FileIdentity, it.Expected.Revision)
		if err != nil {
			return err
		}
		if err := exactlyOneRow(res); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		discard = true
		committed = true
		return err
	}
	committed = true
	return nil
}

// ValidateBoundDrainCompletion proves an exact acknowledged result still
// matches the durable bound hold and committed row. It grants no effects or
// cleanup authority; the later cohort must settle those separately.
func (s *Store) ValidateBoundDrainCompletion(r BoundDrainCommitResult) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	if s.spoolStore == nil {
		return ErrMirrorConflict
	}
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	return s.validateBoundDrainCompletionLocked(r)
}

// Caller holds Store→cache-publication→spool locks.
func (s *Store) validateBoundDrainCompletionLocked(r BoundDrainCommitResult) error {
	if r.CompletionID == "" || r.SpoolID <= 0 || r.Published.Path != r.Path || r.Published.Revision <= 0 {
		return ErrMirrorConflict
	}
	s.mu.RLock()
	conflict := s.preparedCreateConflictLocked(r.Path)
	if cached := s.pathCache[r.Path]; cached != nil && cached.Version != r.Published {
		conflict = true
	}
	s.mu.RUnlock()
	if conflict {
		return ErrMirrorConflict
	}
	current, err := captureToken(s.db, r.Path)
	if err != nil {
		return err
	}
	if current != r.Published {
		return ErrMirrorConflict
	}
	var n int
	err = s.db.QueryRow(`SELECT count(*) FROM spool_completion_holds h JOIN spool_entries e ON e.id=h.spool_id
	 JOIN entries m ON m.path=h.nfs_path JOIN spool_bindings b ON b.spool_id=h.spool_id
	 WHERE h.completion_id=? AND h.spool_id=? AND h.kind='bound' AND h.birth_id=? AND h.nfs_path=?
	 AND h.spool_file=? AND h.birth_identity=? AND h.backing_identity=? AND h.size=?
	 AND h.incarnation=? AND h.published_revision=? AND h.original_inode=? AND h.original_mode=?
	 AND e.id=h.spool_id AND e.drain_state='done' AND e.nfs_path=h.nfs_path AND e.spool_file=h.spool_file
	 AND e.size=h.size AND e.punched_end=0 AND e.sha256=h.source_sha256
	 AND b.birth_id=h.birth_id AND b.birth_identity=h.birth_identity AND b.birth_path=h.nfs_path
	 AND b.birth_file=h.spool_file AND b.incarnation=h.incarnation AND b.phase='bound'
	 AND h.published_revision=h.expected_revision+1 AND m.incarnation=h.incarnation
	 AND m.revision=h.published_revision AND m.size=h.size`,
		r.CompletionID, r.SpoolID, r.BirthID, r.Path, r.SpoolFile, r.FileIdentity, r.BackingIdentity, r.FullSize,
		r.Published.Incarnation, r.Published.Revision, int64(r.OriginalInode), uint32(r.OriginalMode)).Scan(&n)
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrMirrorConflict
	}
	return nil
}

package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"syscall"
	"time"
)

// SpoolBirthOutcome distinguishes a definite pre-commit rollback from a
// result lost at or after COMMIT. Neither state adopts a legacy row.
type SpoolBirthOutcome string

const (
	SpoolBirthNotCommitted SpoolBirthOutcome = "not_committed"
	SpoolBirthUncertain    SpoolBirthOutcome = "uncertain"
	SpoolBirthCommitted    SpoolBirthOutcome = "committed"
)

// SpoolBirthReceipt binds a caller-chosen operation nonce to the private file
// and fresh mirror incarnation. The caller durably creates that file before
// birth and owns namespace admission until this result is settled.
type SpoolBirthReceipt struct {
	BirthID, Path, SpoolFile, FileIdentity, Incarnation string
	SpoolID, Revision                                   int64
	Outcome                                             SpoolBirthOutcome
}

// SpoolBirthIntent contains only values the caller can retain before calling
// InsertBoundSpoolBirth. A lost return (including process death after COMMIT)
// can be investigated using this intent. The caller must durably retain these
// values before birth; an absent binding is ambiguous, not permission to
// dispose of the caller's private file.
type SpoolBirthIntent struct {
	BirthID, Path, SpoolFile, FileIdentity string
}

type PendingSpoolBirth struct {
	Receipt SpoolBirthReceipt
	Cause   error
}

func (e *PendingSpoolBirth) Error() string {
	return fmt.Sprintf("spool birth requires reconciliation: %v", e.Cause)
}
func (e *PendingSpoolBirth) Unwrap() error { return e.Cause }

// InsertBoundSpoolBirth creates a fresh mirror incarnation, writing spool row,
// and immutable birth binding in one FULL transaction. expected is a durable
// leaf observation captured before producer mutation; it does not qualify
// parent ancestry or an unrelated prune. No existing row can be adopted.
func (s *Store) InsertBoundSpoolBirth(expected MirrorToken, entry *Entry, spoolFile, birthID string) (SpoolBirthReceipt, *Entry, error) {
	return s.insertBoundSpoolBirth(expected, entry, spoolFile, birthID, nil)
}

// postCommit is a test-only reply-loss seam, after actual COMMIT and RAM
// publication while the birth locks are still owned.
func (s *Store) insertBoundSpoolBirth(expected MirrorToken, entry *Entry, spoolFile, birthID string, postCommit func() error) (r SpoolBirthReceipt, born *Entry, retErr error) {
	r = SpoolBirthReceipt{BirthID: birthID, SpoolFile: spoolFile, Outcome: SpoolBirthNotCommitted}
	if entry != nil {
		r.Path = entry.Path
	}
	if entry == nil || !validCustodyID(birthID) || !validPrivatePath(spoolFile) || !validCustodyPath(entry.Path) ||
		entry.Name != path.Base(entry.Path) || entry.ParentPath != path.Dir(entry.Path) ||
		entry.IsDir || entry.Size < 0 || s.spoolStore == nil || expected.Path != entry.Path || expected.IsZero() {
		return r, nil, ErrMirrorConflict
	}
	identity, imageSize, err := privateBirthFileState(spoolFile)
	if err != nil {
		return r, nil, err
	}
	if imageSize != entry.Size {
		return r, nil, ErrMirrorConflict
	}
	r.FileIdentity = identity
	incarnation, err := NewSpoolCustodyID()
	if err != nil {
		return r, nil, err
	}
	r.Incarnation = incarnation
	e := entry.snapshot()
	e.Version = MirrorToken{}
	e.ResetGetAttrCache()

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	if StreamPartialName(e.Name) {
		return r, nil, ErrMirrorConflict
	}
	s.mu.RLock()
	conflict := s.preparedCreateConflictLocked(e.Path)
	if cached := s.pathCache[e.Path]; cached != nil && cached.Version != expected {
		conflict = true
	}
	s.mu.RUnlock()
	if conflict {
		return r, nil, ErrMirrorConflict
	}
	tx, releaseConn, err := s.beginSpoolDurableTxWithDiscard()
	if err != nil {
		return r, nil, err
	}
	discardConn := false
	defer func() { releaseConn(discardConn) }()
	commitStarted := false
	defer func() {
		if commitStarted {
			return
		}
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			discardConn = true
			r.Outcome = SpoolBirthUncertain
			born = nil
			retErr = &PendingSpoolBirth{Receipt: r, Cause: errors.Join(retErr, fmt.Errorf("birth rollback: %w", rollbackErr))}
		}
	}()
	if err := validateExpected(tx, expected); err != nil {
		return r, nil, err
	}
	if err := rejectCompletionPath(tx, e.Path, false); err != nil {
		return r, nil, err
	}
	var active, publishing int
	if err := tx.QueryRow(`SELECT count(*) FROM spool_entries WHERE nfs_path=? AND drain_state IN ('writing','ready','draining')`, e.Path).Scan(&active); err != nil {
		return r, nil, err
	}
	if err := tx.QueryRow(`SELECT count(*) FROM spool_bindings WHERE publish_path=? AND phase='publishing'`, e.Path).Scan(&publishing); err != nil {
		return r, nil, err
	}
	if active != 0 || publishing != 0 {
		return r, nil, ErrMirrorConflict
	}
	if err := ftsExternalUpsert(tx, e, s.ftsDefer); err != nil {
		return r, nil, fmt.Errorf("birth mirror %q: %w", e.Path, err)
	}
	// Generic upsert can preserve an old same-inode incarnation. CREATE
	// deliberately allocates a fresh identity before recording the binding.
	if _, err := tx.Exec(`UPDATE entries SET incarnation=?, revision=1 WHERE path=?`, incarnation, e.Path); err != nil {
		return r, nil, err
	}
	if err := loadEntryVersion(tx, e); err != nil {
		return r, nil, err
	}
	if e.Version.Incarnation != incarnation {
		return r, nil, ErrMirrorConflict
	}
	r.Revision = e.Version.Revision
	now := time.Now().Unix()
	res, err := tx.Exec(`INSERT INTO spool_entries(nfs_path,spool_file,size,drain_state,drain_attempts,created_at,updated_at)
 VALUES (?, ?, ?, 'writing', 0, ?, ?)`, e.Path, spoolFile, e.Size, now, now)
	if err != nil {
		return r, nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return r, nil, err
	}
	r.SpoolID = id
	res, err = tx.Exec(`INSERT INTO spool_bindings(spool_id,incarnation,birth_id,birth_path,birth_file,birth_identity) VALUES (?,?,?,?,?,?)`, id, incarnation, birthID, e.Path, spoolFile, identity)
	if err != nil {
		return r, nil, err
	}
	if err := exactlyOneRow(res); err != nil {
		return r, nil, err
	}
	commitStarted = true
	if err := tx.Commit(); err != nil {
		discardConn = true
		r.Outcome = SpoolBirthUncertain
		return r, nil, &PendingSpoolBirth{Receipt: r, Cause: err}
	}
	s.mu.Lock()
	s.publishEntryLocked(e)
	s.mu.Unlock()
	s.wakeFTSCompactor()
	if postCommit != nil {
		if err := postCommit(); err != nil {
			r.Outcome = SpoolBirthUncertain
			return r, nil, &PendingSpoolBirth{Receipt: r, Cause: err}
		}
	}
	r.Outcome = SpoolBirthCommitted
	return r, e.snapshot(), nil
}

func privateBirthFileIdentity(name string) (string, error) {
	identity, _, err := privateBirthFileState(name)
	return identity, err
}

func privateBirthFileState(name string) (string, int64, error) {
	fi, err := os.Lstat(name)
	if err != nil {
		return "", 0, err
	}
	if !fi.Mode().IsRegular() {
		return "", 0, ErrMirrorConflict
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return "", 0, ErrMirrorConflict
	}
	return fmt.Sprintf("%d:%d", uint64(st.Dev), st.Ino), fi.Size(), nil
}

// ResolveBoundSpoolBirthByNonce reads a durable birth using only the caller's
// pre-COMMIT intent. The caller must reopen the same database before boot
// recovery/writer admission and separately verify a retained private FD and
// local spool-root custody. Pathname Lstat is only a conservative cross-check.
func (s *Store) ResolveBoundSpoolBirthByNonce(intent SpoolBirthIntent) (SpoolBirthReceipt, *Entry, error) {
	r := SpoolBirthReceipt{BirthID: intent.BirthID, Path: intent.Path, SpoolFile: intent.SpoolFile, FileIdentity: intent.FileIdentity, Outcome: SpoolBirthUncertain}
	if !validCustodyID(intent.BirthID) || !validCustodyPath(intent.Path) || !validPrivatePath(intent.SpoolFile) || !validIdentity(intent.FileIdentity) {
		return r, nil, ErrMirrorConflict
	}
	// Even a caller mistake that passes a slow/nonlocal pathname cannot hold
	// Store's serving locks across this filesystem operation.
	currentIdentity, identityErr := privateBirthFileIdentity(intent.SpoolFile)
	if identityErr != nil || currentIdentity != intent.FileIdentity {
		return r, nil, ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return r, nil, err
	}
	defer tx.Rollback()
	var id int64
	var incarnation, name, file, fileIdentity, phase string
	var rowPath, rowFile, state sql.NullString
	err = tx.QueryRow(`SELECT b.spool_id,b.incarnation,b.birth_path,b.birth_file,b.birth_identity,b.phase,r.nfs_path,r.spool_file,r.drain_state
 FROM spool_bindings b LEFT JOIN spool_entries r ON r.id=b.spool_id WHERE b.birth_id=?`, intent.BirthID).
		Scan(&id, &incarnation, &name, &file, &fileIdentity, &phase, &rowPath, &rowFile, &state)
	if errors.Is(err, sql.ErrNoRows) {
		// A committed birth may have been retired or its binding changed before
		// this query. No negative receipt can be reconstructed from absence.
		return r, nil, ErrMirrorConflict
	}
	if err != nil {
		return r, nil, err
	}
	if id <= 0 || !validCustodyID(incarnation) || name != intent.Path || file != intent.SpoolFile || fileIdentity != intent.FileIdentity ||
		phase != "bound" || !rowPath.Valid || rowPath.String != intent.Path || !rowFile.Valid || rowFile.String != intent.SpoolFile ||
		!state.Valid || state.String != string(DrainWriting) {
		return r, nil, ErrMirrorConflict
	}
	e, err := scanEntry(tx.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, intent.Path))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return r, nil, ErrMirrorConflict
		}
		return r, nil, err
	}
	if e.Version.Incarnation != incarnation || e.Version.Revision <= 0 {
		return r, nil, ErrMirrorConflict
	}
	s.mu.RLock()
	if cached := s.pathCache[intent.Path]; cached != nil && cached.Version != e.Version {
		s.mu.RUnlock()
		return r, nil, ErrMirrorConflict
	}
	s.mu.RUnlock()
	if err := tx.Rollback(); err != nil {
		return r, nil, err
	}
	r.SpoolID, r.Incarnation, r.Revision, r.Outcome = id, incarnation, e.Version.Revision, SpoolBirthCommitted
	return r, e.snapshot(), nil
}

// ResolveBoundSpoolBirth keeps exact generated-result validation for a caller
// that did receive a complete receipt. Reopening never trusts its mutable
// Outcome field: a successful pre-COMMIT rollback is definitive only on the
// original InsertBoundSpoolBirth return, not after receipt persistence.
func (s *Store) ResolveBoundSpoolBirth(receipt SpoolBirthReceipt) (SpoolBirthOutcome, *Entry, error) {
	if !validCustodyID(receipt.Incarnation) || receipt.SpoolID <= 0 || receipt.Revision <= 0 {
		return SpoolBirthUncertain, nil, ErrMirrorConflict
	}
	intent := SpoolBirthIntent{BirthID: receipt.BirthID, Path: receipt.Path, SpoolFile: receipt.SpoolFile, FileIdentity: receipt.FileIdentity}
	resolved, e, err := s.ResolveBoundSpoolBirthByNonce(intent)
	if err != nil {
		return SpoolBirthUncertain, nil, err
	}
	if resolved.SpoolID != receipt.SpoolID || resolved.Incarnation != receipt.Incarnation || resolved.Revision != receipt.Revision {
		return SpoolBirthUncertain, nil, ErrMirrorConflict
	}
	return SpoolBirthCommitted, e, nil
}

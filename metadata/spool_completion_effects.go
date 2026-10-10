package metadata

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// BoundEffectsProof is an observation copied from the durable FULL hold. It
// authorizes no effect by itself; each transition revalidates the receipt.
type BoundEffectsProof struct {
	Receipt      BoundDrainCommitResult
	SourceSHA256 [32]byte
	Phase        string
}

type PendingBoundEffects struct {
	SpoolID int64
	Cause   error
}

// BoundRetirementCustody is constructed only after the original P38 record,
// image and directory identities are checked through pinned descriptors.
// Its fields are private so arbitrary unlinked files cannot retire SQL state.
type BoundRetirementCustody struct {
	receipt                                        BoundDrainCommitResult
	source, root, files, intents, record           *os.File
	rootPath, rootID, filesID, intentsID, recordID string
	recordSize                                     int64
	recordHash                                     [32]byte
}

type boundRetirementRecord struct {
	Version         int              `json:"version"`
	Intent          SpoolBirthIntent `json:"intent"`
	RootIdentity    string           `json:"root_identity"`
	FilesIdentity   string           `json:"files_identity"`
	IntentsIdentity string           `json:"intents_identity"`
	Size            int64            `json:"size"`
	SHA256          [32]byte         `json:"sha256"`
}

func retirementID(f *os.File) (string, uint64, error) {
	if f == nil {
		return "", 0, ErrMirrorConflict
	}
	fi, err := f.Stat()
	if err != nil {
		return "", 0, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return "", 0, ErrMirrorConflict
	}
	return fmt.Sprintf("%d:%d", uint64(st.Dev), st.Ino), uint64(st.Nlink), nil
}

func retirementOpenAt(parent *os.File, name string, directory bool) (*os.File, error) {
	flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC | unix.O_NONBLOCK
	if directory {
		flags |= unix.O_DIRECTORY
	}
	fd, err := unix.Openat(int(parent.Fd()), name, flags, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func (c *BoundRetirementCustody) Close() error {
	if c == nil {
		return nil
	}
	var err error
	for _, f := range []*os.File{c.record, c.intents, c.files, c.root} {
		if f != nil {
			err = errors.Join(err, f.Close())
		}
	}
	c.record, c.intents, c.files, c.root = nil, nil, nil, nil
	return err
}

// PrepareBoundRetirement pins and verifies the exact P38 birth namespace.
// The caller must retain the path lease and source FD until terminal success.
func PrepareBoundRetirement(r BoundDrainCommitResult, source *os.File, rootPath string) (c *BoundRetirementCustody, retErr error) {
	if source == nil || !validCustodyID(r.BirthID) || rootPath == "" || !filepath.IsAbs(rootPath) || filepath.Clean(rootPath) != rootPath ||
		r.SpoolFile != filepath.Join(rootPath, "files", "birth-"+r.BirthID) {
		return nil, ErrMirrorConflict
	}
	c = &BoundRetirementCustody{receipt: r, source: source, rootPath: rootPath}
	defer func() {
		if retErr != nil {
			_ = c.Close()
			c = nil
		}
	}()
	fd, err := unix.Open(rootPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return c, err
	}
	c.root = os.NewFile(uintptr(fd), rootPath)
	if c.files, err = retirementOpenAt(c.root, "files", true); err != nil {
		return c, err
	}
	if c.intents, err = retirementOpenAt(c.root, "birth-intents", true); err != nil {
		return c, err
	}
	if c.record, err = retirementOpenAt(c.intents, r.BirthID+".json", false); err != nil {
		return c, err
	}
	fi, err := c.record.Stat()
	if err != nil || !fi.Mode().IsRegular() {
		return c, ErrMirrorConflict
	}
	raw, readErr := io.ReadAll(io.LimitReader(c.record, 8193))
	if readErr != nil {
		return c, readErr
	}
	if len(raw) == 0 || len(raw) > 8192 {
		return c, ErrMirrorConflict
	}
	c.recordSize, c.recordHash = int64(len(raw)), sha256.Sum256(raw)
	var rec boundRetirementRecord
	if json.Unmarshal(raw, &rec) != nil || rec.Version != 1 || rec.Intent.BirthID != r.BirthID || rec.Intent.Path != r.Path || rec.Intent.SpoolFile != r.SpoolFile || rec.Intent.FileIdentity != r.FileIdentity {
		return c, ErrMirrorConflict
	}
	if c.rootID, _, err = retirementID(c.root); err != nil {
		return c, err
	}
	if c.rootID != rec.RootIdentity {
		return c, ErrMirrorConflict
	}
	if c.filesID, _, err = retirementID(c.files); err != nil {
		return c, err
	}
	if c.filesID != rec.FilesIdentity {
		return c, ErrMirrorConflict
	}
	if c.intentsID, _, err = retirementID(c.intents); err != nil {
		return c, err
	}
	if c.intentsID != rec.IntentsIdentity {
		return c, ErrMirrorConflict
	}
	if c.recordID, _, err = retirementID(c.record); err != nil {
		return c, err
	}
	if err = c.verifyLinked(); err != nil {
		return c, err
	}
	return c, nil
}

func (c *BoundRetirementCustody) verifyLinked() error {
	if c == nil || c.root == nil || c.files == nil || c.intents == nil || c.record == nil {
		return ErrMirrorConflict
	}
	for _, x := range []struct {
		f  *os.File
		id string
	}{{c.root, c.rootID}, {c.files, c.filesID}, {c.intents, c.intentsID}, {c.record, c.recordID}} {
		id, _, err := retirementID(x.f)
		if err != nil || id != x.id {
			return ErrMirrorConflict
		}
	}
	fi, err := c.record.Stat()
	if err != nil || !fi.Mode().IsRegular() || fi.Size() != c.recordSize {
		return ErrMirrorConflict
	}
	recordBytes := make([]byte, c.recordSize)
	if n, err := c.record.ReadAt(recordBytes, 0); err != nil || int64(n) != c.recordSize || sha256.Sum256(recordBytes) != c.recordHash {
		return ErrMirrorConflict
	}
	r := c.receipt
	fi, err = c.source.Stat()
	if err != nil || !fi.Mode().IsRegular() || fi.Size() != r.FullSize {
		return ErrMirrorConflict
	}
	id, _, err := retirementID(c.source)
	if err != nil || id != r.FileIdentity {
		return ErrMirrorConflict
	}
	for _, x := range []struct {
		parent   *os.File
		name, id string
		dir      bool
	}{{c.root, "files", c.filesID, true}, {c.root, "birth-intents", c.intentsID, true}, {c.files, "birth-" + r.BirthID, r.FileIdentity, false}, {c.intents, r.BirthID + ".json", c.recordID, false}} {
		f, err := retirementOpenAt(x.parent, x.name, x.dir)
		if err != nil {
			return ErrMirrorConflict
		}
		id, _, err := retirementID(f)
		_ = f.Close()
		if err != nil || id != x.id {
			return ErrMirrorConflict
		}
	}
	fd, err := unix.Open(c.rootPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return ErrMirrorConflict
	}
	f := os.NewFile(uintptr(fd), c.rootPath)
	id, _, err = retirementID(f)
	_ = f.Close()
	if err != nil || id != c.rootID {
		return ErrMirrorConflict
	}
	return nil
}

// verifySourceHash runs outside Store locks. The public transition must not
// accept same-inode, same-size edits made after PrepareBoundRetirement.
func (c *BoundRetirementCustody) sourceHash() ([32]byte, error) {
	if err := c.verifyLinked(); err != nil {
		return [32]byte{}, err
	}
	h := sha256.New()
	if n, err := io.Copy(h, io.NewSectionReader(c.source, 0, c.receipt.FullSize)); err != nil || n != c.receipt.FullSize {
		return [32]byte{}, ErrMirrorConflict
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	if err := c.verifyLinked(); err != nil {
		return [32]byte{}, err
	}
	return digest, nil
}

func (e *PendingBoundEffects) Error() string {
	return fmt.Sprintf("bound effects for spool %d require reconciliation: %v", e.SpoolID, e.Cause)
}
func (e *PendingBoundEffects) Unwrap() error { return e.Cause }

// captureBoundEffectsLocked runs with Store→cache-publication→spool locks.
func (s *Store) captureBoundEffectsLocked(r BoundDrainCommitResult) (BoundEffectsProof, error) {
	var proof BoundEffectsProof
	if err := s.validateBoundDrainCompletionLocked(r); err != nil {
		return proof, err
	}
	var digest []byte
	err := s.db.QueryRow(`SELECT source_sha256 FROM spool_completion_holds
	 WHERE completion_id=? AND spool_id=? AND kind='bound' AND birth_id=? AND nfs_path=? AND
	 spool_file=? AND birth_identity=? AND backing_identity=? AND size=? AND
	 incarnation=? AND published_revision=? AND original_inode=? AND original_mode=?`,
		r.CompletionID, r.SpoolID, r.BirthID, r.Path, r.SpoolFile, r.FileIdentity, r.BackingIdentity, r.FullSize,
		r.Published.Incarnation, r.Published.Revision, int64(r.OriginalInode), uint32(r.OriginalMode)).Scan(&digest)
	if err != nil || len(digest) != sha256.Size {
		return proof, ErrMirrorConflict
	}
	proof.Receipt = r
	copy(proof.SourceSHA256[:], digest)
	return proof, nil
}

// CaptureBoundEffects re-reads the current durable hold, row, binding and
// mirror generation; the mutable P40 result alone is never an effects grant.
func (s *Store) CaptureBoundEffects(r BoundDrainCommitResult) (BoundEffectsProof, error) {
	if s.spoolStore == nil {
		return BoundEffectsProof{}, ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	return s.captureBoundEffectsLocked(r)
}

// SettleBoundEffects persists a complete bounded Finder body and an
// outstanding reconcile hint before any private source may be removed. The
// caller hashes the pinned full image outside Store locks; this transaction
// compares that observation to the immutable hold digest.
func (s *Store) SettleBoundEffects(r BoundDrainCommitResult, imageHash [32]byte, sidecarBody []byte, sidecar bool) (retErr error) {
	if s.spoolStore == nil || (sidecar && (r.FullSize <= 0 || r.FullSize > 128<<10 || int64(len(sidecarBody)) != r.FullSize || sha256.Sum256(sidecarBody) != imageHash)) ||
		(!sidecar && sidecarBody != nil) {
		return ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	proof, err := s.captureBoundEffectsLocked(r)
	if err != nil || proof.SourceSHA256 != imageHash {
		return ErrMirrorConflict
	}
	var existing int
	if err := s.db.QueryRow(`SELECT count(*) FROM bound_completion_effects WHERE completion_id=?`, r.CompletionID).Scan(&existing); err != nil {
		return err
	}
	if existing != 0 {
		return ErrMirrorConflict
	}
	tx, release, err := s.beginSpoolDurableTxWithDiscard()
	if err != nil {
		return &PendingBoundEffects{SpoolID: r.SpoolID, Cause: err}
	}
	discard, committed := false, false
	defer func() {
		if !committed {
			if rb := tx.Rollback(); rb != nil {
				discard = true
				retErr = &PendingBoundEffects{SpoolID: r.SpoolID, Cause: errors.Join(retErr, rb)}
			}
		}
		release(discard)
	}()
	var body any
	if sidecar {
		body = sidecarBody
	}
	res, err := tx.Exec(`INSERT INTO bound_completion_effects
	 (completion_id,spool_id,birth_id,nfs_path,incarnation,published_revision,original_inode,size,source_sha256,sidecar_body,phase,reconcile_pending)
	 SELECT h.completion_id,h.spool_id,h.birth_id,h.nfs_path,h.incarnation,h.published_revision,h.original_inode,h.size,h.source_sha256,?,'ready',1
	 FROM spool_completion_holds h JOIN spool_entries e ON e.id=h.spool_id
	 JOIN spool_bindings b ON b.spool_id=h.spool_id JOIN entries m ON m.path=h.nfs_path
	 WHERE h.completion_id=? AND h.spool_id=? AND h.kind='bound' AND h.birth_id=? AND h.birth_identity=?
	 AND h.spool_file=? AND h.backing_identity=? AND h.size=? AND h.incarnation=? AND h.published_revision=?
	 AND h.source_sha256=? AND e.drain_state='done' AND e.spool_file=h.spool_file AND e.size=h.size
	 AND b.birth_id=h.birth_id AND b.birth_identity=h.birth_identity AND b.incarnation=h.incarnation AND b.phase='bound'
	 AND m.incarnation=h.incarnation AND m.revision=h.published_revision`,
		body, r.CompletionID, r.SpoolID, r.BirthID, r.FileIdentity, r.SpoolFile, r.BackingIdentity, r.FullSize,
		r.Published.Incarnation, r.Published.Revision, imageHash[:])
	if err != nil {
		return err
	}
	if err := exactlyOneRow(res); err != nil {
		return err
	}
	// Keep historical hint identities but at most the newest same-path body.
	// This runs in the same transaction as the new durable body insertion.
	if _, err := tx.Exec(`UPDATE bound_completion_effects SET sidecar_body=NULL
	 WHERE nfs_path=? AND completion_id<>? AND phase='retired'`, r.Path, r.CompletionID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		discard = true
		committed = true
		return &PendingBoundEffects{SpoolID: r.SpoolID, Cause: err}
	}
	committed = true
	return nil
}

// RetireBoundEffects records local settlement only. Its name is retained for
// existing callers; no qualified publication receipt exists in this slice.
// Both a successful transition and a failed transition remain pending.
func (s *Store) RetireBoundEffects(r BoundDrainCommitResult, w *BoundRetirementCustody) (retErr error) {
	if w == nil || w.receipt != r {
		return &PendingBoundEffects{SpoolID: r.SpoolID, Cause: ErrMirrorConflict}
	}
	if err := w.verifyLinked(); err != nil {
		return &PendingBoundEffects{SpoolID: r.SpoolID, Cause: err}
	}
	imageHash, err := w.sourceHash()
	if err != nil {
		return &PendingBoundEffects{SpoolID: r.SpoolID, Cause: err}
	}
	if s.spoolStore == nil {
		return &PendingBoundEffects{SpoolID: r.SpoolID, Cause: ErrMirrorConflict}
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	proof, err := s.captureBoundEffectsLocked(r)
	if err != nil {
		return &PendingBoundEffects{SpoolID: r.SpoolID, Cause: err}
	}
	if proof.SourceSHA256 != imageHash {
		return &PendingBoundEffects{SpoolID: r.SpoolID, Cause: ErrMirrorConflict}
	}
	if err := w.verifyLinked(); err != nil {
		return &PendingBoundEffects{SpoolID: r.SpoolID, Cause: err}
	}
	tx, release, err := s.beginSpoolDurableTxWithDiscard()
	if err != nil {
		return &PendingBoundEffects{SpoolID: r.SpoolID, Cause: err}
	}
	discard, committed := false, false
	defer func() {
		if !committed {
			if rb := tx.Rollback(); rb != nil {
				discard = true
				retErr = &PendingBoundEffects{SpoolID: r.SpoolID, Cause: errors.Join(retErr, rb)}
			}
		}
		release(discard)
	}()
	var phase string
	err = tx.QueryRow(`SELECT phase FROM bound_completion_effects WHERE completion_id=? AND spool_id=? AND birth_id=?
	 AND nfs_path=? AND incarnation=? AND published_revision=? AND original_inode=? AND size=? AND source_sha256=?
	 AND reconcile_pending IN (0,1)`,
		r.CompletionID, r.SpoolID, r.BirthID, r.Path, r.Published.Incarnation, r.Published.Revision,
		int64(r.OriginalInode), r.FullSize, proof.SourceSHA256[:]).Scan(&phase)
	if err != nil {
		return &PendingBoundEffects{SpoolID: r.SpoolID, Cause: err}
	}
	if phase == "awaiting_publication" {
		return &PendingBoundEffects{SpoolID: r.SpoolID, Cause: ErrBoundPublicationPending}
	}
	if phase != "ready" {
		return &PendingBoundEffects{SpoolID: r.SpoolID, Cause: ErrMirrorConflict}
	}
	res, err := tx.Exec(`UPDATE bound_completion_effects SET phase='awaiting_publication'
	 WHERE completion_id=? AND spool_id=? AND birth_id=? AND nfs_path=?
	 AND incarnation=? AND published_revision=? AND original_inode=? AND size=?
	 AND source_sha256=? AND phase='ready' AND reconcile_pending IN (0,1)
	 AND EXISTS (SELECT 1 FROM spool_completion_holds h JOIN spool_entries e ON e.id=h.spool_id
	 JOIN spool_bindings b ON b.spool_id=h.spool_id
	 WHERE h.completion_id=bound_completion_effects.completion_id AND h.spool_id=bound_completion_effects.spool_id
	 AND h.kind='bound' AND h.birth_id=bound_completion_effects.birth_id AND h.nfs_path=bound_completion_effects.nfs_path
	 AND h.incarnation=bound_completion_effects.incarnation AND h.published_revision=bound_completion_effects.published_revision
	 AND h.original_inode=bound_completion_effects.original_inode AND h.size=bound_completion_effects.size
	 AND h.source_sha256=bound_completion_effects.source_sha256
	 AND h.spool_file=e.spool_file AND h.size=e.size AND e.drain_state='done'
	 AND b.phase='bound' AND b.birth_id=h.birth_id AND b.birth_identity=h.birth_identity
	 AND b.incarnation=h.incarnation AND b.birth_file=e.spool_file)`,
		r.CompletionID, r.SpoolID, r.BirthID, r.Path, r.Published.Incarnation,
		r.Published.Revision, int64(r.OriginalInode), r.FullSize, proof.SourceSHA256[:])
	if err != nil {
		return &PendingBoundEffects{SpoolID: r.SpoolID, Cause: err}
	}
	if err := exactlyOneRow(res); err != nil {
		return &PendingBoundEffects{SpoolID: r.SpoolID, Cause: err}
	}
	if err := tx.Commit(); err != nil {
		discard = true
		committed = true
		return &PendingBoundEffects{SpoolID: r.SpoolID, Cause: err}
	}
	committed = true
	return &PendingBoundEffects{SpoolID: r.SpoolID, Cause: ErrBoundPublicationPending}
}

// ErrBoundPublicationPending means local SQL settled, while exact private
// source, intent, hold, binding, lease and capacity still await a qualified
// publication receipt.
var ErrBoundPublicationPending = errors.New("bound publication receipt pending")

type BoundEffectsRecoveryRow struct {
	CompletionID string
	SpoolID      int64
	Phase        string
}

type PendingBoundEffectsRecovery struct{ Effects []BoundEffectsRecoveryRow }

func (e *PendingBoundEffectsRecovery) Error() string {
	return fmt.Sprintf("bound effects require publication for %d row(s)", len(e.Effects))
}

// CheckBoundEffectsRecovery validates every phase before boot adoption. Ready
// and awaiting-publication rows must still have the exact FULL hold, bound
// binding, done spool row and mirror generation. Historical retired rows are
// readable only when their active hold is absent. Any unknown or mismatched
// state fails closed.
func (s *SpoolStore) CheckBoundEffectsRecovery() error {
	rows, err := s.db.Query(`SELECT completion_id,spool_id,phase,birth_id,nfs_path,incarnation,
	 published_revision,original_inode,size,source_sha256,reconcile_pending
	 FROM bound_completion_effects ORDER BY rowid`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var pending []BoundEffectsRecoveryRow
	for rows.Next() {
		var x BoundEffectsRecoveryRow
		var birthID, path, incarnation string
		var revision, inode, size int64
		var digest []byte
		var reconcilePending int
		if err := rows.Scan(&x.CompletionID, &x.SpoolID, &x.Phase, &birthID, &path, &incarnation,
			&revision, &inode, &size, &digest, &reconcilePending); err != nil {
			return err
		}
		if reconcilePending != 0 && reconcilePending != 1 {
			return ErrMirrorConflict
		}
		switch x.Phase {
		case "retired":
			var holds int
			if err := s.db.QueryRow(`SELECT count(*) FROM spool_completion_holds WHERE completion_id=? OR spool_id=?`,
				x.CompletionID, x.SpoolID).Scan(&holds); err != nil {
				return err
			}
			if holds != 0 {
				return ErrMirrorConflict
			}
		case "ready", "awaiting_publication":
			if len(digest) != sha256.Size || x.CompletionID == "" || x.SpoolID <= 0 ||
				birthID == "" || path == "" || incarnation == "" || size < 0 {
				return ErrMirrorConflict
			}
			var matches int
			err := s.db.QueryRow(`SELECT count(*) FROM spool_completion_holds h
			 JOIN spool_entries p ON p.id=h.spool_id
			 JOIN spool_bindings b ON b.spool_id=h.spool_id
			 JOIN entries m ON m.path=h.nfs_path
			 WHERE h.completion_id=? AND h.spool_id=? AND h.kind='bound'
			 AND h.birth_id=? AND h.nfs_path=? AND h.incarnation=? AND h.published_revision=?
			 AND h.original_inode=? AND h.size=? AND h.source_sha256=?
			 AND h.birth_identity=b.birth_identity AND h.birth_id=b.birth_id
			 AND h.spool_file=p.spool_file AND p.nfs_path=h.nfs_path AND p.size=h.size
			 AND p.sha256=h.source_sha256 AND p.drain_state='done' AND p.punched_end=0
			 AND b.phase='bound' AND b.incarnation=h.incarnation AND b.birth_path=h.nfs_path
			 AND b.birth_file=h.spool_file
			 AND m.incarnation=h.incarnation AND m.revision=h.published_revision
			 AND m.inode=h.original_inode AND m.size=h.size`,
				x.CompletionID, x.SpoolID, birthID, path, incarnation, revision, inode, size, digest).Scan(&matches)
			if err != nil {
				return err
			}
			if matches != 1 {
				return ErrMirrorConflict
			}
			pending = append(pending, x)
		default:
			return ErrMirrorConflict
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(pending) != 0 {
		return &PendingBoundEffectsRecovery{Effects: pending}
	}
	return nil
}

type BoundSidecarImage struct {
	Body         []byte
	Token        MirrorToken
	SourceSHA256 [32]byte
}

// PendingBoundReconcile is a durable, bounded dispatch identity. Dispatch
// acknowledgement means only that the publisher accepted a hint, not that a
// subscriber applied the authoritative parent rescan.
type PendingBoundReconcile struct {
	CompletionID string
	Path         string
}

func (s *Store) ListPendingBoundReconcile(limit int) ([]PendingBoundReconcile, error) {
	if limit < 1 || limit > 256 {
		return nil, ErrMirrorConflict
	}
	rows, err := s.db.Query(`SELECT completion_id,nfs_path FROM bound_completion_effects
	 WHERE reconcile_pending=1 ORDER BY rowid LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingBoundReconcile
	for rows.Next() {
		var x PendingBoundReconcile
		if err := rows.Scan(&x.CompletionID, &x.Path); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

// DispatchPendingBoundReconcile never does network work under Store locks.
// Failure leaves every undispatched identity pending for a later replay.
// Clearing this hint in ready or awaiting_publication is only a local dispatch
// acknowledgement; it cannot authorize publication or source retirement.
func (s *Store) DispatchPendingBoundReconcile(limit int, publish func(MetadataEvent) error) (int, error) {
	if publish == nil {
		return 0, ErrMirrorConflict
	}
	items, err := s.ListPendingBoundReconcile(limit)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, x := range items {
		if err := publish(MetadataEvent{Op: "reconcile", Path: x.Path}); err != nil {
			return count, err
		}
		res, err := s.db.Exec(`UPDATE bound_completion_effects SET reconcile_pending=0
		 WHERE completion_id=? AND nfs_path=? AND reconcile_pending=1`, x.CompletionID, x.Path)
		if err != nil {
			return count, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return count, err
		}
		if n == 1 {
			count++
		}
	}
	return count, nil
}

// BoundSidecarBody returns an exact-generation durable body. exists reports
// that a bound body owns the path even when it is stale, preventing a fallback
// to the older weak mtime-second/size cache after a successor appears.
func (s *Store) BoundSidecarBody(path string) (image BoundSidecarImage, exists, valid bool, err error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	var count int
	if err = s.db.QueryRow(`SELECT
	 (SELECT count(*) FROM bound_completion_effects WHERE nfs_path=?) +
	 (SELECT count(*) FROM spool_bindings b JOIN spool_entries e ON e.id=b.spool_id
	  WHERE e.nfs_path=? AND b.birth_id<>'')`, path, path).Scan(&count); err != nil {
		return image, false, false, err
	}
	exists = count > 0
	if !exists {
		return image, false, false, nil
	}
	var digest []byte
	err = s.db.QueryRow(`SELECT e.sidecar_body,e.source_sha256,e.incarnation,e.published_revision FROM bound_completion_effects e
	 JOIN entries m ON m.path=e.nfs_path AND m.incarnation=e.incarnation AND m.revision=e.published_revision
	 AND m.size=e.size AND m.inode=e.original_inode
	 WHERE e.nfs_path=? AND e.phase='retired' AND e.sidecar_body IS NOT NULL
	 ORDER BY e.rowid DESC LIMIT 1`, path).Scan(&image.Body, &digest, &image.Token.Incarnation, &image.Token.Revision)
	if errors.Is(err, sql.ErrNoRows) {
		return BoundSidecarImage{}, true, false, nil
	}
	if err != nil {
		return BoundSidecarImage{}, true, false, err
	}
	image.Token.Path = path
	s.mu.RLock()
	conflict := s.preparedCreateConflictLocked(path)
	if cached := s.pathCache[path]; cached != nil && (cached.Version != image.Token || cached.Size != int64(len(image.Body))) {
		conflict = true
	}
	s.mu.RUnlock()
	if conflict {
		return BoundSidecarImage{}, true, false, nil
	}
	computed := sha256.Sum256(image.Body)
	if len(digest) != sha256.Size || !bytes.Equal(computed[:], digest) {
		return BoundSidecarImage{}, true, false, ErrMirrorConflict
	}
	copy(image.SourceSHA256[:], digest)
	return image, true, true, nil
}

// updateBoundSidecarModeLocked is the only mode-only generation carry-forward.
// Caller holds Store.writeMu. A content mutation takes a different path and
// cannot inherit this durable body merely because size/mtime happen to match.
func (s *Store) updateBoundSidecarModeLocked(name string, mode fs.FileMode) (handled bool, retErr error) {
	legacy, err := s.boundModePreflightLocked(name)
	if err != nil {
		return true, err
	}
	if legacy {
		return false, nil
	}
	var completionID, incarnation string
	var revision, size, inode, spoolID int64
	err = s.db.QueryRow(`SELECT e.completion_id,e.incarnation,e.published_revision,e.size,e.original_inode,e.spool_id
	 FROM bound_completion_effects e JOIN entries m ON m.path=e.nfs_path AND m.incarnation=e.incarnation
	 AND m.revision=e.published_revision AND m.size=e.size AND m.inode=e.original_inode
	 WHERE e.nfs_path=? AND e.phase='retired' AND e.sidecar_body IS NOT NULL LIMIT 1`, name).
		Scan(&completionID, &incarnation, &revision, &size, &inode, &spoolID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if s.spoolStore == nil || s.spoolStore.db != s.db {
		return true, ErrMirrorConflict
	}
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	s.mu.RLock()
	conflict := s.preparedCreateConflictLocked(name)
	if cached := s.pathCache[name]; cached != nil && (cached.Version != (MirrorToken{Path: name, Incarnation: incarnation, Revision: revision}) ||
		cached.Size != size || cached.Inode != uint64(inode)) {
		conflict = true
	}
	s.mu.RUnlock()
	if conflict {
		return true, ErrMirrorConflict
	}
	tx, release, err := s.beginSpoolDurableTxWithDiscard()
	if err != nil {
		return true, err
	}
	discard, committed := false, false
	defer func() {
		if !committed {
			if rb := tx.Rollback(); rb != nil {
				discard = true
				retErr = &PendingBoundEffects{SpoolID: spoolID, Cause: errors.Join(retErr, rb)}
			}
		}
		release(discard)
	}()
	res, err := tx.Exec(`UPDATE entries SET mode=(mode & ~511)|? WHERE path=? AND incarnation=? AND revision=? AND size=? AND inode=?`,
		uint32(mode&fs.ModePerm), name, incarnation, revision, size, inode)
	if err != nil {
		return true, err
	}
	if err := exactlyOneRow(res); err != nil {
		return true, err
	}
	updated, err := scanEntry(tx.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, name))
	if err != nil {
		return true, err
	}
	if updated.Version.Revision != revision+1 || updated.Version.Incarnation != incarnation || updated.Size != size || updated.Inode != uint64(inode) {
		return true, ErrMirrorConflict
	}
	res, err = tx.Exec(`UPDATE bound_completion_effects SET published_revision=? WHERE completion_id=? AND nfs_path=?
	 AND incarnation=? AND published_revision=? AND size=? AND original_inode=? AND phase='retired' AND sidecar_body IS NOT NULL`,
		updated.Version.Revision, completionID, name, incarnation, revision, size, inode)
	if err != nil {
		return true, err
	}
	if err := exactlyOneRow(res); err != nil {
		return true, err
	}
	if err := tx.Commit(); err != nil {
		discard = true
		committed = true
		return true, &PendingBoundEffects{SpoolID: spoolID, Cause: err}
	}
	committed = true
	s.mu.Lock()
	s.publishEntryLocked(updated)
	s.mu.Unlock()
	return true, nil
}

// boundModePreflightLocked permits the old metadata-only chmod path only when
// the spool schema is wholly absent. A partial or detached schema cannot be
// treated as evidence that no bound completion owns this path.
func (s *Store) boundModePreflightLocked(name string) (legacy bool, retErr error) {
	if s.spoolStore != nil && s.spoolStore.db != s.db {
		return false, ErrMirrorConflict
	}
	const known = `'spool_entries','spool_bindings','spool_completion_holds','spool_custody','bound_completion_effects'`
	rows, err := s.db.Query(`SELECT name,type FROM main.sqlite_master WHERE name COLLATE NOCASE IN (` + known + `)`)
	if err != nil {
		return false, err
	}
	seen := make(map[string]bool, 5)
	for rows.Next() {
		var table, kind string
		if err := rows.Scan(&table, &kind); err != nil {
			_ = rows.Close()
			return false, err
		}
		if kind != "table" {
			_ = rows.Close()
			return false, ErrMirrorConflict
		}
		seen[strings.ToLower(table)] = true
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return false, err
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	if len(seen) == 0 && s.spoolStore == nil {
		return true, nil
	}
	if len(seen) != 5 {
		return false, ErrMirrorConflict
	}
	var effectsDDL string
	if err := s.db.QueryRow(`SELECT sql FROM main.sqlite_master WHERE name='bound_completion_effects' AND type='table'`).Scan(&effectsDDL); err != nil {
		return false, err
	}
	start := strings.Index(spoolSchema, "CREATE TABLE IF NOT EXISTS bound_completion_effects (")
	if start < 0 {
		return false, ErrMirrorConflict
	}
	end := strings.Index(spoolSchema[start:], ");")
	if end < 0 || boundEffectsDDLSignature(effectsDDL) != boundEffectsDDLSignature(spoolSchema[start:start+end+2]) {
		return false, ErrMirrorConflict
	}
	// LIMIT 0 validates the columns needed by the known spool schema without
	// reading any spool data or changing schema during a chmod.
	for _, query := range []string{
		`SELECT id,nfs_path,spool_file,size,sha256,drain_state,drain_attempts,last_error,suspect_zero_tail,punched_end,created_at,updated_at FROM main.spool_entries LIMIT 0`,
		`SELECT spool_id,incarnation,phase,stage_path,publish_path,birth_id,birth_path,birth_file,birth_identity FROM main.spool_bindings LIMIT 0`,
		`SELECT completion_id,spool_id,nfs_path,spool_file,size,kind,birth_id,birth_identity,incarnation,expected_revision,backing_identity,source_sha256,published_revision,original_inode,original_mode FROM main.spool_completion_holds LIMIT 0`,
		`SELECT operation_id,source_id,spool_id,spool_file,source_identity,store_id,incarnation,target_path,target_revision,epoch,phase,stage_path,stage_identity,target_eof,stage_eof,verified_end,punched_end,public_identity,published_revision,source_cleanup_claimed,stage_cleanup_claimed FROM main.spool_custody LIMIT 0`,
		`SELECT completion_id,spool_id,birth_id,nfs_path,incarnation,published_revision,original_inode,size,source_sha256,sidecar_body,phase,reconcile_pending FROM main.bound_completion_effects LIMIT 0`,
	} {
		check, err := s.db.Query(query)
		if err != nil {
			return false, err
		}
		if err := check.Close(); err != nil {
			return false, err
		}
	}
	var pending int
	err = s.db.QueryRow(`SELECT
	 (SELECT count(*) FROM main.bound_completion_effects WHERE nfs_path=? AND phase IN ('ready','awaiting_publication')) +
	 (SELECT count(*) FROM main.spool_completion_holds WHERE nfs_path=? AND kind='legacy') +
	 (SELECT count(*) FROM main.spool_completion_holds WHERE nfs_path=? AND kind='bound') +
	 (SELECT count(*) FROM main.bound_completion_effects e JOIN main.spool_completion_holds h
	  ON h.completion_id=e.completion_id OR h.spool_id=e.spool_id
	  WHERE e.nfs_path=? AND e.phase='retired')`, name, name, name, name).Scan(&pending)
	if err != nil {
		return false, err
	}
	if pending != 0 {
		return false, ErrMirrorConflict
	}
	var invalid int
	if err := s.db.QueryRow(`SELECT count(*) FROM main.bound_completion_effects WHERE nfs_path=?
	 AND (phase IS NULL OR phase NOT IN ('ready','awaiting_publication','retired'))`, name).Scan(&invalid); err != nil {
		return false, err
	}
	if invalid != 0 {
		return false, ErrMirrorConflict
	}
	return false, nil
}

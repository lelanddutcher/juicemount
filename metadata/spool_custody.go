package metadata

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strings"
)

// SpoolCustody records one operation's private source and stage. The identities
// must be obtained from open filesystem objects by the caller; a path lookup
// alone is not evidence. This ledger is opt-in and does not adopt old rows.
type SpoolCustody struct {
	OperationID, SourceID     string
	SpoolID                   int64
	SpoolFile, SourceIdentity string
	StoreID, Incarnation      string
	TargetPath                string
	TargetRevision, Epoch     int64
	Phase                     string
	StagePath, StageIdentity  string
	TargetEOF, StageEOF       int64
	VerifiedEnd, PunchedEnd   int64
	PublicIdentity            string
	PublishedRevision         int64
	SourceCleanupClaimed      bool
	StageCleanupClaimed       bool
}

const spoolCustodyColumns = `operation_id,source_id,spool_id,spool_file,source_identity,store_id,incarnation,
target_path,target_revision,epoch,phase,stage_path,stage_identity,target_eof,stage_eof,verified_end,
punched_end,public_identity,published_revision,source_cleanup_claimed,stage_cleanup_claimed`

// PendingSpoolCustody is an unresolved private-byte operation, not permission
// to replay or remove either pathname. InspectSpoolCustody supplies the exact
// operation records for an offline reconciler.
type PendingSpoolCustody struct{ Operations []SpoolCustody }

func (e *PendingSpoolCustody) Error() string {
	return fmt.Sprintf("spool custody requires reconciliation for %d operation(s)", len(e.Operations))
}

// NewSpoolCustodyID produces a 128-bit operation or source identifier. A
// producer should put the source ID into its private filename and persist it
// before creating any stage. IDs are never inferred from legacy row IDs.
func NewSpoolCustodyID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func validCustodyID(id string) bool {
	b, err := hex.DecodeString(id)
	return err == nil && len(b) == 16 && id == hex.EncodeToString(b)
}

func validCustodyPath(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.HasPrefix(name, "../") &&
		!strings.HasPrefix(name, "/") && !strings.ContainsRune(name, 0) && path.Clean(name) == name
}

func validPrivatePath(name string) bool {
	return name != "" && filepath.IsAbs(name) && filepath.Clean(name) == name && !strings.ContainsRune(name, 0)
}

func validIdentity(identity string) bool { return identity != "" && !strings.ContainsRune(identity, 0) }

func scanCustody(row *sql.Row) (SpoolCustody, error) {
	var c SpoolCustody
	var sourceClaim, stageClaim int
	err := row.Scan(&c.OperationID, &c.SourceID, &c.SpoolID, &c.SpoolFile, &c.SourceIdentity,
		&c.StoreID, &c.Incarnation, &c.TargetPath, &c.TargetRevision, &c.Epoch, &c.Phase,
		&c.StagePath, &c.StageIdentity, &c.TargetEOF, &c.StageEOF, &c.VerifiedEnd,
		&c.PunchedEnd, &c.PublicIdentity, &c.PublishedRevision, &sourceClaim, &stageClaim)
	if err != nil {
		return SpoolCustody{}, err
	}
	c.SourceCleanupClaimed, c.StageCleanupClaimed = sourceClaim == 1, stageClaim == 1
	if !validCustodyID(c.OperationID) || !validCustodyID(c.SourceID) || c.OperationID == c.SourceID ||
		c.SpoolID <= 0 || !validPrivatePath(c.SpoolFile) || !validIdentity(c.SourceIdentity) ||
		c.StoreID == "" || c.Incarnation == "" || !validCustodyPath(c.TargetPath) ||
		c.TargetRevision < 1 || c.Epoch < 1 || !validPrivatePath(c.StagePath) ||
		c.StagePath == c.SpoolFile || c.TargetEOF < 0 || c.StageEOF < 0 ||
		c.VerifiedEnd < 0 || c.VerifiedEnd > c.StageEOF || c.VerifiedEnd > c.TargetEOF ||
		c.PunchedEnd < 0 || c.PunchedEnd > c.VerifiedEnd ||
		(sourceClaim != 0 && sourceClaim != 1) || (stageClaim != 0 && stageClaim != 1) {
		return SpoolCustody{}, ErrMirrorConflict
	}
	return c, nil
}

func loadCustody(q versionQuerier, operationID string) (SpoolCustody, error) {
	return scanCustody(q.QueryRow(`SELECT `+spoolCustodyColumns+` FROM spool_custody WHERE operation_id=?`, operationID))
}

// InspectSpoolCustody retains all states, including terminal cleanup claims.
// A missing table is an error, not proof that legacy spool rows are safe.
func (s *Store) InspectSpoolCustody() ([]SpoolCustody, error) {
	rows, err := s.db.Query(`SELECT operation_id FROM spool_custody ORDER BY operation_id`)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	operations := make([]SpoolCustody, 0, len(ids))
	for _, id := range ids {
		c, err := loadCustody(s.db, id)
		if err != nil {
			return nil, err
		}
		operations = append(operations, c)
	}
	return operations, nil
}

func (s *Store) CheckSpoolCustodyRecovery() error {
	operations, err := s.InspectSpoolCustody()
	if err != nil {
		return fmt.Errorf("inspect spool custody: %w", err)
	}
	var pending []SpoolCustody
	for _, c := range operations {
		if c.Phase != "published" {
			pending = append(pending, c)
		}
	}
	if len(pending) != 0 {
		return &PendingSpoolCustody{Operations: pending}
	}
	return nil
}

// withCustodyTx gives every pre-irreversible transition one pinned FULL/WAL
// connection. Lock order matches Store's existing bound-spool operations.
func (s *Store) withCustodyTx(fn func(*sql.Tx) (SpoolCustody, error)) (SpoolCustody, error) {
	if s.spoolStore == nil {
		return SpoolCustody{}, ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	tx, closeConn, err := s.beginOnlineMutationTx()
	if err != nil {
		return SpoolCustody{}, err
	}
	defer closeConn()
	defer tx.Rollback()
	c, err := fn(tx)
	if err != nil {
		return SpoolCustody{}, err
	}
	if err := tx.Commit(); err != nil {
		return SpoolCustody{}, err
	}
	return c, nil
}

// custodySourceExists also refuses a source held as copied from another Mac
// (clone_hold, see SpoolStore.HoldRowsCopiedFromOtherMac): no custody step
// may stage, claim or publish it until the user decides.
func custodySourceExists(tx *sql.Tx, c SpoolCustody) error {
	var count int
	err := tx.QueryRow(`SELECT COUNT(*) FROM spool_entries r JOIN spool_bindings b ON b.spool_id=r.id
 WHERE r.id=? AND r.spool_file=? AND r.nfs_path=? AND r.punched_end<=?
 AND r.drain_state IN ('writing','ready','draining') AND r.clone_hold='' AND b.incarnation=? AND b.phase='bound'`,
		c.SpoolID, c.SpoolFile, c.TargetPath, c.PunchedEnd, c.Incarnation).Scan(&count)
	if err != nil {
		return err
	}
	if count != 1 {
		return ErrMirrorConflict
	}
	return nil
}

func custodySourceFinalized(tx *sql.Tx, c SpoolCustody) error {
	if err := custodySourceExists(tx, c); err != nil {
		return err
	}
	var count int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM spool_entries WHERE id=? AND size=? AND punched_end=? AND drain_state='draining' AND clone_hold=''`,
		c.SpoolID, c.TargetEOF, c.PunchedEnd).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return ErrMirrorConflict
	}
	return nil
}

// BeginSpoolCustody commits a private stage-allocation intent before its
// caller may create that stage. The caller supplies independently generated
// operation/source IDs and an identity captured from the open source object.
func (s *Store) BeginSpoolCustody(row *SpoolRow, expected MirrorToken, operationID, sourceID, sourceIdentity, stagePath string) (SpoolCustody, error) {
	if row == nil || !validCustodyID(operationID) || !validCustodyID(sourceID) || operationID == sourceID ||
		!validIdentity(sourceIdentity) || !validPrivatePath(stagePath) || !validPrivatePath(row.SpoolFile) ||
		stagePath == row.SpoolFile || !validCustodyPath(row.NFSPath) || row.Size < 0 ||
		expected.Path != row.NFSPath || expected.Incarnation == "" {
		return SpoolCustody{}, ErrMirrorConflict
	}
	return s.withCustodyTx(func(tx *sql.Tx) (SpoolCustody, error) {
		if err := validateExpected(tx, expected); err != nil {
			return SpoolCustody{}, err
		}
		c := SpoolCustody{OperationID: operationID, SourceID: sourceID, SpoolID: row.ID,
			SpoolFile: row.SpoolFile, SourceIdentity: sourceIdentity, Incarnation: expected.Incarnation,
			TargetPath: row.NFSPath, TargetRevision: expected.Revision, Epoch: 1,
			Phase: "allocating", StagePath: stagePath, TargetEOF: row.Size}
		if err := custodySourceExists(tx, c); err != nil {
			return SpoolCustody{}, err
		}
		if err := tx.QueryRow(`SELECT value FROM store_meta WHERE key='mirror_store_id'`).Scan(&c.StoreID); err != nil {
			return SpoolCustody{}, err
		}
		if c.StoreID == "" {
			return SpoolCustody{}, ErrMirrorConflict
		}
		res, err := tx.Exec(`INSERT INTO spool_custody(operation_id,source_id,spool_id,spool_file,source_identity,
 store_id,incarnation,target_path,target_revision,epoch,phase,stage_path,target_eof)
 VALUES (?,?,?,?,?,?,?,?,?,1,'allocating',?,?)`, c.OperationID, c.SourceID, c.SpoolID,
			c.SpoolFile, c.SourceIdentity, c.StoreID, c.Incarnation, c.TargetPath, c.TargetRevision,
			c.StagePath, c.TargetEOF)
		if err != nil {
			return SpoolCustody{}, err
		}
		if err := exactlyOneRow(res); err != nil {
			return SpoolCustody{}, err
		}
		return loadCustody(tx, operationID)
	})
}

func (s *Store) advanceCustody(expected SpoolCustody, fn func(*sql.Tx, SpoolCustody) error) (SpoolCustody, error) {
	if !validCustodyID(expected.OperationID) || expected.Epoch < 1 {
		return SpoolCustody{}, ErrMirrorConflict
	}
	return s.withCustodyTx(func(tx *sql.Tx) (SpoolCustody, error) {
		current, err := loadCustody(tx, expected.OperationID)
		if err != nil {
			return SpoolCustody{}, err
		}
		if current != expected {
			return SpoolCustody{}, ErrMirrorConflict
		}
		if err := fn(tx, current); err != nil {
			return SpoolCustody{}, err
		}
		next, err := loadCustody(tx, expected.OperationID)
		if err != nil {
			return SpoolCustody{}, err
		}
		if next.Epoch != current.Epoch+1 {
			return SpoolCustody{}, ErrMirrorConflict
		}
		return next, nil
	})
}

// BindSpoolStage records the identity of the exact O_EXCL-created stage.
func (s *Store) BindSpoolStage(expected SpoolCustody, sourceIdentity, stageIdentity string) (SpoolCustody, error) {
	if !validIdentity(stageIdentity) || !validIdentity(sourceIdentity) {
		return SpoolCustody{}, ErrMirrorConflict
	}
	return s.advanceCustody(expected, func(tx *sql.Tx, c SpoolCustody) error {
		if c.Phase != "allocating" || c.SourceIdentity != sourceIdentity {
			return ErrMirrorConflict
		}
		if err := custodySourceExists(tx, c); err != nil {
			return err
		}
		res, err := tx.Exec(`UPDATE spool_custody SET phase='staged',stage_identity=?,epoch=epoch+1
 WHERE operation_id=? AND epoch=? AND phase='allocating' AND source_identity=?`,
			stageIdentity, c.OperationID, c.Epoch, sourceIdentity)
		if err != nil {
			return err
		}
		return exactlyOneRow(res)
	})
}

// PrepareSpoolCheckpoint persists a verified-and-synced stage prefix and a
// possible source punch before F_PUNCHHOLE. The pending state is deliberately
// ambiguous on restart, even if the punch syscall had not yet run. sourceEOF
// comes from the open source descriptor and may grow while the row is writing;
// spool_entries.size is not finalized until close.
func (s *Store) PrepareSpoolCheckpoint(expected SpoolCustody, sourceIdentity, stageIdentity string, sourceEOF, stageEOF, verifiedEnd, punchedEnd int64) (SpoolCustody, error) {
	return s.advanceCustody(expected, func(tx *sql.Tx, c SpoolCustody) error {
		if c.Phase != "staged" || c.SourceIdentity != sourceIdentity || c.StageIdentity != stageIdentity ||
			verifiedEnd < c.VerifiedEnd || punchedEnd < c.PunchedEnd || punchedEnd > verifiedEnd ||
			sourceEOF < c.TargetEOF || stageEOF < c.StageEOF || stageEOF < verifiedEnd ||
			stageEOF > sourceEOF || verifiedEnd > sourceEOF ||
			(sourceEOF == c.TargetEOF && verifiedEnd == c.VerifiedEnd && punchedEnd == c.PunchedEnd) {
			return ErrMirrorConflict
		}
		if err := custodySourceExists(tx, c); err != nil {
			return err
		}
		res, err := tx.Exec(`UPDATE spool_custody SET phase='checkpoint_pending',stage_eof=?,verified_end=?,
 punched_end=?,target_eof=?,epoch=epoch+1 WHERE operation_id=? AND epoch=? AND phase='staged'
 AND source_identity=? AND stage_identity=?`, stageEOF, verifiedEnd, punchedEnd, sourceEOF,
			c.OperationID, c.Epoch, sourceIdentity, stageIdentity)
		if err != nil {
			return err
		}
		return exactlyOneRow(res)
	})
}

// ConfirmSpoolCheckpoint is only for a caller that has checked the exact
// source and stage objects after the punch and any required source sync.
func (s *Store) ConfirmSpoolCheckpoint(expected SpoolCustody, sourceIdentity, stageIdentity string) (SpoolCustody, error) {
	return s.advanceCustody(expected, func(tx *sql.Tx, c SpoolCustody) error {
		if c.Phase != "checkpoint_pending" || c.SourceIdentity != sourceIdentity || c.StageIdentity != stageIdentity {
			return ErrMirrorConflict
		}
		if err := custodySourceExists(tx, c); err != nil {
			return err
		}
		var persistedPunch int64
		if err := tx.QueryRow(`SELECT punched_end FROM spool_entries WHERE id=?`, c.SpoolID).Scan(&persistedPunch); err != nil {
			return err
		}
		if persistedPunch != c.PunchedEnd {
			return ErrMirrorConflict
		}
		res, err := tx.Exec(`UPDATE spool_custody SET phase='staged',epoch=epoch+1
 WHERE operation_id=? AND epoch=? AND phase='checkpoint_pending' AND source_identity=? AND stage_identity=?`,
			c.OperationID, c.Epoch, sourceIdentity, stageIdentity)
		if err != nil {
			return err
		}
		return exactlyOneRow(res)
	})
}

// RetargetSpoolCustody carries the same private bytes through an exact mirror
// rename/requeue. It cannot resolve an ambiguous punch or publication. The
// namespace coordinator must hold its reservation until this commits.
func (s *Store) RetargetSpoolCustody(expected SpoolCustody, row *SpoolRow, target MirrorToken) (SpoolCustody, error) {
	if row == nil || !validCustodyPath(row.NFSPath) || target.Path != row.NFSPath || target.Incarnation == "" {
		return SpoolCustody{}, ErrMirrorConflict
	}
	return s.advanceCustody(expected, func(tx *sql.Tx, c SpoolCustody) error {
		if (c.Phase != "allocating" && c.Phase != "staged") || target.Incarnation != c.Incarnation ||
			row.SpoolFile != c.SpoolFile || row.Size < 0 || row.PunchedEnd != c.PunchedEnd {
			return ErrMirrorConflict
		}
		if err := validateExpected(tx, target); err != nil {
			return err
		}
		candidate := c
		candidate.SpoolID, candidate.TargetPath, candidate.TargetEOF = row.ID, row.NFSPath, max(row.Size, c.TargetEOF)
		if err := custodySourceExists(tx, candidate); err != nil {
			return err
		}
		res, err := tx.Exec(`UPDATE spool_custody SET spool_id=?,target_path=?,target_revision=?,target_eof=?,epoch=epoch+1
 WHERE operation_id=? AND epoch=? AND phase=?`, row.ID, row.NFSPath, target.Revision, candidate.TargetEOF,
			c.OperationID, c.Epoch, c.Phase)
		if err != nil {
			return err
		}
		return exactlyOneRow(res)
	})
}

// ClaimPunchedSpool is the only ready->draining claim for a source with holes.
// The ordinary drainer excludes punched rows: it cannot reconstruct their
// prefix from the local spool file. This claim is reserved for the operation
// with the verified private stage and exact persisted punched boundary.
func (s *Store) ClaimPunchedSpool(expected SpoolCustody, target MirrorToken) (SpoolCustody, error) {
	return s.advanceCustody(expected, func(tx *sql.Tx, c SpoolCustody) error {
		if c.Phase != "staged" || c.PunchedEnd <= 0 || c.StageIdentity == "" ||
			c.VerifiedEnd < c.PunchedEnd || target.Path != c.TargetPath ||
			target.Incarnation != c.Incarnation || target.Revision != c.TargetRevision {
			return ErrMirrorConflict
		}
		if err := validateExpected(tx, target); err != nil {
			return err
		}
		if err := custodySourceExists(tx, c); err != nil {
			return err
		}
		res, err := tx.Exec(`UPDATE spool_entries SET drain_state='draining',updated_at=strftime('%s','now')
 WHERE id=? AND nfs_path=? AND spool_file=? AND size=? AND punched_end=? AND drain_state='ready' AND clone_hold=''`,
			c.SpoolID, c.TargetPath, c.SpoolFile, c.TargetEOF, c.PunchedEnd)
		if err != nil {
			return err
		}
		if err := exactlyOneRow(res); err != nil {
			return err
		}
		res, err = tx.Exec(`UPDATE spool_custody SET epoch=epoch+1
 WHERE operation_id=? AND epoch=? AND phase='staged'`, c.OperationID, c.Epoch)
		if err != nil {
			return err
		}
		return exactlyOneRow(res)
	})
}

// BeginCustodyPublish is the last durable marker before an external rename.
// It cannot run with an incomplete or ambiguous stage checkpoint.
func (s *Store) BeginCustodyPublish(expected SpoolCustody, target MirrorToken, sourceIdentity, stageIdentity string) (SpoolCustody, error) {
	return s.advanceCustody(expected, func(tx *sql.Tx, c SpoolCustody) error {
		if c.Phase != "staged" || target.Path != c.TargetPath || target.Incarnation != c.Incarnation ||
			target.Revision != c.TargetRevision || c.SourceIdentity != sourceIdentity ||
			c.StageIdentity != stageIdentity || c.StageEOF != c.TargetEOF || c.VerifiedEnd != c.TargetEOF {
			return ErrMirrorConflict
		}
		if err := validateExpected(tx, target); err != nil {
			return err
		}
		if err := custodySourceFinalized(tx, c); err != nil {
			return err
		}
		res, err := tx.Exec(`UPDATE spool_custody SET phase='publish_pending',epoch=epoch+1
 WHERE operation_id=? AND epoch=? AND phase='staged' AND source_identity=? AND stage_identity=?`,
			c.OperationID, c.Epoch, sourceIdentity, stageIdentity)
		if err != nil {
			return err
		}
		return exactlyOneRow(res)
	})
}

// ConfirmCustodyPublished records an exact successful mirror/public-object
// observation after the publisher has durably installed the stage. This API
// does not itself prove filesystem Sync or a remote shared-writer lease.
func (s *Store) ConfirmCustodyPublished(expected SpoolCustody, published MirrorToken, publicIdentity string) (SpoolCustody, error) {
	if !validIdentity(publicIdentity) {
		return SpoolCustody{}, ErrMirrorConflict
	}
	return s.advanceCustody(expected, func(tx *sql.Tx, c SpoolCustody) error {
		if c.Phase != "publish_pending" || published.Path != c.TargetPath ||
			published.Incarnation != c.Incarnation || published.Revision <= c.TargetRevision ||
			c.VerifiedEnd != c.TargetEOF || c.StageEOF != c.TargetEOF {
			return ErrMirrorConflict
		}
		if err := validateExpected(tx, published); err != nil {
			return err
		}
		var size int64
		if err := tx.QueryRow(`SELECT size FROM entries WHERE path=?`, c.TargetPath).Scan(&size); err != nil {
			return err
		}
		if size != c.TargetEOF {
			return ErrMirrorConflict
		}
		res, err := tx.Exec(`UPDATE spool_custody SET phase='published',public_identity=?,published_revision=?,epoch=epoch+1
 WHERE operation_id=? AND epoch=? AND phase='publish_pending'`, publicIdentity, published.Revision,
			c.OperationID, c.Epoch)
		if err != nil {
			return err
		}
		return exactlyOneRow(res)
	})
}

// CancelSpoolCustody never authorizes cleanup. Stage allocation, a possible
// punch, or a pending rename always leaves a quarantine record for recovery.
func (s *Store) CancelSpoolCustody(expected SpoolCustody) (SpoolCustody, error) {
	return s.advanceCustody(expected, func(tx *sql.Tx, c SpoolCustody) error {
		if c.Phase == "published" || c.Phase == "canceled" || c.Phase == "quarantined" {
			return ErrMirrorConflict
		}
		res, err := tx.Exec(`UPDATE spool_custody SET phase='quarantined',epoch=epoch+1
 WHERE operation_id=? AND epoch=? AND phase=?`, c.OperationID, c.Epoch, c.Phase)
		if err != nil {
			return err
		}
		return exactlyOneRow(res)
	})
}

// ClaimSpoolCleanup returns an identity-bound claim for one private object,
// never a public pathname. The caller must still verify that exact object and
// the durable public copy before unlinking. A canceled/ambiguous operation has
// no cleanup claim API, even if its spool_entries row was deleted.
func (s *Store) ClaimSpoolCleanup(expected SpoolCustody, published MirrorToken, publicIdentity, object, privateIdentity string) (SpoolCustody, error) {
	if object != "source" && object != "stage" {
		return SpoolCustody{}, ErrMirrorConflict
	}
	return s.advanceCustody(expected, func(tx *sql.Tx, c SpoolCustody) error {
		if c.Phase != "published" || c.PublicIdentity != publicIdentity || !validIdentity(publicIdentity) ||
			published.Path != c.TargetPath || published.Incarnation != c.Incarnation ||
			published.Revision != c.PublishedRevision || c.VerifiedEnd != c.TargetEOF ||
			c.StageEOF != c.TargetEOF || c.StageIdentity == "" {
			return ErrMirrorConflict
		}
		if err := validateExpected(tx, published); err != nil {
			return err
		}
		column := "source_cleanup_claimed"
		if object == "stage" {
			column = "stage_cleanup_claimed"
			if privateIdentity != c.StageIdentity {
				return ErrMirrorConflict
			}
		} else if privateIdentity != c.SourceIdentity {
			return ErrMirrorConflict
		}
		res, err := tx.Exec(`UPDATE spool_custody SET `+column+`=1,epoch=epoch+1
 WHERE operation_id=? AND epoch=? AND phase='published' AND `+column+`=0`, c.OperationID, c.Epoch)
		if err != nil {
			return err
		}
		return exactlyOneRow(res)
	})
}

// SpoolCustodyForSource refuses a missing operation instead of falling back
// to the legacy path-only spool row.
func (s *Store) SpoolCustodyForSource(sourceID string) (SpoolCustody, error) {
	if !validCustodyID(sourceID) {
		return SpoolCustody{}, ErrMirrorConflict
	}
	var operationID string
	err := s.db.QueryRow(`SELECT operation_id FROM spool_custody WHERE source_id=?`, sourceID).Scan(&operationID)
	if errors.Is(err, sql.ErrNoRows) {
		return SpoolCustody{}, ErrMirrorConflict
	}
	if err != nil {
		return SpoolCustody{}, err
	}
	return loadCustody(s.db, operationID)
}

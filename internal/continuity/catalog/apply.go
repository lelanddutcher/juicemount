package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// Apply checks an identical retry before checking mutable preconditions. Every
// successful new request appends one event, even if a revision already exists.
// A commit error has an uncertain outcome; retry only this exact ID and payload.
func (s *Store) Apply(ctx context.Context, input Operation) (Receipt, error) {
	if err := s.enter(); err != nil {
		return Receipt{}, err
	}
	defer s.leave()
	op, request, digest, err := freezeOperation(input)
	if err != nil {
		return Receipt{}, err
	}
	if err := s.requireLibrary(op.LibraryID); err != nil {
		return Receipt{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Receipt{}, err
	}
	defer tx.Rollback()
	library, err := validateSchema(ctx, tx)
	if err != nil {
		return Receipt{}, err
	}
	if library != s.library {
		return Receipt{}, corrupt("library identity changed")
	}
	if err := checkSettings(ctx, tx); err != nil {
		return Receipt{}, err
	}
	prior, priorRequest, err := readEvent(ctx, tx, s.library, op.ID)
	if err == nil {
		if prior.Receipt.PayloadSHA256 != digest || !bytes.Equal(priorRequest, request) {
			return Receipt{}, ErrChangedRequest
		}
		return prior.Receipt, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Receipt{}, err
	}
	var last int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0) FROM events`).Scan(&last); err != nil {
		return Receipt{}, err
	}
	if last == math.MaxInt64 {
		return Receipt{}, corrupt("event sequence exhausted")
	}
	sequence := last + 1
	for i, change := range op.Changes {
		if err := s.applyChange(ctx, tx, change, sequence); err != nil {
			return Receipt{}, fmt.Errorf("catalog change %d: %w", i, err)
		}
	}
	receipt := Receipt{OperationVersion, s.library, op.ID, sequence, digest, time.Now().UnixNano()}
	receiptJSON, err := json.Marshal(receipt)
	if err != nil {
		return Receipt{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(sequence,library_id,operation_id,request,payload_sha256,recorded_unix_nano,receipt) VALUES(?,?,?,?,?,?,?)`,
		sequence, s.library, op.ID, request, digest, receipt.RecordedUnixNano, receiptJSON); err != nil {
		return Receipt{}, err
	}
	if err := tx.Commit(); err != nil {
		return Receipt{}, fmt.Errorf("catalog commit outcome uncertain: %w", err)
	}
	return receipt, nil
}

func (s *Store) applyChange(ctx context.Context, tx *sql.Tx, c Change, sequence int64) error {
	switch {
	case c.CreateAsset != nil:
		v := c.CreateAsset
		result, err := tx.ExecContext(ctx, `INSERT INTO assets(asset_id,library_id,label,created_sequence,updated_sequence) VALUES(?,?,?,?,?) ON CONFLICT(asset_id) DO NOTHING`,
			v.AssetID, s.library, v.Label, sequence, sequence)
		return changedOne(result, err)
	case c.RenameAsset != nil:
		v := c.RenameAsset
		if _, err := readAsset(ctx, tx, s.library, v.AssetID); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE assets SET label=?,updated_sequence=? WHERE asset_id=? AND library_id=? AND label=?`,
			v.Label, sequence, v.AssetID, s.library, v.ExpectedLabel)
		return changedOne(result, err)
	case c.RecordRevision != nil:
		return s.recordRevision(ctx, tx, *c.RecordRevision, sequence)
	case c.AdvanceHead != nil:
		v := c.AdvanceHead
		if _, err := readAsset(ctx, tx, s.library, v.AssetID); err != nil {
			return err
		}
		revision, err := readRevision(ctx, tx, s.library, v.Revision)
		if err != nil {
			return err
		}
		if revision.Manifest.AssetID != v.AssetID {
			return fmt.Errorf("%w: head belongs to another asset", ErrConflict)
		}
		if v.ExpectedRevision == "" {
			if len(revision.Manifest.Parents) != 0 {
				return fmt.Errorf("%w: initial head must be a root", ErrConflict)
			}
		} else {
			found := false
			for _, parent := range revision.Manifest.Parents {
				if parent == v.ExpectedRevision {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("%w: previous head must be a direct parent", ErrConflict)
			}
		}
		// The SQL predicate, not a preceding Go comparison, is the head CAS.
		result, err := tx.ExecContext(ctx, `UPDATE assets SET head=?,updated_sequence=? WHERE asset_id=? AND library_id=? AND ((head IS NULL AND ?='') OR head=?)`,
			v.Revision, sequence, v.AssetID, s.library, v.ExpectedRevision, v.ExpectedRevision)
		return changedOne(result, err)
	case c.PutRelationship != nil:
		return s.putRelationship(ctx, tx, *c.PutRelationship, sequence)
	case c.RemoveRelationship != nil:
		return s.removeRelationship(ctx, tx, *c.RemoveRelationship, sequence)
	case c.PutLocation != nil:
		return s.putLocation(ctx, tx, *c.PutLocation, sequence)
	case c.RemoveLocation != nil:
		return s.removeLocation(ctx, tx, *c.RemoveLocation, sequence)
	default:
		return invalid("unknown change")
	}
}

func changedOne(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrConflict
	}
	return nil
}

func (s *Store) recordRevision(ctx context.Context, tx *sql.Tx, manifest continuity.Manifest, sequence int64) error {
	id, err := manifest.RevisionID()
	if err != nil {
		return err
	}
	canonical, err := manifest.Canonical()
	if err != nil {
		return err
	}
	prior, err := readRevision(ctx, tx, s.library, id)
	if err == nil {
		if !bytes.Equal(prior.Canonical, canonical) {
			return corrupt("revision digest collision or changed immutable bytes")
		}
		return nil
	}
	if !errors.Is(err, ErrNotFound) {
		return err
	}
	if _, err := readAsset(ctx, tx, s.library, manifest.AssetID); err != nil {
		return err
	}
	for _, parentID := range manifest.Parents {
		parent, err := readRevision(ctx, tx, s.library, parentID)
		if err != nil {
			return err
		}
		if parent.Manifest.AssetID != manifest.AssetID {
			return fmt.Errorf("%w: cross-asset parent", ErrConflict)
		}
	}
	manifestData, err := json.Marshal(manifestWire(manifest))
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO revisions(revision_id,library_id,asset_id,canonical,manifest_json,created_sequence) VALUES(?,?,?,?,?,?)`,
		id, s.library, manifest.AssetID, canonical, manifestData, sequence); err != nil {
		return err
	}
	for _, parent := range manifest.Parents {
		if _, err := tx.ExecContext(ctx, `INSERT INTO revision_parents(revision_id,parent_id,asset_id) VALUES(?,?,?)`, id, parent, manifest.AssetID); err != nil {
			return err
		}
	}
	return nil
}

package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func requireAnchor(ctx context.Context, tx *sql.Tx, library continuity.LibraryID, asset continuity.AssetID, revision continuity.RevisionID) error {
	if _, err := readAsset(ctx, tx, library, asset); err != nil {
		return err
	}
	if revision != "" {
		r, err := readRevision(ctx, tx, library, revision)
		if err != nil {
			return err
		}
		if r.Manifest.AssetID != asset {
			return fmt.Errorf("%w: revision anchor belongs to another asset", ErrConflict)
		}
	}
	return nil
}

func matchRelationship(current Relationship, expected *RelationshipExpected) bool {
	return expected != nil && current.Value == expected.Value && current.UpdatedSequence == expected.UpdatedSequence && current.Removed == expected.Removed
}

func matchLocation(current Location, expected *LocationExpected) bool {
	return expected != nil && current.Value == expected.Value && current.UpdatedSequence == expected.UpdatedSequence && current.Removed == expected.Removed
}

func (s *Store) putRelationship(ctx context.Context, tx *sql.Tx, c PutRelationship, sequence int64) error {
	current, err := readRelationship(ctx, tx, s.library, c.ID)
	if c.Expected == nil {
		if err == nil {
			return ErrConflict
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
	} else {
		if errors.Is(err, ErrNotFound) {
			return ErrConflict
		}
		if err != nil {
			return err
		}
		if !matchRelationship(current, c.Expected) {
			return ErrConflict
		}
	}
	v := c.Value
	if err := requireAnchor(ctx, tx, s.library, v.SourceAssetID, v.SourceRevision); err != nil {
		return err
	}
	if err := requireAnchor(ctx, tx, s.library, v.TargetAssetID, v.TargetRevision); err != nil {
		return err
	}
	if c.Expected == nil {
		result, err := tx.ExecContext(ctx, `INSERT INTO relationships(relationship_id,library_id,source_asset_id,source_revision,target_asset_id,target_revision,predicate,provenance,source,created_sequence,updated_sequence,removed) VALUES(?,?,?,NULLIF(?,''),?,NULLIF(?,''),?,?,?,?,?,0) ON CONFLICT(relationship_id) DO NOTHING`,
			c.ID, s.library, v.SourceAssetID, v.SourceRevision, v.TargetAssetID, v.TargetRevision, v.Predicate, v.Provenance, v.Source, sequence, sequence)
		return changedOne(result, err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE relationships SET source_asset_id=?,source_revision=NULLIF(?,''),target_asset_id=?,target_revision=NULLIF(?,''),predicate=?,provenance=?,source=?,updated_sequence=?,removed=0 WHERE relationship_id=? AND library_id=? AND updated_sequence=? AND removed=?`,
		v.SourceAssetID, v.SourceRevision, v.TargetAssetID, v.TargetRevision, v.Predicate, v.Provenance, v.Source, sequence, c.ID, s.library, c.Expected.UpdatedSequence, c.Expected.Removed)
	return changedOne(result, err)
}

func (s *Store) removeRelationship(ctx context.Context, tx *sql.Tx, c RemoveRelationship, sequence int64) error {
	current, err := readRelationship(ctx, tx, s.library, c.ID)
	if errors.Is(err, ErrNotFound) {
		return ErrConflict
	}
	if err != nil {
		return err
	}
	if current.Removed || !matchRelationship(current, c.Expected) {
		return ErrConflict
	}
	result, err := tx.ExecContext(ctx, `UPDATE relationships SET removed=1,updated_sequence=? WHERE relationship_id=? AND library_id=? AND updated_sequence=? AND removed=?`,
		sequence, c.ID, s.library, c.Expected.UpdatedSequence, c.Expected.Removed)
	return changedOne(result, err)
}

func (s *Store) putLocation(ctx context.Context, tx *sql.Tx, c PutLocation, sequence int64) error {
	current, err := readLocation(ctx, tx, s.library, c.ID)
	if c.Expected == nil {
		if err == nil {
			return ErrConflict
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
	} else {
		if errors.Is(err, ErrNotFound) {
			return ErrConflict
		}
		if err != nil {
			return err
		}
		if !matchLocation(current, c.Expected) || current.Value.AssetID != c.Value.AssetID {
			return ErrConflict
		}
	}
	v := c.Value
	if err := requireAnchor(ctx, tx, s.library, v.AssetID, v.Revision); err != nil {
		return err
	}
	if c.Expected == nil {
		result, err := tx.ExecContext(ctx, `INSERT INTO locations(location_id,library_id,asset_id,revision,source,locator,created_sequence,updated_sequence,removed) VALUES(?,?,?,NULLIF(?,''),?,?,?,?,0) ON CONFLICT(location_id) DO NOTHING`,
			c.ID, s.library, v.AssetID, v.Revision, v.Source, v.Locator, sequence, sequence)
		return changedOne(result, err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE locations SET revision=NULLIF(?,''),source=?,locator=?,updated_sequence=?,removed=0 WHERE location_id=? AND library_id=? AND asset_id=? AND updated_sequence=? AND removed=?`,
		v.Revision, v.Source, v.Locator, sequence, c.ID, s.library, v.AssetID, c.Expected.UpdatedSequence, c.Expected.Removed)
	return changedOne(result, err)
}

func (s *Store) removeLocation(ctx context.Context, tx *sql.Tx, c RemoveLocation, sequence int64) error {
	current, err := readLocation(ctx, tx, s.library, c.ID)
	if errors.Is(err, ErrNotFound) {
		return ErrConflict
	}
	if err != nil {
		return err
	}
	if current.Removed || !matchLocation(current, c.Expected) {
		return ErrConflict
	}
	result, err := tx.ExecContext(ctx, `UPDATE locations SET removed=1,updated_sequence=? WHERE location_id=? AND library_id=? AND updated_sequence=? AND removed=?`,
		sequence, c.ID, s.library, c.Expected.UpdatedSequence, c.Expected.Removed)
	return changedOne(result, err)
}

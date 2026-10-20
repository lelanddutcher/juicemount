package catalog

import (
	"context"
	"database/sql"
	"errors"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// Membership checks are part of the same statement snapshot as mutable values.
const relationshipSelect = `SELECT r.library_id,r.relationship_id,r.source_asset_id,COALESCE(r.source_revision,''),r.target_asset_id,COALESCE(r.target_revision,''),r.predicate,r.provenance,r.source,r.created_sequence,r.updated_sequence,r.removed,
 (sa.asset_id IS NOT NULL AND sa.library_id=r.library_id AND ta.asset_id IS NOT NULL AND ta.library_id=r.library_id
 AND (r.source_revision IS NULL OR (sr.revision_id IS NOT NULL AND sr.asset_id=r.source_asset_id AND sr.library_id=r.library_id))
 AND (r.target_revision IS NULL OR (tr.revision_id IS NOT NULL AND tr.asset_id=r.target_asset_id AND tr.library_id=r.library_id)))
 FROM relationships r LEFT JOIN assets sa ON sa.asset_id=r.source_asset_id LEFT JOIN assets ta ON ta.asset_id=r.target_asset_id
 LEFT JOIN revisions sr ON sr.revision_id=r.source_revision LEFT JOIN revisions tr ON tr.revision_id=r.target_revision`

const locationSelect = `SELECT l.library_id,l.location_id,l.asset_id,COALESCE(l.revision,''),l.source,l.locator,l.created_sequence,l.updated_sequence,l.removed,
 (a.asset_id IS NOT NULL AND a.library_id=l.library_id AND (l.revision IS NULL OR (r.revision_id IS NOT NULL AND r.asset_id=l.asset_id AND r.library_id=l.library_id)))
 FROM locations l LEFT JOIN assets a ON a.asset_id=l.asset_id LEFT JOIN revisions r ON r.revision_id=l.revision`

func scanRelationship(row interface{ Scan(...any) error }, library continuity.LibraryID) (Relationship, error) {
	var r Relationship
	var removed, membership int
	err := row.Scan(&r.LibraryID, &r.ID, &r.Value.SourceAssetID, &r.Value.SourceRevision, &r.Value.TargetAssetID, &r.Value.TargetRevision,
		&r.Value.Predicate, &r.Value.Provenance, &r.Value.Source, &r.CreatedSequence, &r.UpdatedSequence, &removed, &membership)
	if errors.Is(err, sql.ErrNoRows) {
		return Relationship{}, ErrNotFound
	}
	if err != nil {
		return Relationship{}, formatError(err)
	}
	if r.LibraryID != library || !validOperationID(r.ID) || !r.Value.valid() || r.CreatedSequence <= 0 || r.UpdatedSequence < r.CreatedSequence || (removed != 0 && removed != 1) || membership != 1 {
		return Relationship{}, corrupt("relationship value, chronology or membership")
	}
	r.Removed = removed == 1
	return r, nil
}

func scanLocation(row interface{ Scan(...any) error }, library continuity.LibraryID) (Location, error) {
	var l Location
	var removed, membership int
	err := row.Scan(&l.LibraryID, &l.ID, &l.Value.AssetID, &l.Value.Revision, &l.Value.Source, &l.Value.Locator,
		&l.CreatedSequence, &l.UpdatedSequence, &removed, &membership)
	if errors.Is(err, sql.ErrNoRows) {
		return Location{}, ErrNotFound
	}
	if err != nil {
		return Location{}, formatError(err)
	}
	if l.LibraryID != library || !validOperationID(l.ID) || !l.Value.valid() || l.CreatedSequence <= 0 || l.UpdatedSequence < l.CreatedSequence || (removed != 0 && removed != 1) || membership != 1 {
		return Location{}, corrupt("location value, chronology or membership")
	}
	l.Removed = removed == 1
	return l, nil
}

func readRelationship(ctx context.Context, q queryer, library continuity.LibraryID, id string) (Relationship, error) {
	return scanRelationship(q.QueryRowContext(ctx, relationshipSelect+` WHERE r.relationship_id=?`, id), library)
}

func readLocation(ctx context.Context, q queryer, library continuity.LibraryID, id string) (Location, error) {
	return scanLocation(q.QueryRowContext(ctx, locationSelect+` WHERE l.location_id=?`, id), library)
}

// GetRelationship includes tombstones. Missing identities return ErrNotFound.
func (s *Store) GetRelationship(ctx context.Context, library continuity.LibraryID, id string) (Relationship, error) {
	if err := s.enter(); err != nil {
		return Relationship{}, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return Relationship{}, err
	}
	if !validOperationID(id) {
		return Relationship{}, invalid("relationship identity")
	}
	return readRelationship(ctx, s.db, library, id)
}

// GetLocation includes tombstones; a locator is never opened or resolved.
func (s *Store) GetLocation(ctx context.Context, library continuity.LibraryID, id string) (Location, error) {
	if err := s.enter(); err != nil {
		return Location{}, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return Location{}, err
	}
	if !validOperationID(id) {
		return Location{}, invalid("location identity")
	}
	return readLocation(ctx, s.db, library, id)
}

// ListRelationships includes active inbound/outbound edges, with self edges once.
func (s *Store) ListRelationships(ctx context.Context, library continuity.LibraryID, asset continuity.AssetID, afterID string, limit int) ([]Relationship, error) {
	if err := s.enter(); err != nil {
		return nil, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return nil, err
	}
	if asset.Validate() != nil || !validPage(limit) || (afterID != "" && !validOperationID(afterID)) {
		return nil, invalid("relationship page")
	}
	if _, err := readAsset(ctx, s.db, library, asset); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, relationshipSelect+` WHERE r.removed=0 AND (r.source_asset_id=? OR r.target_asset_id=?) AND r.relationship_id>? ORDER BY r.relationship_id LIMIT ?`, asset, asset, afterID, limit)
	if err != nil {
		return nil, formatError(err)
	}
	defer rows.Close()
	result := make([]Relationship, 0, limit)
	for rows.Next() {
		r, err := scanRelationship(rows, library)
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		return nil, formatError(err)
	}
	return result, nil
}

func (s *Store) ListLocations(ctx context.Context, library continuity.LibraryID, asset continuity.AssetID, afterID string, limit int) ([]Location, error) {
	if err := s.enter(); err != nil {
		return nil, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return nil, err
	}
	if asset.Validate() != nil || !validPage(limit) || (afterID != "" && !validOperationID(afterID)) {
		return nil, invalid("location page")
	}
	if _, err := readAsset(ctx, s.db, library, asset); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, locationSelect+` WHERE l.removed=0 AND l.asset_id=? AND l.location_id>? ORDER BY l.location_id LIMIT ?`, asset, afterID, limit)
	if err != nil {
		return nil, formatError(err)
	}
	defer rows.Close()
	result := make([]Location, 0, limit)
	for rows.Next() {
		l, err := scanLocation(rows, library)
		if err != nil {
			return nil, err
		}
		result = append(result, l)
	}
	if err := rows.Err(); err != nil {
		return nil, formatError(err)
	}
	return result, nil
}

package catalog

import (
	"context"
	"database/sql"
	"errors"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const collectionSelect = `SELECT c.library_id,c.collection_id,c.name,c.created_sequence,c.updated_sequence,c.removed,
 (SELECT count(*) FROM collection_members m WHERE m.collection_id=c.collection_id AND m.removed=0) FROM collections c`

func scanCollection(row interface{ Scan(...any) error }, library continuity.LibraryID) (Collection, error) {
	var c Collection
	var removed int
	err := row.Scan(&c.LibraryID, &c.ID, &c.Name, &c.CreatedSequence, &c.UpdatedSequence, &removed, &c.Members)
	if errors.Is(err, sql.ErrNoRows) {
		return Collection{}, ErrNotFound
	}
	if err != nil {
		return Collection{}, formatError(err)
	}
	if c.LibraryID != library || c.ID.Validate() != nil || !validCollectionName(c.Name) || c.CreatedSequence <= 0 || c.UpdatedSequence < c.CreatedSequence || (removed != 0 && removed != 1) || c.Members < 0 {
		return Collection{}, corrupt("collection fields")
	}
	c.Removed = removed == 1
	return c, nil
}

// Membership checks share the statement snapshot, as for locations.
const memberSelect = `SELECT m.collection_id,m.asset_id,m.created_sequence,m.updated_sequence,m.removed,
 (c.collection_id IS NOT NULL AND c.library_id=? AND a.asset_id IS NOT NULL AND a.library_id=c.library_id AND m.created_sequence>=c.created_sequence AND m.created_sequence>=a.created_sequence)
 FROM collection_members m LEFT JOIN collections c ON c.collection_id=m.collection_id LEFT JOIN assets a ON a.asset_id=m.asset_id`

func scanMember(row interface{ Scan(...any) error }) (CollectionMember, error) {
	var m CollectionMember
	var removed, membership int
	err := row.Scan(&m.CollectionID, &m.AssetID, &m.CreatedSequence, &m.UpdatedSequence, &removed, &membership)
	if errors.Is(err, sql.ErrNoRows) {
		return CollectionMember{}, ErrNotFound
	}
	if err != nil {
		return CollectionMember{}, formatError(err)
	}
	if m.CollectionID.Validate() != nil || m.AssetID.Validate() != nil || m.CreatedSequence <= 0 || m.UpdatedSequence < m.CreatedSequence || (removed != 0 && removed != 1) || membership != 1 {
		return CollectionMember{}, corrupt("collection member fields, chronology or membership")
	}
	m.Removed = removed == 1
	return m, nil
}

// GetCollection includes tombstones. Missing identities return ErrNotFound.
func (s *Store) GetCollection(ctx context.Context, library continuity.LibraryID, id CollectionID) (Collection, error) {
	if err := s.enter(); err != nil {
		return Collection{}, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return Collection{}, err
	}
	if err := id.Validate(); err != nil {
		return Collection{}, err
	}
	return scanCollection(s.db.QueryRowContext(ctx, collectionSelect+` WHERE c.collection_id=?`, id), library)
}

// ListCollections returns active collections after an exclusive ID cursor
// ("" starts at the beginning), ordered by ID.
func (s *Store) ListCollections(ctx context.Context, library continuity.LibraryID, after CollectionID, limit int) ([]Collection, error) {
	if err := s.enter(); err != nil {
		return nil, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return nil, err
	}
	if !validPage(limit) || (after != "" && after.Validate() != nil) {
		return nil, invalid("collection page")
	}
	rows, err := s.db.QueryContext(ctx, collectionSelect+` WHERE c.removed=0 AND c.collection_id>? ORDER BY c.collection_id LIMIT ?`, after, limit)
	if err != nil {
		return nil, formatError(err)
	}
	defer rows.Close()
	result := make([]Collection, 0, limit)
	for rows.Next() {
		c, err := scanCollection(rows, library)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	if err := rows.Err(); err != nil {
		return nil, formatError(err)
	}
	return result, nil
}

// ListCollectionMembers returns the active members of an existing (possibly
// removed) collection after an exclusive asset-ID cursor, ordered by asset ID.
func (s *Store) ListCollectionMembers(ctx context.Context, library continuity.LibraryID, id CollectionID, after continuity.AssetID, limit int) ([]CollectionMember, error) {
	if err := s.enter(); err != nil {
		return nil, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return nil, err
	}
	if id.Validate() != nil || !validPage(limit) || (after != "" && after.Validate() != nil) {
		return nil, invalid("collection member page")
	}
	if _, err := scanCollection(s.db.QueryRowContext(ctx, collectionSelect+` WHERE c.collection_id=?`, id), library); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, memberSelect+` WHERE m.collection_id=? AND m.removed=0 AND m.asset_id>? ORDER BY m.asset_id LIMIT ?`, library, id, after, limit)
	if err != nil {
		return nil, formatError(err)
	}
	defer rows.Close()
	result := make([]CollectionMember, 0, limit)
	for rows.Next() {
		m, err := scanMember(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	if err := rows.Err(); err != nil {
		return nil, formatError(err)
	}
	return result, nil
}

// GetCollectionMember includes tombstones; a never-added pair is ErrNotFound.
func (s *Store) GetCollectionMember(ctx context.Context, library continuity.LibraryID, id CollectionID, asset continuity.AssetID) (CollectionMember, error) {
	if err := s.enter(); err != nil {
		return CollectionMember{}, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return CollectionMember{}, err
	}
	if id.Validate() != nil || asset.Validate() != nil {
		return CollectionMember{}, invalid("collection member")
	}
	return scanMember(s.db.QueryRowContext(ctx, memberSelect+` WHERE m.collection_id=? AND m.asset_id=?`, library, id, asset))
}

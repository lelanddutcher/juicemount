package catalog

import (
	"context"
	"database/sql"
	"errors"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const querySelect = `SELECT q.library_id,q.query_id,q.name,q.definition,q.created_sequence,q.updated_sequence,q.removed FROM queries q`

func scanQuery(row interface{ Scan(...any) error }, library continuity.LibraryID) (Query, error) {
	var q Query
	var def string
	var removed int
	err := row.Scan(&q.LibraryID, &q.ID, &q.Name, &def, &q.CreatedSequence, &q.UpdatedSequence, &removed)
	if errors.Is(err, sql.ErrNoRows) {
		return Query{}, ErrNotFound
	}
	if err != nil {
		return Query{}, formatError(err)
	}
	if q.LibraryID != library || q.ID.Validate() != nil || !validCollectionName(q.Name) || q.CreatedSequence <= 0 || q.UpdatedSequence < q.CreatedSequence || (removed != 0 && removed != 1) {
		return Query{}, corrupt("query fields")
	}
	if q.Definition, err = decodeDefinition([]byte(def)); err != nil {
		return Query{}, err
	}
	q.Removed = removed == 1
	return q, nil
}

// GetQuery includes tombstones. Missing identities return ErrNotFound.
func (s *Store) GetQuery(ctx context.Context, library continuity.LibraryID, id QueryID) (Query, error) {
	if err := s.enter(); err != nil {
		return Query{}, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return Query{}, err
	}
	if err := id.Validate(); err != nil {
		return Query{}, err
	}
	if s.format.Load() < 4 {
		return Query{}, ErrNotFound // format 3: no query yet
	}
	return scanQuery(s.db.QueryRowContext(ctx, querySelect+` WHERE q.query_id=?`, id), library)
}

// ListQueries returns active queries after an exclusive ID cursor ("" starts
// at the beginning), ordered by ID.
func (s *Store) ListQueries(ctx context.Context, library continuity.LibraryID, after QueryID, limit int) ([]Query, error) {
	if err := s.enter(); err != nil {
		return nil, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return nil, err
	}
	if !validPage(limit) || (after != "" && after.Validate() != nil) {
		return nil, invalid("query page")
	}
	if s.format.Load() < 4 {
		return []Query{}, nil
	}
	rows, err := s.db.QueryContext(ctx, querySelect+` WHERE q.removed=0 AND q.query_id>? ORDER BY q.query_id LIMIT ?`, after, limit)
	if err != nil {
		return nil, formatError(err)
	}
	defer rows.Close()
	result := make([]Query, 0, limit)
	for rows.Next() {
		q, err := scanQuery(rows, library)
		if err != nil {
			return nil, err
		}
		result = append(result, q)
	}
	if err := rows.Err(); err != nil {
		return nil, formatError(err)
	}
	return result, nil
}

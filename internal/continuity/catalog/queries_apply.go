package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// activeQuery reads a query for a change: missing and removed queries are
// both ErrNotFound.
func activeQuery(ctx context.Context, tx *sql.Tx, library string, id QueryID) error {
	var owner string
	var removed int
	err := tx.QueryRowContext(ctx, `SELECT library_id,removed FROM queries WHERE query_id=?`, id).Scan(&owner, &removed)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: query", ErrNotFound)
	}
	if err != nil {
		return formatError(err)
	}
	if owner != library {
		return corrupt("query library")
	}
	if removed != 0 {
		return fmt.Errorf("%w: query removed", ErrNotFound)
	}
	return nil
}

func (s *Store) applyQueryChange(ctx context.Context, tx *sql.Tx, c Change, sequence int64) error {
	library := string(s.library)
	switch {
	case c.CreateQuery != nil:
		v := c.CreateQuery
		def, err := encodeDefinition(v.Definition)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `INSERT INTO queries(query_id,library_id,name,definition,created_sequence,updated_sequence,removed) VALUES(?,?,?,?,?,?,0) ON CONFLICT(query_id) DO NOTHING`,
			v.QueryID, library, v.Name, string(def), sequence, sequence)
		return changedOne(result, err)
	case c.RenameQuery != nil:
		v := c.RenameQuery
		if err := activeQuery(ctx, tx, library, v.QueryID); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE queries SET name=?,updated_sequence=? WHERE query_id=? AND library_id=? AND removed=0`,
			v.Name, sequence, v.QueryID, library)
		return changedOne(result, err)
	case c.RemoveQuery != nil:
		v := c.RemoveQuery
		if err := activeQuery(ctx, tx, library, v.QueryID); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE queries SET removed=1,updated_sequence=? WHERE query_id=? AND library_id=? AND removed=0`,
			sequence, v.QueryID, library)
		return changedOne(result, err)
	default:
		return invalid("unknown change")
	}
}

func isQueryChange(c Change) bool {
	return c.CreateQuery != nil || c.RenameQuery != nil || c.RemoveQuery != nil
}

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

// checkQueryLimits runs once per operation that creates a query: at most
// MaxQueries active queries per library.
func checkQueryLimits(ctx context.Context, tx *sql.Tx, op Operation) error {
	creates := false
	for _, c := range op.Changes {
		creates = creates || c.CreateQuery != nil
	}
	if !creates {
		return nil
	}
	var n int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT 1 FROM queries WHERE removed=0 LIMIT ?)`, MaxQueries+1).Scan(&n); err != nil {
		return formatError(err)
	}
	if n > MaxQueries {
		return fmt.Errorf("%w: a library holds at most %d queries", ErrQueryLimit, MaxQueries)
	}
	return nil
}

// EnableQueries adds the format 4 query table to an open format 3 catalog
// in place, in one immediate transaction checked as format 4 before it
// commits; nothing existing changes. A format 4 catalog is left as it is.
// After it, a binary from before format 4 refuses the catalog (newer
// format), never damaging it.
func (s *Store) EnableQueries(ctx context.Context) (resultErr error) {
	if err := s.enter(); err != nil {
		return err
	}
	defer s.leave()
	if s.format.Load() >= 4 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	library, version, err := validateSchemaVersion(ctx, tx, false)
	if err != nil {
		return err
	}
	if library != s.library {
		return corrupt("library identity changed")
	}
	if version < 4 {
		if err := checkSettings(ctx, tx); err != nil {
			return err
		}
		if err := migrateV3(ctx, tx); err != nil {
			return err
		}
		actual, current, err := recognizeSchema(ctx, tx, false)
		if err != nil {
			return err
		}
		if actual != s.library || current != 4 {
			return corrupt("migration identity mismatch")
		}
		if err := checkIntegrity(ctx, tx); err != nil {
			return err
		}
	}
	if err := s.checkPaths(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog upgrade commit outcome uncertain: %w", err)
	}
	if err := syncParent(s.path); err != nil {
		return fmt.Errorf("catalog upgrade directory barrier uncertain: %w", err)
	}
	s.format.Store(4)
	return nil
}

// Format is the catalog's format as this Store last saw it.
func (s *Store) Format() int { return int(s.format.Load()) }

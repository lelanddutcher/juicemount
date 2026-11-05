package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
)

// activeCollection reads a collection for a change: missing and removed
// collections are both ErrNotFound.
func activeCollection(ctx context.Context, tx *sql.Tx, library string, id CollectionID) error {
	var owner string
	var removed int
	err := tx.QueryRowContext(ctx, `SELECT library_id,removed FROM collections WHERE collection_id=?`, id).Scan(&owner, &removed)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: collection", ErrNotFound)
	}
	if err != nil {
		return formatError(err)
	}
	if owner != library {
		return corrupt("collection library")
	}
	if removed != 0 {
		return fmt.Errorf("%w: collection removed", ErrNotFound)
	}
	return nil
}

func (s *Store) applyCollectionChange(ctx context.Context, tx *sql.Tx, c Change, sequence int64) error {
	library := string(s.library)
	switch {
	case c.ExpectCollection != nil:
		v := c.ExpectCollection
		if err := activeCollection(ctx, tx, library, v.CollectionID); err != nil {
			return err
		}
		var current int64
		if err := tx.QueryRowContext(ctx, `SELECT updated_sequence FROM collections WHERE collection_id=?`, v.CollectionID).Scan(&current); err != nil {
			return formatError(err)
		}
		if current != v.UpdatedSequence {
			return fmt.Errorf("%w: collection changed at sequence %d", ErrConflict, current)
		}
		return nil
	case c.CreateCollection != nil:
		v := c.CreateCollection
		result, err := tx.ExecContext(ctx, `INSERT INTO collections(collection_id,library_id,name,created_sequence,updated_sequence,removed) VALUES(?,?,?,?,?,0) ON CONFLICT(collection_id) DO NOTHING`,
			v.CollectionID, library, v.Name, sequence, sequence)
		return changedOne(result, err)
	case c.RenameCollection != nil:
		v := c.RenameCollection
		if err := activeCollection(ctx, tx, library, v.CollectionID); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE collections SET name=?,updated_sequence=? WHERE collection_id=? AND library_id=? AND removed=0`,
			v.Name, sequence, v.CollectionID, library)
		return changedOne(result, err)
	case c.RemoveCollection != nil:
		v := c.RemoveCollection
		if err := activeCollection(ctx, tx, library, v.CollectionID); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE collections SET removed=1,updated_sequence=? WHERE collection_id=? AND library_id=? AND removed=0`,
			sequence, v.CollectionID, library)
		return changedOne(result, err)
	case c.AddCollectionMember != nil:
		v := c.AddCollectionMember
		if err := activeCollection(ctx, tx, library, v.CollectionID); err != nil {
			return err
		}
		if _, err := readAsset(ctx, tx, s.library, v.AssetID); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `INSERT INTO collection_members(collection_id,asset_id,created_sequence,updated_sequence,removed) VALUES(?,?,?,?,0)
 ON CONFLICT(collection_id,asset_id) DO UPDATE SET removed=0,updated_sequence=excluded.updated_sequence WHERE removed=1`,
			v.CollectionID, v.AssetID, sequence, sequence)
		return touchCollection(ctx, tx, v.CollectionID, sequence, result, err)
	case c.RemoveCollectionMember != nil:
		v := c.RemoveCollectionMember
		if err := activeCollection(ctx, tx, library, v.CollectionID); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE collection_members SET removed=1,updated_sequence=? WHERE collection_id=? AND asset_id=? AND removed=0`,
			sequence, v.CollectionID, v.AssetID)
		return touchCollection(ctx, tx, v.CollectionID, sequence, result, err)
	default:
		return invalid("unknown change")
	}
}

// touchCollection moves the collection's UpdatedSequence when a member row
// changed, so a membership change is visible to ExpectCollection.
func touchCollection(ctx context.Context, tx *sql.Tx, id CollectionID, sequence int64, result sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil || n == 0 {
		return err
	}
	result, err = tx.ExecContext(ctx, `UPDATE collections SET updated_sequence=? WHERE collection_id=? AND removed=0`, sequence, id)
	return changedOne(result, err)
}

// checkCollectionLimits runs once per operation, after every change, on the
// collections that gained members.
func checkCollectionLimits(ctx context.Context, tx *sql.Tx, op Operation) error {
	grown := map[CollectionID]bool{}
	for _, c := range op.Changes {
		if c.AddCollectionMember != nil {
			grown[c.AddCollectionMember.CollectionID] = true
		}
	}
	ids := make([]string, 0, len(grown))
	for id := range grown {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	for _, id := range ids {
		var n int64
		// LIMIT bounds the scan: only "more than the limit" matters.
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT 1 FROM collection_members WHERE collection_id=? AND removed=0 LIMIT ?)`, id, MaxCollectionMembers+1).Scan(&n); err != nil {
			return formatError(err)
		}
		if n > MaxCollectionMembers {
			return fmt.Errorf("%w: %s holds at most %d members", ErrCollectionFull, id, MaxCollectionMembers)
		}
	}
	return nil
}

func isCollectionChange(c Change) bool {
	return c.ExpectCollection != nil || c.CreateCollection != nil || c.RenameCollection != nil || c.RemoveCollection != nil ||
		c.AddCollectionMember != nil || c.RemoveCollectionMember != nil
}

package metadata

import (
	"database/sql"
	"encoding/json"
	"fmt"
)

// scanProtectedReplacementPaths reads accepted publication ownership in the
// same SQLite transaction that applies a remote SCAN or keyspace observation.
// The writer lock held by callers serializes this check with acceptance and
// settlement. A queued save owns both its new destination entry and the
// captured ancestor chain until local cleanup finishes.
func scanProtectedReplacementPaths(tx *sql.Tx) (map[string]struct{}, error) {
	protected := make(map[string]struct{})
	var schemas int
	if err := tx.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table'
 AND name IN ('spool_entries','spool_replacement_publications')`).Scan(&schemas); err != nil {
		return nil, err
	}
	if schemas == 0 {
		return protected, nil // plain mirror stores have no spool
	}
	if schemas != 2 {
		return nil, ErrMirrorConflict
	}
	rows, err := tx.Query(`SELECT id,nfs_path,replacement_witness FROM spool_entries
 WHERE replacement_publication_required=1`)
	if err != nil {
		return nil, err
	}
	type accepted struct {
		id          int64
		destination string
		raw         []byte
	}
	var acceptedRows []accepted
	for rows.Next() {
		var row accepted
		if err := rows.Scan(&row.id, &row.destination, &row.raw); err != nil {
			rows.Close()
			return nil, err
		}
		acceptedRows = append(acceptedRows, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for _, row := range acceptedRows {
		// The accepted row leads the query. A missing journal or a terminal
		// phase whose payload disagrees with its row must fail closed rather
		// than silently exempting the path.
		pub, _, _, err := readReplacementPublication(tx, row.id)
		if err != nil || pub == nil {
			return nil, fmt.Errorf("accepted replacement scan journal: %w", ErrMirrorConflict)
		}
		if pub.Phase == ReplacementPublicationCleaned || pub.Phase == ReplacementPublicationSuperseded {
			continue
		}
		var w SpoolReplacementWitness
		if err := json.Unmarshal(row.raw, &w); err != nil || w.validate() != nil || row.destination != w.Destination.Path {
			return nil, fmt.Errorf("accepted replacement scan guard: %w", ErrMirrorConflict)
		}
		protected[w.Source.Path] = struct{}{}
		protected[w.Destination.Path] = struct{}{}
		for _, parent := range w.SourceParents {
			protected[parent.Token.Path] = struct{}{}
		}
		for _, node := range w.Destination.Chain {
			protected[node.Path] = struct{}{}
		}
	}
	return protected, nil
}

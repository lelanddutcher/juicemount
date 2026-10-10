package metadata

import (
	"database/sql"
)

// replacementObservation is a read-only ownership snapshot. Its caller holds
// replacementObservationMu until the whole subscription event has completed;
// acceptance and settlement cannot change ownership between this read and the
// event's callbacks or Store mutations. No SQLite transaction remains open
// while those callbacks execute.
type replacementObservation struct {
	paths  map[string]struct{}
	inodes map[uint64]struct{}
}

func (s *Store) replacementObservationSnapshot() (replacementObservation, error) {
	out := replacementObservation{paths: make(map[string]struct{}), inodes: make(map[uint64]struct{})}
	tx, err := s.db.Begin()
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	var schemas int
	if err := tx.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table'
 AND name IN ('spool_entries','spool_replacement_publications')`).Scan(&schemas); err != nil {
		return out, err
	}
	if schemas == 1 {
		return out, ErrMirrorConflict
	}
	if schemas == 2 {
		rows, err := tx.Query(`SELECT id FROM spool_entries
 WHERE replacement_publication_required=1 OR replacement_witness IS NOT NULL`)
		if err != nil {
			return out, err
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return out, err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return out, err
		}
		if err := rows.Close(); err != nil {
			return out, err
		}
		for _, id := range ids {
			publication, _, row, err := readReplacementPublication(tx, id)
			if err != nil || (row.required == 1 && publication == nil) {
				return out, ErrMirrorConflict
			}
			w, err := replacementWitnessByID(tx, id)
			if err != nil || w == nil {
				return out, ErrMirrorConflict
			}
			if publication != nil && (publication.Phase == ReplacementPublicationCleaned || publication.Phase == ReplacementPublicationSuperseded) {
				continue
			}
			// A non-NULL legacy witness without a journal remains owned. Its
			// absence of a receipt is never evidence that remote events may edit it.
			out.paths[w.Source.Path] = struct{}{}
			out.paths[w.Destination.Path] = struct{}{}
			for _, parent := range w.SourceParents {
				out.paths[parent.Token.Path] = struct{}{}
				out.inodes[parent.Inode] = struct{}{}
			}
			for _, node := range w.Destination.Chain {
				out.paths[node.Path] = struct{}{}
				out.inodes[node.Inode] = struct{}{}
			}
		}
	}
	if err := s.addCompletionObservation(tx, &out); err != nil {
		return out, err
	}
	if err := s.addObservationAliases(tx, &out); err != nil {
		return out, err
	}
	// Close the snapshot before any callback, RAM publication or Store writer.
	if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
		return out, err
	}
	return out, nil
}

func (o replacementObservation) protectsPath(p string) bool {
	_, ok := o.paths[p]
	return ok
}

func (s *Store) subscriptionEventProtected(evt MetadataEvent) (protected, active bool, err error) {
	o, err := s.replacementObservationSnapshot()
	if err != nil {
		return true, true, err
	}
	if len(o.paths) == 0 {
		return false, false, nil
	}
	for _, p := range []string{evt.Path, evt.OldPath} {
		if p != "" && !validCustodyPath(p) {
			return true, true, nil
		}
	}
	if o.protectsPath(evt.Path) || o.protectsPath(evt.OldPath) {
		return true, true, nil
	}
	if _, ok := o.inodes[evt.Inode]; ok && evt.Inode != 0 {
		return true, true, nil
	}
	// Directory callbacks invalidate every cached slice mapping, even for an
	// otherwise unrelated directory. A delete/rename may omit the type on wire.
	if evt.IsDir {
		return true, true, nil
	}
	for _, p := range []string{evt.Path, evt.OldPath} {
		if p != "" {
			e := s.LookupByPath(p) // RAM or durable fallback after eviction
			if e == nil {
				continue
			}
			if e.IsDir {
				return true, true, nil
			}
			if _, ok := o.inodes[e.Inode]; ok {
				return true, true, nil
			}
		}
	}
	return false, true, nil
}

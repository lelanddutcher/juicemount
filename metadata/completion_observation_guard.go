package metadata

import (
	"database/sql"
	"path"
)

// Legacy completion holds own their mirror generation from the first FULL
// commit, including uncertainty with no published revision yet. Only exact
// CleanupDrainCompletion deletion releases protection. Bound publication has
// separate unfinished retirement semantics and is outside this guard.
func (s *Store) addCompletionObservation(tx *sql.Tx, out *replacementObservation) error {
	var schemas int
	if err := tx.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table'
 AND name IN ('spool_entries','spool_completion_holds')`).Scan(&schemas); err != nil {
		return err
	}
	if schemas == 0 {
		return nil
	}
	if schemas != 2 {
		return ErrMirrorConflict
	}
	rows, err := tx.Query(`SELECT h.completion_id,h.spool_id,h.nfs_path,h.spool_file,h.size,h.kind,h.original_inode,
 r.id,r.nfs_path,r.spool_file,r.size,r.drain_state,r.retired_by
 FROM spool_completion_holds h LEFT JOIN spool_entries r ON r.id=h.spool_id
 WHERE h.kind <> 'bound'`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, name, file, kind string
		var spoolID, size, inode int64
		var rowID, rowSize, retired sql.NullInt64
		var rowPath, rowFile, state sql.NullString
		if err := rows.Scan(&id, &spoolID, &name, &file, &size, &kind, &inode,
			&rowID, &rowPath, &rowFile, &rowSize, &state, &retired); err != nil {
			return err
		}
		if !validCustodyID(id) || spoolID <= 0 || !validCustodyPath(name) || !validPrivatePath(file) || size < 0 ||
			kind != "legacy" || !rowID.Valid || rowID.Int64 != spoolID ||
			!rowPath.Valid || rowPath.String != name || !rowFile.Valid || rowFile.String != file ||
			!rowSize.Valid || rowSize.Int64 != size || !state.Valid || (state.String != "draining" && state.String != "done") ||
			!retired.Valid || retired.Int64 != 0 {
			return ErrMirrorConflict
		}
		for p := name; ; p = path.Dir(p) {
			out.paths[p] = struct{}{}
			if p == "." {
				break
			}
		}
		if inode != 0 {
			out.inodes[uint64(inode)] = struct{}{}
		}
	}
	return rows.Err()
}

// Include durable and cached identities plus aliases before callbacks run.
// SQLite INTEGER preserves all uint64 bits, including synthetic high-bit IDs.
// The caller holds either writeMu (SCAN) or replacementObservationMu (events).
func (s *Store) addObservationAliases(tx *sql.Tx, out *replacementObservation) error {
	for p := range out.paths {
		var inode int64
		err := tx.QueryRow(`SELECT inode FROM entries WHERE path=?`, p).Scan(&inode)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if err == nil {
			if inode == 0 {
				return ErrMirrorConflict
			}
			out.inodes[uint64(inode)] = struct{}{}
		}
	}
	s.mu.RLock()
	for p := range out.paths {
		if e := s.pathCache[p]; e != nil {
			out.inodes[e.Inode] = struct{}{}
		}
	}
	for inode := range out.inodes {
		if e := s.inodeCache[inode]; e != nil {
			out.paths[e.Path] = struct{}{}
		}
	}
	s.mu.RUnlock()
	for inode := range out.inodes {
		rows, err := tx.Query(`SELECT path FROM entries WHERE inode=?`, int64(inode))
		if err != nil {
			return err
		}
		for rows.Next() {
			var p string
			if err := rows.Scan(&p); err != nil {
				rows.Close()
				return err
			}
			out.paths[p] = struct{}{}
		}
		err = rows.Err()
		closeErr := rows.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func (s *Store) scanProtectedObservation(tx *sql.Tx) (replacementObservation, error) {
	paths, err := scanProtectedReplacementPaths(tx)
	out := replacementObservation{paths: paths, inodes: make(map[uint64]struct{})}
	if err != nil {
		return out, err
	}
	if err := s.addCompletionObservation(tx, &out); err != nil {
		return out, err
	}
	if err := s.addObservationAliases(tx, &out); err != nil {
		return out, err
	}
	return out, nil
}

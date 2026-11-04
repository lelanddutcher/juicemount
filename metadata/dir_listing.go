package metadata

import "fmt"

// dirListingSchema persists the NFS handler's record of directories whose
// mirror rows may be only part of the folder (nfs/dirlisting.go). Without it
// a restart forgot the record and served such a folder's rows as the whole
// folder. It holds membership only; retry and backoff state stay in memory.
//
// Additive (CREATE TABLE IF NOT EXISTS), like prune_ladder, so it lands on
// existing mirror DBs at open and an older binary ignores it. It lives in the
// same SQLite DB as entries, so the "Reset local metadata cache" wipe drops it
// with the rows it describes.
const dirListingSchema = `
CREATE TABLE IF NOT EXISTS dir_listing_incomplete (
    path TEXT PRIMARY KEY
);
`

// dirListingTreeWhere matches a directory and every path below it. The range
// is the byte prefix dir+"/" ('0' is the byte after '/'), so it needs no
// GLOB/LIKE escaping and uses the primary key.
const dirListingTreeWhere = `(path = ?1 OR (path >= ?1 || '/' AND path < ?1 || '0'))`

// MarkDirListingIncomplete durably records dir.
func (s *Store) MarkDirListingIncomplete(dir string) error {
	s.writeMu.Lock()
	_, err := s.db.Exec(`INSERT OR IGNORE INTO dir_listing_incomplete (path) VALUES (?)`, dir)
	s.writeMu.Unlock()
	if err != nil {
		return fmt.Errorf("mark dir listing incomplete %q: %w", dir, err)
	}
	return nil
}

// ClearDirListingIncomplete drops the record of dir alone and reports
// whether there was one.
func (s *Store) ClearDirListingIncomplete(dir string) (bool, error) {
	s.writeMu.Lock()
	res, err := s.db.Exec(`DELETE FROM dir_listing_incomplete WHERE path = ?`, dir)
	s.writeMu.Unlock()
	if err != nil {
		return false, fmt.Errorf("clear dir listing incomplete %q: %w", dir, err)
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// CountDirListingIncomplete returns the number of durable records.
func (s *Store) CountDirListingIncomplete() (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM dir_listing_incomplete`).Scan(&n)
	return n, err
}

// ClearDirListingIncompleteTree drops the records of dir and its descendants
// (dir was deleted).
func (s *Store) ClearDirListingIncompleteTree(dir string) error {
	s.writeMu.Lock()
	_, err := s.db.Exec(`DELETE FROM dir_listing_incomplete WHERE `+dirListingTreeWhere, dir)
	s.writeMu.Unlock()
	if err != nil {
		return fmt.Errorf("clear dir listing incomplete tree %q: %w", dir, err)
	}
	return nil
}

// RenameDirListingIncomplete moves the records of oldDir and its descendants
// under newDir, dropping any records of the newDir tree it replaces, in one
// transaction.
func (s *Store) RenameDirListingIncomplete(oldDir, newDir string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("rename dir listing incomplete begin: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM dir_listing_incomplete WHERE `+dirListingTreeWhere, newDir); err != nil {
		return fmt.Errorf("rename dir listing incomplete clear %q: %w", newDir, err)
	}
	// substr/length count characters on both sides, so a multi-byte name
	// is cut at the same place.
	if _, err := tx.Exec(`UPDATE dir_listing_incomplete SET path = ?2 || substr(path, length(?1) + 1)
		WHERE `+dirListingTreeWhere, oldDir, newDir); err != nil {
		return fmt.Errorf("rename dir listing incomplete %q -> %q: %w", oldDir, newDir, err)
	}
	return tx.Commit()
}

// HasDirListingIncomplete reports whether dir is durably recorded.
func (s *Store) HasDirListingIncomplete(dir string) (bool, error) {
	var present bool
	err := s.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM dir_listing_incomplete WHERE path = ?)`, dir).Scan(&present)
	return present, err
}

// LoadDirListingIncomplete returns the durable records, first dropping those
// whose directory has no mirror rows: a folder without rows is listed from
// the server on its next READDIR anyway, and this also sheds records of
// folders removed while the record was not maintained (another Mac's delete).
func (s *Store) LoadDirListingIncomplete() ([]string, error) {
	s.writeMu.Lock()
	_, err := s.db.Exec(`DELETE FROM dir_listing_incomplete WHERE NOT EXISTS (
		SELECT 1 FROM entries WHERE entries.parent_path = dir_listing_incomplete.path)`)
	s.writeMu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("prune dir listing incomplete: %w", err)
	}
	return s.ListDirListingIncomplete()
}

// ListDirListingIncomplete returns the durable records.
func (s *Store) ListDirListingIncomplete() ([]string, error) {
	rows, err := s.db.Query(`SELECT path FROM dir_listing_incomplete`)
	if err != nil {
		return nil, fmt.Errorf("load dir listing incomplete: %w", err)
	}
	defer rows.Close()
	var dirs []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, fmt.Errorf("load dir listing incomplete: %w", err)
		}
		dirs = append(dirs, p)
	}
	return dirs, rows.Err()
}

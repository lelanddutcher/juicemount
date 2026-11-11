package metadata

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// PendingDir is one folder that NFS MKDIR created at local speed and the
// drainer has not yet created on the volume (table pending_dirs).
type PendingDir struct {
	ID        int64
	Path      string // in-mount path, no leading slash
	Inode     uint64 // the number the mirror and the NFS file id already use
	CreatedAt time.Time
}

// PutPendingDir records a folder to create on the volume. A row for the same
// path is an error: MKDIR of an existing name never reaches here.
func (s *SpoolStore) PutPendingDir(dirPath string, inode uint64) (int64, error) {
	if dirPath == "" || dirPath == "." || strings.HasPrefix(dirPath, "/") || inode == 0 {
		return 0, fmt.Errorf("put pending folder %q: invalid path or inode", dirPath)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(`INSERT INTO pending_dirs (dir_path, inode, created_at) VALUES (?, ?, ?)`,
		dirPath, int64(inode), time.Now().Unix())
	if err != nil {
		return 0, fmt.Errorf("put pending folder %q: %w", dirPath, err)
	}
	return res.LastInsertId()
}

// ListPendingDirs returns every pending folder, oldest first. A folder's row
// is always older than the rows of folders created inside it, and a rename
// keeps each row's id, so this order creates parents before children.
func (s *SpoolStore) ListPendingDirs() ([]PendingDir, error) {
	rows, err := s.db.Query(`SELECT id, dir_path, inode, created_at FROM pending_dirs ORDER BY id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list pending folders: %w", err)
	}
	defer rows.Close()
	var out []PendingDir
	for rows.Next() {
		var p PendingDir
		var ino, created int64
		if err := rows.Scan(&p.ID, &p.Path, &ino, &created); err != nil {
			return nil, err
		}
		p.Inode, p.CreatedAt = uint64(ino), time.Unix(created, 0)
		out = append(out, p)
	}
	return out, rows.Err()
}

// DeletePendingDir removes the row of dirPath if it still has id. The id
// check keeps a drain that created an older generation of the name from
// deleting the row of a folder made again at the same path.
func (s *SpoolStore) DeletePendingDir(dirPath string, id int64) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(`DELETE FROM pending_dirs WHERE dir_path=? AND id=?`, dirPath, id)
	if err != nil {
		return false, fmt.Errorf("delete pending folder %q: %w", dirPath, err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// ErrPendingDirConflict reports a rename onto a path that already has a
// pending folder row.
var ErrPendingDirConflict = errors.New("pending folder already exists at the destination")

// MovePendingDirs renames the pending row of oldPath and of every pending
// folder beneath it to newPath, in one transaction. Ids are kept. Paths are
// compared as bytes in Go: SQLite's substr counts characters.
func (s *SpoolStore) MovePendingDirs(oldPath, newPath string) (int, error) {
	if oldPath == newPath {
		return 0, nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.Query(`SELECT id, dir_path FROM pending_dirs`)
	if err != nil {
		return 0, err
	}
	type move struct {
		id   int64
		path string
	}
	var moves []move
	for rows.Next() {
		var m move
		if err := rows.Scan(&m.id, &m.path); err != nil {
			rows.Close()
			return 0, err
		}
		if PathAtOrUnder(m.path, newPath) {
			rows.Close()
			return 0, ErrPendingDirConflict
		}
		if PathAtOrUnder(m.path, oldPath) {
			moves = append(moves, m)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	for _, m := range moves {
		if _, err := tx.Exec(`UPDATE pending_dirs SET dir_path=? WHERE id=?`,
			newPath+m.path[len(oldPath):], m.id); err != nil {
			return 0, fmt.Errorf("move pending folder %q -> %q: %w", oldPath, newPath, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(moves), nil
}

// PathAtOrUnder reports whether p is dir or a path beneath it.
func PathAtOrUnder(p, dir string) bool {
	return p == dir || strings.HasPrefix(p, dir+"/")
}

// CountPendingDirs is the cheap status counterpart of ListPendingDirs.
func (s *SpoolStore) CountPendingDirs() (int, error) {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM pending_dirs`).Scan(&n); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("count pending folders: %w", err)
	}
	return n, nil
}

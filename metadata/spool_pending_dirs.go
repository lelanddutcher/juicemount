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

// ListPendingDirs returns every pending folder in row id order. That order
// does NOT put parents first: a rename keeps ids, so a folder moved into a
// newer one is older than its new parent. Callers order by path
// (Drainer.materializePendingDirs works in waves of folders whose ancestors
// are not pending).
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
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("delete pending folder %q: %w", dirPath, err)
	}
	if n != 1 {
		// The durable row and the caller's view disagree: never succeed
		// silently, or a stale row would come back after a restart.
		return false, fmt.Errorf("delete pending folder %q (row %d): %w", dirPath, id, ErrPendingDirMissing)
	}
	return true, nil
}

// ErrPendingDirMissing reports that a pending folder row was not where the
// caller expected it.
var ErrPendingDirMissing = errors.New("pending folder row not found")

// ErrPendingDirConflict reports a rename onto a path that already has a
// pending folder row. This is refused even when that folder is empty: POSIX
// would replace it, but the destination's row, mirror entry and reserved
// inode would have to go in the same transaction. The NFS handler refuses
// such a rename earlier with JUKEBOX and wakes the drainer, so the retry
// finds the folders on the volume and takes the ordinary rename path.
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
	n, err := movePendingDirsTx(tx, oldPath, newPath)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}

// movePendingDirsTx is MovePendingDirs inside the caller's transaction.
func movePendingDirsTx(tx *sql.Tx, oldPath, newPath string) (int, error) {
	if oldPath == newPath {
		return 0, nil
	}
	rows, err := tx.Query(`SELECT id, dir_path FROM pending_dirs`)
	if err != nil {
		return 0, err
	}
	type move struct {
		id   int64
		path string
	}
	var moves []move
	conflict := false
	for rows.Next() {
		var m move
		if err := rows.Scan(&m.id, &m.path); err != nil {
			rows.Close()
			return 0, err
		}
		if PathAtOrUnder(m.path, oldPath) {
			moves = append(moves, m)
		} else if PathAtOrUnder(m.path, newPath) {
			conflict = true
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	if len(moves) == 0 {
		return 0, nil
	}
	if conflict {
		return 0, ErrPendingDirConflict
	}
	for _, m := range moves {
		if _, err := tx.Exec(`UPDATE pending_dirs SET dir_path=? WHERE id=?`,
			newPath+m.path[len(oldPath):], m.id); err != nil {
			return 0, fmt.Errorf("move pending folder %q -> %q: %w", oldPath, newPath, err)
		}
	}
	return len(moves), nil
}

// PathAtOrUnder reports whether p is dir or a path beneath it.
func PathAtOrUnder(p, dir string) bool {
	return p == dir || strings.HasPrefix(p, dir+"/")
}

// FolderConflict is one pending folder renamed to a conflict name because
// the volume had a file with its name.
type FolderConflict struct {
	OriginalPath string
	ConflictPath string
	CreatedAt    time.Time
}

// RecordFolderConflict keeps a durable, user-visible record of a folder
// conflict.
func (s *SpoolStore) RecordFolderConflict(originalPath, conflictPath string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.Exec(`INSERT INTO folder_conflicts (original_path, conflict_path, created_at) VALUES (?, ?, ?)`,
		originalPath, conflictPath, time.Now().Unix())
	if err != nil {
		return fmt.Errorf("record folder conflict %q: %w", originalPath, err)
	}
	return nil
}

// ListFolderConflicts returns the newest folder conflicts first.
func (s *SpoolStore) ListFolderConflicts(limit int) ([]FolderConflict, error) {
	rows, err := s.db.Query(`SELECT original_path, conflict_path, created_at FROM folder_conflicts ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("list folder conflicts: %w", err)
	}
	defer rows.Close()
	var out []FolderConflict
	for rows.Next() {
		var c FolderConflict
		var created int64
		if err := rows.Scan(&c.OriginalPath, &c.ConflictPath, &created); err != nil {
			return nil, err
		}
		c.CreatedAt = time.Unix(created, 0)
		out = append(out, c)
	}
	return out, rows.Err()
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

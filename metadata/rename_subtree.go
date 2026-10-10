package metadata

import (
	"database/sql"
	"path"
	"strings"
)

// RenameSubtree is the legacy descendants-only interface. New serving callers
// use RenameSubtreeSync or RenameExpected so durable failures are observable.
func (s *Store) RenameSubtree(oldDir, newDir string) int {
	n, _ := s.RenameSubtreeSync(oldDir, newDir)
	return n
}

// RenameSubtreeSync moves durable descendants and their RAM snapshots in one
// ordered publication. Incarnations survive the move; path-bound tokens do not.
func (s *Store) RenameSubtreeSync(oldDir, newDir string) (int, error) {
	return s.renameMirror(oldDir, newDir, nil, nil, false)
}

// RenameExpected moves the source and its descendants. The caller must capture
// both root tokens before changing backing names and retain namespace ownership
// until this returns. This validates the two root observations; a shared remote
// writer contract remains necessary for the backing subtree itself.
func (s *Store) RenameExpected(source, destination MirrorToken) (int, error) {
	return s.renameMirror(source.Path, destination.Path, &source, &destination, true)
}

// CaptureRename refuses a known incomplete mirror before the caller changes
// backing names. Publication rechecks this condition because legacy background
// writers do not yet share the foreground namespace coordinator.
func (s *Store) CaptureRename(oldDir, newDir string) (MirrorToken, MirrorToken, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.mu.RLock()
	oldCached := s.snapshotCachedSubtreeLocked(oldDir, true)
	newCached := s.snapshotCachedSubtreeLocked(newDir, true)
	s.mu.RUnlock()
	tx, err := s.db.Begin()
	if err != nil {
		return MirrorToken{}, MirrorToken{}, err
	}
	defer tx.Rollback()
	for _, snapshot := range [][]MirrorToken{oldCached, newCached} {
		if err := validateCachedSubtreeSnapshot(tx, snapshot); err != nil {
			return MirrorToken{}, MirrorToken{}, err
		}
	}
	source, err := captureToken(tx, oldDir)
	if err != nil {
		return MirrorToken{}, MirrorToken{}, err
	}
	destination, err := captureToken(tx, newDir)
	return source, destination, err
}

// snapshotCachedSubtreeLocked copies only token values while mu is held. The
// caller holds writeMu and cachePublicationMu, so these namespace observations
// remain current throughout the following transaction without holding mu.
func (s *Store) snapshotCachedSubtreeLocked(root string, includeRoot bool) []MirrorToken {
	var tokens []MirrorToken
	for name, e := range s.pathCache {
		if (includeRoot && name == root) || strings.HasPrefix(name, root+"/") {
			tokens = append(tokens, e.Version)
		}
	}
	return tokens
}

func validateCachedSubtreeSnapshot(tx *sql.Tx, tokens []MirrorToken) error {
	for _, token := range tokens {
		if err := validateExpected(tx, token); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) renameMirror(oldDir, newDir string, source, destination *MirrorToken, includeRoot bool) (int, error) {
	if oldDir == newDir || strings.HasPrefix(newDir, oldDir+"/") {
		return 0, ErrMirrorConflict
	}
	committed := false
	invalidateSubtree := !includeRoot
	defer func() {
		if committed && invalidateSubtree {
			s.fireSubtreeRenamed(oldDir, newDir)
		}
	}()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	type rootPreimage struct {
		present bool
		version MirrorToken
	}
	var roots [2]rootPreimage
	s.mu.RLock()
	oldCached := s.snapshotCachedSubtreeLocked(oldDir, includeRoot)
	newCached := s.snapshotCachedSubtreeLocked(newDir, includeRoot)
	if source != nil {
		for i, expected := range []*MirrorToken{source, destination} {
			if expected != nil {
				if e := s.pathCache[expected.Path]; e != nil {
					roots[i] = rootPreimage{present: true, version: e.Version}
				}
			}
		}
	}
	s.mu.RUnlock()
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	for _, snapshot := range [][]MirrorToken{oldCached, newCached} {
		if err := validateCachedSubtreeSnapshot(tx, snapshot); err != nil {
			return 0, err
		}
	}
	if source != nil {
		for i, expected := range []*MirrorToken{source, destination} {
			if expected == nil {
				return 0, ErrMirrorConflict
			}
			if roots[i].present && roots[i].version != *expected {
				return 0, ErrMirrorConflict
			}
			if err := validateExpected(tx, *expected); err != nil {
				return 0, err
			}
		}
	}
	rows, err := tx.Query(`SELECT `+serveColumns+` FROM entries WHERE
 instr(path,?)=1 OR (? AND path=?)`, oldDir+"/", includeRoot, oldDir)
	if err != nil {
		return 0, err
	}
	var moved []*Entry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			rows.Close()
			return 0, err
		}
		moved = append(moved, e)
		if e.Path != oldDir || e.IsDir {
			invalidateSubtree = true
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	if includeRoot && len(moved) == 0 {
		return 0, ErrMirrorConflict
	}
	for _, e := range moved {
		if err := deleteMirrorRow(tx, e.Path, s.ftsDefer); err != nil {
			return 0, err
		}
	}
	oldNames := make([]string, len(moved))
	for i, e := range moved {
		oldNames[i] = e.Path
		identity := e.Version.Incarnation
		revision := e.Version.Revision
		e.Path = newDir + e.Path[len(oldDir):]
		e.Name = path.Base(e.Path)
		e.ParentPath = path.Dir(e.Path)
		if err := ftsExternalUpsert(tx, e, s.ftsDefer); err != nil {
			return 0, err
		}
		result, err := tx.Exec(`UPDATE entries SET incarnation=?, revision=? WHERE path=?`, identity, revision+1, e.Path)
		if err != nil {
			return 0, err
		}
		if err := exactlyOneRow(result); err != nil {
			return 0, err
		}
		if err := loadEntryVersion(tx, e); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	committed = true
	s.mu.Lock()
	for _, old := range oldNames {
		if e := s.pathCache[old]; e != nil {
			s.removeFromChildrenIdx(e)
			s.subtreeRemoveLocked(e)
			if s.inodeCache[e.Inode] == e {
				delete(s.inodeCache, e.Inode)
			}
			delete(s.pathCache, old)
		}
		delete(s.dirVisibilityMtime, old)
	}
	for _, e := range moved {
		s.publishEntryLocked(e)
	}
	s.recomputeSubtreeAggregatesLocked()
	s.mu.Unlock()
	s.wakeFTSCompactor()
	return len(moved), nil
}

func deleteMirrorRow(tx *sql.Tx, name string, deferred bool) error {
	var rowid int64
	var oldName, oldPath string
	if err := tx.QueryRow(`SELECT rowid,name,path FROM entries WHERE path=?`, name).Scan(&rowid, &oldName, &oldPath); err != nil {
		return err
	}
	if err := ftsDeleteOldRowid(tx, rowid, oldName, oldPath, deferred); err != nil {
		return err
	}
	result, err := tx.Exec(`DELETE FROM entries WHERE path=?`, name)
	if err != nil {
		return err
	}
	return exactlyOneRow(result)
}

const renameChunk = 2048

func parentKeyOf(p string) string {
	d := path.Dir(p)
	if d == "" || d == "/" {
		return "."
	}
	return d
}

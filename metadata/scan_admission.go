package metadata

import (
	"database/sql"
	"fmt"
	"time"
)

const scanMutationChunk = 500

// ScanClearLocalOnlyBatch clears one bounded SCAN-owned chunk and returns its
// committed epoch for the next chunk or baseline activation.
func (s *Store) ScanClearLocalOnlyBatch(paths []string, expected StoreEpoch) (StoreEpoch, error) {
	if len(paths) == 0 || len(paths) > scanMutationChunk {
		return StoreEpoch{}, ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.mu.RLock()
	generation := s.cacheGeneration
	s.mu.RUnlock()
	tx, err := s.db.Begin()
	if err != nil {
		return StoreEpoch{}, err
	}
	defer tx.Rollback()
	if err := s.checkScanEpoch(tx, expected, generation); err != nil {
		return StoreEpoch{}, err
	}
	protected, err := s.scanProtectedObservation(tx)
	if err != nil {
		return StoreEpoch{}, err
	}
	for _, p := range paths {
		if _, held := protected.paths[p]; held {
			continue
		}
		result, err := tx.Exec(`UPDATE entries SET local_only=0 WHERE path=? AND local_only=1`, p)
		if err != nil {
			return StoreEpoch{}, fmt.Errorf("SCAN clear local_only %q: %w", p, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return StoreEpoch{}, err
		}
		if affected != 1 {
			return StoreEpoch{}, ErrMirrorConflict
		}
	}
	next, err := captureStoreEpoch(tx, generation)
	if err != nil {
		return StoreEpoch{}, err
	}
	if err := tx.Commit(); err != nil {
		return StoreEpoch{}, err
	}
	s.mu.Lock()
	for _, p := range paths {
		if _, held := protected.paths[p]; held {
			continue
		}
		if e := s.pathCache[p]; e != nil && e.LocalOnly {
			e.LocalOnly = false
			if !e.Version.IsZero() {
				e.Version.Revision++
			}
		}
	}
	s.mu.Unlock()
	return next, nil
}

func (s *Store) checkScanEpoch(tx *sql.Tx, expected StoreEpoch, generation uint64) error {
	got, err := captureStoreEpoch(tx, generation)
	if err != nil {
		return err
	}
	if got != expected {
		return ErrMirrorConflict
	}
	return nil
}

// ScanDeletePathsBatch removes one descendant-first SCAN prune chunk. A
// directory can leave the mirror only when all its direct children are in this
// chunk or have already been removed by an earlier chunk.
func (s *Store) ScanDeletePathsBatch(paths []string, expected StoreEpoch) (StoreEpoch, error) {
	if len(paths) == 0 || len(paths) > scanMutationChunk {
		return StoreEpoch{}, ErrMirrorConflict
	}
	selected := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		if _, duplicate := selected[p]; duplicate {
			return StoreEpoch{}, ErrMirrorConflict
		}
		selected[p] = struct{}{}
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	// The gate freezes cache-only successors; writeMu freezes durable writers.
	// Capture scalar admission facts and identity guards without retaining mu
	// across SQL. Check children even when the selected parent is absent from
	// pathCache: SQLite may still prove it is a directory with a cache-only child.
	type cachePreimage struct {
		entry           *Entry
		present         bool
		isDir           bool
		inode           uint64
		visibility      time.Time
		visible         bool
		unselectedChild bool
	}
	observed := make([]cachePreimage, len(paths))
	s.mu.RLock()
	generation := s.cacheGeneration
	invalidate := s.onScanContentInvalidated
	for i, p := range paths {
		old := &observed[i]
		if e := s.pathCache[p]; e != nil {
			old.entry = e
			old.present = true
			old.isDir = e.IsDir
			old.inode = e.Inode
		}
		old.visibility, old.visible = s.dirVisibilityMtime[p]
		for child := range s.childrenIdx[p] {
			if _, selectedChild := selected[child]; !selectedChild {
				old.unselectedChild = true
				break
			}
		}
	}
	s.mu.RUnlock()
	tx, err := s.db.Begin()
	if err != nil {
		return StoreEpoch{}, err
	}
	defer tx.Rollback()
	if err := s.checkScanEpoch(tx, expected, generation); err != nil {
		return StoreEpoch{}, err
	}
	protected, err := s.scanProtectedObservation(tx)
	if err != nil {
		return StoreEpoch{}, err
	}
	for _, p := range paths {
		if _, held := protected.paths[p]; held {
			return StoreEpoch{}, ErrMirrorConflict
		}
	}
	type oldContent struct {
		path  string
		inode uint64
	}
	oldContentRows := make([]oldContent, 0, len(paths))
	for i, p := range paths {
		var isDir bool
		var inode int64
		err := tx.QueryRow(`SELECT is_dir, inode FROM entries WHERE path=?`, p).Scan(&isDir, &inode)
		if err != nil && err != sql.ErrNoRows {
			return StoreEpoch{}, err
		}
		if err == sql.ErrNoRows {
			return StoreEpoch{}, ErrMirrorConflict
		}
		old := observed[i]
		if old.present && old.isDir != isDir {
			return StoreEpoch{}, ErrMirrorConflict
		}
		if !isDir {
			if old.present {
				oldContentRows = append(oldContentRows, oldContent{path: p, inode: old.inode})
			}
			if !old.present || old.inode != uint64(inode) {
				oldContentRows = append(oldContentRows, oldContent{path: p, inode: uint64(inode)})
			}
			continue
		}
		if old.unselectedChild {
			return StoreEpoch{}, ErrMirrorConflict
		}
		rows, err := tx.Query(`SELECT path FROM entries WHERE parent_path=? LIMIT ?`, p, scanMutationChunk+1)
		if err != nil {
			return StoreEpoch{}, err
		}
		for rows.Next() {
			var child string
			if err := rows.Scan(&child); err != nil {
				rows.Close()
				return StoreEpoch{}, err
			}
			if _, ok := selected[child]; !ok {
				rows.Close()
				return StoreEpoch{}, ErrMirrorConflict
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return StoreEpoch{}, err
		}
		rows.Close()
	}
	for _, p := range paths {
		if err := deleteMirrorRow(tx, p, s.ftsDefer); err != nil {
			return StoreEpoch{}, fmt.Errorf("SCAN prune %q: %w", p, err)
		}
	}
	next, err := captureStoreEpoch(tx, generation)
	if err != nil {
		return StoreEpoch{}, err
	}
	if fn := invalidate; fn != nil {
		for _, old := range oldContentRows {
			fn(old.path, old.inode)
		}
	}
	if err := tx.Commit(); err != nil {
		return StoreEpoch{}, err
	}
	s.mu.Lock()
	for i, p := range paths {
		old := observed[i]
		e := old.entry
		if e != nil && s.pathCache[p] == e {
			if s.inodeCache[old.inode] == e {
				delete(s.inodeCache, old.inode)
			}
			s.removeFromChildrenIdx(e)
			s.shadowEvictedLocked(e)
			s.subtreeRemoveLocked(e)
			delete(s.pathCache, p)
		}
		if old.visible && s.dirVisibilityMtime[p] == old.visibility {
			delete(s.dirVisibilityMtime, p)
		}
	}
	s.mu.Unlock()
	return next, nil
}

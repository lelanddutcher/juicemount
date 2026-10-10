package metadata

import (
	"fmt"
	"time"
)

// DeletePathsConditional removes a captured scoped-prune set only while every
// row and selected directory membership still matches the observation.
func (s *Store) DeletePathsConditional(entries []*Entry) error {
	if len(entries) == 0 {
		return nil
	}
	selected := make(map[string]*Entry, len(entries))
	for _, e := range entries {
		if e == nil || e.Version.IsZero() || e.Version.Path != e.Path {
			return ErrMirrorConflict
		}
		if _, duplicate := selected[e.Path]; duplicate {
			return ErrMirrorConflict
		}
		selected[e.Path] = e
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	protected, err := s.scanProtectedObservation(tx)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if _, held := protected.paths[e.Path]; held {
			return ErrMirrorConflict
		}
	}
	for _, e := range entries {
		if err := validateExpected(tx, e.Version); err != nil {
			return err
		}
		if !e.IsDir {
			continue
		}
		rows, err := tx.Query(`SELECT path FROM entries WHERE parent_path = ?`, e.Path)
		if err != nil {
			return err
		}
		for rows.Next() {
			var child string
			if err := rows.Scan(&child); err != nil {
				rows.Close()
				return err
			}
			if _, ok := selected[child]; !ok {
				rows.Close()
				return ErrMirrorConflict
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
	}

	// SQL work can be large. Keep s.mu free until the final cache-only check.
	for _, e := range entries {
		if err := deleteMirrorRow(tx, e.Path, s.ftsDefer); err != nil {
			return fmt.Errorf("conditional prune %q: %w", e.Path, err)
		}
	}

	// Admission remains late: cache-only publications during the SQL phase must
	// reach the final check and conflict. Once admitted, keep their successors
	// behind the gate until durable Commit and bounded cache cleanup complete.
	finalize := func() error {
		s.cachePublicationMu.Lock()
		defer s.cachePublicationMu.Unlock()
		s.mu.Lock()
		observed := make([]*Entry, len(entries))
		visibility := make([]time.Time, len(entries))
		visibilityPresent := make([]bool, len(entries))
		paths := make([]string, len(entries))
		inodes := make([]uint64, len(entries))
		isDir := make([]bool, len(entries))
		for i, e := range entries {
			current := s.pathCache[e.Path]
			if current != nil && current.Version != e.Version {
				s.mu.Unlock()
				return ErrMirrorConflict
			}
			observed[i] = current
			visibility[i], visibilityPresent[i] = s.dirVisibilityMtime[e.Path]
			paths[i], inodes[i], isDir[i] = e.Path, e.Inode, e.IsDir
			if !e.IsDir {
				continue
			}
			for child, cached := range s.childrenIdx[e.Path] {
				expected := selected[child]
				if expected == nil || cached.Version != expected.Version {
					s.mu.Unlock()
					return ErrMirrorConflict
				}
			}
		}
		invalidate := s.onScanContentInvalidated
		s.mu.Unlock()
		if invalidate != nil {
			for i := range paths {
				if !isDir[i] {
					invalidate(paths[i], inodes[i])
				}
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		removeObservedLocked := func(i int) {
			e := observed[i]
			p := paths[i]
			if e != nil && s.pathCache[p] == e {
				if s.inodeCache[e.Inode] == e {
					delete(s.inodeCache, e.Inode)
				}
				s.removeFromChildrenIdx(e)
				s.shadowEvictedLocked(e)
				s.subtreeRemoveLocked(e)
				delete(s.pathCache, p)
			}
			if visibilityPresent[i] {
				if current, ok := s.dirVisibilityMtime[p]; ok && current == visibility[i] {
					delete(s.dirVisibilityMtime, p)
				}
			}
		}
		// The gate prevents new cache-only children across Commit and parent
		// removal. File removal retains bounded cache-lock chunks.
		s.mu.Lock()
		for i := range paths {
			if isDir[i] {
				removeObservedLocked(i)
			}
		}
		s.mu.Unlock()
		for start := 0; start < len(paths); start += cacheMutationChunk {
			end := start + cacheMutationChunk
			if end > len(paths) {
				end = len(paths)
			}
			s.mu.Lock()
			for i := start; i < end; i++ {
				if !isDir[i] {
					removeObservedLocked(i)
				}
			}
			s.mu.Unlock()
		}
		return nil
	}
	if err := finalize(); err != nil {
		return err
	}
	if s.deletePathsAfterCommitHook != nil {
		s.deletePathsAfterCommitHook()
	}
	return nil
}

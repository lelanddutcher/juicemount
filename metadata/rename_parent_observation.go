package metadata

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// RenameParentObservation is the parent path from the validated NFS call, its
// original handle inode, and the file ID observed by the caller's parent Stat.
type RenameParentObservation struct {
	Path           string
	HandleInode    uint64
	ObservedFileid uint64
}

// observedRenameParentsLocked checks both parents against one live RAM map
// snapshot. Call with s.mu.RLock. A fully absent real-inode mapping may be
// proven from the durable mirror; any conflicting live owner fails closed.
func (s *Store) observedRenameParentsLocked(parents []RenameParentObservation) (bool, []RenameParentObservation) {
	var durable []RenameParentObservation
	now := time.Now()
	for _, parent := range parents {
		if parent.Path == "." {
			if parent.HandleInode != 1 || parent.ObservedFileid != 1 {
				return false, nil
			}
			continue
		}
		if parent.HandleInode == 0 || parent.HandleInode == 1 || parent.ObservedFileid == 0 {
			return false, nil
		}
		owner := s.pathCache[parent.Path]
		alias := s.inodeCache[parent.HandleInode]
		if owner == nil && alias == nil {
			// Path-only synthetic fallback is not proof of a live generation.
			// SQL can prove an exact real-inode owner after complete RAM eviction.
			if parent.HandleInode != parent.ObservedFileid || parent.HandleInode&(1<<63) != 0 {
				return false, nil
			}
			if shadow, ok := s.recentlyEvicted[parent.HandleInode]; ok && shadow.Deleted && shadow.ExpiresAt.After(now) {
				return false, nil
			}
			durable = append(durable, parent)
			continue
		}
		if owner == nil || alias != owner || !owner.IsDir || owner.Path != parent.Path ||
			owner.Inode != parent.ObservedFileid {
			return false, nil
		}
		// The old handle may be a retained synthetic alias to a promoted real
		// inode. Its current inode-map target must be this exact live path owner.
	}
	return true, durable
}

// RenameParentsMatchObservation admits only parents still owned by the
// original handles and still carrying the caller-observed file IDs. It does
// not establish historical identity before that observation or same-inode ABA.
func (s *Store) RenameParentsMatchObservation(parents ...RenameParentObservation) (bool, error) {
	s.mu.RLock()
	valid, durable := s.observedRenameParentsLocked(parents)
	s.mu.RUnlock()
	if !valid || len(durable) == 0 {
		return valid, nil
	}

	// A durable fallback is uncommon. Freeze local SQL and cache publication
	// long enough to recheck both RAM parents and read one SQLite snapshot.
	// Release every Store lock before the rename's spool or backing effects.
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.mu.RLock()
	valid, durable = s.observedRenameParentsLocked(parents)
	s.mu.RUnlock()
	if !valid || len(durable) == 0 {
		return valid, nil
	}
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	for _, parent := range durable {
		var inode int64
		var isDir int
		err := tx.QueryRow(`SELECT inode, is_dir FROM entries WHERE path = ?`, parent.Path).Scan(&inode, &isDir)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if uint64(inode) != parent.HandleInode || uint64(inode) != parent.ObservedFileid || isDir != 1 {
			return false, nil
		}
	}
	return true, nil
}

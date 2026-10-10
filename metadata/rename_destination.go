package metadata

// HasKnownChildren checks both volatile names and the durable local mirror.
// It does not enumerate a directory or contact the backend. A false result is
// not proof that an external backend writer has left the destination empty.
func (s *Store) HasKnownChildren(parentPath string) (bool, error) {
	s.mu.RLock()
	cached := len(s.childrenIdx[parentPath]) != 0
	s.mu.RUnlock()
	if cached {
		return true, nil
	}
	var present bool
	err := s.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM entries WHERE parent_path=? LIMIT 1)`, parentPath).Scan(&present)
	return present, err
}

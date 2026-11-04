package metadata

import "database/sql"

// LookupSiblingPaths is a conservative, complete LOCAL name inventory for a
// non-exact LOOKUP. Durable names survive RAM eviction; cache-only publications
// and prepared CREATE reservations must also veto an ambiguous alias. It is not
// a backend scan and must never fall back to an incomplete inventory on error.
// Only the Unicode-miss path pays for this query. The existing writer order is
// writeMu -> cachePublicationMu -> mu; no namespace or spool lock is held here.
func (s *Store) LookupSiblingPaths(parent string) ([]string, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	rows, err := s.db.Query(`SELECT path FROM entries WHERE parent_path=?
 UNION SELECT path FROM prepared_creates WHERE parent_path=?`, parent, parent)
	if err != nil {
		return nil, err
	}
	names, err := lookupPaths(rows)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	for name := range s.childrenIdx[parent] {
		names[name] = struct{}{}
	}
	for name, prepared := range s.preparedCreates {
		if prepared.ParentPath == parent {
			names[name] = struct{}{}
		}
	}
	s.mu.RUnlock()
	return lookupPathSlice(names), nil
}

// LookupReservedPaths includes durable pending/failed saves and offline links,
// even before their live index publication. Completed audit rows are not names.
func (s *SpoolStore) LookupReservedPaths() ([]string, error) {
	rows, err := s.db.Query(`SELECT nfs_path FROM spool_entries WHERE drain_state<>'done'
 UNION SELECT link_path FROM pending_symlinks`)
	if err != nil {
		return nil, err
	}
	names, err := lookupPaths(rows)
	if err != nil {
		return nil, err
	}
	return lookupPathSlice(names), nil
}

func lookupPaths(rows *sql.Rows) (map[string]struct{}, error) {
	defer rows.Close()
	names := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return names, rows.Close()
}

func lookupPathSlice(names map[string]struct{}) []string {
	out := make([]string, 0, len(names))
	for name := range names {
		out = append(out, name)
	}
	return out
}

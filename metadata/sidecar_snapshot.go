package metadata

import "io/fs"

// ResidentSnapshotFile is a single RAM publication of a durable mirror row.
// It never falls back to SQLite; restored Finder snapshots are lazy-admitted
// only after the normal mirror has populated this row in memory.
type ResidentSnapshotFile struct {
	Identity CachedFileIdentity
	Name     string
	Mode     fs.FileMode
}

func (s *Store) LookupResidentSnapshotFile(name string) (ResidentSnapshotFile, bool) {
	if s == nil {
		return ResidentSnapshotFile{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.pathCache[name]
	if e == nil || e.IsDir || e.LocalOnly || !e.MtimeNsecKnown || e.cacheIdentity == 0 || e.Version.Incarnation == "" || e.Version.Revision <= 0 || e.Inode == 0 || !e.Mode.IsRegular() {
		return ResidentSnapshotFile{}, false
	}
	return ResidentSnapshotFile{
		Identity: CachedFileIdentity{Path: e.Path, Inode: e.Inode, Size: e.Size, MtimeNS: e.Mtime.UnixNano(), Version: e.Version, Publication: e.cacheIdentity},
		Name:     e.Name, Mode: e.Mode,
	}, true
}

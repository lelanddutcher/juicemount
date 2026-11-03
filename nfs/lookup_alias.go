package nfs

import (
	"os"
	"path"
	"strings"
	"unicode/utf8"

	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/text/unicode/norm"
)

// ResolveLookupAlias is only used after a byte-exact LOOKUP miss. NFC is a
// comparison key, never a stored/mutated name. Every local sibling/reservation
// participates, including unservable names: ambiguity or incomplete evidence
// returns no alias. An active-only name without a bound inode also refuses.
func (jfs *juiceFS) ResolveLookupAlias(parent, name string) (result *nfslib.LookupAlias, retErr error) {
	if !utf8.ValidString(name) || strings.IndexFunc(name, func(r rune) bool { return r > 127 }) < 0 || strings.ContainsAny(name, "/\x00") {
		return nil, os.ErrNotExist
	}
	parent = strings.TrimPrefix(parent, "/")
	if parent == "" {
		parent = "."
	}
	if path.Clean(parent) != parent || parent == ".." || strings.HasPrefix(parent, "../") {
		return nil, os.ErrNotExist
	}
	requested := path.Join(parent, name)
	if err := jfs.handler.namespace.servingError(requested); err != nil {
		return nil, err
	}
	// A directory ticket invalidates if any local sibling mutation starts;
	// it does not hold the namespace mutex across SQL or block writers.
	ticket, err := jfs.handler.namespace.admitRead(parent)
	if err != nil {
		return nil, err
	}
	defer func() {
		// A namespace failure during inventory must not become an apparent
		// missing/ambiguous name, including any early no-alias return below.
		if err := ticket.check(); err != nil {
			result, retErr = nil, err
		}
		ticket.release()
	}()
	names, err := jfs.handler.store.LookupSiblingPaths(parent)
	if err != nil {
		return nil, err
	}
	if spool := jfs.handler.spool.Load(); spool != nil {
		reserved, err := spool.lookupReservedPaths()
		if err != nil {
			return nil, err
		}
		names = append(names, reserved...)
	}
	// A newly published exact spelling always wins over the alias. Rechecking
	// the bound metadata path also preserves the normal synthetic-handle map.
	exact, err := jfs.boundLookupAlias(requested)
	if err != nil || exact != nil {
		return exact, err
	}
	key, candidate := norm.NFC.String(name), ""
	for _, stored := range names {
		if path.Dir(stored) != parent || !utf8.ValidString(stored) || strings.ContainsRune(stored, 0) {
			continue
		}
		if stored == requested {
			return nil, os.ErrNotExist // exact but unservable/reserved is not an alias miss
		}
		if norm.NFC.String(path.Base(stored)) == key {
			if candidate != "" && candidate != stored {
				return nil, os.ErrNotExist
			}
			candidate = stored
		}
	}
	if candidate == "" {
		return nil, os.ErrNotExist
	}
	alias, err := jfs.boundLookupAlias(candidate)
	if err != nil {
		return nil, err
	}
	if alias == nil {
		return nil, os.ErrNotExist
	}
	return alias, nil
}

// Bind without ToHandle's synthetic/FUSE fallback or Lstat's network fallback.
// The original stored path supplies both the file identity and response attrs.
func (jfs *juiceFS) boundLookupAlias(name string) (*nfslib.LookupAlias, error) {
	return jfs.boundLookupAliasSnapshot(name, nil)
}

// afterSnapshot is a per-call test barrier, nil in production. No serving lock
// spans metadata lookup or a conflict's local Stat. Recheck the captured cohort
// instead: the drainer can publish a conflict without a namespace operation.
func (jfs *juiceFS) boundLookupAliasSnapshot(name string, afterSnapshot func()) (*nfslib.LookupAlias, error) {
	if err := jfs.handler.namespace.servingError(name); err != nil {
		return nil, err
	}
	spool := jfs.handler.spool.Load()
	var view *conflictReadView
	var active *SpoolEntry
	if spool != nil {
		if spool.index == nil || spool.closed.Load() {
			return nil, errFUSETimeout
		}
		view = spool.conflictRead(name)
		active, _ = spool.LookupActive(name)
	}
	var info os.FileInfo
	var inode uint64
	var entry *metadata.Entry
	if view != nil {
		if view.inode == 0 {
			return nil, errFUSETimeout
		}
		if err := view.check(); err != nil {
			return nil, err
		}
		inode, info = view.inode, view.info()
	} else {
		jfs.handler.writeSizeMu.Lock()
		writeSize, hasWriteSize := jfs.handler.writeSizes[name]
		jfs.handler.writeSizeMu.Unlock()
		entry = jfs.handler.store.LookupByPath(name)
		if entry == nil {
			return nil, nil // no handleable mirror identity; never fabricate one
		}
		inode = entry.Inode
		if active != nil {
			activeInfo := spoolFileInfoForEntry(path.Base(name), active)
			if activeInfo.inode == 0 || activeInfo.inode != inode {
				return nil, errFUSETimeout
			}
			info = activeInfo
		} else {
			if hasWriteSize && writeSize > entry.Size {
				clone := *entry
				clone.Size = writeSize
				clone.ResetGetAttrCache()
				info = jfs.entryToFileInfo(&clone)
			} else {
				info = jfs.entryToFileInfo(entry)
			}
		}
	}
	if afterSnapshot != nil {
		afterSnapshot()
	}
	if err := jfs.handler.namespace.servingError(name); err != nil {
		return nil, err
	}
	if jfs.handler.spool.Load() != spool {
		return nil, errFUSETimeout
	}
	if spool != nil {
		currentActive, _ := spool.LookupActive(name)
		if spool.closed.Load() || spool.conflictRead(name) != view || currentActive != active || (active != nil && active.Inode() != inode) {
			return nil, errFUSETimeout
		}
	}
	if view != nil {
		if err := view.check(); err != nil {
			return nil, err
		}
	} else {
		current := jfs.handler.store.LookupByPath(name)
		if current == nil || current.Inode != inode || current.Version != entry.Version {
			return nil, errFUSETimeout
		}
		jfs.handler.store.RecordSyntheticHandle(inode, name)
	}
	return &nfslib.LookupAlias{Name: path.Base(name), Handle: jfs.handler.export.handle(inode), Info: info}, nil
}

func (s *SpoolStore) lookupReservedPaths() ([]string, error) {
	if s.meta == nil || s.index == nil || s.closed.Load() {
		return nil, os.ErrNotExist
	}
	s.birthMu.Lock()
	birthPending := len(s.pendingBirths) != 0
	s.birthMu.Unlock()
	if birthPending {
		return nil, os.ErrNotExist // an unresolved private birth is not a complete namespace
	}
	names, err := s.meta.LookupReservedPaths()
	if err != nil {
		return nil, err
	}
	s.index.mu.RLock()
	for name := range s.index.byPath {
		names = append(names, name)
	}
	s.index.mu.RUnlock()
	s.conflictMu.RLock()
	for name := range s.conflictByPath {
		names = append(names, name)
	}
	s.conflictMu.RUnlock()
	s.pendingContinuationMu.Lock()
	for name := range s.pendingContinuationPaths {
		names = append(names, name)
	}
	s.pendingContinuationMu.Unlock()
	s.readFaults.Range(func(key, _ any) bool {
		names = append(names, key.(string))
		return true
	})
	return names, nil
}

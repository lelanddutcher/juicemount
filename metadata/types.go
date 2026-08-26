package metadata

import (
	"io/fs"
	"os"
	"sync/atomic"
	"syscall"
	"time"
)

// currentUID and currentGID are cached at init time so that every
// NFS GETATTR/LOOKUP response reports the mount user's identity.
// Without this, entries served from the metadata cache have UID=0/GID=0
// (root:wheel), causing macOS Finder to show the red "no access" badge.
var (
	currentUID uint32
	currentGID uint32
)

func init() {
	currentUID = uint32(os.Getuid())
	currentGID = uint32(os.Getgid())
}

// Entry represents a single file or directory in the metadata store.
type Entry struct {
	Path       string
	Name       string
	ParentPath string
	IsDir      bool
	Size       int64
	Mtime      time.Time
	Inode      uint64
	Mode       fs.FileMode
	LocalOnly  bool // true = created locally, not yet confirmed in Redis

	// getAttrCache is shared by immutable read snapshots of this entry. The
	// payload is published atomically because GETATTR is a hot concurrent path.
	// Store mutations replace the cache pointer before publishing the new
	// scalar values, so an older snapshot can never repopulate stale XDR bytes
	// for a newer size/mode/mtime generation.
	getAttrCache *entryGetAttrCache
}

type entryGetAttrBody struct {
	body []byte
}

type entryGetAttrCache struct {
	body atomic.Pointer[entryGetAttrBody]
}

// prepareGetAttrCache initializes the cache before an Entry is published in
// Store's RAM indexes. Caller must own the Entry or hold Store.mu for writing.
func (e *Entry) prepareGetAttrCache() {
	if e != nil && e.getAttrCache == nil {
		e.getAttrCache = &entryGetAttrCache{}
	}
}

// ResetGetAttrCache detaches this Entry from any previously serialized
// attributes. Use whenever a clone changes a field encoded by NFS GETATTR.
func (e *Entry) ResetGetAttrCache() {
	if e != nil {
		e.getAttrCache = &entryGetAttrCache{}
	}
}

// CachedGetAttr returns immutable pre-serialized NFS GETATTR bytes, if any.
func (e *Entry) CachedGetAttr() []byte {
	if e == nil || e.getAttrCache == nil {
		return nil
	}
	if cached := e.getAttrCache.body.Load(); cached != nil {
		return cached.body
	}
	return nil
}

// CacheGetAttr atomically publishes immutable pre-serialized NFS GETATTR
// bytes. The caller hands ownership of body to Entry and must not mutate it.
func (e *Entry) CacheGetAttr(body []byte) {
	if e == nil || e.getAttrCache == nil || len(body) == 0 {
		return
	}
	e.getAttrCache.body.Store(&entryGetAttrBody{body: body})
}

// snapshot returns a stable scalar view for callers outside Store's lock.
// The concurrency-safe GETATTR cache remains shared until a mutation replaces
// the cached Entry's cache pointer.
func (e *Entry) snapshot() *Entry {
	if e == nil {
		return nil
	}
	clone := *e
	if clone.getAttrCache == nil {
		clone.getAttrCache = &entryGetAttrCache{}
	}
	return &clone
}

// FileInfo implements fs.FileInfo for an Entry.
type FileInfo struct {
	entry         *Entry
	mtimeOverride time.Time
}

func (e *Entry) FileInfo() *FileInfo {
	return &FileInfo{entry: e}
}

// FileInfoWithModTime preserves the concrete *metadata.FileInfo type (and its
// pre-serialized GETATTR cache) while allowing the NFS adapter to expose a
// push-driven directory visibility generation. The override is never persisted
// and is ignored for zero values.
func (e *Entry) FileInfoWithModTime(mtime time.Time) *FileInfo {
	return &FileInfo{entry: e, mtimeOverride: mtime}
}

// Entry returns the underlying metadata Entry.
func (fi *FileInfo) Entry() *Entry { return fi.entry }

func (fi *FileInfo) Name() string      { return fi.entry.Name }
func (fi *FileInfo) Size() int64       { return fi.entry.Size }
func (fi *FileInfo) Mode() fs.FileMode { return fi.entry.Mode }
func (fi *FileInfo) ModTime() time.Time {
	if !fi.mtimeOverride.IsZero() {
		return fi.mtimeOverride
	}
	return fi.entry.Mtime
}
func (fi *FileInfo) IsDir() bool { return fi.entry.IsDir }

// Sys returns a *syscall.Stat_t with the correct UID, GID, and Ino so that
// the NFS file attribute builder (internal/nfs/file) reports the current
// user's identity instead of root:wheel. This is what makes Finder show
// folders as accessible instead of showing the red minus badge.
func (fi *FileInfo) Sys() any {
	return &syscall.Stat_t{
		Ino:   fi.entry.Inode,
		Uid:   currentUID,
		Gid:   currentGID,
		Nlink: 1,
	}
}

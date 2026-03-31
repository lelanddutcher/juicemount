package metadata

import (
	"io/fs"
	"os"
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

	// [JM5] Pre-serialized XDR bytes for the NFS GETATTR response body
	// (NFSStatusOk + fattr3). Cached here so that repeated GETATTRs for
	// the same file skip XDR marshaling entirely — just copy 88 bytes.
	// nil means not yet computed; set lazily on first GETATTR.
	PreSerializedGetAttr []byte
}

// FileInfo implements fs.FileInfo for an Entry.
type FileInfo struct {
	entry *Entry
}

func (e *Entry) FileInfo() *FileInfo {
	return &FileInfo{entry: e}
}

// Entry returns the underlying metadata Entry.
func (fi *FileInfo) Entry() *Entry     { return fi.entry }

func (fi *FileInfo) Name() string      { return fi.entry.Name }
func (fi *FileInfo) Size() int64       { return fi.entry.Size }
func (fi *FileInfo) Mode() fs.FileMode { return fi.entry.Mode }
func (fi *FileInfo) ModTime() time.Time { return fi.entry.Mtime }
func (fi *FileInfo) IsDir() bool       { return fi.entry.IsDir }

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

/*
 * JuiceMount local source (not upstream JuiceFS). Apache-2.0.
 */

package object

import (
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// fileOps is every filesystem operation the file:// backend performs. Each
// takes the absolute path filestore.path() computed. On Linux the backend
// uses beneathOps (file_beneath_linux.go), which resolves every path
// beneath the backend's root directory. Elsewhere it uses pathOps, the
// upstream behaviour.
type fileOps interface {
	Lstat(p string) (os.FileInfo, error)
	Stat(p string) (os.FileInfo, error)
	Open(p string) (*os.File, error)
	OpenFile(p string, flag int, perm os.FileMode) (*os.File, error)
	CreateTemp(dir, prefix string) (*os.File, error)
	MkdirAll(p string, perm os.FileMode) error
	Rename(oldPath, newPath string) error
	Link(oldPath, newPath string) error
	Remove(p string) error
	Chmod(p string, mode os.FileMode) error
	Lchown(p string, uid, gid int) error
	Lchtimes(p string, mtime time.Time) error
	Symlink(target, p string) error
	Readlink(p string) (string, error)
	// ReadDir lists the folder p. Each entry carries Lstat information.
	ReadDir(p string) ([]os.FileInfo, error)
	// SyncDir fsyncs the folder p.
	SyncDir(p string) error
}

// errOutsideRoot is returned (inside an *os.PathError) when a path
// resolves outside the root (file_beneath_linux.go). It unwraps to EXDEV.
var errOutsideRoot = &outsideRootError{}

type outsideRootError struct{}

func (*outsideRootError) Error() string {
	return "refused: path resolves outside the file storage root (symlink or ..)"
}
func (*outsideRootError) Unwrap() error { return syscall.EXDEV }

// pathOps resolves paths the ordinary way: symlinks anywhere are followed.
type pathOps struct{}

func (pathOps) Lstat(p string) (os.FileInfo, error) { return os.Lstat(p) }
func (pathOps) Stat(p string) (os.FileInfo, error)  { return os.Stat(p) }
func (pathOps) Open(p string) (*os.File, error)     { return os.Open(p) }
func (pathOps) OpenFile(p string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(p, flag, perm)
}
func (pathOps) CreateTemp(dir, prefix string) (*os.File, error) {
	return os.CreateTemp(dir, prefix+"*")
}
func (pathOps) MkdirAll(p string, perm os.FileMode) error { return os.MkdirAll(p, perm) }
func (pathOps) Rename(oldPath, newPath string) error      { return os.Rename(oldPath, newPath) }
func (pathOps) Link(oldPath, newPath string) error        { return os.Link(oldPath, newPath) }
func (pathOps) Remove(p string) error                     { return os.Remove(p) }
func (pathOps) Chmod(p string, mode os.FileMode) error    { return os.Chmod(p, mode) }
func (pathOps) Lchown(p string, uid, gid int) error       { return os.Lchown(p, uid, gid) }
func (pathOps) Lchtimes(p string, mtime time.Time) error  { return lchtimesPath(p, mtime) }
func (pathOps) Symlink(target, p string) error            { return os.Symlink(target, p) }
func (pathOps) Readlink(p string) (string, error)         { return os.Readlink(p) }
func (pathOps) SyncDir(p string) error                    { return syncObjectDirectory(p) }
func (pathOps) ReadDir(p string) ([]os.FileInfo, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.Readdir(-1)
}

// storageBaseDir is the folder a file:// root confines to: the named
// path itself. Upstream treats a root without a trailing separator as a
// name prefix inside its parent folder; confining to that parent would
// widen the root (at "/sources" it is "/"). With the named path as the
// base, such a root can still address keys beneath it ("<root>/x"), but
// listing its parent or a same-prefix sibling is refused. Every in-tree
// caller (format, load, metadata backup) passes a root ending in "/".
func storageBaseDir(root string) string {
	return filepath.Clean(root)
}

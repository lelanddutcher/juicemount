package smartfolders

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"syscall"
)

// FileType is the type of a volume entry, as the reconciler needs it.
type FileType uint8

const (
	TypeOther FileType = iota
	TypeRegular
	TypeDir
	TypeSymlink
)

// Info is what the reconciler learns about one entry. Ino identifies the
// entry: a recorded directory or link whose inode changed is not the one the
// Manager created.
type Info struct {
	Type FileType
	Ino  uint64
	Perm fs.FileMode // permission bits (0o777)
}

// ErrSymlinkInPath: a parent component of the path is a symlink. FS
// implementations never follow symlinks on the way to an entry.
var ErrSymlinkInPath = errors.New("path crosses a symlink")

// FS is the volume as the reconciler sees it. Paths are volume-relative and
// slash-separated ("Recents/Projects/clip.mov"). No method follows a symlink
// in any component; errors satisfy errors.Is(err, fs.ErrNotExist) /
// fs.ErrExist where those apply.
//
// The production implementation is jfsmeta.FS (the JuiceFS metadata client,
// standalone mode). OSFS exists only as the second backend of the shared
// behaviour suite (sftest).
type FS interface {
	Lstat(p string) (Info, error)
	Readlink(p string) (string, error)
	// Mkdir creates a directory with perm (a umask may clear bits; Info
	// reports the mode it got); fs.ErrExist if the name is taken.
	Mkdir(p string, perm fs.FileMode) (Info, error)
	// Chmod sets perm on the directory at p only if it is still the
	// directory with inode ino (checked on an open handle, never by name).
	Chmod(p string, ino uint64, perm fs.FileMode) error
	// Symlink creates p with the given text; fs.ErrExist if p is taken.
	Symlink(text, p string) (Info, error)
	// Place renames tmp to p (same directory) without ever replacing an
	// existing p: fs.ErrExist if p is taken.
	Place(tmp, p string) error
	// Swap atomically exchanges tmp and p (same directory; both exist).
	// Never a plain rename: nothing at p is ever destroyed.
	Swap(tmp, p string) error
	// Remove unlinks a non-directory, bypassing the JuiceFS trash.
	Remove(p string) error
	// Empty reports whether the directory p holds no entries.
	Empty(p string) (bool, error)
	// Rmdir removes the directory p (never anything else), bypassing the
	// JuiceFS trash; an error satisfying errors.Is(err, syscall.ENOTEMPTY)
	// if it holds anything.
	Rmdir(p string) error
}

func splitPath(p string) ([]string, error) {
	parts, ok := cleanVolumePath(p)
	if !ok || strings.HasPrefix(p, "/") {
		return nil, fmt.Errorf("invalid volume path %q: %w", p, syscall.EINVAL)
	}
	return parts, nil
}

// OSFS is a local directory tree as an FS. It is a TEST backend: it runs
// the shared behaviour suite (sftest) next to the JuiceFS metadata backend,
// and no production code path uses it. It cannot identify a directory it
// has just created atomically (mkdirat returns no handle, so the identity
// comes from a second lookup that a concurrent rename can redirect), which
// is why the Manager runs smart folders only through jfsmeta.FS.
//
// Every operation walks the path one directory at a time from an os.Root,
// refuses a symlink component, and checks the directory it opened is the
// one it inspected.
type OSFS struct {
	root *os.Root
}

// OpenOSFS anchors an OSFS at dir.
func OpenOSFS(dir string) (*OSFS, error) {
	if !osfsSupported {
		return nil, fmt.Errorf("the OS backend needs inode numbers: %w", errors.ErrUnsupported)
	}
	r, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	return &OSFS{root: r}, nil
}

// Close releases the root.
func (o *OSFS) Close() error { return o.root.Close() }

// parent opens the directory holding p's last component.
func (o *OSFS) parent(p string) (*os.Root, string, func(), error) {
	parts, err := splitPath(p)
	if err != nil {
		return nil, "", nil, err
	}
	cur := o.root
	closeCur := func() {}
	for _, c := range parts[:len(parts)-1] {
		fi, err := cur.Lstat(c)
		if err != nil {
			closeCur()
			return nil, "", nil, err
		}
		if fi.Mode()&fs.ModeSymlink != 0 {
			closeCur()
			return nil, "", nil, ErrSymlinkInPath
		}
		if !fi.IsDir() {
			closeCur()
			return nil, "", nil, syscall.ENOTDIR
		}
		next, err := cur.OpenRoot(c)
		if err != nil {
			closeCur()
			return nil, "", nil, err
		}
		opened, err := next.Stat(".")
		if err != nil || !os.SameFile(fi, opened) {
			next.Close()
			closeCur()
			if err == nil {
				err = ErrSymlinkInPath // swapped between Lstat and open
			}
			return nil, "", nil, err
		}
		closeCur()
		cur = next
		closeCur = func() { next.Close() }
	}
	return cur, parts[len(parts)-1], closeCur, nil
}

func infoOf(fi fs.FileInfo) Info {
	in := Info{Type: TypeOther}
	switch m := fi.Mode(); {
	case m&fs.ModeSymlink != 0:
		in.Type = TypeSymlink
	case m.IsDir():
		in.Type = TypeDir
	case m.IsRegular():
		in.Type = TypeRegular
	}
	in.Perm = fi.Mode().Perm()
	in.Ino = inodeOf(fi)
	return in
}

func (o *OSFS) Lstat(p string) (Info, error) {
	d, leaf, done, err := o.parent(p)
	if err != nil {
		return Info{}, err
	}
	defer done()
	fi, err := d.Lstat(leaf)
	if err != nil {
		return Info{}, err
	}
	return infoOf(fi), nil
}

func (o *OSFS) Readlink(p string) (string, error) {
	d, leaf, done, err := o.parent(p)
	if err != nil {
		return "", err
	}
	defer done()
	return d.Readlink(leaf)
}

func (o *OSFS) Mkdir(p string, perm fs.FileMode) (Info, error) {
	d, leaf, done, err := o.parent(p)
	if err != nil {
		return Info{}, err
	}
	defer done()
	if err := d.Mkdir(leaf, perm); err != nil {
		return Info{}, err
	}
	// Identify what was created through a handle opened without following
	// a symlink, not by a second lookup of the name.
	dir, err := d.Open(".")
	if err != nil {
		return Info{}, err
	}
	defer dir.Close()
	return dirHandleOp(int(dir.Fd()), leaf, 0, 0)
}

func (o *OSFS) Chmod(p string, ino uint64, perm fs.FileMode) error {
	d, leaf, done, err := o.parent(p)
	if err != nil {
		return err
	}
	defer done()
	dir, err := d.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	_, err = dirHandleOp(int(dir.Fd()), leaf, ino, perm)
	return err
}

func (o *OSFS) Symlink(text, p string) (Info, error) {
	d, leaf, done, err := o.parent(p)
	if err != nil {
		return Info{}, err
	}
	defer done()
	if err := d.Symlink(text, leaf); err != nil {
		return Info{}, err
	}
	fi, err := d.Lstat(leaf)
	if err != nil {
		return Info{}, err
	}
	return infoOf(fi), nil
}

func (o *OSFS) Place(tmp, p string) error { return o.renameIn(tmp, p, renameNoReplace) }

func (o *OSFS) Swap(tmp, p string) error { return o.renameIn(tmp, p, renameExchange) }

// renameIn renames within one directory with the platform's no-replace or
// exchange flag, through a descriptor of the directory opened from the
// anchored root (renameat2 on Linux, renameatx_np on macOS). Elsewhere it
// refuses: a plain rename could destroy what is at p.
func (o *OSFS) renameIn(tmp, p string, op renameOp) error {
	if path.Dir(tmp) != path.Dir(p) {
		return fmt.Errorf("rename across directories: %w", syscall.EINVAL)
	}
	d, leaf, done, err := o.parent(p)
	if err != nil {
		return err
	}
	defer done()
	dir, err := d.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	return renameAt(int(dir.Fd()), path.Base(tmp), leaf, op)
}

func (o *OSFS) Remove(p string) error {
	d, leaf, done, err := o.parent(p)
	if err != nil {
		return err
	}
	defer done()
	fi, err := d.Lstat(leaf)
	if err != nil {
		return err
	}
	if fi.IsDir() {
		return syscall.EISDIR
	}
	return d.Remove(leaf)
}

func (o *OSFS) Empty(p string) (bool, error) {
	// The parent walk of a path one level down opens p itself, checked
	// like every other component (a real directory, not a symlink).
	d, _, done, err := o.parent(p + "/.probe")
	if err != nil {
		return false, err
	}
	defer done()
	f, err := d.Open(".")
	if err != nil {
		return false, err
	}
	defer f.Close()
	names, err := f.Readdirnames(1)
	if errors.Is(err, io.EOF) {
		return true, nil
	}
	return len(names) == 0, err
}

func (o *OSFS) Rmdir(p string) error {
	d, leaf, done, err := o.parent(p)
	if err != nil {
		return err
	}
	defer done()
	dir, err := d.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	return rmdirAt(int(dir.Fd()), leaf)
}

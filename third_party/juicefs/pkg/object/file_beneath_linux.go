//go:build linux

/*
 * JuiceMount local source (not upstream JuiceFS). Apache-2.0.
 */

package object

import (
	"errors"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// beneathOps confines the file:// backend to its root directory.
//
// The root folder is opened once. Every later path is resolved relative to
// that fd with openat2(RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS), or with an
// *at syscall on a parent fd obtained that way. The kernel then refuses,
// atomically per call, any resolution that would leave the root: an
// absolute symlink, a ".." above it, or a chain of symlinks that ends
// outside it. Symlinks that stay beneath the root are followed as before,
// and symlinks themselves can still be created and read (--links).
//
// A refusal surfaces as an *os.PathError whose error is errOutsideRoot
// (it unwraps to EXDEV). Without openat2 (Linux before 5.6, or a seccomp
// policy that refuses it) every operation fails closed with
// errNoOpenat2.
//
// The root path itself is trusted: it is the folder the caller named.
type beneathOps struct {
	base string // clean absolute root folder

	mu     sync.Mutex
	rootFd int // -1 until opened
}

// errOutsideRoot is returned when a path resolves outside the root.
var errOutsideRoot = &outsideRootError{}

type outsideRootError struct{}

func (*outsideRootError) Error() string {
	return "refused: path resolves outside the file storage root (symlink or ..)"
}
func (*outsideRootError) Unwrap() error { return syscall.EXDEV }

// errNoOpenat2 is returned when the kernel cannot confine paths.
var errNoOpenat2 = errors.New("refused: openat2 is unavailable (Linux 5.6 or later is required to confine file storage to its root)")

// maxOpenat2Tries bounds retries when openat2 reports EAGAIN (a rename
// raced a RESOLVE_BENEATH lookup) or EINTR. After that the call fails.
const maxOpenat2Tries = 64

// openat2Func is unix.Openat2; tests replace it to simulate an old kernel.
var openat2Func = unix.Openat2

func newFileOps(root string) fileOps {
	base := storageBaseDir(root)
	if !filepath.IsAbs(base) {
		if abs, err := filepath.Abs(base); err == nil {
			base = abs
		}
	}
	return &beneathOps{base: base, rootFd: -1}
}

// root returns the root fd, opening it on first use. With create, a
// missing root folder is created first (a sync destination may not exist
// yet). A failure to open is not cached, so a later call can retry.
func (b *beneathOps) root(create bool) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rootFd >= 0 {
		return b.rootFd, nil
	}
	fd, err := unix.Open(b.base, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOENT) && create {
		if err = os.MkdirAll(b.base, 0777); err != nil {
			return -1, err
		}
		fd, err = unix.Open(b.base, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	}
	if err != nil {
		return -1, &os.PathError{Op: "open", Path: b.base, Err: err}
	}
	// Probe openat2 once, so an old kernel fails closed with a clear error.
	probe, perr := openat2Func(fd, ".", &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS,
	})
	if perr != nil {
		unix.Close(fd)
		if errors.Is(perr, unix.ENOSYS) || errors.Is(perr, unix.EPERM) {
			return -1, errNoOpenat2
		}
		return -1, &os.PathError{Op: "openat2", Path: b.base, Err: perr}
	}
	unix.Close(probe)
	b.rootFd = fd
	return fd, nil
}

// rel turns an absolute path from filestore.path() into a path relative to
// the root. "." is the root itself.
func (b *beneathOps) rel(op, p string) (string, error) {
	c := filepath.Clean(p)
	if c == b.base {
		return ".", nil
	}
	prefix := b.base
	if prefix != "/" {
		prefix += "/"
	}
	if !strings.HasPrefix(c, prefix) {
		return "", &os.PathError{Op: op, Path: p, Err: errOutsideRoot}
	}
	return c[len(prefix):], nil
}

func (b *beneathOps) pathErr(op, p string, err error) error {
	switch {
	case errors.Is(err, unix.EXDEV):
		err = errOutsideRoot
	case errors.Is(err, unix.ENOSYS):
		err = errNoOpenat2
	}
	return &os.PathError{Op: op, Path: p, Err: err}
}

// openat2 resolves rel beneath the root. create opens (and creates) a
// missing root first.
func (b *beneathOps) openat2(op, p string, flags uint64, mode uint32, create bool) (int, error) {
	r, err := b.rel(op, p)
	if err != nil {
		return -1, err
	}
	rootFd, err := b.root(create)
	if err != nil {
		return -1, err
	}
	for try := 1; ; try++ {
		fd, err := openat2Func(rootFd, r, &unix.OpenHow{
			Flags:   flags | unix.O_CLOEXEC,
			Mode:    uint64(mode),
			Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS,
		})
		if (errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR)) && try < maxOpenat2Tries {
			continue // a concurrent rename raced the lookup; retry
		}
		if err != nil {
			return -1, b.pathErr(op, p, err)
		}
		return fd, nil
	}
}

// parent opens the parent folder of p beneath the root and returns its fd
// and the final name. For the root itself it returns the root fd dup and
// name "".
func (b *beneathOps) parent(op, p string, create bool) (int, string, error) {
	r, err := b.rel(op, p)
	if err != nil {
		return -1, "", err
	}
	if r == "." {
		fd, err := b.openat2(op, b.base, unix.O_PATH|unix.O_DIRECTORY, 0, create)
		return fd, "", err
	}
	dir, name := filepath.Split(r)
	fd, err := b.openat2(op, filepath.Join(b.base, dir), unix.O_PATH|unix.O_DIRECTORY, 0, create)
	if err != nil {
		return -1, "", err
	}
	return fd, name, nil
}

func (b *beneathOps) statFd(op, p string, flags uint64) (os.FileInfo, error) {
	fd, err := b.openat2(op, p, unix.O_PATH|flags, 0, false)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		return nil, b.pathErr(op, p, err)
	}
	return newStatInfo(filepath.Base(p), &st), nil
}

func (b *beneathOps) Lstat(p string) (os.FileInfo, error) {
	return b.statFd("lstat", p, unix.O_NOFOLLOW)
}

func (b *beneathOps) Stat(p string) (os.FileInfo, error) {
	return b.statFd("stat", p, 0)
}

func (b *beneathOps) Open(p string) (*os.File, error) {
	return b.OpenFile(p, os.O_RDONLY, 0)
}

func (b *beneathOps) OpenFile(p string, flag int, perm os.FileMode) (*os.File, error) {
	create := flag&os.O_CREATE != 0
	fd, err := b.openat2("open", p, uint64(flag), uint32(perm.Perm()), create)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), p), nil
}

// CreateTemp creates a new file named prefix+random in dir, exclusively.
func (b *beneathOps) CreateTemp(dir, prefix string) (*os.File, error) {
	for i := 0; i < 10000; i++ {
		name := filepath.Join(dir, prefix+strconv.FormatUint(uint64(rand.Uint32()), 10))
		f, err := b.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		return f, err
	}
	return nil, &os.PathError{Op: "createtemp", Path: dir + "/" + prefix + "*", Err: fs.ErrExist}
}

// MkdirAll creates p and its missing parents beneath the root. Each level
// is resolved from the root fd, so a symlinked level is followed only when
// it stays beneath the root.
func (b *beneathOps) MkdirAll(p string, perm os.FileMode) error {
	r, err := b.rel("mkdir", p)
	if err != nil {
		return err
	}
	if _, err := b.root(true); err != nil {
		return err
	}
	if r == "." {
		return nil
	}
	parts := strings.Split(r, "/")
	for i := range parts {
		cur := filepath.Join(b.base, filepath.Join(parts[:i+1]...))
		fd, err := b.openat2("mkdir", cur, unix.O_PATH|unix.O_DIRECTORY, 0, false)
		if err == nil {
			unix.Close(fd)
			continue
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		pfd, name, err := b.parent("mkdir", cur, false)
		if err != nil {
			return err
		}
		err = unix.Mkdirat(pfd, name, uint32(perm.Perm()))
		unix.Close(pfd)
		if err != nil && !errors.Is(err, unix.EEXIST) {
			return b.pathErr("mkdir", cur, err)
		}
		// Re-check: a name that already existed (or was raced in) must be
		// a folder beneath the root.
		fd, err = b.openat2("mkdir", cur, unix.O_PATH|unix.O_DIRECTORY, 0, false)
		if err != nil {
			return err
		}
		unix.Close(fd)
	}
	return nil
}

func (b *beneathOps) twoParents(op, oldPath, newPath string) (int, string, int, string, error) {
	ofd, oname, err := b.parent(op, oldPath, false)
	if err != nil {
		return -1, "", -1, "", err
	}
	nfd, nname, err := b.parent(op, newPath, false)
	if err != nil {
		unix.Close(ofd)
		return -1, "", -1, "", err
	}
	if oname == "" || nname == "" {
		unix.Close(ofd)
		unix.Close(nfd)
		return -1, "", -1, "", &os.PathError{Op: op, Path: oldPath, Err: unix.EBUSY}
	}
	return ofd, oname, nfd, nname, nil
}

func (b *beneathOps) Rename(oldPath, newPath string) error {
	ofd, oname, nfd, nname, err := b.twoParents("rename", oldPath, newPath)
	if err != nil {
		return err
	}
	defer unix.Close(ofd)
	defer unix.Close(nfd)
	if err := unix.Renameat(ofd, oname, nfd, nname); err != nil {
		return &os.LinkError{Op: "rename", Old: oldPath, New: newPath, Err: err}
	}
	return nil
}

func (b *beneathOps) Link(oldPath, newPath string) error {
	ofd, oname, nfd, nname, err := b.twoParents("link", oldPath, newPath)
	if err != nil {
		return err
	}
	defer unix.Close(ofd)
	defer unix.Close(nfd)
	if err := unix.Linkat(ofd, oname, nfd, nname, 0); err != nil {
		return &os.LinkError{Op: "link", Old: oldPath, New: newPath, Err: err}
	}
	return nil
}

// Remove removes a file, a symlink (not its target) or an empty folder.
func (b *beneathOps) Remove(p string) error {
	pfd, name, err := b.parent("remove", p, false)
	if err != nil {
		return err
	}
	defer unix.Close(pfd)
	if name == "" {
		return &os.PathError{Op: "remove", Path: p, Err: unix.EBUSY}
	}
	err = unix.Unlinkat(pfd, name, 0)
	if errors.Is(err, unix.EISDIR) || errors.Is(err, unix.EPERM) {
		if derr := unix.Unlinkat(pfd, name, unix.AT_REMOVEDIR); derr == nil || !errors.Is(derr, unix.ENOTDIR) {
			err = derr
		}
	}
	if err != nil {
		return b.pathErr("remove", p, err)
	}
	return nil
}

// Chmod follows a final symlink only beneath the root, then changes the
// exact inode through its fd's /proc entry (fchmod does not take O_PATH).
func (b *beneathOps) Chmod(p string, mode os.FileMode) error {
	fd, err := b.openat2("chmod", p, unix.O_PATH, 0, false)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := unix.Chmod("/proc/self/fd/"+strconv.Itoa(fd), uint32(syscallMode(mode))); err != nil {
		return b.pathErr("chmod", p, err)
	}
	return nil
}

func (b *beneathOps) Lchown(p string, uid, gid int) error {
	pfd, name, err := b.parent("lchown", p, false)
	if err != nil {
		return err
	}
	defer unix.Close(pfd)
	if name == "" {
		err = unix.Fchownat(pfd, "", uid, gid, unix.AT_EMPTY_PATH)
	} else {
		err = unix.Fchownat(pfd, name, uid, gid, unix.AT_SYMLINK_NOFOLLOW)
	}
	if err != nil {
		return b.pathErr("lchown", p, err)
	}
	return nil
}

func (b *beneathOps) Lchtimes(p string, mtime time.Time) error {
	ts := []unix.Timespec{{Sec: unix.UTIME_OMIT, Nsec: unix.UTIME_OMIT}, unix.NsecToTimespec(mtime.UnixNano())}
	pfd, name, err := b.parent("lchtimes", p, false)
	if err != nil {
		return err
	}
	defer unix.Close(pfd)
	if name == "" {
		err = unix.UtimesNanoAt(unix.AT_FDCWD, "/proc/self/fd/"+strconv.Itoa(pfd), ts, 0)
	} else {
		err = unix.UtimesNanoAt(pfd, name, ts, unix.AT_SYMLINK_NOFOLLOW)
	}
	if err != nil {
		return b.pathErr("lchtimes", p, err)
	}
	return nil
}

// Symlink stores a symlink with the given target text. The link is not
// followed here; any later access through it is resolved beneath the root.
func (b *beneathOps) Symlink(target, p string) error {
	pfd, name, err := b.parent("symlink", p, true)
	if err != nil {
		return err
	}
	defer unix.Close(pfd)
	if name == "" {
		return &os.PathError{Op: "symlink", Path: p, Err: unix.EEXIST}
	}
	if err := unix.Symlinkat(target, pfd, name); err != nil {
		return &os.LinkError{Op: "symlink", Old: target, New: p, Err: err}
	}
	return nil
}

func (b *beneathOps) Readlink(p string) (string, error) {
	pfd, name, err := b.parent("readlink", p, false)
	if err != nil {
		return "", err
	}
	defer unix.Close(pfd)
	if name == "" {
		return "", &os.PathError{Op: "readlink", Path: p, Err: unix.EINVAL}
	}
	for size := 256; ; size *= 2 {
		buf := make([]byte, size)
		n, err := unix.Readlinkat(pfd, name, buf)
		if err != nil {
			return "", b.pathErr("readlink", p, err)
		}
		if n < size {
			return string(buf[:n]), nil
		}
	}
}

// ReadDir lists p. Each entry's information comes from an O_NOFOLLOW open
// beneath the root, so it describes the entry itself (Lstat).
func (b *beneathOps) ReadDir(p string) ([]os.FileInfo, error) {
	fd, err := b.openat2("readdir", p, unix.O_RDONLY|unix.O_DIRECTORY, 0, false)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), p)
	names, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	out := make([]os.FileInfo, 0, len(names))
	for _, n := range names {
		fi, err := b.Lstat(filepath.Join(p, n))
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue // removed since the listing
			}
			return nil, err
		}
		out = append(out, fi)
	}
	return out, nil
}

func (b *beneathOps) SyncDir(p string) error {
	fd, err := b.openat2("fsync", p, unix.O_RDONLY|unix.O_DIRECTORY, 0, false)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := unix.Fsync(fd); err != nil {
		return b.pathErr("fsync", p, err)
	}
	return nil
}

// statInfo is an os.FileInfo built from a stat buffer, matching what
// os.Lstat returns (Sys is *syscall.Stat_t, as getOwnerGroup expects).
type statInfo struct {
	name string
	st   syscall.Stat_t
}

func newStatInfo(name string, st *syscall.Stat_t) *statInfo {
	return &statInfo{name: name, st: *st}
}

func (s *statInfo) Name() string       { return s.name }
func (s *statInfo) Size() int64        { return s.st.Size }
func (s *statInfo) ModTime() time.Time { return time.Unix(s.st.Mtim.Unix()) }
func (s *statInfo) IsDir() bool        { return s.Mode().IsDir() }
func (s *statInfo) Sys() any           { return &s.st }
func (s *statInfo) Mode() os.FileMode {
	m := os.FileMode(s.st.Mode & 0777)
	switch s.st.Mode & syscall.S_IFMT {
	case syscall.S_IFBLK:
		m |= os.ModeDevice
	case syscall.S_IFCHR:
		m |= os.ModeDevice | os.ModeCharDevice
	case syscall.S_IFDIR:
		m |= os.ModeDir
	case syscall.S_IFIFO:
		m |= os.ModeNamedPipe
	case syscall.S_IFLNK:
		m |= os.ModeSymlink
	case syscall.S_IFSOCK:
		m |= os.ModeSocket
	}
	if s.st.Mode&syscall.S_ISGID != 0 {
		m |= os.ModeSetgid
	}
	if s.st.Mode&syscall.S_ISUID != 0 {
		m |= os.ModeSetuid
	}
	if s.st.Mode&syscall.S_ISVTX != 0 {
		m |= os.ModeSticky
	}
	return m
}

// syscallMode converts an os.FileMode to the chmod bits, as os.Chmod does.
func syscallMode(i os.FileMode) uint32 {
	o := uint32(i.Perm())
	if i&os.ModeSetuid != 0 {
		o |= syscall.S_ISUID
	}
	if i&os.ModeSetgid != 0 {
		o |= syscall.S_ISGID
	}
	if i&os.ModeSticky != 0 {
		o |= syscall.S_ISVTX
	}
	return o
}

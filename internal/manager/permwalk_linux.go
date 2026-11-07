//go:build linux

package manager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"golang.org/x/sys/unix"
)

// applyPermsBeneath gives the entry at rel (relative to mountReal) the
// client-usable mode u+rwX,g+rwX,o+rX and, when uid > 0, the owner
// uid[:gid] (gid < 0 leaves the group). With recursive, it does the same
// for everything below a folder.
//
// It never follows a symlink, and it never resolves a path name twice:
//   - The mount is opened first. Every component of rel is then opened
//     with openat(O_NOFOLLOW) from the folder before it, so a component
//     that became a symlink after the caller's check fails with ELOOP
//     instead of leading outside the mount.
//   - Folders are changed through their own fd (fchmod, fchown) and
//     read through it. Other entries are opened O_PATH|O_NOFOLLOW, and
//     changed through that fd. A symlink found while recursing gets
//     only an lchown-style owner change, never a mode change, and is
//     never descended.
//
// It replaces `chmod -R` / `chown -R`: GNU chmod follows a symlink named
// on its command line, and the command line names a path that can be
// swapped between the check and the run. Errors on single entries do not
// stop the walk; the first one is returned.
func applyPermsBeneath(ctx context.Context, mountReal, rel string, uid, gid int, recursive bool) error {
	dirfd, err := unix.Open(mountReal, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open mount: %w", err)
	}
	comps := splitPathComponents(rel)
	if len(comps) == 0 {
		defer unix.Close(dirfd)
		w := permWalker{ctx: ctx, uid: uid, gid: gid, recursive: recursive}
		w.applyDir(dirfd)
		return w.first
	}
	for _, c := range comps[:len(comps)-1] {
		if c == ".." {
			unix.Close(dirfd)
			return errors.New("path climbs out of the mount")
		}
		next, err := unix.Openat(dirfd, c, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(dirfd)
		if err != nil {
			return fmt.Errorf("open %q beneath the mount: %w", c, err)
		}
		dirfd = next
	}
	defer unix.Close(dirfd)
	last := comps[len(comps)-1]
	if last == ".." {
		return errors.New("path climbs out of the mount")
	}
	w := permWalker{ctx: ctx, uid: uid, gid: gid, recursive: recursive, refuseLink: true}
	w.applyAt(dirfd, last)
	return w.first
}

type permWalker struct {
	ctx       context.Context
	uid, gid  int
	recursive bool
	// refuseLink: the named entry itself must not be a symlink (the
	// caller resolved it, so a symlink there was swapped in).
	refuseLink bool
	first      error
}

func (w *permWalker) fail(err error) {
	if w.first == nil {
		w.first = err
	}
}

// clientMode is u+rwX,g+rwX,o+rX applied to mode: X adds execute for a
// folder or an entry that already has an execute bit.
func clientMode(mode uint32, isDir bool) uint32 {
	m := mode&07777 | 0o664
	if isDir || mode&0o111 != 0 {
		m |= 0o111
	}
	return m
}

// applyAt handles the entry name inside the folder dirfd.
func (w *permWalker) applyAt(dirfd int, name string) {
	refuseLink := w.refuseLink
	w.refuseLink = false
	fd, err := unix.Openat(dirfd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err == nil {
		defer unix.Close(fd)
		w.applyDir(fd)
		return
	}
	if errors.Is(err, unix.ENOENT) {
		if refuseLink {
			w.fail(fmt.Errorf("%q does not exist", name))
		}
		return
	}
	if !errors.Is(err, unix.ENOTDIR) && !errors.Is(err, unix.ELOOP) {
		w.fail(fmt.Errorf("open %q: %w", name, err))
		return
	}
	// Not a folder: a file, a symlink or a special file. O_PATH does not
	// open the file itself (no device or FIFO side effects).
	pfd, err := unix.Openat(dirfd, name, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if !errors.Is(err, unix.ENOENT) || refuseLink {
			w.fail(fmt.Errorf("open %q: %w", name, err))
		}
		return
	}
	defer unix.Close(pfd)
	var st unix.Stat_t
	if err := unix.Fstat(pfd, &st); err != nil {
		w.fail(fmt.Errorf("stat %q: %w", name, err))
		return
	}
	if st.Mode&unix.S_IFMT == unix.S_IFLNK {
		if refuseLink {
			w.fail(fmt.Errorf("%q became a symlink; refusing to follow it", name))
			return
		}
		// Owner of the link itself; a symlink has no mode of its own.
		if w.uid > 0 {
			if err := unix.Fchownat(dirfd, name, w.uid, w.gid, unix.AT_SYMLINK_NOFOLLOW); err != nil && !errors.Is(err, unix.ENOENT) {
				w.fail(fmt.Errorf("chown link %q: %w", name, err))
			}
		}
		return
	}
	// chmod through the fd's /proc magic link: it names this exact inode,
	// so no path is resolved again. (fchmod does not accept O_PATH fds.)
	if err := unix.Chmod("/proc/self/fd/"+strconv.Itoa(pfd), clientMode(st.Mode, false)); err != nil {
		w.fail(fmt.Errorf("chmod %q: %w", name, err))
	}
	if w.uid > 0 {
		if err := unix.Fchownat(pfd, "", w.uid, w.gid, unix.AT_EMPTY_PATH); err != nil {
			w.fail(fmt.Errorf("chown %q: %w", name, err))
		}
	}
}

// applyDir changes the folder fd and, when recursive, its contents.
func (w *permWalker) applyDir(fd int) {
	if err := w.ctx.Err(); err != nil {
		w.fail(err)
		return
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		w.fail(fmt.Errorf("stat folder: %w", err))
		return
	}
	if err := unix.Fchmod(fd, clientMode(st.Mode, true)); err != nil {
		w.fail(fmt.Errorf("chmod folder: %w", err))
	}
	if w.uid > 0 {
		if err := unix.Fchown(fd, w.uid, w.gid); err != nil {
			w.fail(fmt.Errorf("chown folder: %w", err))
		}
	}
	if !w.recursive {
		return
	}
	// Read the entries through a duplicate of fd, so closing the
	// *os.File does not close fd (still needed for openat below).
	dup, err := unix.Dup(fd)
	if err != nil {
		w.fail(fmt.Errorf("dup folder fd: %w", err))
		return
	}
	unix.CloseOnExec(dup)
	f := os.NewFile(uintptr(dup), "dir")
	names, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		w.fail(fmt.Errorf("read folder: %w", err))
	}
	for _, n := range names {
		if w.ctx.Err() != nil {
			w.fail(w.ctx.Err())
			return
		}
		w.applyAt(fd, n)
	}
}

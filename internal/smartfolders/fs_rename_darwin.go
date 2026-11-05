package smartfolders

import (
	"fmt"
	"io/fs"

	"golang.org/x/sys/unix"
)

type renameOp int

const (
	renameNoReplace renameOp = iota
	renameExchange
)

func renameAt(dirfd int, from, to string, op renameOp) error {
	flag := uint32(unix.RENAME_EXCL)
	if op == renameExchange {
		flag = unix.RENAME_SWAP
	}
	return unix.RenameatxNp(dirfd, from, dirfd, to, flag)
}

// dirHandleOp opens the directory leaf in dirfd without following a
// symlink, reads its identity from the handle and, when perm is set,
// changes its mode through the handle, but only when it is the directory
// with inode wantIno (0: any).
func dirHandleOp(dirfd int, leaf string, wantIno uint64, perm fs.FileMode) (Info, error) {
	fd, err := unix.Openat(dirfd, leaf, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return Info{}, err
	}
	defer unix.Close(fd)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return Info{}, err
	}
	in := Info{Type: TypeDir, Ino: uint64(st.Ino), Perm: fs.FileMode(uint32(st.Mode) & 0o777)}
	if wantIno != 0 && in.Ino != wantIno {
		return in, fmt.Errorf("%s is no longer the directory the Manager created", leaf)
	}
	if perm != 0 && in.Perm != perm {
		if err := unix.Fchmod(fd, uint32(perm)); err != nil {
			return in, err
		}
		in.Perm = perm
	}
	return in, nil
}

// rmdirAt removes the directory leaf in dirfd; a non-directory is ENOTDIR.
func rmdirAt(dirfd int, leaf string) error {
	return unix.Unlinkat(dirfd, leaf, unix.AT_REMOVEDIR)
}

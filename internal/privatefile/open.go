// Package privatefile opens a leaf relative to an already verified directory
// descriptor. os.Root may resolve in-root symlinks even with O_NOFOLLOW; locks
// and custody records must reject those links at the actual open syscall.
package privatefile

import (
	"errors"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

func OpenAt(dir *os.File, leaf string, flags int, mode os.FileMode) (*os.File, error) {
	if dir == nil || leaf == "" || leaf == "." || leaf == ".." || strings.ContainsAny(leaf, "/\x00") {
		return nil, errors.New("private file requires one leaf in a pinned directory")
	}
	fd, err := unix.Openat(int(dir.Fd()), leaf, flags|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, uint32(mode.Perm()))
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), leaf), nil
}

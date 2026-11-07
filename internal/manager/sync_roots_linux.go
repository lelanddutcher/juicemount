//go:build linux

package manager

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// openRootBeneath opens anchor/rel as a folder (O_PATH) without resolving
// anything outside anchor: rel is resolved with openat2(RESOLVE_BENEATH |
// RESOLVE_NO_MAGICLINKS) from an fd on anchor, so a symlink swapped into
// rel fails with EXDEV instead of leading outside. With create, missing
// folders are made one level at a time with mkdirat beneath anchor.
func openRootBeneath(anchor, rel string, create bool) (*os.File, error) {
	afd, err := unix.Open(anchor, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open anchor: %w", err)
	}
	defer unix.Close(afd)
	how := func() *unix.OpenHow {
		return &unix.OpenHow{
			Flags:   unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC,
			Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS,
		}
	}
	at := func(p string) (int, error) {
		if p == "" {
			p = "."
		}
		for i := 0; ; i++ {
			fd, err := unix.Openat2(afd, p, how())
			if (errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR)) && i < 64 {
				continue
			}
			if errors.Is(err, unix.EXDEV) {
				return -1, errRootNotBeneath
			}
			return fd, err
		}
	}
	if create && rel != "" {
		parts := strings.Split(rel, "/")
		for i := range parts {
			cur := strings.Join(parts[:i+1], "/")
			fd, err := at(cur)
			if err == nil {
				unix.Close(fd)
				continue
			}
			if !errors.Is(err, unix.ENOENT) {
				return nil, err
			}
			pfd, err := at(strings.Join(parts[:i], "/"))
			if err != nil {
				return nil, err
			}
			err = unix.Mkdirat(pfd, parts[i], 0o777)
			unix.Close(pfd)
			if err != nil && !errors.Is(err, unix.EEXIST) {
				return nil, fmt.Errorf("mkdir %q: %w", cur, err)
			}
		}
	}
	fd, err := at(rel)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), filepath.Join(anchor, rel)), nil
}

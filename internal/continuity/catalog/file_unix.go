//go:build darwin || linux

package catalog

import (
	"fmt"
	"os"
	"syscall"
)

func checkFileIdentity(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || stat.Nlink != 1 {
		return corrupt("catalog files must be single-link regular files")
	}
	return nil
}

func openRegular(path string, create bool) (*os.File, error) {
	flags := os.O_RDWR | syscall.O_NOFOLLOW
	if create {
		flags |= os.O_CREATE | os.O_EXCL
	}
	f, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, err
	}
	if create {
		if err := f.Chmod(0600); err != nil {
			f.Close()
			return nil, err
		}
	}
	info, err := f.Stat()
	if err == nil {
		err = checkFileIdentity(info)
	}
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("catalog file: %w", err)
	}
	return f, nil
}

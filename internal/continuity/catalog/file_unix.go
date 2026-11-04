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
	if info.Mode().Perm()&0077 != 0 {
		return corrupt("catalog files must not grant group or other access")
	}
	return nil
}

// checkPrivateDirectory enforces the caller-provisioned private directory:
// another account able to write there could plant or swap WAL sidecars.
func checkPrivateDirectory(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("catalog parent: %w", err)
	}
	if !info.IsDir() || info.Mode().Perm()&0022 != 0 {
		return invalid("catalog directory must not be writable by group or others")
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

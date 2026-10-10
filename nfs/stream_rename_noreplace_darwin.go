//go:build darwin

package nfs

import "golang.org/x/sys/unix"

func renameStreamNoReplace(oldPath, newPath string) error {
	return unix.RenamexNp(oldPath, newPath, unix.RENAME_EXCL)
}

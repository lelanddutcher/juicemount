//go:build darwin

package privatefile

import "golang.org/x/sys/unix"

func renameNoReplace(fromFD int,from string,toFD int,to string) error {
    return unix.RenameatxNp(fromFD,from,toFD,to,unix.RENAME_EXCL)
}

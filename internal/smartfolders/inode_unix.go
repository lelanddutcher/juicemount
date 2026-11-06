//go:build unix

package smartfolders

import (
	"io/fs"
	"syscall"
)

func inodeOf(fi fs.FileInfo) uint64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Ino)
	}
	return 0
}

const osfsSupported = true

//go:build darwin

package farm

import (
	"io/fs"
	"syscall"
	"time"
)

// changeTime is the inode change time (ctime), or the mtime when the platform
// gives no stat record.
func changeTime(info fs.FileInfo) time.Time {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return time.Unix(st.Ctimespec.Sec, st.Ctimespec.Nsec)
	}
	return info.ModTime()
}

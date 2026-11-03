//go:build !darwin && !linux

package farm

import (
	"io/fs"
	"time"
)

// changeTime falls back to the mtime where no ctime is available.
func changeTime(info fs.FileInfo) time.Time {
	return info.ModTime()
}

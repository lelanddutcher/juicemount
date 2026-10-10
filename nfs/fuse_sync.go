package nfs

import "os"

// fuseDurableFile gives the spool drainer a platform-specific durability
// primitive without changing the ordinary *os.File write path.
//
// The Darwin implementation deliberately uses fsync(2), not os.File.Sync's
// F_FULLFSYNC. See fuse_sync_darwin.go.
type fuseDurableFile struct {
	*os.File
}

func (f fuseDurableFile) Sync() error {
	return syncFUSEFile(f.File)
}

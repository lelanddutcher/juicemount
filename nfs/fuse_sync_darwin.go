//go:build darwin

package nfs

import (
	"errors"
	"os"
	"syscall"
)

// syncFUSEFile invokes the ordinary fsync(2) syscall on a JuiceFS/macFUSE
// destination. os.File.Sync cannot be used here on Darwin: Go implements it as
// fcntl(F_FULLFSYNC), a local-physical-disk durability extension that has no
// additional meaning for FUSE and has repeatedly blocked inside macFUSE for
// the daemon timeout. fsync is the operation the FUSE protocol exposes, and
// JuiceFS --writeback is disabled, so a successful call remains the drainer's
// durable backend checkpoint.
func syncFUSEFile(f *os.File) error {
	if f == nil {
		return errors.New("sync fuse file: nil file")
	}
	return syscall.Fsync(int(f.Fd()))
}

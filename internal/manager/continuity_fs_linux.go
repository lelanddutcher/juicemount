package manager

import (
	"fmt"
	"syscall"
)

const fuseSuperMagic = 0x65735546

// refuseFUSEDir refuses a catalog directory on a FUSE filesystem: the
// JuiceFS mount is one, and the catalog must never live in the volume.
func refuseFUSEDir(fd int, dir string) error {
	var st syscall.Statfs_t
	if err := syscall.Fstatfs(fd, &st); err != nil {
		return fmt.Errorf("catalog directory: %w", err)
	}
	if st.Type == fuseSuperMagic {
		return fmt.Errorf("the catalog directory %s is on a FUSE filesystem (the volume?); it must be local", dir)
	}
	return nil
}

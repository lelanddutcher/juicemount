package nfscustody

import "golang.org/x/sys/unix"

// Linux has a numeric filesystem type rather than Darwin's Fstypename.
// Admit only known local disk filesystems on the same parent filesystem.
func localTargetFilesystem(parent, leaf unix.Statfs_t) bool {
	if leaf.Fsid != parent.Fsid || leaf.Type != parent.Type {
		return false
	}
	switch leaf.Type {
	case unix.EXT4_SUPER_MAGIC, unix.XFS_SUPER_MAGIC, unix.BTRFS_SUPER_MAGIC:
		return true
	default:
		return false
	}
}

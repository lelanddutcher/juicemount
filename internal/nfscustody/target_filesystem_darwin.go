package nfscustody

import "golang.org/x/sys/unix"

// Preserve the macOS target guard: same parent filesystem, APFS or HFS only.
func localTargetFilesystem(parent, leaf unix.Statfs_t) bool {
	fsType := unix.ByteSliceToString(leaf.Fstypename[:])
	return leaf.Fsid == parent.Fsid && (fsType == "apfs" || fsType == "hfs")
}

package nfscustody

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestLinuxLocalTargetFilesystemRequiresSameParentAndKnownDiskType(t *testing.T) {
	for _, kind := range []int64{unix.EXT4_SUPER_MAGIC, unix.XFS_SUPER_MAGIC, unix.BTRFS_SUPER_MAGIC} {
		parent := unix.Statfs_t{Type: kind, Fsid: unix.Fsid{Val: [2]int32{1, 2}}}
		if !localTargetFilesystem(parent, parent) {
			t.Fatalf("local filesystem type %#x rejected", kind)
		}
		otherFSID := parent
		otherFSID.Fsid.Val[0]++
		if localTargetFilesystem(parent, otherFSID) {
			t.Fatal("different parent filesystem admitted")
		}
		otherType := parent
		otherType.Type = unix.NFS_SUPER_MAGIC
		if localTargetFilesystem(parent, otherType) {
			t.Fatal("different leaf filesystem type admitted")
		}
	}
	for _, kind := range []int64{unix.NFS_SUPER_MAGIC, unix.TMPFS_MAGIC, 0} {
		fs := unix.Statfs_t{Type: kind, Fsid: unix.Fsid{Val: [2]int32{1, 2}}}
		if localTargetFilesystem(fs, fs) {
			t.Fatalf("unsupported filesystem type %#x admitted", kind)
		}
	}
}

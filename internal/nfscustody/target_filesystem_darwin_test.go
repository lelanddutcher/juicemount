package nfscustody

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestDarwinLocalTargetFilesystemPreservesAPFSAndHFSGuard(t *testing.T) {
	for _, kind := range []string{"apfs", "hfs"} {
		var fs unix.Statfs_t
		copy(fs.Fstypename[:], kind)
		fs.Fsid.Val = [2]int32{1, 2}
		if !localTargetFilesystem(fs, fs) {
			t.Fatalf("local %s filesystem rejected", kind)
		}
		other := fs
		other.Fsid.Val[0]++
		if localTargetFilesystem(fs, other) {
			t.Fatal("different parent filesystem admitted")
		}
	}
	for _, kind := range []string{"nfs", "smbfs", ""} {
		var fs unix.Statfs_t
		copy(fs.Fstypename[:], kind)
		if localTargetFilesystem(fs, fs) {
			t.Fatalf("unsupported filesystem %q admitted", kind)
		}
	}
}

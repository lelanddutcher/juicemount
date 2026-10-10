//go:build darwin && cgo

package unmountd

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// The ctlbyfsid ABI is pinned from the SDK header without executing it:
// struct vfsidctl { int vc_vers; fsid_t vc_fsid; void *vc_ptr; size_t vc_len;
// u_int32_t vc_spare[12]; } must match the kernel's user64_vfsidctl, which
// sysctl_vfs_ctlbyfsid reads with SYSCTL_IN(sizeof(vc.vc64)).
func TestVfsidctlLayoutMatchesKernelABI(t *testing.T) {
	size, vers, fsid, ptr, ln := vfsidctlLayout()
	if size != 80 || vers != 0 || fsid != 4 || ptr != 16 || ln != 24 {
		t.Fatalf("vfsidctl layout size=%d vers@%d fsid@%d ptr@%d len@%d, want 80/0/4/16/24", size, vers, fsid, ptr, ln)
	}
	v1, um, nb, force := sdkConstants()
	if v1 != 1 || um != 0x00010002 || nb != FlagNoBlock || force != FlagForce {
		t.Fatalf("SDK constants vers1=%#x VFS_CTL_UMOUNT=%#x noblock=%#x force=%#x", v1, um, nb, force)
	}
}

// Resolving the OID name is read-only; it proves the node exists on this OS.
func TestCtlByFSIDNodeResolves(t *testing.T) {
	mib, err := ctlbyfsidMIB()
	if err != nil {
		t.Fatalf("vfs.generic.ctlbyfsid does not resolve: %v", err)
	}
	if len(mib) < 4 || mib[0] != 3 /* CTL_VFS */ || mib[1] != 0 /* VFS_GENERIC */ || mib[len(mib)-1] != 0x00010002 {
		t.Fatalf("mib %v", mib)
	}
}

// Real libproc lister, restricted to this test process (no other process's
// descriptors are read, so no mounted volume is stat'ed).
func TestProcHoldersFiltersByUID(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "held"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var sfs unix.Statfs_t
	if err := unix.Fstatfs(int(f.Fd()), &sfs); err != nil {
		t.Fatal(err)
	}
	self := ProcHolders{PIDs: func() ([]int32, error) { return []int32{int32(os.Getpid())}, nil }}
	uid := uint32(os.Geteuid())
	hs, err := self.Holders(uid, FSID(sfs.Fsid.Val))
	if err != nil || len(hs) != 1 || hs[0].PID != int32(os.Getpid()) || hs[0].Files < 1 {
		t.Fatalf("own holders %+v %v", hs, err)
	}
	hs, err = self.Holders(uid+1, FSID(sfs.Fsid.Val))
	if err != nil || len(hs) != 0 {
		t.Fatalf("another uid's lister saw this process: %+v %v", hs, err)
	}
}

// H3 L0 M2: an O_EVTONLY descriptor (what our custodians hold on the mount
// root) is not a holder; a normal descriptor on the same fsid is.
func TestProcHoldersSkipsEventOnlyDescriptors(t *testing.T) {
	dir := t.TempDir()
	var sfs unix.Statfs_t
	if err := unix.Statfs(dir, &sfs); err != nil {
		t.Fatal(err)
	}
	self := ProcHolders{PIDs: func() ([]int32, error) { return []int32{int32(os.Getpid())}, nil }}
	uid := uint32(os.Geteuid())
	files := func() int {
		hs, err := self.Holders(uid, FSID(sfs.Fsid.Val))
		if err != nil {
			t.Fatal(err)
		}
		if len(hs) == 0 {
			return 0
		}
		return hs[0].Files
	}
	before := files()
	var evt []int
	for i := 0; i < 3; i++ {
		fd, err := unix.Open(dir, unix.O_EVTONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if err != nil {
			t.Fatal(err)
		}
		evt = append(evt, fd)
	}
	defer func() {
		for _, fd := range evt {
			unix.Close(fd)
		}
	}()
	if got := files(); got != before {
		t.Fatalf("O_EVTONLY descriptors counted as holders: %d -> %d", before, got)
	}
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if got := files(); got != before+1 {
		t.Fatalf("an ordinary descriptor was not counted: %d -> %d", before, got)
	}
}

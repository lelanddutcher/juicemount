package unmountd

// Kernel is the helper's entire kernel surface. The darwin implementation is
// kernel_darwin.go; tests use fakes. Nothing here accepts a path.
type Kernel interface {
	// Snapshot returns kern.bootsessionuuid plus a complete
	// getfsstat(MNT_NOWAIT) table including f_owner. An incomplete table is an
	// error, never a short snapshot.
	Snapshot() (Snapshot, error)

	// UnmountByFSID writes sysctl vfs.generic.ctlbyfsid.<VFS_CTL_UMOUNT> with
	// struct vfsidctl{vc_vers=VFS_CTL_VERS1, vc_fsid, vc_ptr=&flags,
	// vc_len=sizeof(int)}. The kernel resolves the fsid atomically
	// (mount_list_lookupby_fsid) and calls safedounmount. Root only.
	// Errors are syscall.Errno (EBUSY, ENOENT, EPERM, ...).
	UnmountByFSID(fsid FSID, flags int) error
}

// HolderLister names processes with open files on an fsid (proc_pidinfo).
// Implementations must inspect only processes whose uid equals uid; the core
// filters the result again regardless.
type HolderLister interface {
	Holders(uid uint32, fsid FSID) ([]Holder, error)
}

func filterHolders(uid uint32, hs []Holder) []Holder {
	var out []Holder
	for _, h := range hs {
		if h.UID == uid {
			out = append(out, h)
		}
	}
	return out
}

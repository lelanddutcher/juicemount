//go:build darwin && cgo

package unmountd

/*
#cgo CFLAGS: -mmacosx-version-min=26.0
#cgo LDFLAGS: -mmacosx-version-min=26.0
#include <sys/types.h>
#include <sys/sysctl.h>
#include <sys/mount.h>
#include <libproc.h>
#include <sys/proc_info.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

// jm_ctlbyfsid_mib resolves vfs.generic.ctlbyfsid (an OID_AUTO node, so its
// number is only known at runtime) and appends VFS_CTL_UMOUNT, which the
// handler reads as name[0] (vfs_subr.c sysctl_vfs_ctlbyfsid).
static int jm_ctlbyfsid_mib(int *mib, size_t *len) {
	size_t n = CTL_MAXNAME - 1;
	if (sysctlnametomib("vfs.generic.ctlbyfsid", mib, &n) != 0) return errno;
	mib[n] = VFS_CTL_UMOUNT;
	*len = n + 1;
	return 0;
}

// jm_ctlbyfsid_umount is the helper's only mutating syscall:
//   sysctl(vfs.generic.ctlbyfsid.VFS_CTL_UMOUNT, new = struct vfsidctl{
//       vc_vers = VFS_CTL_VERS1, vc_fsid, vc_ptr = &flags, vc_len = sizeof(int) })
// The kernel resolves the fsid with mount_list_lookupby_fsid and calls
// safedounmount(mp, flags). The OID is CTLFLAG_RW without CTLFLAG_ANYBODY,
// so a write needs root (kern_newsysctl.c proc_suser).
static int jm_ctlbyfsid_umount(int32_t v0, int32_t v1, int flags) {
	int mib[CTL_MAXNAME];
	size_t len = 0;
	int err = jm_ctlbyfsid_mib(mib, &len);
	if (err != 0) return err;
	struct vfsidctl vc;
	memset(&vc, 0, sizeof(vc));
	vc.vc_vers = VFS_CTL_VERS1;
	vc.vc_fsid.val[0] = v0;
	vc.vc_fsid.val[1] = v1;
	vc.vc_ptr = &flags;
	vc.vc_len = sizeof(flags);
	if (sysctl(mib, (u_int)len, NULL, NULL, &vc, sizeof(vc)) != 0) return errno;
	return 0;
}

static size_t jm_vfsidctl_size(void)      { return sizeof(struct vfsidctl); }
static size_t jm_vfsidctl_off_vers(void)  { return offsetof(struct vfsidctl, vc_vers); }
static size_t jm_vfsidctl_off_fsid(void)  { return offsetof(struct vfsidctl, vc_fsid); }
static size_t jm_vfsidctl_off_ptr(void)   { return offsetof(struct vfsidctl, vc_ptr); }
static size_t jm_vfsidctl_off_len(void)   { return offsetof(struct vfsidctl, vc_len); }
static int    jm_vfs_ctl_vers1(void)      { return VFS_CTL_VERS1; }
static int    jm_vfs_ctl_umount(void)     { return VFS_CTL_UMOUNT; }
static int    jm_mnt_noblock(void)        { return MNT_NOBLOCK; }
static int    jm_mnt_force(void)          { return MNT_FORCE; }

// jm_proc_identity reads a process's effective uid and short name.
// Returns 0 on success, -1 if the process is gone or inaccessible.
static int jm_proc_identity(int pid, uint32_t *uid, char *name, int namelen) {
	struct proc_bsdinfo bsd;
	int n = proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &bsd, sizeof(bsd));
	if (n != (int)sizeof(bsd)) return -1;
	*uid = bsd.pbi_uid;
	strlcpy(name, bsd.pbi_name[0] ? bsd.pbi_name : bsd.pbi_comm, (size_t)namelen);
	return 0;
}

// jm_proc_fsid_fds counts pid's vnode descriptors on fsid. Read-only.
// Descriptors opened O_EVTONLY are skipped: they never make a volume busy
// (H3 L0 probe 3: the detach succeeds with one open), and our own custodians
// (juicemount-mount-probe, the FUSE keeper) hold exactly one such root
// descriptor. Without this the busy-Quit panel would name our own custodian
// as the app keeping the volume busy (probe M2). The kernel reports the
// O_EVTONLY bit in fi_openflags (verified on macOS 26).
static int jm_proc_fsid_fds(int pid, int32_t v0, int32_t v1) {
	int bytes = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, NULL, 0);
	if (bytes <= 0) return -1;
	bytes += 32 * (int)sizeof(struct proc_fdinfo);
	struct proc_fdinfo *fds = malloc((size_t)bytes);
	if (fds == NULL) return -1;
	bytes = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, fds, bytes);
	if (bytes <= 0) { free(fds); return -1; }
	int count = 0, nfds = bytes / (int)sizeof(struct proc_fdinfo);
	for (int i = 0; i < nfds; i++) {
		if (fds[i].proc_fdtype != PROX_FDTYPE_VNODE) continue;
		struct vnode_fdinfo vi;
		int r = proc_pidfdinfo(pid, fds[i].proc_fd, PROC_PIDFDVNODEINFO, &vi, sizeof(vi));
		if (r != (int)sizeof(vi)) continue;
		if (vi.pfi.fi_openflags & O_EVTONLY) continue;
		if (vi.pvi.vi_fsid.val[0] == v0 && vi.pvi.vi_fsid.val[1] == v1) count++;
	}
	free(fds);
	return count;
}

static int jm_list_pids(int *buf, int cap) {
	return proc_listallpids(buf, cap * (int)sizeof(int));
}
*/
import "C"

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	maxSnapshotEntries    = 8192
	maxSnapshotRetries    = 8
	snapshotCapacitySlack = 16
)

// DarwinKernel is the real kernel surface. It must run as root for
// UnmountByFSID. It is never exercised by unit tests: live proof needs a
// disposable mount (lane L0).
type DarwinKernel struct{}

func cString(raw []byte) string {
	if i := bytes.IndexByte(raw, 0); i >= 0 {
		raw = raw[:i]
	}
	return string(raw)
}

// Snapshot captures kern.bootsessionuuid and getfsstat(MNT_NOWAIT) with
// f_owner. MNT_NOWAIT is not a deadline (internal/mounttable notes a live
// park in the kernel); Service.snapshot bounds and single-flights this call.
func (DarwinKernel) Snapshot() (Snapshot, error) {
	boot, err := unix.Sysctl("kern.bootsessionuuid")
	if err != nil {
		return Snapshot{}, fmt.Errorf("unmountd snapshot: boot session: %w", err)
	}
	boot = strings.TrimSpace(strings.Trim(boot, "\x00"))
	if boot == "" {
		return Snapshot{}, errors.New("unmountd snapshot: empty boot session")
	}
	count, err := unix.Getfsstat(nil, unix.MNT_NOWAIT)
	if err != nil {
		return Snapshot{}, fmt.Errorf("unmountd snapshot: count: %w", err)
	}
	if count <= 0 || count > maxSnapshotEntries-snapshotCapacitySlack {
		return Snapshot{}, fmt.Errorf("unmountd snapshot: invalid mount count %d", count)
	}
	capacity := count + snapshotCapacitySlack
	for attempt := 0; attempt < maxSnapshotRetries && capacity <= maxSnapshotEntries; attempt++ {
		rows := make([]unix.Statfs_t, capacity)
		n, err := unix.Getfsstat(rows, unix.MNT_NOWAIT)
		if err != nil {
			return Snapshot{}, fmt.Errorf("unmountd snapshot: capture: %w", err)
		}
		if n <= 0 {
			return Snapshot{}, fmt.Errorf("unmountd snapshot: invalid returned count %d", n)
		}
		if n >= len(rows) { // table may have grown: retry larger
			capacity *= 2
			continue
		}
		snap := Snapshot{Boot: boot, Entries: make([]Entry, 0, n)}
		for i, row := range rows[:n] {
			e := Entry{
				FSID: FSID(row.Fsid.Val), Owner: row.Owner, Type: cString(row.Fstypename[:]),
				MntOnName: cString(row.Mntonname[:]), MntFromName: cString(row.Mntfromname[:]),
			}
			if e.MntOnName == "" || e.MntFromName == "" || e.Type == "" {
				return Snapshot{}, fmt.Errorf("unmountd snapshot: incomplete kernel entry %d", i)
			}
			snap.Entries = append(snap.Entries, e)
		}
		return snap, nil
	}
	return Snapshot{}, errors.New("unmountd snapshot: mount table grew beyond bounded capacity")
}

// UnmountByFSID issues the ctlbyfsid unmount. Only 0, MNT_NOBLOCK and
// MNT_FORCE are accepted (design §2).
func (DarwinKernel) UnmountByFSID(fsid FSID, flags int) error {
	if flags != 0 && flags != FlagNoBlock && flags != FlagForce {
		return fmt.Errorf("unmountd: flags %#x not in {0, MNT_NOBLOCK, MNT_FORCE}", flags)
	}
	if fsid.IsZero() {
		return syscall.EINVAL
	}
	if e := C.jm_ctlbyfsid_umount(C.int32_t(fsid[0]), C.int32_t(fsid[1]), C.int(flags)); e != 0 {
		return syscall.Errno(e)
	}
	return nil
}

// vfsidctlLayout exposes the SDK's struct layout so a test can pin it
// without executing the syscall.
func vfsidctlLayout() (size, offVers, offFSID, offPtr, offLen uintptr) {
	return uintptr(C.jm_vfsidctl_size()), uintptr(C.jm_vfsidctl_off_vers()), uintptr(C.jm_vfsidctl_off_fsid()),
		uintptr(C.jm_vfsidctl_off_ptr()), uintptr(C.jm_vfsidctl_off_len())
}

func sdkConstants() (vers1, ctlUnmountOp, noblock, force int) {
	return int(C.jm_vfs_ctl_vers1()), int(C.jm_vfs_ctl_umount()), int(C.jm_mnt_noblock()), int(C.jm_mnt_force())
}

// ctlbyfsidMIB resolves the OID without writing to it.
func ctlbyfsidMIB() ([]int32, error) {
	var mib [C.CTL_MAXNAME]C.int
	var n C.size_t
	if e := C.jm_ctlbyfsid_mib(&mib[0], &n); e != 0 {
		return nil, syscall.Errno(e)
	}
	out := make([]int32, int(n))
	for i := range out {
		out[i] = int32(mib[i])
	}
	return out, nil
}

// ProcHolders names processes with descriptors on an fsid via libproc. It
// inspects descriptors only of processes whose effective uid equals the
// caller's uid; others are skipped before any descriptor is read.
//
// Caveat for the live lane: PROC_PIDFDVNODEINFO fills vinfo_stat through
// vn_stat, a VNOP_GETATTR, which can block on a wedged volume. Service bounds
// the call with SnapshotWatch and drops a late answer.
type ProcHolders struct {
	// PIDs overrides process enumeration (tests). Nil lists all processes.
	PIDs func() ([]int32, error)
}

func allPIDs() ([]int32, error) {
	n := int(C.jm_list_pids(nil, 0))
	if n <= 0 {
		return nil, errors.New("unmountd: proc_listallpids failed")
	}
	capacity := n + 64
	buf := make([]C.int, capacity)
	got := int(C.jm_list_pids(&buf[0], C.int(capacity)))
	if got <= 0 {
		return nil, errors.New("unmountd: proc_listallpids failed")
	}
	if got > capacity {
		got = capacity
	}
	out := make([]int32, 0, got)
	for _, p := range buf[:got] {
		if p > 0 {
			out = append(out, int32(p))
		}
	}
	return out, nil
}

// Holders implements HolderLister.
func (h ProcHolders) Holders(uid uint32, fsid FSID) ([]Holder, error) {
	list := h.PIDs
	if list == nil {
		list = allPIDs
	}
	pids, err := list()
	if err != nil {
		return nil, err
	}
	var out []Holder
	var name [64]C.char
	for _, pid := range pids {
		var puid C.uint32_t
		if C.jm_proc_identity(C.int(pid), &puid, &name[0], C.int(len(name))) != 0 {
			continue
		}
		if uint32(puid) != uid {
			continue // never read another user's descriptors
		}
		n := int(C.jm_proc_fsid_fds(C.int(pid), C.int32_t(fsid[0]), C.int32_t(fsid[1])))
		if n > 0 {
			out = append(out, Holder{PID: pid, UID: uint32(puid), Name: C.GoString(&name[0]), Files: n})
		}
	}
	return out, nil
}

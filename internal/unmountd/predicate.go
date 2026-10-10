package unmountd

import (
	"fmt"
	"strings"
)

// VerdictKind is the outcome of the authority predicate on one snapshot.
type VerdictKind int

const (
	// VerdictAct: exactly our instance is present and the caller may act on it.
	VerdictAct VerdictKind = iota
	// VerdictAbsent: no entry carries the receipt's fsid. The instance is gone.
	// (A replacement at the same path has a different fsid and lands here.)
	VerdictAbsent
	// VerdictVoid: the boot changed. Kernel mounts do not survive a reboot.
	VerdictVoid
	// VerdictRefused: something carries the fsid but is not provably ours.
	// No syscall may be issued on this snapshot.
	VerdictRefused
)

func (k VerdictKind) String() string {
	return [...]string{"act", "absent", "void", "refused"}[k]
}

// Verdict carries the predicate result plus the descendants seen under our
// mount point. Descendants never block an ordinary attempt (the kernel
// returns EBUSY by itself) but always block MNT_FORCE, whose recursion in
// dounmount_submounts would remove them (vfs_syscalls.c:2759,2987).
type Verdict struct {
	Kind        VerdictKind
	Reason      string
	Entry       Entry
	Descendants []Entry
}

// Verify is the authority predicate (design §2). It must be evaluated on a
// fresh snapshot immediately before each unmount syscall. It returns
// VerdictAct only when ALL of these hold:
//
//  1. the snapshot is from the receipt's boot;
//  2. the caller's uid is the receipt's uid;
//  3. exactly one entry has f_fsid == receipt fsid;
//  4. that entry has f_owner == caller uid (the safety bound: the kernel
//     already lets the owner unmount its own mount, vfs_syscalls.c:2658);
//  5. that entry has the receipt's fstypename, mntonname and mntfromname,
//     and those strings carry the receipt's random token;
//  6. for NFS only: no other entry has the same mntfromname. The NFS source
//     carries our token (127.0.0.1:/jm-<token>), so a duplicate means an
//     instance we cannot tell apart. The macFUSE source is "JuiceFS:<volume
//     name>": it is the same for every generation and every installation
//     (H3 L0 probe M1: identical in 12/12 generations), so a foreign mount
//     of a same-named volume would make the helper refuse to retire ours
//     exactly when it is needed. macFUSE authority rests on the token- and
//     generation-bearing mntonname plus fsid and owner (rules 3-5).
//
// The receipt's strings are the KERNEL's f_mntonname/f_mntfromname, never a
// path rebuilt from configuration: with a symlinked mount point the kernel
// records the resolved target path (H3 L0 probe M3), and a configured path
// would never match.
//
// The fsid is treated as reusable (macFUSE `-o fsid=` lets a same-user
// process claim one). Safety rests on owner + token + receipt, never on the
// fsid counter: an fsid hit with any other string or owner is a refusal.
func Verify(r Receipt, caller Caller, snap Snapshot) Verdict {
	if snap.Boot != r.Boot {
		return Verdict{Kind: VerdictVoid, Reason: "boot changed"}
	}
	if caller.UID != r.UID {
		return Verdict{Kind: VerdictRefused, Reason: fmt.Sprintf("caller uid %d is not receipt uid %d", caller.UID, r.UID)}
	}
	var hits []Entry
	for _, e := range snap.Entries {
		if e.FSID == r.FSID {
			hits = append(hits, e)
		}
	}
	switch {
	case len(hits) == 0:
		return Verdict{Kind: VerdictAbsent, Reason: "fsid absent"}
	case len(hits) > 1:
		return Verdict{Kind: VerdictRefused, Reason: fmt.Sprintf("fsid %s is ambiguous (%d entries)", r.FSID, len(hits))}
	}
	e := hits[0]
	refuse := func(why string) Verdict { return Verdict{Kind: VerdictRefused, Reason: why, Entry: e} }
	if e.Owner != caller.UID {
		return refuse(fmt.Sprintf("f_owner %d is not caller uid %d", e.Owner, caller.UID))
	}
	if e.Type != string(r.Type) {
		return refuse(fmt.Sprintf("fstypename %q is not %q", e.Type, r.Type))
	}
	if e.MntOnName != r.MntOnName || e.MntFromName != r.MntFromName {
		return refuse("mount strings differ from the receipt")
	}
	if !tokenCarried(r.Type, r.Token, r.Generation, e.MntOnName, e.MntFromName) {
		return refuse("mount strings do not carry our token")
	}
	var desc []Entry
	prefix := strings.ToLower(r.MntOnName) + "/"
	for _, o := range snap.Entries {
		if o.FSID == r.FSID {
			continue
		}
		if r.Type == TypeNFS && o.MntFromName == r.MntFromName {
			return refuse("another entry has our mount source")
		}
		// Case-folded: the default APFS volume is case-insensitive, so a
		// differently-cased path can still be inside our mount.
		if strings.HasPrefix(strings.ToLower(o.MntOnName), prefix) {
			desc = append(desc, o)
		}
	}
	return Verdict{Kind: VerdictAct, Entry: e, Descendants: desc}
}

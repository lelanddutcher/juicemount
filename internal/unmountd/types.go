// Package unmountd is the pure core of the privileged unmount helper
// (juicemount-unmountd). It owns three things:
//
//   - a receipt store: root-owned, per-uid records of the kernel mounts the
//     JuiceMount service created (receipt.go);
//   - the authority predicate, evaluated on a fresh mount-table snapshot
//     immediately before every unmount syscall (predicate.go);
//   - the convergence state machine that drives one receipt to RETIRED
//     (converge.go, service.go).
//
// The helper's whole power is the root-only sysctl vfs.generic.ctlbyfsid with
// VFS_CTL_UMOUNT. That call resolves a mount by fsid, never by path. Nothing in
// this package or in cmd/juicemount-unmountd may take a path to unmount; a grep
// gate test enforces that (grepgate_test.go).
//
// An fsid is NOT an identity. macFUSE `-o fsid=` lets any same-user process
// claim a chosen fsid, and the kernel counter wraps. Authority rests on
// owner + token + receipt, all re-checked on a fresh snapshot each time.
//
// The transport (XPC, peer code-signing checks) and packaging live in other
// lanes. They construct a Caller from the XPC connection and call Service.
//
// Quit policy ("force-eject after 30 s"). The caller implements it by calling
// Retire(EjectNow) after 30 s; that joins the running job and EjectNow is
// sticky. Two limits the caller must surface to the user:
//
//   - 30 s is a floor, not a deadline. If an ordinary unmount call is still in
//     the kernel (IN_KERNEL), nothing more can be issued for that fsid until
//     it returns (MNT_LUNMOUNT makes even MNT_FORCE return EBUSY), so the
//     force waits for that call.
//   - A foreign mount nested inside ours blocks EjectNow: MNT_FORCE would
//     recursively remove it, so the job sits in WAIT_NESTED and Outcome
//     lists the Descendants. The user must eject the nested volume.
package unmountd

import (
	"errors"
	"fmt"
	"time"
)

// FSID is the kernel fsid_t: { int32_t val[2]; }.
type FSID [2]int32

func (f FSID) String() string { return fmt.Sprintf("%08x:%08x", uint32(f[0]), uint32(f[1])) }

// IsZero reports the all-zero fsid, which never names a JuiceMount mount.
func (f FSID) IsZero() bool { return f == FSID{} }

// MountType is the kernel f_fstypename of a receipt's mount.
type MountType string

const (
	TypeNFS     MountType = "nfs"
	TypeMacFUSE MountType = "macfuse"
)

func (t MountType) valid() bool { return t == TypeNFS || t == TypeMacFUSE }

// Kernel unmount flags. These are the only flags the helper may pass
// (design §2: flags ⊂ {0, MNT_NOBLOCK, MNT_FORCE}). Values are from
// <sys/mount.h>.
const (
	FlagNoBlock = 0x00020000 // MNT_NOBLOCK
	FlagForce   = 0x00080000 // MNT_FORCE
)

// Entry is one row of a getfsstat(MNT_NOWAIT) snapshot.
type Entry struct {
	FSID        FSID
	Owner       uint32 // f_owner
	Type        string // f_fstypename
	MntOnName   string // f_mntonname
	MntFromName string // f_mntfromname
}

// Snapshot is one complete, same-instant mount-table observation.
type Snapshot struct {
	Boot    string // kern.bootsessionuuid
	Entries []Entry
}

// Role is what the transport proved about the peer's code signature. It is
// never read from the request payload.
type Role int

const (
	RoleNone    Role = iota
	RoleGUI          // com.juicemount.app: may call Status only
	RoleService      // com.juicemount.service (engine, volume owner): may register and retire
)

// Caller is the authenticated peer. UID is the connection's effective uid
// (xpc_connection_get_euid), never a payload field. There is deliberately no
// PID: a pid is reusable and must never stand for identity. AuditSession and
// SigningID are diagnostic, audit-derived context only.
type Caller struct {
	UID          uint32
	Role         Role
	AuditSession uint32
	SigningID    string
}

func (c Caller) String() string {
	return fmt.Sprintf("uid=%d role=%d asid=%d sig=%q", c.UID, c.Role, c.AuditSession, c.SigningID)
}

// Liveness is what the caller has PROVEN about the server behind a mount
// (the NFS engine or the juicefs FUSE daemon). Anything not proven is Alive
// for the purpose of choosing an unmount flavor: only proof of death or a
// wedge unlocks a direct MNT_FORCE.
type Liveness int

const (
	LivenessUnknown Liveness = iota // treated as Alive
	LivenessAlive
	LivenessDead   // server process proven exited (reaped / waitDone)
	LivenessWedged // server proven alive but not answering (e.g. SIGSTOP, macFUSE timeout)
)

func (l Liveness) provenGone() bool { return l == LivenessDead || l == LivenessWedged }

// Holder is one process keeping a volume busy. It names a process for the
// user; it is never used as identity or authority.
type Holder struct {
	PID   int32
	UID   uint32
	Name  string
	Files int
}

// Clock is injectable time.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Sentinel errors.
var (
	ErrNotAuthorized   = errors.New("unmountd: caller role may not perform this operation")
	ErrReceiptLimit    = errors.New("unmountd: per-uid receipt limit reached")
	ErrReceiptExists   = errors.New("unmountd: receipt already exists for this op")
	ErrNoReceipt       = errors.New("unmountd: no receipt for this op")
	ErrInvalidReceipt  = errors.New("unmountd: invalid receipt")
	ErrIdentityRefused = errors.New("unmountd: identity predicate refused")
	ErrOpIDReused      = errors.New("unmountd: op id already used in this daemon's lifetime")
	ErrPostState       = errors.New("unmountd: unmount reported success but the fsid stayed present")
	ErrNotCancellable  = errors.New("unmountd: retirement cannot be cancelled: a force was requested or issued")
)

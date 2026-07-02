package pin

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// FUSE-identity gate (V2.3 G0, 2026-07-01).
//
// When the macFUSE kext cannot load (approval lost after a macOS update —
// kernelmanagerd: "Extension io.macfuse.filesystems.macfuse.N not approved to
// load"), every juicefs mount hangs and the configured FUSE mountpoint is a
// PLAIN LOCAL DIRECTORY. Every path-based operation against it still
// "succeeds": the drainer os.Creates into the boot SSD and reports the upload
// complete (108 misdirected drains + 174GB stranded across months, found
// 2026-07-01), and a FUSE Lstat ENOENT "confirms" phantom purges for files
// that exist fine in the backend. This gate is the single cheap, bounded,
// cached authority those consumers must ask before trusting the mountpoint:
// is there actually a filesystem mounted here right now?
//
// Mechanism: statfs(2) the mountpoint and its PARENT and compare Fsid. Any
// mounted filesystem has its own fsid; a plain directory shares its parent's.
// Name-agnostic (no fstype string matching), ~µs when healthy. statfs against
// a WEDGED FUSE mount can block in the kernel, so the check runs bounded; a
// timeout reports NOT-OK — a wedged mount is equally undrainable, and every
// consumer fails safe (park drains / skip purge / skip prune).
//
// Unconfigured (SetFUSEIdentityPath never called — unit tests, tools that
// don't own a FUSE mount) the gate is INERT: always OK. Kill switch:
// JM_FUSE_IDENTITY_GATE=0 (read per call so tests can flip it).
//
// Consumers are non-hot-path only (drainer dispatch/worker, async phantom
// confirm, reconcile prune passes). Do NOT call from Stat/Lstat/OpenFile RPC
// hot paths — the cached read is cheap, but a cache miss pays a statfs and,
// on a wedge, blocks for the full bound (see feedback_perf_hot_path).

// ErrFUSEIdentityGate marks a failure caused by the identity gate refusing
// the mountpoint (absent/plain-dir/wedged). The drainer's failTransient
// treats it as an infrastructure pause — requeue without burning the row's
// per-file retry budget — exactly like an offline window.
var ErrFUSEIdentityGate = errors.New("fuse identity gate: mountpoint is not a mounted filesystem")

const (
	fuseIdentityTTL           = 2 * time.Second
	fuseIdentityStatfsTimeout = 3 * time.Second
)

type fuseIdentityResult struct {
	ok     bool
	reason string
	at     time.Time
}

var (
	fuseIdentityMu    sync.Mutex
	fuseIdentityPathV string
	fuseIdentityCache fuseIdentityResult
)

// SetFUSEIdentityPath arms the gate for the given FUSE mountpoint. Called
// once at boot by the owners of the mount (bridge core, jm5). Passing ""
// disarms it (gate inert).
func SetFUSEIdentityPath(path string) {
	fuseIdentityMu.Lock()
	fuseIdentityPathV = path
	fuseIdentityCache = fuseIdentityResult{} // invalidate
	fuseIdentityMu.Unlock()
}

// ResetFUSEIdentityForTest disarms the gate and clears the cache. Test-only.
func ResetFUSEIdentityForTest() { SetFUSEIdentityPath("") }

// FUSEIdentityOK reports whether the configured FUSE mountpoint currently has
// a real filesystem mounted on it. True when the gate is unconfigured or
// disabled. Cached (fuseIdentityTTL); worst case one bounded statfs.
func FUSEIdentityOK() bool {
	ok, _ := FUSEIdentityState()
	return ok
}

// FUSEIdentityState is FUSEIdentityOK plus a human-readable reason for
// logging and health surfacing.
func FUSEIdentityState() (bool, string) {
	if os.Getenv("JM_FUSE_IDENTITY_GATE") == "0" {
		return true, "gate disabled (JM_FUSE_IDENTITY_GATE=0)"
	}
	fuseIdentityMu.Lock()
	path := fuseIdentityPathV
	cached := fuseIdentityCache
	fuseIdentityMu.Unlock()
	if path == "" {
		return true, "gate unconfigured"
	}
	if time.Since(cached.at) < fuseIdentityTTL {
		return cached.ok, cached.reason
	}
	ok, reason := checkFUSEIdentity(path)
	fuseIdentityMu.Lock()
	// Another caller may have refreshed while we ran; last write wins — both
	// results are current within the TTL window, so either is acceptable.
	fuseIdentityCache = fuseIdentityResult{ok: ok, reason: reason, at: time.Now()}
	fuseIdentityMu.Unlock()
	return ok, reason
}

// fuseIdentityProbeGate single-flights the statfs prober (review fix, phase-1
// adversarial review): syscall.Statfs against a WEDGED macFUSE mount blocks
// in the kernel and pins an OS thread; without a gate every TTL expiry (the
// parked drainer alone re-consults every 3s) spawns another, accumulating
// hundreds of pinned threads per hour toward Go's 10000-thread abort —
// turning a safely-parked degraded state into a process crash. The slot is
// released only when the statfs actually RETURNS (never by a timed-out
// waiter), capping the leak at one thread per wedge episode; a gate-full
// consult reports NOT-OK immediately, which is both fail-safe and faster
// than waiting out the timeout against a probe that is already stuck.
var fuseIdentityProbeGate = make(chan struct{}, 1)

// checkFUSEIdentity runs the bounded, single-flighted statfs fsid comparison.
func checkFUSEIdentity(path string) (bool, string) {
	select {
	case fuseIdentityProbeGate <- struct{}{}:
	default:
		return false, "identity probe already in flight (previous statfs still blocked — mount wedged)"
	}
	type res struct {
		ok     bool
		reason string
	}
	ch := make(chan res, 1)
	go func() {
		defer func() { <-fuseIdentityProbeGate }()
		ok, reason := statfsIdentityOK(path)
		ch <- res{ok: ok, reason: reason}
	}()
	select {
	case r := <-ch:
		return r.ok, r.reason
	case <-time.After(fuseIdentityStatfsTimeout):
		return false, "statfs timed out — mount wedged"
	}
}

// statfsIdentityOK is the raw comparison: a mounted filesystem has its own
// fsid; a plain directory shares its parent's.
func statfsIdentityOK(path string) (bool, string) {
	var st, pst syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return false, "statfs mountpoint: " + err.Error()
	}
	if err := syscall.Statfs(filepath.Dir(path), &pst); err != nil {
		return false, "statfs parent: " + err.Error()
	}
	if st.Fsid == pst.Fsid {
		return false, "mountpoint is a plain directory — no filesystem mounted (macFUSE kext not loaded, or mount absent)"
	}
	return true, "ok"
}

// FUSEIdentityFresh is FUSEIdentityState with the TTL cache BYPASSED for the
// read (the result still re-stamps the cache, so ladder-path consumers in the
// same cycle see the fresh verdict). Use at decision points where ≤TTL-stale
// evidence is not acceptable — e.g. after G5's Lstat probes and before
// pruneAbsent mutation: ENOENT evidence is only trustworthy if the mount is
// verified real AFTER the last probe. Same kill-switch/unconfigured
// semantics; same single-flight bound (a wedge returns NOT-OK immediately).
func FUSEIdentityFresh() (bool, string) {
	if os.Getenv("JM_FUSE_IDENTITY_GATE") == "0" {
		return true, "gate disabled (JM_FUSE_IDENTITY_GATE=0)"
	}
	fuseIdentityMu.Lock()
	path := fuseIdentityPathV
	fuseIdentityMu.Unlock()
	if path == "" {
		return true, "gate unconfigured"
	}
	ok, reason := checkFUSEIdentity(path)
	fuseIdentityMu.Lock()
	fuseIdentityCache = fuseIdentityResult{ok: ok, reason: reason, at: time.Now()}
	fuseIdentityMu.Unlock()
	return ok, reason
}

// FUSEIdentityCheckFD verifies that an OPEN file descriptor lives on a real
// mounted filesystem rather than the gate path's parent volume (= plain-dir
// mountpoint). This is the race-free half of the drain gate (phase-1
// adversarial review, HIGH): the path-based check is cached and a mount can
// be torn down inside the TTL window, but an fd's filesystem binding is
// immutable — created on the plain dir it fails this check no matter what
// mounts later; created on the real FUSE fs, a subsequent unmount makes
// writes/fsync on it fail instead. fstatfs on a live fd does not consult the
// path namespace, so no timeout bound is needed for the plain-dir case; on a
// wedged FUSE fd it can block, which is the same pre-existing hazard class as
// the copy that follows it (drainer worker only — never the NFS hot path).
// Returns nil when the gate is unconfigured or disabled.
func FUSEIdentityCheckFD(fd uintptr) error {
	if os.Getenv("JM_FUSE_IDENTITY_GATE") == "0" {
		return nil
	}
	fuseIdentityMu.Lock()
	path := fuseIdentityPathV
	fuseIdentityMu.Unlock()
	if path == "" {
		return nil
	}
	var st syscall.Statfs_t
	if err := syscall.Fstatfs(int(fd), &st); err != nil {
		return fmt.Errorf("fstatfs dest: %v: %w", err, ErrFUSEIdentityGate)
	}
	var pst syscall.Statfs_t
	if err := syscall.Statfs(filepath.Dir(path), &pst); err != nil {
		return fmt.Errorf("statfs parent: %v: %w", err, ErrFUSEIdentityGate)
	}
	if st.Fsid == pst.Fsid {
		return fmt.Errorf("dest fd is on the mountpoint's parent volume — plain-dir mountpoint: %w", ErrFUSEIdentityGate)
	}
	return nil
}

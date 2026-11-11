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

const fuseIdentityTTL = 2 * time.Second

// Variables so tests can shorten them.
var fuseIdentityStatfsTimeout = 3 * time.Second

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

// Probes are single-flighted and BOUNDED (review fixes, phase-1 adversarial
// review and 2026-11-11). syscall.Statfs against a WEDGED macFUSE mount
// blocks in the kernel and pins an OS thread, so a probe is never abandoned
// for a new one at every consult: that leaked hundreds of threads per hour
// toward Go's 10000-thread abort.
//
//   - A consult while a probe younger than fuseIdentityReprobeAfter runs
//     WAITS for that probe's answer (up to the statfs timeout). Before, it
//     read "already in flight" as a wedge at once: on a distant link, where
//     one statfs takes two round trips, a second consult inside those 0.4 s
//     parked the drainer about 20 times per 10 minutes.
//   - A probe older than fuseIdentityReprobeAfter that has not returned does
//     not block recovery forever: a new probe may start beside it, up to
//     fuseIdentityMaxStuck in flight. One statfs that never returned (seen in
//     the remote lab) used to pause the drainer for good. The leak bound is
//     fuseIdentityMaxStuck threads per wedge episode.
//   - A probe's slot is freed only when its statfs RETURNS.
var fuseIdentityReprobeAfter = 30 * time.Second

const fuseIdentityMaxStuck = 4

type identityProbe struct {
	started time.Time
	done    chan struct{}
	ok      bool
	reason  string
}

var (
	identityProbeMu sync.Mutex
	identityProbes  []*identityProbe // in flight, oldest first
	// statfsIdentityProbe is statfsIdentityOK; tests replace it.
	statfsIdentityProbe = statfsIdentityOK
)

// checkFUSEIdentity runs the bounded, shared statfs fsid comparison.
func checkFUSEIdentity(path string) (bool, string) {
	identityProbeMu.Lock()
	var p *identityProbe
	if n := len(identityProbes); n > 0 && time.Since(identityProbes[n-1].started) < fuseIdentityReprobeAfter {
		p = identityProbes[n-1] // share the running probe's answer
	} else if n >= fuseIdentityMaxStuck {
		identityProbeMu.Unlock()
		return false, fmt.Sprintf("mount check not answering: %d statfs calls have not returned", n)
	} else {
		p = &identityProbe{started: time.Now(), done: make(chan struct{})}
		identityProbes = append(identityProbes, p)
		probe := statfsIdentityProbe
		go func(p *identityProbe) {
			ok, reason := probe(path)
			identityProbeMu.Lock()
			p.ok, p.reason = ok, reason
			for i, q := range identityProbes {
				if q == p {
					identityProbes = append(identityProbes[:i], identityProbes[i+1:]...)
					break
				}
			}
			identityProbeMu.Unlock()
			close(p.done)
		}(p)
	}
	timeout := fuseIdentityStatfsTimeout
	identityProbeMu.Unlock()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		return p.ok, p.reason
	case <-timer.C:
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

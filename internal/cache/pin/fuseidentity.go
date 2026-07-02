package pin

import (
	"errors"
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

// checkFUSEIdentity runs the bounded statfs fsid comparison. On timeout the
// straggler goroutine's result is discarded — the next TTL expiry re-checks.
func checkFUSEIdentity(path string) (bool, string) {
	type res struct {
		ok     bool
		reason string
	}
	ch := make(chan res, 1)
	go func() {
		var st, pst syscall.Statfs_t
		if err := syscall.Statfs(path, &st); err != nil {
			ch <- res{false, "statfs mountpoint: " + err.Error()}
			return
		}
		if err := syscall.Statfs(filepath.Dir(path), &pst); err != nil {
			ch <- res{false, "statfs parent: " + err.Error()}
			return
		}
		if st.Fsid == pst.Fsid {
			ch <- res{false, "mountpoint is a plain directory — no filesystem mounted (macFUSE kext not loaded, or mount absent)"}
			return
		}
		ch <- res{true, "ok"}
	}()
	select {
	case r := <-ch:
		return r.ok, r.reason
	case <-time.After(fuseIdentityStatfsTimeout):
		return false, "statfs timed out — mount wedged"
	}
}

package pin

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ErrOfflineNotAvailable signals that an NFS operation was refused
// because offline mode is engaged and the requested path isn't
// pinned-ready. The NFS handler returns this so the protocol layer
// (internal/nfs/nfs_on{getattr,lookup,read}.go) can map it to
// NFSStatusNXIO ("no such device or address") rather than the
// default NFSStatusIO or NFSStatusNoEnt — the former is generic
// "I/O error," the latter would invalidate the kernel's file handle
// cache and require a remount for the file to reappear after the
// network returns.
var ErrOfflineNotAvailable = errors.New("media not available offline")

// IsOfflineNotAvailable reports whether err is (or wraps)
// ErrOfflineNotAvailable. Use in NFS protocol-layer error
// translation to distinguish offline-refused from genuine I/O
// failures.
func IsOfflineNotAvailable(err error) bool {
	return errors.Is(err, ErrOfflineNotAvailable)
}

// Offline mode is a process-wide toggle that the NFS read path consults
// to decide whether to fall through to FUSE on a cache miss (online) or
// fail fast with EIO (offline).
//
// Two independent sources can engage offline mode:
//
//   - User intent (SetOffline): the user clicked the "Offline" toggle.
//     They want the strict policy on cellular, willing to accept "media
//     not available offline" errors for un-pinned files.
//
//   - Auto (SetAutoOffline): the reachability monitor observed that
//     this Mac has lost its network path to the backend. We engage
//     offline mode so un-pinned reads fail-fast instead of waiting for
//     kernel-NFS to time out (7 s+) and tying up Finder. When the
//     network comes back, the monitor calls SetAutoOffline(false, "")
//     and the policy lifts automatically.
//
// IsOffline reports the OR of both — offline if either source has
// engaged it. This matches the user mental model: "if either I OR the
// network says I'm offline, I'm offline."
//
// The two sources are intentionally tracked separately so the UI can
// show distinct states ("offline by your choice" vs "offline because
// the network dropped") and so toggling one doesn't accidentally undo
// the other.

var (
	// User-intent flag. Cheap atomic read for hot paths.
	userOfflineFlag atomic.Int32

	// Auto-engage flag. Cheap atomic read for hot paths.
	autoOfflineFlag atomic.Int32

	// Reason + since are read by the UI / metrics endpoint, not by
	// hot paths. Protected by a small RW mutex; reads only happen
	// when surfacing state, not on every NFS read.
	autoOfflineMu     sync.RWMutex
	autoOfflineReason string
	autoOfflineSince  time.Time
	// userOfflineSince records when the user-intent toggle last engaged
	// (zero when not user-offline). Protected by autoOfflineMu alongside the
	// auto fields. Used by WithinOfflineReadStallWindow to bound how long an
	// in-flight read is JUKEBOX-stalled after going offline.
	userOfflineSince time.Time
)

// V2.3 U3 (field report: "clicking the offline toggle doesn't just start it
// in offline mode"): user-intent offline PERSISTS across launches via a
// marker file. Armed at boot (SetOfflinePersistPath); SetOffline writes/
// removes the marker best-effort, and the boot path consults
// PersistedOfflineIntent() to start offline without touching the network.
// Unconfigured (tests, tools) → fully inert.
var (
	offlinePersistMu   sync.Mutex
	offlinePersistPath string
)

// SetOfflinePersistPath arms user-offline persistence at the given marker
// path. "" disarms.
func SetOfflinePersistPath(path string) {
	offlinePersistMu.Lock()
	offlinePersistPath = path
	offlinePersistMu.Unlock()
}

// PersistedOfflineIntent reports whether a previous session left user-intent
// offline engaged. False when persistence is unconfigured.
func PersistedOfflineIntent() bool {
	offlinePersistMu.Lock()
	p := offlinePersistPath
	offlinePersistMu.Unlock()
	if p == "" {
		return false
	}
	_, err := os.Stat(p)
	return err == nil
}

// persistOfflineIntent mirrors the user flag to disk, best-effort — a
// failed write must never block or fail the toggle itself.
func persistOfflineIntent(on bool) {
	offlinePersistMu.Lock()
	p := offlinePersistPath
	offlinePersistMu.Unlock()
	if p == "" {
		return
	}
	if on {
		_ = os.WriteFile(p, []byte("user-offline\n"), 0o644)
	} else {
		_ = os.Remove(p)
	}
}

// SetOffline switches the process to user-intent offline mode (or
// back online). This is the existing API; preserved for callers that
// only know about manual toggles.
func SetOffline(on bool) {
	if on {
		// Stamp BEFORE the flag store: an in-flight read on another goroutine
		// must never observe IsOffline()==true while WithinOfflineReadStallWindow()
		// is still false (it would get a terminal NXIO instead of a retryable
		// JUKEBOX — the exact abort this feature prevents).
		autoOfflineMu.Lock()
		if userOfflineSince.IsZero() {
			userOfflineSince = time.Now()
		}
		autoOfflineMu.Unlock()
		userOfflineFlag.Store(1)
	} else {
		userOfflineFlag.Store(0)
		autoOfflineMu.Lock()
		userOfflineSince = time.Time{}
		autoOfflineMu.Unlock()
	}
	persistOfflineIntent(on)
}

// SetAutoOffline is called by the reachability monitor when the
// network path to the metadata host transitions. `on=true` engages
// auto-offline with `reason` (a human-readable string suitable for
// surfacing in the UI). `on=false` clears both flag and reason.
//
// Safe to call concurrently with SetOffline — they track independent
// flags and never block each other.
func SetAutoOffline(on bool, reason string) {
	if on {
		// Stamp BEFORE the flag store (see SetOffline) so WithinOfflineReadStallWindow()
		// is already valid the instant IsOffline() flips true.
		autoOfflineMu.Lock()
		autoOfflineReason = reason
		if autoOfflineSince.IsZero() {
			autoOfflineSince = time.Now()
		}
		autoOfflineMu.Unlock()
		autoOfflineFlag.Store(1)
	} else {
		autoOfflineFlag.Store(0)
		autoOfflineMu.Lock()
		autoOfflineReason = ""
		autoOfflineSince = time.Time{}
		autoOfflineMu.Unlock()
	}
}

// IsOffline reports the effective state. Cheap; safe in hot paths.
// True iff either the user has engaged offline mode OR the auto-
// engage signal has fired.
func IsOffline() bool {
	return userOfflineFlag.Load() != 0 || autoOfflineFlag.Load() != 0
}

// IsUserOffline reports whether the user-intent toggle is engaged.
// Use this when the UI needs to render the toggle's checked state
// without the auto-engaged state coloring the answer.
func IsUserOffline() bool {
	return userOfflineFlag.Load() != 0
}

// IsAutoOffline reports whether auto-engage has fired.
func IsAutoOffline() bool {
	return autoOfflineFlag.Load() != 0
}

// OfflineReadStallWindow bounds how long an IN-FLIGHT read (one already in
// progress when offline engaged — the client is mid-file) is answered with
// NFS3ERR_JUKEBOX (the kernel NFS client holds + retries the RPC, resuming on
// reconnect) instead of a terminal NXIO. Measured from when offline engaged by
// either source. Sized against the macOS soft-mount per-RPC budget so a brief
// blip or a toggle-offline-then-back rides through and the copy survives, while
// a genuinely sustained offline still fails the copy cleanly rather than
// hanging it forever. A var so it can be tuned / env-overridden.
var OfflineReadStallWindow = 90 * time.Second

// WithinOfflineReadStallWindow reports whether offline (by either source)
// engaged recently enough that an in-flight read should be JUKEBOX-stalled
// (stall + resume on reconnect) rather than NXIO-failed. False when fully
// online, and false once we've been continuously offline longer than
// OfflineReadStallWindow (give up — don't beach-ball the copy forever).
func WithinOfflineReadStallWindow() bool {
	autoOfflineMu.RLock()
	auto := autoOfflineSince
	user := userOfflineSince
	autoOfflineMu.RUnlock()
	// "Since" = the EARLIEST still-active engage = how long we've been
	// continuously offline. An in-flight copy is caught at the transition, so
	// measuring from the first engage is what bounds its stall correctly.
	since := auto
	if since.IsZero() || (!user.IsZero() && user.Before(since)) {
		since = user
	}
	if since.IsZero() {
		return false
	}
	return time.Since(since) < OfflineReadStallWindow
}

// ErrSpoolIncomplete signals an NFS read landed at/past the contiguous-written
// prefix of a file STILL ARRIVING in the write spool, with not-yet-written bytes
// still expected below the high-water mark (off in [contiguousEnd, writtenEnd)).
// The NFS read path (internal/nfs/nfs_on read) maps it to NFS3ERR_JUKEBOX so the
// kernel client HOLDS and retries until the bytes land / the file drains — never
// EOF, which would let a client treat a partially-arrived file as COMPLETE
// (silent truncation, task #65). Cross-package sentinel (the nfs-side spool read
// handle returns it; internal/nfs recognizes it) — same pattern as
// ErrOfflineNotAvailable, avoiding an internal/nfs <- nfs import cycle.
var ErrSpoolIncomplete = errors.New("spool: read offset not yet written (in-flight)")

// IsSpoolIncomplete reports whether err is (or wraps) ErrSpoolIncomplete.
func IsSpoolIncomplete(err error) bool { return errors.Is(err, ErrSpoolIncomplete) }

// ErrSpoolDrained signals the spool file was evicted + unlinked (drain
// completed) between a read's LookupActive hit and its first fd open — the
// drain-evict race. The bytes are now in FUSE at the published size (the drainer
// publishes BEFORE it evicts), so the NFS read path maps this to NFS3ERR_NOENT
// so the client reissues OPEN and lands on the drained backend copy, rather than
// a terminal NFSStatusIO that would abort the copy ("error 100060").
var ErrSpoolDrained = errors.New("spool: file drained+evicted before read (reopen)")

// IsSpoolDrained reports whether err is (or wraps) ErrSpoolDrained.
func IsSpoolDrained(err error) bool { return errors.Is(err, ErrSpoolDrained) }

// SpoolIncompleteStallWindow bounds how long a read of a not-yet-written spool
// hole is held with JUKEBOX before giving up (EOF/terminal). Keyed off the
// entry's LAST WRITE (applied where the entry is available, in the nfs-side read
// handle): an actively-arriving file holds-and-resumes, but a wedged/abandoned
// writer (silent past this window) stops beach-balling the client. A var so it
// can be tuned / env-overridden; mirrors OfflineReadStallWindow's sizing.
var SpoolIncompleteStallWindow = 90 * time.Second

// OfflineState is a snapshot of the offline subsystem suitable for
// surfacing to the UI or HTTP metrics endpoint.
type OfflineState struct {
	// Offline is the effective state — what callers should branch on.
	Offline bool `json:"offline"`
	// UserOffline is the user-intent flag.
	UserOffline bool `json:"user_offline"`
	// AutoOffline is true when the reachability monitor has engaged
	// offline mode due to a network-path failure.
	AutoOffline bool `json:"auto_offline"`
	// Reason is a human-readable explanation of WHY auto-offline is
	// engaged. Empty when AutoOffline is false.
	Reason string `json:"reason,omitempty"`
	// Since is when auto-offline engaged. Zero when not engaged.
	Since time.Time `json:"since,omitempty"`
	// SinceSec is the same as Since but as elapsed-seconds-since,
	// included to make the JSON shape useful without timestamp math
	// on the client.
	SinceSec int64 `json:"since_sec"`
}

// State returns a consistent snapshot of all offline-mode fields.
// Reads are RLock-only and complete in microseconds.
//
// Note on consistency: the flag reads (atomic) happen before the
// RLock. If a concurrent SetAutoOffline(true, ...) is mid-execution,
// State() can briefly observe `AutoOffline=true` paired with an empty
// `Reason`/zero `Since`. This window is sub-microsecond and the
// resulting JSON is harmless — the next poll converges. Holding the
// write lock across the atomic Store would serialize every
// IsOffline() hot-path call, which defeats the purpose. The relaxed
// consistency is intentional.
func State() OfflineState {
	user := userOfflineFlag.Load() != 0
	auto := autoOfflineFlag.Load() != 0
	autoOfflineMu.RLock()
	reason := autoOfflineReason
	since := autoOfflineSince
	autoOfflineMu.RUnlock()
	var sinceSec int64
	if !since.IsZero() {
		sinceSec = int64(time.Since(since).Seconds())
	}
	return OfflineState{
		Offline:     user || auto,
		UserOffline: user,
		AutoOffline: auto,
		Reason:      reason,
		Since:       since,
		SinceSec:    sinceSec,
	}
}

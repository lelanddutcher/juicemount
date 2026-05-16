package pin

import (
	"sync"
	"sync/atomic"
	"time"
)

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
)

// SetOffline switches the process to user-intent offline mode (or
// back online). This is the existing API; preserved for callers that
// only know about manual toggles.
func SetOffline(on bool) {
	if on {
		userOfflineFlag.Store(1)
	} else {
		userOfflineFlag.Store(0)
	}
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
		autoOfflineFlag.Store(1)
		autoOfflineMu.Lock()
		autoOfflineReason = reason
		if autoOfflineSince.IsZero() {
			autoOfflineSince = time.Now()
		}
		autoOfflineMu.Unlock()
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

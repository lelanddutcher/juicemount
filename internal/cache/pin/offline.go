package pin

import (
	"sync/atomic"
)

// OfflineMode is a process-wide toggle that the NFS read path consults
// to decide whether to fall through to FUSE on a cache miss (online) or
// fail fast with EIO (offline).
//
// Mental model: when the user is on cellular and a Premiere read misses
// our cache, we don't want to wait 30 seconds for a slow B2 GET to time
// out — we want to error immediately so the user knows the file isn't
// available offline and can either reconnect or skip the clip.
//
// Set via SetOffline(true). Read via IsOffline() in the read hot path.
// Atomic int32 so the read costs roughly nothing (1-2 ns).
var offlineFlag atomic.Int32

// SetOffline switches the process to offline mode (or back online).
func SetOffline(on bool) {
	if on {
		offlineFlag.Store(1)
	} else {
		offlineFlag.Store(0)
	}
}

// IsOffline reports the current state. Cheap; safe in hot paths.
func IsOffline() bool {
	return offlineFlag.Load() != 0
}

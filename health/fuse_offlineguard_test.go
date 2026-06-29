package health

import (
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// TestFUSEOfflineGuard_PreventsEscalation proves the release-critical watchdog
// guard (f2891d5, the "connection interrupted" fix): a stale-but-juicefs-alive
// FUSE tick must NOT escalate to a remount while the user is offline.
//
// monitorLoop calls fuseSkipEscalateWhileOffline() at the top of its
// stale-but-alive branch; when it returns true the loop ZEROES
// staleWhileAliveTicks and continues, so the counter can never accumulate to the
// escalate-to-remount threshold (fuse.go:1184 fm.Mount()). Offline, the daemon
// is busy draining (saturated), not wedged — a remount would diskutil-unmount-
// force a HEALTHY daemon and drop the NFS mount. This locks the predicate's
// truth table so a future refactor can't silently re-enable the false remount.
func TestFUSEOfflineGuard_PreventsEscalation(t *testing.T) {
	defer func() {
		pin.SetOffline(false)
		pin.SetAutoOffline(false, "test cleanup")
	}()

	// ONLINE: a genuine wedge online must STILL be allowed to escalate.
	pin.SetOffline(false)
	pin.SetAutoOffline(false, "")
	if fuseSkipEscalateWhileOffline() {
		t.Fatal("online: guard must NOT skip escalation (a real wedge online still needs the remount)")
	}

	// USER-OFFLINE: guard MUST skip escalation.
	pin.SetOffline(true)
	if !fuseSkipEscalateWhileOffline() {
		t.Fatal("user-offline: guard MUST skip escalation (busy-draining daemon is alive; remount = Finder 'connection interrupted')")
	}
	pin.SetOffline(false)

	// AUTO-OFFLINE (auto-engaged on a link drop) also trips the guard — the fix
	// must hold whether offline was user-toggled or auto-detected, since
	// pin.IsOffline() covers both.
	pin.SetAutoOffline(true, "test link drop")
	if !fuseSkipEscalateWhileOffline() {
		t.Fatal("auto-offline: guard MUST skip escalation too")
	}
	pin.SetAutoOffline(false, "")

	// KILL-SWITCH (JM_FUSE_OFFLINE_ESCALATE=1): operator override restores the
	// pre-fix escalate-when-offline behavior even while offline.
	pin.SetOffline(true)
	saved := fuseOfflineNoEscalate
	fuseOfflineNoEscalate = false // simulates JM_FUSE_OFFLINE_ESCALATE=1
	defer func() { fuseOfflineNoEscalate = saved }()
	if fuseSkipEscalateWhileOffline() {
		t.Fatal("kill-switch on + offline: guard must NOT skip (operator chose to restore escalate-when-offline)")
	}
}

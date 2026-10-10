package health

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// remountTestHooks tightens the tunables and forces "juicefs gone" so the
// recovery path runs deterministically. On a host where a real juicefs is
// running (e.g. the developer's own app) the handler would otherwise defer
// to the FUSE watchdog and never act.
func remountTestHooks(t *testing.T, threshold int, cooldown time.Duration) {
	t.Helper()
	prevThreshold := NFSStaleThreshold
	prevCooldown := NFSRemountCooldown
	prevAlive := isJuiceFSProcessAliveFn
	NFSStaleThreshold = threshold
	NFSRemountCooldown = cooldown
	isJuiceFSProcessAliveFn = func(string) bool { return false }
	t.Cleanup(func() {
		NFSStaleThreshold = prevThreshold
		NFSRemountCooldown = prevCooldown
		isJuiceFSProcessAliveFn = prevAlive
	})
}

// TestNFSAutoRemountThreshold verifies that, for a verified-absent mount
// (nothing to detach), the auto-remount handler:
//   - does NOT trigger before the failure threshold
//   - triggers exactly once at the threshold
//   - resets the streak after the trigger
//   - is suppressed by the cooldown on subsequent failures
func TestNFSAutoRemountThreshold(t *testing.T) {
	remountTestHooks(t, 3, 100*time.Millisecond)

	var calls atomic.Int32
	m := &HealthMonitor{
		cfg: Config{NFSMountPoint: "/owned/jm-test-mount"},
	}
	m.EnableNFSRemount(func() error {
		calls.Add(1)
		return nil
	})

	// Two failures: should not yet remount.
	m.handleNFSAutoRemount(false, true)
	m.handleNFSAutoRemount(false, true)
	if calls.Load() != 0 {
		t.Fatalf("calls = %d before threshold, want 0", calls.Load())
	}

	// Third failure: triggers remount (synchronously).
	m.handleNFSAutoRemount(false, true)
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d at threshold, want 1", got)
	}

	// Subsequent failures within cooldown should NOT remount again.
	m.handleNFSAutoRemount(false, true)
	m.handleNFSAutoRemount(false, true)
	m.handleNFSAutoRemount(false, true)
	if got := calls.Load(); got != 1 {
		t.Errorf("calls = %d during cooldown, want 1", got)
	}

	// Wait past cooldown, accumulate threshold failures, expect another
	// remount.
	time.Sleep(NFSRemountCooldown + 20*time.Millisecond)
	m.handleNFSAutoRemount(false, true)
	m.handleNFSAutoRemount(false, true)
	m.handleNFSAutoRemount(false, true)
	if got := calls.Load(); got != 2 {
		t.Errorf("calls = %d after cooldown, want 2", got)
	}
}

// TestNFSAutoRemountResetOnHealthy verifies that a healthy check
// resets the streak counter.
func TestNFSAutoRemountResetOnHealthy(t *testing.T) {
	remountTestHooks(t, 3, NFSRemountCooldown)

	var calls atomic.Int32
	m := &HealthMonitor{cfg: Config{NFSMountPoint: "/owned/jm-test-mount"}}
	m.EnableNFSRemount(func() error {
		calls.Add(1)
		return nil
	})

	m.handleNFSAutoRemount(false, true)
	m.handleNFSAutoRemount(false, true)
	m.handleNFSAutoRemount(true, false) // healthy resets streak
	m.handleNFSAutoRemount(false, true)
	m.handleNFSAutoRemount(false, true)
	if got := calls.Load(); got != 0 {
		t.Errorf("calls = %d, expected reset to prevent any remount", got)
	}
}

// TestNFSAutoRemountDisabled verifies remount never runs when not enabled.
func TestNFSAutoRemountDisabled(t *testing.T) {
	prevThreshold := NFSStaleThreshold
	NFSStaleThreshold = 1
	t.Cleanup(func() { NFSStaleThreshold = prevThreshold })

	m := &HealthMonitor{cfg: Config{NFSMountPoint: "/owned/jm-test-mount"}}
	// no remount fn registered
	for i := 0; i < 10; i++ {
		m.handleNFSAutoRemount(false, false)
	}
	m.mu.RLock()
	state := m.nfsRecovery
	m.mu.RUnlock()
	if state != NFSRecoveryNone {
		t.Fatalf("disabled remount entered recovery state %q", state)
	}
}

// TestNFSStaleMountWithoutHelperHoldsTypedState is the H-3 L6 contract: a
// present-but-stale mount past the threshold, with no unmount helper wired,
// must NOT be remounted (a remount over a live mount would stack, and the
// old path-based force-unmount is gone). The monitor enters the typed
// NFSRecoveryHelperRequired state, surfaces it as the NFS message, and
// clears it on the next healthy observation.
func TestNFSStaleMountWithoutHelperHoldsTypedState(t *testing.T) {
	remountTestHooks(t, 2, time.Millisecond)

	var remounts atomic.Int32
	m := New(Config{NFSMountPoint: "/owned/jm-test-mount"})
	m.EnableNFSRemount(func() error { remounts.Add(1); return nil })

	stale := ComponentStatus{Healthy: false, Message: "stale: stat failed"}
	for i := 0; i < 6; i++ {
		m.routeNFSRecovery(stale, true)
		time.Sleep(2 * time.Millisecond) // past the cooldown every tick
	}
	if got := remounts.Load(); got != 0 {
		t.Fatalf("stale present mount remounted %d times without the helper", got)
	}
	m.mu.RLock()
	state := m.nfsRecovery
	m.mu.RUnlock()
	if state != NFSRecoveryHelperRequired {
		t.Fatalf("state = %q, want %q", state, NFSRecoveryHelperRequired)
	}

	shown := overlayNFSRecovery(stale, state)
	if shown.Message != NFSStaleHelperRequiredMessage {
		t.Fatalf("user-visible NFS message = %q, want %q", shown.Message, NFSStaleHelperRequiredMessage)
	}
	// The app reads "not mounted" as absence; the stale message must not.
	if strings.Contains(shown.Message, nfsMsgNotMounted) {
		t.Fatalf("stale message %q would read as absent in the app", shown.Message)
	}
	if healthy := overlayNFSRecovery(ComponentStatus{Healthy: true, Message: "ok"}, state); healthy.Message != "ok" {
		t.Fatalf("healthy status rewritten to %q", healthy.Message)
	}

	m.routeNFSRecovery(ComponentStatus{Healthy: true, Message: "ok"}, true)
	m.mu.RLock()
	state = m.nfsRecovery
	m.mu.RUnlock()
	if state != NFSRecoveryNone {
		t.Fatalf("healthy observation left state %q", state)
	}
}

// TestNFSStaleMountHelperSeam verifies the seam ordering: the helper retires
// first and the new birth runs only after a successful retirement. A failed
// retirement must not remount and must keep the typed state.
func TestNFSStaleMountHelperSeam(t *testing.T) {
	remountTestHooks(t, 1, time.Hour)

	for _, tc := range []struct {
		name         string
		retireErr    error
		wantRemounts int32
		wantState    NFSRecoveryState
	}{
		{"retired", nil, 1, NFSRecoveryNone},
		{"retire failed", errors.New("helper not approved"), 0, NFSRecoveryHelperRequired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var remounts atomic.Int32
			r := &countingRetirer{err: tc.retireErr}
			m := New(Config{NFSMountPoint: "/owned/jm-test-mount"})
			m.EnableNFSRemount(func() error {
				if r.calls.Load() == 0 {
					t.Error("remount ran before the helper retired the stale mount")
				}
				remounts.Add(1)
				return nil
			})
			m.SetStaleMountRetirer(r)
			m.SetJuiceFSLiveness(func() (bool, bool) { return false, true }) // exact child exited
			m.handleNFSAutoRemount(false, false)
			if r.unproven.Load() != 0 {
				t.Fatal("helper asked without proven liveness")
			}
			if r.calls.Load() != 1 || remounts.Load() != tc.wantRemounts {
				t.Fatalf("retire=%d remount=%d, want 1/%d", r.calls.Load(), remounts.Load(), tc.wantRemounts)
			}
			m.mu.RLock()
			state := m.nfsRecovery
			m.mu.RUnlock()
			if state != tc.wantState {
				t.Fatalf("state = %q, want %q", state, tc.wantState)
			}
		})
	}
}

// TestNFSStaleNeverCallsHelperWithoutProvenDeath: alive or unproven
// liveness (no exact child in this process, or only a ps-title probe) keeps
// the typed helper-required state and never asks the helper, at any streak.
func TestNFSStaleNeverCallsHelperWithoutProvenDeath(t *testing.T) {
	remountTestHooks(t, 1, time.Hour)
	for name, live := range map[string]func() (bool, bool){
		"proven alive": func() (bool, bool) { return true, true },
		"unproven":     nil, // ps fallback only
	} {
		t.Run(name, func(t *testing.T) {
			old := isJuiceFSProcessAliveFn
			isJuiceFSProcessAliveFn = func(string) bool { return false } // a ps title is not proof
			t.Cleanup(func() { isJuiceFSProcessAliveFn = old })
			r := &countingRetirer{}
			var remounts atomic.Int32
			m := New(Config{NFSMountPoint: "/owned/jm-test-mount"})
			m.EnableNFSRemount(func() error { remounts.Add(1); return nil })
			m.SetStaleMountRetirer(r)
			if live != nil {
				m.SetJuiceFSLiveness(live)
			}
			for i := 0; i < NFSStaleHardRemountThreshold+2; i++ {
				m.handleNFSAutoRemount(false, false)
			}
			m.mu.RLock()
			state := m.nfsRecovery
			m.mu.RUnlock()
			if r.calls.Load() != 0 || remounts.Load() != 0 || state != NFSRecoveryHelperRequired {
				t.Fatalf("retire=%d remount=%d state=%s", r.calls.Load(), remounts.Load(), state)
			}
		})
	}
}

// TestNFSStaleThenAbsentReportsNotMounted covers the review scenario:
// juicefs dies, the mount goes stale (helper-required), the user picks
// Disconnect in macOS's server-not-responding dialog, the volume is really
// gone, and the remount fails (retained prior operation). The app must
// then see "not mounted", not the helper-required text.
func TestNFSStaleThenAbsentReportsNotMounted(t *testing.T) {
	remountTestHooks(t, 2, time.Hour)

	var remounts atomic.Int32
	m := New(Config{NFSMountPoint: "/owned/jm-test-mount"})
	m.EnableNFSRemount(func() error {
		remounts.Add(1)
		return errors.New("prior NFS operation is retained")
	})

	stale := ComponentStatus{Healthy: false, Message: "unresponsive (stat timeout)"}
	for i := 0; i < 3; i++ {
		m.routeNFSRecovery(stale, true)
	}
	m.mu.RLock()
	state := m.nfsRecovery
	m.mu.RUnlock()
	if state != NFSRecoveryHelperRequired {
		t.Fatalf("precondition: state = %q, want %q", state, NFSRecoveryHelperRequired)
	}

	absent := ComponentStatus{Healthy: false, Message: nfsMsgNotMounted}
	for i := 0; i < 3; i++ {
		m.routeNFSRecovery(absent, true)
	}
	if remounts.Load() == 0 {
		t.Fatal("verified absence never attempted the remount")
	}
	m.mu.RLock()
	state = m.nfsRecovery
	m.mu.RUnlock()
	if state != NFSRecoveryNone {
		t.Fatalf("confirmed-absent probe left state %q after a failed remount", state)
	}

	// Independently of the state, an absent status is never rewritten.
	if got := overlayNFSRecovery(absent, NFSRecoveryHelperRequired).Message; got != nfsMsgNotMounted {
		t.Fatalf("absent NFS message rewritten to %q; the app would never see %q", got, nfsMsgNotMounted)
	}
}

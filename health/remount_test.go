package health

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestNFSAutoRemountThreshold verifies that the auto-remount handler:
//   - does NOT trigger before the failure threshold
//   - triggers exactly once at the threshold
//   - resets the streak after the trigger
//   - is suppressed by the cooldown on subsequent failures
func TestNFSAutoRemountThreshold(t *testing.T) {
	// Tighten the tunables for deterministic, fast testing.
	prevThreshold := NFSStaleThreshold
	prevCooldown := NFSRemountCooldown
	prevUmount := forceUnmountFn
	prevAlive := isJuiceFSProcessAliveFn
	NFSStaleThreshold = 3
	NFSRemountCooldown = 100 * time.Millisecond
	forceUnmountFn = func(string) error { return nil }
	// Force "juicefs gone" so the remount path runs deterministically — on a
	// host where a real juicefs is running (e.g. the dev's own app) the handler
	// would otherwise defer to the FUSE watchdog and never remount.
	isJuiceFSProcessAliveFn = func() bool { return false }
	t.Cleanup(func() {
		NFSStaleThreshold = prevThreshold
		NFSRemountCooldown = prevCooldown
		forceUnmountFn = prevUmount
		isJuiceFSProcessAliveFn = prevAlive
	})

	var calls atomic.Int32
	m := &HealthMonitor{
		cfg: Config{NFSMountPoint: "/tmp/jm-test-mount"},
	}
	m.EnableNFSRemount(func() error {
		calls.Add(1)
		return nil
	})

	// Two failures: should not yet remount.
	m.handleNFSAutoRemount(false)
	m.handleNFSAutoRemount(false)
	if calls.Load() != 0 {
		t.Fatalf("calls = %d before threshold, want 0", calls.Load())
	}

	// Third failure: triggers remount.
	m.handleNFSAutoRemount(false)
	// The remount runs synchronously inside handleNFSAutoRemount, so the
	// counter is updated by the time we return. We DO call sudo umount
	// inside that path which may emit warnings, but the test mount point
	// doesn't need to actually exist for the counter check.
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d at threshold, want 1", got)
	}

	// Subsequent failures within cooldown should NOT remount again.
	m.handleNFSAutoRemount(false)
	m.handleNFSAutoRemount(false)
	m.handleNFSAutoRemount(false)
	if got := calls.Load(); got != 1 {
		t.Errorf("calls = %d during cooldown, want 1", got)
	}

	// Wait past cooldown, accumulate threshold failures, expect another
	// remount.
	time.Sleep(NFSRemountCooldown + 20*time.Millisecond)
	m.handleNFSAutoRemount(false)
	m.handleNFSAutoRemount(false)
	m.handleNFSAutoRemount(false)
	if got := calls.Load(); got != 2 {
		t.Errorf("calls = %d after cooldown, want 2", got)
	}
}

// TestNFSAutoRemountResetOnHealthy verifies that a healthy check
// resets the streak counter.
func TestNFSAutoRemountResetOnHealthy(t *testing.T) {
	prevThreshold := NFSStaleThreshold
	prevUmount := forceUnmountFn
	prevAlive := isJuiceFSProcessAliveFn
	NFSStaleThreshold = 3
	forceUnmountFn = func(string) error { return nil }
	isJuiceFSProcessAliveFn = func() bool { return false }
	t.Cleanup(func() {
		NFSStaleThreshold = prevThreshold
		forceUnmountFn = prevUmount
		isJuiceFSProcessAliveFn = prevAlive
	})

	var calls atomic.Int32
	m := &HealthMonitor{cfg: Config{NFSMountPoint: "/tmp/jm-test-mount"}}
	m.EnableNFSRemount(func() error {
		calls.Add(1)
		return nil
	})

	m.handleNFSAutoRemount(false)
	m.handleNFSAutoRemount(false)
	m.handleNFSAutoRemount(true) // healthy resets streak
	m.handleNFSAutoRemount(false)
	m.handleNFSAutoRemount(false)
	if got := calls.Load(); got != 0 {
		t.Errorf("calls = %d, expected reset to prevent any remount", got)
	}
}

// TestNFSAutoRemountDisabled verifies remount never runs when not enabled.
func TestNFSAutoRemountDisabled(t *testing.T) {
	prevThreshold := NFSStaleThreshold
	NFSStaleThreshold = 1
	t.Cleanup(func() { NFSStaleThreshold = prevThreshold })

	m := &HealthMonitor{cfg: Config{NFSMountPoint: "/tmp/jm-test-mount"}}
	// no remount fn registered
	for i := 0; i < 10; i++ {
		m.handleNFSAutoRemount(false)
	}
	// Reaching here without panic / blocking is the test.
	_ = errors.New("placeholder so the import is used in CI builds")
}

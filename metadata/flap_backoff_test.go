package metadata

import (
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// farLink drives the shared netprofile to a far class, where
// flapDebounceInterval() is non-zero — the cellular case this fix is about.
func farLink(t *testing.T) {
	t.Helper()
	t.Setenv("JM_NET_LATENCY_CEILING", "")
	np := netprofile.Default()
	for i := 0; i < 6; i++ {
		np.ObserveRTT(400 * time.Millisecond)
	}
	if flapDebounceInterval() <= 0 {
		t.Fatal("precondition: a far link must have a non-zero flap debounce")
	}
}

// debounce is the class-aware window the shared profile currently yields.
func debounce() time.Duration { return flapDebounceInterval() }

// Measured on a real cellular link 2026-07-31. doReconcile computed a 300s
// backoff and the network-change ("flap") path walked straight past it, because
// the debounce window was independent of consecutiveFailures:
//
//	21:24:47 reconciliation failed (attempt 21) ... next_in_sec: 300
//	21:24:47 reconciliation triggered by network change   <- immediately
//	... 28 attempts, ending "SCAN budget 5m0s exceeded"
//
// Each attempt is a full-keyspace SCAN that saturates the uplink, which makes
// the link look changed, which fires another flap. The scan is its own trigger.
// User report: "when index rebuilding on cellular it really hogs all bandwidth
// and even a simple web search fails."
func TestFlapSuppressWindow_BackoffIsTheFloorWhileFailing(t *testing.T) {
	farLink(t)
	if got := flapSuppressWindow(21, 300*time.Second); got != 300*time.Second {
		t.Errorf("suppress window = %v with 21 failures and a 300s backoff, want 300s — "+
			"a flap must not retry sooner than the retry already scheduled", got)
	}
}

// With no failures the backoff is meaningless and the classic debounce applies,
// or a stale backoff would suppress a legitimate reconnect reconcile.
func TestFlapSuppressWindow_HealthyUsesDebounceOnly(t *testing.T) {
	farLink(t)
	if got := flapSuppressWindow(0, 300*time.Second); got != debounce() {
		t.Errorf("suppress window = %v when healthy, want the %v debounce", got, debounce())
	}
}

// An early, short backoff must not shrink the window below the debounce.
func TestFlapSuppressWindow_NeverShrinksBelowDebounce(t *testing.T) {
	farLink(t)
	if got := flapSuppressWindow(1, 1*time.Second); got != debounce() {
		t.Errorf("suppress window = %v, want the %v debounce — a small early backoff "+
			"must not defeat it", got, debounce())
	}
}

// LAN: debounce is 0, so a reconnect reconciles immediately. Byte-identical to
// pre-fix behavior — this must never throttle a fast link.
func TestFlapSuppressWindow_LANUnchanged(t *testing.T) {
	t.Setenv("JM_NET_LATENCY_CEILING", "")
	np := netprofile.Default()
	for i := 0; i < 60; i++ {
		np.ObserveRTT(300 * time.Microsecond)
	}
	if flapDebounceInterval() != 0 {
		t.Skip("shared netprofile not settled to a fast class")
	}
	if got := flapSuppressWindow(0, 0); got != 0 {
		t.Errorf("LAN suppress window = %v, want 0 (no debounce)", got)
	}
}

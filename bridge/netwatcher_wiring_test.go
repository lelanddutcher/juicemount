package main

import (
	"os"
	"regexp"
	"testing"
)

// The health monitor's network-change grace period must be WIRED in the app.
//
// health/monitor.go:369 suppresses a transient FUSE failure while the network
// re-establishes, gated on InGracePeriod() — which returns false unconditionally
// when netWatcher is nil (monitor.go:347). The only caller of SetNetWatcher
// anywhere in the repo was cmd/jm5, the dev CLI. Both objects existed in
// cbridge.go the whole time and were simply never connected, so the suppression
// NEVER FIRED in any shipped build: every WiFi<->cellular flip reported FUSE
// unhealthy instead of riding out the reconnect.
//
// This is a SOURCE-LEVEL assertion on purpose. The defect is not a wrong value
// that a behavioural test could catch — it is a missing call inside
// NFSServerStart, which cannot be invoked from a unit test (it mounts). A test
// that exercised health.SetNetWatcher directly would have passed happily for
// every month this bug was live, because the health package was never the
// broken part. The invariant worth pinning is "the app connects these two", so
// that is what is pinned.
//
// If this fails after a refactor, do not delete it — re-point it at wherever the
// wiring moved, or the grace period silently goes dead again.
func TestAppWiresNetWatcherIntoHealthMonitor(t *testing.T) {
	src, err := os.ReadFile("cbridge.go")
	if err != nil {
		t.Fatalf("read cbridge.go: %v", err)
	}
	// globalMonitor.SetNetWatcher(<something>) — allow whitespace and any arg.
	call := regexp.MustCompile(`globalMonitor\s*\.\s*SetNetWatcher\s*\(`)
	if !call.Match(src) {
		t.Error("cbridge.go never calls globalMonitor.SetNetWatcher — the network-change " +
			"grace period (health/monitor.go InGracePeriod) is dead, so every network " +
			"transition will report FUSE unhealthy immediately instead of being suppressed")
	}
}

// The watcher passed in must be a real one, not a nil literal. A nil argument
// compiles, satisfies the call-site check above, and restores exactly the broken
// behaviour — so the call alone is not sufficient evidence the fix is live.
func TestNetWatcherWiringDoesNotPassNil(t *testing.T) {
	src, err := os.ReadFile("cbridge.go")
	if err != nil {
		t.Fatalf("read cbridge.go: %v", err)
	}
	nilCall := regexp.MustCompile(`globalMonitor\s*\.\s*SetNetWatcher\s*\(\s*nil\s*\)`)
	if nilCall.Match(src) {
		t.Error("globalMonitor.SetNetWatcher(nil) is the same as not calling it at all")
	}
}

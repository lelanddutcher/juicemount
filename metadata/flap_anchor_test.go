package metadata

import (
	"testing"
	"time"
)

// The scenario under test, in real numbers from the tunnel profile:
//
//	scanContextTimeout (tunnel)  = 300s   the SCAN's own budget
//	maxBackoff                   = 300s   the largest suppress window
//
// A full-budget SCAN over a tunnel therefore runs for exactly as long as the
// longest window that could suppress the next one. Whether the next flap
// launches another full-tree SCAN comes down entirely to which timestamp the
// window is measured from.

const (
	tunnelScanBudget = 300 * time.Second
	maxSuppress      = 300 * time.Second
)

func TestSuppressionHoldsAfterAFailedFullBudgetScan(t *testing.T) {
	now := time.Now()
	startedAt := now.Add(-tunnelScanBudget) // began 300s ago
	endedAt := now                          // just gave up: "SCAN budget exceeded"
	var syncedAt time.Time                  // never SUCCEEDED, so this is unset

	recent := mostRecentSyncMark(startedAt, syncedAt, endedAt)
	if since := now.Sub(recent); since >= maxSuppress {
		t.Fatalf("time since anchor = %v >= suppress window %v, so a flap arriving "+
			"now would launch ANOTHER full-budget SCAN immediately. That is the "+
			"self-sustaining loop: SCAN saturates the uplink -> link looks degraded "+
			"-> flap -> SCAN, at ~100%% duty cycle on the link where a full-tree "+
			"SCAN is most expensive", since, maxSuppress)
	}
}

// Guards the fix against being "simplified" back: anchoring on the START of
// the failed attempt is precisely the defect.
func TestAnchoringOnStartAloneWouldNotSuppress(t *testing.T) {
	now := time.Now()
	startedAt := now.Add(-tunnelScanBudget)
	if now.Sub(startedAt) < maxSuppress {
		t.Fatal("premise broken: this test asserts that START-anchoring fails to " +
			"suppress; if it now suppresses, the budget/window numbers changed and " +
			"TestSuppressionHoldsAfterAFailedFullBudgetScan needs re-deriving")
	}
}

func TestSuppressionStillHoldsAfterASuccessfulScan(t *testing.T) {
	// The success path was already correct via lastSyncTime. It must stay so.
	now := time.Now()
	startedAt := now.Add(-tunnelScanBudget)
	syncedAt := now
	endedAt := now

	recent := mostRecentSyncMark(startedAt, syncedAt, endedAt)
	if since := now.Sub(recent); since >= maxSuppress {
		t.Fatalf("time since anchor = %v after a SUCCESSFUL sync — regression on a "+
			"path that already worked", since)
	}
}

func TestAnchorIsTheLatestOfTheThree(t *testing.T) {
	base := time.Now()
	a := base.Add(-3 * time.Minute)
	b := base.Add(-2 * time.Minute)
	c := base.Add(-1 * time.Minute)
	for _, tc := range []struct {
		name                         string
		started, synced, ended, want time.Time
	}{
		{"ended is latest", a, b, c, c},
		{"synced is latest", a, c, b, c},
		{"started is latest", c, a, b, c},
		{"synced unset", c, time.Time{}, b, c},
		{"ended unset", a, b, time.Time{}, b},
		{"all unset", time.Time{}, time.Time{}, time.Time{}, time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := mostRecentSyncMark(tc.started, tc.synced, tc.ended); !got.Equal(tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// A never-run sync must leave the anchor zero so the FIRST reconcile is not
// suppressed — startup must still index.
func TestFirstEverSyncIsNotSuppressed(t *testing.T) {
	recent := mostRecentSyncMark(time.Time{}, time.Time{}, time.Time{})
	if !recent.IsZero() {
		t.Fatalf("anchor = %v before any sync ran, want zero — a non-zero anchor "+
			"here could suppress the very first index build", recent)
	}
}

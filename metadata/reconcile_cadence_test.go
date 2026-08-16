package metadata

import (
	"testing"
	"time"
)

const testMaxBackoff = 5 * time.Minute

// THE REGRESSION, with the exact numbers observed on 2026-08-16.
//
// Push was working (verdict "working", subscribed to __keyspace@1__:d*), the
// engagement logic had correctly set the backstop to 900s, and the client still
// ran a full 420,699-entry SCAN every ~5 minutes that upserted NOTHING. The
// adaptive stretch was applying its ceiling after its floor, so a mechanism
// meant to LENGTHEN the interval truncated it to a third.
func TestExpensiveSyncNeverShortensTheBackstop(t *testing.T) {
	base := 900 * time.Second            // push ENABLED, LAN
	lastSync := 26237 * time.Millisecond // the logged last_sync_ms

	got := adaptReconcileInterval(base, lastSync, testMaxBackoff)

	if got < base {
		t.Fatalf("interval = %v, SHORTER than the %v backstop. That is a full-tree "+
			"SCAN every %v on a client whose keyspace push is healthy — the "+
			"'index keeps rebuilding for no reason' report", got, base, got)
	}
	if got == 5*time.Minute {
		t.Fatalf("interval = 5m exactly: maxBackoff (the FAILURE-retry cap) is still " +
			"governing the healthy cadence")
	}
}

// The stretch must still work where it was designed to: push DEGRADED, where
// the backstop drops to 30s and an expensive SCAN should back it off.
func TestExpensiveSyncStillStretchesAShortBackstop(t *testing.T) {
	base := 30 * time.Second
	lastSync := 20 * time.Second // 20 x 30 = 600s, above the cap

	got := adaptReconcileInterval(base, lastSync, testMaxBackoff)

	if got <= base {
		t.Errorf("interval = %v, want a stretch above the %v backstop — an expensive "+
			"SCAN on a 30s cadence is the anemic-ingest case this exists for", got, base)
	}
	if got != testMaxBackoff {
		t.Errorf("interval = %v, want it capped at %v", got, testMaxBackoff)
	}
}

// A cheap sync leaves the backstop alone.
func TestCheapSyncLeavesTheBackstopAlone(t *testing.T) {
	base := 900 * time.Second
	for _, cheap := range []time.Duration{0, 100 * time.Millisecond, reconcileAdaptiveThreshold} {
		if got := adaptReconcileInterval(base, cheap, testMaxBackoff); got != base {
			t.Errorf("lastSync=%v gave %v, want the untouched backstop %v", cheap, got, base)
		}
	}
}

// The invariant, swept: whatever the inputs, the cadence never comes out below
// the backstop the engagement logic chose.
func TestCadenceIsNeverBelowTheBackstop(t *testing.T) {
	for _, base := range []time.Duration{30 * time.Second, 5 * time.Minute, 900 * time.Second, 20 * time.Minute} {
		for _, last := range []time.Duration{
			0, time.Second, 2 * time.Second, 26 * time.Second, 5 * time.Minute, time.Hour,
		} {
			got := adaptReconcileInterval(base, last, testMaxBackoff)
			if got < base {
				t.Errorf("base=%v lastSync=%v -> %v, which is below the backstop", base, last, got)
			}
		}
	}
}

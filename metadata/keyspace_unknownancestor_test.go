package metadata

import (
	"testing"
	"time"
)

// TestUnknownAncestorPromotionLimiter pins the noteUnknownAncestor contract:
// first sighting of an inode promotes ONE full SCAN; repeat sightings of the
// same inode never promote again (the scan-filtered farm-namespace steady
// state); distinct new inodes inside the global rate window defer to the
// backstop instead of promoting.
func TestUnknownAncestorPromotionLimiter(t *testing.T) {
	// Promotion is now ALSO gated on link latency (915b209): a far link skips
	// the promoted SCAN entirely. netprofile.Default() is a PROCESS SINGLETON,
	// so this test must state the link it assumes rather than inherit whatever
	// a previously-run test left behind.
	settleNetprofileLAN(t)

	rc := &RedisClient{}
	triggered := 0
	rc.testTriggerSync = func() { triggered++ }

	// First sighting: promotes.
	rc.noteUnknownAncestor(1001)
	if triggered != 1 {
		t.Fatalf("first sighting: triggered=%d, want 1", triggered)
	}

	// Same inode again and again (farm writing into its derivative dir):
	// NEVER promotes again.
	for i := 0; i < 500; i++ {
		rc.noteUnknownAncestor(1001)
	}
	if triggered != 1 {
		t.Fatalf("repeat sightings promoted: triggered=%d, want 1", triggered)
	}

	// A DIFFERENT unknown inode inside the rate window: recorded but
	// deferred (global limiter), no promotion.
	rc.noteUnknownAncestor(1002)
	if triggered != 1 {
		t.Fatalf("in-window new inode promoted: triggered=%d, want 1", triggered)
	}

	// Age the limiter past the window: the NEXT new inode may promote.
	rc.unknownAncestorMu.Lock()
	rc.unknownAncestorLastSync = time.Now().Add(-time.Hour)
	rc.unknownAncestorMu.Unlock()
	rc.noteUnknownAncestor(1003)
	if triggered != 2 {
		t.Fatalf("post-window new inode did not promote: triggered=%d, want 2", triggered)
	}

	// But 1002 (already seen, even though it never got its own promotion)
	// stays suppressed — once-per-inode is strict.
	rc.unknownAncestorMu.Lock()
	rc.unknownAncestorLastSync = time.Now().Add(-time.Hour)
	rc.unknownAncestorMu.Unlock()
	rc.noteUnknownAncestor(1002)
	if triggered != 2 {
		t.Fatalf("seen inode re-promoted after window: triggered=%d, want 2", triggered)
	}

	// Drop counter advanced for the suppressed repeats.
	rc.unknownAncestorMu.Lock()
	drops := rc.unknownAncestorDrops
	rc.unknownAncestorMu.Unlock()
	if drops < 500 {
		t.Fatalf("drop counter = %d, want >= 500", drops)
	}
}

// TestUnknownAncestorSeenCapReset pins the overflow behavior: at capacity the
// seen-set resets wholesale rather than growing unbounded.
func TestUnknownAncestorSeenCapReset(t *testing.T) {
	rc := &RedisClient{}
	rc.testTriggerSync = func() {}
	rc.unknownAncestorMu.Lock()
	rc.unknownAncestorSeen = make(map[uint64]struct{}, unknownAncestorSeenCap)
	for i := uint64(0); i < unknownAncestorSeenCap; i++ {
		rc.unknownAncestorSeen[i] = struct{}{}
	}
	rc.unknownAncestorMu.Unlock()

	rc.noteUnknownAncestor(9999999) // at cap → reset, then record this one
	rc.unknownAncestorMu.Lock()
	n := len(rc.unknownAncestorSeen)
	_, has := rc.unknownAncestorSeen[9999999]
	rc.unknownAncestorMu.Unlock()
	if n != 1 || !has {
		t.Fatalf("cap reset wrong: len=%d has-new=%v, want 1/true", n, has)
	}
}

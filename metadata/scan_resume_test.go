package metadata

import (
	"testing"
	"time"
)

// A full SCAN used to be all-or-nothing: on a budget expiry or a dropped
// connection the cursor AND every entry already pulled were discarded, and the
// next attempt restarted at "0". On a link where the SCAN cannot finish inside
// its budget that is a treadmill — Leland's log: 28 attempts, none finishing,
// each re-pulling the same prefix of a ~200k keyspace over cellular. His words:
// "it really hogs all bandwidth and even a simple web search fails."
func rawsN(n int) []scanRawEntry {
	out := make([]scanRawEntry, n)
	for i := range out {
		out[i] = scanRawEntry{inode: uint64(i + 2), parentInode: "1", name: "f"}
	}
	return out
}

func TestScanResume_CarriesProgressAcrossAttempts(t *testing.T) {
	t.Setenv("JM_SCAN_RESUME", "")
	rc := &RedisClient{}

	rc.saveScanResume(rawsN(1200), map[string][2]string{"7": {"1", "a"}}, "4096")

	raws, rev, cursor := rc.takeScanResume()
	if cursor != "4096" {
		t.Errorf("resumed cursor = %q, want 4096 — the next attempt must continue, not restart", cursor)
	}
	if len(raws) != 1200 {
		t.Errorf("carried %d entries, want 1200 — discarding them is the wasted bandwidth", len(raws))
	}
	if len(rev) != 1 {
		t.Errorf("rev map lost across resume (%d entries) — path reconstruction needs it", len(rev))
	}
}

// A resume is consumed exactly once; a second take must start clean or two
// concurrent attempts would double-apply the same prefix.
func TestScanResume_ConsumedOnce(t *testing.T) {
	t.Setenv("JM_SCAN_RESUME", "")
	rc := &RedisClient{}
	rc.saveScanResume(rawsN(10), map[string][2]string{}, "99")

	if _, _, c := rc.takeScanResume(); c != "99" {
		t.Fatalf("first take cursor = %q, want 99", c)
	}
	if _, _, c := rc.takeScanResume(); c != "0" {
		t.Errorf("second take cursor = %q, want 0 — a resume must not be reusable", c)
	}
}

// A COMPLETED scan has nothing to resume from.
func TestScanResume_CompletedScanSavesNothing(t *testing.T) {
	t.Setenv("JM_SCAN_RESUME", "")
	rc := &RedisClient{}
	rc.saveScanResume(rawsN(500), map[string][2]string{}, "0")

	if _, _, c := rc.takeScanResume(); c != "0" {
		t.Errorf("a finished scan (cursor 0) was saved for resume: %q", c)
	}
}

// Entries pulled long ago describe a backend that has moved on; reconstruction
// would mix epochs. Correctness beats saved bytes.
func TestScanResume_StalePartialDiscarded(t *testing.T) {
	t.Setenv("JM_SCAN_RESUME", "")
	rc := &RedisClient{}
	rc.saveScanResume(rawsN(900), map[string][2]string{}, "128")
	rc.mu.Lock()
	rc.scanResumeAt = time.Now().Add(-2 * scanResumeMaxAge)
	rc.mu.Unlock()

	raws, _, cursor := rc.takeScanResume()
	if cursor != "0" || len(raws) != 0 {
		t.Errorf("stale partial was resumed (cursor=%q entries=%d) — it would mix epochs",
			cursor, len(raws))
	}
}

func TestScanResume_KillSwitch(t *testing.T) {
	t.Setenv("JM_SCAN_RESUME", "0")
	rc := &RedisClient{}
	rc.saveScanResume(rawsN(700), map[string][2]string{}, "256")

	if _, _, c := rc.takeScanResume(); c != "0" {
		t.Errorf("JM_SCAN_RESUME=0 did not restore all-or-nothing behavior (cursor %q)", c)
	}
}

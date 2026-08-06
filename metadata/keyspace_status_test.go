package metadata

import (
	"os"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/metrics"
)

func resetKeyspaceCounters(t *testing.T) {
	t.Helper()
	keyspaceEventsApplied.Store(0)
	keyspacePublished.Store(0)
	keyspaceLastEventNanos.Store(0)
	t.Cleanup(func() {
		keyspaceEventsApplied.Store(0)
		keyspacePublished.Store(0)
		keyspaceLastEventNanos.Store(0)
	})
}

// THE REFUSAL. Zero events with zero published mutations means nothing happened
// to observe — NOT that push works. Reporting a pass here would be a verdict
// from no measurement, which is the harness failure this project has paid for
// repeatedly (client-cache-served listings, a proxy with zero connections, a
// dirs=0 walk).
func TestKeyspaceVerdictRefusesWhenNothingWasObserved(t *testing.T) {
	resetKeyspaceCounters(t)
	st := keyspaceVerdictFor(0, 0, true)
	if st.Verdict != KeyspaceUnknown {
		t.Errorf("verdict = %q with nothing published and nothing received, want %q — "+
			"a pass here is a number from no measurement", st.Verdict, KeyspaceUnknown)
	}
	if !strings.Contains(st.Reason, "NOT a pass") {
		t.Errorf("reason %q does not say plainly that this is not a pass", st.Reason)
	}
}

// Published mutations with zero events IS a failure, because this client's own
// writes are replayed back to it. That property is what turns silence from
// ambiguous into diagnostic.
func TestKeyspaceVerdictIsBrokenWhenOurOwnWritesNeverReturn(t *testing.T) {
	resetKeyspaceCounters(t)
	st := keyspaceVerdictFor(0, 5, true)
	if st.Verdict != KeyspaceBroken {
		t.Errorf("verdict = %q after publishing 5 mutations and receiving none, want %q",
			st.Verdict, KeyspaceBroken)
	}
	if !strings.Contains(st.Reason, "replayed") {
		t.Errorf("reason %q does not explain WHY silence is conclusive here", st.Reason)
	}
}

// Events observed is the only thing that earns a pass.
func TestKeyspaceVerdictIsWorkingOnlyWithRealEvents(t *testing.T) {
	resetKeyspaceCounters(t)
	if st := keyspaceVerdictFor(12, 0, true); st.Verdict != KeyspaceWorking {
		t.Errorf("verdict = %q with 12 events applied, want %q", st.Verdict, KeyspaceWorking)
	}
}

// An unreadable config must not be reported as healthy. Empty flags is the
// 2026-07-10 failure exactly — notify-keyspace-events unset on the NAS, push
// permanently unable to engage, a 30s SCAN over the tunnel forever.
func TestKeyspaceVerdictSurfacesAnUnreadableConfig(t *testing.T) {
	resetKeyspaceCounters(t)
	st := keyspaceVerdictFor(0, 0, false)
	if st.Verdict != KeyspaceUnreachable {
		t.Errorf("verdict = %q with unreadable config, want %q", st.Verdict, KeyspaceUnreachable)
	}
	if !strings.Contains(st.Reason, "2026-07-10") {
		t.Errorf("reason %q does not point at the known failure this reproduces", st.Reason)
	}
}

// The counters must actually move, or the whole status is a dead mechanism
// reporting confident zeros — the exact class this sprint has spent the night
// removing.
func TestKeyspaceCountersMove(t *testing.T) {
	resetKeyspaceCounters(t)
	applied, published := KeyspaceCounters()
	if applied != 0 || published != 0 {
		t.Fatalf("counters not reset: %d/%d", applied, published)
	}
	noteKeyspaceEventApplied()
	noteKeyspaceEventApplied()
	noteKeyspacePublished()
	applied, published = KeyspaceCounters()
	if applied != 2 {
		t.Errorf("applied = %d after two events, want 2", applied)
	}
	if published != 1 {
		t.Errorf("published = %d after one publish, want 1", published)
	}
	if keyspaceLastEventNanos.Load() == 0 {
		t.Error("last-event timestamp never set — 'seconds since last event' would " +
			"always report never")
	}
}

// THE WIRING, not the counter.
//
// This test exists because its absence let a neuter pass silently: removing
// noteKeyspaceEventApplied() from applyEvent failed NOTHING, because
// TestKeyspaceCountersMove calls the counter function directly. That is exactly
// the dead-mechanism class — a correct counter reached by nothing, reporting
// confident zeros while push is fine or broken alike.
//
// So this drives the REAL applyEvent path and asserts the count moved.
func TestApplyEventCountsTowardKeyspaceStatus(t *testing.T) {
	resetKeyspaceCounters(t)
	rc := &RedisClient{store: newTestStore(t)}

	before, _ := KeyspaceCounters()
	rc.applyEvent(MetadataEvent{
		Op:    "create",
		Path:  "DCIM/A001/clip.mov",
		Size:  1234,
		Inode: 42,
	})
	after, _ := KeyspaceCounters()

	if after != before+1 {
		t.Fatalf("applied count %d -> %d after a real applyEvent, want +1 — the "+
			"counter is not wired into the event path, so KeyspaceStatus would "+
			"report zero events forever and call working push BROKEN", before, after)
	}
	if keyspaceLastEventNanos.Load() == 0 {
		t.Error("last-event timestamp not stamped by the real path")
	}
}

// A FILTERED event must still count. The question the status answers is "did
// push deliver to us at all", which an internal-namespace event answers just as
// well as an applied one. Counting only post-filter events would report a
// healthy pipeline as broken whenever the only traffic was .trash/.juicemount
// churn.
func TestFilteredEventsStillCountAsDelivery(t *testing.T) {
	resetKeyspaceCounters(t)
	rc := &RedisClient{store: newTestStore(t)}

	before, _ := KeyspaceCounters()
	rc.applyEvent(MetadataEvent{Op: "create", Path: ".juicemount/derivatives/1/x", Inode: 7})
	after, _ := KeyspaceCounters()

	if after != before+1 {
		t.Errorf("a scan-filtered event did not count as delivery (%d -> %d) — a "+
			"volume whose only traffic is internal-namespace churn would be "+
			"reported as broken push", before, after)
	}
}

// The PUBLISH side must count too, and this is a source-level assertion because
// PublishEvent needs a live Redis connection.
//
// The published count is the VALIDITY DENOMINATOR. Without it, "zero events" can
// never be distinguished from "nothing happened", so a genuinely broken push
// reports "unknown" forever — the status becomes permanently unable to fail,
// which is worse than not having it.
//
// A behavioural test would need a Redis; a source assertion has no such excuse
// to be absent. If PublishEvent is refactored, re-point this rather than delete
// it.
func TestPublishEventIsWiredToTheValidityCounter(t *testing.T) {
	src, err := readSourceFile("redis.go")
	if err != nil {
		t.Fatalf("read redis.go: %v", err)
	}
	if !strings.Contains(src, "noteKeyspacePublished()") {
		t.Error("redis.go never calls noteKeyspacePublished — the validity " +
			"denominator is dead, so KeyspaceStatus can only ever answer 'unknown' " +
			"and a genuinely broken push will never be reported as broken")
	}
}

func readSourceFile(name string) (string, error) {
	b, err := os.ReadFile(name)
	return string(b), err
}

// KeyspaceStatus must be REACHABLE from /metrics.
//
// A status nothing calls is indistinguishable from no status at all, and that is
// the single most common defect shape found this sprint — the FUSE ceiling with
// zero readers, the network grace period, the fd stats, SetStatsProvider,
// finishStream, the boot sweep. Registering from an init means this one cannot
// join them.
func TestKeyspaceVerdictIsReportedInMetrics(t *testing.T) {
	resetKeyspaceCounters(t)

	snap := metrics.Default().Snapshot()
	if snap.Keyspace == nil {
		t.Fatal("metrics snapshot has no keyspace section — push health is invisible " +
			"in /metrics; is the init() provider registration still there?")
	}
	// With nothing observed the verdict must be the REFUSAL, not a pass.
	if snap.Keyspace.Verdict != string(KeyspaceUnknown) {
		t.Errorf("verdict %q with nothing observed, want %q — a pass here would be a "+
			"number from no measurement", snap.Keyspace.Verdict, KeyspaceUnknown)
	}

	// And the counters must be LIVE, not a constant zero: a snapshot that never
	// moves satisfies the nil-check above while reporting nothing.
	noteKeyspaceEventApplied()
	noteKeyspaceEventApplied()
	live := metrics.Default().Snapshot()
	if live.Keyspace.EventsApplied != 2 {
		t.Errorf("events_applied = %d after two events, want 2 — the gauge is "+
			"constant, so it can never show push failing", live.Keyspace.EventsApplied)
	}
	if live.Keyspace.Verdict != string(KeyspaceWorking) {
		t.Errorf("verdict %q with events observed, want %q",
			live.Keyspace.Verdict, KeyspaceWorking)
	}
}

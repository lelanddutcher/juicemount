package nfs

import (
	"encoding/json"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// These tests assert the WIRING, not the function.
//
// InflightStats and snapshotJukebox both worked correctly for months and were
// both unreadable, because nothing called them from the metrics path —
// InflightStats' own doc comment claimed "Exposed for the metrics endpoint"
// while its only caller was a >=22s watchdog. Six mechanisms shipped
// correct-but-uncalled in a single prior sprint. A test that calls
// InflightStats() directly would have passed the entire time the telemetry was
// invisible, so it would prove nothing. Every assertion below therefore goes
// through metrics.Default().Snapshot() — the same call /metrics makes.

func TestStallProviderIsRegisteredWithMetrics(t *testing.T) {
	snap := metrics.Default().Snapshot()
	if snap.Stall == nil {
		t.Fatal("Snapshot().Stall is nil — the stall provider was never registered, " +
			"so in-flight RPCs and JUKEBOX totals are invisible to /metrics exactly " +
			"as they were before this fix")
	}
}

func TestJukeboxCountsReachTheMetricsSnapshot(t *testing.T) {
	before := metrics.Default().Snapshot().Stall
	if before == nil {
		t.Fatal("stall provider not registered")
	}
	start := before.JukeboxByOp["nfs.TESTOP"]

	recordJukebox("nfs.TESTOP")
	recordJukebox("nfs.TESTOP")

	after := metrics.Default().Snapshot().Stall
	if got := after.JukeboxByOp["nfs.TESTOP"] - start; got != 2 {
		t.Errorf("nfs.TESTOP JUKEBOX delta = %d, want 2 — a JUKEBOX storm (the "+
			"mechanism behind Finder error 100060) would not be visible on /metrics", got)
	}
	if after.JukeboxTotal < before.JukeboxTotal+2 {
		t.Errorf("JukeboxTotal did not advance: %d -> %d", before.JukeboxTotal, after.JukeboxTotal)
	}
}

func TestInflightRPCIsVisibleWhileItRuns(t *testing.T) {
	// The whole point is seeing a hang WHILE it hangs. Hold an RPC open and
	// assert the scrape sees it, rather than checking after it completed.
	id := inflightRegister("nfs.SLOWTEST")
	snap := metrics.Default().Snapshot().Stall
	inflightDone(id)

	if snap == nil {
		t.Fatal("stall provider not registered")
	}
	if snap.Inflight < 1 {
		t.Errorf("Inflight = %d while an RPC was open, want >= 1 — an operator "+
			"could not watch oldest_age_ms climb toward the client timeout", snap.Inflight)
	}
	if snap.OldestOp == "" {
		t.Error("OldestOp empty while an RPC was open — a stall would not name its own op")
	}
}

func TestStallSnapshotSerializesUnderTheExpectedJSONKeys(t *testing.T) {
	// test/perf/metrics_delta.py and cell_scoreboard.py address /metrics by
	// literal key. A field rename is silent to Go and fatal to the harness.
	b, err := json.Marshal(metrics.Default().Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	stall, ok := m["stall"].(map[string]any)
	if !ok {
		t.Fatal(`/metrics JSON has no "stall" object`)
	}
	for _, k := range []string{"inflight", "oldest_age_ms", "jukebox_total"} {
		if _, ok := stall[k]; !ok {
			t.Errorf("stall.%s missing from /metrics JSON", k)
		}
	}
}

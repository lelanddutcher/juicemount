package metadata

import (
	"encoding/json"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// These assert the WIRING, not the counting. A counter that increments
// correctly and reaches no reader is the defect this codebase keeps paying
// for — InflightStats claimed in its own doc comment to be "exposed for the
// metrics endpoint" while its only caller was a watchdog, and the keyspace
// verdict read "working" off a counter fed by the wrong subscription. So every
// assertion below goes through metrics.Default().Snapshot(), the same call
// /metrics makes.

func TestRedisProviderIsRegisteredWithMetrics(t *testing.T) {
	if metrics.Default().Snapshot().Redis == nil {
		t.Fatal("Snapshot().Redis is nil — the provider was never registered, so " +
			"Redis round trips are invisible to /metrics exactly as they were " +
			"before this hook existed")
	}
}

func TestCommandsReachTheMetricsSnapshot(t *testing.T) {
	before := metrics.Default().Snapshot().Redis
	if before == nil {
		t.Fatal("provider not registered")
	}
	redisCommands.Add(3)
	after := metrics.Default().Snapshot().Redis
	if got := after.Commands - before.Commands; got != 3 {
		t.Errorf("commands delta = %d, want 3 — the gauge is constant, so it can "+
			"never show metadata chatter", got)
	}
	if got := after.RoundTrips - before.RoundTrips; got != 3 {
		t.Errorf("round_trips delta = %d, want 3", got)
	}
}

// THE UNIT DECISION, pinned. A pipeline is ONE network exchange no matter how
// many commands it carries. Counting its commands individually would make
// pipelining — the actual fix for a high-RTT link — look like the problem.
func TestPipelineCostsOneRoundTripNotN(t *testing.T) {
	before := metrics.Default().Snapshot().Redis
	redisPipelines.Add(1)
	redisPipelined.Add(50) // fifty commands in that single batch
	after := metrics.Default().Snapshot().Redis

	if got := after.RoundTrips - before.RoundTrips; got != 1 {
		t.Errorf("a 50-command pipeline moved round_trips by %d, want 1. Pipelining "+
			"exists to turn N trips into one; charging it N would recommend "+
			"exactly the wrong change on a 300ms link", got)
	}
	if got := after.PipelinedCommands - before.PipelinedCommands; got != 50 {
		t.Errorf("pipelined_commands delta = %d, want 50 — the command count is "+
			"still worth reporting, just not as round trips", got)
	}
}

// Dials are round trips too: on a flapping cellular link, reconnects are not
// noise.
func TestDialsCountAsRoundTrips(t *testing.T) {
	before := metrics.Default().Snapshot().Redis
	redisDials.Add(2)
	after := metrics.Default().Snapshot().Redis
	if got := after.RoundTrips - before.RoundTrips; got != 2 {
		t.Errorf("dials moved round_trips by %d, want 2", got)
	}
}

func TestRedisSnapshotSerializesUnderTheExpectedKeys(t *testing.T) {
	// test/perf/cellular_efficiency.py addresses /metrics by literal key; a
	// rename is silent to Go and fatal to the harness.
	b, err := json.Marshal(metrics.Default().Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	r, ok := m["redis"].(map[string]any)
	if !ok {
		t.Fatal(`/metrics JSON has no "redis" object`)
	}
	for _, k := range []string{"commands", "pipelines", "pipelined_commands",
		"dials", "round_trips"} {
		if _, ok := r[k]; !ok {
			t.Errorf("redis.%s missing from /metrics JSON", k)
		}
	}
}

package metrics

import (
	"encoding/json"
	"testing"
	"time"
)

// The whole point of exposing buckets is that SUBTRACTING two snapshots yields
// percentiles over the interval between them. A cumulative percentile cannot do
// that: after a long run it is dominated by history and barely moves.
func TestBucketDeltaMeasuresOnlyTheInterval(t *testing.T) {
	r := NewRegistry()
	// History: 10,000 fast RPCs. A lifetime percentile is now pinned by these.
	for i := 0; i < 10000; i++ {
		r.Observe(RPCRead, 100*time.Microsecond, nil)
	}
	before := r.Snapshot().RPCs[string(RPCRead)]

	// The interval under test: 100 SLOW RPCs.
	for i := 0; i < 100; i++ {
		r.Observe(RPCRead, 300*time.Millisecond, nil)
	}
	after := r.Snapshot().RPCs[string(RPCRead)]

	if len(after.Buckets) == 0 {
		t.Fatal("buckets are not exposed, so no interval measurement is possible")
	}
	if len(after.Buckets) != len(before.Buckets) {
		t.Fatalf("bucket count changed between snapshots: %d then %d",
			len(before.Buckets), len(after.Buckets))
	}

	// The cumulative view barely notices — that is the problem being solved.
	if after.P50Us > 1000 {
		t.Fatalf("expected the lifetime p50 to stay pinned near the fast history, got %.0f us", after.P50Us)
	}

	// The delta sees only the slow interval.
	bounds := r.Snapshot().RPCBucketBoundsUs
	if len(bounds) != len(after.Buckets) {
		t.Fatalf("bounds/buckets length mismatch: %d vs %d", len(bounds), len(after.Buckets))
	}
	var total, slow uint64
	for i := range after.Buckets {
		d := after.Buckets[i] - before.Buckets[i]
		total += d
		if bounds[i] >= 100_000 { // 100ms and slower
			slow += d
		}
	}
	if total != 100 {
		t.Fatalf("interval should contain exactly the 100 RPCs issued, got %d", total)
	}
	if slow != 100 {
		t.Fatalf("all 100 interval RPCs were 300ms and must land in a >=100ms bucket, got %d", slow)
	}
}

// Bounds must ship with the counts, or the numbers cannot be interpreted.
func TestBucketBoundsAreEmittedAndSerialize(t *testing.T) {
	r := NewRegistry()
	r.Observe(RPCRead, time.Millisecond, nil)
	var out Snapshot
	b, err := json.Marshal(r.Snapshot())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out.RPCBucketBoundsUs) == 0 {
		t.Fatal("bucket bounds did not survive JSON — the counts would be uninterpretable")
	}
	if len(out.RPCs[string(RPCRead)].Buckets) != len(out.RPCBucketBoundsUs) {
		t.Fatalf("buckets (%d) and bounds (%d) must be the same length",
			len(out.RPCs[string(RPCRead)].Buckets), len(out.RPCBucketBoundsUs))
	}
}

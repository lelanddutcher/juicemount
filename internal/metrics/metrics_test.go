package metrics

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRegistryObserve(t *testing.T) {
	r := NewRegistry()

	r.Observe(RPCGetAttr, 100*time.Microsecond, nil)
	r.Observe(RPCGetAttr, 200*time.Microsecond, nil)
	r.Observe(RPCGetAttr, 50*time.Microsecond, errors.New("boom"))
	r.Observe(RPCRead, 5*time.Millisecond, nil)
	r.AddBytesRead(1024)
	r.AddBytesWritten(2048)

	snap := r.Snapshot()

	if snap.RPCTotal != 4 {
		t.Errorf("RPCTotal = %d, want 4", snap.RPCTotal)
	}
	if snap.RPCErrors != 1 {
		t.Errorf("RPCErrors = %d, want 1", snap.RPCErrors)
	}
	if snap.BytesRead != 1024 {
		t.Errorf("BytesRead = %d, want 1024", snap.BytesRead)
	}
	if snap.BytesWritten != 2048 {
		t.Errorf("BytesWritten = %d, want 2048", snap.BytesWritten)
	}

	getattr, ok := snap.RPCs["GETATTR"]
	if !ok {
		t.Fatal("expected GETATTR in snapshot")
	}
	if getattr.Count != 3 {
		t.Errorf("GETATTR count = %d, want 3", getattr.Count)
	}
	if getattr.MaxUs == 0 {
		t.Error("expected non-zero MaxUs after observations")
	}
	if getattr.MeanUs == 0 {
		t.Error("expected non-zero MeanUs")
	}
}

func TestNavLatencyCounters(t *testing.T) {
	r := NewRegistry()

	// Increment each nav-latency counter a distinct number of times so a
	// mis-wired Inc method (wrong field) is caught by the exact-count asserts.
	r.IncReaddirMirrorHit()
	r.IncReaddirMirrorHit()
	r.IncReaddirEmptyRefill()
	r.IncReaddirColdShed()
	r.IncReaddirColdShed()
	r.IncReaddirColdShed()
	r.IncLookupHit()
	r.IncLookupHit()
	r.IncLookupHit()
	r.IncLookupHit()
	r.IncLookupNoent()
	r.IncReadColdSubread()
	r.IncReadColdSubread()
	r.IncReadWarmSubread()
	r.IncReadaheadTriggered()
	r.AddReadaheadPrefetchedBlocks(7)
	r.AddReadaheadPrefetchedBlocks(0)  // no-op guard
	r.AddReadaheadPrefetchedBlocks(-5) // negative guard

	// S6 H2 stale-recovery graders.
	r.IncRecoverLstat()
	r.IncRecoverLstat()
	r.IncRecoverLstat()
	r.IncRecoverLstat()
	r.IncRecoverLstat()
	r.IncRecoverStale()
	r.IncRecoverStale()
	r.IncRecoverSuccess()

	// S6 paged-readdir cache graders.
	r.IncReaddirVerifierHit()
	r.IncReaddirVerifierHit()
	r.IncReaddirVerifierHit()
	r.IncReaddirVerifierHit()
	r.IncReaddirVerifierHit()
	r.IncReaddirVerifierHit()
	r.IncReaddirFsReaddir()
	r.IncReaddirFsReaddir()

	// #104 spool zero-tail detection.
	r.IncZeroTailSuspect()
	r.IncZeroTailSuspect()

	// S6 H1 admission-wait grader. Exercise the CAS-max gauge, both threshold
	// buckets, and the us<=0 drop (a sub-microsecond wait records nothing).
	r.ObserveAdmitWait(5 * time.Millisecond)  // max=5000, over_1ms=1
	r.ObserveAdmitWait(2 * time.Millisecond)  // max stays 5000, over_1ms=2
	r.ObserveAdmitWait(100 * time.Nanosecond) // rounds to 0µs → dropped entirely
	r.ObserveAdmitWait(12 * time.Millisecond) // max=12000, over_1ms=3, over_10ms=1

	snap := r.Snapshot()

	checks := []struct {
		name string
		got  uint64
		want uint64
	}{
		{"readdir_mirror_hit", snap.ReaddirMirrorHit, 2},
		{"readdir_empty_refill", snap.ReaddirEmptyRefill, 1},
		{"readdir_cold_shed", snap.ReaddirColdShed, 3},
		{"lookup_hit", snap.LookupHit, 4},
		{"lookup_noent", snap.LookupNoent, 1},
		{"read_cold_subread", snap.ReadColdSubread, 2},
		{"read_warm_subread", snap.ReadWarmSubread, 1},
		{"readahead_triggered", snap.ReadaheadTriggered, 1},
		{"readahead_prefetched_blocks", snap.ReadaheadPrefetchedBlocks, 7},
		{"recover_lstat_total", snap.RecoverLstat, 5},
		{"recover_stale_total", snap.RecoverStale, 2},
		{"recover_success_total", snap.RecoverSuccess, 1},
		{"readdir_verifier_hit", snap.ReaddirVerifierHit, 6},
		{"readdir_fs_readdir", snap.ReaddirFsReaddir, 2},
		{"rpc_admit_wait_us", snap.RPCAdmitWaitUs, 12000},
		{"rpc_admit_wait_over_1ms", snap.RPCAdmitWaitOver1ms, 3},
		{"rpc_admit_wait_over_10ms", snap.RPCAdmitWaitOver10ms, 1},
		{"zero_tail_suspect_total", snap.ZeroTailSuspect, 2},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}

	// The counters must serialize to /metrics under their documented JSON keys.
	blob, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	js := string(blob)
	for _, key := range []string{
		`"readdir_mirror_hit":2`,
		`"readdir_empty_refill":1`,
		`"readdir_cold_shed":3`,
		`"lookup_hit":4`,
		`"lookup_noent":1`,
		`"read_cold_subread":2`,
		`"read_warm_subread":1`,
		`"readahead_triggered":1`,
		`"readahead_prefetched_blocks":7`,
		`"recover_lstat_total":5`,
		`"recover_stale_total":2`,
		`"recover_success_total":1`,
		`"readdir_verifier_hit":6`,
		`"readdir_fs_readdir":2`,
		`"rpc_admit_wait_us":12000`,
		`"rpc_admit_wait_over_1ms":3`,
		`"rpc_admit_wait_over_10ms":1`,
		`"zero_tail_suspect_total":2`,
	} {
		if !strings.Contains(js, key) {
			t.Errorf("serialized /metrics JSON missing %q\nfull: %s", key, js)
		}
	}
}

// TestAdmitWaitGauge pins the H1 admission-wait mechanism (S6): the max-gauge
// is monotonic (a smaller wait after a larger one does NOT lower it), a zero /
// sub-microsecond wait records nothing, and the threshold buckets are exact.
func TestAdmitWaitGauge(t *testing.T) {
	r := NewRegistry()

	// Nothing observed yet → all zero (the warm/uncontended expectation).
	if s := r.Snapshot(); s.RPCAdmitWaitUs != 0 || s.RPCAdmitWaitOver1ms != 0 || s.RPCAdmitWaitOver10ms != 0 {
		t.Fatalf("fresh registry admit-wait not zero: %+v", s)
	}

	r.ObserveAdmitWait(0)                     // exact zero → dropped
	r.ObserveAdmitWait(500 * time.Nanosecond) // sub-µs → rounds to 0 → dropped
	if s := r.Snapshot(); s.RPCAdmitWaitUs != 0 || s.RPCAdmitWaitOver1ms != 0 {
		t.Fatalf("sub-µs / zero wait should record nothing, got %+v", s)
	}

	r.ObserveAdmitWait(8 * time.Millisecond) // max=8000, over_1ms=1
	r.ObserveAdmitWait(3 * time.Millisecond) // SMALLER: max must stay 8000, over_1ms=2
	s := r.Snapshot()
	if s.RPCAdmitWaitUs != 8000 {
		t.Errorf("max-gauge not monotonic: rpc_admit_wait_us = %d, want 8000", s.RPCAdmitWaitUs)
	}
	if s.RPCAdmitWaitOver1ms != 2 {
		t.Errorf("rpc_admit_wait_over_1ms = %d, want 2", s.RPCAdmitWaitOver1ms)
	}
	if s.RPCAdmitWaitOver10ms != 0 {
		t.Errorf("rpc_admit_wait_over_10ms = %d, want 0 (no wait >= 10ms yet)", s.RPCAdmitWaitOver10ms)
	}

	r.ObserveAdmitWait(25 * time.Millisecond) // max=25000, over_1ms=3, over_10ms=1
	s = r.Snapshot()
	if s.RPCAdmitWaitUs != 25000 || s.RPCAdmitWaitOver1ms != 3 || s.RPCAdmitWaitOver10ms != 1 {
		t.Errorf("after 25ms wait: got max=%d over1ms=%d over10ms=%d, want 25000/3/1",
			s.RPCAdmitWaitUs, s.RPCAdmitWaitOver1ms, s.RPCAdmitWaitOver10ms)
	}
}

// BenchmarkObserveAdmitWait guards the QA-35 invariant that the H1 admission
// grader's recording path (taken only when the acquire actually blocked) is
// atomic-only and allocation-free. The uncontended fast path in conn.go records
// nothing at all, so this benchmarks the worst case — the contended branch.
func BenchmarkObserveAdmitWait(b *testing.B) {
	r := NewRegistry()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.ObserveAdmitWait(5 * time.Millisecond)
	}
}

func TestRegistryStableShape(t *testing.T) {
	r := NewRegistry()
	snap := r.Snapshot()
	// All canonical RPC types should be present even with no samples.
	for _, want := range trackedTypes {
		if _, ok := snap.RPCs[string(want)]; !ok {
			t.Errorf("missing canonical rpc %q in empty snapshot", want)
		}
	}
}

func TestPercentile(t *testing.T) {
	r := NewRegistry()
	for i := 0; i < 100; i++ {
		r.Observe(RPCRead, time.Duration(i+1)*time.Microsecond, nil)
	}
	snap := r.Snapshot()
	rd := snap.RPCs["READ"]
	if rd.Count != 100 {
		t.Fatalf("count = %d, want 100", rd.Count)
	}
	if rd.P50Us <= 0 || rd.P95Us <= 0 || rd.P99Us <= 0 {
		t.Errorf("expected non-zero percentiles, got p50=%.1f p95=%.1f p99=%.1f",
			rd.P50Us, rd.P95Us, rd.P99Us)
	}
	if rd.P50Us > rd.P95Us || rd.P95Us > rd.P99Us {
		t.Errorf("percentile order broken: p50=%.1f p95=%.1f p99=%.1f",
			rd.P50Us, rd.P95Us, rd.P99Us)
	}
}

func TestNFSObserverMapping(t *testing.T) {
	cases := []struct {
		prog uint32
		proc uint32
		want RPCType
	}{
		{nfsProgram, procGetAttr, RPCGetAttr},
		{nfsProgram, procRead, RPCRead},
		{nfsProgram, procWrite, RPCWrite},
		{nfsProgram, procReadDirPlus, RPCReadDirPlus},
		{nfsProgram, 999, RPCOther},
		{12345, procRead, RPCOther},
	}
	for _, c := range cases {
		got := rpcTypeFor(c.prog, c.proc)
		if got != c.want {
			t.Errorf("rpcTypeFor(%d,%d) = %q, want %q", c.prog, c.proc, got, c.want)
		}
	}
}

func TestServerEndpoints(t *testing.T) {
	r := NewRegistry()
	r.Observe(RPCGetAttr, 50*time.Microsecond, nil)
	r.SetHealthProvider(func() HealthSnapshot {
		return HealthSnapshot{Healthy: true, Components: map[string]string{"redis": "ok"}}
	})
	srv := NewServer("127.0.0.1:0", r)
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()

	addr := srv.Addr()
	if !strings.Contains(addr, ":") {
		t.Fatalf("unexpected addr: %q", addr)
	}

	// /metrics
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var out Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}
	if out.RPCTotal != 1 {
		t.Errorf("expected 1 RPC, got %d", out.RPCTotal)
	}

	// /health
	resp2, err := http.Get("http://" + addr + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp2.StatusCode)
	}
	var h HealthSnapshot
	if err := json.NewDecoder(resp2.Body).Decode(&h); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if !h.Healthy {
		t.Error("expected healthy=true")
	}
}

func TestServerHealthDegradedStatus(t *testing.T) {
	r := NewRegistry()
	r.SetHealthProvider(func() HealthSnapshot {
		return HealthSnapshot{Healthy: false, Reason: "down"}
	})
	srv := NewServer("127.0.0.1:0", r)
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()
	resp, err := http.Get("http://" + srv.Addr() + "/health")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

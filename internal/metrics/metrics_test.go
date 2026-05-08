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

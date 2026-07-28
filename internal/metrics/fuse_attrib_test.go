package metrics

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestObserveFUSECallAttribution pins the core contract: a call is recorded
// against the (source, op) cell it was labeled with — and NOT any neighbour.
// Distinct counts per cell mean a mis-indexed slot is caught, not masked.
func TestObserveFUSECallAttribution(t *testing.T) {
	r := NewRegistry()

	// FOREGROUND: 3 lstats, 10ms each, uncontended (zero gate wait).
	for i := 0; i < 3; i++ {
		r.ObserveFUSECall(FUSESrcForeground, FUSEOpLstat, FUSEGateNFSLstat, 1,
			0, 10*time.Millisecond, FUSEOutcomeOK)
	}
	// SIDECAR WARM: 2 opens on the FOREGROUND gate (the known doctrine gap),
	// each having queued 4ms for a slot.
	for i := 0; i < 2; i++ {
		r.ObserveFUSECall(FUSESrcSidecarWarm, FUSEOpOpen, FUSEGateNFSLstat, 24,
			4*time.Millisecond, 30*time.Millisecond, FUSEOutcomeOK)
	}
	// PREFETCH: 1 readdir on the background gate that TIMED OUT in the syscall.
	r.ObserveFUSECall(FUSESrcPrefetch, FUSEOpReadDir, FUSEGatePrefetch, 2,
		0, 800*time.Millisecond, FUSEOutcomeTimeout)
	// DIR REFRESH: 1 direntry-info that never even got a gate slot.
	r.ObserveFUSECall(FUSESrcDirRefresh, FUSEOpDirEntryInfo, FUSEGatePrefetch, 0,
		800*time.Millisecond, 800*time.Millisecond, FUSEOutcomeGateTimeout)

	snap := r.Snapshot().FUSEAttrib

	// --- per-cell ---------------------------------------------------------
	cells := map[string]FUSECellSnapshot{}
	for _, c := range snap.Cells {
		cells[c.Source+"/"+c.Op] = c
	}
	if len(cells) != 4 {
		t.Fatalf("expected exactly 4 non-zero cells, got %d: %+v", len(cells), snap.Cells)
	}

	fg := cells["foreground/lstat"]
	if fg.Calls != 3 {
		t.Errorf("foreground/lstat calls = %d, want 3", fg.Calls)
	}
	if fg.TotalNs != uint64(30*time.Millisecond) {
		t.Errorf("foreground/lstat total_ns = %d, want %d", fg.TotalNs, uint64(30*time.Millisecond))
	}
	if fg.MeanUs != 10000 {
		t.Errorf("foreground/lstat mean_us = %v, want 10000", fg.MeanUs)
	}
	if fg.GateWaitNs != 0 {
		t.Errorf("foreground/lstat gate_wait_ns = %d, want 0 (uncontended acquires record an exact zero)", fg.GateWaitNs)
	}
	if fg.Timeouts != 0 || fg.GateTimeouts != 0 {
		t.Errorf("foreground/lstat timeouts = %d/%d, want 0/0", fg.Timeouts, fg.GateTimeouts)
	}

	sw := cells["sidecar_warm/open"]
	if sw.Calls != 2 || sw.GateWaitNs != uint64(8*time.Millisecond) {
		t.Errorf("sidecar_warm/open = calls %d gate_wait_ns %d, want 2 / %d",
			sw.Calls, sw.GateWaitNs, uint64(8*time.Millisecond))
	}

	pf := cells["prefetch/readdir"]
	if pf.Calls != 1 || pf.Timeouts != 1 || pf.GateTimeouts != 0 {
		t.Errorf("prefetch/readdir = calls %d timeouts %d gate_timeouts %d, want 1/1/0",
			pf.Calls, pf.Timeouts, pf.GateTimeouts)
	}

	dr := cells["dir_refresh/direntry_info"]
	if dr.Calls != 1 || dr.Timeouts != 1 || dr.GateTimeouts != 1 {
		t.Errorf("dir_refresh/direntry_info = calls %d timeouts %d gate_timeouts %d, want 1/1/1",
			dr.Calls, dr.Timeouts, dr.GateTimeouts)
	}

	// --- rollups ----------------------------------------------------------
	// Every source and op key is present even at zero (stable JSON shape).
	for s := FUSESource(0); s < NumFUSESources; s++ {
		if _, ok := snap.BySource[s.String()]; !ok {
			t.Errorf("by_source missing stable key %q", s)
		}
	}
	for o := FUSEOp(0); o < NumFUSEOps; o++ {
		if _, ok := snap.ByOp[o.String()]; !ok {
			t.Errorf("by_op missing stable key %q", o)
		}
	}
	if got := snap.BySource["foreground"].Calls; got != 3 {
		t.Errorf("by_source[foreground].calls = %d, want 3", got)
	}
	if got := snap.BySource["thumb_warm"].Calls; got != 0 {
		t.Errorf("by_source[thumb_warm].calls = %d, want 0 (never observed)", got)
	}
	if got := snap.ByOp["open"].Calls; got != 2 {
		t.Errorf("by_op[open].calls = %d, want 2", got)
	}
	if got := snap.ByOp["lstat"].MeanUs; got != 10000 {
		t.Errorf("by_op[lstat].mean_us = %v, want 10000", got)
	}

	// --- gates ------------------------------------------------------------
	// nfs_lstat took 5 slots (3 foreground lstats + 2 sidecar-warm opens), 2
	// of which had to queue. High-water depth is the sidecar warm's 24 — i.e.
	// the FOREGROUND budget was observed FULL while serving BACKGROUND work.
	nl := snap.Gates["nfs_lstat"]
	if nl.Acquires != 5 || nl.Blocked != 2 || nl.MaxDepth != 24 || nl.Timeouts != 0 {
		t.Errorf("gate nfs_lstat = acquires %d blocked %d max_depth %d timeouts %d, want 5/2/24/0",
			nl.Acquires, nl.Blocked, nl.MaxDepth, nl.Timeouts)
	}
	if nl.WaitNs != uint64(8*time.Millisecond) {
		t.Errorf("gate nfs_lstat wait_ns = %d, want %d", nl.WaitNs, uint64(8*time.Millisecond))
	}
	// prefetch: one acquire (the timed-out readdir still HELD a slot) and one
	// abandoned acquire (the gate-timeout never took one).
	pg := snap.Gates["prefetch"]
	if pg.Acquires != 1 || pg.Timeouts != 1 || pg.MaxDepth != 2 {
		t.Errorf("gate prefetch = acquires %d timeouts %d max_depth %d, want 1/1/2",
			pg.Acquires, pg.Timeouts, pg.MaxDepth)
	}
	if _, ok := snap.Gates["fuse_fstat"]; !ok {
		t.Error("gates missing stable key fuse_fstat")
	}
}

// TestObserveFUSECallGuards: out-of-range enums must never corrupt a
// neighbouring slot, and FUSEGateNone must record the cell but no gate.
func TestObserveFUSECallGuards(t *testing.T) {
	r := NewRegistry()

	r.ObserveFUSECall(FUSESource(200), FUSEOpStat, FUSEGateNone, 0, 0, time.Millisecond, FUSEOutcomeOK)
	r.ObserveFUSECall(FUSESrcForeground, FUSEOp(200), FUSEGateNFSLstat, 1, 0, time.Millisecond, FUSEOutcomeOK)

	snap := r.Snapshot().FUSEAttrib
	if got := snap.BySource["other"].Calls; got != 1 {
		t.Errorf("out-of-range source should fall into `other`: calls = %d, want 1", got)
	}
	if got := snap.BySource["foreground"].Calls; got != 0 {
		t.Errorf("out-of-range op must be dropped, not recorded: foreground calls = %d, want 0", got)
	}
	for name, g := range snap.Gates {
		if g.Acquires != 0 || g.Timeouts != 0 {
			t.Errorf("gate %s recorded %d acquires / %d timeouts from guarded calls, want 0/0",
				name, g.Acquires, g.Timeouts)
		}
	}
}

// TestFUSEAttribJSONShape: the attribution block must be present, valid, and
// stable in the /metrics payload — with `cells` an ARRAY (never null; a null
// aborts the whole Swift decode, the CacheStatus roots:null bug class).
func TestFUSEAttribJSONShape(t *testing.T) {
	r := NewRegistry()

	blob, err := json.Marshal(r.Snapshot())
	if err != nil {
		t.Fatalf("marshal empty snapshot: %v", err)
	}
	js := string(blob)
	if !strings.Contains(js, `"fuse_attrib"`) {
		t.Fatalf("/metrics JSON missing fuse_attrib: %s", js)
	}
	if !strings.Contains(js, `"cells":[]`) {
		t.Errorf("fuse_attrib.cells must serialize as an empty ARRAY, not null: %s", js)
	}
	for _, key := range []string{`"by_source"`, `"by_op"`, `"gates"`, `"nfs_lstat"`, `"prefetch"`, `"fuse_fstat"`} {
		if !strings.Contains(js, key) {
			t.Errorf("/metrics JSON missing %s", key)
		}
	}

	r.ObserveFUSECall(FUSESrcThumbWarm, FUSEOpOpen, FUSEGateNFSLstat, 3,
		time.Millisecond, 250*time.Millisecond, FUSEOutcomeOK)
	blob, err = json.Marshal(r.Snapshot())
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	// Round-trip: the payload must decode cleanly back into a Snapshot.
	var back Snapshot
	if err := json.Unmarshal(blob, &back); err != nil {
		t.Fatalf("round-trip decode: %v", err)
	}
	if len(back.FUSEAttrib.Cells) != 1 ||
		back.FUSEAttrib.Cells[0].Source != "thumb_warm" ||
		back.FUSEAttrib.Cells[0].Op != "open" {
		t.Fatalf("round-tripped cells = %+v, want one thumb_warm/open row", back.FUSEAttrib.Cells)
	}
	if back.FUSEAttrib.Cells[0].MeanUs != 250000 {
		t.Errorf("round-tripped mean_us = %v, want 250000", back.FUSEAttrib.Cells[0].MeanUs)
	}
}

// TestFUSEGateProvider: live gate depth/cap comes from the registered
// provider (internal/metrics cannot import nfs), and is inert when unset.
func TestFUSEGateProvider(t *testing.T) {
	r := NewRegistry()
	if g := r.Snapshot().FUSEAttrib.Gates["nfs_lstat"]; g.Cap != 0 || g.Depth != 0 {
		t.Fatalf("no provider registered → depth/cap must read 0, got %+v", g)
	}
	r.SetFUSEGateProvider(func() [NumFUSEGates]FUSEGateLevel {
		var out [NumFUSEGates]FUSEGateLevel
		out[FUSEGateNFSLstat] = FUSEGateLevel{Depth: 19, Cap: 24}
		out[FUSEGatePrefetch] = FUSEGateLevel{Depth: 2, Cap: 2}
		out[FUSEGateFstat] = FUSEGateLevel{Depth: 0, Cap: 8}
		return out
	})
	gates := r.Snapshot().FUSEAttrib.Gates
	if gates["nfs_lstat"].Depth != 19 || gates["nfs_lstat"].Cap != 24 {
		t.Errorf("nfs_lstat = %+v, want depth 19 cap 24", gates["nfs_lstat"])
	}
	if gates["prefetch"].Depth != 2 || gates["fuse_fstat"].Cap != 8 {
		t.Errorf("prefetch/fuse_fstat levels wrong: %+v %+v", gates["prefetch"], gates["fuse_fstat"])
	}
}

// BenchmarkObserveFUSECall guards the QA-35 invariant that the recording path
// added to every bounded FUSE metadata helper is atomic-only and
// allocation-free — a fixed array index plus a handful of atomic adds. This is
// the per-call overhead the instrumentation adds; compare it against what the
// helper already pays per call (a time.Timer, a buffered channel and a
// goroutine — hundreds of ns and 3 allocations) and against the FUSE syscall
// itself (µs warm, up to the full 800ms/2s budget cold).
func BenchmarkObserveFUSECall(b *testing.B) {
	r := NewRegistry()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.ObserveFUSECall(FUSESrcForeground, FUSEOpLstat, FUSEGateNFSLstat, 4,
			0, 120*time.Microsecond, FUSEOutcomeOK)
	}
}

// BenchmarkObserveFUSECallContended is the worst case: a blocked gate acquire,
// so the gate-wait counters and the CAS-max depth gauge all fire too.
func BenchmarkObserveFUSECallContended(b *testing.B) {
	r := NewRegistry()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.ObserveFUSECall(FUSESrcSidecarWarm, FUSEOpOpen, FUSEGateNFSLstat, 24,
			3*time.Millisecond, 40*time.Millisecond, FUSEOutcomeOK)
	}
}

// BenchmarkObserveFUSECallParallel checks the recording path under real
// concurrency (the 48-way sidecar warmer + foreground RPCs all hitting the
// same cells): still no allocation, no lock.
func BenchmarkObserveFUSECallParallel(b *testing.B) {
	r := NewRegistry()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.ObserveFUSECall(FUSESrcForeground, FUSEOpLstat, FUSEGateNFSLstat, 4,
				0, 120*time.Microsecond, FUSEOutcomeOK)
		}
	})
}

package nfs

// End-to-end FUSE attribution: drive the REAL bounded helpers against a temp
// directory standing in for the FUSE mount and assert /metrics attributes each
// call to the source that issued it — the map of who touches FUSE.
//
// All assertions are DELTAS against metrics.Default(), which is process-global
// and shared with every other test in this package.

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// fuseCellCounts is a flattened (source/op) → row view of a snapshot.
func fuseCellCounts(t *testing.T) map[string]metrics.FUSECellSnapshot {
	t.Helper()
	out := map[string]metrics.FUSECellSnapshot{}
	for _, c := range metrics.Default().Snapshot().FUSEAttrib.Cells {
		out[c.Source+"/"+c.Op] = c
	}
	return out
}

func cellDelta(before, after map[string]metrics.FUSECellSnapshot, key string) (calls, timeouts, gateTimeouts uint64, totalNs uint64) {
	b, a := before[key], after[key]
	return a.Calls - b.Calls, a.Timeouts - b.Timeouts, a.GateTimeouts - b.GateTimeouts, a.TotalNs - b.TotalNs
}

// TestFUSEAttributionEndToEnd: each bounded helper, called with a source
// label, lands in that source's cell — and nowhere else. This is the test that
// would catch a call site classified as foreground when it is really a
// background warmer (or vice versa), which is the whole point of the exercise.
func TestFUSEAttributionEndToEnd(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "clip.mov")
	if err := os.WriteFile(file, []byte("bytes"), 0o644); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		key  string
		run  func()
	}{
		{
			name: "foreground stat",
			key:  "foreground/stat",
			run: func() {
				if _, _, ok := statWithTimeout(metrics.FUSESrcForeground, file, 2*time.Second); !ok {
					t.Fatal("statWithTimeout: unexpected timeout")
				}
			},
		},
		{
			name: "foreground lstat",
			key:  "foreground/lstat",
			run: func() {
				if _, ok := lstatWithTimeout(metrics.FUSESrcForeground, file, 2*time.Second); !ok {
					t.Fatal("lstatWithTimeout: unexpected timeout")
				}
			},
		},
		{
			// The async phantom-purge confirmation runs OFF the RPC path but
			// still on the FOREGROUND gate — it must not read as foreground.
			name: "phantom purge lstat_notexist",
			key:  "phantom_purge/lstat_notexist",
			run: func() {
				if _, ok := lstatNotExistWithTimeout(metrics.FUSESrcPhantomPurge, file, 2*time.Second); !ok {
					t.Fatal("lstatNotExistWithTimeout: unexpected timeout")
				}
			},
		},
		{
			name: "prefetch readdir",
			key:  "prefetch/readdir",
			run: func() {
				if _, _, ok := readDirWithTimeout(metrics.FUSESrcPrefetch, dir, 2*time.Second, prefetchGate); !ok {
					t.Fatal("readDirWithTimeout: unexpected timeout")
				}
			},
		},
		{
			name: "dir refresh readdir",
			key:  "dir_refresh/readdir",
			run: func() {
				if _, _, ok := readDirWithTimeout(metrics.FUSESrcDirRefresh, dir, 2*time.Second, prefetchGate); !ok {
					t.Fatal("readDirWithTimeout: unexpected timeout")
				}
			},
		},
		{
			// The sidecar warmer is BACKGROUND work drawing on the FOREGROUND
			// gate (openFileWithTimeout hard-codes nfsLstatGate). Proving it
			// shows up as sidecar_warm — not foreground — is exactly the
			// attribution the retune depends on.
			name: "sidecar warm open",
			key:  "sidecar_warm/open",
			run: func() {
				f, _, ok := openFileWithTimeout(metrics.FUSESrcSidecarWarm, file, os.O_RDONLY, 0, 2*time.Second)
				if !ok {
					t.Fatal("openFileWithTimeout: unexpected timeout")
				}
				if f != nil {
					_ = f.Close()
				}
			},
		},
		{
			name: "thumb warm open",
			key:  "thumb_warm/open",
			run: func() {
				f, _, ok := openFileWithTimeout(metrics.FUSESrcThumbWarm, file, os.O_RDONLY, 0, 2*time.Second)
				if !ok {
					t.Fatal("openFileWithTimeout: unexpected timeout")
				}
				if f != nil {
					_ = f.Close()
				}
			},
		},
		{
			name: "foreground mkdirall",
			key:  "foreground/mkdirall",
			run: func() {
				if _, ok := mkdirAllWithTimeout(metrics.FUSESrcForeground, filepath.Join(dir, "sub"), 0o755, 2*time.Second); !ok {
					t.Fatal("mkdirAllWithTimeout: unexpected timeout")
				}
			},
		},
		{
			name: "foreground symlink",
			key:  "foreground/symlink",
			run: func() {
				if _, ok := symlinkWithTimeout(metrics.FUSESrcForeground, "clip.mov", filepath.Join(dir, "link.mov"), 2*time.Second); !ok {
					t.Fatal("symlinkWithTimeout: unexpected timeout")
				}
			},
		},
		{
			name: "foreground chmod",
			key:  "foreground/chmod",
			run: func() {
				if _, ok := chmodWithTimeout(metrics.FUSESrcForeground, file, 0o600, 2*time.Second); !ok {
					t.Fatal("chmodWithTimeout: unexpected timeout")
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := fuseCellCounts(t)
			tc.run()
			after := fuseCellCounts(t)

			calls, timeouts, gateTimeouts, totalNs := cellDelta(before, after, tc.key)
			if calls != 1 {
				t.Fatalf("%s: calls delta = %d, want 1", tc.key, calls)
			}
			if timeouts != 0 || gateTimeouts != 0 {
				t.Errorf("%s: healthy call recorded timeouts %d / gate_timeouts %d, want 0/0",
					tc.key, timeouts, gateTimeouts)
			}
			if totalNs == 0 {
				t.Errorf("%s: total_ns delta = 0, want a measured duration", tc.key)
			}
			// Nothing else moved: a mis-indexed slot would show up here.
			for key := range after {
				if key == tc.key {
					continue
				}
				if n, _, _, _ := cellDelta(before, after, key); n != 0 {
					t.Errorf("%s: unrelated cell %s also moved by %d", tc.key, key, n)
				}
			}
		})
	}
}

// TestFUSEAttributionDirEntryInfo: infoWithTimeout is the one helper reached
// from BOTH a blocked client RPC and a background mirror warm (via
// coldDirListing), so the source must travel with the gate.
func TestFUSEAttributionDirEntryInfo(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(dir)
	if err != nil || len(ents) != 1 {
		t.Fatalf("ReadDir: %v (%d entries)", err, len(ents))
	}

	for _, src := range []struct {
		src  metrics.FUSESource
		gate chan struct{}
		key  string
	}{
		{metrics.FUSESrcForeground, nfsLstatGate, "foreground/direntry_info"},
		{metrics.FUSESrcDirRefresh, prefetchGate, "dir_refresh/direntry_info"},
	} {
		before := fuseCellCounts(t)
		if _, _, ok := infoWithTimeout(src.src, ents[0], 2*time.Second, src.gate); !ok {
			t.Fatalf("%s: infoWithTimeout unexpected timeout", src.key)
		}
		after := fuseCellCounts(t)
		if calls, _, _, _ := cellDelta(before, after, src.key); calls != 1 {
			t.Errorf("%s: calls delta = %d, want 1", src.key, calls)
		}
	}
}

// TestFUSEAttributionSyscallTimeout: a wedged FUSE syscall (gate acquired, the
// call never returns) must record a timeout but NOT a gate timeout. Timeouts
// are the NFS3ERR_JUKEBOX generator we are chasing, so the two must not be
// conflated.
func TestFUSEAttributionSyscallTimeout(t *testing.T) {
	gate := make(chan struct{}, 1) // not one of the three real gates
	de := &blockingDirEntry{name: "wedged.mov", release: make(chan struct{})}
	defer close(de.release)

	before := fuseCellCounts(t)
	if _, _, ok := infoWithTimeout(metrics.FUSESrcSidecarWarm, de, 50*time.Millisecond, gate); ok {
		t.Fatal("wedged Info() must report ok=false")
	}
	after := fuseCellCounts(t)

	calls, timeouts, gateTimeouts, totalNs := cellDelta(before, after, "sidecar_warm/direntry_info")
	if calls != 1 || timeouts != 1 {
		t.Errorf("wedged call: calls %d timeouts %d, want 1/1", calls, timeouts)
	}
	if gateTimeouts != 0 {
		t.Errorf("wedged SYSCALL must not count as a GATE timeout, got %d", gateTimeouts)
	}
	if totalNs < uint64(50*time.Millisecond) {
		t.Errorf("total_ns = %d, want >= the 50ms budget the caller actually paid", totalNs)
	}
}

// TestFUSEAttributionGateTimeout: a FULL gate means the budget is burned
// QUEUEING and the syscall never starts — pure head-of-line blocking. That has
// to be distinguishable from a slow daemon, because the fixes differ.
func TestFUSEAttributionGateTimeout(t *testing.T) {
	dir := t.TempDir()
	gate := make(chan struct{}, 1)
	gate <- struct{}{} // saturate: no slot available for the whole budget

	before := fuseCellCounts(t)
	start := time.Now()
	if _, _, ok := readDirWithTimeout(metrics.FUSESrcPrefetch, dir, 60*time.Millisecond, gate); ok {
		t.Fatal("a saturated gate must report ok=false")
	}
	elapsed := time.Since(start)
	after := fuseCellCounts(t)

	calls, timeouts, gateTimeouts, totalNs := cellDelta(before, after, "prefetch/readdir")
	if calls != 1 || timeouts != 1 || gateTimeouts != 1 {
		t.Errorf("gate-timeout call: calls %d timeouts %d gate_timeouts %d, want 1/1/1",
			calls, timeouts, gateTimeouts)
	}
	if totalNs == 0 {
		t.Error("gate-timeout call recorded no duration — the caller paid the full budget")
	}
	// Sanity: the helper still honors its deadline (behavior unchanged).
	if elapsed > time.Second {
		t.Errorf("saturated-gate call took %v, want ~60ms", elapsed)
	}
}

// TestFUSEGateLevelsRegistered: the nfs package registers the live gate
// provider at init, so /metrics reports the real capacities of the three
// bounded gates — the head-of-line point for navigation, previously
// completely unmeasured.
func TestFUSEGateLevelsRegistered(t *testing.T) {
	gates := metrics.Default().Snapshot().FUSEAttrib.Gates
	for name, wantCap := range map[string]int{
		"nfs_lstat":  cap(nfsLstatGate),
		"prefetch":   cap(prefetchGate),
		"fuse_fstat": cap(fuseFstatGate),
	} {
		g, ok := gates[name]
		if !ok {
			t.Fatalf("/metrics missing gate %q", name)
		}
		if g.Cap != wantCap {
			t.Errorf("gate %s cap = %d, want %d", name, g.Cap, wantCap)
		}
		if g.Depth < 0 || g.Depth > g.Cap {
			t.Errorf("gate %s depth = %d, out of range [0,%d]", name, g.Depth, g.Cap)
		}
	}
}

// TestFUSEAttributionGateAccounting: a real-gate call must move that gate's
// acquire/depth counters, and a call on a non-tracked gate must move none of
// them (FUSEGateNone).
func TestFUSEAttributionGateAccounting(t *testing.T) {
	dir := t.TempDir()

	before := metrics.Default().Snapshot().FUSEAttrib.Gates
	if _, _, ok := readDirWithTimeout(metrics.FUSESrcDirRefresh, dir, 2*time.Second, prefetchGate); !ok {
		t.Fatal("readDirWithTimeout: unexpected timeout")
	}
	after := metrics.Default().Snapshot().FUSEAttrib.Gates

	if d := after["prefetch"].Acquires - before["prefetch"].Acquires; d != 1 {
		t.Errorf("prefetch gate acquires delta = %d, want 1", d)
	}
	if after["prefetch"].MaxDepth < 1 {
		t.Errorf("prefetch gate max_depth = %d, want >= 1 after an acquire", after["prefetch"].MaxDepth)
	}

	// Same call on an ad-hoc gate: attributed to the cell, but to no gate.
	adhoc := make(chan struct{}, 2)
	before = metrics.Default().Snapshot().FUSEAttrib.Gates
	if _, _, ok := readDirWithTimeout(metrics.FUSESrcDirRefresh, dir, 2*time.Second, adhoc); !ok {
		t.Fatal("readDirWithTimeout(adhoc): unexpected timeout")
	}
	after = metrics.Default().Snapshot().FUSEAttrib.Gates
	for _, name := range []string{"nfs_lstat", "prefetch", "fuse_fstat"} {
		if d := after[name].Acquires - before[name].Acquires; d != 0 {
			t.Errorf("untracked gate leaked %d acquires into %s", d, name)
		}
	}
}

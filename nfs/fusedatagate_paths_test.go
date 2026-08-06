package nfs

import (
	"os"
	"path/filepath"
	"testing"
)

// fillFUSEDataGate saturates the ceiling and returns a cleanup that drains it.
//
// Every test below asserts "this path is GATED" by proving it refuses when the
// ceiling is full. That shape is deliberate: it fails if the acquire is deleted,
// which is the regression that actually matters. Asserting the happy path would
// pass just as well with no gate at all.
func fillFUSEDataGate(t *testing.T) func() {
	t.Helper()
	if !fuseDataGateEnabled {
		t.Skip("ceiling disabled via JM_FUSE_DATA_GATE=0")
	}
	width := int(effectiveFUSEDataWidth())
	held := make([]func(), 0, width)
	for i := 0; i < width; i++ {
		r, ok := acquireFUSEData()
		if !ok {
			for _, rel := range held {
				rel()
			}
			t.Fatalf("could not fill the ceiling at slot %d/%d", i, width)
		}
		held = append(held, r)
	}
	return func() {
		for _, r := range held {
			r()
		}
	}
}

// billyFile.ReadAt was the largest ungated FUSE data path.
//
// It is the NFS READ hot path for the no-metadata-cache open, and its only
// shaping was defaultReadQoS — which its own comment describes as "Inert on
// medium/fast". So on the 10GbE LAN where the machine kernel-panicked twice on
// 2026-08-05 it was bounded only by rpcSem (128), eight times the concurrency
// that demonstrably killed the session.
func TestBillyFileReadAtIsGated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.bin")
	if err := os.WriteFile(path, []byte("juicemount"), 0o644); err != nil {
		t.Fatal(err)
	}
	fh, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()

	f := &billyFile{File: fh, name: "data.bin", handler: &JuiceMountHandler{}}

	// Sanity: ungated, the read succeeds. Without this the refusal below could
	// be a broken fixture rather than the gate doing its job.
	buf := make([]byte, 4)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatalf("baseline read failed, fixture is wrong: %v", err)
	}

	drain := fillFUSEDataGate(t)
	defer drain()

	_, _, refusalsBefore := FUSEDataGateStats()
	n, err := f.ReadAt(buf, 0)
	if err != errFUSETimeout {
		t.Fatalf("read past a full ceiling: n=%d err=%v, want errFUSETimeout — "+
			"is the acquireFUSEData still in billyFile.ReadAt?", n, err)
	}
	if _, _, after := FUSEDataGateStats(); after != refusalsBefore+1 {
		t.Errorf("refusals %d -> %d, want +1: a shed that is not counted is "+
			"invisible in the field", refusalsBefore, after)
	}
}

// The PREFETCHER must be gated, and must shed rather than wait.
//
// Readahead adds up to 8 concurrent readers on a fast link — half the 16-wide
// ceiling — spent on blocks nobody has asked for yet. Ungated, that is a third
// of the budget the ceiling exists to protect, handed to speculative work
// during exactly the bulk operation that panicked the machine.
func TestPrefetchIsGated(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "big.bin"),
		make([]byte, 2*readaheadBlockSize), 0o644); err != nil {
		t.Fatal(err)
	}

	// Baseline FIRST, ungated. Without it a shed inside prefetch for an
	// unrelated reason (bulk-QoS token, worker budget, fd open) would make the
	// gated assertion below pass while proving nothing.
	base := NewReadaheadManager(dir, nil, nil)
	base.prefetch("big.bin", 0, 2*readaheadBlockSize, 4)
	base.statsMu.Lock()
	baseline := base.prefetched
	base.statsMu.Unlock()
	if baseline == 0 {
		t.Fatal("baseline prefetch read nothing — the fixture never reached the " +
			"block loop, so this test cannot discriminate")
	}

	drain := fillFUSEDataGate(t)
	defer drain()

	rm := NewReadaheadManager(dir, nil, nil)
	rm.prefetch("big.bin", 0, 2*readaheadBlockSize, 4)
	rm.statsMu.Lock()
	got := rm.prefetched
	rm.statsMu.Unlock()
	if got != 0 {
		t.Errorf("prefetched %d blocks past a full ceiling (baseline %d) — is the "+
			"tryAcquireFUSEDataBackground still in the block loop?", got, baseline)
	}
}

// The MEMBUF loader must be gated, and on refusal must publish NOTHING.
//
// This loop pulls a whole file through FUSE in the background. It cannot skip a
// block the way prefetch can: the torn-read guard rejects a short load because
// publishing a partial buffer as complete is the 2026-06-15 black-frame bug. So
// the correct refusal behaviour is to abandon the load entirely, and this test
// pins that it does — a gate that instead left a hole would be worse than none.
func TestMemBufLoadIsGatedAndPublishesNothing(t *testing.T) {
	dir := t.TempDir()
	const size = 4096
	path := filepath.Join(dir, "clip.mov")
	if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
		t.Fatal(err)
	}

	drain := fillFUSEDataGate(t)
	defer drain()

	mb := NewMemoryBuffer(1<<20, 1<<24)
	entry := &memBufEntry{size: size, loading: true, ready: make(chan struct{})}
	mb.mu.Lock()
	mb.entries["clip.mov"] = entry
	mb.mu.Unlock()
	mb.loadSem <- struct{}{} // loadFile releases this on the way out

	mb.loadFile("clip.mov", path, size, entry)

	<-entry.ready // loadFile always closes this; a hang here is itself a failure

	mb.mu.Lock()
	_, stillThere := mb.entries["clip.mov"]
	mb.mu.Unlock()
	if stillThere {
		t.Error("membuf published an entry loaded past a full ceiling — on refusal " +
			"the load must be abandoned, never completed or partially published")
	}
	if entry.data != nil {
		t.Errorf("entry carries %d bytes after a refused load; must be nil", len(entry.data))
	}
}

// A refused background acquire must return a usable no-op release.
//
// (That background work never WAITS is already covered by
// TestBackgroundAcquireNeverWaits in fusedatagate_test.go, which also bounds the
// yield time — not duplicated here.)
//
// readahead and membuf both call the release unconditionally after their read.
// A nil or double-decrementing release on the refusal path would corrupt the
// in-flight counter and, over time, either wedge the gate shut or open it wide —
// the second being the failure mode that ends in another kernel panic.
func TestBackgroundRefusalDoesNotMoveOccupancy(t *testing.T) {
	drain := fillFUSEDataGate(t)
	before, _ := fuseDataGateDepth()

	release, ok := tryAcquireFUSEDataBackground()
	if ok {
		release()
		drain()
		t.Fatal("admitted past a full ceiling")
	}
	release() // the no-op path callers always run

	after, _ := fuseDataGateDepth()
	drain()
	if after != before {
		t.Errorf("occupancy moved on a REFUSED acquire: %d -> %d", before, after)
	}
	if final, _ := fuseDataGateDepth(); final != 0 {
		t.Errorf("gate did not drain to empty: %d still in flight", final)
	}
}

package nfs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// Lever 1 (JM_DRAIN_BATCH_INSERT) tests. These exercise the drain metadata
// write-coalescer through the SAME spool-wired handler stack production uses
// (newSpoolWiredHandler → SetSpool wires SetOnBatchDrainComplete →
// metadata.Store.BatchDrainComplete), with the flag toggled via t.Setenv BEFORE
// the drainer is constructed (NewDrainer reads the env once).

// newBatchWiredHandler is newSpoolWiredHandler with JM_DRAIN_BATCH_INSERT=1 set
// before construction, so the drainer comes up with its coalescer enabled.
func newBatchWiredHandler(t *testing.T) (*juiceFS, *SpoolStore, *Drainer, string) {
	t.Helper()
	t.Setenv("JM_DRAIN_BATCH_INSERT", "1")
	return newSpoolWiredHandler(t)
}

// newBatchWiredHandlerNoDispatch builds a batch-enabled drainer that is NOT
// started (no dispatch loop), with all production hooks wired manually so
// DrainOnceForTest exercises the real batched-commit path deterministically.
// maxOps lets a test set the op threshold without writing 256 files; a maxOps
// of 0 keeps the production default. Because the dispatcher never runs, tests
// can safely configure the batcher before driving drains — no field-write race.
func newBatchWiredHandlerNoDispatch(t *testing.T, maxOps int) (*juiceFS, *SpoolStore, *Drainer, string) {
	t.Helper()
	t.Setenv("JM_DRAIN_BATCH_INSERT", "1")
	fuseRoot := t.TempDir()
	spoolDir := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "meta.db")

	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	handler := NewHandler(store, fuseRoot)
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatalf("init spool schema: %v", err)
	}
	meta := metadata.NewSpoolStore(store.DB())
	spool, err := NewSpoolStore(spoolDir, 0, meta)
	if err != nil {
		t.Fatalf("new spool store: %v", err)
	}
	drainer, err := NewDrainer(spool, DrainerConfig{
		FuseRoot:     fuseRoot,
		BackoffBase:  5 * time.Millisecond,
		PollFallback: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new drainer: %v", err)
	}
	if drainer.batch == nil {
		t.Fatal("expected batcher wired with flag on")
	}
	if maxOps > 0 {
		drainer.batch = newDrainBatcher(drainer, maxOps, drainBatchMaxDelay)
	}
	// SetSpool(spool, nil) does NOT wire the drainer hooks (they only wire when
	// drainer != nil); wire them here as production's SetSpool(spool, drainer)
	// would, so the batched commit path is exercised end to end.
	handler.SetSpool(spool, nil)
	drainer.SetOnSizeReady(handler.publishDrainedSize)
	drainer.SetOnDrainComplete(handler.onSpoolDrained)
	drainer.SetOnBatchDrainComplete(store.BatchDrainComplete)
	t.Cleanup(func() {
		handler.StopHandler()
		_ = store.Close()
	})
	return &juiceFS{handler: handler}, spool, drainer, fuseRoot
}

// writeReadySpool writes a payload into the spool and finalizes it to `ready`
// (sweepOnce(0)) so the next DrainOnceForTest scan picks it up.
func writeReadySpool(t *testing.T, s *SpoolStore, nfsPath string, payload []byte) {
	t.Helper()
	e, err := s.OpenWrite(nfsPath)
	if err != nil {
		t.Fatalf("open write %s: %v", nfsPath, err)
	}
	if _, err := e.WriteAt(payload, 0); err != nil {
		t.Fatalf("write %s: %v", nfsPath, err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close %s: %v", nfsPath, err)
	}
}

// waitForDrains blocks until DrainsSucceeded reaches want and the drainer is
// idle (InFlight==0), or fails on timeout.
func waitForDrains(t *testing.T, d *Drainer, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if d.Metrics().DrainsSucceeded.Load() >= want && d.Metrics().InFlight.Load() == 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d drains: got succeeded=%d inFlight=%d",
		want, d.Metrics().DrainsSucceeded.Load(), d.Metrics().InFlight.Load())
}

// TestDrainBatchEnabledWiresCoalescer confirms the flag actually turns the
// batcher on (and, in the sibling test below, that OFF leaves it nil) — so the
// A/B is a real toggle, not a no-op.
func TestDrainBatchEnabledWiresCoalescer(t *testing.T) {
	_, _, drainer, _ := newBatchWiredHandler(t)
	if drainer.batch == nil {
		t.Fatal("JM_DRAIN_BATCH_INSERT=1 but drainer.batch is nil (coalescer not wired)")
	}
	if !drainer.batchInsert {
		t.Fatal("JM_DRAIN_BATCH_INSERT=1 but drainer.batchInsert is false")
	}
}

// TestDrainBatchDisabledIsByteIdentical asserts the flag-off path leaves the
// coalescer entirely absent (nil batcher), so drains take the unchanged
// per-file path — and still complete correctly.
func TestDrainBatchDisabledIsByteIdentical(t *testing.T) {
	// Explicitly OFF (default), constructed via the plain helper.
	t.Setenv("JM_DRAIN_BATCH_INSERT", "0")
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandler(t)
	if drainer.batch != nil {
		t.Fatal("flag off but drainer.batch is non-nil (coalescer must be absent)")
	}
	if drainer.batchInsert {
		t.Fatal("flag off but drainer.batchInsert is true")
	}

	payload := []byte("flag-off drains via the per-file path unchanged")
	simulateNFSCreateThenWrite(t, jfs, "off.bin", payload, 1<<20)
	spool.sweepOnce(0)

	waitForDrains(t, drainer, 1, 5*time.Second)

	got, err := os.ReadFile(filepath.Join(fuseRoot, "off.bin"))
	if err != nil {
		t.Fatalf("drained file missing: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("content mismatch: got %q want %q", got, payload)
	}
	// Post-drain Stat sees real size via the per-file onSizeReady path.
	fi, err := jfs.Stat("off.bin")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() != int64(len(payload)) {
		t.Errorf("post-drain size=%d, want %d", fi.Size(), len(payload))
	}
}

// TestDrainBatchFlushesOnSizeThreshold drives MORE than one batch worth of
// small files through a single DrainOnceForTest scan and asserts every one
// drains — the op-count threshold flushing mid-stream (at 3 and 6), not only at
// the final idle flush. Uses a tiny maxOps on a NON-started drainer so there is
// no dispatcher race on the batcher field.
func TestDrainBatchFlushesOnSizeThreshold(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newBatchWiredHandlerNoDispatch(t, 3)
	_ = jfs

	// workers*2 == 8 rows per scan; write 8 so one scan dispatches all of them
	// and the op threshold (3) flushes twice mid-batch.
	const n = 8
	names := make([]string, n)
	for i := 0; i < n; i++ {
		names[i] = "/burst/f" + string(rune('a'+i)) + ".bin"
		writeReadySpool(t, spool, names[i], []byte("payload-"+names[i]))
	}
	spool.sweepOnce(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := drainer.DrainOnceForTest(ctx)
	if got != n {
		t.Fatalf("DrainOnceForTest processed %d rows, want %d", got, n)
	}
	if s := drainer.Metrics().DrainsSucceeded.Load(); s != n {
		t.Fatalf("DrainsSucceeded=%d, want %d (op-threshold flush dropped files?)", s, n)
	}

	for _, name := range names {
		if _, err := os.Stat(filepath.Join(fuseRoot, name[1:])); err != nil {
			t.Errorf("file %s not drained to FUSE: %v", name, err)
		}
	}
	if used, _ := spool.Capacity(); used != 0 {
		t.Errorf("capacity not released after batched drains: used=%d, want 0", used)
	}
}

// TestDrainBatchFlushesOnTimeThreshold drives a SINGLE file (well under the op
// threshold) and asserts it still drains — proving the time/idle threshold
// flushes a partial batch rather than stranding it until 256 ops that never
// arrive.
func TestDrainBatchFlushesOnTimeThreshold(t *testing.T) {
	_, spool, drainer, fuseRoot := newBatchWiredHandler(t)
	// Keep the default large op threshold (256): the ONLY thing that can flush a
	// single-file batch is the time/idle path.

	payload := []byte("a lone file that must flush on the timer")
	e, err := spool.OpenWrite("/lonely.bin")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	if _, err := e.WriteAt(payload, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	spool.sweepOnce(0)

	waitForDrains(t, drainer, 1, 5*time.Second)

	if _, err := os.Stat(filepath.Join(fuseRoot, "lonely.bin")); err != nil {
		t.Errorf("lone file not drained (partial batch stranded?): %v", err)
	}
	if used, _ := spool.Capacity(); used != 0 {
		t.Errorf("capacity not released: used=%d, want 0", used)
	}
}

// TestDrainBatchSizePublishBeforeEviction is the task #65 invariant under
// batching: after the drain completes, a fresh Stat resolves the file's REAL
// size (not the Create-time 0 / a stale partial) AND the spool shadow is gone
// (index entry evicted, spool file removed). If the batch evicted the shadow
// before publishing the size, this Stat would read 0.
func TestDrainBatchSizePublishBeforeEviction(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newBatchWiredHandler(t)

	const size = 3 << 20 // 3 MiB
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i*11 + 5)
	}
	simulateNFSCreateThenWrite(t, jfs, "sized.mov", payload, 1<<20)
	spool.sweepOnce(0)

	waitForDrains(t, drainer, 1, 5*time.Second)

	// Post-drain, drainer idle: the metadata size must be the real size — this
	// is the batched size-publish committed before the shadow eviction.
	fi, err := jfs.Stat("sized.mov")
	if err != nil {
		t.Fatalf("post-drain Stat: %v", err)
	}
	if fi.Size() != int64(size) {
		t.Errorf("post-drain Stat size=%d, want %d — size-publish-before-eviction VIOLATED under batching", fi.Size(), size)
	}

	// The spool shadow must be gone (evicted only AFTER the size committed).
	if _, ok := spool.LookupActive("/sized.mov"); ok {
		t.Errorf("spool index entry still present after batched drain (shadow not evicted)")
	}
	// FUSE file present at full size.
	if st, err := os.Stat(filepath.Join(fuseRoot, "sized.mov")); err != nil || st.Size() != int64(size) {
		t.Errorf("FUSE file wrong: err=%v size=%v want=%d", err, st, size)
	}
	// Capacity released.
	if used, _ := spool.Capacity(); used != 0 {
		t.Errorf("capacity not released: used=%d, want 0", used)
	}
}

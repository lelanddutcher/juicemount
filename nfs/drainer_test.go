package nfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// newTestDrainer wires up a SpoolStore + Drainer with a temp dir
// playing the role of the FUSE root. drainOne's "copy to FUSE" lands
// in that temp dir.
func newTestDrainer(t *testing.T, cfg DrainerConfig) (*SpoolStore, *Drainer) {
	t.Helper()
	spool := newTestSpoolStore(t, 0)
	fuseRoot := t.TempDir()
	cfg.FuseRoot = fuseRoot
	// Fast backoff for tests.
	if cfg.BackoffBase == 0 {
		cfg.BackoffBase = 5 * time.Millisecond
	}
	if cfg.PollFallback == 0 {
		cfg.PollFallback = 100 * time.Millisecond
	}
	d, err := NewDrainer(spool, cfg)
	if err != nil {
		t.Fatalf("new drainer: %v", err)
	}
	return spool, d
}

func writeSpoolEntry(t *testing.T, s *SpoolStore, nfsPath string, payload []byte) *SpoolEntry {
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
	return e
}

func TestDrainerHappyPath(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})

	payload := []byte("a sample file's worth of bytes")
	e := writeSpoolEntry(t, spool, "/Films/clip.mov", payload)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	n := d.DrainOnceForTest(ctx)
	if n != 1 {
		t.Fatalf("expected 1 row processed, got %d", n)
	}

	// File should exist at FuseRoot/Films/clip.mov.
	dest := filepath.Join(d.fuseRoot, "Films/clip.mov")
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("dest content mismatch: got %q, want %q", got, payload)
	}

	// SQL row should be done.
	row, _ := spool.Meta().Get(e.ID())
	if row.DrainState != metadata.DrainDone {
		t.Errorf("state=%q, want done", row.DrainState)
	}

	// Spool file should be removed.
	if _, err := os.Stat(e.SpoolFilePath()); !os.IsNotExist(err) {
		t.Errorf("spool file should be deleted: stat err=%v", err)
	}

	// Capacity should be released.
	used, _ := spool.Capacity()
	if used != 0 {
		t.Errorf("used=%d, want 0", used)
	}

	// Index entry should be gone.
	if _, ok := spool.LookupActive("/Films/clip.mov"); ok {
		t.Errorf("index entry should be removed after drain")
	}

	// Metrics.
	if d.Metrics().DrainsSucceeded.Load() != 1 {
		t.Errorf("succeeded=%d, want 1", d.Metrics().DrainsSucceeded.Load())
	}
	if d.Metrics().BytesDrained.Load() != int64(len(payload)) {
		t.Errorf("bytes=%d, want %d", d.Metrics().BytesDrained.Load(), len(payload))
	}
}

func TestDrainerWorkerPoolBounded(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{Workers: 2})

	// 8 entries, but only 2 workers should be active concurrently.
	for i := 0; i < 8; i++ {
		writeSpoolEntry(t, spool, "/file"+strconv.Itoa(i), []byte("x"))
	}

	// Watch InFlight metric while DrainOnceForTest runs.
	peak := int64(0)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			cur := d.Metrics().InFlight.Load()
			if cur > peak {
				peak = cur
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = d.DrainOnceForTest(ctx)
	close(stop)

	// Peak InFlight should never have exceeded the worker cap.
	if peak > 2 {
		t.Errorf("InFlight peaked at %d, expected <= 2 (worker cap)", peak)
	}
}

func TestDrainerTransientFailureRetries(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{
		MaxAttempts: 3,
		BackoffBase: 2 * time.Millisecond,
	})

	// Create a ready entry, then make the destination parent read-only
	// so the first drain attempts fail.
	payload := []byte("retry-me")
	e := writeSpoolEntry(t, spool, "/sub/retry.bin", payload)

	// Make the fuse root dir non-writable so MkdirAll for the parent
	// directory fails the FIRST time. We restore writability after
	// one round so the second attempt succeeds.
	if err := os.Chmod(d.fuseRoot, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = d.DrainOnceForTest(ctx)

	// First attempt should have failed and the row should be back to
	// ready with attempts bumped.
	row, _ := spool.Meta().Get(e.ID())
	if row.DrainAttempts < 1 {
		t.Errorf("attempts=%d, want >= 1", row.DrainAttempts)
	}
	if row.DrainState != metadata.DrainReady {
		t.Errorf("after transient fail, state=%q, want ready", row.DrainState)
	}
	if row.LastError == "" {
		t.Errorf("last_error should be populated")
	}

	// Restore writability and drain again.
	if err := os.Chmod(d.fuseRoot, 0o755); err != nil {
		t.Fatalf("chmod restore: %v", err)
	}
	_ = d.DrainOnceForTest(ctx)
	row, _ = spool.Meta().Get(e.ID())
	if row.DrainState != metadata.DrainDone {
		t.Errorf("after recovery, state=%q, want done", row.DrainState)
	}
}

func TestDrainerExhaustsRetries(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{
		MaxAttempts: 2,
		BackoffBase: 1 * time.Millisecond,
	})

	payload := []byte("doomed")
	e := writeSpoolEntry(t, spool, "/doomed.bin", payload)

	// Permanently break the fuse root.
	if err := os.Chmod(d.fuseRoot, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(d.fuseRoot, 0o755) })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Drain twice — first attempt retries, second hits MaxAttempts.
	_ = d.DrainOnceForTest(ctx)
	_ = d.DrainOnceForTest(ctx)

	row, _ := spool.Meta().Get(e.ID())
	if row.DrainState != metadata.DrainFailed {
		t.Errorf("state=%q, want failed after exhausting retries", row.DrainState)
	}
	if d.Metrics().DrainsFailed.Load() == 0 {
		t.Errorf("DrainsFailed counter should be > 0")
	}
}

func TestDrainerMissingSpoolFileMarksFailed(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})

	e := writeSpoolEntry(t, spool, "/gone.bin", []byte("about to disappear"))

	// Delete the spool file out from under the drainer.
	if err := os.Remove(e.SpoolFilePath()); err != nil {
		t.Fatalf("rm: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_ = d.DrainOnceForTest(ctx)

	row, _ := spool.Meta().Get(e.ID())
	if row.DrainState != metadata.DrainFailed {
		t.Errorf("state=%q, want failed (spool file missing)", row.DrainState)
	}
	if row.LastError == "" {
		t.Errorf("last_error should be set")
	}
}

func TestDrainerSHAMismatchQuarantines(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})

	payload := []byte("genuine content")
	e := writeSpoolEntry(t, spool, "/corrupt.bin", payload)

	// Verify the streaming SHA was set (this was a sequential write).
	if e.SHA256() == nil {
		t.Fatal("expected streaming SHA to be set")
	}

	// Corrupt the spool file on disk so drain's re-read SHA mismatches
	// the stored streaming SHA.
	if err := os.WriteFile(e.SpoolFilePath(), []byte("CORRUPTED!!!!!!"), 0o600); err != nil {
		t.Fatalf("corrupt: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_ = d.DrainOnceForTest(ctx)

	row, _ := spool.Meta().Get(e.ID())
	if row.DrainState != metadata.DrainFailed {
		t.Errorf("state=%q, want failed (sha mismatch)", row.DrainState)
	}
	if d.Metrics().Quarantined.Load() != 1 {
		t.Errorf("Quarantined=%d, want 1", d.Metrics().Quarantined.Load())
	}

	// Spool file should be moved to quarantine/, not deleted.
	quarantineFile := filepath.Join(spool.Root(), "quarantine", filepath.Base(e.SpoolFilePath()))
	if _, err := os.Stat(quarantineFile); err != nil {
		t.Errorf("quarantined file not at %s: %v", quarantineFile, err)
	}
	// Destination FUSE file should be removed (the bad bytes we wrote).
	dest := filepath.Join(d.fuseRoot, "corrupt.bin")
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Errorf("corrupt fuse write should have been removed: stat err=%v", err)
	}
}

func TestDrainerOutOfOrderWritesNoSHACompare(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})

	// Write out of order so streaming SHA is invalidated.
	e, _ := spool.OpenWrite("/sparse.bin")
	if _, err := e.WriteAt([]byte("part2"), 5); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := e.WriteAt([]byte("FIRST"), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if e.SHA256() != nil {
		t.Fatal("expected SHA to be nil due to out-of-order writes")
	}
	row, _ := spool.Meta().Get(e.ID())
	if row.SHA256 != nil {
		t.Fatal("SQL sha should be nil for out-of-order writes")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_ = d.DrainOnceForTest(ctx)

	// Without a stored SHA, drain skips comparison and treats success.
	row, _ = spool.Meta().Get(e.ID())
	if row.DrainState != metadata.DrainDone {
		t.Errorf("state=%q, want done (no sha to compare)", row.DrainState)
	}

	// Verify the actual destination file has the right content.
	dest := filepath.Join(d.fuseRoot, "sparse.bin")
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	want := []byte("FIRSTpart2")
	if !bytes.Equal(got, want) {
		t.Errorf("dest=%q, want %q", got, want)
	}
}

func TestDrainerStopWaitsForInFlight(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{Workers: 2})

	// Pre-populate a few entries.
	for i := 0; i < 4; i++ {
		writeSpoolEntry(t, spool, "/stop"+strconv.Itoa(i), make([]byte, 4096))
	}

	d.Start()
	// Give the dispatcher a moment to claim work.
	time.Sleep(50 * time.Millisecond)

	if !d.Stop(2 * time.Second) {
		t.Errorf("Stop should have completed within deadline")
	}

	// All metrics should add up.
	succeeded := d.Metrics().DrainsSucceeded.Load()
	if succeeded == 0 {
		t.Errorf("expected some drains to succeed, got 0")
	}
}

func TestDrainerStopIdempotent(t *testing.T) {
	_, d := newTestDrainer(t, DrainerConfig{})
	d.Start()
	if !d.Stop(1 * time.Second) {
		t.Errorf("first Stop should return true")
	}
	if !d.Stop(1 * time.Second) {
		t.Errorf("second Stop should also return true (no-op)")
	}
}

// TestDrainerHashSpoolFileMatchesStreaming verifies the spool_sha.go
// helper agrees with the streaming hasher on sequential writes.
func TestDrainerHashSpoolFileMatchesStreaming(t *testing.T) {
	spool := newTestSpoolStore(t, 0)
	payload := []byte("the quick brown fox jumps over the lazy juicemount")
	e := writeSpoolEntry(t, spool, "/agree.bin", payload)

	streamingSHA := e.SHA256()
	if streamingSHA == nil {
		t.Fatal("streaming SHA should be set for sequential write")
	}

	diskSHA, n, err := hashSpoolFile(e.SpoolFilePath())
	if err != nil {
		t.Fatalf("hash from disk: %v", err)
	}
	if n != int64(len(payload)) {
		t.Errorf("hashed bytes=%d, want %d", n, len(payload))
	}
	if !bytes.Equal(streamingSHA, diskSHA) {
		t.Errorf("streaming SHA disagrees with disk SHA")
	}

	// And sanity-check against crypto/sha256 directly.
	expect := sha256.Sum256(payload)
	if !bytes.Equal(diskSHA, expect[:]) {
		t.Errorf("disk SHA disagrees with crypto/sha256 of source")
	}
}

func TestDrainerNewDrainerValidation(t *testing.T) {
	spool := newTestSpoolStore(t, 0)
	if _, err := NewDrainer(nil, DrainerConfig{FuseRoot: "/x"}); err == nil {
		t.Errorf("expected nil-spool error")
	}
	if _, err := NewDrainer(spool, DrainerConfig{}); err == nil {
		t.Errorf("expected missing-FuseRoot error")
	}
}


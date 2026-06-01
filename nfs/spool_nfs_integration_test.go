package nfs

import (
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// newSpoolWiredHandler builds a JuiceMountHandler with the spool + drainer
// wired exactly as cmd/jm5/main.go and bridge/cbridge.go do in production,
// with a temp dir standing in for the JuiceFS FUSE mount root. It returns the
// juiceFS billy filesystem (the layer the go-nfs fork's onCreate/onWrite call
// into), the spool, the drainer, and the FUSE root path.
func newSpoolWiredHandler(t *testing.T) (*juiceFS, *SpoolStore, *Drainer, string) {
	t.Helper()
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
	handler.SetSpool(spool, drainer)
	drainer.Start()
	t.Cleanup(func() {
		handler.StopHandler() // stops drainer + spool + fdPool + verifier loop
		_ = store.Close()
	})

	return &juiceFS{handler: handler}, spool, drainer, fuseRoot
}

// simulateNFSCreateThenWrite drives the EXACT billy.Filesystem calls the
// go-nfs fork makes for a Finder file copy:
//
//   - CREATE RPC → internal/nfs/nfs_oncreate.go:82  fs.Create(path); file.Close()
//   - each WRITE → internal/nfs/nfs_onwrite.go:56    fs.OpenFile(path, os.O_RDWR); WriteAt; Close
//
// This is the seam where Finding 1 lived: the spool write branch was gated on
// os.O_CREATE, which NEITHER of these calls ever sets — so the spool was inert
// on the live write path. Driving the real call shapes here (rather than
// calling SpoolStore.OpenWrite directly, as the unit tests do) is what makes
// this an honest certification of the integration.
func simulateNFSCreateThenWrite(t *testing.T, jfs *juiceFS, name string, payload []byte, chunk int) {
	t.Helper()

	// CREATE RPC: create then immediately close the 0-byte handle.
	cf, err := jfs.Create(name)
	if err != nil {
		t.Fatalf("Create(%s): %v", name, err)
	}
	if err := cf.Close(); err != nil {
		t.Fatalf("Create.Close(%s): %v", name, err)
	}

	// WRITE RPCs: reopen (O_RDWR), positioned write, close — per RPC.
	for off := 0; off < len(payload); off += chunk {
		end := off + chunk
		if end > len(payload) {
			end = len(payload)
		}
		wf, err := jfs.OpenFile(name, os.O_RDWR, 0o644)
		if err != nil {
			t.Fatalf("OpenFile(%s) write @%d: %v", name, off, err)
		}
		wa, ok := wf.(io.WriterAt)
		if !ok {
			t.Fatalf("write handle %T does not implement io.WriterAt", wf)
		}
		if _, err := wa.WriteAt(payload[off:end], int64(off)); err != nil {
			t.Fatalf("WriteAt(%s) @%d: %v", name, off, err)
		}
		if err := wf.Close(); err != nil {
			t.Fatalf("write Close(%s) @%d: %v", name, off, err)
		}
	}
}

// TestSpoolInterceptsNFSCreateWriteSequence is the certification test for
// Finding 1. It drives a real NFS create+write sequence with the spool
// enabled and asserts the bytes were routed THROUGH the spool and drained
// into the FUSE root — not silently written straight to FUSE by the legacy
// path. Pre-fix (spool gated on O_CREATE) the spool is never touched, so
// sweepOnce finalizes nothing and the drainer never runs: this test fails.
func TestSpoolInterceptsNFSCreateWriteSequence(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandler(t)

	const size = 5 << 20 // 5 MiB across several 1 MiB WRITE RPCs
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i*7 + 3)
	}
	want := sha256.Sum256(payload)

	simulateNFSCreateThenWrite(t, jfs, "clip.mov", payload, 1<<20)

	// The writer has gone idle (every per-RPC handle is closed). Force the
	// idle finalize the production sweeper does on its timer.
	if n := spool.sweepOnce(0); n < 1 {
		t.Fatalf("FINDING 1 REGRESSION: the NFS create/write sequence never "+
			"routed through the spool (sweepOnce finalized %d entries). "+
			"The spool is inert on the live write path.", n)
	}

	// Wait for the drainer to copy the spool file into the FUSE root.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && drainer.Metrics().DrainsSucceeded.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := drainer.Metrics().DrainsSucceeded.Load(); got < 1 {
		t.Fatalf("FINDING 1 REGRESSION: drainer never drained the file "+
			"(DrainsSucceeded=%d) — bytes did not move out of the spool into FUSE.", got)
	}

	// The file must now exist in the FUSE root with byte-identical content.
	got, err := os.ReadFile(filepath.Join(fuseRoot, "clip.mov"))
	if err != nil {
		t.Fatalf("drained file missing from FUSE root: %v", err)
	}
	if gotSHA := sha256.Sum256(got); gotSHA != want {
		t.Fatalf("drained content mismatch: len(got)=%d want=%d\n sha got=%x\n sha want=%x",
			len(got), len(payload), gotSHA, want)
	}

	// Spool capacity must be released once the entry drained.
	if used, _ := spool.Capacity(); used != 0 {
		t.Errorf("spool capacity not released after drain: used=%d, want 0", used)
	}
}

// TestSpoolLeavesInPlaceModifyOfExistingFileOnLegacyPath is the safety guard
// for the routing rule. A WRITE to a path that already exists in FUSE (and has
// NO active spool entry — i.e. it was created in a prior session) must NOT be
// diverted into a fresh empty spool entry, because draining that entry would
// os.Create(dest) and truncate the existing file. Such writes must stay on the
// legacy in-place FUSE path.
func TestSpoolLeavesInPlaceModifyOfExistingFileOnLegacyPath(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandler(t)

	// A file that already exists in FUSE from a "previous session".
	original := []byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA") // 32 bytes
	if err := os.WriteFile(filepath.Join(fuseRoot, "existing.bin"), original, 0o644); err != nil {
		t.Fatalf("seed existing file: %v", err)
	}

	// In-place modify: overwrite the first 4 bytes via an O_RDWR WRITE RPC.
	wf, err := jfs.OpenFile("existing.bin", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile in-place: %v", err)
	}
	if _, err := wf.(io.WriterAt).WriteAt([]byte("ZZZZ"), 0); err != nil {
		t.Fatalf("WriteAt in-place: %v", err)
	}
	if err := wf.Close(); err != nil {
		t.Fatalf("Close in-place: %v", err)
	}

	// It must NOT have gone through the spool (no entry created, no drain).
	if n := spool.sweepOnce(0); n != 0 {
		t.Errorf("in-place modify of an existing file routed through the spool "+
			"(sweepOnce finalized %d) — would truncate the file on drain", n)
	}
	if got := drainer.Metrics().DrainsSucceeded.Load(); got != 0 {
		t.Errorf("drainer ran for an in-place modify (DrainsSucceeded=%d)", got)
	}

	// And the in-place edit must be visible with the tail preserved.
	got, err := os.ReadFile(filepath.Join(fuseRoot, "existing.bin"))
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	want := append([]byte("ZZZZ"), original[4:]...)
	if string(got) != string(want) {
		t.Errorf("in-place modify corrupted: got %q want %q", got, want)
	}
}

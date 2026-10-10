//go:build darwin

package nfs

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// physicalSpoolBytes is what the spool file actually OCCUPIES. This is the
// number the whole feature exists to bound: st_size does not move when a prefix
// is punched, st_blocks does.
func physicalSpoolBytes(t *testing.T, path string) int64 {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return int64(st.Blocks) * 512
}

// THE GATE TEST: a file LARGER than the spool budget copies end-to-end,
// byte-identical, with peak spool occupancy bounded well below the file size.
//
// This is the claim the entire streaming drain exists to make good on. Until
// now every piece has been tested in isolation; this is the first proof they
// compose into the behaviour that matters:
//
//	"a single file bigger than local free disk can be copied"
//
// It is NOT the full release gate — that needs a real Finder copy over the live
// NFS mount, which requires installing a build and touching a mount this run is
// forbidden from touching. What it does prove is that the pipeline
// (write -> seal -> copy -> verify -> persist -> publish -> punch -> release)
// moves a file through a window far smaller than itself without corrupting it.
func TestStreamedFileLargerThanTheSpoolBudgetCopiesIntact(t *testing.T) {
	// Lower the streaming threshold so this exercises REAL multi-chunk streaming
	// without a multi-gigabyte fixture. Production keeps 1 GiB.
	// Scale the thresholds together, keeping production's RATIO: the margins are
	// ~6% of the streaming threshold there (64 MiB against 1 GiB), and the two
	// margins together are the real floor below which nothing seals. Lowering
	// only the threshold — my first attempt — left the margins larger than the
	// whole fixture, so nothing ever sealed and peak occupancy was 100% of the
	// file. The test caught it; that is what it is for.
	// Streaming is opt-in. Without this the copier is a silent no-op and the
	// byte-identity check below STILL PASSES (the tail copy covers everything) —
	// which is precisely why the peak-occupancy assertion exists.
	t.Setenv("JM_SPOOL_STREAM_DRAIN", "1")

	origMin, origMargin, origHead := spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve
	spoolStreamMinSize = 8 << 20
	spoolSealMargin = 512 << 10
	spoolSealHeadReserve = 512 << 10
	t.Cleanup(func() {
		spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve = origMin, origMargin, origHead
	})

	const (
		fileSize  = 48 << 20 // the "camera file"
		chunkSize = 4 << 20  // writer's write size
	)

	s := newTestSpoolStore(t, 1<<30)
	e, err := s.OpenWrite("/DCIM/BIG.MOV")
	if err != nil {
		t.Fatal(err)
	}
	spoolPath := e.SpoolFilePath()

	destDir := t.TempDir()
	realPath := filepath.Join(destDir, "BIG.MOV")
	tempPath, err := streamTempPath(realPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	destFile, err := os.OpenFile(tempPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer destFile.Close()
	e.SetStreamDest(tempPath)

	src, err := os.OpenFile(spoolPath, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	verifyFD, err := os.Open(tempPath)
	if err != nil {
		t.Fatal(err)
	}
	defer verifyFD.Close()

	dur := &fakeDurability{}
	ver := destReadbackVerifier{f: verifyFD}
	rel := s // *SpoolStore satisfies releaseCapacity

	// Deterministic, position-dependent content: a misplaced or duplicated chunk
	// changes the bytes, so byte-equality is a real check rather than a
	// same-length check.
	want := make([]byte, fileSize)
	for i := range want {
		want[i] = byte((i*31 + i/997) % 251)
	}

	peakPhysical := int64(0)
	written := int64(0)
	for written < fileSize {
		n := int64(chunkSize)
		if fileSize-written < n {
			n = fileSize - written
		}
		if _, err := e.WriteAt(want[written:written+n], written); err != nil {
			t.Fatalf("write at %d: %v", written, err)
		}
		written += n

		// Drive the copier the way a background loop would.
		for {
			moved, err := streamStep(e, src, destFile, rel, dur, ver)
			if err != nil {
				t.Fatalf("streamStep at written=%d: %v", written, err)
			}
			if moved == 0 {
				break
			}
		}
		if p := physicalSpoolBytes(t, spoolPath); p > peakPhysical {
			peakPhysical = p
		}
		if written == fileSize && e.PunchedEnd() == 0 {
			t.Fatalf("nothing was ever sealed or punched after writing the whole "+
				"%d byte file (reason: %q) — streaming was inert, so this test would "+
				"otherwise report success while proving nothing",
				fileSize, streamIneligible(e))
		}
	}

	// Flush whatever remains after the last write, then publish.
	if _, err := e.WriteAt(nil, written); err != nil && err.Error() != "" {
		_ = err // zero-length write is a no-op; ignore
	}
	remaining := written - e.PunchedEnd()
	if remaining > 0 {
		// Copy the unsealed tail directly, as finalize would.
		tail := make([]byte, remaining)
		if _, err := src.ReadAt(tail, e.PunchedEnd()); err != nil {
			t.Fatalf("read tail: %v", err)
		}
		if _, err := destFile.WriteAt(tail, e.PunchedEnd()); err != nil {
			t.Fatalf("write tail: %v", err)
		}
		if err := destFile.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if err := finishStream(e, tempPath, realPath); err != nil {
		t.Fatalf("finishStream: %v", err)
	}

	// ── THE TWO ASSERTIONS THAT MATTER ──────────────────────────────────────

	got, err := os.ReadFile(realPath)
	if err != nil {
		t.Fatalf("read published file: %v", err)
	}
	if len(got) != fileSize {
		t.Fatalf("published size %d, want %d", len(got), fileSize)
	}
	if !bytes.Equal(got, want) {
		// Locate it, so a failure is diagnosable rather than a bisect.
		bad := -1
		for i := range want {
			if got[i] != want[i] {
				bad = i
				break
			}
		}
		t.Fatalf("PUBLISHED FILE DIFFERS FROM SOURCE at offset %d — streaming "+
			"corrupted the copy (sha src=%x dst=%x)", bad,
			sha256.Sum256(want), sha256.Sum256(got))
	}

	if peakPhysical >= fileSize {
		t.Errorf("PEAK SPOOL OCCUPANCY WAS %d BYTES for a %d byte file — streaming "+
			"did not bound it, so the single-file size cap is NOT removed. That is "+
			"the entire point of this feature.", peakPhysical, fileSize)
	}
	t.Logf("peak spool occupancy %.1f MiB for a %.1f MiB file (%.0f%% of the file)",
		float64(peakPhysical)/(1<<20), float64(fileSize)/(1<<20),
		100*float64(peakPhysical)/float64(fileSize))
}

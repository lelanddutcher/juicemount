//go:build darwin

package nfs

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// A DISABLED FEATURE MUST COST NOTHING.
//
// Not merely "does nothing" — must not allocate a session map or start a
// goroutine. A ticker that wakes only to decide it has no work is the idle churn
// this codebase has had to hunt down before.
func TestStreamerDoesNotStartWhenDisabled(t *testing.T) {
	t.Setenv("JM_SPOOL_STREAM_DRAIN", "0")
	d := &Drainer{}
	stop := d.StartStreamer(10 * time.Millisecond)
	defer stop()
	time.Sleep(30 * time.Millisecond)
	if d.streamer != nil {
		t.Error("streamer state allocated while the feature is disabled")
	}
}

// THE WIRING, not the function.
//
// streamStep being correct proves nothing if the background loop never calls it.
// Three neuters have passed silently this sprint for exactly this reason, so
// this drives the REAL ticker against a REAL entry and asserts bytes were
// reclaimed — the observable effect, not an internal flag.
func TestStreamerLoopActuallyReclaimsBytes(t *testing.T) {
	t.Setenv("JM_SPOOL_STREAM_DRAIN", "1")
	origMin, origMargin, origHead := spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve
	spoolStreamMinSize = 8 << 20
	spoolSealMargin = 512 << 10
	spoolSealHeadReserve = 512 << 10
	t.Cleanup(func() {
		spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve = origMin, origMargin, origHead
	})

	s := newTestSpoolStore(t, 1<<30)
	e, err := s.OpenWrite("/DCIM/BIG.MOV")
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 32<<20)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	if _, err := e.WriteAt(payload, 0); err != nil {
		t.Fatal(err)
	}

	d := &Drainer{spool: s, fuseRoot: t.TempDir()}
	stop := d.StartStreamer(5 * time.Millisecond)
	defer stop()

	// Wait for the loop to make progress on its own.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && e.PunchedEnd() == 0 {
		time.Sleep(10 * time.Millisecond)
	}

	if e.PunchedEnd() == 0 {
		t.Fatalf("the streamer loop never advanced anything (eligibility: %q) — "+
			"streamStep may be correct, but nothing CALLS it, so streaming is inert "+
			"in production", streamIneligible(e))
	}
	if e.StreamDestPath() == "" {
		t.Error("no stream destination published — a punched read would have nowhere " +
			"to go and would fail closed")
	}

	// The spool file must have physically shrunk. punchedEnd moving is the
	// bookkeeping; freed blocks are the point.
	//
	// FLAKE FIX: two legitimate mechanisms delay block reclamation past the
	// moment the loop advances punchedEnd:
	//   1. APFS deallocates punched blocks ASYNCHRONOUSLY — fstat immediately
	//      after a successful punch can still report pre-punch block counts.
	//   2. The darwin puncher aligns inward to block size and returns a
	//      silent no-op for sub-block tails; the streamer correctly defers
	//      those bytes to the next tick/finalize.
	// Neither is a product bug, so poll up to 2 s for the physical count to
	// drop, then fail WITH diagnostics instead of a bare number pair.
	var st syscall.Stat_t
	physical := int64(-1)
	reclaimDeadline := time.Now().Add(2 * time.Second)
	for {
		if sErr := syscall.Stat(e.SpoolFilePath(), &st); sErr != nil {
			t.Fatal(sErr)
		}
		physical = int64(st.Blocks) * 512
		if physical < int64(len(payload)) || time.Now().After(reclaimDeadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if physical >= int64(len(payload)) {
		t.Errorf("spool file still occupies %d bytes of %d after 2s poll — punchedEnd=%d "+
			"advanced but no blocks were returned (APFS async reclaim did not land)",
			physical, len(payload), e.PunchedEnd())
	}
}

// The partial must be written to the HIDDEN SIBLING, never the real path. A
// partial at the real path is visible to the farm, the mirror and other clients
// for the whole transfer.
func TestStreamerWritesToTheHiddenSiblingNotTheRealPath(t *testing.T) {
	t.Setenv("JM_SPOOL_STREAM_DRAIN", "1")
	origMin, origMargin, origHead := spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve
	spoolStreamMinSize = 8 << 20
	spoolSealMargin = 512 << 10
	spoolSealHeadReserve = 512 << 10
	t.Cleanup(func() {
		spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve = origMin, origMargin, origHead
	})

	s := newTestSpoolStore(t, 1<<30)
	e, err := s.OpenWrite("/DCIM/BIG.MOV")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.WriteAt(make([]byte, 32<<20), 0); err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	d := &Drainer{spool: s, fuseRoot: root, streamer: &streamer{sessions: map[int64]*streamSession{}}}
	d.streamOnce()
	defer d.closeAllStreamSessions()

	realPath := filepath.Join(root, "DCIM", "BIG.MOV")
	if _, err := os.Stat(realPath); err == nil {
		t.Error("a partial was created at the REAL destination path — it would be " +
			"visible to the farm, the mirror and other clients for the whole transfer")
	}
	dest := e.StreamDestPath()
	if dest == "" {
		t.Fatal("no destination published")
	}
	if !isStreamTempName(filepath.Base(dest)) {
		t.Errorf("destination %q is not a hidden stream partial", dest)
	}
	if _, err := os.Stat(dest); err != nil {
		t.Errorf("hidden partial not created: %v", err)
	}
}

// A FINALIZED STREAMED FILE MUST BE PUBLISHED.
//
// finishStream had NO CALLER until this wiring: a streamed file would copy,
// punch, and then sit as a hidden partial forever — invisible to the mirror, the
// farm and the user, with the real path never appearing at all. Found by
// auditing the design doc against the code.
func TestStreamerPublishesAFinalizedEntry(t *testing.T) {
	t.Setenv("JM_SPOOL_STREAM_DRAIN", "1")
	origMin, origMargin, origHead := spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve
	spoolStreamMinSize = 8 << 20
	spoolSealMargin = 512 << 10
	spoolSealHeadReserve = 512 << 10
	t.Cleanup(func() {
		spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve = origMin, origMargin, origHead
	})

	s := newTestSpoolStore(t, 1<<30)
	e, err := s.OpenWrite("/DCIM/BIG.MOV")
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 32<<20)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	if _, err := e.WriteAt(payload, 0); err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	d := &Drainer{spool: s, fuseRoot: root, streamer: &streamer{sessions: map[int64]*streamSession{}}}

	// Stream a few chunks, then finalize and let the loop publish.
	for i := 0; i < 8; i++ {
		d.streamOnce()
	}
	if e.PunchedEnd() == 0 {
		t.Fatalf("nothing streamed (%q); the publish assertion would prove nothing",
			streamIneligible(e))
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	d.streamOnce() // sees closed + session -> publishes

	realPath := filepath.Join(root, "DCIM", "BIG.MOV")
	got, err := os.ReadFile(realPath)
	if err != nil {
		t.Fatalf("finalized streamed file was never published at %s: %v — it would "+
			"remain a hidden partial forever, invisible to the mirror, the farm and "+
			"the user", realPath, err)
	}
	if len(got) != len(payload) {
		t.Fatalf("published %d bytes, want %d", len(got), len(payload))
	}
	for i := range payload {
		if got[i] != payload[i] {
			t.Fatalf("published file differs at offset %d", i)
		}
	}
}

// LOSING ELIGIBILITY MID-STREAM must freeze the session, not keep streaming
// against a hash it can no longer trust — and must NOT clear punchedEnd, or boot
// recovery would re-drain a holey spool file and upload zeros over good data.
func TestStreamerFreezesWhenEligibilityIsLostMidStream(t *testing.T) {
	t.Setenv("JM_SPOOL_STREAM_DRAIN", "1")
	origMin, origMargin, origHead := spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve
	spoolStreamMinSize = 8 << 20
	spoolSealMargin = 512 << 10
	spoolSealHeadReserve = 512 << 10
	t.Cleanup(func() {
		spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve = origMin, origMargin, origHead
	})

	s := newTestSpoolStore(t, 1<<30)
	e, err := s.OpenWrite("/DCIM/BIG.MOV")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.WriteAt(make([]byte, 32<<20), 0); err != nil {
		t.Fatal(err)
	}

	d := &Drainer{spool: s, fuseRoot: t.TempDir(), streamer: &streamer{sessions: map[int64]*streamSession{}}}
	d.streamOnce()
	punchedBefore := e.PunchedEnd()
	if punchedBefore == 0 {
		t.Fatal("nothing streamed; cannot test eligibility loss")
	}

	// A below-punch rewrite invalidates the streaming hash — the real trigger.
	if _, err := e.WriteAt([]byte("REWRITE!"), 1024); err != nil {
		t.Fatalf("below-punch rewrite: %v", err)
	}
	if e.StreamingHashValid() {
		t.Fatal("the rewrite did not invalidate the hash; this test would prove nothing")
	}

	d.streamOnce() // sees ineligible + session -> freezes

	if d.existingSession(e.ID()) != nil {
		t.Error("session still live after eligibility was lost — it would keep " +
			"streaming against a hash it cannot trust")
	}
	if e.PunchedEnd() != punchedBefore {
		t.Errorf("punchedEnd changed %d -> %d on freeze — it MUST persist, or boot "+
			"recovery re-drains the holey spool file and uploads zeros over good "+
			"backend bytes", punchedBefore, e.PunchedEnd())
	}
}

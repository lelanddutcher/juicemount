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
	t.Setenv("JM_SPOOL_STREAM_DRAIN", "")
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
	var st syscall.Stat_t
	if err := syscall.Stat(e.SpoolFilePath(), &st); err != nil {
		t.Fatal(err)
	}
	if physical := int64(st.Blocks) * 512; physical >= int64(len(payload)) {
		t.Errorf("spool file still occupies %d bytes of %d — punchedEnd advanced but "+
			"no blocks were returned", physical, len(payload))
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

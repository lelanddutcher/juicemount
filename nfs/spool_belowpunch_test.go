package nfs

import (
	"os"
	"path/filepath"
	"testing"
)

func belowPunchFixture(t *testing.T) (*SpoolEntry, string) {
	t.Helper()
	s := newTestSpoolStore(t, 64<<20)
	e, err := s.OpenWrite("/clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.WriteAt(make([]byte, 64<<10), 0); err != nil {
		t.Fatal(err)
	}
	// A destination holding the drained prefix.
	destPath := filepath.Join(t.TempDir(), ".juicemount-streaming-1-clip.mov")
	if err := os.WriteFile(destPath, make([]byte, 64<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	e.SetStreamDest(destPath)
	e.mu.Lock()
	e.punchedEnd = 32 << 10
	e.mu.Unlock()
	return e, destPath
}

// A WRITE BELOW punchedEnd MUST LAND AT THE DESTINATION.
//
// Those bytes were drained and punched away, and reads of that range are routed
// to the destination. Writing them to the spool would put the new data in a hole
// nothing reads, while every reader keeps getting the OLD bytes — a silently
// lost write and a final file with stale content.
func TestWriteBelowPunchedEndGoesToTheDestination(t *testing.T) {
	e, destPath := belowPunchFixture(t)

	payload := []byte("NEWDATA!")
	n, err := e.WriteAt(payload, 4096)
	if err != nil {
		t.Fatalf("write below punchedEnd: %v", err)
	}
	if n != len(payload) {
		t.Errorf("wrote %d of %d bytes", n, len(payload))
	}

	got := make([]byte, len(payload))
	f, err := os.Open(destPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.ReadAt(got, 4096); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Errorf("destination holds %q at 4096, want %q — the write did not reach "+
			"the only place those bytes exist, so a reader would see stale content",
			got, payload)
	}
}

// A STRADDLING write must be split, not rejected. A writer has no idea where our
// punch boundary is and must not be penalised for crossing it.
func TestWriteStraddlingThePunchBoundaryIsSplit(t *testing.T) {
	e, destPath := belowPunchFixture(t)

	// 8 bytes ending 4 bytes past the boundary.
	payload := []byte("ABCDEFGH")
	off := int64(32<<10) - 4
	n, err := e.WriteAt(payload, off)
	if err != nil {
		t.Fatalf("straddling write: %v", err)
	}
	if n != len(payload) {
		t.Errorf("straddling write reported %d of %d bytes", n, len(payload))
	}

	// Below half at the destination.
	f, err := os.Open(destPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	below := make([]byte, 4)
	if _, err := f.ReadAt(below, off); err != nil {
		t.Fatal(err)
	}
	if string(below) != "ABCD" {
		t.Errorf("destination holds %q below the boundary, want \"ABCD\"", below)
	}

	// Above half in the spool.
	sf, err := os.Open(e.SpoolFilePath())
	if err != nil {
		t.Fatal(err)
	}
	defer sf.Close()
	above := make([]byte, 4)
	if _, err := sf.ReadAt(above, 32<<10); err != nil {
		t.Fatal(err)
	}
	if string(above) != "EFGH" {
		t.Errorf("spool holds %q above the boundary, want \"EFGH\"", above)
	}
}

// Punched bytes with NO destination means the stream was abandoned. Writing into
// the hole would be worse than an error the client can retry.
func TestWriteBelowPunchedEndWithoutADestinationFails(t *testing.T) {
	e, _ := belowPunchFixture(t)
	e.SetStreamDest("")

	if _, err := e.WriteAt([]byte("x"), 4096); err == nil {
		t.Error("a write below punchedEnd with no destination succeeded — it went " +
			"into a punched hole that nothing will ever read")
	}
}

// Writes at or above the boundary must be untouched by any of this.
func TestWriteAtOrAbovePunchedEndStillUsesTheSpool(t *testing.T) {
	e, _ := belowPunchFixture(t)

	payload := []byte("SPOOLED!")
	if _, err := e.WriteAt(payload, 32<<10); err != nil {
		t.Fatalf("write at the boundary: %v", err)
	}
	sf, err := os.Open(e.SpoolFilePath())
	if err != nil {
		t.Fatal(err)
	}
	defer sf.Close()
	got := make([]byte, len(payload))
	if _, err := sf.ReadAt(got, 32<<10); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Errorf("spool holds %q at the boundary, want %q — the punched range is "+
			"half-open, so this offset still belongs to the spool", got, payload)
	}
}

// A BELOW-PUNCH WRITE MUST INVALIDATE THE STREAMING HASH.
//
// It is a seek-back by definition. The normal WriteAt path sets hashValid=false
// on `off < writtenEnd`; the redirect returns before reaching that, so without
// this the drainer would trust a SHA that no longer describes the file — and the
// spool-side check would QUARANTINE an intact file on the mismatch.
//
// This bug was introduced BY the redirect and caught by auditing the design doc
// against the code, which is the same habit that found the redirect gap itself.
func TestWriteBelowPunchedEndInvalidatesTheStreamingHash(t *testing.T) {
	e, _ := belowPunchFixture(t)
	if !e.StreamingHashValid() {
		t.Fatal("fixture already had an invalid hash; the assertion below would " +
			"prove nothing")
	}
	if _, err := e.WriteAt([]byte("NEWDATA!"), 4096); err != nil {
		t.Fatal(err)
	}
	if e.StreamingHashValid() {
		t.Error("hashValid stayed TRUE after a below-punch write — the streaming SHA " +
			"no longer describes the file, and the drainer would quarantine an " +
			"intact file on the resulting mismatch")
	}
}

// It must also count as writer activity. The idle sweeper finalizes quiet
// entries; a writer actively rewriting below the punch is not quiet, and
// finalizing under it would end the file mid-rewrite.
func TestWriteBelowPunchedEndCountsAsWriterActivity(t *testing.T) {
	e, _ := belowPunchFixture(t)
	e.lastWrite.Store(0)

	if _, err := e.WriteAt([]byte("NEWDATA!"), 4096); err != nil {
		t.Fatal(err)
	}
	if e.lastWrite.Load() == 0 {
		t.Error("a below-punch write did not stamp lastWrite — the idle sweeper " +
			"would treat an actively-rewriting entry as quiescent and finalize it " +
			"mid-rewrite")
	}
}

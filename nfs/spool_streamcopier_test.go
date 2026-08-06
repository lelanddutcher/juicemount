package nfs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Streaming must be OFF unless explicitly enabled. It changes the write path's
// durability story and cannot ship dark-by-accident.
func TestStreamIneligibleWhenDisabled(t *testing.T) {
	t.Setenv("JM_SPOOL_STREAM_DRAIN", "")
	e, _, _ := streamFixture(t, 8<<10)
	if got := streamIneligible(e); !strings.Contains(got, "disabled") {
		t.Errorf("streamIneligible = %q with the flag unset, want a 'disabled' reason", got)
	}
}

// AN OUT-OF-ORDER WRITER MUST FALL BACK TO THE WHOLE-FILE DRAIN.
//
// Streaming reads the spool prefix sequentially and hashes as it goes, so the
// accumulated hash is only a valid reference if the writer wrote in order. A
// preallocating or scattering writer (ftruncate-then-fill, parallel WRITE
// dispatch) makes StreamingHashValid false, and such an entry must take today's
// path — which re-hashes from disk and can afford to, because it never punches.
//
// Getting this wrong would punch a prefix whose hash reference is meaningless.
func TestStreamIneligibleForAnOutOfOrderWriter(t *testing.T) {
	t.Setenv("JM_SPOOL_STREAM_DRAIN", "1")
	s := newTestSpoolStore(t, 64<<20)
	e, err := s.OpenWrite("/clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	// Write high first, then low: the out-of-order signature.
	if _, err := e.WriteAt([]byte("tail"), 4096); err != nil {
		t.Fatal(err)
	}
	if _, err := e.WriteAt([]byte("head"), 0); err != nil {
		t.Fatal(err)
	}
	if e.StreamingHashValid() {
		t.Fatal("fixture did not produce an out-of-order writer; the assertion below " +
			"would prove nothing")
	}
	if got := streamIneligible(e); !strings.Contains(got, "hash invalid") {
		t.Errorf("streamIneligible = %q for an out-of-order writer, want a hash-invalid "+
			"reason — streaming would punch a prefix whose hash reference is meaningless", got)
	}
}

// A step on an ineligible entry must be a silent no-op, never an error: most
// polls of most entries land here.
func TestStreamStepIsANoOpWhenIneligible(t *testing.T) {
	t.Setenv("JM_SPOOL_STREAM_DRAIN", "")
	e, _, events := streamFixture(t, 8<<10)
	src, err := os.OpenFile(e.SpoolFilePath(), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	dest := &recordingDest{data: map[int64][]byte{}, events: events}
	n, err := streamStep(e, src, dest, &recordingReleaser{events: events}, &fakeDurability{}, nil)
	if err != nil {
		t.Errorf("streamStep on an ineligible entry errored: %v", err)
	}
	if n != 0 {
		t.Errorf("streamStep reclaimed %d bytes while ineligible", n)
	}
	if len(*events) != 0 {
		t.Errorf("streamStep touched the destination while ineligible: %v", *events)
	}
}

// The readback verifier must ACCEPT a faithful copy...
func TestDestReadbackVerifierAcceptsAMatchingChunk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dest.bin")
	want := []byte("the quick brown fox")
	if err := os.WriteFile(path, want, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	v := destReadbackVerifier{f: f}
	if err := v.verifyChunk(0, want); err != nil {
		t.Errorf("verifier rejected a faithful copy: %v", err)
	}
}

// ...and REJECT a corrupted one, naming the offset.
//
// A bare "mismatch" turns an incident into a bisect; an offset turns it into a
// lookup. The message must also say the range is retryable, because the whole
// point of verifying before the punch is that the spool copy is still intact.
func TestDestReadbackVerifierReportsTheDivergingOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dest.bin")
	stored := []byte("the quick brown fox")
	if err := os.WriteFile(path, stored, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sent := []byte("the quick BROWN fox")
	v := destReadbackVerifier{f: f}
	err = v.verifyChunk(0, sent)
	if err == nil {
		t.Fatal("verifier accepted a corrupted readback")
	}
	if !strings.Contains(err.Error(), "offset 10") {
		t.Errorf("error %q does not name the first diverging offset (10) — an "+
			"unlocated mismatch turns an incident into a bisect", err)
	}
	if !strings.Contains(err.Error(), "retryable") {
		t.Errorf("error %q does not say the range is retryable; verifying before the "+
			"punch exists precisely so it is", err)
	}
}

// A verifier with no destination handle must fail, not silently pass. Passing
// would make the whole at-rest check a no-op while looking configured.
func TestDestReadbackVerifierFailsWithoutAHandle(t *testing.T) {
	if err := (destReadbackVerifier{}).verifyChunk(0, []byte("x")); err == nil {
		t.Error("verifier with a nil handle accepted a chunk — the at-rest check " +
			"would be a no-op while appearing configured")
	}
}

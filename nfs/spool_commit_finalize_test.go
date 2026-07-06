package nfs

import (
	"bytes"
	"testing"
	"time"
)

// TestSpoolCommittedLargeFinalizesOnShortIdle is the #105 regression: a LARGE
// (>16MiB) entry that received an NFS COMMIT finalizes on the SHORT idle (a
// Premiere export closes ~seconds after its close-COMMIT), while an
// UNcommitted large entry still waits the full window. Safe because a
// continuation write during the resulting drain defers to the in-place fdPool
// path (TestReopenDuringDrainDefersToFuse), not a destructive fresh entry.
func TestSpoolCommittedLargeFinalizesOnShortIdle(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	const fullWindow = 30 * time.Second

	mk := func(path string, commit bool, quiescent time.Duration) *SpoolEntry {
		e, err := s.OpenWrite(path)
		if err != nil {
			t.Fatalf("OpenWrite %s: %v", path, err)
		}
		big := bytes.Repeat([]byte{0x7E}, int(smallSpoolFinalizeBytes)+4096) // > 16MiB
		if _, err := e.WriteAt(big, 0); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		e.ReleaseHandle() // refcount → 0 (NFS per-RPC semantics), NOT finalize
		if commit {
			e.MarkCommitted()
		}
		e.lastWrite.Store(time.Now().Add(-quiescent).UnixNano())
		return e
	}

	// Committed + quiescent past the SHORT idle (but far under the full window):
	// must finalize now.
	committed := mk("/exp-committed.bin", true, smallSpoolFinalizeIdle+time.Second)
	if !committed.finalizeIfIdle(fullWindow) {
		t.Fatalf("committed large entry did NOT finalize on the short idle — #105 fast-finalize broken")
	}

	// Uncommitted large, same quiescence: must NOT finalize yet (full window).
	uncommitted := mk("/exp-uncommitted.bin", false, smallSpoolFinalizeIdle+time.Second)
	if uncommitted.finalizeIfIdle(fullWindow) {
		t.Fatalf("uncommitted large entry finalized on the short idle — should wait the full window")
	}
	// …but it DOES finalize once quiescent past the full window.
	uncommitted.lastWrite.Store(time.Now().Add(-(fullWindow + time.Second)).UnixNano())
	if !uncommitted.finalizeIfIdle(fullWindow) {
		t.Fatalf("uncommitted large entry did not finalize even past the full window")
	}
}

package nfs

// Phase-1 BUG 2 (docs/LAUNCH_PLAN.md): finalizeIfIdle returned false forever
// (silently) while refcount != 0. A leaked write handle — e.g. an error path
// in the NFS lib that dropped a billy.File without Close — left the entry
// stuck in `writing` indefinitely: never finalized, never drained, capacity
// leaked, path phantom-stat'able (43 entries × 5+ hours in the 2026-06-08
// repro). These tests cover the defense-in-depth sweeper escalation: an
// entry that is QUIESCENT past a long window with refcount>0 is
// force-finalized loudly; a genuinely-active slow writer is never touched
// (quiescence is the discriminator, refcount alone is not).

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

func spoolRefcountForTest(e *SpoolEntry) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.refcount
}

// TestSweeperEscalatesQuiescentLeakedHandle: a handle that is never released
// keeps refcount>0 forever. Once the entry has been quiescent past the
// escalation window, the sweeper must force-finalize it (bytes are durable
// on the spool SSD; discarding them would be data loss) instead of skipping
// silently forever.
func TestSweeperEscalatesQuiescentLeakedHandle(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	s.SetStuckEscalationWindow(40 * time.Millisecond)

	e, err := s.OpenWrite("/leak.bin")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	data := []byte("leaked but durable bytes")
	if _, err := e.WriteAt(data, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	// NO Close / ReleaseHandle — this is the leak.

	// Within the escalation window the entry must be left alone (it could
	// be a handle between two write RPCs).
	if n := s.sweepOnce(0); n != 0 {
		t.Fatalf("sweeper escalated a recently-active entry (finalized %d)", n)
	}

	// Backdate the last write far past the window: the entry is now
	// quiescent + refcount>0 → leaked. The sweeper must escalate.
	e.lastWrite.Store(time.Now().Add(-time.Minute).UnixNano())
	if n := s.sweepOnce(0); n < 1 {
		t.Fatal("sweeper silently skipped a quiescent refcount-leaked entry forever (finalized 0)")
	}

	row, err := s.Meta().Get(e.ID())
	if err != nil {
		t.Fatalf("row: %v", err)
	}
	if row.DrainState != metadata.DrainReady {
		t.Errorf("escalated entry state=%s, want ready (bytes preserved for drain)", row.DrainState)
	}
	if row.Size != int64(len(data)) {
		t.Errorf("escalated entry size=%d, want %d", row.Size, len(data))
	}
}

// TestSweeperEscalationSparesActiveSlowWriter: a large copy writes
// continuously — refcount stays >0 across sweeps but lastWrite keeps
// advancing. The escalation must NEVER fire on such an entry.
func TestSweeperEscalationSparesActiveSlowWriter(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	s.SetStuckEscalationWindow(60 * time.Millisecond)

	e, err := s.OpenWrite("/slow.bin")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}

	// Simulate a slow-but-alive writer: a write every 20ms, sweeping after
	// each. Total elapsed exceeds the 60ms window several times over, but
	// the entry is never QUIESCENT for the full window.
	buf := []byte("chunk")
	for i := 0; i < 10; i++ {
		if _, err := e.WriteAt(buf, int64(i*len(buf))); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if n := s.sweepOnce(0); n != 0 {
			t.Fatalf("sweeper finalized an ACTIVELY-WRITING entry at iteration %d", i)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Writer finishes normally: release the handle; the normal idle path
	// finalizes (escalation must not be needed).
	e.ReleaseHandle()
	if n := s.sweepOnce(0); n != 1 {
		t.Fatalf("normal finalize after release: finalized %d, want 1", n)
	}
}

// TestSweeperEscalationEndToEndDrains: the escalated entry's bytes must
// actually reach FUSE via the drainer — escalation is recovery, not autopsy.
func TestSweeperEscalationEndToEndDrains(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	s.SetStuckEscalationWindow(10 * time.Millisecond)
	fuseRoot := t.TempDir()
	d, err := NewDrainer(s, DrainerConfig{FuseRoot: fuseRoot, BackoffBase: time.Millisecond})
	if err != nil {
		t.Fatalf("drainer: %v", err)
	}

	e, err := s.OpenWrite("recovered.bin")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	payload := []byte("escalation must preserve these bytes")
	if _, err := e.WriteAt(payload, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Leak the handle, backdate, escalate.
	e.lastWrite.Store(time.Now().Add(-time.Hour).UnixNano())
	if n := s.sweepOnce(0); n < 1 {
		t.Fatal("escalation did not finalize the leaked entry")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d.DrainOnceForTest(ctx)

	got, err := os.ReadFile(filepath.Join(fuseRoot, "recovered.bin"))
	if err != nil {
		t.Fatalf("escalated entry never drained: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("escalated entry drained wrong bytes")
	}
	if used, _ := s.Capacity(); used != 0 {
		t.Errorf("capacity leaked after escalated drain: used=%d", used)
	}
}

package nfs

import (
	"os"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// TestMain shortens the spool capacity-backpressure wait for the whole package
// so capacity-full tests don't each stall for the production 5s deadline.
// Individual tests that assert specific wait timing override these locally.
func TestMain(m *testing.M) {
	capacityWaitDeadline = 80 * time.Millisecond
	capacityWaitPoll = 5 * time.Millisecond
	os.Exit(m.Run())
}

// TestSpoolBackpressureWaitsThenSucceeds proves the core flow-control contract:
// a write that can't immediately reserve capacity WAITS for the drainer to free
// space and then SUCCEEDS, instead of hard-failing the copy with ErrSpoolFull.
// This is what lets a large SD-card offload pace itself to drain throughput
// rather than aborting the instant the spool fills.
func TestSpoolBackpressureWaitsThenSucceeds(t *testing.T) {
	old := capacityWaitDeadline
	capacityWaitDeadline = 2 * time.Second
	t.Cleanup(func() { capacityWaitDeadline = old })

	s := newTestSpoolStore(t, 100) // 100-byte cap
	e, _ := s.OpenWrite("/a")
	if _, err := e.WriteAt(make([]byte, 100), 0); err != nil { // fill to cap
		t.Fatalf("fill to cap: %v", err)
	}

	// Simulate a drain freeing 50 bytes shortly after the next write blocks.
	go func() {
		time.Sleep(150 * time.Millisecond)
		s.releaseCapacity(50)
	}()

	start := time.Now()
	if _, err := e.WriteAt(make([]byte, 50), 100); err != nil {
		t.Fatalf("backpressured write should succeed once capacity frees, got %v", err)
	}
	if waited := time.Since(start); waited < 100*time.Millisecond {
		t.Errorf("expected the write to block ~150ms for capacity, only waited %v", waited)
	}
	if used, _ := s.Capacity(); used != 100 {
		t.Errorf("used after backpressured write = %d, want 100", used)
	}
}

// TestSpoolBackpressureOfflineStallsThenResumes proves the offline-ingest
// contract (2026-06-15): while OFFLINE, a full spool must STALL the write to zero
// speed — never failing with ErrSpoolFull even long past the online wedge
// backstop, because the paused drain can't free space until the user reconnects —
// and the write must RESUME and succeed once the user reconnects and the drain
// frees headroom. (Pre-fix, offline + full hit "disk is full": the fixed 30s
// deadline expired with the drain paused, aborting the whole copy.)
func TestSpoolBackpressureOfflineStallsThenResumes(t *testing.T) {
	old := capacityWaitDeadline
	capacityWaitDeadline = 50 * time.Millisecond // short ONLINE backstop...
	t.Cleanup(func() { capacityWaitDeadline = old })

	pin.SetOffline(true) // ...which OFFLINE must ignore entirely
	t.Cleanup(func() { pin.SetOffline(false) })

	s := newTestSpoolStore(t, 100)
	e, _ := s.OpenWrite("/a")
	if _, err := e.WriteAt(make([]byte, 100), 0); err != nil { // fill to cap
		t.Fatalf("fill to cap: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := e.WriteAt(make([]byte, 50), 100)
		done <- err
	}()

	// Far past the online backstop, the offline write must still be STALLED —
	// not returned (and certainly not failed with ErrSpoolFull).
	select {
	case err := <-done:
		t.Fatalf("offline full-spool write must stall, but it returned early: %v", err)
	case <-time.After(250 * time.Millisecond):
		// still blocked — correct
	}
	if got := s.stallWaiters.Load(); got != 1 {
		t.Errorf("stallWaiters = %d while one write is parked, want 1", got)
	}

	// Reconnect and free space; the stalled write must now resume and succeed,
	// not fail on the (now stale) offline duration vs the short backstop.
	pin.SetOffline(false)
	s.releaseCapacity(50)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("after reconnect + drain, stalled write must succeed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stalled write did not resume after reconnect + capacity free")
	}
	if got := s.stallWaiters.Load(); got != 0 {
		t.Errorf("stallWaiters = %d after the write resumed, want 0", got)
	}
}

// TestCapacityStallParkedWriterNotEscalated is the data-loss guard for the
// graceful stall (2026-06-15 adversarial review, critical finding #4): a WRITE
// parked in the offline capacity stall must NOT be mistaken by the sweeper's
// leaked-handle escalation for a quiescent abandoned handle and force-finalized
// TRUNCATED — which would silently lose the rest of the file. The parked write
// refreshes the entry's lastWrite each poll, so escalation never fires; on
// reconnect + capacity-free the write completes with the FULL content.
//
// RED on pre-fix code: without the touch, lastWrite stays frozen at the head
// write, so sweepOnce escalates and finalizes the entry truncated at 100 bytes
// while the second write is still parked.
func TestCapacityStallParkedWriterNotEscalated(t *testing.T) {
	old := capacityWaitDeadline
	capacityWaitDeadline = 50 * time.Millisecond
	t.Cleanup(func() { capacityWaitDeadline = old })
	pin.SetOffline(true)
	t.Cleanup(func() { pin.SetOffline(false) })

	s := newTestSpoolStore(t, 100)
	s.SetStuckEscalationWindow(30 * time.Millisecond) // short: a stale handle would escalate fast

	e, err := s.OpenWrite("/parked.bin")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := e.WriteAt(make([]byte, 100), 0); err != nil { // fill the 100-byte cap
		t.Fatalf("fill: %v", err)
	}

	// A second write that needs more capacity → parks indefinitely (offline + full).
	done := make(chan error, 1)
	go func() { _, werr := e.WriteAt(make([]byte, 60), 100); done <- werr }()

	// Sweep repeatedly while the write is parked, well past the escalation
	// window. The parked write's touch keeps lastWrite fresh, so escalation must
	// NEVER fire.
	deadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n := s.sweepOnce(0); n != 0 {
			t.Fatalf("sweeper escalated a write PARKED in the capacity stall (finalized %d) — truncation/data-loss path", n)
		}
		time.Sleep(15 * time.Millisecond)
	}
	select {
	case err := <-done:
		t.Fatalf("parked write returned early while offline + full: %v", err)
	default:
	}

	// Reconnect + free capacity: the parked write resumes and completes FULLY.
	pin.SetOffline(false)
	s.releaseCapacity(60)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("parked write failed after reconnect: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked write did not resume after reconnect")
	}
	e.mu.Lock()
	end := e.writtenEnd
	e.mu.Unlock()
	if end != 160 {
		t.Fatalf("writtenEnd=%d after resume, want 160 (a truncating escalation would leave 100)", end)
	}
}

// TestSpoolBackpressureTimesOut proves the bounded fallback: when capacity never
// frees (drain truly stuck), backpressure still fails with ErrSpoolFull after
// the deadline rather than hanging the client forever.
func TestSpoolBackpressureTimesOut(t *testing.T) {
	old := capacityWaitDeadline
	capacityWaitDeadline = 120 * time.Millisecond
	t.Cleanup(func() { capacityWaitDeadline = old })

	s := newTestSpoolStore(t, 100)
	e, _ := s.OpenWrite("/a")
	if _, err := e.WriteAt(make([]byte, 100), 0); err != nil {
		t.Fatalf("fill to cap: %v", err)
	}

	start := time.Now()
	if _, err := e.WriteAt(make([]byte, 50), 100); err != ErrSpoolFull {
		t.Fatalf("expected ErrSpoolFull after deadline, got %v", err)
	}
	if waited := time.Since(start); waited < 100*time.Millisecond {
		t.Errorf("expected to wait ~the deadline before failing, waited %v", waited)
	}
}

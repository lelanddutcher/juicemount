package nfs

import (
	"os"
	"testing"
	"time"
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

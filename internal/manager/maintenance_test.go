package manager

// SLICE 6 — MaintenanceManager tests.
//
// Three required test cases per the slice prompt:
//
//   1. TestMaintenancePerKindMutex — a second same-kind request returns
//      errKindBusy while the first is still running.
//   2. TestMaintenanceCrossKindConcurrency — gc + fsck run in parallel.
//   3. TestMaintenanceOutputCap — Output is capped at 1000 lines with a
//      "[truncated]" marker once the cap is exceeded.
//
// The tests use SetRunner to inject a deterministic runner — we never
// shell out to a real juicefs binary in unit tests. This mirrors the
// JobManager.SetRunner pattern used by jobs_test.go.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestMaintenancePerKindMutex verifies the per-kind gate: while a gc
// op is running, a second gc start returns errKindBusy. The first op
// is held open by blocking the runner on a channel the test controls
// — when we close release, the runner returns and the mutex unlocks,
// at which point a third gc start succeeds.
func TestMaintenancePerKindMutex(t *testing.T) {
	mm := newMaintenanceManager("/dev/null", "/mnt/jfs", "redis://x", "/jfs")
	release := make(chan struct{})
	mm.SetRunner(func(ctx context.Context, argv []string, op *MaintenanceOp) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	// First gc start should succeed.
	op1, err := mm.tryStart(MaintenanceGC, []string{"juicefs", "gc", "redis://x"})
	if err != nil {
		t.Fatalf("first gc start failed: %v", err)
	}
	if op1 == nil {
		t.Fatal("first gc start returned nil op")
	}
	// Give the goroutine a moment to grab the lock and transition to
	// running. Without this the second TryLock can race the goroutine
	// scheduling and the test becomes flaky on busy CI hosts.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mm.mu.Lock()
		running := mm.active[MaintenanceGC]
		mm.mu.Unlock()
		if running != nil {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Second gc start should return errKindBusy (409 at HTTP layer).
	_, err = mm.tryStart(MaintenanceGC, []string{"juicefs", "gc", "redis://x"})
	if !errors.Is(err, errKindBusy) {
		t.Fatalf("second gc start: got err=%v, want errKindBusy", err)
	}

	// Release the first op; wait for it to finish.
	close(release)
	waitForState(t, op1, MaintenanceDone, 2*time.Second)

	// Third gc start should now succeed — the mutex was released.
	op3, err := mm.tryStart(MaintenanceGC, []string{"juicefs", "gc", "redis://x"})
	if err != nil {
		t.Fatalf("third gc start (after release) failed: %v", err)
	}
	// Drain op3 — runner is still blocking on `release` (now closed),
	// so it returns immediately.
	waitForState(t, op3, MaintenanceDone, 2*time.Second)
}

// TestMaintenanceCrossKindConcurrency verifies that two different
// kinds (gc and fsck) can run at the same time. Each kind has its
// own mutex; the global manager does NOT serialize across kinds.
//
// Pattern: start both ops with a runner that signals "I'm in" on a
// per-kind channel and then waits for the test to release it. If
// the manager incorrectly serialized across kinds, the second start
// would block until the first finished and the "both running" check
// would fail.
func TestMaintenanceCrossKindConcurrency(t *testing.T) {
	mm := newMaintenanceManager("/dev/null", "/mnt/jfs", "redis://x", "/jfs")
	gcIn := make(chan struct{}, 1)
	fsckIn := make(chan struct{}, 1)
	release := make(chan struct{})
	mm.SetRunner(func(ctx context.Context, argv []string, op *MaintenanceOp) error {
		switch op.Kind {
		case MaintenanceGC:
			gcIn <- struct{}{}
		case MaintenanceFSCK:
			fsckIn <- struct{}{}
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	// Start gc.
	if _, err := mm.tryStart(MaintenanceGC, []string{"juicefs", "gc"}); err != nil {
		t.Fatalf("start gc: %v", err)
	}
	select {
	case <-gcIn:
	case <-time.After(2 * time.Second):
		t.Fatal("gc runner never entered")
	}

	// Start fsck — should not block on gc's mutex.
	if _, err := mm.tryStart(MaintenanceFSCK, []string{"juicefs", "fsck"}); err != nil {
		t.Fatalf("start fsck: %v", err)
	}
	select {
	case <-fsckIn:
		// good — both kinds are concurrently running
	case <-time.After(2 * time.Second):
		t.Fatal("fsck runner never entered while gc was running (cross-kind serialization?)")
	}

	// Verify both are in MaintenanceRunning state simultaneously.
	mm.mu.Lock()
	gcOp := mm.active[MaintenanceGC]
	fsckOp := mm.active[MaintenanceFSCK]
	mm.mu.Unlock()
	if gcOp == nil || gcOp.GetState() != MaintenanceRunning {
		t.Errorf("gc not in running state: %+v", gcOp)
	}
	if fsckOp == nil || fsckOp.GetState() != MaintenanceRunning {
		t.Errorf("fsck not in running state: %+v", fsckOp)
	}

	// Release both; let them clean up so the test doesn't leak goroutines.
	close(release)
	waitForState(t, gcOp, MaintenanceDone, 2*time.Second)
	waitForState(t, fsckOp, MaintenanceDone, 2*time.Second)
}

// TestMaintenanceOutputCap exercises the ring-trim policy: when the
// runner emits more than maintenanceOutputCap (1000) lines, the
// stored Output keeps the most recent (cap-1) lines plus a single
// "[truncated]" marker at index 0.
//
// We drive the append directly (op.appendOutputLocked) so the test
// doesn't depend on subprocess timing. The runner path uses the same
// helper.
func TestMaintenanceOutputCap(t *testing.T) {
	op := &MaintenanceOp{Kind: MaintenanceGC, State: MaintenanceRunning}

	// Push 1200 lines through the helper. Expected end state: len ==
	// 1000, Output[0] == "[truncated]", Output[1..999] == the most
	// recent 999 lines (i.e. lines 201..1199 by 0-indexed input).
	const total = 1200
	for i := 0; i < total; i++ {
		op.mu.Lock()
		op.appendOutputLocked(fmt.Sprintf("line-%d", i))
		op.mu.Unlock()
	}

	if len(op.Output) != maintenanceOutputCap {
		t.Fatalf("output length = %d, want %d", len(op.Output), maintenanceOutputCap)
	}
	if op.Output[0] != maintenanceTruncMarker {
		t.Errorf("head of output = %q, want %q (truncation marker)", op.Output[0], maintenanceTruncMarker)
	}
	// The tail should be the most recent line we emitted.
	wantTail := fmt.Sprintf("line-%d", total-1)
	if op.Output[len(op.Output)-1] != wantTail {
		t.Errorf("tail of output = %q, want %q", op.Output[len(op.Output)-1], wantTail)
	}
	// First post-marker line should be (total - cap + 1) = 201.
	// (cap-1 live lines + 1 marker = cap; the live window starts
	// (total - (cap-1)) = total - 999 = 201.)
	wantFirstLive := fmt.Sprintf("line-%d", total-(maintenanceOutputCap-1))
	if op.Output[1] != wantFirstLive {
		t.Errorf("first post-marker line = %q, want %q", op.Output[1], wantFirstLive)
	}
	// Sanity-check the truncated flag stays set after further appends.
	op.mu.Lock()
	op.appendOutputLocked("one-more")
	op.mu.Unlock()
	if !op.truncated {
		t.Error("truncated flag cleared unexpectedly")
	}
	if op.Output[0] != maintenanceTruncMarker {
		t.Errorf("marker dropped after extra append; head = %q", op.Output[0])
	}
	if op.Output[len(op.Output)-1] != "one-more" {
		t.Errorf("extra append did not reach tail; tail = %q", op.Output[len(op.Output)-1])
	}

	// Sub-cap appends should NOT mark truncated.
	op2 := &MaintenanceOp{Kind: MaintenanceFSCK, State: MaintenanceRunning}
	for i := 0; i < maintenanceOutputCap-10; i++ {
		op2.mu.Lock()
		op2.appendOutputLocked(fmt.Sprintf("l-%d", i))
		op2.mu.Unlock()
	}
	if op2.truncated {
		t.Error("truncated flag set before cap was exceeded")
	}
	if op2.Output[0] == maintenanceTruncMarker {
		t.Error("marker inserted before cap was exceeded")
	}
	if !strings.HasPrefix(op2.Output[0], "l-") {
		t.Errorf("unexpected head line: %q", op2.Output[0])
	}
}

// GetState is a tiny accessor used by the cross-kind test. Lives here
// (test file) rather than in maintenance.go because production code
// reads state under the manager's mu via the snapshot path.
func (op *MaintenanceOp) GetState() MaintenanceState {
	op.mu.Lock()
	defer op.mu.Unlock()
	return op.State
}

// waitForState polls op.State up to `within` for `want`. Test helper
// — avoids the flaky "sleep N ms and check" pattern.
func waitForState(t *testing.T, op *MaintenanceOp, want MaintenanceState, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if op.GetState() == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("op (kind=%s) did not reach state %s within %s; current=%s", op.Kind, want, within, op.GetState())
}

// _ silences any "declared but not used" lints from sync.WaitGroup
// in the import block above (we don't use it in this file but keep
// the symbol available for downstream consumers that copy-paste
// the test file as a template). Go's unused-import rule applies to
// the import itself, not symbols, so this is just defensive.
var _ = sync.WaitGroup{}

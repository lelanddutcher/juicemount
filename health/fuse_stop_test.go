package health

import (
	"context"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

func testEmptyFUSESnapshot(context.Context) (mounttable.Snapshot, error) {
	return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: []mounttable.MountEntry{}}, nil
}

func TestFUSEStopReturnsInFlightWhenLifecycleMutexHeldAndCanRetry(t *testing.T) {
	fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir()})
	fm.observeMount = testEmptyFUSESnapshot
	fm.mu.Lock()
	result := fm.stopWithBounds(20*time.Millisecond, 20*time.Millisecond)
	if result.State != FUSEInFlight || result.Complete() {
		t.Fatalf("held lifecycle lock produced %+v", result)
	}
	fm.mu.Unlock()
	if result := fm.stopWithBounds(time.Second, time.Second); !result.Complete() {
		t.Fatalf("repeat Stop did not settle: %+v", result)
	}
}

func TestFUSEStopUsesLockedObservationWhenMutexAvailable(t *testing.T) {
	fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir()})
	calls := 0
	fm.observeMount = func(ctx context.Context) (mounttable.Snapshot, error) {
		calls++
		return testEmptyFUSESnapshot(ctx)
	}
	if result := fm.stopWithBounds(time.Second, time.Second); !result.Complete() || calls != 2 {
		t.Fatalf("complete Stop requires two observations: result=%+v calls=%d", result, calls)
	}
}

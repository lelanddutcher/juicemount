package health

import (
	"testing"
	"time"
)

func TestFUSEStopReturnsWhenMonitorAndLifecycleMutexAreWedged(t *testing.T) {
	fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir()})

	// Reproduce the field failure: the watchdog never finishes and retains the
	// lifecycle mutex after Stop has already exhausted its join budget.
	fm.mu.Lock()
	defer fm.mu.Unlock()

	fallbackCalled := make(chan struct{}, 1)
	returned := make(chan struct{})
	go func() {
		fm.stopWithBounds(20*time.Millisecond, 20*time.Millisecond, func() {
			fallbackCalled <- struct{}{}
		})
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("FUSEManager.Stop remained blocked behind a wedged lifecycle mutex")
	}

	select {
	case <-fallbackCalled:
	default:
		t.Fatal("lock-free cleanup fallback was not invoked")
	}
}

func TestFUSEStopUsesLockedCleanupWhenMutexBecomesAvailable(t *testing.T) {
	fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir()})
	close(fm.done)

	fallbackCalled := false
	fm.stopWithBounds(time.Second, time.Second, func() { fallbackCalled = true })
	if fallbackCalled {
		t.Fatal("fallback ran even though lifecycle mutex was available")
	}
}

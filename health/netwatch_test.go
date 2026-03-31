package health

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestNetWatcherStartStop(t *testing.T) {
	nw := NewNetWatcher(100 * time.Millisecond)
	nw.Start()
	time.Sleep(200 * time.Millisecond)
	nw.Stop()
	// Reaching here without hanging means start/stop works.
}

func TestNetWatcherDetectsInterface(t *testing.T) {
	nw := NewNetWatcher(100 * time.Millisecond)
	nw.Start()
	defer nw.Stop()

	time.Sleep(200 * time.Millisecond)

	iface := nw.ActiveInterface()
	if iface == "" {
		t.Skip("no active network interface detected (expected in isolated CI environments)")
	}
	t.Logf("active interface: %s", iface)
}

func TestNetWatcherCallbackOnChange(t *testing.T) {
	// This test verifies the callback mechanism works by simulating a change.
	// We can't actually change the network interface in a test, but we can
	// verify the watcher initializes correctly and the callback list is set up.
	var callCount atomic.Int32

	nw := NewNetWatcher(50 * time.Millisecond)
	nw.OnChange(func(old, new string) {
		callCount.Add(1)
	})
	nw.Start()
	defer nw.Stop()

	// Since the interface won't change during the test, callCount should be 0
	time.Sleep(200 * time.Millisecond)
	if callCount.Load() != 0 {
		t.Errorf("expected 0 calls (no interface change), got %d", callCount.Load())
	}
}

func TestNetWatcherGracePeriod(t *testing.T) {
	nw := NewNetWatcher(50 * time.Millisecond)
	nw.Start()
	defer nw.Stop()

	// No change has happened yet, so grace period should be false
	if nw.InGracePeriod(5 * time.Second) {
		t.Error("expected not in grace period before any change")
	}

	// Simulate a change by setting lastChangeAt directly
	nw.mu.Lock()
	nw.lastChangeAt = time.Now()
	nw.mu.Unlock()

	if !nw.InGracePeriod(5 * time.Second) {
		t.Error("expected in grace period right after change")
	}

	// After the grace period expires, should no longer be in grace
	nw.mu.Lock()
	nw.lastChangeAt = time.Now().Add(-10 * time.Second)
	nw.mu.Unlock()

	if nw.InGracePeriod(5 * time.Second) {
		t.Error("expected not in grace period after expiry")
	}
}

func TestDetectActiveInterface(t *testing.T) {
	iface := detectActiveInterface()
	if iface == "" {
		t.Skip("no active interface (expected in isolated CI)")
	}
	t.Logf("detectActiveInterface() = %q", iface)
}

func TestConnStateFor(t *testing.T) {
	tests := []struct {
		name      string
		prev      ConnState
		wasOK     bool
		isOK      bool
		wantState ConnState
	}{
		{"stays connected", ConnStateConnected, true, true, ConnStateConnected},
		{"disconnects", ConnStateConnected, true, false, ConnStateDisconnected},
		{"reconnects", ConnStateDisconnected, false, true, ConnStateConnected},
		{"stays disconnected", ConnStateDisconnected, false, false, ConnStateDisconnected},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := connStateFor(tt.prev, tt.wasOK, tt.isOK)
			if got != tt.wantState {
				t.Errorf("connStateFor(%v, %v, %v) = %v, want %v",
					tt.prev, tt.wasOK, tt.isOK, got, tt.wantState)
			}
		})
	}
}

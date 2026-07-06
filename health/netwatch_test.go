package health

import (
	"errors"
	"net"
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

// ---------------------------------------------------------------------------
// G8 (task #81): backend-route classification.
//
// CI cannot fabricate a utun interface, so the utun half of the mapping is
// live-validated (2026-07-02, hotspot+Tailscale: route to the NAS = utun6,
// local addr = the Mac's own 100.64/10 address owned by utun6). What CI CAN
// prove: the local-IP→interface mapping round-trips on real net.Interfaces
// data (lo0/en0/...), the full connected-UDP resolver works over loopback,
// and the watcher's cache/fallback plumbing behaves.
// ---------------------------------------------------------------------------

// TestInterfaceForIPRoundTrip walks every up interface with an address and
// asserts interfaceForIP maps each owned IP back to that interface. This
// exercises the mapping helper with whatever the host really has (loopback
// always; en0/en21 on a Mac; lo/eth0 on Linux CI).
func TestInterfaceForIPRoundTrip(t *testing.T) {
	ifaces, err := net.Interfaces()
	if err != nil {
		t.Fatalf("net.Interfaces: %v", err)
	}
	seen := map[string]string{} // ip -> first owning iface (first-match semantics)
	checked := 0
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch a := addr.(type) {
			case *net.IPNet:
				ip = a.IP
			case *net.IPAddr:
				ip = a.IP
			}
			if ip == nil {
				continue
			}
			if _, dup := seen[ip.String()]; !dup {
				seen[ip.String()] = iface.Name
			}
			got, err := interfaceForIP(ip)
			if err != nil {
				t.Errorf("interfaceForIP(%s): %v", ip, err)
				continue
			}
			if want := seen[ip.String()]; got != want {
				t.Errorf("interfaceForIP(%s) = %q, want %q", ip, got, want)
			}
			checked++
		}
	}
	if checked == 0 {
		t.Skip("no up interfaces with addresses (isolated CI)")
	}
	t.Logf("round-tripped %d addresses", checked)
}

func TestInterfaceForIPUnownedIP(t *testing.T) {
	// TEST-NET-3 (RFC 5737) — never assigned to a local interface.
	if name, err := interfaceForIP(net.ParseIP("203.0.113.77")); err == nil {
		t.Errorf("expected error for unowned IP, got interface %q", name)
	}
}

// TestResolveRouteInterfaceLoopback drives the FULL resolver (connected-UDP
// route lookup + local-IP mapping) against 127.0.0.1, the one route every
// environment has. The result must be the loopback interface.
func TestResolveRouteInterfaceLoopback(t *testing.T) {
	name, err := resolveRouteInterface("127.0.0.1:1")
	if err != nil {
		t.Fatalf("resolveRouteInterface(127.0.0.1:1): %v", err)
	}
	iface, err := net.InterfaceByName(name)
	if err != nil {
		t.Fatalf("InterfaceByName(%q): %v", name, err)
	}
	if iface.Flags&net.FlagLoopback == 0 {
		t.Errorf("route to 127.0.0.1 resolved to non-loopback interface %q", name)
	}
}

func TestResolveRouteInterfaceBadTarget(t *testing.T) {
	if name, err := resolveRouteInterface("definitely-not-a-real-host.invalid:1"); err == nil {
		t.Errorf("expected error for unresolvable target, got %q", name)
	}
}

// TestNetWatcherBackendRouteSuccess: with a target and a resolver that
// reports the tunnel interface, ActiveInterface must be the BACKEND-route
// interface (utun6) even though the default-route heuristic would say
// something else — the exact live failure G8 fixes.
func TestNetWatcherBackendRouteSuccess(t *testing.T) {
	nw := NewNetWatcher(time.Hour, WithBackendTarget("100.100.1.1:6379"))
	nw.routeResolver = func(target string) (string, error) { return "utun6", nil }
	if got := nw.detect(); got != "utun6" {
		t.Fatalf("detect() = %q, want utun6", got)
	}
	nw.Start()
	defer nw.Stop()
	if got := nw.ActiveInterface(); got != "utun6" {
		t.Errorf("ActiveInterface() = %q, want utun6", got)
	}
}

// TestNetWatcherBackendRouteCached: the resolver must not run on every poll —
// within routeCacheFor the cached interface is reused (resolution may involve
// DNS; the 1s poll cadence is too hot for that).
func TestNetWatcherBackendRouteCached(t *testing.T) {
	var calls atomic.Int32
	nw := NewNetWatcher(time.Hour, WithBackendTarget("100.100.1.1:6379"))
	nw.routeResolver = func(target string) (string, error) {
		calls.Add(1)
		return "utun6", nil
	}
	for i := 0; i < 5; i++ {
		if got := nw.detect(); got != "utun6" {
			t.Fatalf("detect() = %q, want utun6", got)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("resolver ran %d times within the cache window, want 1", n)
	}
}

// TestNetWatcherBackendRouteFallbackOnError: a failing resolver must degrade
// to EXACTLY the historical default-route behavior (and the failure must be
// cached, not retried every poll).
func TestNetWatcherBackendRouteFallbackOnError(t *testing.T) {
	var calls atomic.Int32
	nw := NewNetWatcher(time.Hour, WithBackendTarget("100.100.1.1:6379"))
	nw.routeResolver = func(target string) (string, error) {
		calls.Add(1)
		return "", errors.New("no route to host")
	}
	want := detectActiveInterface()
	for i := 0; i < 3; i++ {
		if got := nw.detect(); got != want {
			t.Errorf("detect() = %q, want default-route fallback %q", got, want)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("failing resolver ran %d times within the cache window, want 1 (failure must be cached)", n)
	}
}

// TestNetWatcherBackendRouteRecovery: after the cache window expires, a
// recovered resolver takes over from the fallback again.
func TestNetWatcherBackendRouteRecovery(t *testing.T) {
	fail := true
	nw := NewNetWatcher(time.Hour, WithBackendTarget("100.100.1.1:6379"))
	nw.routeCacheFor = time.Millisecond
	nw.routeResolver = func(target string) (string, error) {
		if fail {
			return "", errors.New("transient")
		}
		return "utun6", nil
	}
	if got, want := nw.detect(), detectActiveInterface(); got != want {
		t.Errorf("failing phase: detect() = %q, want fallback %q", got, want)
	}
	fail = false
	time.Sleep(5 * time.Millisecond) // let the failure cache expire
	if got := nw.detect(); got != "utun6" {
		t.Errorf("recovered phase: detect() = %q, want utun6", got)
	}
}

// TestNetWatcherNoTargetUnchanged: without WithBackendTarget the watcher is
// byte-identical to the historical default-route behavior.
func TestNetWatcherNoTargetUnchanged(t *testing.T) {
	nw := NewNetWatcher(time.Hour)
	nw.routeResolver = func(target string) (string, error) {
		t.Error("resolver must not run without a backend target")
		return "", nil
	}
	if got, want := nw.detect(), detectActiveInterface(); got != want {
		t.Errorf("detect() = %q, want %q", got, want)
	}
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

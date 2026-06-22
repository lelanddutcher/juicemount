package health

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeDialer is a deterministic dialer for Reachability tests. Its
// reach state can be flipped from the test goroutine; subsequent
// probes return success or failure accordingly. Counts attempts so
// tests can assert cadence.
type fakeDialer struct {
	mu        sync.Mutex
	reachable bool
	attempts  atomic.Int64
}

func (f *fakeDialer) setReachable(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reachable = v
}

func (f *fakeDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	f.attempts.Add(1)
	f.mu.Lock()
	reachable := f.reachable
	f.mu.Unlock()
	if !reachable {
		// Simulate the real "no route to host" wait — return after a
		// tiny delay so tests that count probes see them at the same
		// cadence as real probes.
		time.Sleep(5 * time.Millisecond)
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("no route to host")}
	}
	// Return a closeable null conn. Cheap mock.
	left, _ := net.Pipe()
	return left, nil
}

// TestReachability_InitialStateIsReachable verifies the monitor is
// "presumed reachable" before any probe — preventing a spurious
// offline notification at startup.
func TestReachability_InitialStateIsReachable(t *testing.T) {
	r := NewReachability("ignored:0", withDialer(&fakeDialer{reachable: true}))
	if !r.Reachable() {
		t.Errorf("expected initial Reachable()=true, got false")
	}
}

// TestReachability_TransitionToUnreachable verifies the 2-failure
// debounce: one failure stays reachable; two consecutive failures
// transition to unreachable.
func TestReachability_TransitionToUnreachable(t *testing.T) {
	d := &fakeDialer{reachable: false} // start unreachable
	r := NewReachability("ignored:0",
		withDialer(d),
		WithBaseInterval(10*time.Millisecond),
		WithDialTimeout(20*time.Millisecond),
		WithFailureThreshold(2),
	)
	var transitions atomic.Int64
	var lastState atomic.Bool
	r.OnChange(func(reachable bool, _ string) {
		transitions.Add(1)
		lastState.Store(reachable)
	})

	r.Start()
	defer r.Stop()

	// Poll for the transition counter rather than r.Reachable():
	// applyResult releases r.mu BEFORE firing callbacks, so
	// Reachable() can flip a few nanoseconds before transitions
	// increments. Waiting on transitions guarantees the callback
	// chain has run.
	if err := waitFor(500*time.Millisecond, func() bool { return transitions.Load() == 1 }); err != nil {
		t.Fatalf("expected 1 transition, attempts=%d transitions=%d reachable=%v",
			d.attempts.Load(), transitions.Load(), r.Reachable())
	}
	if r.Reachable() {
		t.Errorf("expected Reachable()=false after transition")
	}
	if lastState.Load() {
		t.Errorf("expected last callback state = false (unreachable)")
	}
}

// TestReachability_RecoveryAfterFailure verifies that after going
// unreachable, a single successful probe transitions back to
// reachable.
func TestReachability_RecoveryAfterFailure(t *testing.T) {
	d := &fakeDialer{reachable: false}
	r := NewReachability("ignored:0",
		withDialer(d),
		WithBaseInterval(10*time.Millisecond),
		WithFailureThreshold(2),
		WithSuccessThreshold(1),
	)
	var states []bool
	var statesMu sync.Mutex
	r.OnChange(func(reachable bool, _ string) {
		statesMu.Lock()
		states = append(states, reachable)
		statesMu.Unlock()
	})

	r.Start()
	defer r.Stop()

	// Wait for the unreachable transition.
	if err := waitFor(200*time.Millisecond, func() bool { return !r.Reachable() }); err != nil {
		t.Fatalf("unreachable transition didn't fire: %v", err)
	}
	// Flip the network back on.
	d.setReachable(true)
	if err := waitFor(200*time.Millisecond, func() bool { return r.Reachable() }); err != nil {
		t.Fatalf("recovery transition didn't fire: %v", err)
	}

	statesMu.Lock()
	defer statesMu.Unlock()
	if len(states) != 2 {
		t.Fatalf("expected 2 transitions (false, true), got %d: %v", len(states), states)
	}
	if states[0] || !states[1] {
		t.Errorf("expected transitions [false, true], got %v", states)
	}
}

// TestReachability_SeedUnreachableFiresRecoveryOnFirstSuccess locks in the R-4
// boot-race fix. When the app starts offline and pre-engages auto-offline, it
// seeds the monitor "unreachable" so that even a backend that is reachable from
// the very first probe still produces an unreachable→reachable transition — the
// event that calls SetAutoOffline(false). Without the seed (the default
// "presumed reachable" state) an immediately-reachable backend fires NO
// transition, and the boot-engaged offline mode would never lift.
func TestReachability_SeedUnreachableFiresRecoveryOnFirstSuccess(t *testing.T) {
	d := &fakeDialer{reachable: true} // backend is UP from the first probe
	r := NewReachability("ignored:0",
		withDialer(d),
		WithBaseInterval(10*time.Millisecond),
		WithSuccessThreshold(1),
	)
	var states []bool
	var statesMu sync.Mutex
	r.OnChange(func(reachable bool, _ string) {
		statesMu.Lock()
		states = append(states, reachable)
		statesMu.Unlock()
	})

	r.SeedUnreachable()
	if r.Reachable() {
		t.Fatal("after SeedUnreachable, Reachable() must be false before the first probe")
	}

	r.Start()
	defer r.Stop()

	// The first successful probe must produce a false→true transition.
	if err := waitFor(200*time.Millisecond, func() bool { return r.Reachable() }); err != nil {
		t.Fatalf("seeded recovery transition didn't fire: %v", err)
	}
	statesMu.Lock()
	defer statesMu.Unlock()
	if len(states) != 1 || !states[0] {
		t.Fatalf("expected exactly one [true] transition from seeded-unreachable boot, got %v", states)
	}
}

// TestReachability_SingleFailureDoesNotFlip confirms the debounce
// works at the boundary: one failure followed by a success should
// not produce a transition.
func TestReachability_SingleFailureDoesNotFlip(t *testing.T) {
	d := &fakeDialer{reachable: false}
	// Use a custom test-driven probe scheme by setting a long base
	// interval and triggering manually. Avoids race with the auto-loop.
	r := NewReachability("ignored:0",
		withDialer(d),
		WithBaseInterval(10*time.Second), // effectively manual
		WithFailureThreshold(2),
	)
	var transitions atomic.Int64
	r.OnChange(func(bool, string) { transitions.Add(1) })

	r.Start()
	defer r.Stop()
	// Allow the initial probe to fire and apply.
	time.Sleep(50 * time.Millisecond)

	if transitions.Load() != 0 {
		t.Errorf("expected no transition after 1 failure, got %d", transitions.Load())
	}
	if !r.Reachable() {
		t.Errorf("expected to still be reachable after only 1 failure")
	}

	// Now flip on. Trigger another probe.
	d.setReachable(true)
	r.Notify()
	time.Sleep(50 * time.Millisecond)

	if transitions.Load() != 0 {
		t.Errorf("expected no transition after fail→success (never crossed threshold), got %d", transitions.Load())
	}
}

// TestReachability_NotifyForcesImmediateProbe verifies the Notify
// hook bypasses the base interval.
func TestReachability_NotifyForcesImmediateProbe(t *testing.T) {
	d := &fakeDialer{reachable: true}
	r := NewReachability("ignored:0",
		withDialer(d),
		WithBaseInterval(1*time.Hour), // effectively manual
	)
	r.Start()
	defer r.Stop()

	// Initial probe runs immediately.
	if err := waitFor(100*time.Millisecond, func() bool { return d.attempts.Load() >= 1 }); err != nil {
		t.Fatalf("initial probe didn't fire: %v", err)
	}
	initialAttempts := d.attempts.Load()

	r.Notify()
	if err := waitFor(100*time.Millisecond, func() bool { return d.attempts.Load() > initialAttempts }); err != nil {
		t.Fatalf("Notify did not trigger a probe within 100ms: %v", err)
	}
}

// TestReachability_TimeSinceLastReachable verifies the timer reports
// reasonable values across transitions.
func TestReachability_TimeSinceLastReachable(t *testing.T) {
	d := &fakeDialer{reachable: true}
	r := NewReachability("ignored:0",
		withDialer(d),
		WithBaseInterval(20*time.Millisecond),
	)
	r.Start()
	defer r.Stop()

	// Wait until first probe completes.
	if err := waitFor(200*time.Millisecond, func() bool {
		return r.TimeSinceLastReachable() > 0 && r.TimeSinceLastReachable() < 200*time.Millisecond
	}); err != nil {
		t.Fatalf("TimeSinceLastReachable never landed in expected band: %v (got %v)", err, r.TimeSinceLastReachable())
	}
}

// TestReachability_StopIdempotent verifies Stop is safe to call
// multiple times.
func TestReachability_StopIdempotent(t *testing.T) {
	r := NewReachability("ignored:0", withDialer(&fakeDialer{reachable: true}))
	r.Start()
	r.Stop()
	r.Stop() // must not panic on double-close
}

// waitFor polls cond until it returns true or timeout elapses.
func waitFor(timeout time.Duration, cond func() bool) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return errors.New("timed out waiting for condition")
}

// TestReachabilityAdaptiveDialTimeout is the LAN-no-regress + WAN-tolerance
// guard for the adaptive jitter fix (2026-06-15 cellular test: a fixed 1s dial
// timeout flapped 80x/5min on a 500ms link with 900ms jitter spikes).
func TestReachabilityAdaptiveDialTimeout(t *testing.T) {
	// LAN: sub-ms RTT must leave the dial timeout at the floor — IDENTICAL to
	// the pre-adaptive fixed behavior. This is the no-regression assertion.
	lan := NewReachability("127.0.0.1:1")
	for i := 0; i < 30; i++ {
		lan.observeRTT(300 * time.Microsecond)
	}
	if got := lan.effectiveDialTimeout(); got != lan.dialTimeout {
		t.Errorf("LAN regression: effectiveDialTimeout=%v, want floor %v", got, lan.dialTimeout)
	}

	// WAN/cellular: ~500ms RTT with 850ms jitter spikes must grow the timeout
	// ABOVE the floor and comfortably PAST the spike (so a slow dial no longer
	// false-fails), but never above the ceiling.
	wan := NewReachability("127.0.0.1:1")
	for i := 0; i < 60; i++ {
		s := 500 * time.Millisecond
		if i%3 == 0 {
			s = 850 * time.Millisecond
		}
		wan.observeRTT(s)
	}
	got := wan.effectiveDialTimeout()
	if got <= wan.dialTimeout {
		t.Errorf("WAN: effectiveDialTimeout=%v did not grow above floor %v", got, wan.dialTimeout)
	}
	if got > wan.maxDialTimeout {
		t.Errorf("WAN: effectiveDialTimeout=%v exceeded ceiling %v", got, wan.maxDialTimeout)
	}
	if got < 850*time.Millisecond {
		t.Errorf("WAN: effectiveDialTimeout=%v must tolerate the 850ms jitter spike", got)
	}

	// Adaptive disabled: always the floor regardless of RTT.
	off := NewReachability("127.0.0.1:1")
	off.adaptive = false
	for i := 0; i < 30; i++ {
		off.observeRTT(800 * time.Millisecond)
	}
	if got := off.effectiveDialTimeout(); got != off.dialTimeout {
		t.Errorf("adaptive-off: effectiveDialTimeout=%v, want fixed %v", got, off.dialTimeout)
	}
}

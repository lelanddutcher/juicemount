package health

import (
	"context"
	"errors"
	"io"
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
	// silent models a middlebox that accepts the TCP connection but is not
	// actually the backend — it never sends a RESP reply.
	silent bool
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
	// Return a conn that behaves like a LIVE Redis: the probe now requires a
	// protocol-level answer (any RESP reply), not just a completed handshake,
	// so a mock that never speaks would be — correctly — "unreachable".
	return respondingPipe(f.silent), nil
}

// respondingPipe returns one end of a pipe whose peer answers any request with
// "+PONG\r\n" (a live Redis). With silent=true the peer accepts the connection
// and then says NOTHING — modelling the carrier NAT / middlebox that completes
// a TCP handshake for an address it cannot actually route to. That false
// "reachable" is what wrongly lifted auto-offline on a hotspot.
func respondingPipe(silent bool) net.Conn {
	left, right := net.Pipe()
	go func() {
		defer right.Close()
		buf := make([]byte, 64)
		if _, err := right.Read(buf); err != nil {
			return
		}
		if silent {
			// Hold the connection open, answer nothing.
			time.Sleep(2 * time.Second)
			return
		}
		_, _ = right.Write([]byte("+PONG\r\n"))
	}()
	return left
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
	if err := waitFor(200*time.Millisecond, func() bool {
		statesMu.Lock()
		defer statesMu.Unlock()
		return len(states) == 2
	}); err != nil {
		t.Fatalf("recovery callbacks didn't arrive: %v", err)
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

func TestReachabilityCallbacksPreserveTransitionOrder(t *testing.T) {
	d := &fakeDialer{reachable: false}
	r := NewReachability("ignored:0",
		withDialer(d),
		WithBaseInterval(10*time.Second),
		WithFailureThreshold(1),
		WithSuccessThreshold(1),
	)
	var states []bool
	var mu sync.Mutex
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	r.OnChange(func(reachable bool, _ string) {
		if !reachable {
			close(firstEntered)
			<-releaseFirst
		}
		mu.Lock()
		states = append(states, reachable)
		mu.Unlock()
	})

	r.Start()
	defer r.Stop()
	select {
	case <-firstEntered:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("offline callback did not start")
	}

	// Recover while the offline callback is deliberately parked. The probe and
	// state machine must advance, but the online callback must remain ordered
	// behind the offline callback.
	d.setReachable(true)
	r.Notify()
	if err := waitFor(500*time.Millisecond, r.Reachable); err != nil {
		t.Fatalf("slow callback parked reachability recovery: %v", err)
	}
	close(releaseFirst)
	if err := waitFor(500*time.Millisecond, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(states) == 2
	}); err != nil {
		t.Fatalf("ordered callbacks did not drain: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if states[0] || !states[1] {
		t.Fatalf("callback order = %v, want [false true]", states)
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

// TestReachability_DrainLivenessSuppressesFalseFlap is the fix-(a) gate for the
// NFSv3-sprint slow-link false-flap. When a Finder copy saturates the uplink,
// the drainer's bulk PUT traffic queues a cold probe SYN past its dial timeout —
// the dial FAILS though the backend is provably reachable (a drain just landed
// over the same link). The monitor must consult the liveness hook on a probe
// failure and SUPPRESS the false-failure (no fail-streak advance, no
// unreachable transition) while a real drain is recent. A genuine outage stops
// the drains → the window lapses → the normal 2-failure flip proceeds.
//
// All three subtests drive the SAME failing dialer so the ONLY variable is the
// hook, proving the override (not some incidental timing) is what gates the
// flip — i.e. the green case cannot false-pass via a never-failing probe.
func TestReachability_DrainLivenessSuppressesFalseFlap(t *testing.T) {
	t.Run("recent_drain_suppresses_flip", func(t *testing.T) {
		d := &fakeDialer{reachable: false} // every probe FAILS (saturated-uplink stand-in)
		// Hook reports a drain 1s ago — well inside the ~2*baseInterval window.
		r := NewReachability("ignored:0",
			withDialer(d),
			WithBaseInterval(10*time.Millisecond),
			WithFailureThreshold(2),
			WithLivenessHook(func() time.Duration { return 1 * time.Second }),
		)
		// livenessWindow scales from baseInterval (2*10ms=20ms); 1s would be
		// STALE against that. Set an explicit, realistic window so the test
		// asserts the INTENT (a recent drain suppresses) rather than the tiny
		// scaled value. This is a white-box field set, same idiom as the
		// adaptive-timeout test above poking .adaptive/.observeRTT.
		r.livenessWindow = 4 * time.Second

		var transitions atomic.Int64
		r.OnChange(func(bool, string) { transitions.Add(1) })

		r.Start()
		defer r.Stop()

		// Let MANY probes fail (each would normally bump the fail streak). With
		// the override active, NONE may flip the state.
		if err := waitFor(300*time.Millisecond, func() bool { return d.attempts.Load() >= 5 }); err != nil {
			t.Fatalf("probes did not run: attempts=%d", d.attempts.Load())
		}
		if transitions.Load() != 0 {
			t.Errorf("liveness override failed: %d transition(s) on a failing dial with a recent drain — must suppress", transitions.Load())
		}
		if !r.Reachable() {
			t.Error("liveness override failed: monitor went unreachable despite a recent proven drain")
		}
	})

	t.Run("stale_drain_flips_as_before", func(t *testing.T) {
		d := &fakeDialer{reachable: false}
		// Hook reports the last drain was 10 minutes ago — far outside any
		// window. A real outage looks like this (drains stopped landing).
		r := NewReachability("ignored:0",
			withDialer(d),
			WithBaseInterval(10*time.Millisecond),
			WithFailureThreshold(2),
			WithLivenessHook(func() time.Duration { return 10 * time.Minute }),
		)
		var transitions atomic.Int64
		var lastState atomic.Bool
		lastState.Store(true)
		r.OnChange(func(reachable bool, _ string) {
			transitions.Add(1)
			lastState.Store(reachable)
		})

		r.Start()
		defer r.Stop()

		if err := waitFor(500*time.Millisecond, func() bool { return transitions.Load() == 1 }); err != nil {
			t.Fatalf("stale hook must NOT suppress: expected the normal unreachable flip, attempts=%d transitions=%d",
				d.attempts.Load(), transitions.Load())
		}
		if lastState.Load() {
			t.Error("expected the flip to be → unreachable (false) with a stale drain")
		}
	})

	t.Run("absent_hook_flips_as_before", func(t *testing.T) {
		d := &fakeDialer{reachable: false}
		r := NewReachability("ignored:0",
			withDialer(d),
			WithBaseInterval(10*time.Millisecond),
			WithFailureThreshold(2),
		) // no WithLivenessHook → override inert
		var transitions atomic.Int64
		r.OnChange(func(bool, string) { transitions.Add(1) })

		r.Start()
		defer r.Stop()

		if err := waitFor(500*time.Millisecond, func() bool { return transitions.Load() == 1 }); err != nil {
			t.Fatalf("no hook must behave exactly like today (flip after 2 fails): attempts=%d transitions=%d",
				d.attempts.Load(), transitions.Load())
		}
		if r.Reachable() {
			t.Error("expected Reachable()=false after 2 failures with no liveness hook")
		}
	})
}

// TestReachability_DrainLivenessKillSwitchByteIdentical locks the WAN-tuning
// revert discipline: JM_REACH_DRAIN_LIVENESS=0 must make behavior byte-identical
// to the pre-fix monitor — the liveness hook is NOT consulted and a probe
// failure flips after the normal threshold EVEN WHEN a drain just landed. This
// is the 10GbE/regression guarantee in docs/TUNING/REVERT_LOG.md.
func TestReachability_DrainLivenessKillSwitchByteIdentical(t *testing.T) {
	t.Setenv("JM_REACH_DRAIN_LIVENESS", "0")

	d := &fakeDialer{reachable: false}
	hookCalls := atomic.Int64{}
	r := NewReachability("ignored:0",
		withDialer(d),
		WithBaseInterval(10*time.Millisecond),
		WithFailureThreshold(2),
		// A "recent drain" that WOULD suppress if the override were on.
		WithLivenessHook(func() time.Duration { hookCalls.Add(1); return 0 }),
	)
	r.livenessWindow = 4 * time.Second // generous — irrelevant when override is OFF

	if r.livenessOverride {
		t.Fatal("JM_REACH_DRAIN_LIVENESS=0 must clear livenessOverride")
	}

	var transitions atomic.Int64
	var lastState atomic.Bool
	lastState.Store(true)
	r.OnChange(func(reachable bool, _ string) {
		transitions.Add(1)
		lastState.Store(reachable)
	})

	r.Start()
	defer r.Stop()

	// Identical to the pre-fix path: 2 failures → unreachable, hook never read.
	if err := waitFor(500*time.Millisecond, func() bool { return transitions.Load() == 1 }); err != nil {
		t.Fatalf("kill-switch must restore the old flip: attempts=%d transitions=%d",
			d.attempts.Load(), transitions.Load())
	}
	if lastState.Load() {
		t.Error("expected → unreachable (false) flip with the override killed")
	}
	if hookCalls.Load() != 0 {
		t.Errorf("override disabled but liveness hook was consulted %d time(s) — must be byte-identical (never called)", hookCalls.Load())
	}
}

// TestReachability_MiddleboxHandshakeIsNotReachable is the regression test for
// the bug that produced the user-reported symptom.
//
// Observed live 2026-07-29 on an iPhone hotspot, where the NAS (192.168.0.x) is
// genuinely unroutable: the probe's bare TCP dial SUCCEEDED — carrier NAT
// answered the SYN — so the monitor logged "reachability transition →
// reachable" and lifted auto-offline, while a real Redis PING to that same
// address timed out and ping(8) reported 100% packet loss.
//
// Lifting auto-offline on that false signal leaves the app ONLINE WITH A DEAD
// BACKEND, which is worse than either honest state: navigation still feels fine
// (the mirror serves it, ~25ms) but the FIRST open of every file walks the full
// timeout stack before falling back to locally cached blocks — measured 1,527ms
// / 6,250ms / 28,150ms for 32 KB, versus 24-36ms for later reads of the same
// file. Hence "offline mode is amazing, but with offline mode disabled
// performance is absolute trash and even my pinned files struggle".
//
// A completed handshake must NOT count as reachable.
func TestReachability_MiddleboxHandshakeIsNotReachable(t *testing.T) {
	d := &fakeDialer{reachable: true, silent: true} // connects, never answers
	r := NewReachability("ignored:0",
		WithBaseInterval(20*time.Millisecond),
		WithDialTimeout(150*time.Millisecond),
		WithFailureThreshold(2),
		withDialer(d),
	)
	r.Start()
	defer r.Stop()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !r.Reachable() {
			return // correctly refused to call a silent middlebox "reachable"
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("a TCP handshake with no RESP reply was treated as REACHABLE — " +
		"this lifts auto-offline against a dead backend and reintroduces the " +
		"multi-second first-open stall on cellular")
}

func TestWithDialContextUsesFarSideDialerAndRequiresRESP(t *testing.T) {
	var gotNetwork, gotAddress string
	r := NewReachability("nas.private:6379", WithDialContext(func(_ context.Context, network, address string) (net.Conn, error) {
		gotNetwork, gotAddress = network, address
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			request := make([]byte, len("PING\r\n"))
			if _, err := io.ReadFull(server, request); err != nil {
				return
			}
			if string(request) == "PING\r\n" {
				_, _ = server.Write([]byte("+PONG\r\n"))
			}
		}()
		return client, nil
	}))
	if !r.probe() {
		t.Fatal("far-side Redis PING through custom dialer was not accepted")
	}
	if gotNetwork != "tcp" || gotAddress != "nas.private:6379" {
		t.Fatalf("custom dialer received (%q, %q)", gotNetwork, gotAddress)
	}
}

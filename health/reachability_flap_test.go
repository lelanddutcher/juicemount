package health

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

// scriptedProbe returns a WithProbe function whose results come from ok. A
// failing probe returns at once (the state machine only sees the result).
func scriptedProbe(ok *atomic.Bool) func(context.Context) (time.Duration, error) {
	return func(context.Context) (time.Duration, error) {
		if ok.Load() {
			return time.Millisecond, nil
		}
		return 0, errors.New("probe lost")
	}
}

// TestReachabilityLossySequenceStaysOnline drives the state machine with the
// probe pattern a lossy WAN produces: isolated failures between successes.
// Before the fix one failure flipped the volume offline (the 2026-11-11 lab saw
// 38 flips in 80 minutes at 1% loss each way). Now an isolated failure only
// opens a streak that the next success closes.
func TestReachabilityLossySequenceStaysOnline(t *testing.T) {
	var ok atomic.Bool
	r := NewReachability("ignored:0", WithProbe(scriptedProbe(&ok)))
	var transitions atomic.Int64
	r.OnChange(func(bool, string) { transitions.Add(1) })

	// 1% loss each way on a 4-packet probe, exaggerated to 10% so the test
	// sees many failures. A fixed seed keeps the run deterministic, and the
	// script never puts two failures back to back (that is the dead-link case).
	rng := rand.New(rand.NewSource(20261111))
	failures := 0
	prevFailed := false
	for i := 0; i < 2000; i++ {
		fail := !prevFailed && rng.Float64() < 0.10
		ok.Store(!fail)
		r.probeAndUpdate()
		if fail {
			failures++
		}
		prevFailed = fail
		if !r.Reachable() {
			t.Fatalf("probe %d: went offline on an isolated failure", i)
		}
	}
	if failures < 100 {
		t.Fatalf("script produced only %d failures; the test is not exercising loss", failures)
	}
	if n := transitions.Load(); n != 0 {
		t.Fatalf("%d transitions on a lossy but working link, want 0", n)
	}
}

// TestReachabilityTrafficProofKeepsLossyLinkOnline covers back-to-back probe
// failures while real traffic still succeeds (reads and lookups flowing on the
// same link). Any recent Redis reply or drain is proof the link works, so even
// consecutive probe failures must not flip the state.
func TestReachabilityTrafficProofKeepsLossyLinkOnline(t *testing.T) {
	var ok atomic.Bool // every probe fails
	r := NewReachability("ignored:0",
		WithProbe(scriptedProbe(&ok)),
		WithLivenessHook(func() time.Duration { return time.Duration(math.MaxInt64) }), // no drain ever
		WithLivenessHook(func() time.Duration { return 500 * time.Millisecond }),       // Redis replied 0.5 s ago
	)
	for i := 0; i < 20; i++ {
		r.probeAndUpdate()
	}
	if !r.Reachable() {
		t.Fatal("went offline although Redis traffic proved the link live")
	}
	r.mu.RLock()
	fails := r.consecutiveFails
	r.mu.RUnlock()
	if fails != 0 {
		t.Fatalf("consecutiveFails=%d, want 0: traffic proof must reset the streak", fails)
	}
}

// TestReachabilityTrafficProofResetsOpenStreak checks the order case: a probe
// fails with no proof, then traffic proves the link, then another probe fails
// once the proof is stale. The middle proof broke the streak, so the third
// probe is a first failure, not a second.
func TestReachabilityTrafficProofResetsOpenStreak(t *testing.T) {
	var ok atomic.Bool
	var proofAge atomic.Int64
	proofAge.Store(int64(time.Hour))
	r := NewReachability("ignored:0",
		WithProbe(scriptedProbe(&ok)),
		WithLivenessHook(func() time.Duration { return time.Duration(proofAge.Load()) }),
	)
	r.probeAndUpdate() // failure 1, no proof
	proofAge.Store(int64(100 * time.Millisecond))
	r.probeAndUpdate() // suppressed: proof is recent
	proofAge.Store(int64(time.Hour))
	r.probeAndUpdate() // failure 1 again
	if !r.Reachable() {
		t.Fatal("went offline although traffic proof broke the failure streak")
	}
	r.probeAndUpdate() // failure 2 with no proof: a real outage
	if r.Reachable() {
		t.Fatal("two consecutive unproven failures must go offline")
	}
}

// TestReachabilityDeadLinkGoesOfflineWithinBound runs the real probe loop on a
// link that dies and checks the documented detection bound:
//
//	I + 2T + c   (I base interval, T probe timeout, c confirm delay)
//
// The probe blocks until its context deadline, like a dial to a dead route.
// Durations are scaled down; the production values (I=2s, T=1s LAN,
// c=250ms) give about 4.3 s.
func TestReachabilityDeadLinkGoesOfflineWithinBound(t *testing.T) {
	const (
		interval = 200 * time.Millisecond
		timeout  = 100 * time.Millisecond
		confirm  = 50 * time.Millisecond
	)
	var alive atomic.Bool
	alive.Store(true)
	var dead atomic.Int64 // UnixNano when the link died
	probe := func(ctx context.Context) (time.Duration, error) {
		if alive.Load() {
			return time.Millisecond, nil
		}
		<-ctx.Done()
		return 0, ctx.Err()
	}
	r := NewReachability("ignored:0",
		WithProbe(probe),
		WithBaseInterval(interval),
		WithDialTimeout(timeout),
		WithConfirmDelay(confirm),
	)
	offlineAt := make(chan time.Time, 1)
	r.OnChange(func(reachable bool, _ string) {
		if !reachable {
			select {
			case offlineAt <- time.Now():
			default:
			}
		}
	})
	r.Start()
	defer r.Stop()

	time.Sleep(interval / 2) // let the initial probe succeed
	dead.Store(time.Now().UnixNano())
	alive.Store(false)

	bound := interval + 2*timeout + confirm
	select {
	case at := <-offlineAt:
		took := at.Sub(time.Unix(0, dead.Load()))
		// Scheduler slack on a busy, shared CI host. Generous on purpose:
		// the check is that detection is bounded, not that timers are exact.
		if took > bound+500*time.Millisecond {
			t.Fatalf("dead link went offline after %v, bound %v", took, bound)
		}
		if took < 2*timeout {
			t.Fatalf("went offline after %v, faster than two probe timeouts (%v): only one failure counted?", took, 2*timeout)
		}
	case <-time.After(10 * bound):
		t.Fatalf("dead link never went offline (bound %v)", bound)
	}
}

// TestReachabilityConfirmProbeRunsEarly checks that a single failure schedules
// the confirming probe after confirmDelay, and that no confirm is pending once
// the streak closes or the state has flipped.
func TestReachabilityConfirmProbeRunsEarly(t *testing.T) {
	var ok atomic.Bool
	r := NewReachability("ignored:0",
		WithProbe(scriptedProbe(&ok)),
		WithConfirmDelay(250*time.Millisecond),
	)
	if _, due := r.confirmDue(); due {
		t.Fatal("confirm pending with no failure")
	}
	r.probeAndUpdate() // one failure
	if d, due := r.confirmDue(); !due || d != 250*time.Millisecond {
		t.Fatalf("after one failure confirmDue=(%v,%v), want (250ms,true)", d, due)
	}
	ok.Store(true)
	r.probeAndUpdate()
	if _, due := r.confirmDue(); due {
		t.Fatal("confirm pending after recovery")
	}
	// Once offline, probes return to the base cadence (no busy retry).
	ok.Store(false)
	r.probeAndUpdate()
	r.probeAndUpdate()
	if r.Reachable() {
		t.Fatal("expected offline after two failures")
	}
	if _, due := r.confirmDue(); due {
		t.Fatal("confirm pending while offline")
	}
}

// TestReachabilityProbeDurationDoesNotStretchCadence is the M1 regression.
// Resetting the ticker after each probe made the cycle interval + probe time,
// which slowed recovery after an outage. While offline, slow failing probes
// must still start once per interval.
func TestReachabilityProbeDurationDoesNotStretchCadence(t *testing.T) {
	const (
		interval = 100 * time.Millisecond
		timeout  = 70 * time.Millisecond
	)
	var attempts atomic.Int64
	probe := func(ctx context.Context) (time.Duration, error) {
		attempts.Add(1)
		<-ctx.Done()
		return 0, ctx.Err()
	}
	r := NewReachability("ignored:0",
		WithProbe(probe),
		WithBaseInterval(interval),
		WithDialTimeout(timeout),
		WithConfirmDelay(10*time.Millisecond),
	)
	r.SeedUnreachable() // already offline: only the regular cadence runs
	r.Start()
	defer r.Stop()
	time.Sleep(2 * time.Second)
	// Fixed cadence: about 20 probes. Stretched (interval + timeout): about 12.
	if n := attempts.Load(); n < 16 {
		t.Fatalf("%d probes in 2 s at a %v interval: the probe duration is stretching the cadence", n, interval)
	}
}

// TestReachabilityDeadLinkWithTrafficUntilCutWithinBound is the realistic
// case: Redis traffic flows right up to the moment the link dies, so the
// liveness window W must lapse before failures count. Bound W + I + 2T + c;
// with production values that is about 8.3 s on a LAN.
func TestReachabilityDeadLinkWithTrafficUntilCutWithinBound(t *testing.T) {
	const (
		interval = 200 * time.Millisecond
		timeout  = 100 * time.Millisecond
		confirm  = 50 * time.Millisecond
	)
	var alive atomic.Bool
	alive.Store(true)
	var lastTraffic atomic.Int64
	lastTraffic.Store(time.Now().UnixNano())
	stopTraffic := make(chan struct{})
	go func() { // a steady stream of Redis replies while the link is up
		for {
			select {
			case <-stopTraffic:
				return
			case <-time.After(20 * time.Millisecond):
				if alive.Load() {
					lastTraffic.Store(time.Now().UnixNano())
				}
			}
		}
	}()
	defer close(stopTraffic)
	probe := func(ctx context.Context) (time.Duration, error) {
		if alive.Load() {
			return time.Millisecond, nil
		}
		<-ctx.Done()
		return 0, ctx.Err()
	}
	r := NewReachability("ignored:0",
		WithProbe(probe),
		WithBaseInterval(interval),
		WithDialTimeout(timeout),
		WithConfirmDelay(confirm),
		WithLivenessHook(func() time.Duration { return time.Since(time.Unix(0, lastTraffic.Load())) }),
	)
	window := r.livenessWindow // 2 * interval
	offlineAt := make(chan time.Time, 1)
	r.OnChange(func(reachable bool, _ string) {
		if !reachable {
			select {
			case offlineAt <- time.Now():
			default:
			}
		}
	})
	r.Start()
	defer r.Stop()

	time.Sleep(interval / 2)
	cut := time.Now()
	alive.Store(false)

	bound := window + interval + 2*timeout + confirm
	select {
	case at := <-offlineAt:
		took := at.Sub(cut)
		if took > bound+500*time.Millisecond {
			t.Fatalf("went offline %v after the cut, bound %v", took, bound)
		}
		if took < window {
			t.Fatalf("went offline %v after the cut, before the %v traffic window lapsed", took, window)
		}
	case <-time.After(10 * bound):
		t.Fatalf("dead link never went offline (bound %v)", bound)
	}
}

// TestReachabilityWANBudgetCoversOneRetransmit reproduces the lab link: 200 ms
// RTT, so a probe (TCP handshake + PING) takes about 410 ms. The old budget
// 2*srtt+4*rttvar was below the 1 s floor, so a single lost SYN (1 s
// retransmit) failed the probe. The budget must now cover probe + one
// retransmit, and a LAN must keep the exact 1 s floor.
func TestReachabilityWANBudgetCoversOneRetransmit(t *testing.T) {
	wan := NewReachability("127.0.0.1:1")
	for i := 0; i < 50; i++ {
		wan.observeRTT(410 * time.Millisecond)
	}
	if got, want := wan.effectiveDialTimeout(), 410*time.Millisecond+retransmitAllowance; got < want {
		t.Fatalf("WAN budget %v, want >= %v (probe plus one retransmit)", got, want)
	}

	lan := NewReachability("127.0.0.1:1")
	for i := 0; i < 50; i++ {
		lan.observeRTT(2 * time.Millisecond)
	}
	if got := lan.effectiveDialTimeout(); got != lan.dialTimeout {
		t.Fatalf("LAN budget %v, want floor %v", got, lan.dialTimeout)
	}

	t.Setenv("JM_REACH_WAN_RETRANSMIT", "0")
	noAllowance := NewReachability("127.0.0.1:1")
	for i := 0; i < 50; i++ {
		noAllowance.observeRTT(410 * time.Millisecond)
	}
	if got := noAllowance.effectiveDialTimeout(); got >= 410*time.Millisecond+retransmitAllowance {
		t.Fatalf("JM_REACH_WAN_RETRANSMIT=0 budget %v still includes the allowance", got)
	}

	t.Setenv("JM_REACH_ADAPTIVE", "0")
	fixed := NewReachability("127.0.0.1:1")
	for i := 0; i < 50; i++ {
		fixed.observeRTT(410 * time.Millisecond)
	}
	if got := fixed.effectiveDialTimeout(); got != fixed.dialTimeout {
		t.Fatalf("JM_REACH_ADAPTIVE=0 budget %v, want fixed %v", got, fixed.dialTimeout)
	}
}

// TestReachabilityFailsToOfflineKillSwitch checks the revert switch: with
// JM_REACH_FAILS_TO_OFFLINE=1 one failure flips again, overriding the
// bridge's WithFailureThreshold(2).
func TestReachabilityFailsToOfflineKillSwitch(t *testing.T) {
	t.Setenv("JM_REACH_FAILS_TO_OFFLINE", "1")
	var ok atomic.Bool
	r := NewReachability("ignored:0", WithProbe(scriptedProbe(&ok)), WithFailureThreshold(2))
	r.probeAndUpdate()
	if r.Reachable() {
		t.Fatal("JM_REACH_FAILS_TO_OFFLINE=1: one failure must flip")
	}
}

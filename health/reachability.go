// Package health — reachability monitor.
//
// Reachability actively probes whether a configured backend host:port is
// reachable from this Mac. Used by the offline-mode auto-engage path: when
// the network drops, we want to know within ~5 seconds so we can fail-fast
// un-pinned reads instead of letting the kernel-NFS timeout cascade into
// Finder hangs.
//
// Design constraints (from docs/ROADMAP/tier-4-network-resilience.md
// "NETWORK-MONITOR-SPECIFIC NON-NEGOTIABLES"):
//
//   - The probe MUST be a cheap TCP connect, not a Redis EVAL or a MinIO
//     HEAD. Don't load the network we're already worried is degraded.
//   - Don't flip state on a single failed probe — use a debounced signal.
//   - Distinguish "currently reachable" from "last seen reachable N ago"
//     so consumers can implement their own grace windows.
//
// Cadence: 2-second base probe interval, 2-consecutive-failure threshold
// to transition reachable→unreachable, 1-success threshold to transition
// back. Net detection time on a real network drop: ~4 seconds (within
// the 5-second target). Probe cost: <1 ms per attempt on a healthy LAN.
package health

import (
	"context"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// ReachabilityCallback is invoked whenever the monitor's reachable
// state transitions. `reachable` is the new state; `reason` is a
// human-readable explanation suitable for logging or UI. Callbacks
// fire from the monitor's polling goroutine; they MUST NOT block —
// dispatch to your own goroutine if needed.
type ReachabilityCallback func(reachable bool, reason string)

// dialer is the interface segment of net.Dialer that we need. Pulled
// out so tests can inject a fake without touching real sockets.
type dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// Reachability monitors whether a backend host:port is reachable from
// this machine. Concurrent-safe; one instance per app process.
type Reachability struct {
	target         string
	dialTimeout    time.Duration // FLOOR for the per-probe TCP dial timeout
	baseInterval   time.Duration // FLOOR for the probe cadence
	maxDialTimeout time.Duration // ceiling for the adaptive dial timeout
	adaptive       bool          // grow dialTimeout/interval with observed RTT
	failsToOffline int           // consecutive failures before flipping reachable→unreachable
	passesToOnline int           // consecutive successes before flipping unreachable→reachable

	// [JM6] Adaptive jitter tolerance (WAN/cellular). TCP-RTO-style smoothed
	// RTT + variance, measured from SUCCESSFUL probe dials. Only the probe loop
	// goroutine touches these (probe→update→next probe are sequential in loop()),
	// so no lock is needed. The effective dial timeout is
	// clamp(dialTimeout, 2*srtt + 4*rttvar, maxDialTimeout): on a LAN (sub-ms
	// RTT) the formula is far below the dialTimeout floor, so it clamps to the
	// floor and behavior is IDENTICAL to the fixed 1s/2s defaults — adaptation
	// engages only when RTT climbs into the hundreds of ms (a real WAN link),
	// where a fixed 1s timeout would otherwise flap on jitter spikes.
	srtt   time.Duration
	rttvar time.Duration

	// rttObserver, if set, receives every successful-probe dial latency. The
	// network profile (netprofile) uses it to bootstrap link classification from
	// RTT before any throughput sample arrives. Kept as a plain func so this
	// package stays free of a netprofile import (clean layering). Called from the
	// probe loop goroutine; MUST NOT block.
	rttObserver func(time.Duration)

	dialer dialer

	mu               sync.RWMutex
	reachable        bool
	lastReachableAt  time.Time
	lastTransitionAt time.Time
	consecutiveFails int
	consecutivePass  int

	callbacksMu sync.Mutex
	callbacks   []ReachabilityCallback

	triggerCh chan struct{}
	stopCh    chan struct{}
	stopOnce  sync.Once
	running   atomic.Bool
}

// ReachabilityOption customizes monitor behavior. Defaults are tuned
// for tier-1's 5-second offline-detection target.
type ReachabilityOption func(*Reachability)

// WithBaseInterval sets the probe cadence under steady state.
// Default: 2 * time.Second.
func WithBaseInterval(d time.Duration) ReachabilityOption {
	return func(r *Reachability) { r.baseInterval = d }
}

// WithDialTimeout sets the per-probe TCP dial timeout. Default 1s.
// Should be strictly less than the base interval.
func WithDialTimeout(d time.Duration) ReachabilityOption {
	return func(r *Reachability) { r.dialTimeout = d }
}

// WithFailureThreshold sets the consecutive-failure count required
// to transition reachable→unreachable. Default 2.
func WithFailureThreshold(n int) ReachabilityOption {
	return func(r *Reachability) { r.failsToOffline = n }
}

// WithSuccessThreshold sets the consecutive-success count required
// to transition unreachable→reachable. Default 1 (fast recovery).
func WithSuccessThreshold(n int) ReachabilityOption {
	return func(r *Reachability) { r.passesToOnline = n }
}

// withDialer injects a custom dialer (test hook). Unexported because
// production code never needs it.
func withDialer(d dialer) ReachabilityOption {
	return func(r *Reachability) { r.dialer = d }
}

// WithRTTObserver registers a callback invoked with the dial latency of every
// SUCCESSFUL probe. Used to feed the network-profile link estimator. The
// callback runs on the probe-loop goroutine and MUST NOT block.
func WithRTTObserver(fn func(time.Duration)) ReachabilityOption {
	return func(r *Reachability) { r.rttObserver = fn }
}

// NewReachability constructs a monitor against the given "host:port"
// target. The monitor starts in the "presumed reachable" state — it
// won't flip to unreachable until consecutive probes fail. This
// prevents a spurious "offline" notification at startup when the
// first probe is racing application initialization.
func NewReachability(target string, opts ...ReachabilityOption) *Reachability {
	r := &Reachability{
		target:         target,
		dialTimeout:    1 * time.Second,
		baseInterval:   2 * time.Second,
		maxDialTimeout: 10 * time.Second,
		adaptive:       true,
		failsToOffline: 2,
		passesToOnline: 1,
		reachable:      true, // presumed reachable until proven otherwise
		dialer:         &net.Dialer{},
		triggerCh:      make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
	}
	for _, opt := range opts {
		opt(r)
	}
	// Env overrides (operator escape hatch; defaults are LAN-safe).
	//   JM_REACH_ADAPTIVE=0        disable adaptive growth (fixed dialTimeout)
	//   JM_REACH_MAX_DIAL_MS=<n>   ceiling for the adaptive dial timeout
	if os.Getenv("JM_REACH_ADAPTIVE") == "0" {
		r.adaptive = false
	}
	if v := os.Getenv("JM_REACH_MAX_DIAL_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms >= 1000 {
			r.maxDialTimeout = time.Duration(ms) * time.Millisecond
		}
	}
	return r
}

// effectiveDialTimeout returns the per-probe dial timeout, grown to tolerate WAN
// jitter when adaptation is on. clamp(dialTimeout, 2*srtt+4*rttvar, maxDialTimeout):
// on a LAN the RTT-based term is far below the floor so this returns dialTimeout
// exactly (no behavior change); on a 500ms cellular link it grows to ~1.8s so a
// jittery dial no longer false-fails. Called only from the probe loop goroutine.
func (r *Reachability) effectiveDialTimeout() time.Duration {
	if !r.adaptive {
		return r.dialTimeout
	}
	want := 2*r.srtt + 4*r.rttvar
	if want < r.dialTimeout {
		return r.dialTimeout
	}
	if want > r.maxDialTimeout {
		return r.maxDialTimeout
	}
	return want
}

// observeRTT folds a successful-probe dial duration into the smoothed RTT/var
// (RFC 6298 style: alpha=1/8, beta=1/4). Loop-goroutine only.
func (r *Reachability) observeRTT(sample time.Duration) {
	if r.srtt == 0 {
		r.srtt = sample
		r.rttvar = sample / 2
		return
	}
	diff := r.srtt - sample
	if diff < 0 {
		diff = -diff
	}
	r.rttvar += (diff - r.rttvar) / 4
	r.srtt += (sample - r.srtt) / 8
}

// Start begins probing in a background goroutine. Safe to call once
// per instance; subsequent calls are no-ops.
func (r *Reachability) Start() {
	if !r.running.CompareAndSwap(false, true) {
		return
	}
	go r.loop()
}

// Stop halts the monitor. Idempotent.
func (r *Reachability) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
}

// OnChange registers a callback for reachability transitions.
// Multiple callbacks may be registered; they fire in registration
// order from the monitor's polling goroutine.
func (r *Reachability) OnChange(cb ReachabilityCallback) {
	r.callbacksMu.Lock()
	defer r.callbacksMu.Unlock()
	r.callbacks = append(r.callbacks, cb)
}

// Reachable reports the current reachable state.
func (r *Reachability) Reachable() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.reachable
}

// TimeSinceLastReachable returns how long it's been since we last
// observed the backend as reachable. Returns 0 if we've never seen
// it reachable (i.e., still in the initial pre-probe state).
func (r *Reachability) TimeSinceLastReachable() time.Duration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.lastReachableAt.IsZero() {
		return 0
	}
	return time.Since(r.lastReachableAt)
}

// LastTransitionAt returns when the current reachable state was
// entered. Useful for "disconnected for Mm:Ss" UI strings.
func (r *Reachability) LastTransitionAt() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastTransitionAt
}

// Notify forces an immediate probe outside the regular cadence.
// Consumers that detect a network interface change (e.g., NetWatcher)
// can call this to converge faster than the base interval would
// allow. Non-blocking: if a probe is already pending, the call is
// dropped silently.
func (r *Reachability) Notify() {
	select {
	case r.triggerCh <- struct{}{}:
	default:
	}
}

func (r *Reachability) loop() {
	// Initial probe runs immediately so Reachable() reflects real
	// state within one dialTimeout of Start().
	r.probeAndUpdate()

	ticker := time.NewTicker(r.baseInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.probeAndUpdate()
		case <-r.triggerCh:
			r.probeAndUpdate()
			// Reset the ticker so we don't immediately probe again.
			ticker.Reset(r.baseInterval)
		}
	}
}

// probeAndUpdate runs one probe and applies the result to the state
// machine. Fires callbacks on transition.
func (r *Reachability) probeAndUpdate() {
	ok := r.probe()
	r.applyResult(ok)
}

// probe attempts a single TCP dial with the (possibly adaptive) timeout.
// Returns true on success and folds the dial latency into the smoothed RTT so
// the timeout self-tunes to the link.
func (r *Reachability) probe() bool {
	ctx, cancel := context.WithTimeout(context.Background(), r.effectiveDialTimeout())
	defer cancel()
	start := time.Now()
	conn, err := r.dialer.DialContext(ctx, "tcp", r.target)
	if err != nil {
		return false
	}
	rtt := time.Since(start)
	r.observeRTT(rtt)
	if r.rttObserver != nil {
		r.rttObserver(rtt)
	}
	_ = conn.Close()
	return true
}

// applyResult updates the state machine. Holds r.mu only briefly;
// fires callbacks outside the lock so callback delays can't park
// the loop holding a writer lock.
func (r *Reachability) applyResult(ok bool) {
	var (
		transitioned bool
		newState     bool
		reason       string
	)
	now := time.Now()

	r.mu.Lock()
	if ok {
		r.lastReachableAt = now
		r.consecutiveFails = 0
		r.consecutivePass++
		if !r.reachable && r.consecutivePass >= r.passesToOnline {
			r.reachable = true
			r.lastTransitionAt = now
			transitioned = true
			newState = true
			reason = "probe succeeded — backend reachable"
		}
	} else {
		r.consecutivePass = 0
		r.consecutiveFails++
		if r.reachable && r.consecutiveFails >= r.failsToOffline {
			r.reachable = false
			r.lastTransitionAt = now
			transitioned = true
			newState = false
			reason = "probe failed " + strconv.Itoa(r.consecutiveFails) + "x — backend unreachable"
		}
	}
	r.mu.Unlock()

	if transitioned {
		if newState {
			jmlog.Info("reachability transition", "target", r.target, "state", "reachable", "reason", reason)
		} else {
			jmlog.Warn("reachability transition", "target", r.target, "state", "unreachable", "reason", reason)
		}
		r.callbacksMu.Lock()
		callbacks := append([]ReachabilityCallback(nil), r.callbacks...)
		r.callbacksMu.Unlock()
		// QA-15 defense (2026-05-17): dispatch callbacks ASYNCHRONOUSLY.
		// The ReachabilityCallback doc already specifies "MUST NOT block",
		// but a misbehaving callback (e.g., one that does a Redis round
		// trip without a deadline, or grabs a contended mutex) could park
		// this probe loop indefinitely and prevent recovery transitions
		// from ever firing. That is exactly the failure mode QA-15
		// documented: 15 min stuck unreachable while external probes to
		// the same target succeed. Wrapping each callback in `go` ensures
		// a hung callback is at most a leaked goroutine — the probe loop
		// itself stays alive and keeps observing real state.
		//
		// Transitions are infrequent (only on actual state change, not on
		// every probe), so unbounded `go` is bounded in practice.
		for _, cb := range callbacks {
			go cb(newState, reason)
		}
	}
}

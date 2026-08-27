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
	"io"
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
// human-readable explanation suitable for logging or UI. Callbacks fire in
// transition order from a dedicated dispatcher (never the probe loop); they
// still MUST NOT block, because one callback delays later state delivery.
type ReachabilityCallback func(reachable bool, reason string)

type reachabilityTransition struct {
	reachable bool
	reason    string
	callbacks []ReachabilityCallback
}

// dialer is the interface segment of net.Dialer that we need. Pulled
// out so tests can inject a fake without touching real sockets.
type dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// DialContextFunc adapts a function to the reachability dialer contract. Link
// uses this to make probes traverse its userspace encrypted route instead of
// probing the loopback proxy listener (which is reachable even when the far
// side is not).
type DialContextFunc func(context.Context, string, string) (net.Conn, error)

func (f DialContextFunc) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return f(ctx, network, address)
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

	// livenessHook, if set, returns the elapsed time since the backend was last
	// proven reachable by REAL data-plane I/O over the same link the probe uses
	// (a completed drain / MinIO PUT). When a probe dial FAILS but the hook
	// reports a recent success (within livenessWindow), the failure is treated as
	// NOT-a-real-outage and the fail streak is left untouched — the cold SYN
	// merely queued behind the drainer's own bulk PUT traffic on a saturated
	// uplink (slow-link false-flap, NFSv3 sprint). A genuine outage stops the
	// drains within seconds, the window lapses, and the normal failure flip
	// proceeds. Kept as a plain func so this package stays free of an nfs import
	// (clean layering). Returns a sentinel (e.g. a very large duration) when no
	// drain has ever succeeded, which never satisfies the window. Called from the
	// probe-loop goroutine; MUST NOT block.
	livenessHook func() time.Duration

	// livenessOverride enables the drain-liveness override above. Default ON;
	// env kill-switch JM_REACH_DRAIN_LIVENESS=0 disables it so behavior is
	// byte-identical to the pre-fix monitor (WAN-tuning revert discipline —
	// docs/TUNING/REVERT_LOG.md). When disabled the hook is never consulted.
	livenessOverride bool

	// livenessWindow is the max age of a proven backend I/O that still suppresses
	// a probe failure. Sized to ~2*baseInterval so the override covers the brief
	// congestion spike that queues a cold SYN past its dial timeout, but lapses
	// fast on a real outage (the next probe after drains stop sees a stale hook
	// and flips). Derived once at construction from baseInterval.
	livenessWindow time.Duration

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
	// transitionCh keeps state notifications ordered without letting a slow
	// callback park the probe loop. The old "go cb" fan-out could deliver a
	// later online transition before an earlier offline transition, leaving UI
	// and auto-offline consumers in a stale state even though Reachable() was
	// correct. Transitions are rare; a modest buffer absorbs normal callback
	// latency, and overflow coalesces toward the newest truthful state.
	transitionCh chan reachabilityTransition
	stopCh       chan struct{}
	stopOnce     sync.Once
	running      atomic.Bool
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

// WithDialContext routes probes through fn. A successful result must still
// answer the Redis protocol-level PING performed by probe(), so a connected
// local listener alone can never be mistaken for backend readiness.
func WithDialContext(fn func(context.Context, string, string) (net.Conn, error)) ReachabilityOption {
	return func(r *Reachability) {
		if fn != nil {
			r.dialer = DialContextFunc(fn)
		}
	}
}

// WithRTTObserver registers a callback invoked with the dial latency of every
// SUCCESSFUL probe. Used to feed the network-profile link estimator. The
// callback runs on the probe-loop goroutine and MUST NOT block.
func WithRTTObserver(fn func(time.Duration)) ReachabilityOption {
	return func(r *Reachability) { r.rttObserver = fn }
}

// WithLivenessHook registers a hook returning the elapsed time since the
// backend was last proven reachable by real data-plane I/O (a completed drain /
// MinIO PUT) over the same physical link the probe uses. It is consulted ONLY
// on a probe-dial FAILURE: if the hook reports a success more recent than the
// liveness window (~2*baseInterval), the failure is suppressed (the fail streak
// is not advanced) because a successful drain is positive proof the backend is
// reachable — the cold SYN merely queued behind the drainer's own bulk PUT
// traffic on a saturated uplink. A genuine outage stops drains within seconds,
// the window lapses, and the normal 2-failure flip proceeds. The hook MUST
// return a large sentinel duration when no drain has ever succeeded so it never
// satisfies the window. nil hook → override inert (behavior identical to
// today). Disabled wholesale by JM_REACH_DRAIN_LIVENESS=0. Runs on the
// probe-loop goroutine and MUST NOT block.
func WithLivenessHook(fn func() time.Duration) ReachabilityOption {
	return func(r *Reachability) { r.livenessHook = fn }
}

// NewReachability constructs a monitor against the given "host:port"
// target. The monitor starts in the "presumed reachable" state — it
// won't flip to unreachable until consecutive probes fail. This
// prevents a spurious "offline" notification at startup when the
// first probe is racing application initialization.
func NewReachability(target string, opts ...ReachabilityOption) *Reachability {
	r := &Reachability{
		target:           target,
		dialTimeout:      1 * time.Second,
		baseInterval:     2 * time.Second,
		maxDialTimeout:   10 * time.Second,
		adaptive:         true,
		failsToOffline:   2,
		passesToOnline:   1,
		reachable:        true, // presumed reachable until proven otherwise
		livenessOverride: true, // drain-liveness false-flap suppression (kill: JM_REACH_DRAIN_LIVENESS=0)
		dialer:           &net.Dialer{},
		triggerCh:        make(chan struct{}, 1),
		transitionCh:     make(chan reachabilityTransition, 16),
		stopCh:           make(chan struct{}),
	}
	for _, opt := range opts {
		opt(r)
	}
	// livenessWindow is derived AFTER options so a non-default WithBaseInterval
	// scales it. ~2*baseInterval: long enough to cover a congestion spike that
	// queues a cold SYN past its dial timeout, short enough that a real outage
	// (drains stop) lapses it within a probe cycle or two.
	r.livenessWindow = 2 * r.baseInterval
	// Env overrides (operator escape hatch; defaults are LAN-safe).
	//   JM_REACH_ADAPTIVE=0         disable adaptive growth (fixed dialTimeout)
	//   JM_REACH_MAX_DIAL_MS=<n>    ceiling for the adaptive dial timeout
	//   JM_REACH_DRAIN_LIVENESS=0   disable the drain-liveness false-flap override
	//                               (byte-identical pre-fix behavior; REVERT_LOG)
	if os.Getenv("JM_REACH_ADAPTIVE") == "0" {
		r.adaptive = false
	}
	if v := os.Getenv("JM_REACH_MAX_DIAL_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms >= 1000 {
			r.maxDialTimeout = time.Duration(ms) * time.Millisecond
		}
	}
	if os.Getenv("JM_REACH_DRAIN_LIVENESS") == "0" {
		r.livenessOverride = false
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

// SeedUnreachable forces the monitor's initial state to "unreachable" BEFORE
// Start(), so the FIRST successful probe produces a real unreachable→reachable
// transition (firing OnChange(true)). R-4 (start-while-offline): when the app
// boots with the backend down and pre-engages auto-offline, recovery depends on
// that false→true transition to call SetAutoOffline(false). Without seeding, the
// monitor's default "presumed reachable" state means a backend that returns
// quickly never transitions, and the boot-engaged offline mode would never lift
// — leaving the mount stuck read-refused. Idempotent; must be called before
// Start() (it seeds the state the first probe will move off of).
func (r *Reachability) SeedUnreachable() {
	r.mu.Lock()
	r.reachable = false
	r.consecutivePass = 0
	r.consecutiveFails = r.failsToOffline // already "fully failed"; next success flips
	r.lastTransitionAt = time.Now()
	r.mu.Unlock()
}

// Start begins probing in a background goroutine. Safe to call once
// per instance; subsequent calls are no-ops.
func (r *Reachability) Start() {
	if !r.running.CompareAndSwap(false, true) {
		return
	}
	go r.dispatchTransitions()
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

// dispatchTransitions invokes callbacks serially in transition order. It is a
// separate goroutine so callbacks cannot delay probes. A callback that violates
// its non-blocking contract can still park only this dispatcher, never network
// detection itself; enqueue overflow is coalesced to the newest state below.
func (r *Reachability) dispatchTransitions() {
	for {
		select {
		case <-r.stopCh:
			return
		case evt := <-r.transitionCh:
			for _, cb := range evt.callbacks {
				cb(evt.reachable, evt.reason)
			}
		}
	}
}

func (r *Reachability) enqueueTransition(evt reachabilityTransition) {
	select {
	case r.transitionCh <- evt:
		return
	default:
	}

	// A callback has violated the non-blocking contract long enough to fill the
	// queue. Drop one stale transition and retain the newest state; consumers
	// may miss an intermediate flap but cannot be left permanently behind the
	// monitor's current truth.
	select {
	case <-r.transitionCh:
	default:
	}
	select {
	case r.transitionCh <- evt:
	default:
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
	defer conn.Close()
	rtt := time.Since(start)

	// A COMPLETED TCP HANDSHAKE IS NOT PROOF THE BACKEND IS THERE.
	//
	// This probe used to return true right here, and that produced the single
	// worst state this app can be in. Observed live on 2026-07-29 from an
	// iPhone hotspot, where the NAS (a 192.168.0.x LAN address) is genuinely
	// unroutable:
	//
	//	05:27:03  backend unreachable for sustained window — engaging offline mode
	//	08:43:15  network path lost … i/o timeout
	//	08:44:50  reachability transition → REACHABLE "probe succeeded"   <-- FALSE
	//	08:44:50  network path to backend recovered → SetAutoOffline(false)
	//
	// while a real Redis PING to that same address timed out and ping(8) showed
	// 100% packet loss. Carrier NAT, captive portals and other middleboxes
	// routinely answer the SYN for an address they cannot actually route to, so
	// the dial "succeeds" against the middlebox rather than the backend.
	//
	// Lifting auto-offline on that false signal leaves the app ONLINE WITH A
	// DEAD BACKEND — strictly worse than either honest state. Navigation still
	// feels fine because the mirror serves it (measured 25ms), but the FIRST
	// open of each file walks the full timeout stack before falling back to
	// locally cached blocks: measured 1,527ms / 6,250ms / 28,150ms for 32 KB,
	// against 24-36ms for every subsequent read of the same file. That is
	// exactly the reported symptom — "offline mode is amazing, but with offline
	// mode disabled performance is absolute trash and even my pinned files
	// struggle" — because the bytes ARE local and we pay the network anyway.
	//
	// So require a PROTOCOL-LEVEL answer. Any RESP reply proves a real Redis is
	// on the other end: "+PONG" normally, and "-NOAUTH ..." when the server
	// wants AUTH — an error reply is still proof of life, and this probe is a
	// liveness check, not an auth check. A middlebox that accepted the
	// connection will send nothing and hit the deadline.
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	if _, err := conn.Write([]byte("PING\r\n")); err != nil {
		return false
	}
	var buf [1]byte
	if _, err := io.ReadFull(conn, buf[:]); err != nil {
		return false
	}
	switch buf[0] {
	case '+', '-', ':', '$', '*': // any RESP type == a real server answered
	default:
		return false
	}

	// RTT is measured across dial+PING now, which is the more honest number:
	// it is what an actual metadata round trip costs, and it feeds the adaptive
	// dial timeout and the netprofile RTT estimate.
	rtt = time.Since(start)
	r.observeRTT(rtt)
	if r.rttObserver != nil {
		r.rttObserver(rtt)
	}
	return true
}

// drainLivenessSuppresses reports whether a probe FAILURE should be ignored
// because the backend was proven reachable by real data-plane I/O (a completed
// drain) within the liveness window. A successful drain rides the SAME physical
// link the probe dials, so it is direct evidence the link is up — the failed
// cold SYN merely queued behind the drainer's bulk PUT traffic on a saturated
// uplink (slow-link false-flap). Returns false when the override is disabled
// (JM_REACH_DRAIN_LIVENESS=0), no hook is wired, or the last proven I/O is
// older than the window (a real outage: drains stop landing → the override
// lapses → the normal failure flip proceeds). Called from the probe-loop
// goroutine OUTSIDE r.mu so the hook (which may touch the bridge's globalMu via
// the drainer) never nests under the reachability lock.
func (r *Reachability) drainLivenessSuppresses() bool {
	if !r.livenessOverride || r.livenessHook == nil {
		return false
	}
	since := r.livenessHook()
	return since >= 0 && since <= r.livenessWindow
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

	// Drain-liveness false-flap override (NFSv3 sprint). A probe-dial FAILURE
	// while a real drain landed within the liveness window is NOT a real outage
	// — a cold SYN queued behind the drainer's own bulk PUT traffic on the
	// saturated uplink. Suppress it entirely: do NOT advance (nor reset) the
	// fail streak, leave reachable/consecutivePass untouched, fire no callback.
	// Checked OUTSIDE r.mu (the hook may take the bridge's globalMu). A genuine
	// outage stops drains within seconds → the window lapses → the very next
	// failed probe falls through here and the normal 2-failure flip proceeds.
	if !ok && r.drainLivenessSuppresses() {
		jmlog.Info("reachability probe failed but drain proves backend live — suppressing false-flap",
			"target", r.target, "liveness_window", r.livenessWindow.String())
		return
	}

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
		// the same target succeed. The separate dispatcher contains a hung
		// callback away from the probe loop, which stays alive and keeps
		// observing real state.
		//
		// The dedicated dispatcher preserves transition order. The earlier
		// per-callback goroutines allowed recovery=true to run before the prior
		// offline=false callback, which could leave auto-offline re-engaged while
		// Reachable() already reported healthy.
		r.enqueueTransition(reachabilityTransition{
			reachable: newState,
			reason:    reason,
			callbacks: callbacks,
		})
	}
}

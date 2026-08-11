package metadata

import (
	"context"
	"io/fs"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// ============================================================================
// Redis keyspace-notification push (FOREIGN-write fast path).
//
// THE PROBLEM this solves: the authoritative full-tree Lua SCAN
// (syncMetadata) was running every 30s, taking 87-178s over cellular and
// finding zero changes ~93% of the time — saturating the link and killing
// navigation. THE FIX: subscribe to Redis keyspace notifications so only
// CHANGED directories are pushed, and incrementally reconcile just those
// dirs (one HGETALL each). The full SCAN demotes to a rare, class-gated
// backstop.
//
// This path is COMPLEMENTARY to:
//   - the self-write juicemount:metadata pub/sub (subscribeLoop/applyEvent),
//     which covers ONLY JuiceMount's own writes (nfs/handler.go publishEvent);
//   - the periodic full SCAN (reconcileLoop/syncMetadata), which remains the
//     sole source of truth and the convergence backstop.
//
// FAILURE MODEL: Redis keyspace notifications are fire-and-forget pub/sub —
// NO buffering, NO replay, NO ack. Any event published while we are
// disconnected, mid-reconnect, or not-yet-subscribed is permanently lost.
// Therefore push is an ACCELERATOR, never the sole source of truth.
// Convergence is guaranteed by three mandatory backstops:
//   (A) startup blocking SyncOnce() (cbridge.go) establishes the baseline;
//   (B) every (re)connect runs PSUBSCRIBE-then-full-SCAN gap-fill;
//   (C) a rare, class-gated periodic SCAN (backstopNanos) defends against
//       silently-dropped events, half-open TCP, and unmapped schema ops.
//
// Idempotency underpins all of it: reconcileDir and syncMetadata both
// upsert-by-path comparing mtime/size/inode before any write, so
// subscribe-before-SCAN double-processing and DEGRADED<->ENABLED flapping
// are harmless.
// ============================================================================

// keyspaceEngagement is the state of the keyspace-push subsystem.
type keyspaceEngagement int32

const (
	keyspaceDisabled keyspaceEngagement = iota // not engaged: classic 30s SCAN
	keyspaceEnabled                            // push carrying deltas, backstop long
	keyspaceDegraded                           // was enabled, subscription dropped; 30s SCAN until back
)

func (e keyspaceEngagement) String() string {
	switch e {
	case keyspaceEnabled:
		return "ENABLED"
	case keyspaceDegraded:
		return "DEGRADED"
	default:
		return "DISABLED"
	}
}

// keyspacePushEnabled reports whether the keyspace-notification push is
// enabled via the env kill switch. Default OFF until validated against a
// NAS with notify-keyspace-events configured. JM_METADATA_KEYSPACE_PUSH=1
// engages; anything else (including unset) keeps the proven 30s-SCAN path.
func (rc *RedisClient) keyspacePushEnabled() bool {
	return os.Getenv("JM_METADATA_KEYSPACE_PUSH") == "1"
}

// ----------------------------------------------------------------------------
// Class-gating signal hooks.
//
// metadata must not import health/ (and bridge holds the NetWatcher +
// Reachability instances), so the class signals are injected as function
// hooks set once at startup from bridge — mirroring the existing lstatFnPtr
// injection idiom in redis.go. When unset, class-gating degrades to a safe
// default band derived from JM_WAN_MODE alone.
// ----------------------------------------------------------------------------

var (
	keyspaceSignalMu sync.RWMutex
	// activeIfaceFn returns the current active interface NAME (e.g. "en21",
	// "en0", "utun4"). Set from bridge to NetWatcher.ActiveInterface.
	activeIfaceFn func() string
	// reachableFn reports backend reachability. Set from bridge to
	// globalReach.Reachable.
	reachableFn func() bool
)

// SetClassSignals wires the link-class signals used to gate the rare-backstop
// cadence and the coalescer's debounce/burst behavior. Called once at startup
// from bridge after NetWatcher + Reachability exist. Either argument may be
// nil; a nil signal simply isn't consulted (the gating falls back to its
// JM_WAN_MODE-only default).
func SetClassSignals(activeIface func() string, reachable func() bool) {
	keyspaceSignalMu.Lock()
	activeIfaceFn = activeIface
	reachableFn = reachable
	keyspaceSignalMu.Unlock()
}

// linkClass is a coarse band derived from the active interface name, matching
// the SAME rules NetWatcher.detectActiveInterface uses (health/netwatch.go).
type linkClass int

const (
	classLAN    linkClass = iota // ethernet (en* != en0, eth*) — fastest
	classWiFi                    // en0 on macOS
	classTunnel                  // utun*/tailscale0, OR JM_WAN_MODE=1 (cellular/WAN)
)

func (c linkClass) String() string {
	switch c {
	case classLAN:
		return "lan"
	case classWiFi:
		return "wifi"
	default:
		return "tunnel"
	}
}

// currentLinkClass derives the link class from the active interface name band
// (same rules as health/netwatch.go detectActiveInterface) plus the coarse
// JM_WAN_MODE override. JM_WAN_MODE=1 always forces the tunnel/cellular band
// (the loosest, link-sparing) regardless of interface — it is the explicit
// "treat this as metered WAN" flag.
func currentLinkClass() linkClass {
	if os.Getenv("JM_WAN_MODE") == "1" {
		return classTunnel
	}
	keyspaceSignalMu.RLock()
	fn := activeIfaceFn
	keyspaceSignalMu.RUnlock()
	if fn == nil {
		// No interface signal wired — default to WiFi: snappier than tunnel
		// but not as aggressive as assuming wired LAN.
		return classWiFi
	}
	name := fn()
	switch {
	case (strings.HasPrefix(name, "en") && name != "en0") || strings.HasPrefix(name, "eth"):
		return classLAN
	case name == "en0":
		return classWiFi
	case strings.HasPrefix(name, "utun") || name == "tailscale0":
		return classTunnel
	default:
		// Unknown band — be conservative (treat like WiFi, not LAN).
		return classWiFi
	}
}

// reachableNow reports backend reachability via the injected hook. If no hook
// is wired it optimistically returns true (the keyspace read loop's own
// read-error detection still drives reconnect).
func reachableNow() bool {
	keyspaceSignalMu.RLock()
	fn := reachableFn
	keyspaceSignalMu.RUnlock()
	if fn == nil {
		return true
	}
	return fn()
}

// backstopForClass returns the long, class-gated backstop interval used when
// the subscription is ENABLED+healthy. The motivation is that the cellular
// 87-178s SCAN must become RARE — but NOT so rare that the two backstop-bounded
// staleness windows (foreign in-place size/mtime edits, which fire no d* event;
// and a delete missed during a subscriber-down window, which heals only via the
// PruneThreshold ladder) stretch to hours. So tunnel/cellular is CAPPED at 5 min:
// still a 6x+ reduction from the old 30s cadence (the link-saturation fix holds),
// while bounding attr drift to <=5 min and a missed-delete ghost to
// PruneThreshold x 5 min instead of x 45 min. See QA residual risks / REVERT_LOG.
// reconcileBackstopOverride forces the demoted periodic-SCAN backstop to a
// caller-chosen interval for ALL link classes when JM_RECONCILE_BACKSTOP_SEC is a
// positive integer. Field-tuning kill switch per the cellular-revert-safety
// doctrine (env-overridable + logged; record changes in docs/TUNING/REVERT_LOG.md).
// 0 / unset keeps the class-gated defaults below.
func reconcileBackstopOverride() (time.Duration, bool) {
	if v := os.Getenv("JM_RECONCILE_BACKSTOP_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second, true
		}
	}
	return 0, false
}

// backstopForClass returns the DEMOTED periodic full-SCAN cadence used while
// keyspace-push is engaged. The push (real-time per-dir d-key reconcile) is the
// live path; this SCAN is only a missed-event safety net, so it is a long
// interval. SAFETY: if push DROPS, keyspaceLoop calls setEngagement(degraded)
// which resets the cadence to DefaultReconcileInterval (30s) until push
// re-engages — so a long value here only ever applies WHILE PUSH IS HEALTHY,
// and a real outage still converges fast on reconnect.
//
// Lengthened 2026-06-30 (from 10/15/5 min) after field testing: the SCAN
// "rebuilding the index every ~5 min" over WAN was too eager and churned the
// 438MB mirror (177MB WAL) — visible to the user as periodic sluggishness. With
// push proven engaged on the live NAS (subscribed + per-dir reconcile), the SCAN
// can safely be rare. Tunable live via JM_RECONCILE_BACKSTOP_SEC.
func backstopForClass(c linkClass) time.Duration {
	if d, ok := reconcileBackstopOverride(); ok {
		return d
	}
	switch c {
	case classLAN:
		return 15 * time.Minute
	case classWiFi:
		return 20 * time.Minute
	default: // tunnel / cellular / WAN — the SCAN is MOST expensive here (377k
		// rows over the tunnel), so a long backstop helps most; a missed push
		// event for a dir is re-covered by that dir's next d-key event, and a true
		// push drop snaps the cadence back to 30s (setEngagement above).
		return 15 * time.Minute
	}
}

// unknownAncestorSyncMin is the global floor between full-SCAN promotions
// from the unknown-ancestor path (reconcileDir). Env override
// JM_UNKNOWN_ANCESTOR_SYNC_SEC. See noteUnknownAncestor.
// unknownAncestorScanOnWAN restores the pre-fix behavior: promote a full SCAN
// for an unknown ancestor even on a high-latency link. Off by default.
func unknownAncestorScanOnWAN() bool {
	return os.Getenv("JM_UNKNOWN_ANCESTOR_SCAN_ON_WAN") == "1"
}

func unknownAncestorSyncMin() time.Duration {
	if raw := os.Getenv("JM_UNKNOWN_ANCESTOR_SYNC_SEC"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 10 * time.Minute
}

// unknownAncestorSeenCap bounds the seen-inode set. On overflow the set
// resets wholesale — worst case each accumulated inode earns ONE more
// (rate-limited) promotion round, then re-suppresses.
const unknownAncestorSeenCap = 65536

// noteUnknownAncestor handles a dir event whose inode the mirror doesn't
// hold. First sighting of an inode → promote to a full SCAN, but never more
// than one promotion per unknownAncestorSyncMin globally. Repeat sightings
// (the permanent steady state for scan-filtered farm namespaces, which the
// promoted SCAN can never establish) are dropped with a counter that logs
// every 1000 drops.
func (rc *RedisClient) noteUnknownAncestor(dirInode uint64) {
	rc.unknownAncestorMu.Lock()
	if rc.unknownAncestorSeen == nil {
		rc.unknownAncestorSeen = make(map[uint64]struct{})
	}
	if _, seen := rc.unknownAncestorSeen[dirInode]; seen {
		rc.unknownAncestorDrops++
		drops := rc.unknownAncestorDrops
		rc.unknownAncestorMu.Unlock()
		if drops%1000 == 1 {
			jmlog.Info("metadata keyspace push: unknown-ancestor events suppressed (scan-filtered namespace churn — farm output)",
				"dropped_total", drops)
		}
		return
	}
	if len(rc.unknownAncestorSeen) >= unknownAncestorSeenCap {
		rc.unknownAncestorSeen = make(map[uint64]struct{})
	}
	rc.unknownAncestorSeen[dirInode] = struct{}{}
	now := time.Now()
	allowed := now.Sub(rc.unknownAncestorLastSync) >= unknownAncestorSyncMin()
	if allowed {
		rc.unknownAncestorLastSync = now
	}
	rc.unknownAncestorMu.Unlock()

	// HIGH-LATENCY LINKS NEVER PAY FOR A PROMOTED SCAN.
	//
	// The once-per-inode dedup above bounds REPEATS, not ARRIVALS. The farm
	// mints a new derivative directory per asset under .juicemount/…, a
	// namespace the mirror deliberately never holds (#78), so each one is a
	// brand-new unknown inode and earns its own promotion. Observed on a real
	// cellular link 2026-07-29: NINE promotions in twelve hours, every one a
	// different inode — a full-tree SCAN over a phone tunnel, roughly hourly,
	// which is precisely the "index rebuilding for seemingly no reason" the
	// user reports. The 10-minute rate limit permits up to six an hour.
	//
	// A full SCAN is the most expensive thing this client can do and, for the
	// filtered namespace that triggers it, it CANNOT establish the ancestor —
	// the comment at the call site says so. On a far link that is pure cost.
	// Defer to the class backstop, which already guarantees eventual
	// convergence for anything deferred here (see keyspaceBackstop). On a
	// LAN, promotion is cheap and behavior is unchanged.
	//
	// Gated on HighLatency() rather than Class() deliberately: what makes a
	// SCAN ruinous is round trips, and a link can be metered-but-near or
	// fast-but-far. Kill switch JM_UNKNOWN_ANCESTOR_SCAN_ON_WAN=1 restores
	// unconditional promotion.
	if allowed && netprofile.Default().HighLatency() && !unknownAncestorScanOnWAN() {
		jmlog.Info("metadata keyspace push: unknown ancestor — SKIPPING promoted SCAN on a high-latency link (backstop will converge)",
			"inode", dirInode)
		return
	}

	if allowed {
		jmlog.Info("metadata keyspace push: unknown ancestor — promoting ONE full SCAN (rate-limited)",
			"inode", dirInode)
		rc.keyspaceTriggerSync()
	} else {
		jmlog.Debug("metadata keyspace push: unknown ancestor deferred to backstop (promotion rate-limited)",
			"inode", dirInode)
	}
}

// scanContextTimeout returns the wall-clock budget for ONE full-SCAN reconcile
// attempt (syncMetadata's context deadline around the SCAN batch loop).
//
// G7 (task #80): the budget was a fixed 120s. On a degraded cellular relay the
// full SCAN needs ~200-300s, so EVERY attempt died with "redis SCAN batch:
// context deadline exceeded" (live 2026-07-01: 31 consecutive failures over
// 3h), each one saturating the metered link for the full 120s first. Class
// gate, same accessor as the backstop/coalescer/G6 gating:
//
//   - LAN / WiFi        → 120s (byte-identical to the historical fixed budget)
//   - tunnel / cellular → 300s (utun*/tailscale0/JM_WAN_MODE=1 — enough for the
//     observed ~200-300s slow-link SCAN to actually finish)
//
// Env override JM_SCAN_TIMEOUT_SEC: a positive integer forces that budget (in
// seconds) for ALL classes — field-tuning kill switch per the cellular-revert
// doctrine (docs/TUNING/REVERT_LOG.md). 0 / unset / garbage keeps the class
// logic. Read per call so it can be flipped live.
func scanContextTimeout() time.Duration {
	if v := os.Getenv("JM_SCAN_TIMEOUT_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	if currentLinkClass() == classTunnel {
		return 300 * time.Second
	}
	return 120 * time.Second
}

// coalescerTuning bundles the class-gated debounce/burst parameters.
type coalescerTuning struct {
	debounce     time.Duration // quiet-period before flushing a batch
	maxWait      time.Duration // hard ceiling from first event in a batch to flush
	burstCeiling int           // distinct dirs above which we promote to one full SCAN
}

// tuningForClass returns the coalescer parameters for a link class: tighter on
// LAN (snappier UX), looser on tunnel/cellular (fewer, larger batches to spare
// the metered link).
func tuningForClass(c linkClass) coalescerTuning {
	switch c {
	case classLAN:
		return coalescerTuning{debounce: 200 * time.Millisecond, maxWait: 2 * time.Second, burstCeiling: 200}
	case classWiFi:
		return coalescerTuning{debounce: 400 * time.Millisecond, maxWait: 3 * time.Second, burstCeiling: 200}
	default: // tunnel / cellular
		return coalescerTuning{debounce: 1500 * time.Millisecond, maxWait: 5 * time.Second, burstCeiling: 200}
	}
}

// ----------------------------------------------------------------------------
// Engagement state + connected-state freshness.
// ----------------------------------------------------------------------------

// setEngagement records the engagement state and adjusts the backstop cadence.
// ENABLED + backend reachable -> long class-gated interval; anything else ->
// the config cadence / DefaultReconcileInterval (30s) so the periodic SCAN
// resumes as the authoritative fallback. We never go long while the backend is
// known unreachable: an unreachable backend means push delivery has stopped, so
// the SCAN must stay frequent enough to converge fast on reconnect.
//
// The actual backstop value is computed by resolveBackstop, the SINGLE
// precedence resolver shared with SetReconcileInterval (#90). setEngagement's
// only extra job is to RECORD the new engagement state (rc.engaged) so a later
// config change can re-derive with the same precedence.
func (rc *RedisClient) setEngagement(e keyspaceEngagement) {
	rc.engaged.Store(int32(e))
	rc.resolveBackstop("engagement=" + e.String())
}

// resolveBackstop computes the live periodic-SCAN backstop with the #90
// precedence and stores it into backstopNanos, logging every ACTUAL change
// (old->new + reason) so a future 900->300 collapse can never hide.
//
// Precedence (highest first):
//  1. JM_RECONCILE_BACKSTOP_SEC env override — folded INTO backstopForClass via
//     reconcileBackstopOverride, so it wins for every class in the ENABLED path.
//     (In the non-ENABLED path we also honor it explicitly, so the field-tuning
//     kill switch wins regardless of engagement.)
//  2. push ENABLED + backend reachable -> max(classBackstop, configReconcile):
//     the config seed can only LENGTHEN the push-mode backstop, never shorten it
//     below the class-gated floor. This is the crux of #90 — the 300s config seed
//     used to clobber the 900s class value straight into backstopNanos.
//  3. otherwise (DISABLED / DEGRADED / ENABLED-but-unreachable) -> the config
//     cadence drives the SCAN directly (config, else DefaultReconcileInterval).
//     This is the non-push authoritative fallback, so the LB-4 preference still
//     applies when push isn't carrying deltas.
//
// reason is a short tag (e.g. "config", "engagement=ENABLED") threaded into the
// change log so the driver of each transition is auditable.
func (rc *RedisClient) resolveBackstop(reason string) {
	e := keyspaceEngagement(rc.engaged.Load())
	cfg := time.Duration(rc.configReconcileNanos.Load())

	var next time.Duration
	if e == keyspaceEnabled && reachableNow() {
		// Precedence 1+2. backstopForClass already short-circuits to the
		// JM_RECONCILE_BACKSTOP_SEC override when set, so that override wins here.
		classBackstop := backstopForClass(currentLinkClass())
		next = classBackstop
		if cfg > next {
			// The config seed can only LENGTHEN the push-mode backstop.
			next = cfg
		}
	} else {
		// Precedence 1+3. The env override (highest precedence) still wins even
		// off the push path; otherwise the config cadence drives the SCAN, else
		// the 30s default.
		if d, ok := reconcileBackstopOverride(); ok {
			next = d
		} else if cfg > 0 {
			next = cfg
		} else {
			next = DefaultReconcileInterval
		}
	}

	old := time.Duration(rc.backstopNanos.Swap(int64(next)))
	if old != next {
		jmlog.Info("metadata reconcile backstop changed",
			"old_sec", old.Seconds(), "new_sec", next.Seconds(),
			"reason", reason, "engaged", e.String(),
			"class", currentLinkClass().String(),
			"config_sec", cfg.Seconds())
	}
}

// markKeyspaceConnected/Disconnected keep rc.connected/lastReconnect/
// lastDisconnect fresh from the keyspace loop's own transitions. With the
// full SCAN now rare, doReconcile (the only other writer of these fields)
// runs infrequently, so RecentlyDegraded() — which gates the prune
// skipIncrement and the nfs phantom-purge — would otherwise read stale data.
// These mirror doReconcile's bookkeeping exactly, under rc.mu.
func (rc *RedisClient) markKeyspaceConnected() {
	rc.mu.Lock()
	if !rc.connected {
		rc.connected = true
		rc.lastReconnect = time.Now()
	}
	rc.mu.Unlock()
}

func (rc *RedisClient) markKeyspaceDisconnected() {
	rc.mu.Lock()
	if rc.connected {
		rc.connected = false
		rc.lastDisconnect = time.Now()
	}
	rc.mu.Unlock()
}

// ----------------------------------------------------------------------------
// Availability detection: does this Redis publish the notifications we need?
// ----------------------------------------------------------------------------

// keyspaceNotifySufficient reports whether a notify-keyspace-events flags
// string is sufficient for the d-key push to function. We require the keyspace
// channel family (K) AND coverage of hash + generic verbs — either via 'A'
// (all classes) or explicitly both 'g' (generic: DEL/RENAME) and 'h' (hash:
// HSET/HDEL).
func keyspaceNotifySufficient(flags string) bool {
	return strings.Contains(flags, "K") &&
		(strings.Contains(flags, "A") ||
			(strings.Contains(flags, "g") && strings.Contains(flags, "h")))
}

// keyspaceNotifyWant is the notify-keyspace-events value the self-heal path
// (JM_KEYSPACE_AUTOCONFIG) writes when the NAS has it unset/insufficient. "KEA"
// = Keyspace + Keyevent + All-classes — the canonical "enable everything" that
// keyspaceNotifySufficient accepts; the extra classes cost negligible publish
// overhead on a dedicated metadata Redis and we only PSUBSCRIBE :d*.
const keyspaceNotifyWant = "KEA"

// probeKeyspaceConfig runs CONFIG GET notify-keyspace-events and reports
// whether the live config is sufficient, along with the raw flags string for
// logging. On any error it returns (false, "") — fail safe to DISABLED.
func (rc *RedisClient) probeKeyspaceConfig(ctx context.Context) (sufficient bool, flags string) {
	vals, err := rc.redisDB().ConfigGet(ctx, "notify-keyspace-events").Result()
	if err != nil {
		return false, ""
	}
	flags = vals["notify-keyspace-events"]
	return keyspaceNotifySufficient(flags), flags
}

// ----------------------------------------------------------------------------
// keyspaceLoop: the subscriber goroutine.
// ----------------------------------------------------------------------------

// keyspaceLoop owns the PSUBSCRIBE on __keyspace@<db>__:d* and the inode
// coalescer. It mirrors subscribeLoop's retry idiom: run runKeyspaceSubscribe,
// and on return (subscription lost) reset the backstop to 30s, sleep 2s, retry.
func (rc *RedisClient) keyspaceLoop() {
	jmlog.Info("metadata keyspace push: loop starting (JM_METADATA_KEYSPACE_PUSH=1)")
	for {
		select {
		case <-rc.stopCh:
			return
		default:
		}

		established := rc.runKeyspaceSubscribe()

		// Reset the backstop to 30s so the periodic SCAN resumes as the fallback
		// BEFORE the retry sleep (regardless of how this iteration ended).
		rc.setEngagement(keyspaceDegraded)

		// Only mark the connection DEGRADED (which makes RecentlyDegraded report
		// true, gating the QA-30 prune skipIncrement and the NFS phantom-purge)
		// when a subscription was ACTUALLY established this iteration and then
		// dropped. If we never subscribed — notify-keyspace-events insufficient
		// (the live NAS today with the kill switch on), URL parse error, or
		// PSUBSCRIBE failure — Redis itself is still healthy and reachable, so
		// flipping connected=false here would wrongly suppress legitimate
		// prune/purge work and fight the reconcileLoop (which sets
		// connected=true). See EDGE Bug B.
		if established {
			rc.markKeyspaceDisconnected()
		}

		select {
		case <-rc.stopCh:
			return
		case <-time.After(2 * time.Second):
			jmlog.Info("metadata keyspace push: reconnecting")
		}
	}
}

// runKeyspaceSubscribe establishes the PSUBSCRIBE, runs the gap-fill SCAN, and
// pumps events into the coalescer until the subscription drops or stop fires.
// It returns true iff a subscription was ACTUALLY established (PSUBSCRIBE
// confirmed) this iteration — keyspaceLoop uses that to decide whether to mark
// the connection DEGRADED on return (EDGE Bug B: never flip connected=false on
// a healthy-but-unconfigured NAS where we never subscribed).
//
// CRITICAL ORDERING (subscribe-before-SCAN): establish the PSUBSCRIBE FIRST,
// confirm it active, START draining its channel into the coalescer, THEN run
// the full gap-fill SCAN. Events landing during the SCAN are coalesced and
// double-processed (idempotent upsert — harmless) but NONE are lost. We do NOT
// flip to ENABLED (and the backstop does NOT go long) until the gap-fill SCAN
// has actually COMPLETED successfully — otherwise push would be trusted, and
// the 30s SCAN demoted to the rare backstop, before the gap-fill that closes
// the "events lost while disconnected" hole has run (EDGE Bug C). This fires on
// initial connect AND every reconnect — a reconnect is the only window the
// subscriber was ever absent.
func (rc *RedisClient) runKeyspaceSubscribe() (established bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Monitor stopCh to cancel the subscription context.
	go func() {
		select {
		case <-rc.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	// Derive the db from the URL — NEVER hardcode @0. Subscribing to the
	// wrong db silently receives ZERO events while appearing healthy.
	_, db, err := ParseRedisURL(rc.redisURL)
	if err != nil {
		jmlog.Warn("metadata keyspace push: cannot parse redis URL, disabling", "error", err.Error())
		return false
	}

	// Re-probe CONFIG on every (re)connect — the NAS config can change under
	// an app update. Insufficient -> DISABLED: do not subscribe, let the 30s
	// SCAN run. Returning here exits runKeyspaceSubscribe; keyspaceLoop will
	// retry in 2s and re-probe (cheap), so a later NAS enablement is picked up.
	sufficient, flags := rc.probeKeyspaceConfig(ctx)
	if !sufficient {
		// SELF-HEAL (2026-07-10): the perpetual 30s SCAN over a slow/cellular
		// link — root cause of "index rebuild takes 10 minutes" — is caused by
		// the NAS Redis simply not having notify-keyspace-events enabled (its
		// default is off, and a Redis started without a config file loses any
		// manual CONFIG SET on restart). Rather than passively fall back to
		// SCAN forever, ENABLE the feature we need: CONFIG SET the flags
		// ourselves (best-effort) and re-probe. This is our own dedicated
		// metadata Redis, the flag only affects our own d-key subscription, and
		// it makes push work on any fresh NAS with zero manual setup + survive a
		// Redis restart (we re-set on every reconnect). Kill switch
		// JM_KEYSPACE_AUTOCONFIG=0 restores the passive-fallback behavior; a SET
		// that fails (ACL/read-only) just falls through to the 30s SCAN as before.
		if os.Getenv("JM_KEYSPACE_AUTOCONFIG") != "0" {
			if err := rc.redisDB().ConfigSet(ctx, "notify-keyspace-events", keyspaceNotifyWant).Err(); err != nil {
				jmlog.Info("metadata keyspace push: auto-enable CONFIG SET failed (staying on SCAN)",
					"error", err.Error(), "want", keyspaceNotifyWant)
			} else {
				sufficient, flags = rc.probeKeyspaceConfig(ctx)
				if sufficient {
					jmlog.Info("metadata keyspace push: auto-enabled notify-keyspace-events (self-heal)",
						"flags", flags, "db", db)
				}
			}
		}
	}
	if !sufficient {
		rc.setEngagement(keyspaceDisabled)
		jmlog.Info("metadata keyspace push: notify-keyspace-events insufficient, staying on 30s SCAN",
			"flags", flags, "db", db)
		// Sleep here (interruptible) so we don't hot-loop CONFIG GET every 2s
		// forever on an un-reconfigured NAS.
		select {
		case <-rc.stopCh:
		case <-time.After(60 * time.Second):
		}
		return false
	}

	pattern := "__keyspace@" + strconv.Itoa(db) + "__:d*"
	prefix := "__keyspace@" + strconv.Itoa(db) + "__:" // strip to get the bare key (e.g. "d732093")

	sub := rc.redisDB().PSubscribe(ctx, pattern)
	defer sub.Close()

	// Confirm the subscription is actually active before the gap-fill SCAN.
	// Receive() blocks for the subscription confirmation; a failure here means
	// PSUBSCRIBE never took effect, so we must NOT mark ENABLED or report
	// established.
	if _, err := sub.Receive(ctx); err != nil {
		jmlog.Warn("metadata keyspace push: PSUBSCRIBE failed", "error", err.Error(), "pattern", pattern)
		return false
	}

	rc.markKeyspaceConnected()
	jmlog.Info("metadata keyspace push: subscribed", "pattern", pattern, "flags", flags, "class", currentLinkClass().String())

	// Start draining the subscription channel into the coalescer IMMEDIATELY —
	// before the gap-fill SCAN — so events arriving during the (cellular: long)
	// SCAN are coalesced rather than backing up in the connection buffer, and so
	// none are lost. The coalescer's reconcileDir is idempotent w.r.t. the SCAN.
	co := newInodeCoalescer(rc)
	defer co.stop()

	// Wire reconcileDir's new-subtree discovery back into THIS coalescer (the
	// B4' burst-ordering fix): a newly-discovered child dir is requeued so a
	// burst-created tree mirrors completely, not just its top level. Cleared
	// before co.stop() runs (defers are LIFO) so SCAN/pinwarm reconciles never
	// recurse through a dead coalescer; co.add's stopped-guard double-covers.
	requeue := requeueFunc(co.add)
	rc.keyspaceRequeue.Store(&requeue)
	defer rc.keyspaceRequeue.Store(nil)

	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		ch := sub.Channel()
		for {
			select {
			case <-rc.stopCh:
				return
			case msg, ok := <-ch:
				if !ok {
					// Channel closed -> subscription ended.
					return
				}
				inode, ok := parseDirInodeFromChannel(msg.Channel, prefix)
				if !ok {
					continue
				}
				noteKeyspacePushDelivered()
				co.add(inode)
			}
		}
	}()

	// [#6/C1] Push-liveness heartbeat: while subscribed, persist a wall-clock
	// "push was alive" stamp every minute (one tiny SQLite meta upsert). On
	// the NEXT boot, a recent stamp bounds our downtime — the basis for
	// skipping the boot gap-fill SCAN below. Stamped immediately so even a
	// short-lived subscription records liveness.
	heartbeatStop := make(chan struct{})
	defer close(heartbeatStop)
	stampAlive := func() {
		if err := rc.store.SetMeta(metaKeyPushLastAlive, strconv.FormatInt(time.Now().Unix(), 10)); err != nil {
			jmlog.Debug("keyspace push: liveness stamp failed", "error", err.Error())
		}
	}
	// ORDER MATTERS: the boot gap-fill freshness decision must read the
	// PREVIOUS process's heartbeat — so it is evaluated HERE, before the
	// first stampAlive() below. (v1 stamped first and then read its own
	// fresh stamp: downtime always ~0s, the skip always fired, and a
	// weeks-stale mirror would have skipped its baseline SCAN.)
	skipBootGapFill := rc.shouldSkipBootGapFill()
	stampAlive()
	go func() {
		t := time.NewTicker(pushHeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-heartbeatStop:
				return
			case <-rc.stopCh:
				return
			case <-t.C:
				stampAlive()
			}
		}
	}()

	// Gap-fill: one immediate authoritative full SCAN, run SYNCHRONOUSLY now
	// that the subscriber is established and draining. Covers any change that
	// happened while we were absent. We only flip to ENABLED (long backstop,
	// trust push deltas) AFTER it succeeds — on error we stay DEGRADED/30s and
	// let keyspaceLoop retry, so push is never trusted before its baseline
	// exists (EDGE Bug C). The SCAN is single-flighted internally, so a
	// concurrent periodic doReconcile cannot double-run it.
	//
	// [#6/C1 skip-SCAN-when-fresh] EXCEPTION: on the FIRST subscribe of this
	// process, if the previous process's push heartbeat is recent (downtime <
	// the freshness window), the gap the SCAN would fill is bounded and tiny —
	// and on a cellular/tunnel link that SCAN costs 160s+ of boot latency plus
	// real data (measured live: 297k entries, 163s, every boot). Skip it, flip
	// to ENABLED on the fresh mirror, and let the push + the periodic backstop
	// (which still runs on its normal cadence) cover the bounded gap. A STALE
	// stamp, a disabled kill-switch (JM_BOOT_SCAN_FRESH_SKIP=0), or any later
	// re-subscribe runs the full gap-fill exactly as before.
	if skipBootGapFill {
		rc.setEngagement(keyspaceEnabled)
	} else if rc.deferBootGapFillToBackground() {
		// SLOW LINK: recall the last state NOW, rebuild in the background.
		//
		// The freshness skip above is a 15-MINUTE window. A laptop that sleeps
		// overnight blows it every single morning, and then pays a synchronous
		// full-tree SCAN over whatever link it woke up on — measured live at
		// 297k entries / 163s on a tunnel, which is the user-visible
		// "Rebuilding index" and a metered-link burn for a mirror that is, in
		// practice, almost entirely still correct.
		//
		// The window is the wrong instrument on a far link. What makes the SCAN
		// affordable is RTT, not recency: on a LAN it is seconds and stays
		// synchronous exactly as before; on a tunnel it is minutes no matter
		// how fresh the mirror is.
		//
		// So: mark ENABLED against the RECALLED mirror and run the same
		// authoritative SCAN on a background goroutine. Nothing is skipped —
		// convergence is deferred, not abandoned, and the periodic backstop is
		// still there behind it.
		//
		// SAFE because the mirror is already the serving authority and is
		// designed to be imperfect: an entry deleted out-of-band is caught by
		// the phantom-purge confirmation, an unmirrored directory populates via
		// the async dir refresh (U7), and push carries live deltas the moment
		// it engages. The exposure is a bounded window where a change made
		// elsewhere DURING DOWNTIME is not yet visible — the same staleness
		// class already accepted for throttled links, and strictly better than
		// today's alternative of showing nothing while the SCAN runs.
		//
		// Kill switch JM_BOOT_SCAN_BACKGROUND=0 restores the synchronous SCAN.
		jmlog.Info("metadata keyspace push: boot gap-fill SCAN DEFERRED to background (slow link)",
			"note", "serving the recalled mirror now; SCAN converges behind it")
		rc.setEngagement(keyspaceEnabled)
		go func() {
			if err := rc.SyncOnce(); err != nil {
				jmlog.Warn("metadata keyspace push: background gap-fill SCAN failed — backstop will retry",
					"error", err.Error())
				return
			}
			jmlog.Info("metadata keyspace push: background gap-fill SCAN complete")
		}()
	} else {
		if err := rc.SyncOnce(); err != nil {
			jmlog.Warn("metadata keyspace push: gap-fill SCAN failed, staying DEGRADED",
				"error", err.Error())
			// Return; defers stop the coalescer and close the subscription. The
			// subscription WAS established, so keyspaceLoop should mark DEGRADED.
			return true
		}
		// Gap-fill complete — NOW ENABLED. Push carries deltas; the periodic
		// SCAN demotes to the rare class-gated backstop.
		rc.setEngagement(keyspaceEnabled)
	}

	// Block until the pump exits (subscription dropped or stop fired).
	select {
	case <-rc.stopCh:
	case <-pumpDone:
	}
	return true
}

// pushHeartbeatInterval is how often the live subscription persists its
// "push alive" stamp (see stampAlive above). One SQLite meta upsert per tick.
const pushHeartbeatInterval = 60 * time.Second

// bootGapFillEvaluated makes the fresh-skip a strictly boot-time (first
// subscribe per process) decision: reconnect gap-fills mid-run always SCAN.
// Guarded by the subscribe loop's serial execution; atomic for safety.
var bootGapFillEvaluated atomic.Bool

// shouldSkipBootGapFill reports whether the boot gap-fill SCAN can be safely
// skipped: first subscribe of the process AND the persisted push heartbeat is
// younger than the freshness window AND the kill-switch is not set. Logs its
// decision either way (the skip saves 160s+ and real data on cellular boots,
// so the operator should always see which path ran).
func (rc *RedisClient) shouldSkipBootGapFill() bool {
	if bootGapFillEvaluated.Swap(true) {
		return false // not the boot subscribe — reconnects always gap-fill
	}
	if os.Getenv("JM_BOOT_SCAN_FRESH_SKIP") == "0" {
		return false
	}
	window := bootScanFreshWindow
	if v := os.Getenv("JM_BOOT_SCAN_FRESH_WINDOW_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			window = time.Duration(n) * time.Second
		}
	}
	// Freshness evidence, either signal suffices: (a) the push heartbeat —
	// bounds true downtime; (b) the existing C1 ShouldSkipBootSync (recent
	// full SCAN + push engaged) — covers a first boot after a clean SCAN
	// before any heartbeat existed.
	downtime := time.Duration(-1)
	if raw, ok, err := rc.store.GetMeta(metaKeyPushLastAlive); err == nil && ok {
		if sec, perr := strconv.ParseInt(raw, 10, 64); perr == nil {
			if d := time.Since(time.Unix(sec, 0)); d >= 0 {
				downtime = d
			}
		}
	}
	if downtime >= 0 && downtime <= window {
		jmlog.Info("metadata keyspace push: boot gap-fill SCAN SKIPPED (mirror fresh)",
			"downtime", downtime.Round(time.Second).String(), "window", window.String(),
			"note", "push+backstop cover the bounded gap; JM_BOOT_SCAN_FRESH_SKIP=0 disables")
		return true
	}
	if rc.ShouldSkipBootSync() {
		jmlog.Info("metadata keyspace push: boot gap-fill SCAN SKIPPED (recent full SCAN, C1)",
			"note", "JM_BOOT_SCAN_FRESH_SKIP=0 disables")
		return true
	}
	jmlog.Info("metadata keyspace push: boot gap-fill SCAN required (mirror not fresh)",
		"heartbeat_downtime", downtime.Round(time.Second).String(), "window", window.String())
	return false
}

// bootScanFreshWindow is the default max downtime for which the boot gap-fill
// SCAN is skipped. Chosen ≥ several heartbeat intervals and ≥ a typical
// deploy/relaunch cycle, but well under the shortest interval in which large
// out-of-band changes typically accumulate.
const bootScanFreshWindow = 15 * time.Minute

// parseDirInodeFromChannel extracts the parent inode from a keyspace channel
// name. channel looks like "__keyspace@1__:d732093"; prefix is
// "__keyspace@1__:". We strip the prefix and the leading 'd', then parse the
// remainder as a uint64. Returns ok=false for anything that isn't a d{inode}
// dir-entry key (defensive — the pattern already restricts to d*).
func parseDirInodeFromChannel(channel, prefix string) (uint64, bool) {
	if !strings.HasPrefix(channel, prefix) {
		return 0, false
	}
	key := channel[len(prefix):] // e.g. "d732093"
	if len(key) < 2 || key[0] != 'd' {
		return 0, false
	}
	inode, err := strconv.ParseUint(key[1:], 10, 64)
	if err != nil {
		return 0, false // d followed by non-numeric (e.g. delfiles/delSlices) — ignore
	}
	return inode, true
}

// ----------------------------------------------------------------------------
// InodeCoalescer: leading-edge debounce of changed-dir inodes.
// ----------------------------------------------------------------------------

// inodeCoalescer collects changed parent inodes into a dirty set and flushes
// them as a batch when EITHER quiet for `debounce` OR `maxWait` elapsed since
// the batch started. On flush, each unique inode is reconciled
// (reconcileDir); if the batch exceeds the class-gated burst ceiling we DROP
// the per-dir work and call TriggerSync() once instead (one full SCAN is
// cheaper than thousands of HGETALLs and reuses the proven path).
//
// Class-gating: debounce/maxWait/burstCeiling are re-read from the current
// link class on each flush, so a class change between batches takes effect
// without restarting the loop.
type inodeCoalescer struct {
	rc *RedisClient

	mu         sync.Mutex
	dirty      map[uint64]struct{}
	batchStart time.Time
	timer      *time.Timer
	stopped    bool
}

func newInodeCoalescer(rc *RedisClient) *inodeCoalescer {
	return &inodeCoalescer{
		rc:    rc,
		dirty: make(map[uint64]struct{}),
	}
}

// add inserts an inode into the dirty set and (re)arms the debounce timer.
// Leading-edge: the first add of an empty batch records batchStart and arms
// the timer; subsequent adds extend the quiet window but never push past
// maxWait (enforced at fire time).
func (c *inodeCoalescer) add(inode uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
	wasEmpty := len(c.dirty) == 0
	c.dirty[inode] = struct{}{}
	if wasEmpty {
		c.batchStart = time.Now()
	}

	tuning := tuningForClass(currentLinkClass())
	// Compute the next fire delay: min(debounce, remaining-until-maxWait).
	delay := tuning.debounce
	elapsed := time.Since(c.batchStart)
	remaining := tuning.maxWait - elapsed
	if remaining < 0 {
		remaining = 0
	}
	if remaining < delay {
		delay = remaining
	}
	if c.timer == nil {
		c.timer = time.AfterFunc(delay, c.flush)
	} else {
		c.timer.Reset(delay)
	}
}

// flush swaps out the dirty set and reconciles each unique inode (or promotes
// to one full SCAN if the batch is too large).
func (c *inodeCoalescer) flush() {
	c.mu.Lock()
	if c.stopped || len(c.dirty) == 0 {
		c.mu.Unlock()
		return
	}
	batch := c.dirty
	c.dirty = make(map[uint64]struct{})
	c.batchStart = time.Time{}
	tuning := tuningForClass(currentLinkClass())
	c.mu.Unlock()

	if len(batch) > tuning.burstCeiling {
		noteKeyspaceScanPromotion()
		jmlog.Info("metadata keyspace push: burst over ceiling, promoting to full SCAN",
			"dirs", len(batch), "ceiling", tuning.burstCeiling, "class", currentLinkClass())
		c.rc.keyspaceTriggerSync()
		return
	}

	for inode := range batch {
		if err := c.rc.keyspaceReconcileDir(inode); err != nil {
			jmlog.Warn("metadata keyspace push: reconcileDir failed",
				"inode", inode, "error", err.Error())
		}
	}
}

// stop halts the debounce timer and flushes any pending batch so a clean
// subscription teardown doesn't strand already-observed changes.
func (c *inodeCoalescer) stop() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.stopped = true
	if c.timer != nil {
		c.timer.Stop()
	}
	pending := len(c.dirty)
	c.mu.Unlock()
	if pending > 0 {
		// Best-effort drain on teardown; reconcileDir is idempotent.
		c.flushOnStop()
	}
}

// flushOnStop drains the remaining dirty set after stop() set the flag. It
// mirrors flush() but is callable post-stop (flush() itself early-returns when
// stopped). Kept separate to keep the stopped-guard in flush() simple.
func (c *inodeCoalescer) flushOnStop() {
	c.mu.Lock()
	batch := c.dirty
	c.dirty = make(map[uint64]struct{})
	tuning := tuningForClass(currentLinkClass())
	c.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	if len(batch) > tuning.burstCeiling {
		noteKeyspaceScanPromotion()
		c.rc.keyspaceTriggerSync()
		return
	}
	for inode := range batch {
		if err := c.rc.keyspaceReconcileDir(inode); err != nil {
			jmlog.Warn("metadata keyspace push: reconcileDir (teardown) failed",
				"inode", inode, "error", err.Error())
		}
	}
}

// ----------------------------------------------------------------------------
// decodeDirChild: SINGLE SOURCE OF TRUTH for the JuiceFS d/i byte layout.
//
// These offsets are load-bearing and MUST match the Lua script's gi()/ft and
// the i-key attr reads (redis.go luaScript). Extracting them here means the
// incremental path and the Lua SCAN share one definition; a future Go-side
// backstop would call these too.
// ----------------------------------------------------------------------------

// be32 reads a big-endian uint32 from b[off:off+4]. Caller guarantees length.
func be32(b []byte, off int) uint64 {
	return uint64(b[off])<<24 | uint64(b[off+1])<<16 | uint64(b[off+2])<<8 | uint64(b[off+3])
}

// be64 reads a big-endian uint64 from b[off:off+8]. Caller guarantees length.
func be64(b []byte, off int) uint64 {
	return be32(b, off)<<32 | be32(b, off+4)
}

// decodeDirChild decodes a 9-byte directory-entry HASH value into the child
// inode and file type. Matches the Lua gi(): the inode is the LOW 4 bytes of
// the value, big-endian — 1-based bytes 6..9 == 0-based [5:9]. ft is the first
// byte (1=file, 2=dir). Returns ok=false if the value isn't exactly 9 bytes.
//
// INODE >= 2^32 note: like the Lua gi(), this reads only the low 32 bits (the
// high word is always 0 in the live tree, max inode ~748363). A >2^32 inode
// would mis-resolve — the identical limitation the current SCAN already has,
// not a regression.
func decodeDirChild(val []byte) (childInode uint64, ft byte, ok bool) {
	if len(val) != 9 {
		return 0, 0, false
	}
	// gi(): bytes 6..9 (1-based) => indices 5..8 (0-based), big-endian.
	childInode = be32(val, 5)
	ft = val[0]
	return childInode, ft, true
}

// decodeInodeAttr decodes mtime (unix seconds) and size from a JuiceFS i-key
// attr blob. Matches the Lua: mtime = u64 at 1-based offset 24 (0-based 23),
// size = u64 at 1-based offset 52 (0-based 51). Guards len>=59; returns
// ok=false otherwise (mtime/size left 0, exactly as the Lua does).
func decodeInodeAttr(attr []byte) (mtime int64, size int64, ok bool) {
	if len(attr) < 59 {
		return 0, 0, false
	}
	mtime = int64(be64(attr, 23))
	size = int64(be64(attr, 51))
	return mtime, size, true
}

// dirChild is one decoded, mirrorable child of a reconciling directory
// (reconcileDir PASS 1 output).
type dirChild struct {
	inode     uint64
	childPath string
	mode      fs.FileMode
	isDir     bool
}

// buildReconcileChildEntries is reconcileDir's PASS 2, split out for direct
// testing: build the upsert set from the decoded children + their (possibly
// incomplete) attr map.
//
// SIZE-CLOBBER FIX (2026-07-10, found live: 1,922 mirror rows zeroed while
// backend data was intact): a FAILED attr read (transient error, blown
// deadline, MGET miss) used to leave size/mtime 0 and upsert that over a
// known-good row — the upsert-diff saw real≠0 as "changed" and wrote the
// zero, so every farm sweep / dir-event storm on a slow link progressively
// zeroed the mirror ("0-byte file" UX, OL verified-offload false-fails,
// task #38's family). A failed read is NOT evidence of an empty file:
// PRESERVE the existing row's size/mtime (the diff then sees no change — no
// write). A genuinely empty file still mirrors as 0 because its attr READS
// OK with size 0. A NEW child with an unreadable attr inserts with 0 (the
// SCAN's new-file semantics) and heals on the next successful read.
func buildReconcileChildEntries(children []dirChild, attrByInode map[uint64][]byte, lookup func(string) *Entry) (toUpsert []*Entry, newDirs []uint64) {
	for _, c := range children {
		var mtime time.Time
		var size int64
		attrOK := false
		if attr, ok := attrByInode[c.inode]; ok {
			if mt, sz, ok2 := decodeInodeAttr(attr); ok2 {
				if mt > 0 {
					mtime = time.Unix(mt, 0)
				}
				size = sz
				attrOK = true
			}
		}
		existing := lookup(c.childPath)
		if !attrOK && existing != nil {
			size = existing.Size
			mtime = existing.Mtime
		}

		e := &Entry{
			Path:       c.childPath,
			Name:       path.Base(c.childPath),
			ParentPath: path.Dir(c.childPath),
			IsDir:      c.isDir,
			Size:       size,
			Mtime:      mtime,
			Inode:      c.inode,
			Mode:       c.mode,
		}

		// Upsert-diff: only write if new or changed (same compare as
		// syncMetadata). Idempotent — harmless under double-processing.
		if existing == nil ||
			existing.Mtime.Unix() != e.Mtime.Unix() ||
			existing.Size != e.Size ||
			existing.Inode != e.Inode {
			toUpsert = append(toUpsert, e)
		}
		// A never-mirrored (or recreated) child DIR needs its own contents
		// reconciled — collect for the post-upsert requeue (B4' fix).
		if c.isDir && (existing == nil || existing.Inode != e.Inode) {
			newDirs = append(newDirs, c.inode)
		}
	}
	return toUpsert, newDirs
}

// JuiceFS directory-entry file-type bytes (the first byte of the 9-byte dir
// HASH value; see decodeDirChild). These mirror the upstream meta encoding.
const (
	jfsTypeFile    byte = 1 // regular file
	jfsTypeDir     byte = 2 // directory
	jfsTypeSymlink byte = 3 // symbolic link
)

// modeForFiletype maps a JuiceFS dir-entry filetype byte to the cache Entry's
// (fs.FileMode, isDir) pair. This is the SINGLE SOURCE OF TRUTH for the
// type→mode classification shared by EVERY decode/sync site (the full Lua SCAN
// in redis.go, this file's live keyspace dir-resync, and the per-key
// MetadataEvent applyEvent path) so a backend-resident symlink (ft==3) is never
// silently collapsed to a 0644 regular file.
//
// ft==2 (dir)     → 0755 | ModeDir, isDir=true   (the existing precedent)
// ft==3 (symlink) → 0644-perm + os.ModeSymlink, isDir=false — the type bit is
//
//	set the SAME way the Create/Symlink write path does
//	(mode = (mode &^ os.ModeType) | os.ModeSymlink) so it
//	survives the uint32(mode) SQLite round-trip (ModeSymlink
//	is 1<<27) and a cache-hit Lstat reports type=symlink, which
//	makes NFS clients issue READLINK.
//
// any other (incl. ft==1 file) → 0644 regular, isDir=false.
func modeForFiletype(ft byte) (mode fs.FileMode, isDir bool) {
	switch ft {
	case jfsTypeDir:
		return 0755 | fs.ModeDir, true
	case jfsTypeSymlink:
		// Preserve perm bits, set ONLY the symlink type bit — identical to the
		// write-side juiceFS.Symlink mapping in nfs/handler.go.
		return (fs.FileMode(0644) &^ os.ModeType) | os.ModeSymlink, false
	default:
		return 0644, false
	}
}

// ----------------------------------------------------------------------------
// reconcileDir: single-dir incremental reconcile (NO scan).
// ----------------------------------------------------------------------------

// reconcileDir incrementally reconciles ONE directory identified by its inode:
// HGETALL d{dirInode}, upsert-diff every child against the store, and prune
// children that vanished from Redis — all guarded by the QA-30 pin/FUSE
// checks so a transient miss can never ESTALE cached media.
//
// It resolves the dir's own path via LookupByInode (root inode 1 -> "/" which
// we represent as the internal-root ""). On a miss (event for a dir not yet in
// the store — out-of-order events or cold start) it defers to the full SCAN
// via TriggerSync and returns; it NEVER re-walks Redis ancestors (that
// re-incurs the SCAN cost the whole feature exists to avoid).
func (rc *RedisClient) reconcileDir(dirInode uint64) error {
	// Resolve two distinct path values:
	//
	//   parentPath  — the prefix used to BUILD each child's path (root: "", so a
	//                 top-level child path is just its bare name, e.g. "movies").
	//   storeParent — the key the Store indexes those children UNDER, which is
	//                 each child's ParentPath = path.Dir(childPath). For a bare
	//                 top-level name path.Dir("movies") == ".", so root children
	//                 live in childrenIdx["."], NOT childrenIdx[""]. The NFS
	//                 handler and syncMetadata both ListChildren(".") for the
	//                 root. scopedPrune MUST use storeParent, or a foreign
	//                 delete/rmdir of a TOP-LEVEL entry (fired as hdel/del on d1)
	//                 would never be pruned by the push path — scopedPrune("")
	//                 hits an empty index and is a no-op (EDGE Bug A).
	var parentPath, storeParent string
	if dirInode == 1 {
		parentPath = "" // tree root: children's paths are bare names
		storeParent = "."
	} else {
		ent := rc.store.LookupByInode(dirInode)
		if ent == nil {
			// Unknown ancestor. Historically this promoted straight to a full
			// SCAN ("let the authoritative SCAN establish it") — right for the
			// rare out-of-order USER event, catastrophic for the farm: its
			// derivative writes land under .juicemount/…, a namespace the
			// mirror DELIBERATELY never holds (task #78 scan-filter), so every
			// farm output dir arrives here — and the promoted SCAN can never
			// establish it, so the SAME inode re-promotes on every subsequent
			// write, forever. Live 2026-07-13 (single user, farm backfill
			// sweep running): a tunnel-priced full SCAN per coalescer flush —
			// the user-visible "index rebuilding every so often" and a
			// saturated cellular link doing zero useful work.
			//
			// noteUnknownAncestor promotes AT MOST ONCE per inode (a genuine
			// user dir is established by that one SCAN and never re-enters; a
			// filtered farm dir stays unknown and is dropped forever after)
			// and rate-limits promotions globally. The class backstop SCAN
			// still guarantees eventual convergence for anything deferred.
			// New user dirs normally never reach this branch at all: their
			// PARENT is known, and the parent's own d-key event mirrors the
			// child.
			rc.noteUnknownAncestor(dirInode)
			return nil
		}
		parentPath = ent.Path
		storeParent = ent.Path // non-root: path.Dir(parent+"/"+name) == parent
	}

	// Task #78: never reconcile a directory inside a scan-filtered namespace
	// (.trash/, .juicemount/ — see scanFilteredPath). The full SCAN can never
	// return these paths, so push-mirrored rows under them are permanently
	// absent from every SCAN diff and cycle in the pruneAbsent ladder forever.
	// Checked BEFORE any Redis round-trip: a trash/derivative burst (mass
	// delete-to-trash, farm fan-out) otherwise costs one HGETALL + N attr GETs
	// per event for rows we'd refuse to mirror anyway.
	if parentPath != "" && scanFilteredPath(parentPath) {
		noteScanFilteredSkip("reconcileDir", parentPath, 1)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rdb := rc.redisDB()
	key := "d" + strconv.FormatUint(dirInode, 10)
	raw, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return err
	}

	// Build the fresh child set and the entries to upsert.
	freshNames := make(map[string]struct{}, len(raw))
	var toUpsert []*Entry
	// Child DIRS the mirror has never seen (or whose inode changed — a
	// recreate): their contents were never reconciled and their create-burst
	// events were dropped as unknown-ancestor. Requeued below (B4' fix).
	var newDirs []uint64
	skippedFiltered := 0 // task #78: scan-filtered children not mirrored
	// PASS 1: decode the dir listing and collect mirrorable children. Attrs
	// are fetched in PASS 2 via chunked MGET — one round trip per chunk
	// instead of one sequential GET per child. The old per-child GETs shared
	// this function's single 30s ctx: a 257-child dir on a ~300ms tunnel
	// needs ~77s of sequential round-trips, so BIG DIRS' TAILS FAILED EVERY
	// RECONCILE on high-RTT links — feeding the size-clobber below.
	children := make([]dirChild, 0, len(raw))
	for name, valStr := range raw {
		val := []byte(valStr)
		childInode, ft, ok := decodeDirChild(val)
		if !ok {
			continue
		}
		freshNames[name] = struct{}{}

		// Classify via the shared single-source-of-truth mapper so ft==3
		// (symlink) maps to os.ModeSymlink instead of collapsing to 0644 — a
		// backend-resident symlink synced through this live dir-resync stays a
		// symlink (NFS then issues READLINK) rather than degrading to a regular
		// file. ft==2 still → ModeDir, ft==1/other → regular.
		mode, isDir := modeForFiletype(ft)

		// Child path: parentPath + "/" + name, mirroring syncMetadata. For the
		// tree root (parentPath==""), the child path is just the name.
		var childPath string
		if parentPath == "" {
			childPath = name
		} else {
			childPath = parentPath + "/" + name
		}

		// Task #78: never mirror a child row in a scan-filtered namespace
		// (reachable here only on a ROOT reconcile, where ".trash"/".juicemount"
		// appear as bare child names — deeper dirs are skipped wholesale above).
		// The name stays in freshNames (set above) so scopedPrune keeps seeing
		// it as Redis-fresh; we just refuse to MIRROR it. Skipping before the
		// attr fetch also saves the per-child Redis payload.
		if scanFilteredPath(childPath) {
			skippedFiltered++
			continue
		}
		children = append(children, dirChild{inode: childInode, childPath: childPath, mode: mode, isDir: isDir})
	}

	// PASS 2: bulk attr fetch (mtime/size) via chunked MGET.
	attrByInode := make(map[uint64][]byte, len(children))
	attrFetchErr := false
	const attrMGetChunk = 1000
	for start := 0; start < len(children); start += attrMGetChunk {
		end := start + attrMGetChunk
		if end > len(children) {
			end = len(children)
		}
		keys := make([]string, 0, end-start)
		for _, c := range children[start:end] {
			keys = append(keys, "i"+strconv.FormatUint(c.inode, 10))
		}
		vals, merr := rdb.MGet(ctx, keys...).Result()
		if merr != nil {
			attrFetchErr = true
			break
		}
		for i, v := range vals {
			if str, okS := v.(string); okS {
				attrByInode[children[start+i].inode] = []byte(str)
			}
		}
	}

	toUpsert, newDirs = buildReconcileChildEntries(children, attrByInode, rc.store.LookupByPath)
	if attrFetchErr {
		jmlog.Warn("reconcileDir: attr MGET failed — existing mirror sizes preserved, new children mirror with size 0 until the next successful read",
			"dir_inode", dirInode, "children", len(children))
	}

	if skippedFiltered > 0 {
		noteScanFilteredSkip("reconcileDir children", parentPath, skippedFiltered)
	}

	// Apply upserts (cache-first, then SQLite) via the applyEvent fast path
	// equivalent. For large dirs, batch via BulkInsert.
	if len(toUpsert) > 0 {
		if len(toUpsert) >= 500 {
			if err := rc.store.BulkInsert(toUpsert, 500); err != nil {
				return err
			}
		} else {
			for _, e := range toUpsert {
				rc.store.InsertToCache(e)
				if err := rc.store.Insert(e); err != nil {
					jmlog.Warn("metadata keyspace push: child insert", "path", e.Path, "error", err.Error())
				}
			}
		}
	}

	// B4' burst-ordering fix: requeue newly-discovered child dirs into the
	// LIVE push coalescer (wired only while a subscription is up), so a
	// burst-created subtree is walked top-down until nothing new upserts —
	// debounced, burst-ceilinged, idempotent. Runs AFTER the upserts land so
	// the requeued reconcile can resolve the child via LookupByInode. Without
	// this, a tree created in one burst (server-side import, farm fan-out)
	// mirrors only its top level: the children's own create-events arrived
	// before their parent was mirrored and were dropped as unknown-ancestor,
	// and the SCAN backstop is tunnel-gated — measured live as B4' (server
	// content invisible to the client indefinitely on cellular).
	if len(newDirs) > 0 {
		if fnp := rc.keyspaceRequeue.Load(); fnp != nil {
			for _, ino := range newDirs {
				(*fnp)(ino)
			}
			jmlog.Info("metadata keyspace push: requeued newly-discovered dirs",
				"parent", parentPath, "count", len(newDirs))
		}
	}

	// Scoped prune: any store child of storeParent whose name is NOT in the
	// fresh Redis set is a removal candidate — but only after the SAME QA-30
	// guards syncMetadata uses (Layer C pin guard, Layer A FUSE-present guard).
	// Reusing these guards is mandatory to not re-open the QA-30 ESTALE bug.
	// storeParent (not parentPath) is the children-index key — see above.
	rc.scopedPrune(storeParent, freshNames)

	jmlog.Info("metadata keyspace push: reconcileDir",
		"inode", dirInode, "parent", parentPath, "children", len(raw), "upserted", len(toUpsert))
	return nil
}

// scopedPrune removes store children of parentPath that are absent from the
// fresh Redis child-name set, applying the QA-30 Layer C (pin) and Layer A
// (FUSE Lstat) guards VERBATIM in spirit from syncMetadata. Unlike the full
// SCAN's PruneThreshold ladder, a push 'hdel'/'del' is an authoritative,
// targeted delete signal, so we act on a single observation — but still never
// prune a pinned file (Layer C) or a FUSE-present file (Layer A).
func (rc *RedisClient) scopedPrune(parentPath string, freshNames map[string]struct{}) {
	children, err := rc.store.ListChildren(parentPath)
	if err != nil || len(children) == 0 {
		return
	}

	// === Degrade gate (NFSv3 sprint slow-link false-flap, fix b.2 — task #66 salvage) ===
	// Defer ALL pruning while the backend is degraded/unreachable. The periodic
	// full-SCAN prune already gates on RecentlyDegraded (redis.go skipIncrement);
	// the keyspace-push prune had NO such gate, so during a slow-link probe flap
	// (drain + SCAN still succeeding, but the app arming offline) a single push
	// 'hdel' observation could remove real files mid-copy → STALE → Finder copy
	// error. fix b.1 makes RecentlyDegraded reachability-aware, so this gate is
	// actually TRUE during the flap. This DEFERS the authoritative push-delete
	// one cycle — it is NEVER dropped: the next keyspace event replays it, and
	// the backstop SCAN heals any missed delete (a removed file lingers at most
	// one degraded window). Placed AFTER the ListChildren early-out (cheap
	// no-children skip first) and BEFORE any candidate computation; the gate
	// covers the subtree-delete path too since the whole function returns.
	// Upserts are UNGATED (handled in reconcileDir before this call), so
	// navigation/convergence keeps working while pruning is paused. scopedPrune
	// holds no rc.mu, so RecentlyDegraded's rc.mu.RLock cannot deadlock here.
	if rc.RecentlyDegraded(60 * time.Second) {
		jmlog.Info("metadata keyspace push: scoped prune deferred (backend degraded)",
			"parent", parentPath, "candidates_unevaluated", len(children))
		return
	}

	var candidates []string // internal paths to delete
	for _, ch := range children {
		if _, present := freshNames[ch.Name]; present {
			continue
		}
		// === `._` AppleDouble guard (release-battery ._dirN had_shadow STALE) ===
		// `._` sidecars are scan-filtered from the backend SCAN, so they are
		// ALWAYS absent from freshNames even when they legitimately exist (Mac-
		// written + drained) — making "absent from Redis" a FALSE delete signal
		// for them specifically. They're managed Mac-side via the explicit Remove
		// path, never the reconcile. Pruning a `._` entry whose path-stable
		// Track-B handle the kernel still holds Forgets it → FromHandle STALE
		// (the `._dir8` had_shadow:true / path_sidecar:true STALE the release
		// battery's 06/08 caught — data stayed intact, but it tripped the strict
		// zero-Finder-error bar). The Stat/Open phantom-purge already skips `._`
		// for exactly this reason; the reconcile prune must too.
		if strings.HasPrefix(ch.Name, "._") {
			continue
		}
		// Task #78: internal-namespace DESCENDANT rows (.trash/…,
		// .juicemount/…) are managed by the one-time open-GC + push-insert
		// filter, never by push-prune. Post-GC none should exist; if one does
		// (seeded by an older build mid-session), a prune attempt here would
		// just churn through Layer A every root reconcile (FUSE shows the
		// path present → spared → re-candidate next cycle).
		//
		// Batch-3 adversarial review #5: scanFilteredDescendant, NOT
		// scanFilteredPath — the BARE ".trash"/".juicemount" dir rows stay
		// eligible as candidates so the QA-30 per-path FUSE Lstat below can
		// prune them if the backend namespace is ever genuinely removed.
		// While the namespace exists, FUSE shows it present and the spare
		// keeps the row (bounded: 2 extra Lstats worst case per root
		// reconcile that reaches this loop).
		if scanFilteredDescendant(ch.Path) {
			continue
		}
		candidates = append(candidates, ch.Path)
	}
	if len(candidates) == 0 {
		return
	}

	// === V2.3 G0: FUSE identity gate — skip this scoped prune when the
	// mountpoint has no real filesystem mounted (kext not loaded / mount
	// absent / wedged). The per-path FUSE Lstat spare below reads ENOENT for
	// everything against a plain directory, disabling its protection exactly
	// when it's needed most. The backstop SCAN re-derives these candidates
	// once the mount is real.
	if identOK, identReason := pin.FUSEIdentityState(); !identOK {
		jmlog.Warn("scoped prune: FUSE identity gate failed — skipping",
			"parent", parentPath, "reason", identReason,
			"would_have_pruned", len(candidates))
		return
	}

	// === QA-30 Layer D: never prune a spool-pending path ===
	// A path with a LIVE write-spool entry was just created by the user and is
	// still draining from local SSD — it is DEFINITIVELY live-and-local. It is
	// absent from the fresh Redis name set (not yet drained) and absent from
	// FUSE (only on the spool), so Layers A and C below both MISS it; pruning it
	// would Forget its path-stable Track-B NFS handle and surface ESTALE
	// mid-copy (live build-438 error 100070). Applied FIRST (before the per-path
	// FUSE Lstat) because the index probe is an O(1) in-memory lookup — far
	// cheaper than a FUSE round-trip — and spares the candidate before any
	// expensive verification. Candidate paths are store/JuiceFS-internal
	// (Entry.Path), the exact scheme the spool keys its index by (Create passes
	// the same string to store.MakeEntry and OpenWrite), so no conversion is
	// needed. The final toDelete set is re-filtered below to also spare any
	// spool-pending DESCENDANT of a (wrongly) candidate directory.
	{
		var spared int
		candidates, spared = rc.filterSpoolPending(candidates)
		if spared > 0 {
			jmlog.Info("metadata keyspace push: scoped prune: spared spool-pending",
				"count", spared, "parent", parentPath)
		}
		if len(candidates) == 0 {
			return
		}
	}

	// === QA-30 Layer C: never prune pinned paths ===
	pinned, perr := rc.store.pinnedSetPublic()
	if perr != nil {
		jmlog.Warn("metadata keyspace push: pin-checker error, skipping scoped prune",
			"error", perr.Error(), "would_have_pruned", len(candidates), "parent", parentPath)
		return
	}
	if len(pinned) > 0 {
		pinnedInternal := make(map[string]struct{}, len(pinned))
		for mp := range pinned {
			pinnedInternal[rc.internalFromMounted(mp)] = struct{}{}
		}
		filtered := candidates[:0]
		for _, p := range candidates {
			if _, ok := pinnedInternal[p]; ok {
				continue // pinned — spare it
			}
			filtered = append(filtered, p)
		}
		candidates = filtered
	}
	if len(candidates) == 0 {
		return
	}

	// === QA-30 Layer A: per-path FUSE Lstat verification ===
	// A FUSE-present-but-Redis-absent child means JuiceFS still has it; keep
	// it. A timed-out Lstat means FUSE is degraded; don't prune (retry later).
	if rc.fuseRoot != "" {
		verified := candidates[:0]
		lstatTimeouts := 0
		for _, p := range candidates {
			fusePath := rc.fusePathFor(p)
			if fusePath == "" {
				verified = append(verified, p)
				continue
			}
			isAbsent, ok := lstatNotExistWithTimeout(fusePath, time.Second)
			if !ok {
				lstatTimeouts++
				continue // timed out — don't prune this cycle
			}
			if isAbsent {
				verified = append(verified, p)
			}
			// else: FUSE says present — keep it (drop from candidates)
		}
		// Same >25% (floor 4) degraded-FUSE bail as syncMetadata.
		if lstatTimeouts >= 4 && lstatTimeouts*4 > len(candidates) {
			jmlog.Warn("metadata keyspace push: FUSE degraded, skipping scoped prune",
				"timeouts", lstatTimeouts, "candidates", len(candidates), "parent", parentPath)
			return
		}
		candidates = verified
	}

	// Expand directory candidates to their full subtrees. Neither Delete nor
	// DeletePaths prefix-prunes, so an rmdir/subtree-delete (HDEL d{parent}
	// <dirname>) would otherwise leave the directory's descendants orphaned in
	// the store until the rare backstop SCAN. We gather descendants from the
	// in-memory children index (no Redis round-trip) and delete the whole set
	// atomically via DeletePaths.
	toDelete := make([]string, 0, len(candidates))
	for _, p := range candidates {
		toDelete = append(toDelete, p)
		toDelete = append(toDelete, rc.collectSubtree(p)...)
	}

	// === QA-30 Layer D (subtree pass): re-filter the FINAL toDelete set ===
	// collectSubtree expanded each candidate DIRECTORY to its descendants. A
	// spool-pending FILE can live inside a candidate dir (e.g. the dir's Redis
	// entry vanished but a child is still draining from the spool), and the
	// candidate-root filter above only vetted the roots. Filtering the whole
	// toDelete set here guarantees no spool-pending descendant is deleted /
	// handle-Forgotten, while still pruning the genuinely-gone siblings around
	// it (Layer D is targeted, never a blanket prune-disable).
	{
		var spared int
		toDelete, spared = rc.filterSpoolPending(toDelete)
		if spared > 0 {
			jmlog.Info("metadata keyspace push: scoped prune: spared spool-pending (subtree)",
				"count", spared, "parent", parentPath)
		}
		if len(toDelete) == 0 {
			return
		}
	}

	if err := rc.store.DeletePaths(toDelete); err != nil {
		jmlog.Warn("metadata keyspace push: scoped prune delete", "parent", parentPath, "error", err.Error())
		return
	}
	jmlog.Info("metadata keyspace push: scoped prune",
		"parent", parentPath, "removed_roots", len(candidates), "removed_total", len(toDelete))
}

// filterSpoolPending applies QA-30 Layer D to a prune candidate set: it removes
// every path that the injected spool guard reports as live-and-local (a
// not-yet-drained write-spool entry). Returns the kept paths (in place, reusing
// the backing array) and the count spared. When no guard is wired (nil) it is a
// no-op returning paths unchanged. The guard is loaded via the race-clean
// accessor (review FIX 1), so this is safe to call from the reconcile goroutine
// while SetSpoolGuard concurrently installs the guard.
//
// Shared by BOTH prune paths so the spool-pending spare is symmetric:
// scopedPrune (keyspace push) and syncMetadata (periodic full-SCAN backstop,
// review FIX 2). A spool-pending path is absent from Redis (not yet drained)
// AND absent from FUSE (only on the spool), so Layers A (FUSE Lstat) and C
// (pin) both MISS it; pruning it would Forget its path-stable Track-B NFS
// handle and surface ESTALE mid-copy (live build-438 error 100070).
func (rc *RedisClient) filterSpoolPending(paths []string) (kept []string, spared int) {
	guard := rc.loadSpoolGuard()
	if guard == nil || len(paths) == 0 {
		return paths, 0
	}
	kept = paths[:0]
	for _, p := range paths {
		if guard(p) {
			spared++
			continue
		}
		kept = append(kept, p)
	}
	return kept, spared
}

// collectSubtree returns every store path strictly below `root` (its
// descendants), gathered breadth-first from the in-memory children index.
// Used to prefix-prune a removed directory's subtree, since the Store's
// Delete/DeletePaths only remove the exact paths given. Returns nil if `root`
// has no children (a plain file). The QA-30 guards in scopedPrune already
// vetted `root`; descendants of a vanished directory are themselves gone, so
// they don't need re-verification (the directory's absence in Redis is
// authoritative for its whole subtree).
func (rc *RedisClient) collectSubtree(root string) []string {
	var out []string
	queue := []string{root}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		children, err := rc.store.ListChildren(cur)
		if err != nil {
			continue
		}
		for _, ch := range children {
			out = append(out, ch.Path)
			if ch.IsDir {
				queue = append(queue, ch.Path)
			}
		}
	}
	return out
}

// deferBootGapFillToBackground reports whether the boot gap-fill SCAN should
// run behind the recalled mirror instead of in front of it.
//
// Gated on measured LATENCY, not on the freshness window, because the two
// answer different questions. The window asks "is the mirror probably still
// correct?"; this asks "can we AFFORD to find out before serving?". On a LAN
// the SCAN is seconds and stays synchronous. On a far link it is minutes
// regardless of how fresh the mirror is, and blocking on it buys accuracy the
// user cannot use while costing the responsiveness they can.
func (rc *RedisClient) deferBootGapFillToBackground() bool {
	if os.Getenv("JM_BOOT_SCAN_BACKGROUND") == "0" {
		return false
	}
	return netprofile.Default().HighLatency()
}

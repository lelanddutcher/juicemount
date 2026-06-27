package metadata

import (
	"context"
	"io/fs"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
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
func backstopForClass(c linkClass) time.Duration {
	switch c {
	case classLAN:
		return 10 * time.Minute
	case classWiFi:
		return 15 * time.Minute
	default: // tunnel / cellular / WAN — capped low to bound staleness windows
		return 5 * time.Minute
	}
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
// DefaultReconcileInterval (30s) so the periodic SCAN resumes as the
// authoritative fallback. We never go long while the backend is known
// unreachable: an unreachable backend means push delivery has stopped, so the
// SCAN must stay frequent enough to converge fast on reconnect.
func (rc *RedisClient) setEngagement(e keyspaceEngagement) {
	if e == keyspaceEnabled && reachableNow() {
		rc.backstopNanos.Store(int64(backstopForClass(currentLinkClass())))
	} else {
		rc.backstopNanos.Store(int64(DefaultReconcileInterval))
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
				co.add(inode)
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
	if err := rc.SyncOnce(); err != nil {
		jmlog.Warn("metadata keyspace push: gap-fill SCAN failed, staying DEGRADED",
			"error", err.Error())
		// Return; defers stop the coalescer and close the subscription. The
		// subscription WAS established, so keyspaceLoop should mark DEGRADED.
		return true
	}

	// Gap-fill complete — NOW ENABLED. Push carries deltas; the periodic SCAN
	// demotes to the rare class-gated backstop.
	rc.setEngagement(keyspaceEnabled)

	// Block until the pump exits (subscription dropped or stop fired).
	select {
	case <-rc.stopCh:
	case <-pumpDone:
	}
	return true
}

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
		jmlog.Info("metadata keyspace push: burst over ceiling, promoting to full SCAN",
			"dirs", len(batch), "ceiling", tuning.burstCeiling)
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
			// Unknown ancestor — let the authoritative SCAN establish it.
			rc.keyspaceTriggerSync()
			return nil
		}
		parentPath = ent.Path
		storeParent = ent.Path // non-root: path.Dir(parent+"/"+name) == parent
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
	for name, valStr := range raw {
		val := []byte(valStr)
		childInode, ft, ok := decodeDirChild(val)
		if !ok {
			continue
		}
		freshNames[name] = struct{}{}

		isDir := ft == 2
		var mode fs.FileMode = 0644
		if isDir {
			mode = 0755 | fs.ModeDir
		}

		// Child path: parentPath + "/" + name, mirroring syncMetadata. For the
		// tree root (parentPath==""), the child path is just the name.
		var childPath string
		if parentPath == "" {
			childPath = name
		} else {
			childPath = parentPath + "/" + name
		}

		// Fetch attrs for mtime/size (single GET, no scan). A missing/short
		// attr leaves mtime/size 0 — identical to the Lua's behavior.
		var mtime time.Time
		var size int64
		if attrStr, aerr := rdb.Get(ctx, "i"+strconv.FormatUint(childInode, 10)).Result(); aerr == nil {
			if mt, sz, ok := decodeInodeAttr([]byte(attrStr)); ok {
				if mt > 0 {
					mtime = time.Unix(mt, 0)
				}
				size = sz
			}
		}

		e := &Entry{
			Path:       childPath,
			Name:       path.Base(childPath),
			ParentPath: path.Dir(childPath),
			IsDir:      isDir,
			Size:       size,
			Mtime:      mtime,
			Inode:      childInode,
			Mode:       mode,
		}

		// Upsert-diff: only write if new or changed (same compare as
		// syncMetadata). Idempotent — harmless under double-processing.
		existing := rc.store.LookupByPath(e.Path)
		if existing == nil ||
			existing.Mtime.Unix() != e.Mtime.Unix() ||
			existing.Size != e.Size ||
			existing.Inode != e.Inode {
			toUpsert = append(toUpsert, e)
		}
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

	var candidates []string // internal paths to delete
	for _, ch := range children {
		if _, present := freshNames[ch.Name]; present {
			continue
		}
		candidates = append(candidates, ch.Path)
	}
	if len(candidates) == 0 {
		return
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

	if err := rc.store.DeletePaths(toDelete); err != nil {
		jmlog.Warn("metadata keyspace push: scoped prune delete", "parent", parentPath, "error", err.Error())
		return
	}
	jmlog.Info("metadata keyspace push: scoped prune",
		"parent", parentPath, "removed_roots", len(candidates), "removed_total", len(toDelete))
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

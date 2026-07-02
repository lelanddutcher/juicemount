package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/redis/go-redis/v9"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

const (
	// Channel used for real-time metadata events between JuiceMount clients.
	SubscribeChannel = "juicemount:metadata"

	// Default batch reconciliation interval.
	DefaultReconcileInterval = 30 * time.Second

	// PruneThreshold is the number of consecutive reconciliation cycles an entry
	// must be absent from Redis before it is deleted from SQLite.
	// At 30s intervals, 10 cycles = ~5 minutes minimum before any prune fires.
	//
	// QA-30 (2026-05-25): bumped from 2 to 10. A single 17s SCAN cycle was
	// observed returning incomplete results under load — only 2 consecutive
	// such cycles (≤60s) were enough to mark a still-present file for
	// deletion. DaVinci saw ESTALE on fully-cached media mid-edit. 10 cycles
	// gives ~5 min buffer for any realistic transient Redis or network blip;
	// pairs with the pin-store guard below (pinned files are never pruned
	// regardless of threshold) and the per-path FUSE Lstat verification
	// added separately to syncMetadata.
	PruneThreshold = 10

	// V2.3 G6 (task #79): bounds on the prune ladder's Layer-A per-path FUSE
	// Lstat verification, mirroring collectFastPathPrunes' probe cap + wall
	// budget. Proven live 2026-07-02: with ~112k permanent ladder candidates
	// (SCAN-coverage bug) the UNBOUNDED Layer-A loop ran 86s on LAN and ~90
	// minutes over a cellular relay (~50ms/Lstat), saturating the link and
	// pinning "Rebuilding index…" forever. Candidates deferred by these bounds
	// keep their ladder position (re-inserted into pruneAbsent at their prior
	// count) so they re-qualify next cycle — deferral, never data loss.
	// Kill switch: JM_LAYERA_BUDGET=0 restores the unbounded pre-G6 behavior.
	layerAProbeCap    = 2048            // max ladder candidates Lstat-probed per cycle
	layerAProbeBudget = 3 * time.Second // max wall time probing ladder candidates
)

// MetadataEvent represents a real-time metadata change published via Redis SUBSCRIBE.
type MetadataEvent struct {
	Op    string `json:"op"` // "create", "update", "rename", "delete"
	Path  string `json:"path"`
	Size  int64  `json:"size,omitempty"`
	Mtime int64  `json:"mtime,omitempty"`
	Inode uint64 `json:"inode,omitempty"`
	IsDir bool   `json:"is_dir,omitempty"`

	// IsSymlink is the per-event symlink discriminator. The live event path
	// only carried IsDir, so a create/update event for a SYMLINK was re-derived
	// as a 0644 regular file in applyEvent — downgrading the locally-minted
	// ModeSymlink cache Entry (juiceFS.Symlink) the moment the self-write
	// pub/sub (or a peer's event) replayed it. Carrying the type explicitly
	// keeps applyEvent's Entry as ModeSymlink, mirroring the ft==3 case the
	// SCAN/dir-resync decoders now handle. Additive + omitempty: older
	// publishers that never set it decode as false (regular/dir), unchanged.
	IsSymlink bool `json:"is_symlink,omitempty"`

	// For rename operations
	OldPath string `json:"old_path,omitempty"`
}

// RedisClient manages Redis connections for metadata sync.
// It provides two sync mechanisms:
//  1. SUBSCRIBE: event-driven, <100ms propagation for real-time changes
//  2. Batch reconciliation: periodic full tree pull, catches anything SUBSCRIBE missed
type RedisClient struct {
	// rdb is an atomic.Pointer because Reconnect() swaps the underlying
	// *redis.Client (on a network-interface change) concurrently with readers
	// on other goroutines — the periodic SCAN (syncMetadata), the self-write
	// pub/sub (subscribeLoop/PublishEvent), AND the keyspace-push loop
	// (ConfigGet/PSubscribe/HGetAll/Get). Reconnect() is wired to
	// NetWatcher.OnChange, and the keyspace feature explicitly targets cellular
	// where interface flaps (=> Reconnect) are frequent, so an unsynchronized
	// word-level read/write of this field is a reachable data race. Mirrors the
	// lstatFnPtr atomic.Pointer idiom already in this file. Access via
	// rc.redisDB(); never read the field directly.
	rdb      atomic.Pointer[redis.Client]
	redisURL string // stored for reconnect
	store    *Store

	reconcileInterval time.Duration
	stopCh            chan struct{}
	stopOnce          sync.Once
	syncNowCh         chan struct{} // signals reconcileLoop to run immediately

	// syncMu single-flights syncMetadata. Originally the full SCAN had exactly
	// one caller goroutine (reconcileLoop), so pruneAbsent (a plain map) was a
	// safe single-writer. The keyspace gap-fill now also runs SyncOnce
	// SYNCHRONOUSLY on the keyspace goroutine (so push is only trusted after the
	// baseline SCAN completes — EDGE Bug C), giving syncMetadata two possible
	// callers. This mutex serializes them so they never overlap, preserving the
	// pruneAbsent single-writer invariant and avoiding two concurrent full SCANs.
	syncMu sync.Mutex

	mu                sync.RWMutex
	lastSyncDuration  time.Duration
	lastSyncTime      time.Time
	lastSyncStartedAt time.Time // when the most recent sync BEGAN (for flap debounce)
	// G7 (task #80): truthful IsSyncing across failed/deferred syncs.
	// lastSyncEndedAt records when the most recent sync attempt ENDED —
	// success, failure, OR deferral (set by noteSyncOutcome on every
	// syncMetadata exit). Before G7, IsSyncing keyed only on
	// lastSyncStartedAt > lastSyncTime, so a FAILING sync left /activity
	// rendering "Rebuilding index…" forever (live 2026-07-01: 3h of it).
	// lastSyncStartedAt itself is NEVER zeroed — the reconcileLoop flap
	// debounce reads it and must keep suppressing flap-triggered SCANs
	// right after a failed/deferred attempt (the link was just saturated).
	lastSyncEndedAt    time.Time
	syncDeferredReason string // non-empty while the most recent attempt was DEFERRED (G7)
	syncDeferredStreak int    // consecutive DEFERRED attempts; Warn logs once per streak (G7)
	lastSyncEntries    int
	connected          bool
	lastDisconnect     time.Time
	lastReconnect      time.Time

	// backstopNanos is the CURRENT desired interval (in nanoseconds) for the
	// periodic full-SCAN reconcile loop, read every loop turn so a running
	// loop's cadence can be changed live (the reconcileLoop captures
	// baseInterval once at entry, so a plain field would never take effect).
	//
	// When the keyspace-notification push is ENABLED and healthy, the
	// keyspace loop sets this to a long, class-gated value (minutes/hours)
	// — the full SCAN becomes a rare backstop. When push is DISABLED or
	// DEGRADED it is reset to DefaultReconcileInterval (30s) so the periodic
	// SCAN resumes as the authoritative fallback. doReconcile's backoff
	// temporarily overrides this on failure and resets back to it on
	// recovery (NOT to the captured baseInterval). Always holds a value >=
	// 1; zero is treated as DefaultReconcileInterval by reconcileLoop.
	backstopNanos atomic.Int64

	// pruneAbsent tracks how many consecutive reconciliation cycles each path
	// has been absent from Redis. Only paths absent for PruneThreshold+ cycles
	// are actually deleted from SQLite. Guarded by mu.
	pruneAbsent map[string]int

	// pruneFollowUp marks the next syncNowCh wake as a G5 fast-path
	// CONTINUATION (a capped mass-delete with confirmed survivors) — it must
	// bypass the keyspace-push deferral and the flap debounce in
	// reconcileLoop, which exist to suppress network-flap triggers, not
	// self-scheduled prune convergence. Set in syncMetadata; consumed
	// (Swap(false)) by reconcileLoop.
	pruneFollowUp atomic.Bool

	// V2.3 U6 (task #37): live progress of the in-flight full SCAN, so the
	// UI can render "Rebuilding index… N / ~M" instead of a bare spinner
	// (field report: an opaque multi-minute rebuild feels broken). scanned
	// counts raw child entries as SCAN batches land; the denominator is the
	// PREVIOUS sync's entry count — an estimate (labelled ~), but accurate
	// for a warm mirror and free (no extra Redis round-trip).
	syncScanned atomic.Int64

	// QA-30 (2026-05-25): path conversion config so syncMetadata can
	// correctly cross-reference the pin store (mountpoint-prefixed paths)
	// against metadata.Store entries (JuiceFS-internal paths, no prefix)
	// and probe FUSE for prune verification. Set once at startup via
	// SetPathConfig; reads thereafter are unsynchronized but safe because
	// they happen on the single reconcile goroutine.
	mountPoint string
	fuseRoot   string

	// spoolPending (QA-30 Layer D, NFSv3 sprint) reports whether a store path
	// has a LIVE write-spool entry that has not yet drain-succeeded — i.e. the
	// user just created the file and it is still on local SSD, NOT yet in
	// Redis/FUSE. scopedPrune (and syncMetadata's full-SCAN prune) use it to
	// spare such paths: they are absent from the fresh Redis name set (not
	// drained) AND absent from FUSE (only on the spool), so Layer A (FUSE Lstat)
	// and Layer C (pin) both MISS them — and pruning would Forget their
	// path-stable Track-B NFS handle, surfacing ESTALE mid-copy (live build-438
	// error 100070).
	//
	// Injected (not imported) to avoid a metadata→nfs import cycle: the bridge
	// wires it via SetSpoolGuard to nfs.SpoolStore.HasPending after both the
	// SpoolStore and this RedisClient exist.
	//
	// CONCURRENCY (review FIX 1): production wiring order is rc.Start() (which
	// launches the reconcile goroutine) BEFORE SetSpoolGuard — the spool is
	// created much later than Start, so the guard is installed while the
	// reconcile goroutine may already be reading it. Storing it in a plain field
	// would be a data race (write in SetSpoolGuard vs. read in
	// scopedPrune/syncMetadata). It is therefore held in an atomic.Pointer to a
	// named func type: SetSpoolGuard Stores, and the reconcile path Loads via
	// loadSpoolGuard. nil pointer = no spool configured = the pre-spool no-op
	// (default).
	spoolPending atomic.Pointer[spoolGuardFunc]

	// Test seams for the keyspace-push path. Production leaves these nil and
	// the real methods run. Tests set them to observe coalescer behavior
	// (which dirs got reconciled, how many full-SCAN promotions) without a
	// live Redis. See keyspaceReconcileDir / keyspaceTriggerSync below.
	testReconcileDir func(uint64) error
	testTriggerSync  func()
}

// keyspaceReconcileDir dispatches to the test seam when set, else the real
// single-dir reconcile. Used by the coalescer so tests can count calls.
func (rc *RedisClient) keyspaceReconcileDir(inode uint64) error {
	if rc.testReconcileDir != nil {
		return rc.testReconcileDir(inode)
	}
	return rc.reconcileDir(inode)
}

// keyspaceTriggerSync dispatches to the test seam when set, else the real
// TriggerSync. Used by the coalescer (burst promotion) and reconcileDir
// (unknown-ancestor defer) so tests can observe full-SCAN promotions.
func (rc *RedisClient) keyspaceTriggerSync() {
	if rc.testTriggerSync != nil {
		rc.testTriggerSync()
		return
	}
	rc.TriggerSync()
}

// SetPathConfig wires the mount point (e.g. "/Volumes/zpool") and the
// JuiceFS FUSE root (e.g. "/Users/x/.juicemount/fuse-internal") used by
// QA-30's pin-filter normalization and per-path Lstat verification. Must
// be called before reconcileLoop is started. No-op if either argument is
// empty — disables QA-30 path-aware behavior gracefully.
func (rc *RedisClient) SetPathConfig(mountPoint, fuseRoot string) {
	rc.mountPoint = strings.TrimRight(mountPoint, "/")
	rc.fuseRoot = strings.TrimRight(fuseRoot, "/")
}

// spoolGuardFunc is the type of the QA-30 Layer D write-spool membership probe.
// Named (rather than an inline func type) so it can be stored in an
// atomic.Pointer — sync/atomic needs a concrete named type.
type spoolGuardFunc func(volRelPath string) bool

// SetSpoolGuard wires the write-spool membership probe used by QA-30 Layer D
// in scopedPrune AND syncMetadata's full-SCAN prune (see the spoolPending
// field). fn(volRelPath) must return true while volRelPath has a live,
// not-yet-drained spool entry — production passes nfs.SpoolStore.HasPending.
// Keyed in the store/JuiceFS-internal path scheme (no leading slash, no mount
// prefix), which is exactly a prune candidate's Entry.Path, so no conversion is
// needed at the call site.
//
// Injected rather than imported to keep metadata free of an nfs dependency
// (metadata must NOT import nfs — the cycle). Nil-safe: passing nil restores
// the pre-spool no-op (the prune spares nothing extra).
//
// CONCURRENCY (review FIX 1): stored atomically. Production calls this AFTER
// rc.Start (the spool is created long after Start), so the reconcile goroutine
// may already be reading the guard via loadSpoolGuard when this Stores it. The
// atomic store/load pair makes that race-clean regardless of wiring order.
func (rc *RedisClient) SetSpoolGuard(fn func(volRelPath string) bool) {
	if fn == nil {
		rc.spoolPending.Store(nil)
		return
	}
	gf := spoolGuardFunc(fn)
	rc.spoolPending.Store(&gf)
}

// loadSpoolGuard atomically loads the QA-30 Layer D guard installed by
// SetSpoolGuard. Returns nil when no spool is configured (the pre-spool no-op).
// This is the race-clean accessor the reconcile goroutine MUST use to read the
// guard (review FIX 1) — never read rc.spoolPending as a plain field.
func (rc *RedisClient) loadSpoolGuard() spoolGuardFunc {
	if p := rc.spoolPending.Load(); p != nil {
		return *p
	}
	return nil
}

// SetReconcileInterval overrides the periodic reconcile cadence (LB-4:
// the app's "Reconcile interval" preference, previously a placebo). Same
// contract as SetPathConfig: must be called before Start — reconcileLoop
// snapshots the value once at launch, so later writes are both racy and
// ineffective. d <= 0 keeps DefaultReconcileInterval, letting callers
// pass an unset (zero) config value straight through.
func (rc *RedisClient) SetReconcileInterval(d time.Duration) {
	if d <= 0 {
		return
	}
	rc.reconcileInterval = d
	// Seed the live backstop so the configured cadence actually takes effect:
	// reconcileLoop reads backstopNanos (via currentBackstop), not
	// reconcileInterval, on every turn. setEngagement (the keyspace loop) may
	// override this live once push is engaged — but until then this is the
	// authoritative periodic-SCAN interval, so the LB-4 preference is honored
	// from launch rather than staying a placebo behind DefaultReconcileInterval.
	rc.backstopNanos.Store(int64(d))
}

// lstatFunc is the type of the package-level Lstat hook. Aliased so we can
// use it with sync/atomic safely (atomic.Pointer needs a concrete named type
// to store).
type lstatFunc func(string) (os.FileInfo, error)

// lstatFnPtr is the injection point for tests. Production callers always
// use os.Lstat; tests can substitute a deterministic blocker to exercise
// the timeout path without relying on scheduler timing.
//
// Stored as atomic.Pointer because tests legitimately swap the value while
// production worker goroutines may still be reading it — the race detector
// requires explicit synchronization (QA-30 code review HIGH-2 follow-up).
var lstatFnPtr atomic.Pointer[lstatFunc]

func init() {
	var def lstatFunc = os.Lstat
	lstatFnPtr.Store(&def)
}

// setLstatFn is the test-only setter; the prior value is returned for
// cleanup via the standard `defer setLstatFn(old)` pattern.
func setLstatFn(fn lstatFunc) lstatFunc {
	old := *lstatFnPtr.Load()
	lstatFnPtr.Store(&fn)
	return old
}

// lstatGate caps concurrent FUSE Lstat calls so a sustained FUSE wedge
// can't spawn an unbounded number of goroutines (QA-30 code review HIGH-1).
// 8 in flight is enough to amortize per-call latency on a healthy FUSE
// without saturating the kernel's request queue. When the gate is full,
// callers block briefly until a slot frees; on a wedged FUSE, the gate's
// own ctx-based wait drops the request rather than queuing forever.
var lstatGate = make(chan struct{}, 8)

// lstatNotExistWithTimeout mirrors nfs/handler.go's helper of the same
// name. Kept package-private here so metadata doesn't depend on nfs.
// Returns (isNotExist, ok); ok=false means the Lstat timed out (FUSE
// likely wedged) — callers MUST NOT interpret a timeout as "file gone".
//
// QA-30 code review HIGH-1 mitigation: bounded concurrency via lstatGate
// caps in-flight goroutines at 8. If the gate is full beyond the deadline,
// the call returns ok=false (treated as "FUSE degraded, don't prune").
// The spawned worker still leaks for the duration of the wedged Lstat,
// but the leak is bounded to gate-size and drains naturally when FUSE
// recovers.
func lstatNotExistWithTimeout(p string, timeout time.Duration) (isNotExist, ok bool) {
	// Acquire gate slot or bail. timer is shared with the actual Lstat
	// deadline below — if we waste 500ms here waiting for a slot, we
	// only have 500ms left for the Lstat itself.
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case lstatGate <- struct{}{}:
	case <-timer.C:
		return false, false
	}

	// Atomic load; safe even when tests swap the function concurrently.
	fn := *lstatFnPtr.Load()
	ch := make(chan error, 1)
	go func() {
		_, err := fn(p)
		ch <- err
		<-lstatGate // release slot only after Lstat actually returns
	}()
	select {
	case err := <-ch:
		return os.IsNotExist(err), true
	case <-timer.C:
		// Worker still owns the gate slot until its Lstat returns; that
		// caps the leak at lstatGate's buffer size. The next caller will
		// either get a slot (if the wedge clears) or bail on the gate
		// wait above.
		return false, false
	}
}

// internalFromMounted converts a mountpoint-prefixed path
// (e.g. "/Volumes/zpool/Foo/Bar") into the JuiceFS-internal form used by
// metadata.Store ("Foo/Bar"). Returns the input unchanged if rc.mountPoint
// is unset or the path doesn't carry the expected prefix.
func (rc *RedisClient) internalFromMounted(p string) string {
	if rc.mountPoint == "" {
		return p
	}
	if p == rc.mountPoint {
		return ""
	}
	prefix := rc.mountPoint + "/"
	if strings.HasPrefix(p, prefix) {
		return p[len(prefix):]
	}
	return p
}

// fusePathFor maps a JuiceFS-internal path back to its absolute location
// in the FUSE mount, so syncMetadata can Lstat-verify a prune candidate
// before deletion. Returns "" if rc.fuseRoot is unset.
func (rc *RedisClient) fusePathFor(internalPath string) string {
	if rc.fuseRoot == "" {
		return ""
	}
	if internalPath == "" {
		return rc.fuseRoot
	}
	return rc.fuseRoot + "/" + strings.TrimLeft(internalPath, "/")
}

// ParseRedisURL extracts host, port, and db from a redis:// URL.
func ParseRedisURL(rawURL string) (addr string, db int, err error) {
	if !strings.HasPrefix(rawURL, "redis://") {
		rawURL = "redis://" + rawURL
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", 0, fmt.Errorf("parse redis URL: %w", err)
	}
	host := u.Hostname()
	port := "6379"
	if p := u.Port(); p != "" {
		port = p
	}
	db = 0
	if len(u.Path) > 1 {
		db, _ = strconv.Atoi(u.Path[1:])
	}
	return host + ":" + port, db, nil
}

// NewRedisClient connects to Redis and prepares for syncing.
// It does NOT start background goroutines — call Start() for that.
func NewRedisClient(redisURL string, store *Store) (*RedisClient, error) {
	addr, db, err := ParseRedisURL(redisURL)
	if err != nil {
		return nil, err
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DB:           db,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 10 * time.Second,
		DialTimeout:  10 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	rc := &RedisClient{
		redisURL:          redisURL,
		store:             store,
		reconcileInterval: DefaultReconcileInterval,
		stopCh:            make(chan struct{}),
		syncNowCh:         make(chan struct{}, 1),
		connected:         true,
		pruneAbsent:       make(map[string]int),
	}
	rc.rdb.Store(rdb)
	rc.backstopNanos.Store(int64(DefaultReconcileInterval))
	return rc, nil
}

// NewRedisClientDeferred builds a RedisClient WITHOUT requiring the backend to
// be reachable — it skips the connectivity Ping and starts in the disconnected
// state. Used by the start-while-offline path (R-4): the app boots with the
// backend down (e.g. a laptop opened on a plane), serves directory NAVIGATION
// from the local SQLite mirror, and this client's reconcile loop flips
// connected=true and catches up automatically once the backend returns.
//
// The returned client is structurally identical to NewRedisClient's (every
// field initialized — stopCh, the buffered syncNowCh that TriggerSync's
// non-blocking send needs, pruneAbsent, reconcileInterval, backstopNanos) so
// every downstream rc.* call site works unchanged; the ONLY differences are
// connected=false and a non-zero lastDisconnect. connected=false matters beyond
// cosmetics: it makes RecentlyDegraded() true, which suppresses the FUSE-Lstat
// phantom-purge in the Stat/open paths so an offline session can't delete real
// entries from the mirror while the backend is unreachable. The reconcile loop
// (rc.Start) lifts connected once a reconcile succeeds; redis.NewClient itself is
// lazy (it dials on first use), so no network happens here.
func NewRedisClientDeferred(redisURL string, store *Store) (*RedisClient, error) {
	addr, db, err := ParseRedisURL(redisURL)
	if err != nil {
		return nil, err
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DB:           db,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 10 * time.Second,
		DialTimeout:  10 * time.Second,
	})
	rc := &RedisClient{
		redisURL:          redisURL,
		store:             store,
		reconcileInterval: DefaultReconcileInterval,
		stopCh:            make(chan struct{}),
		syncNowCh:         make(chan struct{}, 1),
		connected:         false,
		lastDisconnect:    time.Now(),
		pruneAbsent:       make(map[string]int),
	}
	rc.rdb.Store(rdb) // rdb is atomic.Pointer (see the field doc / Reconnect race)
	rc.backstopNanos.Store(int64(DefaultReconcileInterval))
	return rc, nil
}

// redisDB returns the current *redis.Client via an atomic load, so callers
// see a consistent pointer even while Reconnect() swaps it. Never read the
// rc.rdb field directly.
func (rc *RedisClient) redisDB() *redis.Client { return rc.rdb.Load() }

// Store returns the underlying metadata store.
func (rc *RedisClient) Store() *Store { return rc.store }

// Connected returns whether the Redis client is currently connected.
func (rc *RedisClient) Connected() bool {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.connected
}

// LastDisconnect returns when the client last lost connectivity.
func (rc *RedisClient) LastDisconnect() time.Time {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.lastDisconnect
}

// LastReconnect returns when the client last regained connectivity.
func (rc *RedisClient) LastReconnect() time.Time {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.lastReconnect
}

// RecentlyDegraded reports whether the Redis connection (and therefore
// the metadata authority) is unhealthy right now OR was unhealthy
// recently enough that downstream state may still be inconsistent.
//
// Use this to gate destructive cache mutations whose correctness depends
// on Redis being authoritative. Specifically: the phantom-file purge
// in the NFS handler deletes entries when FUSE returns ENOENT for a
// path the SQLite cache thinks exists. If Redis was unavailable
// recently, JuiceFS's view of the filesystem may have been wrong
// during the outage (it can't fetch metadata it doesn't have cached
// locally), and a single FUSE-says-missing observation is not
// trustworthy. Gating purges on !RecentlyDegraded prevents the
// "Redis blip → real files marked phantom → cache deletion → stale
// NFS handles on reconnect" cascade observed 2026-05-16 ~06:14.
//
// cooldown is the window after a reconnect during which we still
// consider the client degraded. Tuned for the typical metadata-sync
// cycle (~30s) plus a safety margin.
func (rc *RedisClient) RecentlyDegraded(cooldown time.Duration) bool {
	// Reachability-aware (NFSv3 sprint slow-link false-flap, fix b.1 — task #66
	// salvage). The rc.connected flag below is written ONLY by doReconcile /
	// Reconnect / the keyspace sub — NEVER by the reachability monitor. During a
	// slow-link probe flap the drain + SCAN keep SUCCEEDING (the backend is
	// provably reachable; only the cold probe SYN false-fails), so rc.connected
	// stays true and this returned false — leaving BOTH prune paths un-gated
	// while the app was about to engage offline. Consult the injected
	// reachability signal too (wired via metadata.SetClassSignals from bridge):
	// an unreachable verdict means the offline-engage path is arming, so any
	// prune in flight must DEFER (the authoritative delete replays next cycle /
	// heals via the backstop SCAN — it is never dropped). Checked BEFORE taking
	// rc.mu.RLock so reachableNow()'s separate keyspaceSignalMu never nests
	// under rc.mu (no lock-order inversion).
	if !reachableNow() {
		return true
	}
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	if !rc.connected {
		return true
	}
	// Reconnected, but still inside cooldown.
	if !rc.lastReconnect.IsZero() && !rc.lastDisconnect.IsZero() {
		if rc.lastReconnect.After(rc.lastDisconnect) {
			if time.Since(rc.lastReconnect) < cooldown {
				return true
			}
		}
	}
	return false
}

// Reconnect tears down and re-establishes the Redis connection.
// Called when a network interface change is detected so that TCP connections
// bound to the old interface are replaced with connections on the new one.
func (rc *RedisClient) Reconnect() error {
	addr, db, err := ParseRedisURL(rc.redisURL)
	if err != nil {
		return err
	}

	// Close the old connection (ignore errors — it may already be broken)
	rc.redisDB().Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DB:           db,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 10 * time.Second,
		DialTimeout:  10 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		rc.mu.Lock()
		rc.connected = false
		rc.lastDisconnect = time.Now()
		rc.mu.Unlock()
		return fmt.Errorf("redis reconnect ping: %w", err)
	}

	rc.rdb.Store(rdb)
	rc.mu.Lock()
	rc.connected = true
	rc.lastReconnect = time.Now()
	rc.mu.Unlock()

	jmlog.Info("redis reconnected")
	return nil
}

// collectFastPathPrunes implements the V2.3 G5 mass-delete fast-path (task
// #73). It walks rc.pruneAbsent (single-writer: only syncMetadata's
// goroutine), finds subtree ROOTS (absent paths whose parent is not absent),
// FUSE-Lstat-confirms each root, and returns every absent path under a
// confirmed-deleted root — removing them from pruneAbsent. `._` AppleDouble
// sidecars are never included (scan-filtered from Redis; absence there is not
// a delete signal — symmetric with the ladder's and scopedPrune's guard).
//
// Bounded three ways (root count, wall time, rows per cycle); `capped=true`
// means work remains and the caller should schedule an immediate follow-up
// cycle. Inert when: the cycle is RecentlyDegraded (a partial Redis view must
// never confirm a deletion — same rule as the ladder), the G0 FUSE-identity
// gate fails, fuseRoot is unset, or JM_PRUNE_FASTPATH=0 (kill switch).
func (rc *RedisClient) collectFastPathPrunes(skipIncrement bool) (fastConfirmed map[string]struct{}, capped bool) {
	fastConfirmed = make(map[string]struct{})
	if len(rc.pruneAbsent) == 0 || skipIncrement || rc.fuseRoot == "" ||
		os.Getenv("JM_PRUNE_FASTPATH") == "0" {
		return fastConfirmed, false
	}
	if identOK, _ := pin.FUSEIdentityState(); !identOK {
		return fastConfirmed, false
	}
	const (
		fastPathRootCap   = 2048            // max subtree roots probed per cycle
		fastPathBudget    = 3 * time.Second // max wall time probing roots
		fastPathDeleteCap = 25000           // max rows fast-pruned per cycle (one DeletePaths tx)
	)
	absent := rc.pruneAbsent
	// Review fix: exclude spool-pending paths at COLLECTION time (roots
	// before their probe, descendants before the sweep), not only in the
	// downstream Layer D filter. This restores the ladder's ordering
	// invariant against a drain completing mid-cycle: the drainer
	// materializes the FUSE dest BEFORE MarkDrainComplete evicts the spool
	// shadow, so a path whose guard reads not-pending and whose root then
	// Lstats ENOENT is genuinely deleted — every interleaving is caught by
	// the guard (still pending) or the probe (already on FUSE). The
	// downstream filterSpoolPending stays as a backstop.
	guard := rc.loadSpoolGuard()
	var roots []string
	for p := range absent {
		if strings.HasPrefix(path.Base(p), "._") {
			continue
		}
		if guard != nil && guard(p) {
			continue
		}
		if _, parentAbsent := absent[path.Dir(p)]; !parentAbsent {
			roots = append(roots, p)
		}
	}
	probeStart := time.Now()
	probed, confirmedRoots := 0, 0
	for _, root := range roots {
		if probed >= fastPathRootCap || time.Since(probeStart) > fastPathBudget ||
			len(fastConfirmed) >= fastPathDeleteCap {
			capped = true
			break
		}
		probed++
		isAbsent, ok := lstatNotExistWithTimeout(rc.fusePathFor(root), time.Second)
		if !ok || !isAbsent {
			continue // timeout or FUSE-present — leave to the ladder
		}
		confirmedRoots++
		prefix := root + "/"
		fastConfirmed[root] = struct{}{}
		for p := range absent {
			if len(fastConfirmed) >= fastPathDeleteCap {
				capped = true
				break
			}
			if strings.HasPrefix(p, prefix) && !strings.HasPrefix(path.Base(p), "._") &&
				(guard == nil || !guard(p)) {
				fastConfirmed[p] = struct{}{}
			}
		}
	}
	// Review fix: ENOENT evidence is only trustworthy if the mount is
	// verified REAL after the last probe — a mount dying to a plain dir
	// mid-cycle turns every Lstat into a false "deleted". Cache-bypassing
	// re-check before any pruneAbsent mutation; a discard leaves the ladder
	// counters intact, and the re-stamped cache means the ladder-path
	// identity gate in the prune pass sees the same fresh verdict.
	if len(fastConfirmed) > 0 {
		if identOK, identReason := pin.FUSEIdentityFresh(); !identOK {
			jmlog.Warn("metadata sync: fast-path prune DISCARDED — FUSE identity failed post-probe",
				"reason", identReason, "would_have_pruned", len(fastConfirmed))
			return make(map[string]struct{}), false
		}
	}
	for p := range fastConfirmed {
		delete(rc.pruneAbsent, p)
	}
	if len(fastConfirmed) > 0 {
		jmlog.Info("metadata sync: fast-path pruning confirmed-deleted subtrees",
			"roots_probed", probed,
			"roots_confirmed", confirmedRoots,
			"paths", len(fastConfirmed),
			"probe_ms", time.Since(probeStart).Round(time.Millisecond).Milliseconds(),
			"capped", capped,
		)
	}
	return fastConfirmed, capped
}

// TriggerSync signals the reconcile loop to run an immediate sync cycle.
// Non-blocking: if a signal is already pending it does nothing.
func (rc *RedisClient) TriggerSync() {
	select {
	case rc.syncNowCh <- struct{}{}:
	default:
	}
}

// LastSyncDuration returns the duration of the most recent batch sync.
func (rc *RedisClient) LastSyncDuration() time.Duration {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.lastSyncDuration
}

// LastSyncTime returns when the most recent batch sync completed.
func (rc *RedisClient) LastSyncTime() time.Time {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.lastSyncTime
}

// LastSyncEntries returns the entry count from the most recent batch sync.
func (rc *RedisClient) LastSyncEntries() int {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.lastSyncEntries
}

// IsSyncing reports whether a reconcile (metadata index rebuild) is running
// right now — i.e. the most recent sync STARTED after the most recent sync
// COMPLETED. Used by /activity to surface "Rebuilding index…" while a full
// SCAN is in flight (the period when Finder can feel sluggish on cold paths).
//
// G7 (task #80): a sync attempt that FAILED or was DEFERRED is not "syncing".
// lastSyncEndedAt is stamped on EVERY syncMetadata exit (noteSyncOutcome), so
// the started-after-ended clause turns this false the moment an attempt dies —
// previously only lastSyncTime (success-only) cleared it, and a failure streak
// pinned /activity on an eternal "Rebuilding index…".
func (rc *RedisClient) IsSyncing() bool {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return !rc.lastSyncStartedAt.IsZero() &&
		rc.lastSyncStartedAt.After(rc.lastSyncTime) &&
		rc.lastSyncStartedAt.After(rc.lastSyncEndedAt)
}

// SyncDeferredReason returns a non-empty reason string while the most recent
// sync attempt was classified DEFERRED (G7: SCAN budget exceeded on a slow
// link while the keyspace push is engaged and carrying deltas). Cleared by
// the next successful — or genuinely-failed — attempt. /activity renders a
// plain-language "sync deferred, live updates continue via push" line off it
// instead of the eternal "Rebuilding index…".
func (rc *RedisClient) SyncDeferredReason() string {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.syncDeferredReason
}

// SyncProgress reports the in-flight rebuild's scanned-entry count and an
// ESTIMATED total (the previous sync's entry count; 0 on a first-ever sync).
// Progress covers the SCAN phase, which dominates rebuild time on slow links;
// the diff/upsert tail after it is seconds. V2.3 U6 (task #37).
func (rc *RedisClient) SyncProgress() (scanned, estTotal int64) {
	rc.mu.RLock()
	est := int64(rc.lastSyncEntries)
	rc.mu.RUnlock()
	// Review fix (MED): the first in-process sync — exactly the multi-minute
	// boot rebuild the field report was about, now backgrounded by U1 — has
	// lastSyncEntries==0. Fall back to the persisted mirror's row count so a
	// warm 264k-entry mirror still gets a denominator. Cheap (indexed
	// COUNT(*)), and only paid while est==0 during that first sync.
	if est == 0 && rc.store != nil {
		if n, err := rc.store.Count(); err == nil {
			est = int64(n)
		}
	}
	return rc.syncScanned.Load(), est
}

// SyncOnce performs a single batch reconciliation (Lua tree pull → SQLite).
func (rc *RedisClient) SyncOnce() error {
	return rc.syncMetadata()
}

// Start begins the SUBSCRIBE listener, periodic batch reconciliation, and —
// when enabled — the Redis keyspace-notification push loop.
//
// The keyspace loop is strictly additive and flag-gated (JM_METADATA_KEYSPACE_PUSH).
// When it is not engaged the system behaves byte-identically to before: 30s
// full-SCAN reconcile + the self-write juicemount:metadata pub/sub. See
// keyspace.go for the engagement state machine (DISABLED/ENABLED/DEGRADED).
func (rc *RedisClient) Start() {
	go rc.subscribeLoop()
	go rc.reconcileLoop()
	if rc.keyspacePushEnabled() {
		go rc.keyspaceLoop()
	}
}

// Stop halts all background goroutines and closes the Redis connection.
func (rc *RedisClient) Stop() {
	rc.stopOnce.Do(func() {
		close(rc.stopCh)
		rc.redisDB().Close()
	})
}

// PublishEvent sends a metadata event to all subscribed clients.
func (rc *RedisClient) PublishEvent(ctx context.Context, evt MetadataEvent) error {
	data, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	return rc.redisDB().Publish(ctx, SubscribeChannel, string(data)).Err()
}

// subscribeLoop listens for real-time metadata events via Redis SUBSCRIBE.
func (rc *RedisClient) subscribeLoop() {
	for {
		select {
		case <-rc.stopCh:
			return
		default:
		}

		rc.runSubscribe()

		// If we get here, subscription was lost. Wait briefly then retry.
		select {
		case <-rc.stopCh:
			return
		case <-time.After(2 * time.Second):
			jmlog.Info("redis SUBSCRIBE reconnecting")
		}
	}
}

func (rc *RedisClient) runSubscribe() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Monitor stopCh to cancel the subscription context
	go func() {
		select {
		case <-rc.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	sub := rc.redisDB().Subscribe(ctx, SubscribeChannel)
	defer sub.Close()

	ch := sub.Channel()
	for msg := range ch {
		var evt MetadataEvent
		if err := json.Unmarshal([]byte(msg.Payload), &evt); err != nil {
			log.Printf("subscribe: invalid event: %v", err)
			continue
		}
		rc.applyEvent(evt)
	}
}

// applyEvent applies a single real-time metadata event to the local store.
// Updates the in-memory cache immediately (never blocked), then writes to
// SQLite with retry (may be briefly blocked by BulkInsert transactions).
func (rc *RedisClient) applyEvent(evt MetadataEvent) {
	// Classify mode from the event's type discriminators. IsSymlink takes
	// precedence so a create/update/rename of a symlink keeps os.ModeSymlink
	// (and never downgrades the locally-minted ModeSymlink cache Entry) instead
	// of collapsing to a 0644 regular file. Mirrors the ft==3 → ModeSymlink
	// mapping the SCAN/dir-resync decoders use (modeForFiletype).
	var mode fs.FileMode = 0644
	switch {
	case evt.IsDir:
		mode = 0755 | fs.ModeDir
	case evt.IsSymlink:
		// Preserve perm bits, set ONLY the symlink type bit — identical to the
		// write-side juiceFS.Symlink mapping and modeForFiletype's ft==3 case.
		mode = (mode &^ os.ModeType) | os.ModeSymlink
	}

	switch evt.Op {
	case "create", "update":
		// Task #78: never mirror push events for scan-filtered namespaces
		// (.trash/, .juicemount/ — see scanFilteredPath). The full SCAN can
		// never return these paths, so a push-inserted row would be
		// permanently absent from every SCAN diff and cycle in the
		// pruneAbsent ladder forever. `._` sidecars are deliberately NOT
		// filtered here (they are SCAN-visible once drained).
		if scanFilteredPath(evt.Path) {
			noteScanFilteredSkip("applyEvent", evt.Path, 1)
			return
		}
		e := &Entry{
			Path:       evt.Path,
			Name:       path.Base(evt.Path),
			ParentPath: path.Dir(evt.Path),
			IsDir:      evt.IsDir,
			Size:       evt.Size,
			Mtime:      time.Unix(evt.Mtime, 0),
			Inode:      evt.Inode,
			Mode:       mode,
		}
		// In-memory cache first (instant, never blocked)
		rc.store.InsertToCache(e)
		// SQLite write (serialized by writeMu, no retry needed)
		if err := rc.store.Insert(e); err != nil {
			log.Printf("subscribe apply create/update: %v", err)
		}

	case "delete":
		// Deletes are NOT namespace-filtered: removing an internal-namespace
		// row (if one was seeded by an older build) is convergent.
		rc.store.DeleteFromCache(evt.Path)
		if err := rc.store.Delete(evt.Path); err != nil {
			log.Printf("subscribe apply delete: %v", err)
		}

	case "rename":
		// The OldPath removal is UNGATED: a rename INTO .trash is JuiceFS's
		// delete-to-trash, and the source row must still be dropped.
		if evt.OldPath != "" {
			rc.store.DeleteFromCache(evt.OldPath)
			rc.store.Delete(evt.OldPath)
		}
		// Task #78: skip mirroring the DESTINATION when it lands in a
		// scan-filtered namespace (delete-to-trash). A rename OUT of .trash
		// (restore) has a non-filtered destination and is mirrored normally.
		if scanFilteredPath(evt.Path) {
			noteScanFilteredSkip("applyEvent", evt.Path, 1)
			return
		}
		e := &Entry{
			Path:       evt.Path,
			Name:       path.Base(evt.Path),
			ParentPath: path.Dir(evt.Path),
			IsDir:      evt.IsDir,
			Size:       evt.Size,
			Mtime:      time.Unix(evt.Mtime, 0),
			Inode:      evt.Inode,
			Mode:       mode,
		}
		rc.store.InsertToCache(e)
		if err := rc.store.Insert(e); err != nil {
			log.Printf("subscribe apply rename: %v", err)
		}
	}
}

// reconcileFlapDebounce gates the flap-triggered-reconcile debounce. Default ON.
// JM_RECONCILE_FLAP_DEBOUNCE=0 (or master JM_NET_ADAPTIVE=0, which pins
// class=medium → window 0) restores the old fire-on-every-flap behavior. See
// docs/TUNING/REVERT_LOG.md (R2).
var reconcileFlapDebounce = os.Getenv("JM_RECONCILE_FLAP_DEBOUNCE") != "0"

// flapDebounceInterval is the minimum time since the last sync start/finish below
// which a NETWORK-CHANGE-triggered reconcile is suppressed. Class-aware: only
// slow/metered links debounce; LAN (medium/fast) returns 0 (no change — flaps are
// rare and SCANs are 2-10s there).
func flapDebounceInterval() time.Duration {
	if !reconcileFlapDebounce {
		return 0
	}
	switch netprofile.Default().Class() {
	case netprofile.ClassMetered:
		return 60 * time.Second
	case netprofile.ClassSlow:
		return 30 * time.Second
	default:
		return 0
	}
}

// reconcileLoop runs periodic batch reconciliation with exponential backoff
// on failure (network transitions, Redis unreachable). It also listens for
// immediate sync requests via syncNowCh (triggered by network changes).
func (rc *RedisClient) reconcileLoop() {
	backoff := rc.currentBackstop()
	lastBase := backoff // the base we last applied to the ticker; distinct from
	// backoff, which doReconcile may STRETCH above this base (task #22).
	maxBackoff := 5 * time.Minute
	consecutiveFailures := 0

	ticker := time.NewTicker(backoff)
	defer ticker.Stop()
	for {
		// Re-read the desired backstop interval each turn so the keyspace
		// loop can change a RUNNING loop's cadence live. Only Reset when we
		// are NOT in a backoff streak — during backoff, doReconcile owns the
		// ticker (short intervals) and must not be overridden by the long
		// backstop value until it recovers. (Regression guard for the
		// `baseInterval` captured-once bug.)
		if consecutiveFailures == 0 {
			want := rc.currentBackstop()
			// Re-baseline only when the BASE itself changed (a keyspace engagement
			// transition or a SetReconcileInterval write) — compare to lastBase,
			// NOT backoff. backoff can legitimately sit ABOVE the base because
			// doReconcile applied a task #22 adaptive STRETCH; comparing to backoff
			// would clobber that stretch every turn, pulling the cadence back down
			// to the base and defeating the 500GB-copy duty-cycle. On a real base
			// change we re-baseline; doReconcile re-applies any stretch next sync.
			if want != lastBase {
				lastBase = want
				backoff = want
				ticker.Reset(backoff)
			}
		}

		select {
		case <-ticker.C:
			rc.doReconcile(&consecutiveFailures, &backoff, maxBackoff, ticker)
		case <-rc.syncNowCh:
			// G5 fast-path continuation (review fix): a capped mass-delete
			// cycle with confirmed survivors schedules its own follow-up.
			// It is authoritative prune convergence, not a network-flap
			// trigger — run it immediately, bypassing the push deferral and
			// flap debounce below (each continuation is still internally
			// gated by RecentlyDegraded + the G0 identity checks inside
			// collectFastPathPrunes, and stops the moment nothing survives).
			if rc.pruneFollowUp.Swap(false) {
				jmlog.Info("fast-path prune continuation — running immediate reconcile")
				rc.doReconcile(&consecutiveFailures, &backoff, maxBackoff, ticker)
				continue
			}
			// Keyspace-push deferral (2026-06-27): when the push is actively
			// ENABLED the backstop is long (setEngagement stores a >30s interval
			// ONLY for ENABLED+reachable). In that state a network-change/reconnect
			// must NOT fire a full SCAN here — the keyspace loop OWNS reconnect
			// convergence (PSUBSCRIBE-then-gap-fill on its OWN (re)connect), and the
			// rare backstop is the safety net. On a flaky link — cellular, or a
			// false-positive reachability blip that never actually dropped the
			// PSUBSCRIBE (observed: backend pinging at 16ms while the probe flapped
			// "unreachable" every ~10s) — this path otherwise fires a full 200k SCAN
			// PER flap, the exact contention the push was built to remove. When the
			// push is NOT carrying deltas (backstop == 30s: DISABLED / DEGRADED /
			// unreachable) we fall through to the classic flap-debounce + SCAN, which
			// is then the authoritative convergence path (byte-identical to before).
			if rc.currentBackstop() > DefaultReconcileInterval {
				jmlog.Info("network-change reconcile deferred to keyspace push",
					"reason", "push ENABLED; keyspace loop owns reconnect gap-fill")
				continue
			}
			// Flap debounce (2026-06-16): on a flaky cellular link the reachability
			// monitor flaps constantly and EACH recovery fires a full-keyspace SCAN
			// (87-178s on cellular, ~93% finding zero changes), stacking back-to-back
			// and burning metered data + contending the link. Suppress a flap-trigger
			// if a sync STARTED or COMPLETED within the class-aware debounce window;
			// the periodic ticker still catches any real drift within the cadence.
			// LAN (medium/fast) → window 0 → NO debounce, behavior unchanged.
			if d := flapDebounceInterval(); d > 0 {
				rc.mu.RLock()
				recent := rc.lastSyncStartedAt
				if rc.lastSyncTime.After(recent) {
					recent = rc.lastSyncTime
				}
				rc.mu.RUnlock()
				if !recent.IsZero() && time.Since(recent) < d {
					jmlog.Info("flap-triggered reconcile suppressed (recent sync within debounce window)",
						"since_last_sec", int(time.Since(recent).Seconds()),
						"debounce_sec", int(d.Seconds()))
					continue
				}
			}
			jmlog.Info("reconciliation triggered by network change")
			rc.doReconcile(&consecutiveFailures, &backoff, maxBackoff, ticker)
		case <-rc.stopCh:
			return
		}
	}
}

// currentBackstop returns the current desired reconcile interval, reading
// the live backstopNanos atomic. Falls back to DefaultReconcileInterval if
// unset/zero so a running loop never spins on a 0-duration ticker.
func (rc *RedisClient) currentBackstop() time.Duration {
	n := rc.backstopNanos.Load()
	if n <= 0 {
		return DefaultReconcileInterval
	}
	return time.Duration(n)
}

func (rc *RedisClient) doReconcile(consecutiveFailures *int, backoff *time.Duration, maxBackoff time.Duration, ticker *time.Ticker) {
	// Backoff is computed off DefaultReconcileInterval, NOT the (possibly
	// minutes/hours-long) backstop interval — a failure streak must drive
	// the loop FASTER to detect recovery, never slower. The backstop only
	// applies once we're connected+ENABLED and reset cleanly (below).
	baseInterval := DefaultReconcileInterval
	if err := rc.syncMetadata(); err != nil {
		// G7 (task #80): DEFERRED, not failed. A SCAN that exceeded its
		// class-gated budget WHILE the keyspace push is engaged (backstop >
		// DefaultReconcileInterval — the established push-carrying test, see
		// the reconcileLoop deferral above) is redundant convergence work:
		// deltas are flowing via push the whole time. The failure backoff is
		// deliberately FASTER than the backstop (outage recovery), which is
		// exactly wrong here — live 2026-07-01, each fast retry saturated a
		// metered cellular relay for the full budget and then died, 31
		// consecutive times over 3h. Instead: no failure bookkeeping (no
		// consecutiveFailures bump, no connected=false flip — push healthy
		// means the backend IS reachable, and flipping it would poison
		// RecentlyDegraded's prune/phantom-purge gates), and the next attempt
		// waits for the normal backstop tick. Resetting the counter/ticker
		// here also ends any PRIOR failure streak's fast cadence — otherwise
		// a short backoff ticker would keep re-burning the link with deferred
		// attempts. Warn is logged once per streak by noteSyncOutcome.
		// Kill switch: JM_SYNC_DEFERRAL=0 (see isDeferredSyncErr).
		if rc.isDeferredSyncErr(err) {
			*consecutiveFailures = 0
			*backoff = rc.currentBackstop()
			ticker.Reset(*backoff)
			return
		}
		*consecutiveFailures++
		*backoff = baseInterval * time.Duration(1<<min(*consecutiveFailures, 6))
		if *backoff > maxBackoff {
			*backoff = maxBackoff
		}
		rc.mu.Lock()
		if rc.connected {
			rc.connected = false
			rc.lastDisconnect = time.Now()
		}
		rc.mu.Unlock()
		// [JM6] Classify network-path errors distinctly from backend
		// errors. The 2026-05-16 incident exposed that every "Redis
		// is degraded" log line was actually a Mac-side route-table
		// failure ("no route to host"), not a backend process fault.
		// Telling the user "Redis is degraded" when the actual cause
		// is "your Wi-Fi blipped" makes the UI lie about cause and
		// sets the user up to look at the wrong thing.
		kind, friendly := classifyConnErr(err)
		msg := "reconciliation failed"
		if kind == errKindNetworkPath {
			msg = "network path to backend lost"
		}
		jmlog.Warn(msg,
			"attempt", *consecutiveFailures,
			"next_in_sec", int64(backoff.Round(time.Second).Seconds()),
			"kind", string(kind),
			"reason", friendly,
			"error", err.Error(),
		)
		ticker.Reset(*backoff)
	} else {
		rc.mu.Lock()
		wasDisconnected := !rc.connected
		rc.connected = true
		if wasDisconnected {
			rc.lastReconnect = time.Now()
		}
		lastSync := rc.lastSyncDuration
		rc.mu.Unlock()

		if *consecutiveFailures > 0 {
			jmlog.Info("reconciliation recovered", "previous_failures", *consecutiveFailures)
			*consecutiveFailures = 0
		}

		// [JM6] Adaptive reconcile cadence (task #22) — now BASED on the LIVE
		// keyspace backstop (rc.currentBackstop(): long when push is
		// ENABLED+healthy, 30s otherwise) instead of the captured baseInterval, so
		// the keyspace-driven backstop and the sync-cost duty-cycle COMPOSE rather
		// than fight. syncMetadata re-reads the FULL backend tree (4-5s on 200k
		// entries) holding Redis/SQLite contention; an expensive sync still
		// stretches the interval further ON TOP of the backstop (the 500GB-copy
		// "anemic ingest" fix, 2026-06-15), clamped to maxBackoff. With push
		// ENABLED the backstop is already long, so the stretch rarely engages;
		// with push DISABLED the backstop is 30s and the stretch behaves as before.
		base := rc.currentBackstop()
		next := base
		if lastSync > reconcileAdaptiveThreshold {
			scaled := lastSync * reconcileDutyDivisor
			if scaled < base {
				scaled = base
			}
			if scaled > maxBackoff {
				scaled = maxBackoff
			}
			next = scaled
		}
		if next != *backoff {
			if next > base {
				jmlog.Info("reconcile cadence adapted to sync cost",
					"last_sync_ms", lastSync.Milliseconds(),
					"next_interval_sec", int64(next.Round(time.Second).Seconds()),
				)
			}
			*backoff = next
			ticker.Reset(*backoff)
		}
	}
}

// isDeferredSyncErr reports whether a syncMetadata error should be classified
// DEFERRED rather than FAILED (G7, task #80): the error wraps
// context.DeadlineExceeded (the SCAN exceeded its class-gated wall budget —
// syncMetadata guarantees the sentinel is present whenever its own context
// expired) AND the keyspace push is engaged and carrying deltas
// (currentBackstop() > DefaultReconcileInterval — the same push-carrying test
// reconcileLoop's network-change deferral uses; setEngagement stores a long
// backstop ONLY for ENABLED+reachable). In that state the backstop SCAN is
// redundant convergence work and must not drive the fast failure backoff.
//
// If the push drops, setEngagement(degraded/disabled) snaps the backstop back
// to 30s and this returns false — a real outage still gets the classic
// fail-fast backoff and recovery. Kill switch: JM_SYNC_DEFERRAL=0 restores
// the pre-G7 behavior exactly (every error is a failure). Read per call.
func (rc *RedisClient) isDeferredSyncErr(err error) bool {
	if err == nil || os.Getenv("JM_SYNC_DEFERRAL") == "0" {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) &&
		rc.currentBackstop() > DefaultReconcileInterval
}

// noteSyncOutcome records the end of a sync attempt (G7, task #80). Called on
// EVERY syncMetadata exit — success, failure, or deferral — so IsSyncing()
// stops reporting an attempt that already died, and /activity can render the
// deferred state truthfully instead of an eternal "Rebuilding index…".
// lastSyncStartedAt is deliberately NOT touched: the reconcileLoop flap
// debounce keys on it and must keep suppressing flap-triggered SCANs right
// after a failed/deferred attempt (the link was just saturated for the full
// budget). The deferral Warn logs ONCE per streak, not per attempt — on a
// cellular relay the streak can run for hours at the backstop cadence.
func (rc *RedisClient) noteSyncOutcome(err error) {
	deferred := rc.isDeferredSyncErr(err)
	now := time.Now()
	rc.mu.Lock()
	rc.lastSyncEndedAt = now
	prevStreak := rc.syncDeferredStreak
	if deferred {
		rc.syncDeferredStreak++
		rc.syncDeferredReason = "SCAN budget exceeded on slow link; keyspace push engaged"
	} else {
		rc.syncDeferredStreak = 0
		rc.syncDeferredReason = ""
	}
	rc.mu.Unlock()

	if deferred && prevStreak == 0 {
		jmlog.Warn("metadata sync DEFERRED — full-SCAN budget exceeded while keyspace push is engaged; "+
			"skipping failure backoff, next attempt at the backstop tick (logged once per streak)",
			"budget_sec", int(scanContextTimeout().Seconds()),
			"backstop_sec", int(rc.currentBackstop().Seconds()),
			"class", currentLinkClass().String(),
			"error", err.Error(),
		)
	} else if err == nil && prevStreak > 0 {
		jmlog.Info("metadata sync recovered after deferred streak",
			"deferred_attempts", prevStreak)
	}
}

// [JM6] Adaptive-reconcile tuning (task #22).
//
//   - reconcileAdaptiveThreshold: syncs faster than this are "cheap" and
//     stay at the base interval — no point slowing down responsiveness
//     when the sync isn't contending for anything meaningful.
//   - reconcileDutyDivisor: target duty cycle. next = lastSync * divisor,
//     so a 5s sync → 150s interval (sync is ~1/30 ≈ 3% of wall-clock),
//     a 1.5s sync → 45s, clamped into [baseInterval, maxBackoff].
const (
	reconcileAdaptiveThreshold = 1500 * time.Millisecond
	reconcileDutyDivisor       = 30
)

// connErrKind classifies a connection-related error. Used to decide
// whether a failure means "the user's machine can't reach the backend"
// (network path) versus "we reached the backend but it rejected or
// timed out our request" (backend). The distinction matters for what
// to tell the user and which recovery story to engage:
//
//   - errKindNetworkPath: the network between this Mac and the backend
//     is the problem. Engage offline mode automatically. Tell the user
//     their connection is the issue, not the server.
//
//   - errKindBackend: we reached the backend but it failed us. The
//     network is fine; the backend may be overloaded, restarting, or
//     genuinely broken. Don't auto-engage offline (the user can still
//     use the network for other things).
//
//   - errKindOther: ambiguous or app-state errors (closed clients,
//     context cancellations). Don't engage offline mode based on these
//     alone.
//
// ConnErrKind classifies a connection-related error. Exported because
// downstream items (network reachability monitor, offline-mode
// auto-engage, NFS handler fail-fast, UI indicator) consume the
// classification from separate packages.
type ConnErrKind string

const (
	ErrKindNetworkPath ConnErrKind = "network_path"
	ErrKindBackend     ConnErrKind = "backend"
	ErrKindOther       ConnErrKind = "other"
)

// Internal aliases for the existing in-package call sites — keeps the
// diff minimal while exposing exported names externally.
type connErrKind = ConnErrKind

const (
	errKindNetworkPath = ErrKindNetworkPath
	errKindBackend     = ErrKindBackend
	errKindOther       = ErrKindOther
)

// classifyConnErr inspects a Go networking error and returns its kind
// plus a friendly one-liner suitable for log/UI display.
//
// Classification rules (from observed real-world errors in JuiceMount
// logs over 2026-05-13 to 2026-05-16):
//
//   - syscall.EHOSTUNREACH ("no route to host") → network path; the
//     Mac's route table doesn't have a path to the destination IP.
//     Common cause: Wi-Fi re-association, sleep/wake, network change.
//   - syscall.ENETUNREACH ("network is unreachable") → network path.
//   - DNS resolution failure (net.DNSError) → network path. The Mac
//     can't even ask the DNS server.
//   - net.OpError with "i/o timeout" during a Dial → network path
//     (couldn't establish connection in time).
//   - syscall.ECONNREFUSED ("connection refused") → backend. The
//     network reached the host but nothing's listening on the port.
//     Backend process is down.
//   - "redis: client is closed" / context.Canceled → other (app
//     state, not a real failure).
//   - Anything else with a timeout → backend (we got through but the
//     backend didn't respond in time).
//
// ClassifyConnErr is the exported entry point. See classifyConnErr
// for the implementation contract and behavior. Re-exported as a
// thin wrapper so cross-package consumers (health/, pin/, nfs/) can
// call without depending on internal helper names.
func ClassifyConnErr(err error) (ConnErrKind, string) {
	return classifyConnErr(err)
}

func classifyConnErr(err error) (connErrKind, string) {
	if err == nil {
		return errKindOther, "no error"
	}
	// Context cancellations and our own teardown look like errors but
	// aren't network or backend faults. Cancellation = app teardown;
	// deadline-exceeded = our caller imposed a deadline that fired,
	// which is a property of OUR config, not a network fault.
	if errors.Is(err, context.Canceled) {
		return errKindOther, "request canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errKindOther, "operation deadline exceeded"
	}
	msg := err.Error()
	if strings.Contains(msg, "redis: client is closed") {
		return errKindOther, "redis client closed (app shutdown or reconnect)"
	}
	// go-redis surfaces backend-side disconnect mid-command as io.EOF
	// or io.ErrUnexpectedEOF wrapped in fmt.Errorf. Classify as
	// backend — the network was fine; the backend closed the socket
	// (restart, OOM kill, etc.). Common during graceful Redis upgrade.
	if strings.Contains(msg, "EOF") {
		return errKindBackend, "backend closed the connection (restart?)"
	}
	// go-redis connection-pool exhaustion is an app-state condition —
	// neither network nor backend is necessarily at fault, but logs
	// should explain it readably.
	if strings.Contains(msg, "redis: connection pool exhausted") {
		return errKindOther, "redis connection pool exhausted"
	}

	// Unwrap to a *net.OpError if present — gives us access to the
	// underlying syscall errno and the operation name (dial/read/etc).
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		// DNS resolution failure surfaces inside net.OpError as a
		// *net.DNSError.
		var dnsErr *net.DNSError
		if errors.As(opErr.Err, &dnsErr) {
			if dnsErr.IsNotFound {
				return errKindNetworkPath, "DNS: host not found"
			}
			return errKindNetworkPath, "DNS resolution failed"
		}
		// Syscall errno cases.
		var syscallErr *os.SyscallError
		if errors.As(opErr.Err, &syscallErr) {
			var errno syscall.Errno
			if errors.As(syscallErr.Err, &errno) {
				switch errno {
				case syscall.EHOSTUNREACH:
					return errKindNetworkPath, "no route to host (network change?)"
				case syscall.ENETUNREACH:
					return errKindNetworkPath, "network unreachable"
				case syscall.ETIMEDOUT:
					if opErr.Op == "dial" {
						return errKindNetworkPath, "connection attempt timed out"
					}
					return errKindBackend, "backend response timed out"
				case syscall.ECONNREFUSED:
					return errKindBackend, "connection refused (backend not listening)"
				case syscall.ECONNRESET:
					return errKindBackend, "connection reset by backend"
				}
			}
		}
		// net.OpError without a typed syscall — check for i/o timeout
		// vs other transport-layer issues by string.
		if opErr.Timeout() {
			if opErr.Op == "dial" {
				return errKindNetworkPath, "connection attempt timed out"
			}
			return errKindBackend, "backend response timed out"
		}
		return errKindOther, "transport error: " + opErr.Err.Error()
	}

	// String-based fallbacks for go-redis wrapped errors that don't
	// unwrap cleanly to *net.OpError. Conservative — only classify
	// what we've actually observed.
	switch {
	case strings.Contains(msg, "no route to host"):
		return errKindNetworkPath, "no route to host (network change?)"
	case strings.Contains(msg, "network is unreachable"):
		return errKindNetworkPath, "network unreachable"
	case strings.Contains(msg, "i/o timeout"):
		// Without operation context we can't be certain; lean
		// toward network for safety (auto-offline is the more
		// conservative action when the cause is ambiguous).
		return errKindNetworkPath, "i/o timeout"
	case strings.Contains(msg, "connection refused"):
		return errKindBackend, "connection refused (backend not listening)"
	}
	return errKindOther, msg
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// luaScript is the JuiceFS Redis metadata tree pull script.
// It SCANs juicefs directory-entry keys (d{inode}), builds an
// inode→path reverse map, resolves full paths, and fetches inode
// attributes (i{inode}) for mtime and size.
//
// QA-34 (2026-05-25): MATCH pattern tightened from 'd*' to 'd[0-9]*'.
// The previous pattern also matched juicefs system keys that start
// with 'd' followed by a letter — `delfiles` (LIST) and `delSlices`
// (LIST). HGETALL on a LIST returns WRONGTYPE, which we observed in
// production as:
//
//	reconciliation failed: redis EVAL: WRONGTYPE Operation against a
//	key holding the wrong kind of value script: …, on @user_script:9
//
// The error surfaces only when juicefs has accumulated del* entries
// (post-delete cleanup pending) — typically after sustained writes,
// which is exactly the QA-34 reproducer. Directory entries are
// d{numeric-inode}; the d[0-9]* MATCH precisely targets those.
//
// Code-review note (HIGH-1): considered also adding a TYPE check per
// key before HGETALL as defense-in-depth, but at 132K entries that
// adds ~130ms of Redis-blocking per reconcile cycle. The MATCH glob
// alone is sufficient for the known juicefs schema; if juicefs ever
// adds new d{number} non-hash key types, this WRONGTYPE will return
// and we'll add the TYPE check then with eyes open. Don't pre-pay
// the cost for a hypothetical future schema change.
// luaScanBatch processes ONE SCAN batch (not the whole tree). The previous
// single EVAL ran a full `repeat SCAN ... until cursor=='0'` over all ~200k
// `d[0-9]*` keys + path reconstruction atomically, leaving single-threaded
// Redis BUSY for ~4.6s and rejecting every concurrent command — which aborted
// in-flight Finder copies with error -36 (2026-06-14, the T2 flat large-file
// copy: "BUSY Redis is busy running a script" coincided exactly with the
// abort). The Go caller (syncMetadata) now drives the SCAN cursor loop, so
// Redis yields between batches and a concurrent copy's ops keep getting served.
// Path reconstruction moved to Go; the binary-attr parsing (gi/u32/u64) is
// UNCHANGED and stays here.
//
// ARGV[1] = SCAN cursor, ARGV[2] = COUNT. Returns a flat list whose FIRST
// element is the next cursor, followed by one
// "fileType:mtime:fileSize:inode:parentInode:name" string per child entry
// (parentInode, not a path — Go reconstructs the path from the reverse map).
const luaScanBatch = `
local function gi(v) if #v~=9 then return nil end
local a,b,c,d=string.byte(v,6,9) return a*16777216+b*65536+c*256+d end
local function u32(s,o) local a,b,c,d=string.byte(s,o,o+3) return a*16777216+b*65536+c*256+d end
local function u64(s,o) return u32(s,o)*4294967296+u32(s,o+4) end
local r=redis.call('SCAN',ARGV[1],'MATCH','d[0-9]*','COUNT',ARGV[2])
local out={r[1]}
for _,key in ipairs(r[2]) do local pi=string.sub(key,2)
local ent=redis.call('HGETALL',key)
for i=1,#ent,2 do local nm=ent[i] local val=ent[i+1]
local ci=gi(val) local ft=string.byte(val,1)
if ci then
local mt=0 local sz=0
local attr=redis.call('GET','i'..tostring(ci))
if attr and #attr>=59 then mt=u64(attr,24) sz=u64(attr,52) end
table.insert(out,tostring(ft)..':'..tostring(mt)..':'..tostring(sz)..':'..tostring(ci)..':'..pi..':'..nm)
end
end end
return out
`

// syncMetadata performs a full batch reconciliation: Lua tree pull → SQLite sync.
//
// Single-flighted by syncMu: the periodic reconcileLoop and the keyspace
// gap-fill can both reach here, but never concurrently — preserving the
// pruneAbsent single-writer invariant and preventing two overlapping full
// SCANs from racing each other's diff.
func (rc *RedisClient) syncMetadata() (err error) {
	rc.syncMu.Lock()
	defer rc.syncMu.Unlock()

	start := time.Now()
	// Record sync START so the flap debounce (reconcileLoop) can tell a sync is
	// in-flight or just-ran and suppress redundant network-change triggers.
	rc.mu.Lock()
	rc.lastSyncStartedAt = start
	// U6 review fix: reset progress in the same critical section that flips
	// IsSyncing()=true, so a concurrent /activity poll can never see the
	// previous sync's final count against an active rebuild ("99%" flash).
	rc.syncScanned.Store(0)
	rc.mu.Unlock()

	// G7 (task #80): record the attempt's outcome on EVERY exit — success,
	// failure, or deferral — so IsSyncing() turns false the moment this
	// attempt ends (named return; runs before the syncMu unlock above).
	defer func() { rc.noteSyncOutcome(err) }()

	// G7 (task #80): class-gated SCAN budget (was a fixed 120s; LAN/WiFi are
	// byte-identical, tunnel/cellular gets 300s so a ~200-300s slow-link SCAN
	// can actually finish). Env override JM_SCAN_TIMEOUT_SEC.
	scanBudget := scanContextTimeout()
	ctx, cancel := context.WithTimeout(context.Background(), scanBudget)
	defer cancel()

	// Drive the SCAN cursor loop from Go so Redis only blocks for one small
	// batch at a time (see luaScanBatch). Collect raw child entries — each
	// carries its parent INODE, not yet a path — plus a reverse map, then
	// reconstruct paths in Go. scanCount caps keys per SCAN; keep it modest so
	// no single batch (incl. a big dir's HGETALL + per-child attr GETs) keeps
	// Redis BUSY long enough to starve a concurrent copy.
	const scanCount = "500"
	type rawEntry struct {
		ft          int
		mtime, size int64
		inode       uint64
		parentInode string
		name        string
	}
	var raws []rawEntry
	rev := make(map[string][2]string) // childInode → {parentInode, name}
	cursor := "0"
	for {
		res, err := rc.redisDB().Eval(ctx, luaScanBatch, nil, cursor, scanCount).StringSlice()
		if err != nil {
			// G7 (task #80): when OUR scan budget expired, make the returned
			// error uniformly match errors.Is(_, context.DeadlineExceeded)
			// regardless of how go-redis dressed the expiry (ctx sentinel vs
			// a net "i/o timeout" from the ctx-derived read deadline) — the
			// deferred-not-failed classification keys on that sentinel.
			if ctx.Err() == context.DeadlineExceeded {
				return fmt.Errorf("redis SCAN batch: %w (SCAN budget %v exceeded): %w",
					err, scanBudget, context.DeadlineExceeded)
			}
			return fmt.Errorf("redis SCAN batch: %w", err)
		}
		if len(res) == 0 {
			break
		}
		cursor = res[0]
		rc.syncScanned.Add(int64(len(res) - 1)) // U6 progress
		for _, raw := range res[1:] {
			// "fileType:mtime:fileSize:inode:parentInode:name"
			parts := strings.SplitN(raw, ":", 6)
			if len(parts) != 6 {
				continue
			}
			ft, _ := strconv.Atoi(parts[0])
			mt, _ := strconv.ParseInt(parts[1], 10, 64)
			sz, _ := strconv.ParseInt(parts[2], 10, 64)
			ino, _ := strconv.ParseUint(parts[3], 10, 64)
			raws = append(raws, rawEntry{ft: ft, mtime: mt, size: sz, inode: ino, parentInode: parts[4], name: parts[5]})
			rev[parts[3]] = [2]string{parts[4], parts[5]}
		}
		if cursor == "0" {
			break
		}
	}

	// Reconstruct each entry's path by walking parentInode → root ("1") via the
	// reverse map, bounded to 50 levels (ported verbatim from the old Lua).
	// Entries that don't resolve to root (orphans / mid-scan races) are dropped,
	// exactly as before.
	redisEntries := make([]*Entry, 0, len(raws))
	redisPaths := make(map[string]struct{}, len(raws))
	for _, e := range raws {
		parts := []string{e.name}
		cur := e.parentInode
		for d := 0; d < 50; d++ {
			if cur == "1" {
				break
			}
			mp, ok := rev[cur]
			if !ok {
				break
			}
			cur = mp[0]
			parts = append([]string{mp[1]}, parts...)
		}
		if cur != "1" && e.parentInode != "1" {
			continue
		}
		entryPath := strings.Join(parts, "/")

		// #78 follow-up (proven live 2026-07-02): `.juicemount/` IS
		// SCAN-visible (unlike `.trash/`, which the root-walk excludes
		// structurally), so without this gate the SCAN's upsert path
		// resurrects ~17k internal-namespace rows every cycle and the
		// open-time GC deletes them again next launch — a permanent churn
		// loop. Filter here, at construction: the entry stays out of BOTH
		// redisEntries (no upsert) and redisPaths (so the absent-tracking
		// exclusion in trackAbsentPaths stays consistent — filtered paths
		// are absent from both sides and tracked nowhere).
		if scanFilteredPath(entryPath) {
			continue
		}

		var mtime time.Time
		if e.mtime > 0 {
			mtime = time.Unix(e.mtime, 0)
		}

		// Classify via the shared single-source-of-truth mapper (keyspace.go)
		// so the full SCAN decodes ft==3 (symlink) as os.ModeSymlink, not a
		// 0644 regular file. Without this, a reconcile of a dir containing a
		// symlink would UPSERT a regular-file Entry over the locally-minted
		// ModeSymlink cache entry and silently degrade it. ft==2 still → ModeDir.
		mode, isDir := modeForFiletype(byte(e.ft))

		redisEntries = append(redisEntries, &Entry{
			Path:       entryPath,
			Name:       path.Base(entryPath),
			ParentPath: path.Dir(entryPath),
			IsDir:      isDir,
			Size:       e.size,
			Mtime:      mtime,
			Inode:      e.inode,
			Mode:       mode,
		})
		redisPaths[entryPath] = struct{}{}
	}

	// Incremental upsert: only insert entries that are new or changed.
	// Comparing with the in-memory cache avoids rewriting all 131K entries
	// to SQLite on every sync cycle (reduces ~6s to <500ms steady-state).
	var toUpsert []*Entry
	for _, e := range redisEntries {
		existing := rc.store.LookupByPath(e.Path)
		if existing == nil ||
			existing.Mtime.Unix() != e.Mtime.Unix() ||
			existing.Size != e.Size ||
			existing.Inode != e.Inode {
			toUpsert = append(toUpsert, e)
		}
	}

	if len(toUpsert) > 0 {
		if err := rc.store.BulkInsert(toUpsert, 500); err != nil {
			return fmt.Errorf("bulk insert: %w", err)
		}
	}

	// Clear local_only flag only for entries that are actually local_only.
	// No need to update all 131K entries — typically only 0-10 are local_only.
	localOnly, _ := rc.store.LocalOnlyEntries()
	if len(localOnly) > 0 {
		var clearPaths []string
		for _, e := range localOnly {
			if _, inRedis := redisPaths[e.Path]; inRedis {
				clearPaths = append(clearPaths, e.Path)
			}
		}
		if len(clearPaths) > 0 {
			if err := rc.store.BulkClearLocalOnly(clearPaths); err != nil {
				return fmt.Errorf("bulk clear local_only: %w", err)
			}
		}
	}

	// Prune entries absent from Redis for PruneThreshold+ consecutive cycles.
	// A single-cycle absence (transient scan miss) does NOT delete the entry —
	// it must be absent PruneThreshold times in a row before removal fires.
	existingPaths, err := rc.store.AllPaths()
	if err != nil {
		return fmt.Errorf("all paths: %w", err)
	}

	// pruneAbsent is single-writer (only syncMetadata mutates it, and
	// syncMetadata is single-flighted via syncNowCh + the periodic loop).
	// External readers (Stats pollers, etc.) take rc.mu.RLock for OTHER
	// fields (lastSyncDuration, lastSyncTime, etc.) — they never touch
	// pruneAbsent. So holding rc.mu around the 131K-entry iteration was
	// unnecessary serialization that made Stats pollers wait tens of
	// milliseconds per sync. Dropped.
	//
	// If a second writer of pruneAbsent ever appears, this must take the
	// lock again. The map type itself isn't goroutine-safe.
	//
	// Reconciliation-recovery handling: if Redis was unstable in the
	// last 60s (i.e., we just recovered from a failure streak), the
	// path set we read could be stale or partial — Redis itself may be
	// catching up after coming back online, or the scan could be racing
	// JuiceFS's own metadata rehydration. Bumping pruneAbsent counters
	// on that partial view would mark genuinely-present files as
	// progressing-toward-deletion. Skip the increment entirely on these
	// recovery cycles; just clear counters for paths that ARE in Redis.
	// The pruneThreshold counter ladder resumes fresh on the next stable
	// cycle. This pairs with the phantom-purge gate in nfs/handler.go
	// (commit bbc6bff) to give recovery a full pruneThreshold-cycle
	// window before any destructive cache mutation can fire.
	skipIncrement := rc.RecentlyDegraded(60 * time.Second)
	rc.trackAbsentPaths(existingPaths, redisPaths, skipIncrement)
	// Collect paths that have been absent long enough to prune.
	//
	// V2.3 G6: ladderCounts snapshots each candidate's counter BEFORE the
	// delete below erases it, so Layer A can re-insert budget-deferred
	// candidates at their prior count (>= PruneThreshold) — preserving ladder
	// position exactly: a deferred candidate re-qualifies on the very next
	// cycle instead of restarting the 10-cycle climb.
	var toDelete []string
	ladderCounts := make(map[string]int)
	// Review fix (G6/U1 adversarial review, HIGH): qualification is gated on
	// !skipIncrement. Pre-G6 this gate was implicit — no counter could
	// survive a cycle boundary at >= PruneThreshold (the loop drained every
	// qualifier; increments are skipped while RecentlyDegraded), so a
	// degraded/recovery cycle was structurally incapable of prune progress
	// (the invariant documented above; same rule gates the G5 fast-path).
	// G6's deferral re-inserts candidates AT >= threshold, so without this
	// gate a backend flap right after a deferring cycle would pull them into
	// toDelete during the recovery window — where a rehydrating JuiceFS can
	// return false ENOENT to Layer A → live entries pruned → ESTALE (QA-30
	// class). Deferred candidates wait out the window at their preserved
	// count and re-qualify on the next stable cycle.
	if !skipIncrement {
		for p, count := range rc.pruneAbsent {
			if count >= PruneThreshold {
				// `._` AppleDouble guard (symmetric with scopedPrune's `._`-skip):
				// `._` sidecars are scan-filtered / managed Mac-side via the explicit
				// Remove path, never the reconcile — their absence from Redis is not a
				// delete signal. Pruning one whose path-stable Track-B handle the
				// kernel still holds Forgets it → FromHandle STALE (the `._dirN`
				// had_shadow STALE the release battery caught). Stop tracking it and
				// never enqueue it for deletion.
				if strings.HasPrefix(path.Base(p), "._") {
					delete(rc.pruneAbsent, p)
					continue
				}
				toDelete = append(toDelete, p)
				ladderCounts[p] = count
				delete(rc.pruneAbsent, p)
			}
		}
	}

	// === V2.3 G5: mass-delete convergence fast-path (task #73) ===
	// The counter ladder alone cannot converge a mass deletion:
	// PruneThreshold(10) consecutive cycles × the 15m LAN backstop ≥ 2.5h,
	// and pruneAbsent is in-memory — every app restart (and every
	// RecentlyDegraded window) resets ALL counters, so a reorganized library
	// lingers as a ghost tree indefinitely (live 2026-07-01: 125k pending,
	// an entire deleted SFX tree still served, files erroring on open).
	//
	// A deletion is safe to confirm WITHOUT the ladder when two independent
	// authorities agree on the SAME cycle: the path is absent from this
	// cycle's Redis SCAN (authoritative metadata) AND its subtree ROOT is
	// Lstat-ENOENT on an identity-verified FUSE mount (authoritative
	// filesystem). Confirmation happens at the subtree-root level — a path
	// whose parent is also absent needs no probe of its own — so a
	// 120k-entry deleted tree costs a handful of Lstats, not 120k.
	//
	// Fast-pathed paths still pass Layer D (spool-pending) and Layer C
	// (pinned) below; they skip only Layer A's per-path Lstat, which their
	// root probe already answered. Bounded three ways (root count, wall
	// time, rows per cycle); when the row cap truncates, TriggerSync
	// schedules the next cycle immediately so full convergence takes
	// minutes, not backstop-hours. Kill switch: JM_PRUNE_FASTPATH=0.
	// Same stability rule as the ladder: skipped entirely on
	// RecentlyDegraded cycles (a partial Redis view must never confirm a
	// deletion), and the G0 FUSE-identity gate must pass.
	fastConfirmed, fastCapped := rc.collectFastPathPrunes(skipIncrement)
	for p := range fastConfirmed {
		toDelete = append(toDelete, p)
	}
	// (Review fix: the capped follow-up is scheduled AFTER DeletePaths below,
	// and only when fast-confirmed paths actually survived the filters —
	// otherwise a fully-spared >cap backlog (all pinned/spool-pending) or a
	// zero-progress timeout cycle would chain back-to-back full SCANs.)

	// === QA-30 Layer D (review FIX 2): never prune a spool-pending path ===
	// The periodic full-SCAN prune is the SAME ESTALE bug class as scopedPrune,
	// reached via the backstop SCAN instead of a keyspace push. A spool-pending
	// file is absent from Redis (not yet drained) AND absent from FUSE (only on
	// the spool), so Layer A (FUSE Lstat) returns isAbsent=true and does NOT
	// spare it, and Layer C (pin) misses it. A large file draining at ~5 MB/s
	// can stay absent for >= PruneThreshold (10) SCAN cycles → pruned →
	// handles.Forget → identical ESTALE/100070 mid-copy. Filter the guard FIRST
	// (before pin/FUSE) because the index probe is an O(1) in-memory lookup,
	// cheaper than a FUSE round-trip — symmetric with scopedPrune's filter (same
	// shared helper). Uses the race-clean accessor (review FIX 1).
	toDelete, prunedSpoolPending := rc.filterSpoolPending(toDelete)
	if prunedSpoolPending > 0 {
		jmlog.Info("metadata sync: spared spool-pending paths (Layer D)",
			"count", prunedSpoolPending)
	}

	// QA-30 (2026-05-25): two-layer protection against pruning still-valid
	// files. Layer C (pin guard) runs first because it's authoritative:
	// pinning is the user's explicit contract that the file remain offline-
	// accessible. Layer A (FUSE Lstat verification) catches everything else
	// — paths Redis SCAN missed but FUSE confirms are present. Together
	// they close the "transient SCAN gap causes ESTALE on cached media"
	// bug class (observed: DaVinci treating fully-cached files as offline).
	//
	// Both layers fail-safe: any unrecoverable error skips the entire prune
	// pass for this cycle. The cost of a delayed prune is at most one
	// cycle of stale entries; the cost of an incorrect prune is the bug
	// we're closing.
	prunedPinned := 0
	prunedFUSEpresent := 0
	// === V2.3 G0: FUSE identity gate — skip the whole prune pass when the
	// mountpoint has no real filesystem on it (macFUSE kext not loaded /
	// mount absent / wedged). In that state Layer A's FUSE Lstat returns
	// ENOENT for EVERY backend path, so its spare-if-present protection is
	// silently disabled and the prune runs unprotected. Same fail-safe shape
	// as the pin-checker error path below: a delayed prune costs one cycle
	// of stale entries; an unprotected prune is the ESTALE bug class.
	if len(toDelete) > 0 {
		if identOK, identReason := pin.FUSEIdentityState(); !identOK {
			jmlog.Warn("metadata sync: FUSE identity gate failed — skipping prune pass this cycle",
				"reason", identReason,
				"would_have_pruned", len(toDelete),
			)
			toDelete = nil
		}
	}
	if len(toDelete) > 0 {
		// === Layer C: filter out pinned paths ===
		pinned, perr := rc.store.pinnedSetPublic()
		if perr != nil {
			jmlog.Warn("metadata sync: pin-checker error, skipping prune this cycle",
				"error", perr.Error(),
				"would_have_pruned", len(toDelete),
			)
			toDelete = nil
		} else if len(pinned) > 0 {
			// Pin store keys on the mountpoint-prefixed path; metadata
			// entries key on the JuiceFS-internal path. Normalize the
			// pinned set to the internal form once, then check by lookup.
			pinnedInternal := make(map[string]struct{}, len(pinned))
			for mp := range pinned {
				pinnedInternal[rc.internalFromMounted(mp)] = struct{}{}
			}
			filtered := toDelete[:0]
			for _, p := range toDelete {
				if _, ok := pinnedInternal[p]; ok {
					prunedPinned++
					continue
				}
				filtered = append(filtered, p)
			}
			toDelete = filtered
		}

		// === Layer A: per-path FUSE Lstat verification ===
		// Extracted to verifyPruneCandidates (V2.3 G6, task #79) so the
		// probe-cap / wall-budget / class-gate bounding is unit-testable
		// without a full syncMetadata run. See the method's doc comment.
		if len(toDelete) > 0 && rc.fuseRoot != "" {
			toDelete, prunedFUSEpresent = rc.verifyPruneCandidates(toDelete, fastConfirmed, ladderCounts)
		}
	}

	if len(toDelete) > 0 {
		if err := rc.store.DeletePaths(toDelete); err != nil {
			return fmt.Errorf("delete pruned: %w", err)
		}
	}

	// Review fix: schedule the fast-path continuation only when capped AND
	// fast-confirmed paths actually survived the filters into the executed
	// delete — a fully-spared backlog (all pinned/spool-pending) or a
	// zero-progress probe cycle must fall back to the periodic backstop, not
	// chain back-to-back full SCANs. pruneFollowUp makes the follow-up bypass
	// the keyspace-push deferral and flap debounce in reconcileLoop: it is a
	// self-scheduled continuation of an already-authoritative SCAN, not a
	// network-flap trigger (without it the accelerator is inert in the
	// deployed push-enabled config).
	if fastCapped && len(fastConfirmed) > 0 {
		survived := false
		for _, p := range toDelete {
			if _, ok := fastConfirmed[p]; ok {
				survived = true
				break
			}
		}
		if survived {
			rc.pruneFollowUp.Store(true)
			rc.TriggerSync()
		}
	}

	duration := time.Since(start)
	rc.mu.Lock()
	rc.lastSyncDuration = duration
	rc.lastSyncTime = time.Now()
	rc.lastSyncEntries = len(redisEntries)
	pendingPrune := len(rc.pruneAbsent)
	rc.mu.Unlock()

	jmlog.Info("metadata sync complete",
		"entries", len(redisEntries),
		"upserted", len(toUpsert),
		"pruned", len(toDelete),
		"pinned_skipped", prunedPinned, // QA-30 Layer C: pinned paths spared
		"fuse_present_skipped", prunedFUSEpresent, // QA-30 Layer A: FUSE-confirmed present
		"spool_pending_skipped", prunedSpoolPending, // QA-30 Layer D: spool-pending paths spared
		"pending_prune", pendingPrune,
		"duration_ms", duration.Round(time.Millisecond).Milliseconds(),
	)
	return nil
}

// trackAbsentPaths advances the pruneAbsent counter ladder from one full-SCAN
// cycle's view (extracted from syncMetadata for task #78 so the tracking
// policy is unit-testable).
//
// Task #78: paths STRICTLY UNDER scan-filtered namespaces (.trash/…,
// .juicemount/… — see scanFilteredDescendant) are NEVER tracked. The SCAN
// structurally cannot return them, so "absent from the SCAN" carries zero
// delete signal for them; tracking them created a permanent ~112k
// pending_prune floor (live-probed 2026-07-02), inflated every diff
// iteration, and fueled the pre-G6 Layer-A probe storms. The cleanup loop
// also actively drops any filtered-namespace counter (belt-and-braces —
// pruneAbsent is in-memory, so restart already clears pre-fix counters).
//
// Batch-3 adversarial review #5: the BARE ".trash"/".juicemount" dir rows
// (spared by the open-GC) ARE tracked — scanFilteredDescendant, not
// scanFilteredPath. While the namespace exists on FUSE, the Layer-A Lstat
// probe spares them every cycle (2 bounded probes, negligible); if the
// backend namespace is ever genuinely removed, this is the ONLY path that
// can prune the ghost dir row. The `._` AppleDouble guard stays where it
// was: at qualification time in syncMetadata (sidecars ARE tracked but
// never pruned).
func (rc *RedisClient) trackAbsentPaths(existingPaths, redisPaths map[string]struct{}, skipIncrement bool) {
	for p := range existingPaths {
		if _, inRedis := redisPaths[p]; !inRedis {
			if !skipIncrement && !scanFilteredDescendant(p) {
				rc.pruneAbsent[p]++
			}
		} else {
			delete(rc.pruneAbsent, p)
		}
	}
	// Remove stale entries from pruneAbsent: paths already deleted from
	// SQLite, and (task #78) scan-filtered-namespace-descendant counters.
	for p := range rc.pruneAbsent {
		if _, exists := existingPaths[p]; !exists || scanFilteredDescendant(p) {
			delete(rc.pruneAbsent, p)
		}
	}
}

// verifyPruneCandidates is the prune ladder's Layer A: per-path FUSE Lstat
// verification (QA-30), extracted from syncMetadata by V2.3 G6 (task #79) so
// its bounding is unit-testable. Even non-pinned paths shouldn't be pruned if
// FUSE still shows them present — that means JuiceFS still has them, Redis
// SCAN just happened to miss them this cycle. Each Lstat capped at 1s so a
// FUSE wedge can't pin the reconcile goroutine. If more than 25% of probes
// time out, treat FUSE as degraded and skip the whole prune (defensive —
// degraded FUSE plus blind prune was what surfaced the bug in production).
//
// V2.3 G6 bounding (task #79, proven live 2026-07-02): the loop is bounded
// the same way as collectFastPathPrunes — layerAProbeCap probes / a
// layerAProbeBudget wall clock. Without bounds, a large permanent candidate
// backlog (~112k from a SCAN-coverage bug) ran 86s on LAN and ~90 min over a
// cellular relay, saturating the link. Once EITHER bound trips, every
// remaining unprobed LADDER candidate is deferred: NOT pruned this cycle and
// re-inserted into rc.pruneAbsent at its prior counter (from ladderCounts, or
// PruneThreshold if unknown), so it keeps its ladder position and re-qualifies
// on the very next cycle. fastConfirmed entries are exempt from all gates —
// their subtree root was FUSE-confirmed this same cycle (G5), so re-Lstat'ing
// 120k descendants individually is exactly what root-level confirmation
// exists to avoid.
//
// Class gate: on the metered/tunnel band (currentLinkClass() == classTunnel:
// utun*/tailscale0/JM_WAN_MODE=1 — same accessor as the keyspace backstop and
// coalescer gating) ladder candidates are NEVER probed — each Lstat costs an
// NFS/FUSE round-trip over the metered link (~50ms each observed on the
// cellular relay). All ladder candidates are deferred; deferral is the
// fail-safe direction (a delayed prune costs a cycle of stale entries, an
// unverified prune is the ESTALE bug class). fastConfirmed entries still
// prune: they were root-verified already.
//
// Kill switch: JM_LAYERA_BUDGET=0 (read per call, like JM_PRUNE_FASTPATH)
// restores the pre-G6 unbounded, un-gated behavior exactly.
//
// Caller contract: single-writer — runs on syncMetadata's goroutine only
// (rc.pruneAbsent mutation). rc.fuseRoot must be non-empty. Returns the
// verified (prunable) subset — nil when the FUSE-degraded bail trips — plus
// the count of FUSE-present spared paths.
func (rc *RedisClient) verifyPruneCandidates(toDelete []string, fastConfirmed map[string]struct{}, ladderCounts map[string]int) (prunable []string, fusePresent int) {
	unbounded := os.Getenv("JM_LAYERA_BUDGET") == "0"
	deferClass := !unbounded && currentLinkClass() == classTunnel
	if rc.pruneAbsent == nil {
		rc.pruneAbsent = make(map[string]int)
	}
	verified := toDelete[:0]
	lstatTimeouts, probed, deferred := 0, 0, 0
	probeStart := time.Now()
	for _, p := range toDelete {
		// V2.3 G5: fast-path entries were already FUSE-confirmed at
		// their subtree root this same cycle.
		if _, ok := fastConfirmed[p]; ok {
			verified = append(verified, p)
			continue
		}
		// V2.3 G6: defer the remaining ladder candidates once the class gate
		// or either budget bound trips. Re-insert at the prior counter so the
		// ladder position survives (see doc comment).
		if deferClass || (!unbounded &&
			(probed >= layerAProbeCap || time.Since(probeStart) > layerAProbeBudget)) {
			deferred++
			count, ok := ladderCounts[p]
			if !ok {
				count = PruneThreshold
			}
			rc.pruneAbsent[p] = count
			continue
		}
		fusePath := rc.fusePathFor(p)
		if fusePath == "" {
			verified = append(verified, p)
			continue
		}
		probed++
		isAbsent, ok := lstatNotExistWithTimeout(fusePath, time.Second)
		if !ok {
			lstatTimeouts++
			// Review fix: a timed-out candidate keeps its ladder position
			// (same re-insertion as the defer branch) instead of restarting
			// the 10-cycle climb — timeout is FUSE slowness, not evidence
			// about the path.
			count, ok2 := ladderCounts[p]
			if !ok2 {
				count = PruneThreshold
			}
			rc.pruneAbsent[p] = count
			continue
		}
		if isAbsent {
			verified = append(verified, p)
		} else {
			fusePresent++ // FUSE says it's there — keep it
		}
	}
	if deferred > 0 {
		reason := "budget"
		if deferClass {
			reason = "class_gate"
		}
		jmlog.Info("metadata sync: Layer-A prune verification bounded — deferring unprobed candidates (G6)",
			"reason", reason,
			"class", currentLinkClass().String(),
			"probed", probed,
			"verified", len(verified),
			"deferred", deferred,
			"elapsed_ms", time.Since(probeStart).Round(time.Millisecond).Milliseconds(),
		)
	}
	// QA-30 code review HIGH-3: absolute floor of timeouts before bailing
	// the whole cycle. Without this, a small batch (N<=3) trips the bail on
	// a single transient timeout — effectively a 100%/50%/33% threshold
	// instead of the intended 25%. Small batches handle individual timeouts
	// fine via the `continue` above (paths simply stay unpruned this cycle,
	// retried next time).
	//
	// Review fixes (G6/U1 adversarial review): the ratio denominator is
	// `probed`, not len(toDelete) — the budget caps probes at 2048 and the
	// list also contains deferred + fastConfirmed entries, so the old
	// denominator made the bail unreachable on exactly the large backlogs it
	// protects against. And under the 3s budget at most 3 serial 1s
	// timeouts can occur before deferral kicks in, so the floor is 3 when
	// budgeted (4 unbounded, preserving QA-30's original constant there).
	bailFloor := 4
	if !unbounded {
		bailFloor = 3
	}
	if lstatTimeouts >= bailFloor && lstatTimeouts*4 > probed {
		jmlog.Warn("metadata sync: FUSE degraded (>25% Lstat timeouts), skipping prune this cycle",
			"timeouts", lstatTimeouts,
			"total_probes", probed,
		)
		return nil, fusePresent
	}
	return verified, fusePresent
}

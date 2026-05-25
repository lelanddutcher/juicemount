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

	"github.com/redis/go-redis/v9"

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
)

// MetadataEvent represents a real-time metadata change published via Redis SUBSCRIBE.
type MetadataEvent struct {
	Op    string `json:"op"`    // "create", "update", "rename", "delete"
	Path  string `json:"path"`
	Size  int64  `json:"size,omitempty"`
	Mtime int64  `json:"mtime,omitempty"`
	Inode uint64 `json:"inode,omitempty"`
	IsDir bool   `json:"is_dir,omitempty"`

	// For rename operations
	OldPath string `json:"old_path,omitempty"`
}

// RedisClient manages Redis connections for metadata sync.
// It provides two sync mechanisms:
//  1. SUBSCRIBE: event-driven, <100ms propagation for real-time changes
//  2. Batch reconciliation: periodic full tree pull, catches anything SUBSCRIBE missed
type RedisClient struct {
	rdb      *redis.Client
	redisURL string // stored for reconnect
	store    *Store

	reconcileInterval time.Duration
	stopCh            chan struct{}
	stopOnce          sync.Once
	syncNowCh         chan struct{} // signals reconcileLoop to run immediately

	mu               sync.RWMutex
	lastSyncDuration time.Duration
	lastSyncTime     time.Time
	lastSyncEntries  int
	connected        bool
	lastDisconnect   time.Time
	lastReconnect    time.Time

	// pruneAbsent tracks how many consecutive reconciliation cycles each path
	// has been absent from Redis. Only paths absent for PruneThreshold+ cycles
	// are actually deleted from SQLite. Guarded by mu.
	pruneAbsent map[string]int

	// QA-30 (2026-05-25): path conversion config so syncMetadata can
	// correctly cross-reference the pin store (mountpoint-prefixed paths)
	// against metadata.Store entries (JuiceFS-internal paths, no prefix)
	// and probe FUSE for prune verification. Set once at startup via
	// SetPathConfig; reads thereafter are unsynchronized but safe because
	// they happen on the single reconcile goroutine.
	mountPoint string
	fuseRoot   string
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

	return &RedisClient{
		rdb:               rdb,
		redisURL:          redisURL,
		store:             store,
		reconcileInterval: DefaultReconcileInterval,
		stopCh:            make(chan struct{}),
		syncNowCh:         make(chan struct{}, 1),
		connected:         true,
		pruneAbsent:       make(map[string]int),
	}, nil
}

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
	rc.rdb.Close()

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

	rc.rdb = rdb
	rc.mu.Lock()
	rc.connected = true
	rc.lastReconnect = time.Now()
	rc.mu.Unlock()

	jmlog.Info("redis reconnected")
	return nil
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

// SyncOnce performs a single batch reconciliation (Lua tree pull → SQLite).
func (rc *RedisClient) SyncOnce() error {
	return rc.syncMetadata()
}

// Start begins both the SUBSCRIBE listener and periodic batch reconciliation.
func (rc *RedisClient) Start() {
	go rc.subscribeLoop()
	go rc.reconcileLoop()
}

// Stop halts all background goroutines and closes the Redis connection.
func (rc *RedisClient) Stop() {
	rc.stopOnce.Do(func() {
		close(rc.stopCh)
		rc.rdb.Close()
	})
}

// PublishEvent sends a metadata event to all subscribed clients.
func (rc *RedisClient) PublishEvent(ctx context.Context, evt MetadataEvent) error {
	data, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	return rc.rdb.Publish(ctx, SubscribeChannel, string(data)).Err()
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

	sub := rc.rdb.Subscribe(ctx, SubscribeChannel)
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
	var mode fs.FileMode = 0644
	if evt.IsDir {
		mode = 0755 | fs.ModeDir
	}

	switch evt.Op {
	case "create", "update":
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
		rc.store.DeleteFromCache(evt.Path)
		if err := rc.store.Delete(evt.Path); err != nil {
			log.Printf("subscribe apply delete: %v", err)
		}

	case "rename":
		if evt.OldPath != "" {
			rc.store.DeleteFromCache(evt.OldPath)
			rc.store.Delete(evt.OldPath)
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

// reconcileLoop runs periodic batch reconciliation with exponential backoff
// on failure (network transitions, Redis unreachable). It also listens for
// immediate sync requests via syncNowCh (triggered by network changes).
func (rc *RedisClient) reconcileLoop() {
	baseInterval := rc.reconcileInterval
	backoff := baseInterval
	maxBackoff := 5 * time.Minute
	consecutiveFailures := 0

	ticker := time.NewTicker(backoff)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rc.doReconcile(&consecutiveFailures, &backoff, baseInterval, maxBackoff, ticker)
		case <-rc.syncNowCh:
			jmlog.Info("reconciliation triggered by network change")
			rc.doReconcile(&consecutiveFailures, &backoff, baseInterval, maxBackoff, ticker)
		case <-rc.stopCh:
			return
		}
	}
}

func (rc *RedisClient) doReconcile(consecutiveFailures *int, backoff *time.Duration, baseInterval, maxBackoff time.Duration, ticker *time.Ticker) {
	if err := rc.syncMetadata(); err != nil {
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
		rc.mu.Unlock()
		if *consecutiveFailures > 0 {
			jmlog.Info("reconciliation recovered", "previous_failures", *consecutiveFailures)
			*consecutiveFailures = 0
			*backoff = baseInterval
			ticker.Reset(*backoff)
		}
	}
}

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
// It SCANs all d* keys (directory entries), builds an inode→path reverse map,
// resolves full paths, and fetches inode attributes (i{inode}) for mtime and size.
const luaScript = `
local function gi(v) if #v~=9 then return nil end
local a,b,c,d=string.byte(v,6,9) return a*16777216+b*65536+c*256+d end
local function u32(s,o) local a,b,c,d=string.byte(s,o,o+3) return a*16777216+b*65536+c*256+d end
local function u64(s,o) return u32(s,o)*4294967296+u32(s,o+4) end
local cursor='0' local rev={} local all={}
repeat local r=redis.call('SCAN',cursor,'MATCH','d*','COUNT',1000) cursor=r[1]
for _,key in ipairs(r[2]) do local pi=string.sub(key,2)
local ent=redis.call('HGETALL',key)
for i=1,#ent,2 do local nm=ent[i] local val=ent[i+1]
local ci=gi(val) local ft=string.byte(val,1)
if ci then rev[tostring(ci)]=pi..'\t'..nm
table.insert(all,{inode=tostring(ci),parent=pi,name=nm,ft=ft}) end
end end until cursor=='0'
local results={}
for _,e in ipairs(all) do
local parts={e.name} local cur=e.parent
for d=1,50 do if cur=='1' then break end
local mp=rev[cur] if not mp then break end
local sp=string.find(mp,'\t',1,true)
cur=string.sub(mp,1,sp-1)
table.insert(parts,1,string.sub(mp,sp+1)) end
if cur=='1' or e.parent=='1' then
local path=table.concat(parts,'/')
local mt=0 local sz=0
local attr=redis.call('GET','i'..e.inode)
if attr and #attr>=59 then
mt=u64(attr,24)
sz=u64(attr,52)
end
table.insert(results,tostring(e.ft)..':'..tostring(mt)..':'..tostring(sz)..':'..e.inode..':'..path)
end
end
return results
`

// syncMetadata performs a full batch reconciliation: Lua tree pull → SQLite sync.
func (rc *RedisClient) syncMetadata() error {
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	result, err := rc.rdb.Eval(ctx, luaScript, nil).StringSlice()
	if err != nil {
		return fmt.Errorf("redis EVAL: %w", err)
	}

	// Parse entries from Lua output
	redisEntries := make([]*Entry, 0, len(result))
	redisPaths := make(map[string]struct{}, len(result))

	for _, raw := range result {
		// Format: "fileType:mtime:fileSize:inode:relative/path"
		parts := strings.SplitN(raw, ":", 5)
		if len(parts) != 5 || parts[4] == "" {
			continue
		}
		ft, _ := strconv.Atoi(parts[0])
		mtimeEpoch, _ := strconv.ParseInt(parts[1], 10, 64)
		fileSize, _ := strconv.ParseInt(parts[2], 10, 64)
		inode, _ := strconv.ParseUint(parts[3], 10, 64)
		entryPath := parts[4]

		isDir := ft == 2
		var mtime time.Time
		if mtimeEpoch > 0 {
			mtime = time.Unix(mtimeEpoch, 0)
		}

		var mode fs.FileMode = 0644
		if isDir {
			mode = 0755 | fs.ModeDir
		}

		redisEntries = append(redisEntries, &Entry{
			Path:       entryPath,
			Name:       path.Base(entryPath),
			ParentPath: path.Dir(entryPath),
			IsDir:      isDir,
			Size:       fileSize,
			Mtime:      mtime,
			Inode:      inode,
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
	for p := range existingPaths {
		if _, inRedis := redisPaths[p]; !inRedis {
			if !skipIncrement {
				rc.pruneAbsent[p]++
			}
		} else {
			delete(rc.pruneAbsent, p)
		}
	}
	// Remove stale entries from pruneAbsent (paths already deleted from SQLite).
	for p := range rc.pruneAbsent {
		if _, exists := existingPaths[p]; !exists {
			delete(rc.pruneAbsent, p)
		}
	}
	// Collect paths that have been absent long enough to prune.
	var toDelete []string
	for p, count := range rc.pruneAbsent {
		if count >= PruneThreshold {
			toDelete = append(toDelete, p)
			delete(rc.pruneAbsent, p)
		}
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
		// Even non-pinned paths shouldn't be pruned if FUSE still shows
		// them present — that means JuiceFS still has them, Redis SCAN
		// just happened to miss them this cycle. Each Lstat capped at 1s
		// so a FUSE wedge can't pin the reconcile goroutine. If more than
		// 25% of probes time out, treat FUSE as degraded and skip the
		// whole prune (defensive — degraded FUSE plus blind prune was
		// what surfaced the bug in production).
		if len(toDelete) > 0 && rc.fuseRoot != "" {
			verified := toDelete[:0]
			lstatTimeouts := 0
			for _, p := range toDelete {
				fusePath := rc.fusePathFor(p)
				if fusePath == "" {
					verified = append(verified, p)
					continue
				}
				isAbsent, ok := lstatNotExistWithTimeout(fusePath, time.Second)
				if !ok {
					lstatTimeouts++
					continue // timed out — don't prune, retry next cycle
				}
				if isAbsent {
					verified = append(verified, p)
				} else {
					prunedFUSEpresent++ // FUSE says it's there — keep it
				}
			}
			// QA-30 code review HIGH-3: absolute floor of 4 timeouts before
			// bailing the whole cycle. Without this, a small batch
			// (N<=3) trips the bail on a single transient timeout —
			// effectively a 100%/50%/33% threshold instead of the
			// intended 25%. Small batches handle individual timeouts
			// fine via the `continue` above (paths simply stay
			// unpruned this cycle, retried next time).
			if lstatTimeouts >= 4 && lstatTimeouts*4 > len(toDelete) {
				jmlog.Warn("metadata sync: FUSE degraded (>25% Lstat timeouts), skipping prune this cycle",
					"timeouts", lstatTimeouts,
					"total_probes", len(toDelete),
				)
				toDelete = nil
			} else {
				toDelete = verified
			}
		}
	}

	if len(toDelete) > 0 {
		if err := rc.store.DeletePaths(toDelete); err != nil {
			return fmt.Errorf("delete pruned: %w", err)
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
		"pinned_skipped", prunedPinned,        // QA-30 Layer C: pinned paths spared
		"fuse_present_skipped", prunedFUSEpresent, // QA-30 Layer A: FUSE-confirmed present
		"pending_prune", pendingPrune,
		"duration_ms", duration.Round(time.Millisecond).Milliseconds(),
	)
	return nil
}

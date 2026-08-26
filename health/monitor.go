// Package health provides periodic health monitoring and recovery triggers
// for the JuiceMount5 NFS loopback server.
package health

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

// Config holds the endpoints and paths that the monitor checks.
type Config struct {
	RedisURL      string // e.g. "127.0.0.1:6379"
	MinIOURL      string // e.g. "http://127.0.0.1:9000"
	FUSEPath      string // e.g. ~/.juicemount/fuse-internal
	NFSMountPoint string // e.g. /mnt/juice
}

// ComponentStatus represents the health of a single subsystem.
type ComponentStatus struct {
	Healthy   bool
	LastCheck time.Time
	Message   string
}

// ConnState represents a connection lifecycle state.
type ConnState string

const (
	ConnStateConnected    ConnState = "connected"
	ConnStateDisconnected ConnState = "disconnected"
	ConnStateReconnecting ConnState = "reconnecting"
)

// MemoryStats holds cache/pool/buffer sizes provided by the main application
// via SetStatsProvider.
type MemoryStats struct {
	PathCacheSize  int
	InodeCacheSize int
	FDPoolOpen     int
	FDPoolActive   int
	MemBufEntries  int
	MemBufSizeMB   float64
}

// HealthStatus is a snapshot of all component health states.
type HealthStatus struct {
	Redis ComponentStatus
	MinIO ComponentStatus
	FUSE  ComponentStatus
	NFS   ComponentStatus
	// Overall is true only when every component is healthy.
	Overall bool

	// Connection state tracking
	RedisConnState ConnState
	MinIOConnState ConnState

	// Runtime memory telemetry
	HeapAllocMB   float64 // current heap allocation in MB
	HeapSysMB     float64 // total heap obtained from OS in MB
	NumGC         uint32  // number of GC cycles
	GCPauseLastUs uint64  // last GC pause in microseconds

	// Application-level memory stats (populated via SetStatsProvider)
	PathCacheSize  int     // entries in metadata pathCache
	InodeCacheSize int     // entries in metadata inodeCache
	FDPoolOpen     int     // open file descriptors in pool
	FDPoolActive   int     // active (in-use) fds in pool
	MemBufEntries  int     // entries in memory buffer
	MemBufSizeMB   float64 // total memory buffer size in MB
}

// GracePeriod is the duration after a network change during which
// FUSE fallback errors are suppressed to allow connections to re-establish.
const GracePeriod = 5 * time.Second

// HealthMonitor runs periodic health checks and exposes the latest status.
type HealthMonitor struct {
	cfg           Config
	rdb           *redis.Client
	http          *http.Client
	netWatcher    *NetWatcher // optional: for grace period awareness
	statsProvider func() MemoryStats
	mu            sync.RWMutex
	status        HealthStatus
	cancel        context.CancelFunc
	done          chan struct{}

	// NFS auto-remount state. Guarded by mu.
	remountFn      func() error // optional: called to remount the NFS volume
	nfsStaleStreak int          // consecutive failed NFS checks
	lastRemountAt  time.Time    // last successful remount time

	// NFS-layer absent-mount recovery (#93 — see absentremount.go).
	// Guarded by mu. absentRemountFn is the PROMPT-CAPABLE two-tier mount
	// (the same machinery boot uses), distinct from remountFn (legacy,
	// passwordless-sudo-only). absentTicks is the consecutive-tick
	// threshold resolved at construction (env JM_NFS_AUTOREMOUNT_TICKS).
	absentState     nfsAbsentState
	absentRemountFn func() error
	nfsServerAddr   string
	absentTicks     int

	// #106: per-component "local-network-permission suspected" latches, so
	// the guidance WARN fires once per suspicion streak instead of every
	// 10s tick. Guarded by mu; lazily allocated.
	localNetSuspected map[string]bool

	// FUSE health debounce (anti-flap). The raw checkFUSE probe flips on
	// every backend-link blip; reporting that verbatim strobes the menu bar.
	// Require several consecutive unhealthy raw checks before flipping the
	// REPORTED FUSE state to degraded; recover on the first healthy check.
	// Guarded by mu.
	fuseRawUnhealthyStreak int
	fuseRawHealthyStreak   int
	fuseDebounced          ComponentStatus
	fuseDebouncedInit      bool

	// Throttle timestamp for wedge diagnostics (logWedgeDiagnostics).
	// Guarded by mu.
	lastWedgeDiagAt time.Time

	// drainProbe, if set, reports the drainer's live ingest state: the number
	// of in-flight drains and the elapsed time since the last COMPLETED drain.
	// Injected as a hook (not an nfs import) so package health stays free of an
	// nfs dependency — mirrors the reachability WithLivenessHook idiom. Wired
	// from bridge via globalDrainerAtomic. Nil when no spool/drainer is
	// configured (the pre-drainer no-op — no online busy-suppression). Guarded
	// by mu. See #89: an ONLINE analogue of the pin.IsOffline() readdir busy-
	// suppression, so a legitimate high-concurrency ingest (the drain's
	// synchronous MinIO PUTs saturating the FUSE request queue) is reported as
	// "busy", not the false "mount losing communication" degraded state.
	drainProbe func() (inFlight int64, sinceLastSuccess time.Duration)
}

// Tunables for NFS auto-remount. Exposed as vars so tests can override.
var (
	// NFSStaleThreshold is the number of consecutive failed NFS health
	// checks (10s apart) before triggering a remount WHEN THE JUICEFS PROCESS
	// TREE IS GONE. While juicefs is alive, the remount is deferred to the
	// FUSE watchdog (see handleNFSAutoRemount / health/fuse.go).
	NFSStaleThreshold = 3

	// NFSStaleHardRemountThreshold is the last-resort streak (10s/tick) after
	// which the NFS path remounts EVEN IF juicefs is still alive — a backstop
	// for a genuinely-stuck NFS layer that the FUSE watchdog's own escalation
	// (FUSEStaleEscalateTicks ≈ 90s) didn't clear. Set well beyond that window
	// so a transient backend blip (which recovers in seconds) never reaches
	// it. 18 ticks ≈ 180s.
	NFSStaleHardRemountThreshold = 18

	// NFSRemountCooldown is the minimum time between auto-remount
	// attempts. Prevents flapping if the underlying issue is persistent.
	NFSRemountCooldown = 60 * time.Second

	// NFSStatTimeout bounds an os.Stat() probe of the NFS mount point.
	// If the syscall hangs longer than this, the mount is considered stale.
	NFSStatTimeout = 5 * time.Second

	// FUSEFlapToDegraded is the number of consecutive unhealthy raw FUSE
	// probes (10s apart) required before the REPORTED FUSE status flips to
	// degraded. Anti-flap: a few-second backend-link blip stalls the FUSE
	// stat and would otherwise strobe the menu-bar indicator up/down every
	// tick. Recovery is immediate (one healthy probe). The watchdog in
	// health/fuse.go has its own, separate liveness logic — this only
	// smooths the reported UI state.
	FUSEFlapToDegraded = 3

	// DrainBusyRecentWindow is how recently the LAST drain must have completed
	// for a stat/readdir TIMEOUT to be treated as "busy (heavy ingest)" rather
	// than a mount fault (#89). InFlight>0 is the primary signal; this window
	// covers the brief lull between a burst's last completed PUT and the next
	// enqueue, so a stat that queued behind the just-finished bulk traffic
	// isn't misreported as degraded. Kept short so a genuinely-wedged mount
	// (no drain activity) still degrades on the very next tick. Exposed as a
	// var so tests can override.
	DrainBusyRecentWindow = 15 * time.Second

	// forceUnmountFn is the function used to force-unmount a stale mount.
	// Replaceable so tests can avoid invoking `sudo`.
	forceUnmountFn = forceUnmount

	// isJuiceFSProcessAliveFn reports whether the juicefs process for one exact
	// mount point is alive.
	// Indirected through a var (default: the real pgrep probe in fuse.go) so
	// the remount-decision tests are deterministic regardless of what's running
	// on the host. Without this, a developer's own running JuiceMount makes the
	// probe return true, the handler defers ("juicefs alive — not remounting"),
	// and TestNFSAutoRemountThreshold sees calls=0 and fails. Production never
	// reassigns it, so behavior is identical to calling isJuiceFSProcessAlive.
	isJuiceFSProcessAliveFn = isJuiceFSProcessAlive
)

// SetDrainProbe registers the drainer ingest-state hook used for the online
// busy-suppression (#89). fn must return the drainer's current in-flight drain
// count and the elapsed time since its last COMPLETED drain (a huge sentinel
// when none has completed). Wired from bridge via globalDrainerAtomic. Passing
// nil restores the pre-drainer no-op (no online suppression). Guarded by mu so
// it is safe to (re)wire while the check loop runs.
func (m *HealthMonitor) SetDrainProbe(fn func() (inFlight int64, sinceLastSuccess time.Duration)) {
	m.mu.Lock()
	m.drainProbe = fn
	m.mu.Unlock()
}

// busyIngesting reports whether a legitimate heavy ingest/drain is active right
// now — either drains in flight, or a drain completed within
// DrainBusyRecentWindow. It is the ONLINE analogue of pin.IsOffline() for the
// stat/readdir TIMEOUT suppression (#89): under such an ingest the drain's
// synchronous MinIO PUTs saturate the FUSE request queue, so a foreground stat
// can time out on a mount that is BUSY, not wedged. Returns false when no drain
// probe is wired (pre-drainer no-op). Concurrent-safe (the hook reads atomics).
func (m *HealthMonitor) busyIngesting() bool {
	m.mu.RLock()
	probe := m.drainProbe
	m.mu.RUnlock()
	if probe == nil {
		return false
	}
	inFlight, since := probe()
	return inFlight > 0 || since < DrainBusyRecentWindow
}

// New creates a HealthMonitor for the given configuration.
func New(cfg Config) *HealthMonitor {
	rdb := redis.NewClient(&redis.Options{
		Addr:        cfg.RedisURL,
		DialTimeout: 3 * time.Second,
		ReadTimeout: 3 * time.Second,
	})

	return &HealthMonitor{
		cfg: cfg,
		rdb: rdb,
		http: &http.Client{
			Timeout: 5 * time.Second,
		},
		done:        make(chan struct{}),
		absentTicks: absentRemountTicksFromEnv(),
	}
}

// Start begins periodic health checks every 10 seconds.
// It runs an immediate check, then ticks every 10s.
func (m *HealthMonitor) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	// Run an initial check immediately.
	m.runChecks(ctx)

	go func() {
		defer close(m.done)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.runChecks(ctx)
			}
		}
	}()
}

// Stop halts the health-check loop and releases resources.
func (m *HealthMonitor) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	<-m.done
	_ = m.rdb.Close()
}

// SetNetWatcher attaches a network watcher for grace-period awareness.
func (m *HealthMonitor) SetNetWatcher(nw *NetWatcher) {
	m.netWatcher = nw
}

// SetStatsProvider registers a callback that returns application-level
// memory statistics (cache sizes, fd pool counts, buffer usage). The
// callback is invoked on every health check tick.
func (m *HealthMonitor) SetStatsProvider(fn func() MemoryStats) {
	m.statsProvider = fn
}

// EnableNFSRemount registers a callback that re-runs the NFS mount
// command. When set, the monitor will automatically force-unmount and
// remount when the NFS mount point becomes stale (RPC hangs, Stat
// timeout, etc.). Auto-remount is gated by NFSStaleThreshold
// consecutive failures and NFSRemountCooldown between attempts.
//
// Pass nil to disable auto-remount.
func (m *HealthMonitor) EnableNFSRemount(fn func() error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.remountFn = fn
}

// EnableNFSAbsentRemount registers the mount callback for the NFS-layer
// absent-mount recovery (#93 — see absentremount.go). fn must be the SAME
// two-tier mount machinery boot uses (passwordless sudo → bounded admin
// prompt) — it may block up to that prompt's 180s bound, so the monitor
// always invokes it on its own goroutine, single-flighted. Pass nil to
// disable (deliberate Stop paths do, so recovery can't fight an
// intentional unmount).
func (m *HealthMonitor) EnableNFSAbsentRemount(fn func() error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.absentRemountFn = fn
}

// SetNFSServerAddr tells the monitor where the app's own NFS listener
// lives (e.g. "127.0.0.1:11049") so the absent-mount recovery can verify
// the server is up before remounting. Empty (never set) keeps the
// recovery disabled — a remount nobody can serve would only wedge the
// kernel client.
func (m *HealthMonitor) SetNFSServerAddr(addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nfsServerAddr = addr
}

// Status returns a snapshot of the current health state.
func (m *HealthMonitor) Status() HealthStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status
}

// InGracePeriod returns true if a network change happened recently enough
// that transient failures should be suppressed.
func (m *HealthMonitor) InGracePeriod() bool {
	if m.netWatcher == nil {
		return false
	}
	return m.netWatcher.InGracePeriod(GracePeriod)
}

// runChecks executes every health probe and updates the stored status.
func (m *HealthMonitor) runChecks(ctx context.Context) {
	prev := m.Status()

	redisStatus := m.checkRedis(ctx)
	minioStatus := m.checkMinIO()
	fuseStatus := m.checkFUSE()
	nfsStatus := m.checkNFS()

	// #93: capture the RAW FUSE verdict before the grace-period and
	// debounce layers soften it — the absent-mount recovery must gate on
	// honest FUSE health, not the smoothed UI indicator.
	rawFUSEHealthy := fuseStatus.Healthy

	// During grace period after network change, suppress FUSE failures
	// since FUSE stat may transiently fail while connections re-establish.
	if !fuseStatus.Healthy && m.InGracePeriod() {
		fuseStatus.Healthy = true
		fuseStatus.Message = "suppressed (network grace period)"
		jmlog.Debug("fuse error suppressed during network grace period")
	}

	// Anti-flap: smooth the reported FUSE status so a few-second backend-link
	// blip doesn't strobe the menu bar. (Functional outage detection is the
	// reachability monitor's job; this is purely the reported UI indicator.)
	fuseStatus = m.debounceFUSE(fuseStatus)

	// #93: NFS-layer absent-mount recovery runs FIRST and, when it claims
	// the tick (mountpoint genuinely absent + juicefs alive + our listener
	// up), the legacy handler is fed "healthy" so the two paths can never
	// double-act on the same condition — no more per-tick "deferring to the
	// fuse watchdog" spam, no 180s hard remount racing this path.
	claimed := m.handleNFSAbsentRecovery(nfsStatus, rawFUSEHealthy)

	// Track NFS staleness streaks and trigger auto-remount when needed.
	m.handleNFSAutoRemount(nfsStatus.Healthy || claimed)

	// Derive connection states
	redisConnState := connStateFor(prev.RedisConnState, prev.Redis.Healthy, redisStatus.Healthy)
	minioConnState := connStateFor(prev.MinIOConnState, prev.MinIO.Healthy, minioStatus.Healthy)

	overall := redisStatus.Healthy && minioStatus.Healthy &&
		fuseStatus.Healthy && nfsStatus.Healthy

	// Collect runtime memory stats.
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	var gcPauseUs uint64
	if ms.NumGC > 0 {
		gcPauseUs = ms.PauseNs[(ms.NumGC+255)%256] / 1000
	}

	next := HealthStatus{
		Redis:          redisStatus,
		MinIO:          minioStatus,
		FUSE:           fuseStatus,
		NFS:            nfsStatus,
		Overall:        overall,
		RedisConnState: redisConnState,
		MinIOConnState: minioConnState,
		HeapAllocMB:    float64(ms.HeapAlloc) / (1024 * 1024),
		HeapSysMB:      float64(ms.HeapSys) / (1024 * 1024),
		NumGC:          ms.NumGC,
		GCPauseLastUs:  gcPauseUs,
	}

	// Populate application-level stats if a provider is registered.
	if m.statsProvider != nil {
		appStats := m.statsProvider()
		next.PathCacheSize = appStats.PathCacheSize
		next.InodeCacheSize = appStats.InodeCacheSize
		next.FDPoolOpen = appStats.FDPoolOpen
		next.FDPoolActive = appStats.FDPoolActive
		next.MemBufEntries = appStats.MemBufEntries
		next.MemBufSizeMB = appStats.MemBufSizeMB
	}

	m.mu.Lock()
	m.status = next
	m.mu.Unlock()

	// Log state transitions.
	m.logTransition("Redis", prev.Redis, next.Redis)
	m.logTransition("MinIO", prev.MinIO, next.MinIO)
	m.logTransition("FUSE", prev.FUSE, next.FUSE)
	m.logTransition("NFS", prev.NFS, next.NFS)

	if prev.Overall != next.Overall {
		if next.Overall {
			jmlog.Info("system recovered — all components healthy")
		} else {
			jmlog.Warn("system degraded — one or more components unhealthy")
		}
	}
}

// handleNFSAutoRemount tracks consecutive NFS health failures and, once
// the threshold is crossed (and cooldown allows), invokes a force-
// unmount + remount sequence. It is a no-op when no remount callback
// is registered or when the mount point is not configured.
func (m *HealthMonitor) handleNFSAutoRemount(healthy bool) {
	m.mu.Lock()
	remountFn := m.remountFn
	mountPoint := m.cfg.NFSMountPoint
	if remountFn == nil || mountPoint == "" {
		m.nfsStaleStreak = 0
		m.mu.Unlock()
		return
	}
	if healthy {
		m.nfsStaleStreak = 0
		m.mu.Unlock()
		return
	}
	m.nfsStaleStreak++
	streak := m.nfsStaleStreak
	if streak < NFSStaleThreshold {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()

	// QA-39 (2026-06-13): do NOT tear the mount down while juicefs is alive.
	// NFS staleness during a backend-link blip is a temporarily-wedged FUSE
	// mount, not a dead one — and force-unmount + SIGKILL of the juicefs tree
	// (the unmount kills processes matching "juicefs mount.*<mountpoint>",
	// which also takes out juicefs's own -d supervisor) is exactly the macFUSE
	// remount-thrash that the fuse.go monitorLoop was rewritten to avoid on
	// 2026-05-29 — yet this NFS path kept the old "remount after N stale
	// checks" policy and fired it on a flap, vanishing the volume mid-copy
	// ("device disappeared"). While juicefs is alive, defer to the FUSE
	// watchdog (health/fuse.go), which owns the last-resort escalation after a
	// sustained window + backend-reachable check. Remount from here only when
	// the juicefs process tree is gone, or — as a far-out backstop — if the
	// NFS layer stays wedged well past the FUSE watchdog's own window.
	alive := isJuiceFSProcessAliveFn(m.cfg.FUSEPath)
	if alive && streak < NFSStaleHardRemountThreshold {
		jmlog.Warn("nfs mount stale but juicefs alive — deferring to the fuse watchdog, not remounting",
			"mount_point", mountPoint, "consecutive_failures", streak)
		return
	}

	m.mu.Lock()
	if !m.lastRemountAt.IsZero() && time.Since(m.lastRemountAt) < NFSRemountCooldown {
		jmlog.Debug("nfs auto-remount suppressed by cooldown",
			"streak", streak,
			"since_last_sec", int64(time.Since(m.lastRemountAt).Seconds()),
		)
		m.mu.Unlock()
		return
	}
	// Reset streak; record attempt so cooldown holds even on failure.
	m.nfsStaleStreak = 0
	m.lastRemountAt = time.Now()
	m.mu.Unlock()

	jmlog.Warn("nfs mount stale, attempting auto-remount",
		"mount_point", mountPoint,
		"consecutive_failures", streak,
		"juicefs_alive", alive,
	)
	if err := forceUnmountFn(mountPoint); err != nil {
		jmlog.Warn("nfs force-unmount failed (continuing to remount anyway)",
			"mount_point", mountPoint, "error", err.Error())
	}
	if err := remountFn(); err != nil {
		jmlog.Error("nfs auto-remount failed",
			"mount_point", mountPoint, "error", err.Error())
		return
	}
	jmlog.Info("nfs auto-remount succeeded", "mount_point", mountPoint)
}

// handleNFSAbsentRecovery is the NFS-layer absent-mount watcher (#93 — the
// decision logic lives in absentremount.go). Returns true when it CLAIMS
// the tick: the mountpoint is genuinely absent from the mount table while
// juicefs is alive and our NFS listener is up (and no guard vetoes) — the
// exact condition the legacy handler used to log "deferring to the fuse
// watchdog" on forever. A claimed tick makes runChecks feed the legacy
// handler "healthy" so only one path acts.
//
// The remount itself runs on a dedicated goroutine (single-flighted via
// absentState.inProgress) because the wired callback may park on the
// 180s-bounded admin prompt — the 10s check loop must keep ticking.
func (m *HealthMonitor) handleNFSAbsentRecovery(st ComponentStatus, rawFUSEHealthy bool) bool {
	m.mu.Lock()
	fn := m.absentRemountFn
	mountPoint := m.cfg.NFSMountPoint
	serverAddr := m.nfsServerAddr
	threshold := m.absentTicks
	m.mu.Unlock()
	if fn == nil || mountPoint == "" {
		return false
	}
	if threshold <= 0 {
		threshold = absentRemountTicksFromEnv()
	}

	// "not mounted" is checkNFS's mount-table-verified absence verdict —
	// the ONLY message that means nothing is mounted (a timeout reports
	// "unresponsive", a sick-but-present mount "stale: …").
	absent := !st.Healthy && st.Message == nfsMsgNotMounted
	in := nfsAbsentInputs{
		Absent:  absent,
		Healthy: st.Healthy,
		Offline: pin.IsOffline(),
		Enabled: nfsAutoRemountEnabled(),
		Now:     time.Now(),
	}
	// Probe process table + listener only on ticks that could qualify —
	// never exec pgrep / dial per tick on the happy path.
	if absent && in.Enabled && !in.Offline {
		in.JuiceFSAlive = rawFUSEHealthy && isJuiceFSProcessAliveFn(m.cfg.FUSEPath)
		in.ServerUp = serverAddr != "" && nfsServerUpFn(serverAddr)
	}
	claimed := absent && in.Enabled && !in.Offline && in.JuiceFSAlive && in.ServerUp

	m.mu.Lock()
	fire, reason := m.absentState.tick(in, threshold)
	streak, failures := m.absentState.streak, m.absentState.failures
	nextAttempt := m.absentState.nextAttempt
	if fire {
		m.absentState.inProgress = true
	}
	m.mu.Unlock()

	if fire {
		jmlog.Warn("nfs mount absent while juicefs alive — nfs-layer auto-remount engaging",
			"mount_point", mountPoint,
			"consecutive_ticks", streak,
			"prior_failures", failures,
			"kill_switch", "JM_NFS_AUTOREMOUNT=0",
		)
		go m.runAbsentRemount(fn, mountPoint)
	} else if absent {
		// One line per held tick at Debug — the WARN budget is reserved for
		// the engage/success/failure edges.
		jmlog.Debug("nfs-layer absent watcher holding",
			"reason", reason,
			"streak", streak,
			"claimed", claimed,
			"backoff_until", nextAttempt.Format(time.RFC3339),
		)
	}
	return claimed
}

// runAbsentRemount executes one absent-mount recovery attempt and folds
// the outcome into the backoff state. Runs on its own goroutine; the
// inProgress flag it clears is what re-arms the decision path.
func (m *HealthMonitor) runAbsentRemount(fn func() error, mountPoint string) {
	err := fn()

	m.mu.Lock()
	m.absentState.inProgress = false
	if err == nil {
		m.absentState.streak, m.absentState.failures = 0, 0
		m.absentState.nextAttempt = time.Time{}
		m.mu.Unlock()
		jmlog.Info("nfs-layer auto-remount succeeded", "mount_point", mountPoint)
		return
	}
	m.absentState.failures++
	failures := m.absentState.failures
	backoff := absentBackoff(failures)
	m.absentState.nextAttempt = time.Now().Add(backoff)
	m.mu.Unlock()

	retry := backoff.Round(time.Second).String()
	if isMountBusyError(err) {
		// The kernel-haunted-mountpoint case: EBUSY with nothing in the
		// mount table. Must not hot-loop — say so and back off loudly.
		jmlog.Warn("nfs-layer auto-remount: mountpoint busy, will retry in "+retry,
			"mount_point", mountPoint, "attempt", failures, "error", err.Error())
		return
	}
	jmlog.Warn("nfs-layer auto-remount failed, will retry in "+retry,
		"mount_point", mountPoint, "attempt", failures, "error", err.Error())
}

// forceUnmount runs `umount -f` on the given mount point. Errors here
// are non-fatal: the subsequent mount attempt is the actual recovery.
//
// Bounded at 20 s. `sudo umount -f` can hang on a truly-wedged kernel
// mount (the original `umount` syscall enters uninterruptible D state on
// some failure modes). Without a context, the runChecks goroutine that
// called this would never tick again, and the health-monitor loop would
// be functionally dead.
func forceUnmount(mountPoint string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sudo", "umount", "-f", mountPoint)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("umount -f %s: timed out after 20s (kernel mount likely wedged)", mountPoint)
		}
		return fmt.Errorf("umount -f %s: %v: %s", mountPoint, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// debounceFUSE applies hysteresis to the raw FUSE probe so a transient
// backend-link blip doesn't strobe the reported status. The reported state
// flips to degraded only after FUSEFlapToDegraded consecutive unhealthy raw
// probes; it recovers on the first healthy probe. When degraded, the message
// tracks the raw probe so the underlying reason is still surfaced.
func (m *HealthMonitor) debounceFUSE(raw ComponentStatus) ComponentStatus {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.fuseDebouncedInit {
		m.fuseDebounced = raw
		m.fuseDebouncedInit = true
		if raw.Healthy {
			m.fuseRawHealthyStreak = 1
		} else {
			m.fuseRawUnhealthyStreak = 1
		}
		return m.fuseDebounced
	}

	if raw.Healthy {
		m.fuseRawHealthyStreak++
		m.fuseRawUnhealthyStreak = 0
		// Recover immediately on a healthy probe.
		if !m.fuseDebounced.Healthy {
			m.fuseDebounced = raw
		}
	} else {
		m.fuseRawUnhealthyStreak++
		m.fuseRawHealthyStreak = 0
		// Flip to degraded only after a sustained unhealthy streak.
		if m.fuseDebounced.Healthy && m.fuseRawUnhealthyStreak >= FUSEFlapToDegraded {
			m.fuseDebounced = raw
		}
	}

	// Keep LastCheck fresh so liveness-of-the-probe is still visible even
	// while the debounced Healthy bit is held steady.
	out := m.fuseDebounced
	out.LastCheck = raw.LastCheck
	return out
}

// connStateFor derives the connection state from previous state and health transitions.
func connStateFor(prevState ConnState, wasHealthy, isHealthy bool) ConnState {
	if isHealthy {
		if prevState == ConnStateDisconnected || prevState == ConnStateReconnecting {
			return ConnStateConnected // reconnected
		}
		return ConnStateConnected
	}
	// Not healthy
	if wasHealthy {
		return ConnStateDisconnected
	}
	return ConnStateDisconnected
}

func (m *HealthMonitor) logTransition(name string, prev, next ComponentStatus) {
	if prev.Healthy && !next.Healthy {
		jmlog.Warn("component degraded",
			"component", name,
			"reason", next.Message,
		)
	} else if !prev.Healthy && next.Healthy && prev.LastCheck != (time.Time{}) {
		jmlog.Info("component recovered", "component", name)
	}
}

// --------------- individual checks ---------------

func (m *HealthMonitor) checkRedis(ctx context.Context) ComponentStatus {
	now := time.Now()
	err := m.rdb.Ping(ctx).Err()
	if err != nil {
		jmlog.Debug("redis ping failed", "error", err.Error())
		return m.classifyBackendFailure("redis", m.cfg.RedisURL, err,
			ComponentStatus{Healthy: false, LastCheck: now, Message: fmt.Sprintf("ping failed: %v", err)})
	}
	m.clearLocalNetSuspect("redis")
	return ComponentStatus{Healthy: true, LastCheck: now, Message: "ok"}
}

func (m *HealthMonitor) checkMinIO() ComponentStatus {
	now := time.Now()
	if m.cfg.MinIOURL == "" {
		return ComponentStatus{Healthy: true, LastCheck: now, Message: "not configured"}
	}
	url := m.cfg.MinIOURL + "/minio/health/live"
	resp, err := m.http.Get(url)
	if err != nil {
		jmlog.Debug("minio health check failed", "error", err.Error())
		return m.classifyBackendFailure("minio", minioHostPort(m.cfg.MinIOURL), err,
			ComponentStatus{Healthy: false, LastCheck: now, Message: fmt.Sprintf("request failed: %v", err)})
	}
	m.clearLocalNetSuspect("minio")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("unexpected status %d", resp.StatusCode)
		jmlog.Debug("minio health check failed", "status", resp.StatusCode)
		return ComponentStatus{Healthy: false, LastCheck: now, Message: msg}
	}
	return ComponentStatus{Healthy: true, LastCheck: now, Message: "ok"}
}

// classifyBackendFailure upgrades a generic backend-dial failure to the
// distinct MsgLocalNetworkPermission status when the failure matches the
// macOS Local Network permission-denial signature (#106 — see localnet.go
// for the exact, deliberately conservative conditions). Non-matching
// failures pass through untouched, so every existing report is preserved.
// The first tick of a suspicion streak logs a WARN with the remediation.
func (m *HealthMonitor) classifyBackendFailure(component, targetAddr string, err error, base ComponentStatus) ComponentStatus {
	if targetAddr == "" || !IsLocalNetPermissionSignature(err, targetAddr, localNetSnapshotFn()) {
		m.clearLocalNetSuspect(component)
		return base
	}
	base.Message = MsgLocalNetworkPermission

	m.mu.Lock()
	first := !m.localNetSuspected[component]
	if first {
		if m.localNetSuspected == nil {
			m.localNetSuspected = make(map[string]bool)
		}
		m.localNetSuspected[component] = true
	}
	m.mu.Unlock()
	if first {
		jmlog.Warn("backend dial blocked — macOS Local Network permission likely denied (it silently resets after every rebuild/re-sign/update)",
			"component", component,
			"target", targetAddr,
			"error", err.Error(),
			"action", "enable JuiceMount in System Settings → Privacy & Security → Local Network, then relaunch",
		)
	}
	return base
}

// clearLocalNetSuspect drops a component's permission-suspected latch so
// the next suspicion streak logs its WARN again.
func (m *HealthMonitor) clearLocalNetSuspect(component string) {
	m.mu.Lock()
	if m.localNetSuspected[component] {
		delete(m.localNetSuspected, component)
	}
	m.mu.Unlock()
}

// minioHostPort extracts "host:port" from the configured MinIO base URL so
// the #106 classifier can judge the target address. Empty on parse failure
// (classifier stays silent — conservative).
func minioHostPort(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Host
}

func (m *HealthMonitor) checkFUSE() ComponentStatus {
	now := time.Now()

	// Check 1: directory exists, AND stat doesn't hang.
	// A bare os.Stat on a wedged FUSE path enters uninterruptible kernel
	// wait. Wrap in the same goroutine+timeout pattern used below for
	// readdir.
	statDone := make(chan struct {
		info os.FileInfo
		err  error
	}, 1)
	go func() {
		info, err := os.Stat(m.cfg.FUSEPath)
		statDone <- struct {
			info os.FileInfo
			err  error
		}{info, err}
	}()
	select {
	case r := <-statDone:
		if r.err != nil || !r.info.IsDir() {
			jmlog.Debug("fuse mount missing", "path", m.cfg.FUSEPath)
			return ComponentStatus{Healthy: false, LastCheck: now, Message: fmt.Sprintf("stat failed: %v", r.err)}
		}
	case <-time.After(5 * time.Second):
		// #89 online busy-suppression: a foreground stat that TIMES OUT while a
		// legitimate heavy ingest is draining is the drain's synchronous MinIO
		// PUTs saturating the FUSE request queue — BUSY, not wedged. Reporting
		// degraded here surfaces as the Finder "mount losing communication"
		// report even though the mount is fine (the destructive remount path
		// already defers safely; this is purely the reporting layer). The
		// ONLINE analogue of the pin.IsOffline() readdir suppression below. Only
		// a TIMEOUT is suppressed — a genuine stat error still degrades (that's
		// the error branch above, unreachable from here).
		if m.busyIngesting() {
			jmlog.Info("fuse stat slow but drain in flight (busy, heavy ingest) — not flagging wedged", "path", m.cfg.FUSEPath)
			return ComponentStatus{Healthy: true, LastCheck: now, Message: "busy (heavy ingest)"}
		}
		jmlog.Warn("fuse stat timed out (path likely wedged)", "path", m.cfg.FUSEPath)
		go m.logWedgeDiagnostics("stat_timeout")
		return ComponentStatus{Healthy: false, LastCheck: now, Message: "stat timed out (wedged FUSE mount)"}
	}

	// Check 2: mount is actually a FUSE filesystem (appears in mount table).
	// `mount` (getfsstat) can hang in the kernel if the table contains a
	// wedged entry; bound it.
	mountCtx, mountCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mountCancel()
	out, mountErr := mounttable.Output(mountCtx)
	if mountErr != nil {
		jmlog.Warn("mount table query timed out in checkFUSE", "path", m.cfg.FUSEPath)
		return ComponentStatus{Healthy: false, LastCheck: now, Message: "mount table query timed out"}
	}
	if !strings.Contains(string(out), m.cfg.FUSEPath) {
		jmlog.Debug("fuse mount not in mount table", "path", m.cfg.FUSEPath)
		// V2.3 G0d: when the miss is the kext-approval loss, say so with the
		// remediation — "no FUSE" alone reads like a transient.
		if KextApprovalBlocked() {
			return ComponentStatus{Healthy: false, LastCheck: now,
				Message: "macFUSE not approved — System Settings → Privacy & Security → Allow, then reboot (writes buffered in spool)"}
		}
		return ComponentStatus{Healthy: false, LastCheck: now, Message: "not mounted (directory exists but no FUSE)"}
	}

	// Check 3: mount is responsive (stale FUSE mounts hang on readdir)
	done := make(chan error, 1)
	go func() {
		_, err := os.ReadDir(m.cfg.FUSEPath)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			jmlog.Debug("fuse readdir failed", "path", m.cfg.FUSEPath, "error", err.Error())
			return ComponentStatus{Healthy: false, LastCheck: now, Message: fmt.Sprintf("readdir failed: %v", err)}
		}
		return ComponentStatus{Healthy: true, LastCheck: now, Message: "ok"}
	case <-time.After(5 * time.Second):
		// Offline-aware (release fix): a root readdir that times out while the
		// user is OFFLINE is the juicefs daemon being SATURATED by in-flight
		// spool drains (writes with nowhere to go offline) — BUSY, not wedged.
		// Reporting unhealthy here feeds the watchdog's escalate-to-remount,
		// which SIGTERMs a healthy daemon and drops the NFS mount → Finder
		// "connection interrupted". Offline, treat a readdir timeout as a
		// transient busy state, not a stale-mount fault.
		if pin.IsOffline() {
			jmlog.Debug("fuse readdir slow but offline (busy draining) — not flagging stale", "path", m.cfg.FUSEPath)
			return ComponentStatus{Healthy: true, LastCheck: now, Message: "offline (busy/draining)"}
		}
		// #89 ONLINE analogue of the offline suppression above: a readdir that
		// times out while a heavy ingest is draining is the same saturated-FUSE-
		// queue busy state, just ONLINE. Suppress it (busy, not stale) so a
		// high-concurrency ingest doesn't surface as "mount losing communication"
		// / feed the watchdog's escalate-to-remount. TIMEOUT-only (genuine
		// readdir errors take the error branch above and still degrade).
		if m.busyIngesting() {
			jmlog.Info("fuse readdir slow but drain in flight (busy, heavy ingest) — not flagging stale", "path", m.cfg.FUSEPath)
			return ComponentStatus{Healthy: true, LastCheck: now, Message: "busy (heavy ingest)"}
		}
		jmlog.Warn("fuse mount unresponsive (stale)", "path", m.cfg.FUSEPath)
		go m.logWedgeDiagnostics("readdir_unresponsive")
		return ComponentStatus{Healthy: false, LastCheck: now, Message: "unresponsive (stale mount)"}
	}
}

// logWedgeDiagnostics captures backend RTT + process liveness at the instant a
// FUSE wedge is detected, so wedges can be correlated with backend/link events
// (the TrueNAS ConnectX flap — docs/HANDOFF_truenas_connectx_flapping.md). It is
// the diagnostic half of the Finder "not responding" investigation: a wedge
// almost always coincides with a Redis/MinIO RTT spike, and this log is the
// evidence to confirm it.
//
// QA-35 perf discipline: runs ONLY in the HealthMonitor goroutine (never on an
// NFS RPC path), probes ONLY the backend sockets + process table — NEVER the
// wedged mountpoint itself — bounds every probe, and is throttled to once per
// minute so a sustained wedge cannot spam Redis/MinIO or flood the log.
func (m *HealthMonitor) logWedgeDiagnostics(reason string) {
	m.mu.Lock()
	if !m.lastWedgeDiagAt.IsZero() && time.Since(m.lastWedgeDiagAt) < time.Minute {
		m.mu.Unlock()
		return
	}
	m.lastWedgeDiagAt = time.Now()
	m.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	t0 := time.Now()
	redisStatus := m.checkRedis(ctx)
	redisMs := time.Since(t0).Milliseconds()
	t1 := time.Now()
	minioStatus := m.checkMinIO()
	minioMs := time.Since(t1).Milliseconds()

	jmlog.Warn("fuse wedge diagnostics",
		"reason", reason,
		"redis_ok", redisStatus.Healthy, "redis_probe_ms", redisMs, "redis_msg", redisStatus.Message,
		"minio_ok", minioStatus.Healthy, "minio_probe_ms", minioMs, "minio_msg", minioStatus.Message,
		"juicefs_alive", isJuiceFSProcessAliveFn(m.cfg.FUSEPath),
	)
}

// checkNFS verifies the NFS mount is responsive. A simple os.Stat() can
// hang indefinitely on a stale mount, so we time-bound the call: any
// stat call that does not return within NFSStatTimeout is treated as a
// stale-mount signal (the same condition that produces the Finder
// spinning beach ball).
func (m *HealthMonitor) checkNFS() ComponentStatus {
	now := time.Now()
	if m.cfg.NFSMountPoint == "" {
		return ComponentStatus{Healthy: true, LastCheck: now, Message: "not configured"}
	}

	done := make(chan ComponentStatus, 1)
	go func() {
		if _, err := os.Stat(m.cfg.NFSMountPoint); err != nil {
			if os.IsNotExist(err) {
				// Mount-point directory gone — nothing is mounted there.
				// Distinct from "stale" so the UI can offer "Mount Now"
				// (LB-2 state honesty) instead of a generic fault, and so
				// the absent-mount recovery (#93) can key on it.
				done <- ComponentStatus{Healthy: false, LastCheck: now, Message: nfsMsgNotMounted}
				return
			}
			jmlog.Debug("nfs stat failed", "mount_point", m.cfg.NFSMountPoint, "error", err.Error())
			done <- ComponentStatus{Healthy: false, LastCheck: now, Message: fmt.Sprintf("stale: %v", err)}
			return
		}
		// A successful stat is NOT enough: a plain directory left behind
		// after an unmount stats fine too. Verify the kernel mount table
		// actually lists the path (LB-2). Bounded — getfsstat can hang on
		// a wedged entry; on table-read failure report stale rather than
		// not-mounted so the UI doesn't offer a Mount Now that would fail
		// against a busy mount point.
		mounted, err := mountTableHas(m.cfg.NFSMountPoint, NFSStatTimeout)
		if err != nil {
			done <- ComponentStatus{Healthy: false, LastCheck: now, Message: fmt.Sprintf("stale: mount table unreadable: %v", err)}
			return
		}
		if !mounted {
			done <- ComponentStatus{Healthy: false, LastCheck: now, Message: nfsMsgNotMounted}
			return
		}
		done <- ComponentStatus{Healthy: true, LastCheck: now, Message: "ok"}
	}()

	select {
	case st := <-done:
		// A resolved probe (success OR a genuine error/ENOTCONN from the inner
		// goroutine) is authoritative — return it verbatim. The busy-suppression
		// below covers ONLY the TIMEOUT branch, never a real error.
		return st
	case <-time.After(NFSStatTimeout):
		// #89 online busy-suppression (same rationale as checkFUSE): an NFS stat
		// that TIMES OUT while a heavy ingest is draining is the drain saturating
		// the FUSE queue behind the NFS layer — BUSY, not stale. Suppress the
		// degraded report so a legitimate ingest doesn't surface as "mount
		// losing communication". TIMEOUT-only: a resolved error/ENOTCONN takes
		// the `case st := <-done` branch above and still degrades.
		if m.busyIngesting() {
			jmlog.Info("nfs stat slow but drain in flight (busy, heavy ingest) — not flagging stale",
				"mount_point", m.cfg.NFSMountPoint, "timeout_ms", NFSStatTimeout.Milliseconds())
			return ComponentStatus{Healthy: true, LastCheck: now, Message: "busy (heavy ingest)"}
		}
		jmlog.Warn("nfs mount unresponsive (stat timed out)",
			"mount_point", m.cfg.NFSMountPoint,
			"timeout_ms", NFSStatTimeout.Milliseconds(),
		)
		return ComponentStatus{Healthy: false, LastCheck: now, Message: "unresponsive (stat timeout)"}
	}
}

// mountTableHas reports whether path appears as a mount point in the
// kernel mount table. Matches " on <path> (" so prefix-sharing paths
// (/Volumes/zpool vs /Volumes/zpool2) can't false-positive. Bounded:
// `mount` calls getfsstat(), which can hang in the kernel when the table
// holds a wedged entry — on timeout (or any exec failure) an error is
// returned and the caller decides how to degrade.
func mountTableHas(path string, timeout time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	out, err := mounttable.Output(ctx)
	if err != nil {
		return false, fmt.Errorf("mount table query failed within %s: %w", timeout, err)
	}
	return strings.Contains(string(out), " on "+path+" ("), nil
}

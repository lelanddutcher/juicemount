// Package health provides periodic health monitoring and recovery triggers
// for the JuiceMount5 NFS loopback server.
package health

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// Config holds the endpoints and paths that the monitor checks.
type Config struct {
	RedisURL      string // e.g. "192.168.0.210:6379"
	MinIOURL      string // e.g. "http://192.168.0.212:9000"
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
	remountFn       func() error // optional: called to remount the NFS volume
	nfsStaleStreak  int          // consecutive failed NFS checks
	lastRemountAt   time.Time    // last successful remount time
}

// Tunables for NFS auto-remount. Exposed as vars so tests can override.
var (
	// NFSStaleThreshold is the number of consecutive failed NFS health
	// checks (10s apart) before triggering a remount.
	NFSStaleThreshold = 3

	// NFSRemountCooldown is the minimum time between auto-remount
	// attempts. Prevents flapping if the underlying issue is persistent.
	NFSRemountCooldown = 60 * time.Second

	// NFSStatTimeout bounds an os.Stat() probe of the NFS mount point.
	// If the syscall hangs longer than this, the mount is considered stale.
	NFSStatTimeout = 5 * time.Second

	// forceUnmountFn is the function used to force-unmount a stale mount.
	// Replaceable so tests can avoid invoking `sudo`.
	forceUnmountFn = forceUnmount
)

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
		done: make(chan struct{}),
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

	// During grace period after network change, suppress FUSE failures
	// since FUSE stat may transiently fail while connections re-establish.
	if !fuseStatus.Healthy && m.InGracePeriod() {
		fuseStatus.Healthy = true
		fuseStatus.Message = "suppressed (network grace period)"
		jmlog.Debug("fuse error suppressed during network grace period")
	}

	// Track NFS staleness streaks and trigger auto-remount when needed.
	m.handleNFSAutoRemount(nfsStatus.Healthy)

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

// forceUnmount runs `umount -f` on the given mount point. Errors here
// are non-fatal: the subsequent mount attempt is the actual recovery.
func forceUnmount(mountPoint string) error {
	cmd := exec.Command("sudo", "umount", "-f", mountPoint)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("umount -f %s: %v: %s", mountPoint, err, strings.TrimSpace(string(out)))
	}
	return nil
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
		return ComponentStatus{Healthy: false, LastCheck: now, Message: fmt.Sprintf("ping failed: %v", err)}
	}
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
		return ComponentStatus{Healthy: false, LastCheck: now, Message: fmt.Sprintf("request failed: %v", err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("unexpected status %d", resp.StatusCode)
		jmlog.Debug("minio health check failed", "status", resp.StatusCode)
		return ComponentStatus{Healthy: false, LastCheck: now, Message: msg}
	}
	return ComponentStatus{Healthy: true, LastCheck: now, Message: "ok"}
}

func (m *HealthMonitor) checkFUSE() ComponentStatus {
	now := time.Now()

	// Check 1: directory exists
	info, err := os.Stat(m.cfg.FUSEPath)
	if err != nil || !info.IsDir() {
		jmlog.Debug("fuse mount missing", "path", m.cfg.FUSEPath)
		return ComponentStatus{Healthy: false, LastCheck: now, Message: fmt.Sprintf("stat failed: %v", err)}
	}

	// Check 2: mount is actually a FUSE filesystem (appears in mount table)
	out, _ := exec.Command("mount").Output()
	if !strings.Contains(string(out), m.cfg.FUSEPath) {
		jmlog.Debug("fuse mount not in mount table", "path", m.cfg.FUSEPath)
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
		jmlog.Warn("fuse mount unresponsive (stale)", "path", m.cfg.FUSEPath)
		return ComponentStatus{Healthy: false, LastCheck: now, Message: "unresponsive (stale mount)"}
	}
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

	done := make(chan error, 1)
	go func() {
		_, err := os.Stat(m.cfg.NFSMountPoint)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			jmlog.Debug("nfs stat failed", "mount_point", m.cfg.NFSMountPoint, "error", err.Error())
			return ComponentStatus{Healthy: false, LastCheck: now, Message: fmt.Sprintf("stale: %v", err)}
		}
		return ComponentStatus{Healthy: true, LastCheck: now, Message: "ok"}
	case <-time.After(NFSStatTimeout):
		jmlog.Warn("nfs mount unresponsive (stat timed out)",
			"mount_point", m.cfg.NFSMountPoint,
			"timeout_ms", NFSStatTimeout.Milliseconds(),
		)
		return ComponentStatus{Healthy: false, LastCheck: now, Message: "unresponsive (stat timeout)"}
	}
}

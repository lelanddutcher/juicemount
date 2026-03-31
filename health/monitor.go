// Package health provides periodic health monitoring and recovery triggers
// for the JuiceMount5 NFS loopback server.
package health

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
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
		log.Printf("[health] FUSE error suppressed during network grace period")
	}

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
			log.Println("[health] system recovered — all components healthy")
		} else {
			log.Println("[health] system degraded — one or more components unhealthy")
		}
	}
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
		log.Printf("[health] %s: healthy → unhealthy: %s", name, next.Message)
	} else if !prev.Healthy && next.Healthy && prev.LastCheck != (time.Time{}) {
		log.Printf("[health] %s: unhealthy → healthy", name)
	}
}

// --------------- individual checks ---------------

func (m *HealthMonitor) checkRedis(ctx context.Context) ComponentStatus {
	now := time.Now()
	err := m.rdb.Ping(ctx).Err()
	if err != nil {
		log.Printf("[health] Redis ping failed: %v — continuing with cached metadata", err)
		return ComponentStatus{Healthy: false, LastCheck: now, Message: fmt.Sprintf("ping failed: %v", err)}
	}
	return ComponentStatus{Healthy: true, LastCheck: now, Message: "ok"}
}

func (m *HealthMonitor) checkMinIO() ComponentStatus {
	now := time.Now()
	url := m.cfg.MinIOURL + "/minio/health/live"
	resp, err := m.http.Get(url)
	if err != nil {
		log.Printf("[health] MinIO health check failed: %v", err)
		return ComponentStatus{Healthy: false, LastCheck: now, Message: fmt.Sprintf("request failed: %v", err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("unexpected status %d", resp.StatusCode)
		log.Printf("[health] MinIO health check failed: %s", msg)
		return ComponentStatus{Healthy: false, LastCheck: now, Message: msg}
	}
	return ComponentStatus{Healthy: true, LastCheck: now, Message: "ok"}
}

func (m *HealthMonitor) checkFUSE() ComponentStatus {
	now := time.Now()

	// Check 1: directory exists
	info, err := os.Stat(m.cfg.FUSEPath)
	if err != nil || !info.IsDir() {
		log.Printf("[health] FUSE mount not found at %s", m.cfg.FUSEPath)
		return ComponentStatus{Healthy: false, LastCheck: now, Message: fmt.Sprintf("stat failed: %v", err)}
	}

	// Check 2: mount is actually a FUSE filesystem (appears in mount table)
	out, _ := exec.Command("mount").Output()
	if !strings.Contains(string(out), m.cfg.FUSEPath) {
		log.Printf("[health] FUSE mount at %s is not in mount table (unmounted)", m.cfg.FUSEPath)
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
			log.Printf("[health] FUSE mount at %s is not readable: %v", m.cfg.FUSEPath, err)
			return ComponentStatus{Healthy: false, LastCheck: now, Message: fmt.Sprintf("readdir failed: %v", err)}
		}
		return ComponentStatus{Healthy: true, LastCheck: now, Message: "ok"}
	case <-time.After(5 * time.Second):
		log.Printf("[health] FUSE mount at %s is unresponsive (stale)", m.cfg.FUSEPath)
		return ComponentStatus{Healthy: false, LastCheck: now, Message: "unresponsive (stale mount)"}
	}
}

func (m *HealthMonitor) checkNFS() ComponentStatus {
	now := time.Now()
	if m.cfg.NFSMountPoint == "" {
		return ComponentStatus{Healthy: true, LastCheck: now, Message: "not configured"}
	}
	_, err := os.Stat(m.cfg.NFSMountPoint)
	if err != nil {
		log.Printf("[health] NFS mount stale at %s: %v", m.cfg.NFSMountPoint, err)
		return ComponentStatus{Healthy: false, LastCheck: now, Message: fmt.Sprintf("stale: %v", err)}
	}
	return ComponentStatus{Healthy: true, LastCheck: now, Message: "ok"}
}

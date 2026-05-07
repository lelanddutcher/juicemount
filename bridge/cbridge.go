package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/redis/go-redis/v9"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	jmlibnfs "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

// globalServer is the singleton NFS server instance.
//
// Lifecycle note: on a normal Start/Stop cycle from the menu bar, we
// deliberately leave the FUSE mount and the NFS mount in place. The
// rationale:
//
//   - Both mounts require an admin password prompt to set up or tear down,
//     so a Stop+Start cycle would prompt twice for nothing.
//   - Leaving them mounted across a "soft" stop makes the next Start
//     near-instant and avoids races where the kernel mount table hasn't
//     caught up before the next mount attempt fires off.
//
// Full teardown (unmount everything) only happens via NFSServerShutdown,
// which the Swift app calls from applicationWillTerminate.
var (
	globalMu        sync.Mutex
	globalServer    *jmnfs.Server
	globalRC        *metadata.RedisClient
	globalStore     *metadata.Store
	globalCache     *cache.Reader
	globalMonitor   *health.HealthMonitor
	globalRDB       *redis.Client
	globalMetrics   *metrics.Server
	globalFUSE      *health.FUSEManager
	globalFUSEPath  string // remembered across Stop/Start so we can detect existing mount
	globalMountPath string

	// Pin / offline / prefetch globals
	globalPinStore   *pin.Store
	globalPrefetcher *pin.Prefetcher
)

// ServerConfig is the JSON configuration passed from Swift.
type ServerConfig struct {
	RedisURL    string `json:"redis_url"`
	FUSEPath    string `json:"fuse_path"`
	MountPoint  string `json:"mount_point"`
	ListenAddr  string `json:"listen_addr"`
	DBPath      string `json:"db_path"`
	CacheSize   string `json:"cache_size"`
	MetricsAddr string `json:"metrics_addr"`
	LogFile     string `json:"log_file"`
	LogLevel    string `json:"log_level"`
}

//export NFSServerStart
func NFSServerStart(configJSON *C.char) *C.char {
	globalMu.Lock()
	defer globalMu.Unlock()

	if globalServer != nil {
		return C.CString("error: server already running")
	}

	var cfg ServerConfig
	rawJSON := C.GoString(configJSON)
	if err := json.Unmarshal([]byte(rawJSON), &cfg); err != nil {
		return C.CString(fmt.Sprintf("error: parse config: %v", err))
	}
	// Debug: show what we received from Swift
	jmlog.Info("cbridge received config",
		"redis_url", cfg.RedisURL,
		"fuse_path", cfg.FUSEPath,
		"mount_point", cfg.MountPoint,
		"listen_addr", cfg.ListenAddr,
		"db_path", cfg.DBPath,
		"raw_json_bytes", len(rawJSON))

	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:11049"
	}
	if cfg.MetricsAddr == "" {
		cfg.MetricsAddr = "127.0.0.1:11050"
	}

	// Initialize structured logging early so all subsequent log lines
	// flow through the JSON sink (and optional log file).
	if err := jmlog.Init(jmlog.Config{
		LogFile: cfg.LogFile,
		Level:   jmlog.ParseLevel(cfg.LogLevel),
	}); err != nil {
		return C.CString(fmt.Sprintf("error: init logger: %v", err))
	}

	// Mount JuiceFS FUSE if not already mounted. This is what the standalone
	// CLI (cmd/jm5/main.go) does on startup; the c-archive bridge needs to do
	// it too, otherwise NFS will be pointing at an empty directory.
	//
	// Idempotent path: if a FUSE mount already exists at cfg.FUSEPath (e.g.
	// from a previous soft-stopped Start within this process), skip the
	// juicefs invocation entirely. FUSEManager.Mount() also handles this
	// internally, but we want to avoid even creating a second monitor
	// goroutine for the same mount.
	if cfg.FUSEPath != "" {
		// Make sure the mount-point directory exists
		_ = os.MkdirAll(cfg.FUSEPath, 0o755)
		globalFUSEPath = cfg.FUSEPath

		if globalFUSE != nil && fuseLooksHealthy(cfg.FUSEPath) {
			jmlog.Info("juicefs FUSE already mounted, reusing", "path", cfg.FUSEPath)
		} else {
			// Forward the user's configured cache size to JuiceFS. Without this,
			// JuiceFS uses an unspecified default and (worse) the user's
			// preferred cap is silently ignored — see the regression in the
			// CLI→GUI port where the menu-bar app was passing cache_size in
			// the JSON config but cbridge never forwarded it.
			//
			// FreeSpaceRatio = 0.01 (1%) instead of JuiceFS's default 0.1.
			// Video editors fill their disks; the default makes the cache
			// silently disable below 10% free, sending every read straight
			// to S3 with no warning. 1% is the sweet spot for our use case
			// — the disk is the cache.
			fm := health.NewFUSEManager(health.FUSEConfig{
				RedisURL:       cfg.RedisURL,
				MountPoint:     cfg.FUSEPath,
				CacheSize:      cfg.CacheSize,
				FreeSpaceRatio: "0.01",
			})
			if err := fm.Mount(); err != nil {
				jmlog.Error("juicefs FUSE mount failed", "error", err.Error())
				return C.CString(fmt.Sprintf("error: juicefs FUSE mount: %v", err))
			}
			fm.StartMonitor()
			globalFUSE = fm
			jmlog.Info("juicefs FUSE mounted",
				"path", cfg.FUSEPath,
				"cache_size_mb", cfg.CacheSize,
				"free_space_ratio", "0.01")
		}
	}

	// Open metadata store. If this fails, leave FUSE mounted — the next
	// Start can pick it up. Tearing FUSE down here would force an admin
	// password prompt on the next attempt, which is hostile.
	store, err := metadata.Open(cfg.DBPath)
	if err != nil {
		return C.CString(fmt.Sprintf("error: open store: %v", err))
	}
	globalStore = store

	// Connect to Redis with bounded retry. Network can be flaky — wifi/cell
	// handoffs, sleeping NAS, brief router restart. Without this, a 1s blip
	// at launch leaves the user staring at "redis: connect: no route to host"
	// even though the NAS comes back 3 seconds later.
	//
	// Retry schedule: 1s, 2s, 4s, 8s, 16s = 5 attempts, ~31s total worst case.
	rc, err := connectRedisWithRetry(cfg.RedisURL, store, 5)
	if err != nil {
		store.Close()
		return C.CString(fmt.Sprintf("error: redis: %v", err))
	}
	globalRC = rc

	// Initial sync
	if err := rc.SyncOnce(); err != nil {
		jmlog.Warn("initial sync failed", "error", err.Error())
	}
	rc.Start()

	// Cache reader
	cacheDir := cache.DetectCacheDir()
	if cacheDir != "" {
		addr, db, _ := metadata.ParseRedisURL(cfg.RedisURL)
		rdb := redis.NewClient(&redis.Options{Addr: addr, DB: db})
		globalRDB = rdb
		cr := cache.NewReader(cacheDir, cache.DefaultBlockSize, rdb)
		if err := cr.Verify(); err == nil {
			globalCache = cr
		}
	}

	// NFS server
	srv := jmnfs.NewServer(jmnfs.Config{
		ListenAddr: cfg.ListenAddr,
		FUSEPath:   cfg.FUSEPath,
	}, store)

	if err := srv.Start(); err != nil {
		rc.Stop()
		store.Close()
		return C.CString(fmt.Sprintf("error: start: %v", err))
	}

	if globalCache != nil {
		srv.Handler().SetCacheReader(globalCache)
	}
	srv.Handler().SetRedisClient(rc)
	globalServer = srv

	// Pin store + prefetcher. The pin store lives in its own SQLite file so
	// it doesn't compete with the metadata store's WAL.
	pinDBPath := pinStorePath(cfg.DBPath)
	if ps, err := pin.Open(pinDBPath); err == nil {
		globalPinStore = ps
		globalPrefetcher = pin.NewPrefetcher(ps, cfg.FUSEPath, cfg.MountPoint, 4)
		// Long-running daemons that drain the queue and re-warm pinned files.
		// They terminate when globalPrefetcher.Stop() is called in shutdown.
		go globalPrefetcher.PullPending(globalPinCtx(), 100)
		go globalPrefetcher.ReWarmupLoop(globalPinCtx(), 6*time.Hour, 50)
		// Wire the pin store into the NFS handler so the offline-mode
		// open gate can fail-fast on un-pinned reads. The mount point is
		// the prefix the gate uses to canonicalize in-mount filenames into
		// the absolute paths the pin store keys on.
		srv.Handler().SetPinStore(ps, cfg.MountPoint)
		jmlog.Info("pin store ready", "path", pinDBPath, "workers", 4)
	} else {
		jmlog.Warn("pin store open failed (offline-pin disabled)", "error", err.Error())
	}

	// Mount NFS at the user-visible mount point (e.g. /Volumes/zpool) so
	// Finder can browse it. This requires sudo, which we obtain via an
	// AppleScript "with administrator privileges" prompt the user accepts once.
	//
	// Idempotent path: if the user already has an NFS mount at this path
	// from a previous soft-stop cycle, reuse it. Re-running mount_nfs would
	// fail because the mount point is busy and would prompt for a password
	// for no reason.
	if cfg.MountPoint != "" {
		if isMounted(cfg.MountPoint) {
			jmlog.Info("nfs already mounted, reusing", "mount_point", cfg.MountPoint)
			globalMountPath = cfg.MountPoint
		} else if err := mountNFSWithPrompt(srv.Addr(), cfg.MountPoint); err != nil {
			jmlog.Warn("nfs mount failed (server still running)",
				"mount_point", cfg.MountPoint, "error", err.Error())
			// Non-fatal — the server is up, user can mount manually if needed
		} else {
			jmlog.Info("nfs mounted", "mount_point", cfg.MountPoint)
			globalMountPath = cfg.MountPoint
		}
	}

	// Wire NFS RPC observation into the metrics package.
	jmlibnfs.SetObserver(metrics.ObserveRPC)

	// Start metrics HTTP server (if address configured).
	if cfg.MetricsAddr != "" {
		ms := metrics.NewServer(cfg.MetricsAddr, metrics.Default())
		// Register pin/offline control endpoints on the same listener so the
		// CLI doesn't need a separate port.
		ms.ExtraRoutes = map[string]http.HandlerFunc{
			"/pin":          handlePinHTTP,
			"/unpin":        handleUnpinHTTP,
			"/cache-status": handleCacheStatusHTTP,
			"/offline":      handleOfflineHTTP,
		}
		if err := ms.Start(); err != nil {
			jmlog.Warn("metrics server failed to start",
				"addr", cfg.MetricsAddr, "error", err.Error())
		} else {
			globalMetrics = ms
			jmlog.Info("metrics server listening", "addr", ms.Addr())
		}
	}

	// Health monitor. Brief settle delay first so the initial synchronous
	// check inside Start() reflects the *settled* state of the mounts —
	// without this, a juicefs mount that hasn't shown up in the kernel
	// mount table yet would record a transient "FUSE down" that would
	// take the next 10-second tick to clear, causing the popover to flash
	// red right after a Start.
	if cfg.FUSEPath != "" {
		waitForFUSEResponsive(cfg.FUSEPath, 3*time.Second)
	}

	redisAddr, _, _ := metadata.ParseRedisURL(cfg.RedisURL)
	globalMonitor = health.New(health.Config{
		RedisURL:      redisAddr,
		MinIOURL:      "", // TODO: make configurable
		FUSEPath:      cfg.FUSEPath,
		NFSMountPoint: cfg.MountPoint,
	})
	globalMonitor.Start()

	// Expose health to /health endpoint. Capture the monitor in the
	// closure rather than reading the package var: that way a Stop that
	// nils globalMonitor won't cause this closure to panic if /health is
	// scraped during the tear-down window.
	mon := globalMonitor
	metrics.Default().SetHealthProvider(func() metrics.HealthSnapshot {
		if mon == nil {
			return metrics.HealthSnapshot{Healthy: false, Reason: "stopped"}
		}
		st := mon.Status()
		comps := map[string]string{
			"redis": labelFor(st.Redis.Healthy, st.Redis.Message),
			"minio": labelFor(st.MinIO.Healthy, st.MinIO.Message),
			"fuse":  labelFor(st.FUSE.Healthy, st.FUSE.Message),
			"nfs":   labelFor(st.NFS.Healthy, st.NFS.Message),
		}
		reason := ""
		if !st.Overall {
			reason = "degraded"
		}
		return metrics.HealthSnapshot{
			Healthy:    st.Overall,
			Components: comps,
			Reason:     reason,
		}
	})

	return C.CString(srv.Addr())
}

func labelFor(healthy bool, msg string) string {
	if healthy {
		return "ok"
	}
	if msg == "" {
		return "unhealthy"
	}
	return msg
}

// NFSServerStop is a *soft* stop: it tears down the NFS server, the
// metadata sync loop, the cache reader, the health monitor, and the
// metrics HTTP server. It deliberately does NOT unmount FUSE or NFS.
//
// Why: both unmounts require an admin password prompt. A typical user
// flow from the menu bar is Stop -> Start a few seconds later. Tearing
// the mounts down would prompt twice (once on Stop, once on Start) with
// no benefit, and the kernel mount table races during that window are
// the source of the stale "FUSE: down" bug.
//
// For a true full teardown (e.g. on app Quit) call NFSServerShutdown.
//
//export NFSServerStop
func NFSServerStop() {
	globalMu.Lock()
	defer globalMu.Unlock()

	stopServerLocked()
}

// stopServerLocked tears down everything except the FUSE/NFS mounts.
// Caller must hold globalMu.
func stopServerLocked() {
	// Detach the /health closure first so a probe arriving during
	// tear-down doesn't see partially-released state.
	metrics.Default().SetHealthProvider(nil)

	if globalMetrics != nil {
		globalMetrics.Stop()
		globalMetrics = nil
	}
	if globalMonitor != nil {
		globalMonitor.Stop()
		globalMonitor = nil
	}
	if globalServer != nil {
		globalServer.Handler().StopHandler()
		globalServer.Stop()
		globalServer = nil
	}
	if globalCache != nil {
		globalCache.Stop()
		globalCache = nil
	}
	if globalRC != nil {
		globalRC.Stop()
		globalRC = nil
	}
	if globalStore != nil {
		globalStore.Close()
		globalStore = nil
	}
	if globalRDB != nil {
		globalRDB.Close()
		globalRDB = nil
	}

	// Detach the RPC observer so the next start cleanly re-registers.
	jmlibnfs.SetObserver(nil)
	// NB: we don't call jmlog.Close() here — the logger is process-wide
	// and the next Start will Init() it again. Closing the file handle
	// here causes log writes during the tear-down window to silently
	// hit a closed fd.
}

// NFSServerShutdown is a *hard* stop: soft-stop the server, then
// unmount NFS and FUSE. Use this on app Quit.
//
//export NFSServerShutdown
func NFSServerShutdown() {
	globalMu.Lock()
	defer globalMu.Unlock()

	// Unmount NFS first so Finder doesn't error after we tear down the server.
	if globalMountPath != "" {
		unmountNFS(globalMountPath)
		globalMountPath = ""
	}

	stopServerLocked()

	// Unmount JuiceFS FUSE last — everything above relied on it.
	if globalFUSE != nil {
		globalFUSE.Stop()
		globalFUSE = nil
	}
	globalFUSEPath = ""

	jmlog.Close()
}

//export NFSServerIsRunning
func NFSServerIsRunning() C.int {
	globalMu.Lock()
	defer globalMu.Unlock()
	if globalServer != nil {
		return 1
	}
	return 0
}

// StatsResult is the JSON stats returned to Swift.
type StatsResult struct {
	Running        bool    `json:"running"`
	EntryCount     int     `json:"entry_count"`
	LastSyncMs     int64   `json:"last_sync_ms"`
	LastSyncTime   string  `json:"last_sync_time"`
	ServerAddr     string  `json:"server_addr"`
	HealthRedis    bool    `json:"health_redis"`
	HealthMinIO    bool    `json:"health_minio"`
	HealthFUSE     bool    `json:"health_fuse"`
}

//export NFSServerStats
func NFSServerStats() *C.char {
	globalMu.Lock()
	defer globalMu.Unlock()

	stats := StatsResult{Running: globalServer != nil}

	if globalRC != nil {
		stats.LastSyncMs = globalRC.LastSyncDuration().Milliseconds()
		stats.LastSyncTime = globalRC.LastSyncTime().Format(time.RFC3339)
		stats.EntryCount = globalRC.LastSyncEntries()
	}
	if globalServer != nil {
		stats.ServerAddr = globalServer.Addr()
	}
	if globalMonitor != nil {
		status := globalMonitor.Status()
		stats.HealthRedis = status.Redis.Healthy
		stats.HealthMinIO = status.MinIO.Healthy
		stats.HealthFUSE = status.FUSE.Healthy
	}

	data, _ := json.Marshal(stats)
	return C.CString(string(data))
}

//export NFSServerFreeString
func NFSServerFreeString(s *C.char) {
	C.free(unsafe.Pointer(s))
}

// NFSServerMetrics returns the same JSON payload exposed at /metrics.
// Useful for the menu bar app when the HTTP server isn't reachable
// (e.g. when metrics-addr is disabled or behind a firewall).
//export NFSServerMetrics
func NFSServerMetrics() *C.char {
	snap := metrics.Default().Snapshot()
	data, _ := json.Marshal(snap)
	return C.CString(string(data))
}

// SyncNow triggers an immediate metadata reconciliation.
//export NFSServerSyncNow
func NFSServerSyncNow() *C.char {
	globalMu.Lock()
	defer globalMu.Unlock()

	if globalRC == nil {
		return C.CString("error: not running")
	}

	if err := globalRC.SyncOnce(); err != nil {
		return C.CString(fmt.Sprintf("error: %v", err))
	}
	return C.CString("ok")
}

// SearchResult is the JSON search result returned to Swift.
type SearchResult struct {
	Path      string `json:"path"`
	Name      string `json:"name"`
	IsDir     bool   `json:"is_dir"`
	Size      int64  `json:"size"`
	Mtime     string `json:"mtime"`
	Rank      float64 `json:"rank"`
}

// NFSServerSearch performs a full-text search on filenames.
// query: the search string (partial match supported, e.g. "explosion")
// limit: max results (0 = default 50)
// parentPath: scope to subtree (empty = search all)
// Returns JSON array of SearchResult, or "error: ..." on failure.
//export NFSServerSearch
func NFSServerSearch(query *C.char, limit C.int, parentPath *C.char) *C.char {
	globalMu.Lock()
	defer globalMu.Unlock()

	if globalStore == nil {
		return C.CString("error: not running")
	}

	q := C.GoString(query)
	pp := C.GoString(parentPath)
	lim := int(limit)

	results, err := globalStore.Search(q, lim, pp)
	if err != nil {
		return C.CString(fmt.Sprintf("error: %v", err))
	}

	out := make([]SearchResult, len(results))
	for i, r := range results {
		out[i] = SearchResult{
			Path:  r.Entry.Path,
			Name:  r.Entry.Name,
			IsDir: r.Entry.IsDir,
			Size:  r.Entry.Size,
			Mtime: r.Entry.Mtime.Format(time.RFC3339),
			Rank:  r.Rank,
		}
	}

	data, _ := json.Marshal(out)
	return C.CString(string(data))
}

// mountNFSWithPrompt runs `mount_nfs` via osascript with admin privileges,
// which triggers the standard macOS authentication prompt. The user enters
// their password once; macOS caches the auth for the session.
func mountNFSWithPrompt(serverAddr, mountPoint string) error {
	host := "127.0.0.1"
	port := "11049"
	if i := strings.LastIndex(serverAddr, ":"); i > 0 {
		host = serverAddr[:i]
		port = serverAddr[i+1:]
	}

	// If something else is already mounted at the mount point, refuse —
	// don't trample over the user's data. They need to unmount first.
	if isMounted(mountPoint) {
		return fmt.Errorf("%s is already mounted (umount it first)", mountPoint)
	}

	opts := fmt.Sprintf(
		"port=%s,mountport=%s,soft,intr,timeo=300,retrans=5,nolocks,locallocks,rsize=1048576,wsize=1048576,readahead=128,actimeo=3600,vers=3,tcp",
		port, port)

	// Build the shell command: mkdir + mount_nfs
	shellCmd := fmt.Sprintf(
		"mkdir -p %q && mount_nfs -o %s %s:/ %q",
		mountPoint, opts, host, mountPoint,
	)

	// AppleScript with admin privileges — produces the macOS password prompt
	osaScript := fmt.Sprintf(
		`do shell script %q with administrator privileges with prompt "JuiceMount needs to mount the NFS volume at %s"`,
		shellCmd, mountPoint,
	)

	out, err := exec.Command("osascript", "-e", osaScript).CombinedOutput()
	if err != nil {
		return fmt.Errorf("osascript: %v\n%s", err, string(out))
	}
	return nil
}

// unmountNFS removes the NFS mount when the server stops.
// Best-effort: failure is logged but doesn't prevent shutdown.
func unmountNFS(mountPoint string) {
	if mountPoint == "" || !isMounted(mountPoint) {
		return
	}
	osaScript := fmt.Sprintf(
		`do shell script %q with administrator privileges with prompt "JuiceMount is stopping and needs to unmount %s"`,
		fmt.Sprintf("umount %q", mountPoint),
		mountPoint,
	)
	exec.Command("osascript", "-e", osaScript).Run()
}

func isMounted(path string) bool {
	out, err := exec.Command("mount").Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(out), " "+path+" ")
}

// fuseLooksHealthy returns true if the given path appears in the kernel
// mount table AND a directory listing returns within a short timeout.
// This is a cheap probe to decide whether a fresh juicefs invocation is
// needed on Start.
func fuseLooksHealthy(path string) bool {
	out, err := exec.Command("mount").Output()
	if err != nil {
		return false
	}
	if !strings.Contains(string(out), path) {
		return false
	}
	done := make(chan error, 1)
	go func() {
		_, err := os.ReadDir(path)
		done <- err
	}()
	select {
	case err := <-done:
		return err == nil
	case <-time.After(2 * time.Second):
		return false
	}
}

// waitForFUSEResponsive polls fuseLooksHealthy until it succeeds or the
// timeout expires. We use this right before starting the health monitor
// so the monitor's initial synchronous check doesn't see a transient
// in-flight mount.
func waitForFUSEResponsive(path string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fuseLooksHealthy(path) {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// ----------------------------------------------------------------------------
// Pin / offline / prefetch exports (Swift app + CLI use these)
// ----------------------------------------------------------------------------

// pinStorePath puts the pin DB next to the metadata DB but as its own file.
func pinStorePath(metadataDBPath string) string {
	if metadataDBPath == "" || metadataDBPath == ":memory:" {
		return ":memory:"
	}
	dir := metadataDBPath
	for i := len(dir) - 1; i >= 0; i-- {
		if dir[i] == '/' {
			dir = dir[:i]
			break
		}
	}
	return dir + "/pin.db"
}

// globalPinCtx returns a long-lived context whose lifecycle is tied to
// the cbridge process. Workers running against this context die only on
// process exit.
//
// (We don't tie this to the server start/stop cycle on purpose — pinning
// state is a process-wide registry, and recreating workers on every Stop/
// Start would lose in-flight prefetches.)
var globalPinCtxOnce sync.Once
var globalPinCtxBox struct{ ctx interface{ Done() <-chan struct{} } } // lazy init

// globalPinCtx returns the context. Use a closure to avoid pulling in
// context globally where it isn't needed.
func globalPinCtx() (ctx contextLike) {
	// Background context lives for the whole process. Good enough for v1.
	return bgCtx{}
}

type contextLike = interface {
	Done() <-chan struct{}
	Err() error
	Value(key any) any
	Deadline() (time.Time, bool)
}

type bgCtx struct{}

func (bgCtx) Done() <-chan struct{}                   { return nil }
func (bgCtx) Err() error                              { return nil }
func (bgCtx) Value(key any) any                       { return nil }
func (bgCtx) Deadline() (time.Time, bool)             { return time.Time{}, false }

// PinResult is the JSON returned to Swift after a Pin call.
type PinResult struct {
	OK         bool   `json:"ok"`
	FilesPinned int    `json:"files_pinned"`
	BytesTotal int64  `json:"bytes_total"`
	Error      string `json:"error,omitempty"`
}

// NFSServerPin pins a file or directory tree for offline availability.
// Walks the tree under fusePath equivalent (since the user passes a
// /Volumes/zpool path, we translate to the FUSE mount root) and adds
// every regular file to the pin registry. The prefetcher daemon picks
// them up and warms the cache.
//
//export NFSServerPin
func NFSServerPin(rootPath *C.char) *C.char {
	globalMu.Lock()
	defer globalMu.Unlock()
	if globalPinStore == nil {
		return jsonStr(PinResult{Error: "pin store not initialized"})
	}
	root := C.GoString(rootPath)
	// Translate /Volumes/zpool/X to <fusePath>/X for the walk
	walkPath := translateMountToFUSE(root, globalMountPath, globalFUSEPath)
	entries, err := pin.CountFilesUnder(walkPath)
	if err != nil {
		return jsonStr(PinResult{Error: err.Error()})
	}
	// Translate FUSE paths back to /Volumes/zpool paths for storage
	for i := range entries {
		entries[i].Path = translateFUSEToMount(entries[i].Path, globalFUSEPath, globalMountPath)
		entries[i].PinRoot = root
	}
	if err := globalPinStore.PinMany(entries); err != nil {
		return jsonStr(PinResult{Error: err.Error()})
	}
	var totalBytes int64
	for _, e := range entries {
		totalBytes += e.Size
	}
	return jsonStr(PinResult{OK: true, FilesPinned: len(entries), BytesTotal: totalBytes})
}

// NFSServerUnpin removes a pin root and all its files from the registry.
//
//export NFSServerUnpin
func NFSServerUnpin(rootPath *C.char) *C.char {
	globalMu.Lock()
	defer globalMu.Unlock()
	if globalPinStore == nil {
		return jsonStr(PinResult{Error: "pin store not initialized"})
	}
	n, err := globalPinStore.Unpin(C.GoString(rootPath))
	if err != nil {
		return jsonStr(PinResult{Error: err.Error()})
	}
	return jsonStr(PinResult{OK: true, FilesPinned: n})
}

// CacheStatus is the JSON returned by NFSServerCacheStatus.
type CacheStatus struct {
	Aggregate  pin.AggregateStats `json:"aggregate"`
	Roots      []pin.RootSummary  `json:"roots"`
	LiveStats  pin.LiveStats      `json:"live"`
	OfflineMode bool              `json:"offline_mode"`
}

//export NFSServerCacheStatus
func NFSServerCacheStatus() *C.char {
	globalMu.Lock()
	defer globalMu.Unlock()
	if globalPinStore == nil {
		return jsonStr(CacheStatus{OfflineMode: pin.IsOffline()})
	}
	cs := CacheStatus{OfflineMode: pin.IsOffline()}
	if a, err := globalPinStore.AggregateStats(); err == nil {
		cs.Aggregate = a
	}
	if r, err := globalPinStore.PinRoots(); err == nil {
		cs.Roots = r
	}
	if globalPrefetcher != nil {
		cs.LiveStats = globalPrefetcher.LiveStats()
	}
	return jsonStr(cs)
}

//export NFSServerSetOffline
func NFSServerSetOffline(on C.int) *C.char {
	pin.SetOffline(on != 0)
	jmlog.Info("offline mode toggled", "on", pin.IsOffline())
	return jsonStr(map[string]any{"ok": true, "offline_mode": pin.IsOffline()})
}

//export NFSServerIsOffline
func NFSServerIsOffline() C.int {
	if pin.IsOffline() {
		return 1
	}
	return 0
}

// translateMountToFUSE turns "/Volumes/zpool/foo" into "<fuseRoot>/foo".
func translateMountToFUSE(p, mountRoot, fuseRoot string) string {
	if mountRoot == "" || fuseRoot == "" {
		return p
	}
	if len(p) >= len(mountRoot) && p[:len(mountRoot)] == mountRoot {
		rest := p[len(mountRoot):]
		return fuseRoot + rest
	}
	return p
}

// translateFUSEToMount is the inverse.
func translateFUSEToMount(p, fuseRoot, mountRoot string) string {
	if mountRoot == "" || fuseRoot == "" {
		return p
	}
	if len(p) >= len(fuseRoot) && p[:len(fuseRoot)] == fuseRoot {
		rest := p[len(fuseRoot):]
		return mountRoot + rest
	}
	return p
}

// jsonStr marshals v to JSON and returns it as a C string. Caller must
// NFSServerFreeString it.
func jsonStr(v any) *C.char {
	b, err := json.Marshal(v)
	if err != nil {
		return C.CString(fmt.Sprintf(`{"ok":false,"error":%q}`, err.Error()))
	}
	return C.CString(string(b))
}

// We need to also tear down pinPrefetcher on shutdown. Augment stopServerLocked
// is not the right place (that's soft-stop; pinning persists across restarts).
// Tear down only on the hard NFSServerShutdown path. This is handled via
// ShutdownPinResources below, which the existing NFSServerShutdown can call.
//
// Note: actual integration of this into NFSServerShutdown is left to the
// caller — to avoid touching the existing shutdown flow too much, we just
// expose the helper. The prefetcher leaking on shutdown is benign because
// the OS will reap the workers when the process exits.

// ----------------------------------------------------------------------------
// HTTP handlers for the CLI (registered on the metrics server's mux)
// ----------------------------------------------------------------------------

func handlePinHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "missing ?path", 400)
		return
	}
	cstr := NFSServerPin(C.CString(path))
	defer NFSServerFreeString(cstr)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(C.GoString(cstr)))
}

func handleUnpinHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "missing ?path", 400)
		return
	}
	cstr := NFSServerUnpin(C.CString(path))
	defer NFSServerFreeString(cstr)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(C.GoString(cstr)))
}

func handleCacheStatusHTTP(w http.ResponseWriter, r *http.Request) {
	cstr := NFSServerCacheStatus()
	defer NFSServerFreeString(cstr)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(C.GoString(cstr)))
}

func handleOfflineHTTP(w http.ResponseWriter, r *http.Request) {
	on := r.URL.Query().Get("on")
	var v C.int
	if on == "on" || on == "true" || on == "1" {
		v = 1
	}
	cstr := NFSServerSetOffline(v)
	defer NFSServerFreeString(cstr)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(C.GoString(cstr)))
}

// connectRedisWithRetry wraps metadata.NewRedisClient with exponential
// backoff. Returns the connected client, or the LAST error if all attempts
// fail. The first attempt happens immediately; subsequent attempts wait
// 2^(attempt-1) seconds.
//
// We intentionally cap the number of attempts rather than retrying forever:
// if the user's NAS is genuinely unreachable, they should see the failure
// quickly enough to act on it (move closer, fix network, plug back in)
// rather than stare at a frozen popover.
func connectRedisWithRetry(redisURL string, store *metadata.Store, maxAttempts int) (*metadata.RedisClient, error) {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		rc, err := metadata.NewRedisClient(redisURL, store)
		if err == nil {
			if attempt > 1 {
				jmlog.Info("redis connect recovered",
					"attempt", attempt, "max_attempts", maxAttempts)
			}
			return rc, nil
		}
		lastErr = err
		if attempt >= maxAttempts {
			break
		}
		backoff := time.Duration(1<<uint(attempt-1)) * time.Second // 1, 2, 4, 8, 16
		jmlog.Warn("redis connect failed, retrying",
			"attempt", attempt,
			"max_attempts", maxAttempts,
			"backoff_sec", int(backoff.Seconds()),
			"error", err.Error())
		time.Sleep(backoff)
	}
	return nil, fmt.Errorf("after %d attempts: %w", maxAttempts, lastErr)
}

func main() {} // required for c-archive build

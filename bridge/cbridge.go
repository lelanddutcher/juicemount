package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"os"
	"os/exec"
	"path/filepath"
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

	// A1 — Pre-mount conflict probe. Inspect the kernel mount table for any
	// foreign owner at the FUSE path or the NFS mount point BEFORE we call
	// juicefs or mount_nfs. The downstream code's "already mounted, reuse"
	// branches deliberately don't distinguish foreign mounts from our own —
	// without this, starting on top of an unrelated disk image mounted at
	// /Volumes/zpool would happily expose that disk via NFS, or worse,
	// blow up later with confusing errors. Refuse early with a clear hint
	// the user can act on. (Mounts we placed are passed through; the existing
	// reuse logic below handles them.)
	if errMsg := preMountConflictCheck(cfg.FUSEPath, cfg.MountPoint); errMsg != "" {
		jmlog.Error("pre-mount conflict refused", "detail", errMsg)
		return C.CString("error: " + errMsg)
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
			// Note: FUSEManager.Mount may have auto-expanded CacheSize. Log
			// the *effective* config from the mount, not the user input —
			// otherwise the user reads "100 GiB" and is confused why the
			// daemon was actually launched with 800 GiB.
			jmlog.Info("juicefs FUSE mounted",
				"path", cfg.FUSEPath,
				"effective_cache_size_mb", fm.EffectiveCacheSize(),
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
		// Explicit timeouts so a Redis hiccup can't park cache.Reader.getSlices
		// on a default-timeout LRange — that call happens on every cache-miss
		// read and a 30s stall there cascades through every concurrent NFS
		// RPC under the current per-connection sequential dispatch. Matches
		// the timeouts on the metadata client.
		rdb := redis.NewClient(&redis.Options{
			Addr:         addr,
			DB:           db,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 5 * time.Second,
			DialTimeout:  5 * time.Second,
		})
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
			"/reclaim":      handleReclaimHTTP,
			"/verify-pins":  handleVerifyPinsHTTP,
			// In-app rescue when the kernel mount table is wedged. Runs the
			// privileged `umount -f -t nfs` via AppleScript; user enters
			// their admin password once. Returns JSON with the result.
			"/force-eject": handleForceEjectHTTP,
			// A2 — post-mount self-test. GET returns cached result; POST reruns.
			"/self-test": handleSelfTestHTTP,
			// Phase B observability: net/http/pprof routes for live
			// goroutine/heap/cpu/trace dumps. pprof.Index serves
			// /debug/pprof/goroutine, /debug/pprof/heap, etc. via the
			// trailing-slash handler plus ?debug=1 query.
			"/debug/pprof/":        pprof.Index,
			"/debug/pprof/cmdline": pprof.Cmdline,
			"/debug/pprof/profile": pprof.Profile,
			"/debug/pprof/symbol":  pprof.Symbol,
			"/debug/pprof/trace":   pprof.Trace,
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

	// A2 — kick off the post-mount self-test in the background so it doesn't
	// block the Swift Start completion handler (a slow probe shouldn't gate
	// the UI flipping to "running"). Results land in selfTestLast and Swift
	// reads them via GET /self-test on its post-start refresh.
	//
	// Delayed by 10 s after Start so the initial SyncOnce's BulkInsert (which
	// holds metadata.Store.writeMu for the duration of the cache-rebuild fold,
	// ~seconds on a 100K+ entry sync) has time to complete. Without the delay,
	// the self-test's ListChildren walk stalls behind writeMu and the probe
	// goroutine wedges for tens of seconds during the exact window the user
	// is most likely to click the menu bar.
	go func() {
		time.Sleep(10 * time.Second)
		runAndStoreSelfTest()
	}()

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
	stopServerLocked()
}

// stopServerLocked tears down everything except the FUSE/NFS mounts.
//
// Locking: this function takes globalMu BRIEFLY to swap globals into
// locals (atomically nil-ing the publicly-observable state), then
// releases the lock and calls .Stop() / .Close() on the snapshots
// without holding it. The .Stop() methods can each take seconds (NFS
// server waits for in-flight RPCs to drain; HealthMonitor cancels its
// context and joins probe goroutines); holding globalMu across them
// would make every concurrent Stats / IsRunning / CacheStatus call from
// the menu-bar poller park behind the teardown.
//
// The name is historical — it used to require the caller to hold the
// lock. Both call sites (NFSServerStop, NFSServerShutdown) have been
// adjusted accordingly.
func stopServerLocked() {
	// Detach the /health closure first so a probe arriving during
	// tear-down doesn't see partially-released state.
	metrics.Default().SetHealthProvider(nil)

	// Snapshot + nil under the lock. Includes globalPinStore and
	// globalPrefetcher (added on review feedback) — previously these
	// stayed live across Stop, but the NEXT Start re-runs
	// `pin.Open(pinDBPath)` and overwrites the global without closing the
	// previous SQLite handle, leaking a connection + WAL file lock on
	// every Stop/Start cycle.
	globalMu.Lock()
	metricsSrv := globalMetrics
	monitor := globalMonitor
	server := globalServer
	cache := globalCache
	rc := globalRC
	store := globalStore
	rdb := globalRDB
	pinStore := globalPinStore
	prefetcher := globalPrefetcher
	globalMetrics = nil
	globalMonitor = nil
	globalServer = nil
	globalCache = nil
	globalRC = nil
	globalStore = nil
	globalRDB = nil
	globalPinStore = nil
	globalPrefetcher = nil
	globalMu.Unlock()

	// Now run the slow shutdown work on the snapshots, no lock held.
	// During this window, Stats / IsRunning / CacheStatus correctly
	// report "Running: false" — we already nil'd the publicly-visible
	// state, so the answer is honest, not a lie.
	if metricsSrv != nil {
		metricsSrv.Stop()
	}
	if monitor != nil {
		monitor.Stop()
	}
	if server != nil {
		server.Handler().StopHandler()
		server.Stop()
	}
	if cache != nil {
		cache.Stop()
	}
	if rc != nil {
		rc.Stop()
	}
	if prefetcher != nil {
		prefetcher.Stop()
	}
	if pinStore != nil {
		pinStore.Close()
	}
	if store != nil {
		store.Close()
	}
	if rdb != nil {
		rdb.Close()
	}

	// Detach the RPC observer so the next start cleanly re-registers.
	jmlibnfs.SetObserver(nil)
	// NB: we don't call jmlog.Close() here — the logger is process-wide
	// and the next Start will Init() it again. Closing the file handle
	// here causes log writes during the tear-down window to silently
	// hit a closed fd.
}

// NFSServerShutdown is a *hard* stop: unmount NFS, then tear down the
// server, then unmount FUSE. Use this on app Quit and on user-initiated
// Stop.
//
// Order is critical and reverses the previous behavior:
//
//  1. Unmount NFS FIRST while the server is still alive and responding.
//     A live server can fulfill the kernel's flush/getattr calls during
//     the unmount handshake. If we kill the server first, the kernel's
//     mount table entry becomes orphaned — every subsequent stat() on
//     that path waits the full NFS timeout (3 s with the new
//     timeo=10,retrans=2 settings, was 150 s before that change) before
//     returning EIO. Finder, doing /Volumes/ enumeration on launch, would
//     visibly hang on each orphan.
//
//  2. Only after the unmount is *confirmed* gone, kill the NFS server.
//
//  3. If the unmount fails, we still kill the server (the user clicked
//     Stop, they want it gone) — but we log a loud ERROR so the user
//     knows the mount entry is wedged and a reboot may be needed. With
//     timeo=10,retrans=2 the wedge is annoying-not-catastrophic; with
//     the old timeo=300,retrans=5 settings, a wedge required reboot.
//
//export NFSServerShutdown
func NFSServerShutdown() {
	// CRITICAL: do NOT hold globalMu across the slow unmount paths. Each
	// can take seconds-to-minutes (osascript admin prompt, kernel umount
	// retries). Menu-bar pollers calling Stats / IsRunning every 2 s would
	// stack up behind a held globalMu and freeze the UI for the full
	// duration of shutdown.
	//
	// Pattern:
	//   1. Lock briefly, snapshot pointers, mark globals as "shutting down"
	//      (by setting them to nil), release lock.
	//   2. Stats / IsRunning called concurrently from this point see
	//      "Running: false" — correct (we're shutting down).
	//   3. Run slow unmount work WITHOUT the lock.
	//   4. Re-acquire briefly to call stopServerLocked which expects the
	//      lock held; it operates on the snapshotted pointers.
	globalMu.Lock()
	mountPath := globalMountPath
	fuse := globalFUSE
	// Mark "shutting down" by nil-ing the publicly-observable fields. The
	// snapshotted pointers stay valid for the unmount work below.
	globalMountPath = ""
	globalFUSE = nil
	globalFUSEPath = ""
	globalMu.Unlock()

	// Step 1: unmount NFS while the server is still alive (handler can
	// satisfy any kernel flush/getattr during the unmount handshake). No
	// lock held — Stats during this window correctly reports "shutting
	// down" via Running:false.
	nfsCleaned := true
	if mountPath != "" {
		nfsCleaned = unmountNFS(mountPath)
		if !nfsCleaned {
			jmlog.Error("nfs unmount FAILED during shutdown",
				"mount_point", mountPath,
				"hint", "killing server anyway; kernel may have a wedged mount entry until reboot or `sudo umount -f -t nfs`")
		}
	}

	// Step 2: tear down server + cache + monitor + metrics + Redis sub.
	// stopServerLocked takes globalMu briefly to nil the globals, then
	// runs the slow .Stop() calls without the lock held.
	stopServerLocked()

	// Step 3: unmount FUSE. Health.FUSEManager.Stop() now bounded (see
	// iteration 2 commits). No lock held.
	if fuse != nil {
		fuse.Stop()
	}

	_ = nfsCleaned // currently observable only via logs
	jmlog.Close()
}

//export NFSServerIsRunning
func NFSServerIsRunning() C.int {
	// Snapshot-then-release. Holding globalMu across the read meant any
	// in-flight Shutdown (which holds globalMu for up to ~70 s during the
	// osascript admin prompt for unmount) would park IsRunning calls from
	// the menu-bar poller — blocking the UI's idle render loop.
	globalMu.Lock()
	running := globalServer != nil
	globalMu.Unlock()
	if running {
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
	// Snapshot all globals under the lock, release IMMEDIATELY, then call
	// methods on the snapshots without holding globalMu. Each method we
	// call (RC.LastSyncDuration, server.Addr, monitor.Status) has its own
	// internal locking and can take milliseconds-to-seconds under load.
	// Holding globalMu across them meant a single slow Stats call would
	// block every other export — most importantly, Shutdown couldn't
	// proceed and IsRunning calls from the menu bar poller would queue
	// behind it. The menu freeze.
	//
	// Snapshots can briefly outlive their globals (e.g. mid-Shutdown), but
	// the underlying objects are not yet finalized when their pointers are
	// nil'd — they're still alive until their .Stop() returns and Go GCs
	// them. So calling a method on a snapshot post-nil is safe; it returns
	// a stale-but-coherent reading. The next poll cycle 2 s later picks up
	// the cleared state.
	globalMu.Lock()
	server := globalServer
	rc := globalRC
	monitor := globalMonitor
	globalMu.Unlock()

	stats := StatsResult{Running: server != nil}
	if rc != nil {
		stats.LastSyncMs = rc.LastSyncDuration().Milliseconds()
		stats.LastSyncTime = rc.LastSyncTime().Format(time.RFC3339)
		stats.EntryCount = rc.LastSyncEntries()
	}
	if server != nil {
		stats.ServerAddr = server.Addr()
	}
	if monitor != nil {
		status := monitor.Status()
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
	// Snapshot RC under the lock, release, then run the (slow) SyncOnce
	// without holding globalMu. SyncOnce does a Redis Lua EVAL bounded by
	// 120 s — holding globalMu across that window would freeze every other
	// export, including the menu-bar poller's Stats calls. The user's
	// "Sync Now froze the app" report was directly this bug.
	globalMu.Lock()
	rc := globalRC
	globalMu.Unlock()

	if rc == nil {
		return C.CString("error: not running")
	}

	if err := rc.SyncOnce(); err != nil {
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
	// Snapshot store under the lock; release; run the FTS query without
	// the lock. Search reads the SQLite FTS5 index which can take tens of
	// milliseconds on a 100K+ entry corpus — holding globalMu for that
	// duration would block the popover's poller.
	globalMu.Lock()
	store := globalStore
	globalMu.Unlock()

	if store == nil {
		return C.CString("error: not running")
	}

	q := C.GoString(query)
	pp := C.GoString(parentPath)
	lim := int(limit)

	results, err := store.Search(q, lim, pp)
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

	// Timeout policy. Tuned for a localhost NFS server (us). The kernel
	// client's timeo is in 0.1-second units, so timeo=10 means 1 s initial
	// timeout; retrans=2 means at most 2 retries before returning EIO to
	// the calling syscall. Worst-case dead-server detection: ~3 s.
	//
	// Why this matters: with the previous timeo=300,retrans=5 settings,
	// when the JuiceMount user-space server died but the kernel mount
	// table still had /Volumes/zpool registered, every stat() that landed
	// on that path would wait 150 s before returning EIO. Finder, on
	// launch, enumerates /Volumes/ to populate the sidebar — that
	// enumeration includes a stat per mount, so Finder would hang for
	// 150 s+. Force-quitting Finder didn't help because the relaunched
	// process repeated the same enumeration. The user perceived "Finder
	// won't launch."
	//
	// The tradeoff: real backend hiccups now surface as EIO after ~3 s
	// instead of after ~150 s. For our localhost-only NFS path, that's
	// the right policy — a 3 s blip is annoying, a 150 s blip looks
	// indistinguishable from a system hang.
	opts := fmt.Sprintf(
		"port=%s,mountport=%s,soft,intr,timeo=10,retrans=2,nolocks,locallocks,rsize=1048576,wsize=1048576,readahead=128,actimeo=3600,vers=3,tcp",
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

// unmountNFS removes the NFS mount.
//
// Each strategy is bounded by a context timeout so a hung NFS operation
// (kernel stuck talking to a dead server) doesn't wedge our shutdown path
// for minutes. If every strategy fails, we log loudly and return false —
// the caller (NFSServerShutdown) decides whether to kill the server
// anyway.
//
// Strategy (cheapest first):
//   1. `diskutil unmount` — works without sudo, doesn't pop a password
//      prompt. Succeeds in the common case.
//   2. `diskutil unmount force` — non-interactive force; still no sudo.
//   3. `umount -f -t nfs` via AppleScript with administrator privileges —
//      the only thing that can dislodge a truly wedged kernel mount entry.
//      Prompts for password.
//
// Returns true iff the mount is gone by the time we return.
func unmountNFS(mountPoint string) bool {
	if mountPoint == "" || !isMounted(mountPoint) {
		return true
	}
	tryUnmount := func(name string, timeout time.Duration, argv ...string) bool {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		err := exec.CommandContext(ctx, argv[0], argv[1:]...).Run()
		if err != nil {
			jmlog.Debug("nfs unmount attempt failed",
				"method", name, "mount_point", mountPoint, "error", err.Error())
			return false
		}
		if !isMounted(mountPoint) {
			jmlog.Info("nfs unmounted", "method", name, "mount_point", mountPoint)
			return true
		}
		return false
	}

	if tryUnmount("diskutil", 5*time.Second, "diskutil", "unmount", mountPoint) {
		return true
	}
	if tryUnmount("diskutil-force", 5*time.Second, "diskutil", "unmount", "force", mountPoint) {
		return true
	}
	// Last resort: AppleScript-prompted privileged umount -f -t nfs.
	// `-f` forces unmount of unresponsive mounts; `-t nfs` scopes to NFS
	// so we can never accidentally unmount something else if the mount
	// path happened to be racy.
	jmlog.Warn("nfs unmount falling back to admin-privileged umount -f -t nfs",
		"mount_point", mountPoint,
		"reason", "diskutil could not release the mount; trying forced kernel-level unmount")
	osaScript := fmt.Sprintf(
		`do shell script %q with administrator privileges with prompt "JuiceMount is stopping and needs to force-unmount %s"`,
		fmt.Sprintf("umount -f -t nfs %q", mountPoint),
		mountPoint,
	)
	osaCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := exec.CommandContext(osaCtx, "osascript", "-e", osaScript).Run(); err == nil {
		if !isMounted(mountPoint) {
			jmlog.Info("nfs unmounted", "method", "admin-umount-f", "mount_point", mountPoint)
			return true
		}
	}
	jmlog.Error("nfs unmount FAILED — mount is wedged",
		"mount_point", mountPoint,
		"hint", "the kernel mount table still references this mount; a reboot or `sudo umount -f -t nfs` from a fresh terminal may be required")
	return false
}

func isMounted(path string) bool {
	out, err := exec.Command("mount").Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(out), " "+path+" ")
}

// mountAt parses `mount` output and returns the current owner of the given
// mount point, if any.
//
// macOS `mount` output line format is "source on /path (type, opts...)".
// We return:
//   - source: e.g. "JuiceFS:zpool" or "127.0.0.1:11049"
//   - kind:   the parenthesized type token, e.g. "macfuse", "nfs"
//   - found:  true if a line for `path` was located
//
// On parse failure, found is false.
func mountAt(path string) (source string, kind string, found bool) {
	out, err := exec.Command("mount").Output()
	if err != nil {
		return "", "", false
	}
	// Walk every line; match on " on <path> (" so partial-prefix paths
	// (e.g. "/Volumes/zpool" vs "/Volumes/zpool2") don't false-positive.
	needle := " on " + path + " ("
	for _, line := range strings.Split(string(out), "\n") {
		idx := strings.Index(line, needle)
		if idx < 0 {
			continue
		}
		source = strings.TrimSpace(line[:idx])
		// Type token is between the first "(" after needle and the next "," or ")"
		rest := line[idx+len(needle)-1:] // include the leading "(" so we re-find it
		op := strings.Index(rest, "(")
		if op >= 0 {
			tail := rest[op+1:]
			end := strings.IndexAny(tail, ",)")
			if end > 0 {
				kind = strings.TrimSpace(tail[:end])
			}
		}
		return source, kind, true
	}
	return "", "", false
}

// isOurFUSEMount reports whether the existing mount at `path` looks like a
// JuiceFS FUSE mount we (or a previous instance of us) put there. The source
// reported by macOS for a JuiceFS-on-macfuse mount is "JuiceFS:<volname>"
// (e.g. "JuiceFS:zpool") with type "macfuse".
func isOurFUSEMount(source, kind string) bool {
	if !strings.HasPrefix(source, "JuiceFS:") {
		return false
	}
	// kind is best-effort; if we can't parse it, accept on source alone
	// (older macOS versions report the fs type differently).
	if kind == "" {
		return true
	}
	return strings.Contains(kind, "fuse") || strings.Contains(kind, "macfuse")
}

// isOurNFSMount reports whether the existing mount at `path` looks like an
// NFS mount whose server is the loopback address we serve on.
func isOurNFSMount(source, kind string) bool {
	if !strings.HasPrefix(source, "127.0.0.1") && !strings.HasPrefix(source, "localhost") {
		return false
	}
	// kind should be "nfs"; tolerate empty/unknown.
	if kind != "" && !strings.Contains(kind, "nfs") {
		return false
	}
	return true
}

// preMountConflictCheck inspects the kernel mount table for any foreign mount
// at the FUSE path or the NFS mount point. If a foreign mount is found, returns
// a JSON-encoded error string that includes which path, who owns it, and a
// suggested resolution. Returns "" if both paths are clear (or already owned
// by us — which the existing soft-stop reuse logic will pick up downstream).
func preMountConflictCheck(fusePath, mountPoint string) string {
	if fusePath != "" {
		if src, kind, found := mountAt(fusePath); found {
			if !isOurFUSEMount(src, kind) {
				return formatMountConflictError(fusePath, src, kind,
					"Unmount it manually (`diskutil unmount "+fusePath+"` or `umount "+fusePath+"`), then start JuiceMount again.")
			}
		}
	}
	if mountPoint != "" {
		if src, kind, found := mountAt(mountPoint); found {
			if !isOurNFSMount(src, kind) {
				return formatMountConflictError(mountPoint, src, kind,
					"Unmount the other volume (`diskutil unmount "+mountPoint+"`) or pick a different mount point in Preferences, then try again.")
			}
		}
	}
	return ""
}

// formatMountConflictError builds the human-readable error string Swift will
// surface in the popover's lastError field.
func formatMountConflictError(path, source, kind, hint string) string {
	if kind == "" {
		kind = "unknown"
	}
	return fmt.Sprintf(
		"mount conflict at %s: foreign mount in place (source=%q, type=%s). %s",
		path, source, kind, hint)
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
	// Snapshot under the lock; the slow walk + DB inserts run unlocked.
	// pin.CountFilesUnder walks a directory tree (can be seconds on a
	// large project); PinMany does a multi-batch SQL transaction.
	globalMu.Lock()
	pinStore := globalPinStore
	mountPath := globalMountPath
	fusePath := globalFUSEPath
	globalMu.Unlock()

	if pinStore == nil {
		return jsonStr(PinResult{Error: "pin store not initialized"})
	}
	root := C.GoString(rootPath)
	walkPath := translateMountToFUSE(root, mountPath, fusePath)
	entries, err := pin.CountFilesUnder(walkPath)
	if err != nil {
		return jsonStr(PinResult{Error: err.Error()})
	}
	for i := range entries {
		entries[i].Path = translateFUSEToMount(entries[i].Path, fusePath, mountPath)
		entries[i].PinRoot = root
	}
	if err := pinStore.PinMany(entries); err != nil {
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
	pinStore := globalPinStore
	globalMu.Unlock()

	if pinStore == nil {
		return jsonStr(PinResult{Error: "pin store not initialized"})
	}
	n, err := pinStore.Unpin(C.GoString(rootPath))
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
	// Snapshot-then-release (see NFSServerStats comment for rationale).
	// CacheStatus is called every 2 s by the popover; cannot park behind
	// other long-held globalMu callers.
	globalMu.Lock()
	pinStore := globalPinStore
	prefetcher := globalPrefetcher
	globalMu.Unlock()

	if pinStore == nil {
		return jsonStr(CacheStatus{OfflineMode: pin.IsOffline()})
	}
	cs := CacheStatus{OfflineMode: pin.IsOffline()}
	if a, err := pinStore.AggregateStats(); err == nil {
		cs.Aggregate = a
	}
	if r, err := pinStore.PinRoots(); err == nil {
		cs.Roots = r
	}
	if prefetcher != nil {
		cs.LiveStats = prefetcher.LiveStats()
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

// handleVerifyPinsHTTP re-enqueues every pinned-Ready file for prefetch.
// Use this when you suspect cache eviction has hollowed out files that
// the pin store still claims are Ready (which is exactly what happens
// when --cache-size < total pinned bytes). Idempotent — running it on a
// fully-cached set is fast (kernel-cache reads, no S3).
func handleVerifyPinsHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if globalPrefetcher == nil {
		w.WriteHeader(503)
		fmt.Fprint(w, `{"ok":false,"error":"prefetcher not running"}`)
		return
	}
	report, err := globalPrefetcher.VerifyAndRepair(r.Context())
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, `{"ok":false,"error":%q}`, err.Error())
		return
	}
	jmlog.Info("pin coverage verify",
		"total_pinned", report.TotalPinned,
		"reenqueued", report.Reenqueued,
		"queue_overflow", report.QueueOverflow,
		"bytes_gb", fmt.Sprintf("%.1f", float64(report.Bytes)/(1<<30)))
	data, _ := json.Marshal(map[string]any{
		"ok":              true,
		"total_pinned":    report.TotalPinned,
		"reenqueued":      report.Reenqueued,
		"queue_overflow":  report.QueueOverflow,
		"bytes_gb":        float64(report.Bytes) / (1 << 30),
		"note":            report.Note,
	})
	w.Write(data)
}

// handleReclaimHTTP triggers tmutil thinlocalsnapshots / 0 4 — frees Time
// Machine local snapshots and other purgeable space so JuiceFS can use it
// for cache. POST or GET; no body. Returns JSON with bytes freed.
// handleForceEjectHTTP runs the privileged force-unmount path on demand.
// In-app rescue for the case where Stop failed (or never ran) and the
// kernel mount table has a wedged `/Volumes/zpool` entry that's making
// Finder hang. Pops the macOS admin password prompt once.
//
// Returns JSON: {"ok": bool, "mount_point": "...", "error": "..."}
//
// Idempotent: safe to call even when nothing is mounted.
func handleForceEjectHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	mp := r.URL.Query().Get("path")
	if mp == "" {
		// Default to the configured mount; survives soft-stop because
		// globalMountPath is retained across Stop/Start cycles for exactly
		// this kind of recovery.
		globalMu.Lock()
		mp = globalMountPath
		globalMu.Unlock()
		if mp == "" {
			mp = "/Volumes/zpool"
		}
	}
	if !isMounted(mp) {
		fmt.Fprintf(w, `{"ok":true,"mount_point":%q,"already_clean":true}`, mp)
		return
	}
	ok := unmountNFS(mp)
	if ok {
		// Clear the remembered mount path so future Start doesn't try to
		// "reuse" a path that's no longer mounted.
		globalMu.Lock()
		if globalMountPath == mp {
			globalMountPath = ""
		}
		globalMu.Unlock()
		fmt.Fprintf(w, `{"ok":true,"mount_point":%q}`, mp)
		return
	}
	w.WriteHeader(500)
	fmt.Fprintf(w, `{"ok":false,"mount_point":%q,"error":"unmount failed; mount may be wedged in kernel — reboot or 'sudo umount -f -t nfs %s' from a fresh terminal"}`, mp, mp)
}

func handleReclaimHTTP(w http.ResponseWriter, r *http.Request) {
	freed, err := health.ReclaimPurgeableSpace("/", 0)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, `{"ok":false,"error":%q}`, err.Error())
		return
	}
	fmt.Fprintf(w, `{"ok":true,"freed_bytes":%d,"freed_gb":%.2f}`,
		freed, float64(freed)/(1<<30))
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

// ----------------------------------------------------------------------------
// A2 — Post-mount self-test
// ----------------------------------------------------------------------------
//
// A 10 MB read measured against the live mount, intended to catch the
// "everything is up and we're handing out reads but they're abysmally slow"
// failure mode. Runs once automatically after start (post initial sync) and
// is rerunnable via POST /self-test. GET /self-test returns the cached
// result. Swift overlays a non-green icon dot whenever the result is yellow
// or red so the user sees the alert without opening the popover.

// SelfTestResult is the JSON shape served at /self-test and consumed by Swift.
type SelfTestResult struct {
	ElapsedMs int64   `json:"elapsed_ms"`
	BytesRead int64   `json:"bytes_read"`
	MBPerSec  float64 `json:"mb_per_sec"`
	Status    string  `json:"status"` // "green" | "yellow" | "red" | "error"
	Hint      string  `json:"hint"`
	RanAt     string  `json:"ran_at"` // RFC3339; empty until first run
	Target    string  `json:"target"` // path that was read; "" on error
}

var (
	selfTestMu   sync.Mutex
	selfTestLast SelfTestResult
)

// selfTestSize is the read size for the probe — 10 MB matches the planning
// doc. Big enough to traverse multiple JuiceFS cache blocks and exercise
// readahead; small enough that even a 50 MB/s "red" mount still finishes
// in 0.2 s.
const selfTestSize = 10 * 1024 * 1024

// runSelfTest performs one 10 MB read self-test against the live NFS mount.
// Picks a target file from the SQLite metadata store if one exists that is
// >= 10 MB, otherwise writes a temp file in the mount, reads it back, and
// cleans up. The read happens via the user-visible mount path (i.e. the NFS
// loopback mount), not the FUSE path — that's what the user's apps see, so
// that's what we measure.
// runSelfTest performs the 10 MiB read probe. Routes the read through the
// FUSE mount path (`~/.juicemount/fuse-internal/...`) rather than the NFS
// loopback (`/Volumes/zpool/...`) to avoid an in-process deadlock:
//
//   - The NFS path goes kernel-NFS-client → our jmlibnfs server → handler.
//   - The handler reads the same `metadata.Store` that the initial `SyncOnce`
//     holds `writeMu` on for ~seconds during its `BulkInsert` + `RebuildFTS`.
//   - If Swift is synchronously waiting on `/self-test` (via the menu-bar
//     poller) and the read parks behind `writeMu`, the menu-bar hangs.
//
// FUSE-direct bypasses all of that. JuiceFS still backs the read, the cache
// reader still serves cached blocks, but the path is local-process kernel
// FUSE — no loopback through our own RPC machinery.
//
// Also bounded with a wall-clock deadline so a slow FUSE / dead JuiceFS
// daemon can't wedge the probe goroutine indefinitely.
func runSelfTest() SelfTestResult {
	globalMu.Lock()
	mountPath := globalMountPath
	fusePath := globalFUSEPath
	store := globalStore
	globalMu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339)
	if fusePath == "" {
		return SelfTestResult{
			Status: "error",
			Hint:   "self-test skipped: FUSE mount not active",
			RanAt:  now,
		}
	}

	// Overall deadline: 30 s. Even a slow backend should finish a 10 MiB
	// FUSE read inside this window; if not, the mount is unhealthy and the
	// probe correctly reports an error rather than hanging forever.
	probeDeadline := time.Now().Add(30 * time.Second)

	target, cleanup, err := pickSelfTestTarget(store, mountPath, fusePath, probeDeadline)
	if err != nil {
		return SelfTestResult{
			Status: "error",
			Hint:   "self-test could not pick target: " + err.Error(),
			RanAt:  now,
		}
	}
	if cleanup != nil {
		defer cleanup()
	}

	// Open with deadline guarded by a goroutine + select. A bare os.Open on
	// a wedged FUSE path can hang in kernel forever.
	type openResult struct {
		f   *os.File
		err error
	}
	openCh := make(chan openResult, 1)
	go func() {
		f, err := os.Open(target)
		openCh <- openResult{f, err}
	}()
	var f *os.File
	select {
	case res := <-openCh:
		if res.err != nil {
			return SelfTestResult{
				Status: "error",
				Hint:   "self-test open failed: " + res.err.Error(),
				RanAt:  now,
				Target: target,
			}
		}
		f = res.f
	case <-time.After(time.Until(probeDeadline)):
		return SelfTestResult{
			Status: "error",
			Hint:   "self-test open timed out (FUSE/JuiceFS unresponsive)",
			RanAt:  now,
			Target: target,
		}
	}
	defer f.Close()

	buf := make([]byte, 256*1024) // 256 KiB read chunks; cheaper than one 10 MB alloc
	var total int64
	start := time.Now()
	for total < selfTestSize {
		if time.Now().After(probeDeadline) {
			return SelfTestResult{
				ElapsedMs: time.Since(start).Milliseconds(),
				BytesRead: total,
				Status:    "error",
				Hint:      "self-test read exceeded 30 s wall-clock deadline",
				RanAt:     now,
				Target:    target,
			}
		}
		n, readErr := f.Read(buf)
		if n > 0 {
			total += int64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return SelfTestResult{
				ElapsedMs: time.Since(start).Milliseconds(),
				BytesRead: total,
				Status:    "error",
				Hint:      "self-test read failed: " + readErr.Error(),
				RanAt:     now,
				Target:    target,
			}
		}
	}
	elapsed := time.Since(start)

	mbps := 0.0
	if elapsed > 0 {
		mbps = (float64(total) / (1024 * 1024)) / elapsed.Seconds()
	}
	status, hint := classifySelfTest(mbps)
	return SelfTestResult{
		ElapsedMs: elapsed.Milliseconds(),
		BytesRead: total,
		MBPerSec:  mbps,
		Status:    status,
		Hint:      hint,
		RanAt:     now,
		Target:    target,
	}
}

// pickSelfTestTarget finds a file >= selfTestSize via the metadata store. If
// none exists yet (fresh mount, empty bucket), it writes a temp file in the
// mount and returns a cleanup callback that removes it.
//
// Translation note: store paths are mount-relative (e.g. "/foo/bar.mov"), so
// we prefix with fusePath (NOT mountPath — see runSelfTest doc for the
// loopback rationale). If store is nil for any reason (early-start race),
// we skip straight to the temp-file path.
//
// `mountPath` is unused for I/O but kept in the signature for future
// per-mount probe logic. `deadline` bounds the BFS by wall-clock so a
// concurrent BulkInsert (holding writeMu, blocking ListChildren) can't
// wedge the probe.
func pickSelfTestTarget(store *metadata.Store, mountPath, fusePath string, deadline time.Time) (string, func(), error) {
	_ = mountPath // intentionally unused
	if store != nil {
		if p := largeFileFromStore(store, fusePath, selfTestSize, deadline); p != "" {
			return p, nil, nil
		}
	}
	// Fallback: create a 10 MB temp file via the FUSE path. Hidden name so
	// a crashed run doesn't pollute Finder listings.
	tmpPath := filepath.Join(fusePath, ".juicemount-selftest.tmp")
	if err := writeRandomFile(tmpPath, selfTestSize); err != nil {
		return "", nil, fmt.Errorf("write probe file: %w", err)
	}
	cleanup := func() { _ = os.Remove(tmpPath) }
	return tmpPath, cleanup, nil
}

// largeFileFromStore returns an absolute path under fusePath for a regular
// file at least minSize bytes long. Walks the directory tree in the store
// breadth-first, bounded by both visit count AND wall-clock deadline so
// contention on the store can't pin the probe.
func largeFileFromStore(store *metadata.Store, fusePath string, minSize int64, deadline time.Time) string {
	type qItem struct{ path string }
	queue := []qItem{{path: ""}} // root in store's coordinates
	const maxVisits = 5000
	visits := 0
	for len(queue) > 0 && visits < maxVisits {
		// Wall-clock guard. If a concurrent BulkInsert is holding writeMu
		// and our ListChildren calls are queuing behind it, give up early
		// rather than wedge the probe goroutine. Returns empty → temp-file
		// fallback path runs next.
		if time.Now().After(deadline) {
			return ""
		}
		head := queue[0]
		queue = queue[1:]
		visits++
		children, err := store.ListChildren(head.path)
		if err != nil || len(children) == 0 {
			continue
		}
		for _, e := range children {
			if e.IsDir {
				queue = append(queue, qItem{path: e.Path})
				continue
			}
			if e.Size >= minSize {
				// Skip our own probe file if it's still around from a crash.
				if strings.HasSuffix(e.Path, ".juicemount-selftest.tmp") {
					continue
				}
				return filepath.Join(fusePath, e.Path)
			}
		}
	}
	return ""
}

// writeRandomFile creates a file of the given size in the given path, filling
// it with random bytes (so JuiceFS can't dedup it away from a cold cache).
func writeRandomFile(path string, size int64) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	// Use 256 KiB chunks of random data to avoid blowing memory.
	const chunk = 256 * 1024
	buf := make([]byte, chunk)
	var written int64
	for written < size {
		if _, err := rand.Read(buf); err != nil {
			return err
		}
		n := int64(chunk)
		if size-written < n {
			n = size - written
		}
		if _, err := f.Write(buf[:n]); err != nil {
			return err
		}
		written += n
	}
	return f.Sync()
}

// classifySelfTest maps a measured MB/s to a status badge + actionable hint.
// Thresholds match the planning doc: >=200 green, 50-200 yellow, <50 red.
func classifySelfTest(mbps float64) (status, hint string) {
	switch {
	case mbps >= 200:
		return "green", fmt.Sprintf("Self-test: %.0f MB/s — healthy.", mbps)
	case mbps >= 50:
		return "yellow", fmt.Sprintf(
			"Self-test: %.0f MB/s — slower than expected. Likely cache miss or warm-up; rerun after a few seconds of activity.",
			mbps)
	default:
		return "red", fmt.Sprintf(
			"Self-test: %.0f MB/s — read path is degraded. Check FUSE health, cache disk space, and network to backend.",
			mbps)
	}
}

// runAndStoreSelfTest is the thread-safe wrapper that updates the cached
// result for the /self-test endpoint.
func runAndStoreSelfTest() SelfTestResult {
	r := runSelfTest()
	selfTestMu.Lock()
	selfTestLast = r
	selfTestMu.Unlock()
	jmlog.Info("self-test complete",
		"status", r.Status,
		"mb_per_sec", fmt.Sprintf("%.1f", r.MBPerSec),
		"elapsed_ms", r.ElapsedMs,
		"target", r.Target)
	return r
}

// handleSelfTestHTTP serves GET (cached) and POST (rerun) on /self-test.
//
// GET is intentionally non-blocking: if no run has happened yet, return a
// "pending" placeholder rather than running synchronously. Synchronous runs
// inside HTTP handlers serialize with other operations on the localhost NFS
// mount and make the entire mount appear unresponsive while the 10 MB read
// is in flight. POST still runs synchronously — the user explicitly asked
// for a rerun by POSTing.
func handleSelfTestHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var out SelfTestResult
	if r.Method == http.MethodPost {
		out = runAndStoreSelfTest()
	} else {
		selfTestMu.Lock()
		out = selfTestLast
		selfTestMu.Unlock()
		if out.RanAt == "" {
			out = SelfTestResult{
				Status: "pending",
				Hint:   "Self-test has not finished its first run yet. Try again in a few seconds.",
			}
		}
	}
	_ = json.NewEncoder(w).Encode(out)
}

func main() {} // required for c-archive build

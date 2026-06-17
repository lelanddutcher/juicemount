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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/redis/go-redis/v9"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	jmlibnfs "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
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
	// globalWantMountPoint is the CONFIGURED mount point from the last
	// Start — unlike globalMountPath it is set even when the mount attempt
	// failed, so /mount-now (LB-2) knows what to mount. Never cleared on
	// stop: retrying against the last-configured path is exactly the
	// recovery /mount-now exists for.
	globalWantMountPoint string

	// Pin / offline / prefetch globals
	globalPinStore   *pin.Store
	globalPrefetcher *pin.Prefetcher

	// [JM6 tier-1.7-1.10] Reachability monitor for offline-mode
	// auto-engage. Runs independently of the health monitor: the
	// health monitor reports backend component status (Redis/MinIO
	// pings), this one tracks "is the route from this Mac to the
	// metadata host alive at all." Items 3-5 of the offline-
	// resilience plan consume its state and OnChange callback.
	globalReach *health.Reachability

	// Auto-offline debounce (2026-06-01). A brief network blip (a few seconds
	// of cold-dial failures) must NOT flip the app to "offline" — that
	// contradicts the still-healthy Redis/MinIO/FUSE components (which ride
	// through the same blip on warm pooled connections) and jars the user. We
	// wait for offlineEngageDelay of CONTINUOUS unreachability before engaging
	// offline mode; recovery is instant. Guarded by offlineEngageMu.
	offlineEngageMu    sync.Mutex
	offlineEngageTimer *time.Timer

	// Spool architecture (Option 2 / slices A-E). Set when
	// JM_SPOOL_ENABLE=1 at startup. Both nil → spool path is fully
	// dormant and the existing fdPool/FUSE write+read path runs
	// unchanged. Stop tears the drainer down with a 30 s deadline
	// before closing the spool store, then nils both globals so
	// /spool returns 503 between Stop and the next Start.
	globalSpool   *jmnfs.SpoolStore
	globalDrainer *jmnfs.Drainer
)

// offlineEngageDelay is how long the backend must be CONTINUOUSLY unreachable
// (on top of globalReach's ~4s detection) before auto-offline engages. Sized
// to ride out the brief LAN/Tailscale blips that trip a cold-dial probe but
// not the warm-connection health checks — keeping the menu bar from flapping
// to "offline" on a 2-6s hiccup. Total engage latency ≈ 22s end-to-end.
//
// Recovery is NOT debounced: the moment reachability returns, the pending
// engage is cancelled and any active auto-offline is lifted immediately.
var offlineEngageDelay = 18 * time.Second

// ServerConfig is the JSON configuration passed from Swift.
type ServerConfig struct {
	RedisURL       string `json:"redis_url"`
	FUSEPath       string `json:"fuse_path"`
	MountPoint     string `json:"mount_point"`
	ListenAddr     string `json:"listen_addr"`
	DBPath         string `json:"db_path"`
	CacheSize      string `json:"cache_size"`
	MetricsAddr    string `json:"metrics_addr"`
	LogFile        string `json:"log_file"`
	LogLevel       string `json:"log_level"`
	BucketOverride string `json:"bucket_override"`
	// Spool (Option 2). Passed from the Swift app via this config JSON —
	// the env var (JM_SPOOL_ENABLE) does NOT work for the embedded
	// c-archive because Go snapshots os.Environ at runtime init, so a
	// host-side setenv() after that is invisible to os.Getenv. The CLI
	// (cmd/jm5) still uses the env var, set before the process starts so
	// it's in the startup snapshot. SpoolSizeGB < 1 means "use default".
	SpoolEnable bool `json:"spool_enable"`
	SpoolSizeGB int  `json:"spool_size_gb"`
	// LB-4 (Phase 3b): tuning knobs that previously existed in the app's
	// preferences UI but were consumed by nothing. All three are optional —
	// 0 (or absent, for config JSON written by older app builds) preserves
	// the previous hardcoded defaults exactly:
	//   memory_buffer_mb      → nfs.DefaultMemBufBudget    (2048 MB)
	//   membuf_file_limit_mb  → nfs.DefaultMemBufThreshold (128 MB)
	//   reconcile_seconds     → metadata.DefaultReconcileInterval (30 s)
	MemoryBufferMB    int `json:"memory_buffer_mb"`
	MemBufFileLimitMB int `json:"membuf_file_limit_mb"`
	ReconcileSeconds  int `json:"reconcile_seconds"`
}

// reconcileInterval resolves the config's reconcile cadence; 0 means
// "keep the metadata package default" (SetReconcileInterval ignores
// non-positive values, so passing 0 through is safe — this helper exists
// so the contract is unit-testable without a live server).
func (c ServerConfig) reconcileInterval() time.Duration {
	return time.Duration(c.ReconcileSeconds) * time.Second
}

//export NFSServerStart
func NFSServerStart(configJSON *C.char) *C.char {
	globalMu.Lock()
	defer globalMu.Unlock()

	if globalServer != nil {
		return C.CString("error: server already running")
	}

	// Reset the /stop gate so the next /stop in this process lifetime
	// actually triggers teardown. Without this, after a Start+Stop+Start
	// cycle the second /stop would CAS-fail and silently no-op.
	stopInProgress.Store(false)

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
			// QA-7 followup: if we're entering this branch with a still-
			// live globalFUSE (e.g. fuseLooksHealthy misfired after
			// NFSServerStopMount which intentionally preserved it), stop
			// the old manager FIRST so we don't leak its monitor
			// goroutine. The old FUSEManager's monitor would otherwise
			// keep ticking against the now-replaced mount and could
			// race with the new one. Idempotent — Stop on an already-
			// stopped manager is a no-op.
			if globalFUSE != nil {
				jmlog.Warn("Start: replacing existing globalFUSE that fuseLooksHealthy rejected — stopping the old one first to prevent monitor leak",
					"path", cfg.FUSEPath)
				oldFM := globalFUSE
				globalFUSE = nil
				oldFM.Stop()
			}
			// Pre-read total pinned bytes so the cache-size policy can grow the
			// cache just enough to keep the pinned set resident. The pin store
			// proper is opened post-mount (~L413); this brief read-only open is
			// best-effort — 0 on any error just means "respect the configured
			// cache size."
			var pinnedBytes int64
			if ps, perr := pin.Open(pinStorePath(cfg.DBPath)); perr == nil {
				if agg, aerr := ps.AggregateStats(); aerr == nil {
					pinnedBytes = agg.TotalBytes
				}
				ps.Close()
			}
			fm := health.NewFUSEManager(health.FUSEConfig{
				RedisURL:       cfg.RedisURL,
				MountPoint:     cfg.FUSEPath,
				CacheSize:      cfg.CacheSize,
				FreeSpaceRatio: "0.01",
				BucketOverride: cfg.BucketOverride,
				PinnedBytes:    pinnedBytes,
			})
			// Start the watchdog and register globalFUSE UNCONDITIONALLY —
			// even if the initial mount fails (a transient backend blip at
			// launch). Pre-2026-05-29 this returned before StartMonitor() on
			// failure, so a launch-time error — especially on a restart that
			// had just Stopped the previous watchdog (see line ~208) — left the
			// app with NO FUSE self-heal at all. The watchdog retries once the
			// backend is reachable; registering globalFUSE keeps a later retry
			// from leaking a second monitor goroutine.
			mountErr := fm.Mount()
			fm.StartMonitor()
			globalFUSE = fm
			if mountErr != nil {
				// Launch-time mount failed — almost always a transient backend
				// blip during startup. Do NOT abort startup: the watchdog
				// (started above) brings FUSE up once the backend is reachable.
				// Returning here used to leave the app HALF-STARTED — juicefs
				// recovering in the background but NFS, the metrics/control
				// server, and the spool never started, so /Volumes/zpool never
				// mounted and the app looked dead (observed 2026-06-01 under a
				// blippy link). Continue instead: the app comes up fully,
				// un-pinned reads fail-fast until FUSE is ready, and the mount
				// self-heals.
				jmlog.Error("juicefs FUSE mount failed at launch — continuing startup; watchdog will mount it once the backend is reachable",
					"error", mountErr.Error())
			}
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
	// QA-30 (2026-05-25): give the reconcile loop the path config it needs
	// for pin-filter normalization (mountpoint-prefixed pin paths vs
	// internal metadata paths) and per-path FUSE Lstat verification of
	// prune candidates. Both are essential to prevent ESTALE on still-
	// valid files; SetPathConfig MUST happen before rc.Start launches the
	// reconcile goroutine below.
	rc.SetPathConfig(cfg.MountPoint, cfg.FUSEPath)
	// LB-4: user-tunable reconcile cadence. Must land before rc.Start()
	// (reconcileLoop snapshots the interval once); 0/absent keeps the
	// 30 s default.
	rc.SetReconcileInterval(cfg.reconcileInterval())

	// [JM6 tier-1.8/1.9] Reachability monitor against the metadata
	// host. Probes a cheap TCP dial at 2s cadence; transitions
	// reachable→unreachable after 2 consecutive failures (~4s
	// detection — under the 5s tier-1.8 acceptance threshold).
	// Items 3-5 will consume its OnChange callback to auto-engage
	// offline mode, refuse un-pinned reads fast, and surface state
	// in the UI. For now this is purely observational — it logs
	// transitions but does not yet affect any other state.
	if reachAddr, _, _ := metadata.ParseRedisURL(cfg.RedisURL); reachAddr != "" {
		// Feed successful-probe RTT into the network-profile link estimator so
		// adaptive readahead can bootstrap link class (fast LAN vs slow WAN)
		// before any throughput sample arrives (internal/netprofile).
		globalReach = health.NewReachability(reachAddr,
			health.WithRTTObserver(netprofile.Default().ObserveRTT))
		globalReach.OnChange(func(reachable bool, reason string) {
			offlineEngageMu.Lock()
			defer offlineEngageMu.Unlock()
			if reachable {
				// Cancel any pending offline-engage and lift auto-offline at
				// once. Recovery is intentionally NOT debounced — the moment
				// the route is back we want un-pinned reads flowing again. The
				// user-intent flag (SetOffline) is untouched.
				if offlineEngageTimer != nil {
					offlineEngageTimer.Stop()
					offlineEngageTimer = nil
				}
				jmlog.Info("network path to backend recovered",
					"target", reachAddr, "reason", reason)
				pin.SetAutoOffline(false, "")
				// Kick reconciliation immediately. Shutdown-safe: `rc` is the
				// captured RedisClient pointer; a late callback after rc.Stop()
				// does a non-blocking send to an undrained channel. Benign.
				rc.TriggerSync()
			} else {
				// DEBOUNCE: do NOT engage offline on a brief blip. Arm a timer;
				// only engage if the backend is STILL unreachable after
				// offlineEngageDelay of continuous failure. A 2-6s blip (which
				// the warm-connection health checks ride straight through)
				// recovers long before this fires, so the app never shows a
				// spurious "offline" while Redis/MinIO/FUSE all read OK.
				if offlineEngageTimer != nil {
					offlineEngageTimer.Stop()
				}
				jmlog.Warn("network path to backend lost — deferring offline engage",
					"target", reachAddr, "reason", reason,
					"engage_after", offlineEngageDelay.String())
				capturedReason := reason
				offlineEngageTimer = time.AfterFunc(offlineEngageDelay, func() {
					offlineEngageMu.Lock()
					defer offlineEngageMu.Unlock()
					if globalReach.Reachable() {
						return // recovered during the debounce window — no-op
					}
					jmlog.Warn("backend unreachable for sustained window — engaging offline mode",
						"target", reachAddr, "reason", capturedReason,
						"sustained", offlineEngageDelay.String())
					// NFS handler reads pin.IsOffline() in the read path to
					// fail-fast un-pinned reads instead of stalling on
					// kernel-NFS timeouts.
					pin.SetAutoOffline(true, capturedReason)
				})
			}
		})
		globalReach.Start()
	}

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
		// LB-4: membuf tuning from preferences. 0/absent → package
		// defaults (2 GiB budget, 128 MB file limit) via NewMemoryBuffer's
		// <= 0 fallback, so old config JSON behaves identically.
		MemBufBudgetMB:    cfg.MemoryBufferMB,
		MemBufFileLimitMB: cfg.MemBufFileLimitMB,
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
		// QA-30 (2026-05-25): wire the pin store as the metadata layer's
		// PinChecker so syncMetadata's prune and Store.evictOldest skip
		// pinned paths. Pinning is an explicit user contract for offline
		// availability — its files MUST remain in the metadata caches to
		// keep kernel-cached NFS handles valid. Without this, transient
		// Redis SCAN gaps trigger ESTALE on still-cached files mid-edit
		// (observed: DaVinci treating fully-cached media as offline).
		store.SetPinChecker(ps)
		globalPrefetcher = pin.NewPrefetcher(ps, cfg.FUSEPath, cfg.MountPoint, 4)
		// Long-running daemons that drain the queue and re-warm pinned
		// files. Launched via Prefetcher.Go so they're tracked by the
		// prefetcher's wg — Stop()'s wg.Wait then actually waits for
		// them to exit before the caller proceeds to pinStore.Close.
		// QA-7a fix: previously these were `go globalPrefetcher.X(...)`
		// directly, leaving them outside wg tracking, so Stop returned
		// while they still had pin.db connections out → SQLite file-
		// descriptor leak on every Stop cycle.
		//
		// Capture the prefetcher into a local before closing over it so
		// a future Stop-then-Start cycle's new globalPrefetcher doesn't
		// silently inherit these still-running closures (review MEDIUM).
		pf := globalPrefetcher
		pinCtx := globalPinCtx()
		pf.Go(func() { pf.PullPending(pinCtx, 100) })
		pf.Go(func() { pf.ReWarmupLoop(pinCtx, 6*time.Hour, 50) })
		// Wire the pin store into the NFS handler so the offline-mode
		// open gate can fail-fast on un-pinned reads. The mount point is
		// the prefix the gate uses to canonicalize in-mount filenames into
		// the absolute paths the pin store keys on.
		srv.Handler().SetPinStore(ps, cfg.MountPoint)
		jmlog.Info("pin store ready", "path", pinDBPath, "workers", 4)
	} else {
		jmlog.Warn("pin store open failed (offline-pin disabled)", "error", err.Error())
	}

	// Spool wiring (Option 2). Env-gated by JM_SPOOL_ENABLE so the
	// pre-spool behavior is preserved by default until the rollout
	// completes (docs/ROADMAP/option-2-spool.md section 9). When
	// enabled, O_CREATE writes route through the spool (slice C) and
	// reads consult the spool index before metadata/FUSE (slice D);
	// when disabled, the handler's spool field stays nil and the
	// pre-spool writeFile / cachedFile paths run unchanged.
	spoolConfigured := cfg.SpoolEnable || os.Getenv("JM_SPOOL_ENABLE") == "1"
	// Even if the spool is toggled OFF, a previous spool-enabled run may have
	// left writing/ready/draining rows whose bytes are on the local SSD but NOT
	// yet on the NAS — while Finder already told the user "copied". Losing those
	// is unacceptable, so force the spool wiring on to recover + drain the
	// backlog (boot recovery runs inside the block); the user can re-disable
	// once it clears. PendingStats errors (no spool schema) mean "no history".
	spoolHasPending := false
	if !spoolConfigured {
		if pf, _, statErr := metadata.NewSpoolStore(store.DB()).PendingStats(); statErr == nil && pf > 0 {
			spoolHasPending = true
			jmlog.Warn("spool disabled but PENDING ENTRIES exist — enabling the spool to drain them to the NAS so the user does not lose data they already saw copied (re-disable once it clears)",
				"pending_files", pf)
		}
	}
	if spoolConfigured || spoolHasPending {
		spoolDir := os.Getenv("JM_SPOOL_DIR")
		if spoolDir == "" {
			home, err := os.UserHomeDir()
			if err != nil || home == "" {
				// Sandboxed-process fallback — without this, an empty
				// home string would prefix to `/Library/...`, silently
				// redirecting the spool to the filesystem root.
				spoolDir = filepath.Join(os.TempDir(), "juicemount-spool")
				jmlog.Warn("UserHomeDir failed; spool dir fell back to tmp",
					"dir", spoolDir, "error", fmt.Sprintf("%v", err))
			} else {
				spoolDir = filepath.Join(home, "Library", "Application Support", "JuiceMount", "spool")
			}
		}
		// Capacity: env override → default 50 GiB. Reviewer HIGH fix:
		// JM_SPOOL_SIZE_GB=0 (or any value < 1) was silently treated
		// as "use default 50 GiB", giving the OPPOSITE of what a user
		// who set 0 to disable buffering would expect. Now we warn
		// explicitly and keep the default so the user knows the env
		// var was ignored.
		// Default auto-sizes to free disk (minus a floor) so a large SD-card
		// offload — e.g. an 87 GB RAW shoot — fits in the spool instead of
		// overflowing the old fixed 50 GiB and aborting the copy with NOSPC.
		// An explicit JM_SPOOL_SIZE_GB still wins (and is clamped to free disk
		// inside NewSpoolStore).
		spoolCapacity := jmnfs.AutoSpoolCapacity(spoolDir)
		if cfg.SpoolSizeGB >= 1 {
			spoolCapacity = int64(cfg.SpoolSizeGB) << 30
		} else if s := os.Getenv("JM_SPOOL_SIZE_GB"); s != "" {
			if v, err := strconv.ParseInt(s, 10, 64); err != nil || v < 1 {
				jmlog.Warn("JM_SPOOL_SIZE_GB ignored (must be >= 1), using auto-sized default",
					"raw", s)
			} else {
				spoolCapacity = v << 30
			}
		}
		if err := metadata.InitSpoolSchema(store.DB()); err != nil {
			jmlog.Warn("spool schema init failed (spool disabled)", "error", err.Error())
		} else {
			meta := metadata.NewSpoolStore(store.DB())
			if spool, err := jmnfs.NewSpoolStore(spoolDir, spoolCapacity, meta); err != nil {
				jmlog.Warn("spool store open failed (spool disabled)",
					"dir", spoolDir, "error", err.Error())
			} else {
				drainer, err := jmnfs.NewDrainer(spool, jmnfs.DrainerConfig{
					FuseRoot: cfg.FUSEPath,
				})
				if err != nil {
					jmlog.Warn("drainer construct failed (spool disabled)", "error", err.Error())
					spool.Stop()
				} else {
					// Slice F: boot-time scrubber. MUST run BEFORE
					// drainer.Start so it doesn't race with worker
					// claims on the `draining`-state rows it resets.
					//
					// Deadline is generous (5 min, up from 30s): recovery
					// integrity outranks a slow boot. At 50k+ rows the per-row
					// SQL writes + writing-row hashes can exceed 30s; a timeout
					// left `draining` rows unreset so their bytes never drained
					// to MinIO (silent loss until the next reboot re-ran
					// recovery). 5 min completes any realistic count while still
					// bounding a truly-wedged disk. Recovery is idempotent, so a
					// rare timeout still recovers on the next boot.
					recCtx, recCancel := context.WithTimeout(context.Background(), 5*time.Minute)
					recReport, recErr := spool.RecoverOnBoot(recCtx)
					recCancel()
					if recErr != nil {
						jmlog.Warn("spool boot scrubber failed (proceeding anyway)",
							"error", recErr.Error())
					} else if recReport.OrphanFilesDeleted > 0 || recReport.OrphanRowsFailed > 0 ||
						recReport.WritingFailedRows > 0 || recReport.WritingResumed > 0 ||
						recReport.DrainingReset > 0 || recReport.ReadyResumed > 0 {
						jmlog.Info("spool boot recovery",
							"orphan_files_deleted", recReport.OrphanFilesDeleted,
							"orphan_rows_failed", recReport.OrphanRowsFailed,
							"writing_failed", recReport.WritingFailedRows,
							"writing_resumed", recReport.WritingResumed,
							"draining_reset", recReport.DrainingReset,
							"ready_resumed", recReport.ReadyResumed)
					}

					globalSpool = spool
					globalDrainer = drainer
					srv.Handler().SetSpool(spool, drainer)
					drainer.Start()
					used, total := spool.Capacity()
					jmlog.Info("spool ready",
						"dir", spoolDir,
						"capacity_gb", float64(total)/(1<<30),
						"used_bytes", used)
				}
			}
		}
	}
	// (The spool-disabled-with-pending-entries case is now handled above by
	// force-enabling the spool to drain the backlog, so there is no stranded-
	// data path left here. A clean disabled start with zero pending rows needs
	// no action.)

	// Mount NFS at the user-visible mount point (e.g. /Volumes/zpool) so
	// Finder can browse it. This requires sudo, which we obtain via an
	// AppleScript "with administrator privileges" prompt the user accepts once.
	//
	// Idempotent path: if the user already has an NFS mount at this path
	// from a previous soft-stop cycle, reuse it. Re-running mount_nfs would
	// fail because the mount point is busy and would prompt for a password
	// for no reason.
	if cfg.MountPoint != "" {
		globalWantMountPoint = cfg.MountPoint
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
			// Clears the on-disk JuiceFS chunk cache. POST-only.
			// Optional ?keep-pinned=true reissues a verify-pins after
			// the clear so pinned content immediately starts re-caching.
			// Returns bytes_freed + files_removed. Destructive but safe
			// — chunks are immutable, in-flight reads via open fds keep
			// working, next access just misses and refetches.
			"/cache-clear": handleCacheClearHTTP,
			"/verify-pins": handleVerifyPinsHTTP,
			// In-app rescue when the kernel mount table is wedged. Runs the
			// privileged `umount -f -t nfs` via AppleScript; user enters
			// their admin password once. Returns JSON with the result.
			"/force-eject": handleForceEjectHTTP,
			// Soft stop: tears down NFS server + metadata + caches without
			// unmounting (Stop/Start cycle stays fast, no admin password
			// re-prompt). Use /force-eject afterward for a full teardown.
			// Returns immediately; teardown runs async so the response
			// flushes before the metrics server closes its own socket.
			"/stop": handleStopHTTP,
			// A2 — post-mount self-test. GET returns cached result; POST reruns.
			"/self-test": handleSelfTestHTTP,
			// Spool status (Option 2 / slice E). Returns pending-files
			// count, pending bytes, in-flight drains, capacity used,
			// and per-entry details. 503 when the spool is disabled
			// (JM_SPOOL_ENABLE != 1) or hasn't been wired yet.
			"/spool": handleSpoolHTTP,
			// Background-operation activity surface (roadmap 4.10): plain-language
			// view of reconcile / drain / prefetch so the UI can explain why
			// Finder is momentarily slow ("Uploading 412 files", "Rebuilding
			// index…", "Warming pinned project"). GET, loopback.
			"/activity": handleActivityHTTP,
			// Spool recovery actions (LB-5): ?action=retry-failed
			// requeues failed rows whose spool file survives;
			// ?action=clear-stalled force-finalizes leaked-handle
			// writing entries (bytes preserved). Loopback-only GET,
			// same mutation convention as /offline?on=….
			"/spool-recover": handleSpoolRecoverHTTP,
			// LB-2 "Mount Now": re-runs the user-visible NFS mount for
			// the configured mount point. No-op success when already
			// mounted. Loopback-only GET, same mutation convention as
			// /spool-recover. May block on the macOS admin prompt.
			"/mount-now": handleMountNowHTTP,
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
	// LB-2: auto-remount for a stale/unmounted NFS volume — the same hook
	// the jm5 CLI has always wired. STRICTLY the non-interactive tier
	// (passwordless sudo): an unattended health tick must never pop an
	// admin-password dialog. Machines without the scoped sudoers entry get
	// a logged failure here, and the user-facing recovery is the popover's
	// "Mount Now" button (→ /mount-now → interactive prompt path).
	// Deliberate Stop paths tear the monitor down, so auto-remount can't
	// fight an intentional unmount.
	if cfg.MountPoint != "" {
		remountAddr := srv.Addr()
		remountPoint := cfg.MountPoint
		globalMonitor.EnableNFSRemount(func() error {
			if err := mountNFSNonInteractive(remountAddr, remountPoint); err != nil {
				return err
			}
			globalMu.Lock()
			globalMountPath = remountPoint
			globalMu.Unlock()
			return nil
		})
	}
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

	// Expose the adaptive link estimate on /metrics for observability + tuning.
	metrics.Default().SetNetworkProvider(func() *metrics.NetworkSnapshot {
		s := netprofile.Default().Snapshot()
		ra := netprofile.Default().Readahead()
		return &metrics.NetworkSnapshot{
			Class:            s.Class.String(),
			RTTMs:            float64(s.RTT.Microseconds()) / 1000.0,
			BandwidthMBps:    s.BytesPerSec / (1024 * 1024),
			HaveRTT:          s.HaveRTT,
			HaveBandwidth:    s.HaveBW,
			ThroughputN:      s.ThroughputN,
			BootstrappedRTT:  s.BootstrappedRTT,
			ReadaheadEnabled: ra.Enabled,
			ReadaheadSeq:     ra.SeqThreshold,
			ReadaheadBlocks:  ra.Blocks,
			ReadaheadWorkers: ra.Workers,
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
	reach := globalReach
	spool := globalSpool
	drainer := globalDrainer
	globalMetrics = nil
	globalMonitor = nil
	globalServer = nil
	globalCache = nil
	globalRC = nil
	globalStore = nil
	globalRDB = nil
	globalPinStore = nil
	globalPrefetcher = nil
	globalReach = nil
	globalSpool = nil
	globalDrainer = nil
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
		// StopHandler tears down drainer + spool internally (slice C
		// integration), but we re-snapshot the globals above and
		// fall through to redundant Stop calls below as belt-and-
		// suspenders for the case where SetSpool was bypassed for
		// some reason — the global is the durable handle.
		server.Handler().StopHandler()
		server.Stop()
	}
	if drainer != nil {
		// Belt-and-suspenders: handler StopHandler already drained
		// this 30 s above. Calling Stop again is idempotent.
		drainer.Stop(5 * time.Second)
	}
	if spool != nil {
		spool.Stop()
	}
	if cache != nil {
		cache.Stop()
	}
	if rc != nil {
		rc.Stop()
	}
	if reach != nil {
		reach.Stop()
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
// NFSServerStopMount is the middle-ground Stop semantic (QA-7,
// 2026-05-17): unmount NFS so /Volumes/<name> disappears from the
// user's view, then tear down the NFS server + metadata + caches +
// metrics — but leave FUSE/JuiceFS alive so the next Start avoids
// the admin-password re-prompt for re-mount.
//
// Use this for the menu-bar "Stop mount and finish sync" button.
// Full teardown (kills JuiceFS daemons + unmounts FUSE) is still
// NFSServerShutdown — wired to the "Stop everything" button.
//
//export NFSServerStopMount
func NFSServerStopMount() {
	globalMu.Lock()
	mountPath := globalMountPath
	mon := globalMonitor
	// Mark mount-gone publicly so concurrent Stats / IsRunning read
	// honest state during the slow unmount.
	globalMountPath = ""
	globalMu.Unlock()

	// Disarm NFS auto-remount BEFORE unmounting (review P1-B): the
	// monitor's 10s tick can land mid-unmount, see its stale-streak hit
	// the threshold, and REMOUNT the volume this deliberate stop is
	// tearing down — once the server then dies, that's an orphaned kernel
	// NFS mount (the QA-27 60s-EIO Finder-hang class). The monitor itself
	// is stopped later in stopServerLocked; only the remount callback must
	// be neutered before the unmount window opens.
	if mon != nil {
		mon.EnableNFSRemount(nil)
	}

	// Step 1: unmount NFS while server is still alive so the kernel
	// gets clean flush/getattr responses during the unmount handshake.
	if mountPath != "" {
		if !unmountNFS(mountPath) {
			jmlog.Error("nfs unmount FAILED during StopMount",
				"mount_point", mountPath,
				"hint", "killing server anyway; user can run sudo umount -f -t nfs")
		}
	}

	// Step 2: tear down server + metadata + caches + metrics. This is
	// the same drain that NFSServerStop does (which is what gives the
	// "finish sync" semantic — the metadata sync goroutine and the
	// in-flight RPC queue both drain cleanly here).
	stopServerLocked()

	// Step 3: deliberately DO NOT touch globalFUSE — leaving
	// FUSE/JuiceFS alive is the whole point of this entry point.
	// The next Start will reuse the existing FUSE mount.
}

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
	mon := globalMonitor
	// Mark "shutting down" by nil-ing the publicly-observable fields. The
	// snapshotted pointers stay valid for the unmount work below.
	globalMountPath = ""
	globalFUSE = nil
	globalFUSEPath = ""
	globalMu.Unlock()

	// Disarm NFS auto-remount BEFORE the unmount window (review P1-B —
	// see NFSServerStopMount for the full rationale): a monitor tick
	// landing mid-unmount must not remount the volume we're shutting down.
	if mon != nil {
		mon.EnableNFSRemount(nil)
	}

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
	Running      bool   `json:"running"`
	EntryCount   int    `json:"entry_count"`
	LastSyncMs   int64  `json:"last_sync_ms"`
	LastSyncTime string `json:"last_sync_time"`
	ServerAddr   string `json:"server_addr"`
	HealthRedis  bool   `json:"health_redis"`
	HealthMinIO  bool   `json:"health_minio"`
	HealthFUSE   bool   `json:"health_fuse"`
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
//
//export NFSServerMetrics
func NFSServerMetrics() *C.char {
	snap := metrics.Default().Snapshot()
	data, _ := json.Marshal(snap)
	return C.CString(string(data))
}

// SyncNow triggers an immediate metadata reconciliation.
//
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
	Path  string  `json:"path"`
	Name  string  `json:"name"`
	IsDir bool    `json:"is_dir"`
	Size  int64   `json:"size"`
	Mtime string  `json:"mtime"`
	Rank  float64 `json:"rank"`
}

// NFSServerSearch performs a full-text search on filenames.
// query: the search string (partial match supported, e.g. "explosion")
// limit: max results (0 = default 50)
// parentPath: scope to subtree (empty = search all)
// Returns JSON array of SearchResult, or "error: ..." on failure.
//
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
// runMountViaSudo attempts to mount via passwordless `sudo`. Probes
// `sudo -n -- true` first to detect whether sudo is configured to run
// without a password for this user (e.g. via /etc/sudoers.d). If the
// probe fails, returns an error so the caller falls back to the
// AppleScript admin-prompt path.
//
// The mount commands themselves stay scoped to /bin/mkdir + /sbin/mount_nfs,
// and the privileged unmount path (see unmountNFS) to /sbin/umount, so a
// minimal sudoers entry suffices:
//
//	%admin ALL=(ALL) NOPASSWD: /sbin/mount_nfs, /sbin/umount, /bin/mkdir
//
// Two separate sudo invocations (mkdir then mount_nfs) so each is a
// single recognized command — wrapping in `sh -c "..."` would require
// granting NOPASSWD on /bin/sh, which is the entire shell. Refuse to
// expand the privileged blast radius.
func runMountViaSudo(mountPoint, opts, host string) error {
	// Probe with one of the actually-allowed binaries — not a generic
	// command. The recommended sudoers entry scopes NOPASSWD to
	// /sbin/mount_nfs + /sbin/umount + /bin/mkdir, so a probe like
	// `sudo -n -- true` would FAIL (requiring a password) even when
	// the real mount call WOULD succeed. Use mount_nfs with no args —
	// sudo either gates with a password (return error) or lets it
	// through to mount_nfs which prints usage and exits non-zero.
	// We don't care about mount_nfs's exit code here, only about
	// sudo's: differentiate via stderr containing "password is required".
	probe := exec.Command("sudo", "-n", "/sbin/mount_nfs")
	probeOut, _ := probe.CombinedOutput()
	if strings.Contains(string(probeOut), "password is required") {
		return fmt.Errorf("passwordless sudo unavailable for /sbin/mount_nfs")
	}

	// 1. mkdir the mount point. Allowed by the same NOPASSWD rule.
	if out, err := exec.Command("sudo", "-n", "/bin/mkdir", "-p", mountPoint).CombinedOutput(); err != nil {
		return fmt.Errorf("sudo mkdir failed: %v\n%s", err, string(out))
	}

	// 2. mount_nfs. The -o args are the same string we'd otherwise
	// shell-interpolate through osascript.
	out, err := exec.Command("sudo", "-n",
		"/sbin/mount_nfs", "-o", opts,
		host+":/", mountPoint).CombinedOutput()
	if err != nil {
		return fmt.Errorf("sudo mount_nfs failed: %v\n%s", err, string(out))
	}
	jmlog.Info("nfs mounted via passwordless sudo", "mount_point", mountPoint)
	return nil
}

// nfsMountArgs derives the mount_nfs host + options string from the NFS
// server's listen address. Shared by the interactive (mountNFSWithPrompt)
// and non-interactive (mountNFSNonInteractive) paths so the heavily-tuned
// option string below can never fork between them.
func nfsMountArgs(serverAddr string) (host, opts string) {
	host = "127.0.0.1"
	port := "11049"
	if i := strings.LastIndex(serverAddr, ":"); i > 0 {
		host = serverAddr[:i]
		port = serverAddr[i+1:]
	}
	opts = nfsMountOpts(port)
	return host, opts
}

// mountNFSNonInteractive re-runs the NFS mount via the passwordless-sudo
// tier ONLY — it can never pop a dialog, so it's safe to call from
// unattended contexts (the health monitor's auto-remount). No-op success
// when already mounted. Machines without the scoped sudoers entry get an
// error (logged by the caller); the user-facing recovery there is the
// interactive "Mount Now" button → /mount-now → mountNFSWithPrompt.
func mountNFSNonInteractive(serverAddr, mountPoint string) error {
	if isMounted(mountPoint) {
		return nil
	}
	host, opts := nfsMountArgs(serverAddr)
	return runMountViaSudo(mountPoint, opts, host)
}

func mountNFSWithPrompt(serverAddr, mountPoint string) error {
	host, opts := nfsMountArgs(serverAddr)

	// If something else is already mounted at the mount point, refuse —
	// don't trample over the user's data. They need to unmount first.
	if isMounted(mountPoint) {
		return fmt.Errorf("%s is already mounted (umount it first)", mountPoint)
	}

	// [JM6] Two-tier mount strategy.
	//
	//   Tier 1: passwordless sudo. If the user has set up a sudoers
	//     entry allowing their account to run `mount_nfs` without a
	//     password (typical dev workflow — one-time config), use that.
	//     Detected by `sudo -n` returning success on a no-op probe.
	//
	//   Tier 2: AppleScript with administrator privileges. The
	//     fallback — prompts the user for their admin password via
	//     the standard macOS dialog. This is the path that bit
	//     automated testing: every restart-cycle pops up a prompt
	//     that blocks startup until the user acknowledges.
	//
	// Setup instructions for tier 1 (one-time, must be run with
	// admin rights):
	//
	//     sudo visudo -f /etc/sudoers.d/juicemount-mount
	//     # add this single line (umount included so the privileged
	//     # unmount path is covered too — see docs/dev-setup.md):
	//     %admin ALL=(ALL) NOPASSWD: /sbin/mount_nfs, /sbin/umount, /bin/mkdir
	//     # or scope to a specific user:
	//     # username ALL=(ALL) NOPASSWD: /sbin/mount_nfs, /sbin/umount, /bin/mkdir
	//
	// With that in place, every subsequent JuiceMount launch mounts
	// the NFS volume non-interactively. The same applies to the
	// privileged unmount path (see unmountNFS).
	if err := runMountViaSudo(mountPoint, opts, host); err == nil {
		return nil
	}

	// Tier 2 fallback: AppleScript-with-admin prompt.
	// Build the shell command: mkdir + mount_nfs
	shellCmd := fmt.Sprintf(
		"mkdir -p %q && mount_nfs -o %s %s:/ %q",
		mountPoint, opts, host, mountPoint,
	)

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

// nfsMountOpts builds the mount_nfs -o option string for our loopback
// NFS server listening on `port`.
//
// Timeout policy. Tuned for a localhost NFS server (us). The kernel
// client's timeo is in 0.1-second units, so timeo=200 means 20 s
// initial timeout; retrans=2 means at most 2 retries before returning
// EIO to the calling syscall. Worst-case dead-server detection: ~60 s.
//
// QA-29 (2026-05-21): bumped from timeo=100 (10 s) to timeo=200 (20 s).
// Under heavy folder-copy load (Editor Resource Vault with thousands
// of small files), per-RPC measurements showed CREATE max = 29.02 s
// while WRITE max stayed at 5.07 s. CREATE chains JuiceFS LOOKUP +
// Create + metadata.Store Insert — the SQLite writeMu contends with
// the reconcile loop's BulkInsert which holds the lock per 500-entry
// batch. The previous timeo=100 (~30 s budget) was right at the edge;
// any TCP RTT pushed CREATEs over the kernel's retransmit window and
// surfaced ETIMEDOUT to Finder as "operation can't be completed
// (error 100060)" mid-copy. timeo=200 gives ~60 s budget — comfortable
// margin for CREATE stalls during metadata sync.
//
// QA-27 (2026-05-21): bumped from timeo=10 (1 s) to timeo=100 (10 s)
// after measuring WRITE p99=1.88 s, worst=6.23 s when JuiceFS disk
// cache was 94% full. That fix covered WRITE-side stalls; QA-29 closes
// the CREATE-side gap.
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
// The tradeoff: real backend hiccups now surface as EIO after ~60 s
// instead of after ~150 s. For our localhost-only NFS path, that's
// the right policy — a 60 s blip is annoying, a 150 s blip looks
// indistinguishable from a system hang. (Original aggressive 3 s
// budget was too tight for big-file copy under JuiceFS writeback
// pressure; QA-27/QA-29 moved the dial to 60 s.)
// [QA-31] rsize=1048576 (1 MiB). Was 262144 (256 KiB).
//
// History: QA-31 (2026-05-25): bumped back to 1 MiB. Live
// measurement on a hot cached MP4 showed NFS throughput at 9.5
// MB/s vs FUSE-direct 1.3 GB/s — a 140× slowdown isolated to the
// per-NFS-RPC overhead (each READ RPC chains an Open via fdPool +
// two Stats via fs.Stat/tryStat which each go through JuiceFS's
// FUSE layer + cachedFile.ReadAt). At rsize=256 KiB this overhead
// fires 4× per MiB; bumping back to 1 MiB cuts the per-MiB
// overhead to a quarter immediately. The original reason to drop
// rsize was timeo=10's tight ~7s per-RPC budget; we're now at
// timeo=200 (60s budget) so 1 MiB even on a cold MinIO fetch is
// safely within budget.
//
// Earlier history: 2026-05-16 cold-read instrumentation showed
// individual NFS READ RPCs for 1 MiB chunks taking up to 4 s
// when JuiceFS-over-MinIO was slow. That problem was fixed at the
// timeout layer (QA-29 timeo=200) and is no longer the binding
// constraint. The server's internal 256-KiB subdivision in
// nfs_onread.go remains as a deadline-protection guarantee inside
// each NFS RPC; the OUTER kernel RPC can again be 1 MiB.
//
// Write size stays at 1 MiB — writes are sequential and the
// failure mode there is different.
// QA-36 (2026-06-13): bumped timeo=200 -> timeo=400 (~120 s budget). During a
// heavy OpenLoupe + native-Finder ingest, a CREATE/first-WRITE RPC stalled past
// the ~60 s budget and Finder aborted with "operation can't be completed
// (error 100060)" = ETIMEDOUT. Root cause traced to metadata.db SQLite
// contention: the spool's synchronous Insert on the OpenWrite hot path
// (spool.go, held under openMu) competes at the SQLite writer with the reconcile
// loop's BulkInsert, amplified by a bloated WAL. This is a STOPGAP to stop the
// aborts; the real fix removes that spool-insert vs reconcile contention so the
// write path never stalls this long. Tradeoff: a genuinely dead backend now
// surfaces EIO after ~120 s instead of ~60 s, which lengthens the worst-case
// Finder /Volumes-enumeration hang in the dead-server case (see QA-29 below).
func nfsMountOpts(port string) string {
	// readahead=16 (was 128 — 8x the macOS default). [torn-read 2026-06-15]
	// readahead=128 over-drives the macOS NFS client's prefetch machinery: under
	// high concurrent reads the client occasionally DELIVERS A TRUNCATED FILE to
	// the app (silent — server serves every byte correctly, proven via per-read
	// logging; the client drops prefetched tail). ~0.5-1.3% of files at 12-18way.
	// Backing off to the stock readahead is the leading mitigation. CANDIDATE —
	// validate a concurrent-readback SHA sweep shows 0 torn before trusting it.
	// hard (was soft): [mmap-SIGBUS 2026-06-15] a `soft` mount returns ETIMEDOUT
	// (errno 60) when a read RPC times out under cold-fetch contention, and a
	// timed-out mmap PAGEIN becomes SIGBUS — crashing any app that mmaps media
	// (NLEs, Quick Look, Preview). Found: 8-26 of 692 cold concurrent mmap reads
	// SIGBUS'd at 8-16way (0 serial, 0 warm). `hard` retries the read instead of
	// timing out → no SIGBUS. The dead-backend hang `soft` guarded against is now
	// covered by offline mode (auto-offline returns NXIO fast) + the FUSE
	// watchdog, so a truly-unresponsive backend no longer hangs reads forever.
	// [#16 phase 2.5] Link-aware readahead. The juicefs FUSE mount (which runs
	// first at startup) has already fed netprofile an RTT sample, so the class is
	// known here. We only ever LOWER readahead below 16 on slow/metered links
	// (strictly safer for the truncation bug + shrinks the whole-file
	// amplification); medium/fast keep 16. nfsReadahead() falls back to 16 if
	// netprofile has no signal, so behavior is unchanged absent a classification.
	ra := netprofile.Default().NFSReadahead()
	return fmt.Sprintf(
		"port=%s,mountport=%s,hard,intr,timeo=400,retrans=2,nolocks,locallocks,rsize=1048576,wsize=1048576,readahead=%d,actimeo=3600,vers=3,tcp",
		port, port, ra)
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
//  1. `diskutil unmount` — works without sudo, doesn't pop a password
//     prompt. Succeeds in the common case.
//  2. `diskutil unmount force` — non-interactive force; still no sudo.
//  3. `umount -f -t nfs` via AppleScript with administrator privileges —
//     the only thing that can dislodge a truly wedged kernel mount entry.
//     Prompts for password.
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
	// [JM6] Two-tier escalation, matching the mount path. Tier 1:
	// passwordless sudo if the user has the sudoers entry configured.
	// Tier 2: AppleScript admin prompt (below). Skipping the prompt
	// in dev workflows keeps automated test-cycles friction-free.
	//
	// Probe via a binary that's actually in the NOPASSWD list (sudoers
	// usually scopes to specific binaries, so `sudo -n -- true` would
	// fail even when the real umount would succeed).
	umountProbe := exec.Command("sudo", "-n", "/sbin/umount")
	umountProbeOut, _ := umountProbe.CombinedOutput()
	if !strings.Contains(string(umountProbeOut), "password is required") {
		if tryUnmount("sudo-umount-f-nfs", 15*time.Second,
			"sudo", "-n", "/sbin/umount", "-f", "-t", "nfs", mountPoint) {
			return true
		}
		if tryUnmount("sudo-umount-f", 15*time.Second,
			"sudo", "-n", "/sbin/umount", "-f", mountPoint) {
			return true
		}
		// Sudo paths failed — fall through to osascript-with-admin
		// since maybe the sudoers rule doesn't cover unmount.
	}
	// Last resort: AppleScript-prompted privileged umount, escalating.
	// `-f` forces unmount of unresponsive mounts. We try -t nfs first
	// (scoped) and fall back to unscoped -f if the kernel still hasn't
	// released after a beat.
	//
	// 2026-05-16 incident: overnight Redis flakes left the kernel NFS
	// state wedged. At shutdown the first privileged attempt completed
	// (osascript exit 0) but isMounted() still returned true a few ms
	// later — the kernel lazy-releases mount table entries and we
	// checked too eagerly. Adding a short post-umount sleep and a
	// second attempt with broader scope recovers the case where the
	// first call succeeded "modally" but hadn't propagated yet.
	//
	// We also raise the osascript timeout to 120s. The default 60s
	// could expire before the user notices the password prompt when
	// the app is in the middle of shutting down (dock activity is
	// unusual, the menu bar icon may be removing itself). 120s is the
	// macOS default for sudo-style admin prompts.
	tryAdminUmount := func(label, command string, promptSuffix string) bool {
		jmlog.Warn("nfs unmount falling back to admin-privileged umount",
			"mount_point", mountPoint,
			"method", label,
			"command", command)
		osaScript := fmt.Sprintf(
			`do shell script %q with administrator privileges with prompt "JuiceMount needs to force-unmount %s%s"`,
			command,
			mountPoint,
			promptSuffix,
		)
		osaCtx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		if err := exec.CommandContext(osaCtx, "osascript", "-e", osaScript).Run(); err != nil {
			jmlog.Debug("admin osascript failed", "method", label, "error", err.Error())
			return false
		}
		// Kernel sometimes needs a beat to actually release the mount
		// table entry after umount returns. Poll a few times before
		// giving up.
		for i := 0; i < 5; i++ {
			if !isMounted(mountPoint) {
				jmlog.Info("nfs unmounted", "method", label, "mount_point", mountPoint, "settle_polls", i)
				return true
			}
			time.Sleep(200 * time.Millisecond)
		}
		return false
	}

	if tryAdminUmount("admin-umount-f-nfs",
		fmt.Sprintf("umount -f -t nfs %q", mountPoint),
		"") {
		return true
	}
	// Pre-flight before the second admin attempt: maybe the first one
	// succeeded but the kernel took longer than our 1 s settle window
	// to release the mount-table entry. Recheck before re-prompting
	// the user — prevents the "second password prompt" anti-UX when
	// the unmount has actually already happened.
	time.Sleep(500 * time.Millisecond)
	if !isMounted(mountPoint) {
		jmlog.Info("nfs unmounted (post-settle recheck)",
			"method", "admin-umount-f-nfs", "mount_point", mountPoint)
		return true
	}
	// Second admin attempt: drop the -t nfs filter. Some wedged states
	// don't dispatch on the type predicate the same way and the
	// unscoped -f variant releases them. The mount path is still
	// specific enough that we can't unmount the wrong thing.
	if tryAdminUmount("admin-umount-f",
		fmt.Sprintf("umount -f %q", mountPoint),
		" (retry without type filter)") {
		return true
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
var globalPinCtxBox struct {
	ctx interface{ Done() <-chan struct{} }
} // lazy init

// globalPinCtx returns the context. Use a closure to avoid pulling in
// context globally where it isn't needed.
// globalPinCtx returns the process-lifetime context that backs
// long-running pin/prefetcher daemons. Returning context.Context
// directly (rather than the historical structural-interface alias)
// lets callers pass the result through to context.WithTimeout /
// context.WithCancel without a type assertion. The concrete bgCtx{}
// implements every Context method so the interface conversion is
// trivial.
func globalPinCtx() context.Context {
	// Background context lives for the whole process. Good enough for v1.
	return bgCtx{}
}

type bgCtx struct{}

func (bgCtx) Done() <-chan struct{}       { return nil }
func (bgCtx) Err() error                  { return nil }
func (bgCtx) Value(key any) any           { return nil }
func (bgCtx) Deadline() (time.Time, bool) { return time.Time{}, false }

// PinResult is the JSON returned to Swift after a Pin call.
type PinResult struct {
	OK          bool   `json:"ok"`
	FilesPinned int    `json:"files_pinned"`
	BytesTotal  int64  `json:"bytes_total"`
	Error       string `json:"error,omitempty"`
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
	Aggregate   pin.AggregateStats `json:"aggregate"`
	Roots       []pin.RootSummary  `json:"roots"`
	LiveStats   pin.LiveStats      `json:"live"`
	OfflineMode bool               `json:"offline_mode"`
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
		return jsonStr(CacheStatus{OfflineMode: pin.IsOffline(), Roots: []pin.RootSummary{}})
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
	// Never emit `"roots": null`: a nil Go slice marshals to JSON null, which the
	// Swift CacheStatus decoder historically choked on (valueNotFound aborted the
	// whole decode → offline_mode silently read as false → offline toggle stuck
	// whenever nothing was pinned). Emit [] so the contract is clean for every
	// consumer. PinRoots can return (nil, nil) when there are no pins.
	if cs.Roots == nil {
		cs.Roots = []pin.RootSummary{}
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

// NFSServerLog bridges a Swift-side line into the Go rotating file log
// (~/Library/Logs/JuiceMount/juicemount.log). The Swift app's os.Logger
// info/warning lines are NOT persisted to `log show`, which made the UI state
// machine invisible across several rounds of the stuck-offline bug. Routing key
// UI-state events through here puts them in the same log everything else uses,
// prefixed "[swift]" for easy grepping.
//
//export NFSServerLog
func NFSServerLog(msg *C.char) {
	jmlog.Info("[swift] " + C.GoString(msg))
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

// handleSpoolHTTP returns the live spool state for menu bar / Manager
// UI consumption. 503 when the spool isn't wired (JM_SPOOL_ENABLE=0
// or pre-Start / post-Stop).
//
// Response shape (GET):
//
//	{
//	  "enabled":         true,
//	  "pending_files":   12,
//	  "pending_bytes":   3400000000,
//	  "in_progress":     4,
//	  "succeeded":       142,
//	  "failed":          0,
//	  "quarantined":     0,
//	  "capacity_used":   3400000000,
//	  "capacity_total":  53687091200,
//	  "stalled_files":   0,
//	  "failed_files":    0,
//	  "oldest_pending_age_sec": 42,
//	  "entries":         [
//	    { "path": "...", "size": ..., "drain_state": "draining",
//	      "drain_attempts": 0, "last_error": "",
//	      "age_sec": 42, "stalled": false },
//	    ...
//	  ]
//	}
//
// The `entries` array lists ACTIVE rows (writing/ready/draining, plus
// still-relevant failed rows) newest-first, followed by a short
// recently-done tail — not all-time history. Capped at 200 rows to
// keep menu-bar payloads responsive on a 1 Hz poll. Older rows live
// in SQLite for audit but are not returned here.
func handleSpoolHTTP(w http.ResponseWriter, r *http.Request) {
	globalMu.Lock()
	spool := globalSpool
	drainer := globalDrainer
	globalMu.Unlock()
	jmnfs.WriteSpoolStatusJSON(w, spool, drainer)
}

// activityOperation is one background task surfaced to the user by /activity.
type activityOperation struct {
	Kind   string `json:"kind"`   // reconcile | drain | prefetch
	Active bool   `json:"active"` // running right now?
	Detail string `json:"detail"` // plain-language, UI-ready
	Files  int    `json:"files,omitempty"`
	Bytes  int64  `json:"bytes,omitempty"`
}

// handleActivityHTTP (roadmap 4.10) aggregates the background operations that
// can make Finder feel sluggish — the metadata reconcile (index rebuild), the
// spool drain (uploading to the backend), and the pin prefetch (warming pinned
// content) — into a single plain-language view. The menu-bar / Manager polls
// this so a slow moment reads as "a known task is in progress", not "broken".
// GET, loopback. Always 200 with a JSON body; absent subsystems are omitted.
func handleActivityHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	globalMu.Lock()
	rc := globalRC
	spool := globalSpool
	drainer := globalDrainer
	pinStore := globalPinStore
	globalMu.Unlock()

	var ops []activityOperation
	var working []string

	// 1. Reconcile — the metadata index rebuild (full Redis SCAN).
	if rc != nil {
		op := activityOperation{Kind: "reconcile", Active: rc.IsSyncing()}
		if op.Active {
			op.Detail = "Rebuilding index…"
			working = append(working, "rebuilding index")
		} else {
			last := rc.LastSyncTime()
			op.Files = rc.LastSyncEntries()
			if last.IsZero() {
				op.Detail = "Index not yet built"
			} else {
				op.Detail = fmt.Sprintf("Index up to date — %d entries, synced %s ago",
					op.Files, time.Since(last).Round(time.Second))
			}
		}
		ops = append(ops, op)
	}

	// 2. Drain — uploading spooled writes to the backend.
	if spool != nil {
		if st, err := jmnfs.BuildSpoolStatus(spool, drainer); err == nil && st.Enabled {
			op := activityOperation{
				Kind:   "drain",
				Active: st.PendingFiles > 0 || st.InProgress > 0,
				Files:  st.PendingFiles,
				Bytes:  st.PendingBytes,
			}
			switch {
			case st.OfflineBufferFull:
				op.Detail = fmt.Sprintf("Offline buffer full — %d copies paused, reconnect to drain", st.StallWaiters)
				working = append(working, "uploads paused (offline)")
			case op.Active:
				op.Detail = fmt.Sprintf("Uploading %d file(s) (%.1f GB) to backend",
					st.PendingFiles, float64(st.PendingBytes)/(1<<30))
				working = append(working, fmt.Sprintf("uploading %d files", st.PendingFiles))
			default:
				op.Detail = "All uploads complete"
			}
			ops = append(ops, op)
		}
	}

	// 3. Prefetch — warming pinned content into the local cache. "Warming"
	// counts files NOT yet Ready and NOT Failed — i.e. both Pending (queued)
	// and Prefetching (actively reading). AggregateStats exposes only Ready /
	// Pending / Failed counts, so the in-flight Prefetching set is the
	// remainder: Total − Ready − Failed (NOT just PendingFiles, which misses
	// the file the worker is actively reading — caught in live test 2026-06-17).
	if pinStore != nil {
		if agg, err := pinStore.AggregateStats(); err == nil && agg.TotalFiles > 0 {
			warming := agg.TotalFiles - agg.ReadyFiles - agg.FailedFiles
			if warming < 0 {
				warming = 0
			}
			op := activityOperation{Kind: "prefetch", Active: warming > 0, Files: warming, Bytes: agg.CachedBytes}
			if op.Active {
				pct := 0.0
				if agg.TotalBytes > 0 {
					pct = 100 * float64(agg.CachedBytes) / float64(agg.TotalBytes)
				}
				op.Detail = fmt.Sprintf("Warming %d pinned file(s) — %.0f%% cached", warming, pct)
				working = append(working, fmt.Sprintf("warming %d pinned files", warming))
			} else {
				op.Detail = fmt.Sprintf("%d pinned file(s) cached", agg.ReadyFiles)
			}
			ops = append(ops, op)
		}
	}

	busy := len(working) > 0
	summary := "Idle — no background work"
	if busy {
		summary = "Working — " + strings.Join(working, "; ")
	}
	data, _ := json.Marshal(map[string]any{
		"busy":       busy,
		"summary":    summary,
		"operations": ops,
	})
	w.Write(data)
}

// handleSpoolRecoverHTTP is the operator-facing recovery action behind the
// LB-5 stuck-spool UI affordance. Loopback-only GET with query params —
// the same mutation convention as /offline?on=true.
//
//	GET /spool-recover?action=retry-failed
//	    ResetForRetry every `failed` row whose spool file still exists on
//	    disk (state→ready, fresh attempt budget) and wake the drainer.
//	    Rows with no surviving data are skipped — never deletes user bytes.
//
//	GET /spool-recover?action=clear-stalled
//	    Force-finalize stalled `writing` entries NOW via the Phase-1
//	    escalation helper (leaked handles zeroed, fsync + SHA +
//	    mark-ready) so their bytes drain like any normal finalize.
//
// Responses:
//
//	200 {"ok":true,"action":"retry-failed","recovered":N}
//	400 {"ok":false,"error":"…"}   — missing/unknown action
//	500 {"ok":false,"error":"…"}   — SQL failure during retry
//	503 {"ok":false,"error":"…"}   — spool not enabled / not wired
func handleSpoolRecoverHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	globalMu.Lock()
	spool := globalSpool
	globalMu.Unlock()
	if spool == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"ok":false,"error":"spool not enabled"}`)
		return
	}
	switch action := r.URL.Query().Get("action"); action {
	case "retry-failed":
		n, err := spool.RetryFailed()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, `{"ok":false,"error":%q}`, err.Error())
			return
		}
		jmlog.Info("spool-recover: retry-failed", "requeued", n)
		fmt.Fprintf(w, `{"ok":true,"action":"retry-failed","recovered":%d}`, n)
	case "clear-stalled":
		n := spool.RecoverStalled()
		jmlog.Info("spool-recover: clear-stalled", "finalized", n)
		fmt.Fprintf(w, `{"ok":true,"action":"clear-stalled","recovered":%d}`, n)
	case "clear-failed":
		// DESTRUCTIVE: discards the un-drained local bytes of permanently-failed
		// rows. Two-phase for safety: without &confirm=true it returns a PREVIEW
		// (no mutation) so the UI can warn before the user commits; with
		// &confirm=true it deletes the spool files + rows. See ClearFailed.
		confirm := r.URL.Query().Get("confirm") == "true"
		items, cleared, bytes, err := spool.ClearFailed(confirm)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, `{"ok":false,"error":%q}`, err.Error())
			return
		}
		var totalBytes int64
		for _, it := range items {
			totalBytes += it.Size
		}
		resp := map[string]any{
			"ok":     true,
			"action": "clear-failed",
		}
		if !confirm {
			resp["preview"] = true
			resp["would_clear"] = len(items)
			resp["would_free_bytes"] = totalBytes
			resp["items"] = items
			resp["note"] = "no changes made — these files never reached the backend; re-call with &confirm=true to discard them"
			jmlog.Info("spool-recover: clear-failed PREVIEW", "would_clear", len(items), "bytes", totalBytes)
		} else {
			resp["preview"] = false
			resp["cleared"] = cleared
			resp["freed_bytes"] = bytes
			jmlog.Warn("spool-recover: clear-failed CONFIRMED — discarded un-drained files", "cleared", cleared, "bytes", bytes)
		}
		data, _ := json.Marshal(resp)
		w.Write(data)
	default:
		w.WriteHeader(http.StatusBadRequest)
		msg := fmt.Sprintf("unknown action %q (want retry-failed | clear-stalled | clear-failed)", action)
		fmt.Fprintf(w, `{"ok":false,"error":%q}`, msg)
	}
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
		"ok":             true,
		"total_pinned":   report.TotalPinned,
		"reenqueued":     report.Reenqueued,
		"queue_overflow": report.QueueOverflow,
		"bytes_gb":       float64(report.Bytes) / (1 << 30),
		"note":           report.Note,
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

// mountNowDeps are the side-effecting pieces of handleMountNowHTTP,
// swappable so the handler's decision logic is unit-testable without a
// live server or exec'ing mount(8) (same extract-the-handler discipline
// as nfs.WriteSpoolStatusJSON). Production values are the real functions.
var mountNowDeps = struct {
	isMounted  func(string) bool
	mount      func(serverAddr, mountPoint string) error
	serverAddr func() (addr string, running bool)
}{
	isMounted: isMounted,
	mount:     mountNFSWithPrompt,
	serverAddr: func() (string, bool) {
		globalMu.Lock()
		srv := globalServer
		globalMu.Unlock()
		if srv == nil {
			return "", false
		}
		return srv.Addr(), true
	},
}

// handleMountNowHTTP re-runs the user-visible NFS mount (LB-2 "Mount
// Now"). Loopback-only GET — the same mutation convention as
// /spool-recover and /offline?on=…. Idempotent: when the volume is
// already mounted it reports success without touching anything.
//
//	GET /mount-now                   → mount the configured mount point
//	GET /mount-now?path=/Volumes/x   → explicit override
//
// Responses:
//
//	200 {"ok":true,"mount_point":"…","already_mounted":true|false}
//	400 {"ok":false,"error":"no mount point configured"}
//	500 {"ok":false,"mount_point":"…","error":"…"}  — mount attempt failed
//	503 {"ok":false,"error":"server not running"}
//
// May block while macOS shows the admin-password prompt (the AppleScript
// tier inside mountNFSWithPrompt) — callers should allow a generous
// timeout, like the Swift force-eject caller does.
//
// Single-flight (Phase 3 review follow-up): mountNFSWithPrompt can sit on
// the admin-password prompt for minutes; a second /mount-now arriving in
// that window must NOT stack a second prompt (or race the first mount).
// The Swift Mount Now button has its own in-flight guard, but the control
// plane is reachable by curl/scripts/a second client, so the server
// enforces it too: concurrent calls get 409 {"mount already in flight"}.
var mountNowInFlight atomic.Bool

func handleMountNowHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !mountNowInFlight.CompareAndSwap(false, true) {
		w.WriteHeader(http.StatusConflict)
		fmt.Fprint(w, `{"ok":false,"error":"mount already in flight"}`)
		return
	}
	defer mountNowInFlight.Store(false)
	globalMu.Lock()
	mp := r.URL.Query().Get("path")
	if mp == "" {
		mp = globalWantMountPoint
	}
	if mp == "" {
		mp = globalMountPath
	}
	globalMu.Unlock()

	addr, running := mountNowDeps.serverAddr()
	if !running {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"ok":false,"error":"server not running"}`)
		return
	}
	if mp == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"ok":false,"error":"no mount point configured"}`)
		return
	}
	if mountNowDeps.isMounted(mp) {
		fmt.Fprintf(w, `{"ok":true,"mount_point":%q,"already_mounted":true}`, mp)
		return
	}
	jmlog.Info("mount-now: mounting", "mount_point", mp)
	if err := mountNowDeps.mount(addr, mp); err != nil {
		// Lost a race with a concurrent mount (auto-remount, a second
		// click): if the volume IS mounted now, that's still success.
		if mountNowDeps.isMounted(mp) {
			fmt.Fprintf(w, `{"ok":true,"mount_point":%q,"already_mounted":true}`, mp)
			return
		}
		jmlog.Warn("mount-now: mount failed", "mount_point", mp, "error", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"ok":false,"mount_point":%q,"error":%q}`, mp, err.Error())
		return
	}
	globalMu.Lock()
	globalMountPath = mp
	globalMu.Unlock()
	jmlog.Info("mount-now: mounted", "mount_point", mp)
	fmt.Fprintf(w, `{"ok":true,"mount_point":%q,"already_mounted":false}`, mp)
}

// stopInProgress gates the /stop handler's teardown goroutine.
// Concurrent /stop POSTs within the 100 ms flush-grace would otherwise
// each spawn their own teardown goroutine. stopServerLocked is already
// serialized by globalMu (snapshot AND nil happen under one lock pair,
// so the second goroutine sees nil and no-ops), but spawning an extra
// goroutine just to nil-then-no-op is wasteful and pollutes the
// goroutine count the iter-11 watchdog watches. CAS gates the spawn
// at the handler level.
//
// Reset by NFSServerStart so the NEXT Start+Stop cycle works. A
// process-wide sync.Once would be permanently "spent" after the first
// /stop and silently break subsequent /stops after a Start.
var stopInProgress atomic.Bool

// handleStopHTTP triggers the soft-stop sequence (stopServerLocked).
// Returns immediately with {"ok": true, "stopping": true} so the
// response is flushed BEFORE the metrics server tears itself down —
// otherwise the active HTTP connection would be killed mid-write by
// metricsSrv.Stop() and the client wouldn't see the response.
//
// Restricted to POST: this is a destructive operation, the listener
// is localhost-only but HTTP convention is to use POST for state-
// changing requests. GET callers get 405 with an Allow: POST header.
//
// Idempotent: stopOnce ensures only the first call triggers
// stopServerLocked; subsequent calls still return ok:true,stopping:
// true so callers can poll until the server actually stops responding.
//
// Does NOT unmount FUSE/NFS — Stop/Start should not require admin
// re-prompt. For a full teardown use /force-eject after /stop, or
// add /shutdown in a future iteration if there's demand.
//
// KNOWN LIMITATION: the 100 ms grace before tearing down the metrics
// listener is a heuristic. Under sustained NFS I/O load (exactly the
// condition the tier-1.2 wedge harness creates) the loopback TCP send
// buffer might not drain in 100 ms, and a slow client may see a
// connection reset instead of the JSON body. The fix would be to
// Hijack() the connection and write+close before sleeping — overkill
// for v1; revisit if the wedge harness sees flaky empty-body results.
func handleStopHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, `{"ok":true,"stopping":true}`)
	// Flush the response onto the socket before triggering teardown.
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	jmlog.Info("handleStopHTTP: soft-stop requested via HTTP", "remote", r.RemoteAddr)
	if stopInProgress.CompareAndSwap(false, true) {
		go func() {
			// Tiny grace so the kernel actually writes the queued bytes
			// to the client before we yank the listener out from under
			// it. See KNOWN LIMITATION in the doc comment.
			time.Sleep(100 * time.Millisecond)
			stopServerLocked()
		}()
	}
}

func handleReclaimHTTP(w http.ResponseWriter, r *http.Request) {
	freed, snapshots, source, err := health.ReclaimPurgeableSpace("/", 0)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, `{"ok":false,"error":%q}`, err.Error())
		return
	}
	fmt.Fprintf(w, `{"ok":true,"freed_bytes":%d,"freed_gb":%.2f,"snapshots_thinned":%d,"source":%q}`,
		freed, float64(freed)/(1<<30), snapshots, source)
}

// cacheClearInProgress gates concurrent /cache-clear POSTs. Without
// it, two simultaneous calls would each walk the chunks tree and
// each spawn a VerifyAndRepair goroutine — double-marking every
// pinned file Pending, which doubles the re-download bandwidth on
// the next prefetcher pass. atomic.Bool matches the /stop pattern.
var cacheClearInProgress atomic.Bool

// handleCacheClearHTTP — POST /cache-clear[?keep-pinned=true]
//
// Walks the JuiceFS chunk cache (~/.juicefs/cache/{uuid}/raw/chunks/)
// and removes every chunk file. Reports bytes freed and files removed.
// Returns 405 on non-POST.
//
// SAFETY: chunks are immutable content-addressed blobs. Removing one
// while JuiceFS has an open fd to it is benign (Unix fd semantics —
// the fd keeps the inode alive until close). New chunk requests after
// the rm will miss and refetch from MinIO. Worst case is a brief
// cache-miss spike on next access, no data loss.
//
// keep-pinned=true: after clearing, issue an internal verify-pins so
// pinned content immediately starts re-downloading. Pinned files are
// chunk-cached too, so without this they would be evicted along with
// everything else — defeating the pinned-for-offline contract until
// the next user action.
func handleCacheClearHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	if !cacheClearInProgress.CompareAndSwap(false, true) {
		w.WriteHeader(http.StatusConflict)
		fmt.Fprint(w, `{"ok":false,"error":"cache-clear already in progress"}`)
		return
	}
	defer cacheClearInProgress.Store(false)

	keepPinned := r.URL.Query().Get("keep-pinned") == "true"

	home, err := os.UserHomeDir()
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, `{"ok":false,"error":%q}`, err.Error())
		return
	}
	cacheBase := filepath.Join(home, ".juicefs", "cache")

	// Walk every UUID's chunks dir under cacheBase. There's usually one
	// UUID per JuiceFS volume; supporting many is robust against future
	// multi-volume setups.
	var bytesFreed int64
	var filesRemoved int64
	entries, err := os.ReadDir(cacheBase)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, `{"ok":false,"error":%q}`, err.Error())
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		chunksDir := filepath.Join(cacheBase, e.Name(), "raw", "chunks")
		// If the chunks dir doesn't exist (no JuiceFS writes yet, or
		// alternate layout), filepath.Walk returns an error at the
		// top level. ENOENT is benign; anything else is a real
		// problem and needs to be visible.
		walkErr := filepath.Walk(chunksDir, func(p string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				// Per-entry error (e.g. file deleted under us) — skip
				// the entry but keep walking the rest of the tree.
				return nil
			}
			if info.IsDir() {
				return nil
			}
			sz := info.Size()
			if rmErr := os.Remove(p); rmErr == nil {
				bytesFreed += sz
				filesRemoved++
			} else if !os.IsNotExist(rmErr) {
				// EPERM/EACCES on a chunk would otherwise be invisible.
				jmlog.Warn("cache-clear: failed to remove chunk",
					"path", p, "error", rmErr.Error())
			}
			return nil
		})
		if walkErr != nil && !os.IsNotExist(walkErr) {
			jmlog.Warn("cache-clear: walk failed for chunks dir",
				"dir", chunksDir, "error", walkErr.Error())
		}
	}

	jmlog.Info("handleCacheClearHTTP: cleared JuiceFS chunk cache",
		"files_removed", filesRemoved,
		"bytes_freed", bytesFreed,
		"keep_pinned", keepPinned,
		"remote", r.RemoteAddr)

	// Optionally re-trigger pin coverage verification so pinned content
	// starts re-caching immediately. Fire-and-forget on the prefetcher
	// since the actual re-download happens later in the worker loop
	// (driven by PullPending). VerifyAndRepair itself just marks rows
	// Pending; that part completes in milliseconds. We bound it at 30s
	// as a safety belt and tie cancellation to globalPinCtx so it
	// dies cleanly on server shutdown rather than orphaning the
	// goroutine.
	reverify := false
	if keepPinned && globalPrefetcher != nil {
		go func() {
			pinCtx := globalPinCtx()
			ctx, cancel := context.WithTimeout(pinCtx.(context.Context), 30*time.Second)
			defer cancel()
			if _, err := globalPrefetcher.VerifyAndRepair(ctx); err != nil {
				jmlog.Warn("cache-clear keep-pinned: verify-pins failed", "error", err.Error())
			}
		}()
		reverify = true
	}

	fmt.Fprintf(w, `{"ok":true,"files_removed":%d,"bytes_freed":%d,"bytes_freed_gb":%.2f,"keep_pinned":%t,"pin_reverify_triggered":%t}`,
		filesRemoved, bytesFreed, float64(bytesFreed)/(1<<30), keepPinned, reverify)
}

func handleOfflineHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// If `on` is present, treat as a user-intent toggle (existing
	// behavior — preserved for callers like the Swift offline switch).
	// If absent, return the full OfflineState snapshot — gives the UI
	// the data it needs to render auto-vs-user distinction and the
	// human-readable reason without a second round trip.
	if on := r.URL.Query().Get("on"); on != "" {
		var v C.int
		if on == "on" || on == "true" || on == "1" {
			v = 1
		}
		cstr := NFSServerSetOffline(v)
		defer NFSServerFreeString(cstr)
		w.Write([]byte(C.GoString(cstr)))
		return
	}
	_ = json.NewEncoder(w).Encode(pin.State())
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

	// Write probe: a 4 KiB create-write-fsync-delete cycle through the
	// user-visible NFS mount path. If a write through NFS fails (as
	// happened during the 2026-05-16 incident: backend issues left
	// directories with stale handles that rejected writes), surfacing
	// it in self-test means Swift can show a degraded status icon and
	// the user notices BEFORE they try to copy a real project file
	// and have it fail silently.
	WriteOK   bool   `json:"write_ok"`
	WriteMs   int64  `json:"write_ms"`
	WriteHint string `json:"write_hint,omitempty"`

	// B.6 (2026-05-17): first-byte read latency in milliseconds.
	// Distinct signal from MBPerSec — measures round-trip latency
	// (FUSE → JuiceFS → metadata Redis → chunk fetch starts), not
	// sustained throughput. High RTT + high MB/s = bursty cache hits
	// after a slow first hop; low RTT + low MB/s = uniformly slow
	// backend. UI surfaces both because each signal points at a
	// different remediation.
	FirstByteMs int64 `json:"first_byte_ms,omitempty"`
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

	// B.6: time the FIRST chunk separately. Captures round-trip
	// latency (how long until any byte comes back) — different
	// signal from sustained throughput. The overall MB/s
	// measurement still covers the full read including the first
	// chunk, so the two metrics agree at the integral level even
	// though FirstByteMs is reported separately.
	buf := make([]byte, 256*1024) // 256 KiB read chunks; cheaper than one 10 MB alloc
	var total int64
	var firstByteMs int64
	start := time.Now()
	firstN, firstErr := f.Read(buf)
	firstByteMs = time.Since(start).Milliseconds()
	if firstN > 0 {
		total += int64(firstN)
	}
	if firstErr != nil && firstErr != io.EOF {
		return SelfTestResult{
			ElapsedMs:   time.Since(start).Milliseconds(),
			BytesRead:   total,
			Status:      "error",
			Hint:        "self-test read failed: " + firstErr.Error(),
			RanAt:       now,
			Target:      target,
			FirstByteMs: firstByteMs,
		}
	}
	// If the first read already hit EOF (regular files can return data
	// AND io.EOF in one call), don't re-enter the loop — it would
	// spin returning (0, EOF) every iteration until probeDeadline,
	// turning a tiny file into a 30s false-timeout.
	for firstErr != io.EOF && total < selfTestSize {
		if time.Now().After(probeDeadline) {
			return SelfTestResult{
				ElapsedMs:   time.Since(start).Milliseconds(),
				BytesRead:   total,
				Status:      "error",
				Hint:        "self-test read exceeded 30 s wall-clock deadline",
				RanAt:       now,
				Target:      target,
				FirstByteMs: firstByteMs,
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

	// Write probe: exercise the user-facing write path (NFS → handler →
	// FUSE → JuiceFS → MinIO/Redis). Failures here are the 2026-05-16
	// incident class: read paths can look fine while writes silently
	// fail. If the write probe fails on an otherwise-green read, we
	// downgrade overall status to "yellow" so Swift surfaces the
	// degraded state.
	writeOK, writeMs, writeHint := runWriteProbe(mountPath, 5*time.Second)
	if !writeOK {
		if status == "green" || status == "yellow" {
			status = "yellow"
			hint = "read OK; WRITE failed: " + writeHint
		}
	}

	return SelfTestResult{
		ElapsedMs:   elapsed.Milliseconds(),
		BytesRead:   total,
		MBPerSec:    mbps,
		Status:      status,
		Hint:        hint,
		RanAt:       now,
		Target:      target,
		WriteOK:     writeOK,
		WriteMs:     writeMs,
		WriteHint:   writeHint,
		FirstByteMs: firstByteMs,
	}
}

// runWriteProbe creates a 4 KiB sentinel under the user-visible mount
// path, writes random bytes, fsyncs, and deletes it. Each step is
// individually bounded by the per-step budget; the whole probe is
// bounded by overall to detect either a single step stalling or the
// aggregate dragging on. Returns (ok, elapsed_ms, hint).
//
// We deliberately go through `mountPath` (the NFS-loopback path, e.g.
// /Volumes/zpool-dev) rather than the FUSE-internal path, because the
// failure modes that bit the user on 2026-05-16 only manifested on
// the user-facing path (stale NFS handles in the kernel mount table,
// handler-layer bugs). A FUSE-direct probe would have reported green
// while the user couldn't copy a file.
//
// If mountPath is empty (mount hasn't come up yet), return ok=true
// with an explanatory hint — the read probe will already have caught
// the "FUSE not active" case.
func runWriteProbe(mountPath string, overallBudget time.Duration) (ok bool, elapsedMs int64, hint string) {
	if mountPath == "" {
		return true, 0, "write probe skipped: mount path not configured"
	}
	start := time.Now()
	deadline := start.Add(overallBudget)
	sentinelPath := filepath.Join(mountPath, ".juicemount-writetest.tmp")
	const sentinelSize = 4096

	// Run the write probe in a goroutine so a stuck syscall (kernel NFS
	// retry storm on a wedged mount, FUSE daemon hung) can be bounded
	// without leaking the syscall on us — the goroutine continues until
	// the kernel returns, but the probe returns to the caller.
	type result struct {
		ok   bool
		hint string
	}
	resCh := make(chan result, 1)
	go func() {
		// 1. Create + write.
		buf := make([]byte, sentinelSize)
		for i := range buf {
			buf[i] = byte(i)
		}
		f, err := os.OpenFile(sentinelPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			resCh <- result{false, "create: " + err.Error()}
			return
		}
		if _, err := f.Write(buf); err != nil {
			_ = f.Close()
			_ = os.Remove(sentinelPath)
			resCh <- result{false, "write: " + err.Error()}
			return
		}
		// 2. Fsync — verifies the bytes actually committed, not just
		//    landed in client write cache. If the backend (MinIO or
		//    Redis) is wedged, this is where it surfaces.
		if err := f.Sync(); err != nil {
			_ = f.Close()
			_ = os.Remove(sentinelPath)
			resCh <- result{false, "fsync: " + err.Error()}
			return
		}
		if err := f.Close(); err != nil {
			_ = os.Remove(sentinelPath)
			resCh <- result{false, "close: " + err.Error()}
			return
		}
		// 3. Delete — exercises a different path (REMOVE → handler →
		//    metadata.Store.Delete + JuiceFS unlink + Redis prune).
		if err := os.Remove(sentinelPath); err != nil {
			resCh <- result{false, "remove: " + err.Error()}
			return
		}
		resCh <- result{true, ""}
	}()

	select {
	case r := <-resCh:
		elapsedMs = time.Since(start).Milliseconds()
		return r.ok, elapsedMs, r.hint
	case <-time.After(time.Until(deadline)):
		// Probe goroutine leaked — it'll finish whenever the kernel
		// returns. Acceptable cost.
		return false, overallBudget.Milliseconds(), "write probe exceeded " + overallBudget.String() + " budget"
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

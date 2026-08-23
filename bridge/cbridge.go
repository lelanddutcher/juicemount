package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/redis/go-redis/v9"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/cplane"
	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	jmlibnfs "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/internal/thumbcache"
	"github.com/lelanddutcher/juicemount/internal/version"
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

	// Tier-B shared-derivative index (contract JM-14). Backs /derivatives +
	// /metadata. Its own SQLite file (sibling of metadata.db / pin.db) so it
	// doesn't contend on their WALs. Server-side truth, mirror-synced via JM-15
	// later; for now the farm (JM-16) and tests populate it. Read-only on the
	// query path. nil between Stop and the next Start ⇒ endpoints fail closed.
	globalDerivStore *derivatives.Store
	// #1 hydration pack: bounded local thumbnail blob cache + the
	// folder-open warmer feeding it (nil when JM_THUMB_WARM=0 or open failed).
	globalThumbCache  *thumbcache.Cache
	globalThumbWarmer *jmnfs.ThumbWarmer

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

	// globalKeyspaceNetWatcher provides the active-interface NAME signal used
	// to class-gate the metadata keyspace-push backstop cadence + coalescer
	// (LAN/WiFi/tunnel). Only instantiated when JM_METADATA_KEYSPACE_PUSH=1;
	// nil otherwise (class-gating then falls back to JM_WAN_MODE-only).
	globalKeyspaceNetWatcher *health.NetWatcher

	// Spool architecture (Option 2 / slices A-E). Set when
	// JM_SPOOL_ENABLE=1 at startup. Both nil → spool path is fully
	// dormant and the existing fdPool/FUSE write+read path runs
	// unchanged. Stop tears the drainer down with a 30 s deadline
	// before closing the spool store, then nils both globals so
	// /spool returns 503 between Stop and the next Start.
	globalPresence *jmnfs.PresenceTracker
	globalSpool   *jmnfs.SpoolStore
	globalDrainer *jmnfs.Drainer

	// globalDrainerAtomic mirrors globalDrainer for lock-free readers that
	// must never contend on globalMu (review fix: the reachability liveness
	// hook runs on the prober goroutine while NFSServerStart can hold
	// globalMu for its entire body — a mutex read there freezes offline
	// detection during start). Stored wherever globalDrainer is assigned.
	globalDrainerAtomic atomic.Pointer[jmnfs.Drainer]

	// Control-plane / contract identity (JM-1 /whoami, JM-6 version-of-record).
	// Captured at Start because cfg.MountPoint/DBPath/MetricsAddr are otherwise
	// local to NFSServerStart; the /whoami handler needs them later. Read under
	// globalMu like every other global in this block.
	globalDBPath      string // cfg.DBPath — metadata.db path + instance-id dir
	globalMetricsAddr string // cfg.MetricsAddr — control-plane bind addr
	// globalFUSEMetricsAddr is the host:port the JuiceFS FUSE daemon binds its
	// Prometheus /metrics to (health.FUSEManager.EffectiveMetricsAddr). The
	// block-cache scraper (pin.BlockCacheBytes) reads juicefs_blockcache_bytes
	// from here to report the true cache_used_bytes in /cache-status. Distinct
	// from globalMetricsAddr (our own control plane) and from the :9567 sync
	// metrics addr — see the loopback-port-collision note in health/fuse.go.
	globalFUSEMetricsAddr string
	globalVolumeName      string   // basename of the mount point
	globalInstanceID      string   // stable per-install UUID (minted/persisted once)
	globalCapabilities    []string // capabilities DERIVED from this binary's routes
	// globalRedisURL is the configured metadata-backend URL from the last
	// Start — the /diagnose network probes (INSTANT-NAV #14) dial its
	// host:port. Like globalWantMountPoint it is deliberately NOT cleared on
	// stop: diagnosing "why did it break" right after a stop still needs to
	// know where the backend was.
	globalRedisURL string
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

	// JuiceMount Link (Tier-2 T2.2): embedded tailnet node for remote use.
	NetControlURL string `json:"net_control_url"` // Headscale coordination URL
	NetAuthKey    string `json:"net_authkey"`    // preauth key from pairing
	NetHostname   string `json:"net_hostname"`   // this Mac's node name
	NetNASAddr    string `json:"net_nas_addr"`   // NAS tailnet IP (mount/health target)

	// Spool (Option 2). Passed from the Swift app via this config JSON —
	// the env var (JM_SPOOL_ENABLE) does NOT work for the embedded
	// c-archive because Go snapshots os.Environ at runtime init, so a
	// host-side setenv() after that is invisible to os.Getenv. The CLI
	// (cmd/jm5) still uses the env var, set before the process starts so
	// it's in the startup snapshot. SpoolSizeGB < 1 means "use default".
	SpoolEnable bool `json:"spool_enable"`
	SpoolSizeGB int  `json:"spool_size_gb"`
	// OpenCacheTTL is the juicefs --open-cache duration, e.g. "5s". Empty = OFF
	// (today's behaviour: every open re-validates against Redis).
	//
	// It rides the config for the same reason SpoolEnable does — os.Getenv
	// cannot see a Swift setenv() after c-archive init — but the stakes are
	// higher here. Every other cellular lever in health/fuse.go defaults to the
	// behaviour we want, so being env-only merely made them untunable. This one
	// defaults OFF, so being env-only made the largest measured win on a real
	// cellular link (66x: first open 1226-5660ms, second 24-36ms, 2026-07-29)
	// impossible to switch on in the build under test.
	//
	// Deliberately NOT surfaced in the Settings UI: the exposure is a stale
	// slice map — WRONG BYTES from an existing file — which is the family behind
	// #18 torn reads, #104 black frames and the membuf stale-partial image.
	// Set it for a measured test with:
	//     defaults write com.juicemount.app openCacheTTL -string 5s
	OpenCacheTTL string `json:"open_cache_ttl"`
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

var (
	linkMu         sync.Mutex
	globalLinkNode *jmnfs.LinkNode
)

// startLinkIfConfigured brings up the embedded tailnet node before mounting
// so backend traffic can route over it during the rest of startup. Returns
// the mount host override (NAS tailnet IP) or "" when Link is off.
func startLinkIfConfigured(cfg ServerConfig) string {
	if cfg.NetControlURL == "" || cfg.NetAuthKey == "" {
		return ""
	}
	node, addrs, err := jmnfs.StartLinkNode(cfg.NetControlURL, cfg.NetAuthKey, cfg.NetHostname, "")
	if err != nil {
		jmlog.Warn("JuiceMount Link: node failed to start (continuing without remote)", "error", err.Error())
		return ""
	}
	linkMu.Lock()
	globalLinkNode = node
	linkMu.Unlock()
	if len(addrs) > 0 {
		jmlog.Info("JuiceMount Link: node up", "addrs", fmt.Sprint(addrs))
	}
	return cfg.NetNASAddr
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

	// Capture control-plane identity for /whoami (JM-1). These config values
	// are local to this function; the handler reads them later under globalMu.
	// instance_id is minted+persisted once next to metadata.db.
	globalDBPath = cfg.DBPath
	globalMetricsAddr = cfg.MetricsAddr
	globalVolumeName = filepath.Base(cfg.MountPoint)
	globalInstanceID = cplane.LoadOrMintInstanceID(cfg.DBPath)
	// JM_DEBUG_META_ADDR (dev): rewrite the metadata endpoint ONCE, here at config
	// ingest, so every consumer agrees — the Go metadata client, the reachability
	// probes, AND the juicefs mount args (which are built from this same value).
	// Rewriting only the juicefs URL left the Go client dialling the real LAN
	// address: it was blocked by macOS Local Network Privacy, the backend read as
	// unreachable, and the app took start-while-offline and never mounted FUSE at
	// all — so the proxy saw zero connections. Unset, this is a pure passthrough.
	cfg.RedisURL = health.MetaURLForMount(cfg.RedisURL)
	globalRedisURL = cfg.RedisURL

	// Initialize structured logging early so all subsequent log lines
	// flow through the JSON sink (and optional log file).
	if err := jmlog.Init(jmlog.Config{
		LogFile: cfg.LogFile,
		Level:   jmlog.ParseLevel(cfg.LogLevel),
	}); err != nil {
		return C.CString(fmt.Sprintf("error: init logger: %v", err))
	}

	// F1 (cellular): the control plane starts FIRST — before FUSE, Redis,
	// sync, pin store, spool, or mount. The menu bar's truth and the pprof
	// endpoints must exist during slow-link startup stalls, not after them.
	// Routes are attached later via RegisterRoutes as subsystems wire up.
	var ms *metrics.Server
	if cfg.MetricsAddr != "" {
		ms = metrics.NewServer(cfg.MetricsAddr, metrics.Default())
		if err := ms.Start(); err != nil {
			jmlog.Warn("metrics server failed to start",
				"addr", cfg.MetricsAddr, "error", err.Error())
			ms = nil
		} else {
			globalMetrics = ms
			jmlog.Info("metrics server listening", "addr", ms.Addr())
		}
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

	// R-4 (start-while-offline): probe the metadata backend ONCE, up front, with
	// a cheap short TCP dial. When it's unreachable (laptop opened on a plane,
	// NAS asleep, router down) startup takes the offline path: skip the
	// synchronous boot-time FUSE mount (which would otherwise stall ~30s on a
	// dead Redis before failing), don't burn ~31s on connect retries, build a
	// deferred Redis client, and pre-engage auto-offline so directory navigation
	// serves from the SQLite mirror while reads fail-fast. The watchdog +
	// reconcile loop recover everything automatically when the backend returns.
	// A reachable backend — the overwhelmingly common case — takes the unchanged
	// online path.
	// V2.3 U3: persisted user-offline intent — the user toggled offline in a
	// previous session and never toggled back, so this session STARTS with
	// the user-offline gates engaged (field report: "clicking the offline
	// toggle doesn't just start it in offline mode"). One menu click returns
	// online (SetOffline clears the marker).
	//
	// Batch-3 adversarial review (HIGH): a persisted-offline boot still
	// PROBES the backend and — when reachable — MOUNTS FUSE. Offline pinned
	// reads REQUIRE the mount: OpenFile's read path opens the FUSE fd and the
	// JuiceFS LRU serves pinned bytes from local SSD, so the original
	// skip-the-probe/skip-the-mount behavior made EVERY pinned-and-ready file
	// unreadable (ENOENT against a plain empty dir) for the entire relaunched
	// offline session — and the U4 watchdog stand-down (correctly) never
	// remounts while the USER flag is held, so nothing recovered it. The
	// original "touches no network at all" goal was WRONG for the same
	// reason: juicefs needs its Redis metadata engine to mount, so a
	// user-offline session was never a zero-network session (this also
	// subsumes/rejects the companion suggestion to skip the boot Redis
	// connect while user-offline). The split that actually holds:
	//   - FUSE mount + Redis connect budget key on backendUp (TRUE probed
	//     reachability, exactly like every other boot);
	//   - boot sync/SCAN suppression, the offline read/readdir gates, and
	//     the started_offline banner key on startedOffline (forced below).
	// On a metered link the only added traffic is the 1.5s TCP probe + the
	// juicefs mount metadata handshake — no data reads (the offline gates
	// refuse un-pinned opens from the first RPC). On a dead link the probe
	// fails fast and boot degenerates to the existing R-4 path.
	//
	// Batch-3 review #7: resolve the marker path via os.UserHomeDir with
	// warn-and-disable — os.Getenv("HOME") made the marker path cwd-relative
	// when HOME was unset (arbitrary in a launchd/daemon context), silently
	// losing the user's offline choice. Unconfigured persistence is the pin
	// package's designed inert mode: degrade loudly and safely.
	if home, homeErr := os.UserHomeDir(); homeErr == nil && home != "" {
		pin.SetOfflinePersistPath(filepath.Join(home, ".juicemount", "offline-intent"))
	} else {
		jmlog.Warn("cannot resolve home directory — offline-intent persistence disabled (the offline toggle will not stick across launches)",
			"error", fmt.Sprintf("%v", homeErr))
	}
	bootUserOffline := pin.PersistedOfflineIntent()
	// Batch-3 review #10: a marker that outlived the metadata mirror DB (an
	// app-data reset — including the app's own "Reset local metadata cache" —
	// deletes Application Support but not ~/.juicemount) would make this boot
	// skip the U1 empty-mirror blocking sync and serve a completely EMPTY
	// volume that looks like data loss. Treat the marker as stale when there
	// is nothing to serve: clear it and take the normal online boot.
	if bootUserOffline && pin.DropStaleOfflineIntent(cfg.DBPath) {
		jmlog.Warn("persisted offline intent but the metadata mirror DB is missing/empty (app data reset?) — ignoring the stale marker and booting online",
			"db_path", cfg.DBPath)
		bootUserOffline = false
	}
	if bootUserOffline {
		jmlog.Info("persisted user-offline intent found — starting offline (U3); still probing + mounting FUSE so pinned files stay readable")
	}
	backendUp, bootRTT := backendReachableRTT(cfg.RedisURL, 1500*time.Millisecond)
	// V2.3 U2/K2: a link can be reachable-but-useless — the TCP handshake
	// completes inside 1.5s but the RTT is so high that the synchronous
	// online boot (juicefs mount + first syncs) would churn for minutes.
	// Above the threshold, take the SAME start-while-offline path as a dead
	// backend: nav serves from the mirror instantly and the watchdog +
	// reconcile loop bring everything online in the background. Conservative
	// default (tonight's 42ms DERP-relay cellular booted fine in 19s —
	// deferral is for genuinely broken links); JM_BOOT_DEFER_RTT_MS tunes,
	// 0 disables (REVERT_LOG 2026-07-02).
	// Batch-3 adversarial review (HIGH follow-through): the U2 RTT defer is
	// SKIPPED on a persisted-offline (U3) boot. The defer works by fabricating
	// backendUp=false, which skips the boot fm.Mount() — but a U3 session
	// holds the USER offline flag, so the watchdog's gone-branch remount
	// stands down (fuseSkipRemountWhileUserOffline) and FUSE would NEVER
	// mount: the same pinned-files-unreadable failure on a slow-but-alive
	// link. A U3 boot already has everything the defer buys (startedOffline
	// is forced below: no boot sync, offline gates live, banner) EXCEPT the
	// mount skip — which is exactly the part that must not happen.
	bootRTTDeferred := false
	if backendUp && bootRTT > 0 && !bootUserOffline {
		deferMS := int64(500)
		if v := os.Getenv("JM_BOOT_DEFER_RTT_MS"); v != "" {
			if n, pErr := strconv.ParseInt(v, 10, 64); pErr == nil {
				deferMS = n
			}
		}
		if deferMS > 0 && bootRTT.Milliseconds() > deferMS {
			// Review fix (HIGH): do NOT just flip backendUp — a
			// reachable-but-slow backend ANSWERS the single connect attempt,
			// which would leave startedOffline=false and produce the worst
			// of both worlds (no offline gating, NFS serving over an
			// unmounted FUSE, and a fresh-install boot blocking on a
			// synchronous SCAN over the terrible link). Carry the decision
			// and force the FULL offline-start semantics after the connect
			// block below.
			backendUp = false
			bootRTTDeferred = true
			jmlog.Warn("backend reachable but RTT above boot-defer threshold — forcing the start-while-offline path (U2)",
				"rtt_ms", bootRTT.Milliseconds(), "threshold_ms", deferMS)
		}
	}
	if !backendUp {
		jmlog.Warn("metadata backend unreachable at startup — taking the start-while-offline path (serving cached navigation; reads resume when the backend returns)",
			"redis_url", cfg.RedisURL)
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
	//
	// S1 (2026-07-02): this block, metadata.Open(), and connectRedisWithRetry
	// are mutually independent (the mount needs only cfg + backendUp; connect
	// needs only the opened store), so they are run CONCURRENTLY and JOINED
	// before srv.Start(). No semantic change — identical work, identical
	// causal order (store→connect stays serial; the mount overlaps both),
	// only the ~1.4-7s fm.Mount() and the ~6s Open+~2s connect now overlap
	// instead of stacking (the ~16s serial boot → ~max(mount, open+connect)).
	// Extracted into a closure so the parallel and serial (JM_BOOT_PARALLEL=0
	// kill switch) paths share ONE copy of the mount logic verbatim. Runs
	// under the already-held globalMu; the only global it writes (globalFUSE /
	// globalFUSEPath / globalFUSEMetricsAddr) is disjoint from the store lane's
	// globalStore, and the parent blocks on the join before any other code
	// reads them, so there is no added race.
	mountFUSE := func() {
		if cfg.FUSEPath == "" {
			return
		}
		// Make sure the mount-point directory exists
		_ = os.MkdirAll(cfg.FUSEPath, 0o755)
		globalFUSEPath = cfg.FUSEPath
		// V2.3 G0: arm the FUSE identity gate. Drains, phantom purges, and
		// reconcile prunes all refuse to act while this path has no real
		// filesystem mounted on it (kext-approval loss / mount absent /
		// wedge) instead of silently operating against a plain local dir.
		pin.SetFUSEIdentityPath(cfg.FUSEPath)

		if globalFUSE != nil && fuseLooksHealthy(cfg.FUSEPath) {
			jmlog.Info("juicefs FUSE already mounted, reusing", "path", cfg.FUSEPath)
			return
		}
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
			RedisURL:        cfg.RedisURL,
			MountPoint:      cfg.FUSEPath,
			CacheSize:       cfg.CacheSize,
			FreeSpaceRatio:  "0.01",
			BucketOverride:  cfg.BucketOverride,
			PinnedBytes:     pinnedBytes,
			FUSEMetricsAddr: health.DefaultFUSEMetricsAddr,
			OpenCacheTTL:    cfg.OpenCacheTTL,
		})
		// Pin-capacity baseline (audit fix v2, 2026-07-14): record the
		// CONFIGURED budget before any mount attempt so the capacity
		// verdict caps correctly on EVERY mount path (inline success,
		// launch-fail → watchdog remount, adopt-existing). The
		// inline-success branch below refines it with the effective
		// (possibly auto-expanded) value; watchdog paths keep this
		// config baseline — the user's intent and the right ceiling
		// for pin math. Live gap: first deploy only wired the inline
		// branch and a watchdog-remounted boot reported budget 0.
		if mb, perr := strconv.Atoi(cfg.CacheSize); perr == nil && mb > 0 {
			pin.SetCacheBudgetBytes(int64(mb) << 20)
		}
		// Tell the block-cache scraper where the FUSE daemon's prometheus
		// /metrics endpoint is so /cache-status can report the TRUE on-disk
		// block-cache size (juicefs_blockcache_bytes) as cache_used_bytes.
		globalFUSEMetricsAddr = fm.EffectiveMetricsAddr()
		pin.SetBlockCacheMetricsAddr(globalFUSEMetricsAddr)
		// Start the watchdog and register globalFUSE UNCONDITIONALLY —
		// even if the initial mount fails (a transient backend blip at
		// launch). Pre-2026-05-29 this returned before StartMonitor() on
		// failure, so a launch-time error — especially on a restart that
		// had just Stopped the previous watchdog (see line ~208) — left the
		// app with NO FUSE self-heal at all. The watchdog retries once the
		// backend is reachable; registering globalFUSE keeps a later retry
		// from leaking a second monitor goroutine.
		// R-4: skip the SYNCHRONOUS boot-time mount when the backend is
		// unreachable. juicefs mount needs Redis as its metadata engine, so
		// fm.Mount() would block up to ~30s before failing — the dominant
		// offline-boot stall. We still StartMonitor + register globalFUSE so
		// the watchdog mounts it the moment the backend returns (the same
		// self-heal path a failed mount already relies on).
		var mountErr error
		if backendUp {
			mountErr = fm.Mount()
		}
		fm.StartMonitor()
		globalFUSE = fm
		if !backendUp {
			jmlog.Warn("deferred FUSE mount — backend unreachable at startup; the watchdog will mount it once the backend returns",
				"path", cfg.FUSEPath)
		} else if mountErr != nil {
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
		} else {
			// Note: FUSEManager.Mount may have auto-expanded CacheSize. Log
			// the *effective* config from the mount, not the user input —
			// otherwise the user reads "100 GiB" and is confused why the
			// daemon was actually launched with 800 GiB.
			jmlog.Info("juicefs FUSE mounted",
				"path", cfg.FUSEPath,
				"effective_cache_size_mb", fm.EffectiveCacheSize(),
				"free_space_ratio", "0.01")
			// Pin-capacity audit (2026-07-14): the user's cache budget is a
			// hard ceiling on what juicefs will keep resident — capacity
			// verdicts must cap at it, or a budget-exceeding pin set is
			// approved and then perpetually evicted.
			if mb, perr := strconv.Atoi(fm.EffectiveCacheSize()); perr == nil && mb > 0 {
				pin.SetCacheBudgetBytes(int64(mb) << 20)
			}
		}
	}

	// openStore opens the metadata store. Fatal on error (as before): a caller
	// with no store cannot serve nav. The connect lane runs AFTER this (it
	// needs the opened store), so store→connect stays serial; only the mount
	// overlaps them.
	openStore := func() (*metadata.Store, error) {
		// JuiceMount Link: bring up the embedded tailnet node first so backend
	// traffic can route over it during the rest of startup.
	mountHostOverride := startLinkIfConfigured(cfg)
	_ = mountHostOverride // T2.2 next step: thread into mountNFSWithPrompt

	// Open metadata store. If this fails, leave FUSE mounted — the next
		// Start can pick it up. Tearing FUSE down here would force an admin
		// password prompt on the next attempt, which is hostile.
		return metadata.Open(cfg.DBPath)
	}

	// Retry schedule: 1s, 2s, 4s, 8s, 16s = 5 attempts, ~31s total worst case.
	// R-4: when the up-front probe already found the backend down, don't burn
	// those ~31s — one quick attempt, then take the offline-start path.
	connectAttempts := 5
	if !backendUp {
		connectAttempts = 1
	}

	// S1: run mount ‖ (open → connect). The mount lane never returns from the
	// function (mount failure is non-fatal, handled inline in mountFUSE); the
	// store/connect lane can early-return (fatal open error / malformed URL),
	// so it stays in the main flow where the C-string returns are clean. The
	// only concurrency is the backgrounded mount; we JOIN it before any code
	// below reads globalFUSE or reaches srv.Start(). Kill switch
	// JM_BOOT_PARALLEL=0 runs the original strictly-serial order (mount, then
	// open, then connect) for a bisect if the orchestration is ever suspected.
	var store *metadata.Store
	var rc *metadata.RedisClient
	var connectErr error
	if os.Getenv("JM_BOOT_PARALLEL") == "0" {
		mountFUSE()
		var openErr error
		store, openErr = openStore()
		if openErr != nil {
			return C.CString(fmt.Sprintf("error: open store: %v", openErr))
		}
		globalStore = store
		// Connect to Redis with bounded retry. Network can be flaky — wifi/cell
		// handoffs, sleeping NAS, brief router restart. Without this, a 1s blip
		// at launch leaves the user staring at "redis: connect: no route to host"
		// even though the NAS comes back 3 seconds later.
		rc, connectErr = connectRedisWithRetry(cfg.RedisURL, store, connectAttempts)
	} else {
		var mountWG sync.WaitGroup
		mountWG.Add(1)
		go func() {
			defer mountWG.Done()
			mountFUSE()
		}()
		// Main flow: open the store, then (if it opened) connect — serial
		// because connect needs the store — while the mount runs concurrently.
		var openErr error
		store, openErr = openStore()
		if openErr != nil {
			// The mount goroutine writes only its own globals and never returns
			// from the function; join it so we don't abandon an in-flight mount
			// goroutine writing globalFUSE after we've released globalMu.
			mountWG.Wait()
			return C.CString(fmt.Sprintf("error: open store: %v", openErr))
		}
		globalStore = store
		// Connect to Redis with bounded retry (see the schedule note above).
		rc, connectErr = connectRedisWithRetry(cfg.RedisURL, store, connectAttempts)
		// JOIN the mount before srv.Start() (preserves pinned-files-instant —
		// NFS must not serve before the mount lane has registered globalFUSE +
		// armed the identity gate). Done here, before we touch rc/globalFUSE
		// below, so every downstream invariant sees the same post-join state as
		// the serial path.
		mountWG.Wait()
	}

	startedOffline := false
	// Batch-3 adversarial review #8: carry a per-cause reason for the
	// "started_offline:" status instead of always reusing the R-4 constant —
	// a deliberate user-offline relaunch was reported to Swift (and os_log'd)
	// as "backend unreachable at launch", a false causal story, and the U2
	// slow-backend defer claimed "unreachable" while its own SetAutoOffline
	// reason said "responding slowly".
	startOfflineReason := offlineStartReason
	if err := connectErr; err != nil {
		// R-4: do NOT abort. Start offline. A DEFERRED Redis client
		// (connected=false) keeps every downstream rc.* call site working
		// unchanged and self-heals via the reconcile loop (which flips
		// connected=true on its first successful sync once the backend returns).
		// Directory navigation serves from the SQLite mirror; file reads
		// fail-fast with NXIO until the watchdog remounts FUSE.
		drc, derr := metadata.NewRedisClientDeferred(cfg.RedisURL, store)
		if derr != nil {
			// Only a malformed Redis URL reaches here (ParseRedisURL) — a real
			// config error the user must fix, not a transient outage.
			store.Close()
			return C.CString(fmt.Sprintf("error: redis: %v", derr))
		}
		jmlog.Warn("started offline — metadata backend unreachable; serving cached navigation, will recover automatically",
			"reason", err.Error())
		rc = drc
		startedOffline = true
		// Engage offline mode NOW (don't wait the ~22s reachability debounce) so
		// the open/read gates AND the readdir empty-fast path are live from the
		// first RPC. LOAD-BEARING (not cosmetic): without it, navigating into an
		// un-synced subtree falls through to a FUSE timeout instead of returning
		// fast. connected=false on the deferred client also makes RecentlyDegraded
		// true, which suppresses the phantom-purge so the offline session can't
		// delete real entries from the mirror.
		pin.SetAutoOffline(true, offlineStartReason)
	}
	// V2.3 U2 review fix (HIGH): the RTT defer must yield the SAME offline-
	// start semantics as a dead backend even when the connect above
	// SUCCEEDED (a reachable-but-slow backend usually answers within the
	// 10s dial). With startedOffline forced true: the boot skips the
	// synchronous fm.Mount() and the U1 blocking-sync branch (no full SCAN
	// over the terrible link, fresh installs included), the offline
	// read/readdir gates are live from the first RPC, and Swift gets the
	// started_offline banner. Recovery is the standard R-4 machinery — the
	// reachability monitor + watchdog bring the mount and sync up in the
	// background (with a LIVE rc here, the first reconcile tick syncs
	// without waiting for a reconnect).
	if bootRTTDeferred && !startedOffline {
		startedOffline = true
		startOfflineReason = "backend responding slowly — started offline; recovering in background"
		pin.SetAutoOffline(true, startOfflineReason)
	}
	// V2.3 U3: persisted USER intent — engage the user flag (not auto), so
	// U4's watchdog stand-down holds and only an explicit toggle clears it.
	// Batch-3 review (HIGH): when the backend was reachable, FUSE is ALREADY
	// mounted above (the mount keys on backendUp, never on user intent) —
	// this block only engages the gates/banner and suppresses the boot sync,
	// so pinned files stay readable through the whole offline session.
	if bootUserOffline {
		startedOffline = true
		startOfflineReason = "offline mode is on (your setting from last session)"
		pin.SetOffline(true)
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
			health.WithRTTObserver(netprofile.Default().ObserveRTT),
			// Drain-liveness false-flap override (task #66 salvage, NFSv3
			// sprint): a completed drain (a MinIO PUT that landed) is positive
			// proof the backend is reachable over the SAME link the probe
			// dials. When the drainer saturates the uplink, a cold probe SYN
			// can queue behind its bulk PUT traffic and exceed the dial
			// timeout — a FALSE "unreachable" that would arm the 18s
			// offline-engage deferral and (pre-fix) let a reconcile prune real
			// files mid-copy. This hook reports the age of the last proven
			// drain; the monitor suppresses a probe FAILURE while that age is
			// within ~2*baseInterval. Review fix (phase-1 adversarial review):
			// the hook runs on the reachability prober's goroutine — taking
			// globalMu here would block the ENTIRE offline-detection loop for
			// as long as NFSServerStart holds globalMu (its whole body, which
			// includes this very Reachability.Start), freezing offline
			// detection exactly during start-while-offline. Read the drainer
			// through a dedicated atomic instead (LastDrainSuccess is
			// internally synchronized). Nil (spool disabled / server stopped)
			// → MaxInt64 sentinel → the override never fires.
			health.WithLivenessHook(func() time.Duration {
				d := globalDrainerAtomic.Load()
				if d == nil {
					return time.Duration(math.MaxInt64)
				}
				last := d.LastDrainSuccess()
				if last.IsZero() {
					return time.Duration(math.MaxInt64)
				}
				return time.Since(last)
			}))
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
		// R-4: if we started offline, seed the monitor "unreachable" so the FIRST
		// successful probe is a real unreachable→reachable transition that fires
		// the OnChange(true) branch above → pin.SetAutoOffline(false) + TriggerSync.
		// Without this the monitor boots "presumed reachable" and a backend that
		// returns quickly never transitions, leaving the boot-engaged offline mode
		// stuck on. Must precede Start() (it seeds the state the first probe moves off).
		if startedOffline {
			globalReach.SeedUnreachable()
		}
		globalReach.Start()
	}

	// [metadata keyspace push] Wire the class-gating signals BEFORE rc.Start so
	// the keyspace loop (started inside Start when JM_METADATA_KEYSPACE_PUSH=1)
	// can class-gate its rare-backstop cadence + coalescer from launch.
	// Review fix (phase-1 adversarial review): the reachability signal is
	// wired UNCONDITIONALLY — RecentlyDegraded's reachableNow() gate (the G2
	// b.1 false-flap prune protection) must be live in every config, not
	// only when the push is enabled. Only the NetWatcher (active-interface
	// NAME signal, an extra 1s-poll goroutine) stays push-gated; with a nil
	// interface fn, class-gating falls back to JM_WAN_MODE + a WiFi default
	// — safe and link-sparing, byte-identical to before for push-off.
	var reachFn func() bool
	if globalReach != nil {
		reachFn = globalReach.Reachable
	}
	var ifaceFn func() string
	if os.Getenv("JM_METADATA_KEYSPACE_PUSH") == "1" {
		// G8 (task #81): classify by the route to the BACKEND, not the default
		// route. Proven live 2026-07-02 (hotspot + Tailscale): the NAS route was
		// utun6 but the default-route heuristic reported en0, so currentLinkClass
		// said WiFi on a metered tunnel — G7's SCAN budget used 120s instead of
		// 300s, the SCAN never finished, engagement never ENABLED, and the 60s
		// retry loop burned the metered link (also mis-gated G6's deferral, the
		// backstop cadence, and the coalescer). WithBackendTarget makes the
		// watcher resolve the interface the kernel routes to the Redis host
		// (connected-UDP trick, no probe traffic; resolver failure falls back to
		// the old default-route behavior — see health/netwatch.go). JM_WAN_MODE /
		// JM_NET_FORCE_CLASS override precedence is untouched: those are consulted
		// in metadata.currentLinkClass / netprofile BEFORE this signal.
		var opts []health.NetWatcherOption
		if backendAddr, _, _ := metadata.ParseRedisURL(cfg.RedisURL); backendAddr != "" {
			opts = append(opts, health.WithBackendTarget(backendAddr))
		}
		globalKeyspaceNetWatcher = health.NewNetWatcher(1*time.Second, opts...)
		globalKeyspaceNetWatcher.Start()
		ifaceFn = globalKeyspaceNetWatcher.ActiveInterface
	}
	metadata.SetClassSignals(ifaceFn, reachFn)

	// Initial sync. R-4: skip when started offline — it would only fail against
	// the dead backend. rc.Start() still launches the reconcile loop, which flips
	// connected=true and catches up the mirror on its first success once the
	// backend returns.
	//
	// V2.3 U1 (serve-first boot, field report "5+ min to first items"): the
	// full-tree SyncOnce used to BLOCK here, ahead of the NFS server and the
	// control plane — 136s of empty Finder over a cellular relay (measured
	// 2026-07-02: mount up in 5s, everything else waiting on this call). The
	// mirror is already RAM-hydrated from disk (metadata.Open→rebuildCaches),
	// so serving it immediately is exactly what offline mode already does;
	// the sync runs in the background (syncMu single-flights it against the
	// reconcile loop) and IsSyncing drives the "Rebuilding index…" indicator.
	// Blocking is kept for: (a) an EMPTY mirror (fresh install / wiped DB) —
	// there is nothing to serve and an empty Finder tree is worse than a
	// short wait; (b) JM_BOOT_SYNC_FIRST=1 (kill switch, restores the old
	// ordering; REVERT_LOG 2026-07-02).
	// Review fix (G6/U1 adversarial review): the background sync must not
	// reach its prune pass before the prune guards are wired — SetPinChecker
	// (Layer C) and SetSpoolGuard (Layer D) are installed further down in
	// this function. bootSyncWired is closed by the deferred call on EVERY
	// exit path (success = all wiring done; error = rc is being torn down
	// and the goroutine's SyncOnce fails harmlessly), so the goroutine can
	// never run a guard-less prune and never leaks.
	bootSyncWired := make(chan struct{})
	defer close(bootSyncWired)
	if !startedOffline {
		// C1 (2026-07-02): skip the boot SCAN entirely when the mirror is fresh
		// AND keyspace push is engaged. In that config the PSUBSCRIBE gap-fill +
		// the periodic backstop already guarantee convergence, so a full boot
		// SCAN over a recently-synced mirror is redundant work (the 174s
		// "Rebuilding index…" spinner over a cellular relay). ShouldSkipBootSync
		// fails safe on a first-run/wiped mirror (no persisted last_sync_time),
		// on push-off, on a stale timestamp, and under the JM_BOOT_SYNC_SKIP=0
		// kill switch — every one of those falls through to today's behavior
		// below. We deliberately do NOT stamp lastSyncStartedAt here, so
		// IsSyncing() stays false and G7's /activity never shows "Rebuilding
		// index…" for a skipped boot. rc.Start() below still launches the
		// reconcile + keyspace loops as normal.
		if rc.ShouldSkipBootSync() {
			jmlog.Info("boot SCAN skipped — mirror fresh + push engaged; PSUBSCRIBE + backstop carry deltas")
		} else {
			bootSyncBlocking := os.Getenv("JM_BOOT_SYNC_FIRST") == "1"
			if !bootSyncBlocking {
				if n, cErr := store.Count(); cErr != nil || n == 0 {
					bootSyncBlocking = true
				}
			}
			if bootSyncBlocking {
				if err := rc.SyncOnce(); err != nil {
					jmlog.Warn("initial sync failed", "error", err.Error())
				}
			} else {
				go func() {
					<-bootSyncWired
					t0 := time.Now()
					jmlog.Info("initial metadata sync running in background (serve-first boot)")
					if err := rc.SyncOnce(); err != nil {
						jmlog.Warn("background initial sync failed — reconcile loop retries",
							"error", err.Error())
						return
					}
					jmlog.Info("background initial sync complete",
						"duration_ms", time.Since(t0).Round(time.Millisecond).Milliseconds())
				}()
			}
		}
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

	jmlog.Info("STARTUP-TRACE: A-post-srvStart")
	if globalCache != nil {
		srv.Handler().SetCacheReader(globalCache)
	}
	jmlog.Info("STARTUP-TRACE: B-post-cacheReader")
	srv.Handler().SetRedisClient(rc)
	jmlog.Info("STARTUP-TRACE: C-post-redisClient")
	pt := jmnfs.NewPresenceTracker(rc.RawDB())
	jmlog.Info("STARTUP-TRACE: D-post-presenceTracker")
	srv.Handler().SetPresence(pt)
	globalPresence = pt // already holding globalMu from function entry
	jmlog.Info("STARTUP-TRACE: E-post-globalMu")

	if globalFUSE != nil {
		h := srv.Handler()
		globalFUSE.SetOnRemount(func() {
			closed, marked := h.FlushStaleFDs()
			jmlog.Info("fd pool flushed after FUSE remount (#12)",
				"closed_idle", closed, "marked_stale_held", marked)
		})
	}
	globalServer = srv
	jmlog.Info("STARTUP-TRACE: F-post-globalServer")

	// Pin store + prefetcher. The pin store lives in its own SQLite file so
	// it doesn't compete with the metadata store's WAL.
	jmlog.Info("BOOT-TRACE: step-2 pre-pin-store")
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
		// R-1: keep the pinned-set-vs-disk-capacity verdict fresh. Gates the
		// re-warm loop above (no futile thrash when the pinned set can't fit)
		// and feeds the over-capacity banner in /cache-status. Empty cacheBaseDir
		// uses the default ~/.juicefs/cache.
		pf.Go(func() { pf.CapacityLoop(pinCtx, 60*time.Second, "") })
		// Eviction watch (pin-integrity guarantee, 2026-07-14): juicefs
		// eviction has no pin awareness, so transient traffic pushing the
		// cache past its budget can evict pinned blocks even when the
		// pinned set itself fits. CapacityLoop refreshes CacheUsageBytes
		// every 60s; a large drop with a stable pinned set means eviction
		// churn ran — schedule ONE VerifyAndRepair (re-read: present
		// blocks local-speed, missing blocks re-pulled) so pinned content
		// converges back to fully-resident within minutes instead of the
		// 6h re-warm TTL. Gated: never over-capacity (R-1 thrash guard),
		// never on Metered/Slow links (the re-pull belongs on LAN), and
		// single-flight with a cooldown.
		pf.Go(func() { evictionWatchLoop(pinCtx) })
		// Wire the pin store into the NFS handler so the offline-mode
		// open gate can fail-fast on un-pinned reads. The mount point is
		// the prefix the gate uses to canonicalize in-mount filenames into
		// the absolute paths the pin store keys on.
		srv.Handler().SetPinStore(ps, cfg.MountPoint)
		jmlog.Info("BOOT-TRACE: step-3 pin-store-ready", "path", pinDBPath, "workers", 4)

		// Item 0: heal dirs pinned in a PRIOR session. On boot the pin store
		// remembers the pinned roots but the metadata mirror may not have their
		// subtree rows (older build, or the warm never completed) — so those
		// dirs would list empty offline. Enumerate PinRoots() and warm each
		// root's ancestor chain + subtree. Runs in the background so it never
		// blocks boot; self-gates on JM_PIN_WARM_METADATA and pin.IsOffline()
		// (offline reconcileDir burns a 30s Redis timeout per dir — the metadata
		// warm resumes when online; blocks are already cached).
		warmMount := cfg.MountPoint
		go warmPinnedRootsAtBoot(ps, rc, warmMount)
	} else {
		jmlog.Warn("pin store open failed (offline-pin disabled)", "error", err.Error())
	}

	// Tier-B shared-derivative index (contract JM-14). Its own SQLite file
	// alongside metadata.db / pin.db. Read-only query path (/derivatives,
	// /metadata); the farm (JM-16) + JM-15 sync populate it later. Open failure
	// is non-fatal — the endpoints fail closed (exists:false) until it's back.
	derivDBPath := derivStorePath(cfg.DBPath)
	if ds, err := derivatives.Open(derivDBPath); err == nil {
		globalDerivStore = ds
		jmlog.Info("derivative index ready", "path", derivDBPath)
	} else {
		jmlog.Warn("derivative index open failed (JM-14 reads disabled)", "error", err.Error())
	}

	// #1 (INSTANT-NAV) hydration pack: bounded local thumb cache + the
	// folder-open warmer. The warmer hydrates farm poster thumbnails (kind
	// "thumbnail", ~10-50KB) into the local cache when a dir is listed, so
	// preview bytes are local before anything asks for source bytes; /blob
	// serves small kinds local-first from the same cache. Fail-quiet: no
	// derivative index / cache-open failure just leaves them nil.
	// Kill switch JM_THUMB_WARM=0; size JM_THUMB_CACHE_MB (default 2048).
	if os.Getenv("JM_THUMB_WARM") != "0" && globalDerivStore != nil && derivDBPath != ":memory:" {
		maxBytes := int64(thumbcache.DefaultMaxBytes)
		if v := os.Getenv("JM_THUMB_CACHE_MB"); v != "" {
			if mb, perr := strconv.Atoi(v); perr == nil && mb >= 64 && mb <= 65536 {
				maxBytes = int64(mb) << 20
			}
		}
		thumbDir := derivDBPath[:len(derivDBPath)-len("/derivatives.db")] + "/thumbs"
		if tc, terr := thumbcache.Open(thumbDir, maxBytes); terr == nil {
			globalThumbCache = tc
			warmer := jmnfs.NewThumbWarmer(tc, resolveThumbBlobPath, func(dir string) []jmnfs.ThumbChildRef {
				entries, lerr := store.ListChildren(dir)
				if lerr != nil {
					return nil
				}
				refs := make([]jmnfs.ThumbChildRef, 0, len(entries))
				for _, e := range entries {
					refs = append(refs, jmnfs.ThumbChildRef{Inode: e.Inode, Name: e.Name, IsDir: e.IsDir})
				}
				return refs
			})
			globalThumbWarmer = warmer
			srv.Handler().SetThumbWarmer(warmer)
			// Sidecar-cache disk persistence (2026-07-13): `._`/.DS_Store
			// bodies survive restarts, so a re-launch no longer re-cools
			// every folder (the tunnel first-visit tax was ~2min/dir).
			// Same parent dir as the thumb cache; per-serve mirror
			// validation makes loading stale entries harmless.
			srv.Handler().SidecarPersistEnable(thumbDir + "/../sidecars.gob")
			st := tc.Stats()
			jmlog.Info("thumb cache ready", "path", thumbDir,
				"resident_mb", st.Bytes>>20, "files", st.Files, "max_mb", maxBytes>>20)
		} else {
			jmlog.Warn("thumb cache open failed (hydration pack disabled)", "error", terr.Error())
		}
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

					// Reclaim streamed partials orphaned by a crash. MUST run
					// AFTER RecoverOnBoot, which settles which rows are still
					// live — the sweep only deletes partials belonging to
					// not-yet-done rows, so running it first would consult
					// pre-recovery state.
					//
					// Nothing else can ever clean these up: a partial is filtered
					// out of the mirror, skipped by the farm, and dot-hidden from
					// the user. Each of those is correct in isolation and together
					// they mean an interrupted streamed copy would hold hundreds
					// of gigabytes of backend space indefinitely. A no-op unless
					// streaming has actually run.
					// OFF THE STARTUP CRITICAL PATH (2026-08-19, live incident).
					// The sweep stats EVERY not-done spool row against the FUSE
					// mount, serialized. That is O(pending rows) metadata round
					// trips, and metadata is this system's slowest resource
					// (~15 ops/s per directory). It used to run inline here —
					// which is ~180 lines BEFORE the control plane binds — so a
					// 16,150-row backlog from an ordinary Finder copy stopped
					// JuiceMount from finishing startup at all: juicefs mounted,
					// NFS listened on :11049, and :11050 never opened. The app
					// looked hung; it was counting.
					//
					// The sweep must precede the DRAINER (a concurrent drain
					// would re-create a temp this is about to delete), not the
					// control plane. So it keeps its ordering guarantee against
					// the drainer and gives up the one it never needed.
					//
					// drainer.Start() therefore moves INTO this goroutine. Stop()
					// is safe against a drainer that was never started — its
					// `started` flag gates the wait on d.done — so a shutdown
					// racing a long sweep cannot hang.
					go func() {
						if err := drainer.SweepStreamPartials(); err != nil {
							jmlog.Warn("stream partial sweep failed (proceeding anyway)",
								"error", err.Error())
						}
						drainer.Start()
						jmlog.Info("stream partial sweep complete — drainer started")
					}()

					globalSpool = spool
					globalDrainer = drainer
					globalDrainerAtomic.Store(drainer)
					srv.Handler().SetSpool(spool, drainer)
					// QA-30 Layer D: let the reconcile's scopedPrune spare any
					// path with a live, not-yet-drained spool entry so a
					// spool-pending file (absent from Redis AND FUSE) is never
					// pruned mid-copy — which would Forget its Track-B NFS handle
					// and surface ESTALE (build-438 error 100070). Must precede
					// rc.Start's reconcile goroutine, which already launched
					// above; safe because no prune can fire before the first
					// keyspace/SCAN reconcile and the spool index is live now.
					rc.SetSpoolGuard(spool.HasPending)
					// drainer.Start() is deliberately NOT here — it runs in the
					// sweep goroutine above, so the drain cannot begin until the
					// sweep that protects it has finished.
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

	// [#6 metrics-server-first] Record the intended mount point NOW, but do
	// the actual NFS mount AFTER the metrics server is up (below). The mount
	// step can hang or fail (haunted mountpoint EBUSY, osascript admin
	// prompt, wedged diskarbitrationd — all observed live 2026-07-10), and
	// when it ran first, a hung mount left the app fully functional but
	// HEADLESS: no /health, /offline, /diagnose, /spool — undebuggable and
	// uncontrollable unattended. The control plane must never be gated on
	// the mount.
	if cfg.MountPoint != "" {
		globalWantMountPoint = cfg.MountPoint
	}

	// Wire NFS RPC observation into the metrics package.
	jmlibnfs.SetObserver(metrics.ObserveRPC)
	enableContentionProfilers()

	jmlog.Info("BOOT-TRACE: step-6 pre-metrics-routes")
	// Attach control-plane routes to the already-running metrics server
	// (started early per F1). Safe on a live listener: Go 1.22+ ServeMux.
	if cfg.MetricsAddr != "" && ms != nil {
		// Register pin/offline control endpoints on the same listener so the
		// CLI doesn't need a separate port.
		routes := map[string]http.HandlerFunc{
			"/pin":          handlePinHTTP,
			"/unpin":        handleUnpinHTTP,
			"/cache-status": handleCacheStatusHTTP,
			"/offline":      handleOfflineHTTP,
			// Contract endpoints (juicemount-contract v1). /whoami: identity +
			// version + capabilities handshake (JM-1). /residency: honest
			// per-path cache residency for OpenLoupe's green badge (JM-2).
			// /lookup: stable (inode, nas_rel_path) identity (JM-4).
			"/whoami":    handleWhoamiHTTP,
			"/residency": handleResidencyHTTP,
			"/lookup":    handleLookupHTTP,
			// JM-14 Tier-B shared-derivative reads. /derivatives?inode=N: the
			// per-asset derivative manifest (what machine-derived artifacts
			// exist + their integrity hash). /metadata?inode=N&kind=tech:
			// structured ffprobe tech/EXIF. Keyed by durable inode (from
			// /lookup); read-only + fail-closed.
			"/derivatives":         countingManifestHandler(handleDerivativesHTTP),
			"/derivatives/changes": handleDerivativesChangesHTTP,
			// JM-22 bulk manifest query: POST /derivatives/batch answers
			// "which of these inodes do you have?" for up to 1000 inodes in one
			// round trip. NO on-miss reconcile (unlike /derivatives) — that is
			// what makes it cheap.
			"/derivatives/batch": handleDerivativesBatchHTTP,
			"/metadata":          handleMetadataHTTP,
			// PROXY-CODEC (#50) byte-range blob delivery. GET /blob?inode=N&kind=proxy
			// streams a derivative blob (proxy.mp4 etc.) with Accept-Ranges: bytes,
			// answers Range with 206 + Content-Range/Content-Length, serves the
			// Content-Type from the manifest media_type, and 200 + Content-Length
			// unranged — what a browser <video> / remote AVPlayer need to seek over
			// HTTP. Capability token `blob` (route path == token).
			"/blob":        countingBlobHandler(handleBlobHTTP),
			"/deriv-reads": handleDerivReadsHTTP,
			// #1 hydration pack observability: local thumb-cache stats
			// (bytes/files/hits/misses/puts/evictions). Read-only.
			"/thumbs": handleThumbsHTTP,
			// Wave-3 QuickLook appex fetch: GET /thumb-local?path=<abs>&size=N
			// serves the farm poster from the LOCAL cache (hit <5ms), does a
			// bounded read-through populate on miss, or 404s fast so the appex
			// errors and macOS falls back to its own generator.
			"/thumb-local": handleThumbLocalHTTP,
			// Release UX: the popover's warm-up card — one consolidated phase
			// machine (starting/indexing/warming/steady) with a progress pct.
			"/warmup": handleWarmupHTTP,
			// JM-ASSERT (#51) portable-human-metadata channel. POST /assertions writes
			// the <media>.loupe.json sidecar (source of truth — atomic, LWW,
			// merge-not-clobber) + upserts the asset_key-keyed Tier-B index; GET
			// reads the resolved set by asset_key (or inode/path → asset_key).
			// Capability token `assertions` (route path == token).
			"/assertions": handleAssertionsHTTP,
			// OL-1 on-device AI contribute-back (write). POST: a consumer that
			// wrote ai.loupe.json through the mount registers it so the server
			// indexes it for every client + the web UI. Capability token =
			// `contribute` (route-aliased in cplane.DeriveCapabilities).
			"/derivatives/register": handleDerivativesRegisterHTTP,
			"/reclaim":              handleReclaimHTTP,
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
			// Tier-1 #3: cross-Mac who-has-this-open. GET /presence → this
			// Mac's open write-handles; GET /presence?all=1 → every host.
			"/presence": handlePresenceHTTP,
			// Background-operation activity surface (roadmap 4.10): plain-language
			// view of reconcile / drain / prefetch so the UI can explain why
			// Finder is momentarily slow ("Uploading 412 files", "Rebuilding
			// index…", "Warming pinned project"). GET, loopback.
			"/activity": handleActivityHTTP,
			// "Why is it slow?" self-diagnosis (INSTANT-NAV #14): runs the six
			// known silent-failure probes (Local Network permission, tunnel
			// route, FUSE identity, backend components, spool backlog, link
			// RTT) concurrently and time-bounded, on demand only. GET, loopback.
			// Not in the contract capability vocabulary — operational/UI route,
			// excluded from /whoami automatically like reclaim/mount-now.
			"/diagnose": handleDiagnoseHTTP,
			// Instant recursive folder size (INSTANT-NAV #2): GET /du?path=…
			// answers "total bytes + file count under this folder" in O(1)
			// from the mirror's incrementally-maintained subtree aggregates
			// (JM_SUBTREE_SIZES, default on) — the app/OpenLoupe surface that
			// replaces du-walking. GET, loopback. Operational/UI route,
			// excluded from /whoami automatically like reclaim/mount-now.
			"/du": handleDuHTTP,
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
			// /debug/pprof/mutex and /debug/pprof/block are served by
			// pprof.Index's trailing-slash handler, but they are USELESS
			// until the runtime is told to sample: both profilers default
			// to off, so those two endpoints returned "sampling period=0"
			// and an empty profile for every one of them.
			//
			// That is the one instrument this codebase most needed and never
			// had. A CPU profile CANNOT see blocking by construction — it
			// samples running goroutines — so the 2026-08-19 finding that
			// "our RPC handling is only 12.73% of the profile" said nothing
			// about a read path whose ceiling (~1,650 MB/s, flat from 16 to
			// 88 in-flight RPCs) sits ~10x below the FUSE layer beneath it
			// (17,108 MB/s at 4 readers) and below the loopback transport
			// above it (6,949 MB/s). Contention is exactly what a mutex or
			// block profile shows and a CPU profile hides.
			//
			// Both stay OFF by default: they add cost to every lock and every
			// blocking operation, which is not something to carry in a
			// shipped mount daemon. Set the env var to sample.
			"/debug/pprof/":        pprof.Index,
			"/debug/pprof/cmdline": pprof.Cmdline,
			"/debug/pprof/profile": pprof.Profile,
			"/debug/pprof/symbol":  pprof.Symbol,
			"/debug/pprof/trace":   pprof.Trace,
		}
		// Derive the contract capability list (JM-1) from the routes this binary
		// actually serves plus the metrics-server built-ins (/health, /metrics),
		// so /whoami can never advertise a route that isn't registered.
		served := []string{"/health", "/metrics"}
		ms.RegisterRoutes(routes)
		for route := range ms.ExtraRoutes {
			served = append(served, route)
		}
		globalCapabilities = cplane.DeriveCapabilities(served)
	}

	// Mount NFS at the user-visible mount point (e.g. /Volumes/zpool) so
	// Finder can browse it. Runs AFTER the metrics server (see the [#6
	// metrics-server-first] note above) so a hung/failed mount never leaves
	// the app headless. Requires sudo, obtained via an AppleScript "with
	// administrator privileges" prompt the user accepts once.
	//
	// Idempotent path: if the user already has an NFS mount at this path
	// from a previous soft-stop cycle, reuse it. Re-running mount_nfs would
	// fail because the mount point is busy and would prompt for a password
	// for no reason.
	// DEADLOCK FIX (2026-07-10, INSTANT-NAV): this mount attempt used to run
	// INLINE here — inside NFSServerStart's whole-body globalMu hold — and
	// mountNFSWithPrompt blocks on an INTERACTIVE admin-password prompt. An
	// unanswered prompt (user away, unattended window, or the kernel-haunted
	// mountpoint forcing a prompt every boot) therefore deadlocked every
	// globalMu consumer: /du, /diagnose, /thumbs, offline detection — while
	// /health kept answering, masking it. Now: reuse-check + mount run on
	// their own goroutine AFTER the boot wiring, taking globalMu only for
	// the globalMountPath publish; the prompt itself is bounded (180s) in
	// mountNFSWithPrompt. Boot never waits on a human again.
	if cfg.MountPoint != "" {
		jmlog.Info("BOOT-TRACE: step-4 pre-mount-check")
		if isMounted(cfg.MountPoint) {
			jmlog.Info("nfs already mounted, reusing", "mount_point", cfg.MountPoint)
			warmupMarkServing()
			globalMountPath = cfg.MountPoint
		} else {
			mountAddr, mountPoint := srv.Addr(), cfg.MountPoint
			go func() {
				if err := mountNFSWithPrompt(mountAddr, mountPoint); err != nil {
					jmlog.Warn("nfs mount failed (server still running)",
						"mount_point", mountPoint, "error", err.Error())
					// Non-fatal — the server is up, user can mount manually.
					return
				}
				jmlog.Info("nfs mounted", "mount_point", mountPoint)
				warmupMarkServing()
				globalMu.Lock()
				globalMountPath = mountPoint
				globalMu.Unlock()
			}()
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
	jmlog.Info("BOOT-TRACE: step-7 pre-monitor")
	globalMonitor = health.New(health.Config{
		RedisURL:      redisAddr,
		MinIOURL:      "", // TODO: make configurable
		FUSEPath:      cfg.FUSEPath,
		NFSMountPoint: cfg.MountPoint,
	})
	// L1 (2026-08-06): the network-change grace period had NEVER FIRED in the
	// shipping app.
	//
	// health/monitor.go:369 suppresses a transient FUSE failure while the
	// network is re-establishing, gated on InGracePeriod(). That returns false
	// unconditionally when netWatcher is nil (monitor.go:347) — and the ONLY
	// caller of SetNetWatcher anywhere was cmd/jm5 (the dev CLI). Both objects
	// have existed in this file all along and were simply never connected, so
	// every WiFi<->cellular flip reported FUSE unhealthy immediately instead of
	// riding out the reconnect.
	//
	// Reuse the watcher the keyspace consumer already runs rather than starting
	// a second one: it is created above (globalKeyspaceNetWatcher) and polls the
	// active interface every 1s, which is exactly the signal the grace period
	// needs. Nil-guarded because that watcher is created conditionally — a nil
	// here would merely restore the old behaviour, but being explicit keeps the
	// dependency visible to the next reader.
	if globalKeyspaceNetWatcher != nil {
		globalMonitor.SetNetWatcher(globalKeyspaceNetWatcher)
	}
	// L5b (2026-08-06): SetStatsProvider had no caller either, so /health
	// reported PathCacheSize/FDPoolOpen/MemBuf* as hard zeros while the fields,
	// the struct and the copy at health/monitor.go:425 all existed. Found by the
	// dead-wiring sweep AFTER the /metrics half was fixed — the two halves of
	// this chain were separately dead, which is exactly why a sweep beats
	// finding them one at a time.
	//
	// Zeros are reported when no mount is up (LiveFDStats ok=false). That is
	// honest here: /health describes a running server, and its consumer is a
	// status indicator rather than an fd investigation. The absent-vs-empty
	// distinction that matters for leak-hunting is preserved in /metrics, which
	// omits the section entirely.
	globalMonitor.SetStatsProvider(func() health.MemoryStats {
		open, active, mbEntries, mbMB, ok := jmnfs.LiveFDStats()
		if !ok {
			return health.MemoryStats{}
		}
		return health.MemoryStats{
			FDPoolOpen:    open,
			FDPoolActive:  active,
			MemBufEntries: mbEntries,
			MemBufSizeMB:  mbMB,
		}
	})
	// #89 online busy-suppression: give the health monitor a lock-free view of
	// the drainer's live ingest state so a stat/readdir TIMEOUT during a
	// legitimate high-concurrency ingest is reported as "busy (heavy ingest)"
	// rather than the false "mount losing communication" degraded state. Mirrors
	// the reachability WithLivenessHook idiom EXACTLY (read the drainer via the
	// dedicated atomic, never globalMu — the probe runs on the health-check
	// goroutine and must not block on NFSServerStart holding globalMu). Nil
	// drainer (spool disabled / server stopped) → inFlight 0 + MaxInt64 age
	// sentinel → busyIngesting() is false → no suppression (byte-identical to
	// the pre-drainer behavior).
	globalMonitor.SetDrainProbe(func() (int64, time.Duration) {
		d := globalDrainerAtomic.Load()
		if d == nil {
			return 0, time.Duration(math.MaxInt64)
		}
		inFlight := d.Metrics().InFlight.Load()
		since := time.Duration(math.MaxInt64)
		if last := d.LastDrainSuccess(); !last.IsZero() {
			since = time.Since(last)
		}
		return inFlight, since
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
		// #93 NFS-layer absent-mount recovery: when the volume is genuinely
		// GONE from the mount table while juicefs is alive and this listener
		// is up, the monitor re-runs the SAME two-tier mount boot uses
		// (passwordless sudo → bounded 180s prompt). Shares /mount-now's
		// single-flight CAS so it can never stack a prompt on a user click.
		globalMonitor.SetNFSServerAddr(remountAddr)
		globalMonitor.EnableNFSAbsentRemount(func() error {
			if !mountNowInFlight.CompareAndSwap(false, true) {
				return fmt.Errorf("mount already in flight")
			}
			defer mountNowInFlight.Store(false)
			if err := mountNFSWithPrompt(remountAddr, remountPoint); err != nil {
				return err
			}
			globalMu.Lock()
			globalMountPath = remountPoint
			globalMu.Unlock()
			return nil
		})
	}
	globalMonitor.Start()
	jmlog.Info("BOOT-TRACE: step-8 monitor-started")

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

	// Expose backend-vs-cache byte accounting on /metrics. This is the only
	// place a cellular run can learn whether an open was served from the local
	// SSD or pulled over the link — our own bytes_read cannot distinguish them.
	// Returns nil (field omitted) until the juicefs daemon has been scraped
	// successfully, so a failed scrape never reports as a zero-backend session.
	metrics.Default().SetBackendProvider(func() *metrics.BackendSnapshot {
		bs, ok := pin.BackendStatsSnapshot()
		if !ok {
			return nil
		}
		return &metrics.BackendSnapshot{
			CacheHits:      bs.CacheHits,
			CacheMiss:      bs.CacheMiss,
			CacheHitBytes:  bs.CacheHitBytes,
			CacheMissBytes: bs.CacheMissBytes,
			ObjectGetBytes: bs.ObjectGetBytes,
			ObjectPutBytes: bs.ObjectPutBytes,
			MetaOps:        bs.MetaOps,
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
			HighLatency:      netprofile.Default().HighLatency(),
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

	// R-4: when started offline, hand Swift a "started_offline:" status so it
	// enters .running with the "showing cached state" banner instead of treating
	// the start as a failure. The NFS share is already mounted (above); the
	// listen addr remains available via the stats path. Recovery is automatic
	// (watchdog remounts FUSE; reachability OnChange lifts auto-offline).
	if startedOffline {
		// Batch-3 review #8: per-cause reason (R-4 unreachable / U2 slow
		// backend / U3 persisted user intent), not the R-4 constant for all.
		return C.CString("started_offline: " + startOfflineReason)
	}
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
	keyspaceNW := globalKeyspaceNetWatcher
	spool := globalSpool
	drainer := globalDrainer
	globalDrainerAtomic.Store(nil)
	derivStore := globalDerivStore
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
	globalKeyspaceNetWatcher = nil
	globalSpool = nil
	globalDrainer = nil
	globalDerivStore = nil
	thumbWarmer := globalThumbWarmer
	globalThumbWarmer = nil
	globalThumbCache = nil
	globalMu.Unlock()

	// Now run the slow shutdown work on the snapshots, no lock held.
	// During this window, Stats / IsRunning / CacheStatus correctly
	// report "Running: false" — we already nil'd the publicly-visible
	// state, so the answer is honest, not a lie.
	// DIAGNOSTIC (2026-07-15): per-step logging to pinpoint the "Stop
	// everything"/quit teardown deadlock — the last "shutdown step" logged
	// before the freeze names the component whose Stop()/Close() hangs.
	if thumbWarmer != nil {
		jmlog.Info("shutdown step", "component", "thumbWarmer.Stop")
		thumbWarmer.Stop()
	}
	if server != nil {
		// Final sidecar-cache snapshot: a clean stop preserves every warmed
		// `._`/.DS_Store body for the next launch (the periodic saver bounds
		// loss on a hard kill to the last interval).
		jmlog.Info("shutdown step", "component", "SidecarPersistStop")
		server.Handler().SidecarPersistStop()
		warmupReset()
	}
	if metricsSrv != nil {
		jmlog.Info("shutdown step", "component", "metricsSrv.Stop")
		metricsSrv.Stop()
	}
	if monitor != nil {
		jmlog.Info("shutdown step", "component", "monitor.Stop")
		monitor.Stop()
	}
	if server != nil {
		// StopHandler tears down drainer + spool internally (slice C
		// integration), but we re-snapshot the globals above and
		// fall through to redundant Stop calls below as belt-and-
		// suspenders for the case where SetSpool was bypassed for
		// some reason — the global is the durable handle.
		jmlog.Info("shutdown step", "component", "StopHandler")
		server.Handler().StopHandler()
		jmlog.Info("shutdown step", "component", "server.Stop")
		server.Stop()
	}
	if drainer != nil {
		// Belt-and-suspenders: handler StopHandler already drained
		// this 30 s above. Calling Stop again is idempotent.
		jmlog.Info("shutdown step", "component", "drainer.Stop")
		drainer.Stop(5 * time.Second)
	}
	if spool != nil {
		jmlog.Info("shutdown step", "component", "spool.Stop")
		spool.Stop()
	}
	if cache != nil {
		jmlog.Info("shutdown step", "component", "cache.Stop")
		cache.Stop()
	}
	if rc != nil {
		jmlog.Info("shutdown step", "component", "rc.Stop")
		rc.Stop()
	}
	if reach != nil {
		jmlog.Info("shutdown step", "component", "reach.Stop")
		reach.Stop()
	}
	if keyspaceNW != nil {
		jmlog.Info("shutdown step", "component", "keyspaceNW.Stop")
		keyspaceNW.Stop()
	}
	if prefetcher != nil {
		jmlog.Info("shutdown step", "component", "prefetcher.Stop")
		prefetcher.Stop()
	}
	if pinStore != nil {
		jmlog.Info("shutdown step", "component", "pinStore.Close")
		pinStore.Close()
	}
	if derivStore != nil {
		jmlog.Info("shutdown step", "component", "derivStore.Close")
		derivStore.Close()
	}
	if store != nil {
		jmlog.Info("shutdown step", "component", "store.Close")
		store.Close()
	}
	if rdb != nil {
		jmlog.Info("shutdown step", "component", "rdb.Close")
		rdb.Close()
	}
	jmlog.Info("shutdown step", "component", "stopServerLocked-complete")

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
		mon.EnableNFSAbsentRemount(nil) // #93 path too — same rationale
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
		mon.EnableNFSAbsentRemount(nil) // #93 path too — same rationale
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

	// Bounded: an unanswered admin prompt must never hang forever (see the
	// NFSServerStart deadlock-fix note). 180s is generous for an attended
	// user; unattended it self-clears and the next app start re-prompts.
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "osascript", "-e", osaScript).CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("mount prompt timed out after 180s (unanswered)")
	}
	if err != nil {
		return fmt.Errorf("osascript: %v\n%s", err, string(out))
	}
	return nil
}

// nfsMountOpts builds the mount_nfs -o option string for our loopback
// NFS server listening on `port`.
//
// Timeout policy. Tuned for a localhost NFS server (us). The kernel
// client's timeo is in 0.1-second units. CURRENT VALUE IS timeo=400
// (40 s initial timeout, ~120 s worst-case dead-server detection) — set
// by QA-36 below. retrans=2 means at most 2 retries before returning EIO
// to the calling syscall.
//
// The historical notes below are kept for the reasoning, but they quote
// the values that were current when written (timeo=100, then 200). Read
// the format string at the bottom for what is actually passed; that
// drift cost a re-read on 2026-08-06.
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
	// [B4' Fix B] actimeo=3600 → split attribute caching: acreg stays 3600
	// (file attrs — unchanged behavior for the read/write paths), but acdir
	// drops hard. With actimeo=3600 the client cached DIRECTORY attributes
	// — and therefore its name cache, including NEGATIVE entries — for up to
	// AN HOUR: one NoEnt answered during the ~3s keyspace-push window and
	// server-created content (farm output, another machine's import, a
	// folder move) stayed invisible until a manual readdir flushed it.
	// Measured live on cellular: content never appeared (>120s, sprint B4').
	//
	// [T6v2 2026-08-21] acdirmin/max 3/15 → 1/2. The negative-entry window is
	// now the LAST-MILE bound on external-write visibility, and it is pure
	// loss: live proof — keyspace push mirrors a server-side WebDAV write in
	// ~220ms (reconcileDir upsert log), a mount created after the upsert sees
	// the file instantly, yet mounts whose kernel cached the earlier ENOENT
	// stayed blind 15-24s (negative dentries live to acdirmax). With the
	// metadata mirror serving GETATTR/LOOKUP from RAM in µs over loopback,
	// re-validating directory attrs every 1-2s costs nothing measurable and
	// bounds new-content visibility at ~(push ≤3s + acdirmax 2s) ≈ ≤5s worst
	// case, ~2s typical. JM_NFS_LEGACY_ACTIMEO=1 restores actimeo=3600;
	// JM_NFS_ACDIR="min,max" overrides the split values.
	acOpts := "acregmin=3600,acregmax=3600,acdirmin=1,acdirmax=2"
	if v := os.Getenv("JM_NFS_ACDIR"); v != "" {
		if parts := strings.SplitN(v, ",", 2); len(parts) == 2 {
			acOpts = "acregmin=3600,acregmax=3600,acdirmin=" + strings.TrimSpace(parts[0]) + ",acdirmax=" + strings.TrimSpace(parts[1])
		}
	}
	if os.Getenv("JM_NFS_LEGACY_ACTIMEO") == "1" {
		acOpts = "actimeo=3600"
	}
	// mutejukebox (L3, 2026-08-06). NFS3ERR_JUKEBOX is the documented trigger for
	// macOS declaring a volume unresponsive — `man mount_nfs`: mutejukebox
	// suppresses the "server is not responding" alert for jukebox replies. We
	// RETURN JUKEBOX BY DESIGN on several paths: the FUSE stat budget
	// (errFUSETimeout), the FUSE data ceiling shedding at its ceiling, and the
	// spool in-flight hole hold (spoolReadFile.ReadAt) all surface as a jukebox
	// retry rather than an error, because retrying is correct and failing is not.
	//
	// So every one of those deliberate holds also pops "connection interrupted"
	// at the user — the symptom chased in [[project_lookup_flicker_rootcause]]
	// and [[project_slow_copy_fsetxattr]]. The retry semantics are what we want;
	// only the alert is wrong. This mutes the alert and changes nothing else.
	return fmt.Sprintf(
		"port=%s,mountport=%s,hard,intr,timeo=400,retrans=2,mutejukebox,nolocks,locallocks,rsize=1048576,wsize=1048576,readahead=%d,%s,vers=3,tcp",
		port, port, ra, acOpts)
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

// derivStorePath puts the Tier-B derivative index (JM-14) next to the metadata
// DB as its own SQLite file, so it doesn't contend on metadata.db / pin.db WALs.
func derivStorePath(metadataDBPath string) string {
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
	return dir + "/derivatives.db"
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
	// Scanning is true when the pin's subtree enumeration was kicked off
	// asynchronously (R-2). FilesPinned/BytesTotal are 0 in that case — the
	// real counts land in /cache-status as the walk completes. The UI shows a
	// "Scanning…" row meanwhile (see pin.ScanningRoots).
	//
	// NOT omitempty: Swift's synthesized Codable uses required decode() for
	// this non-optional Bool, so a missing key throws keyNotFound and aborts
	// the whole PinResult decode (the roots:null lesson). Always emit it.
	Scanning bool `json:"scanning"`
}

// NFSServerPin pins a file or directory tree for offline availability.
// Walks the tree under fusePath equivalent (since the user passes a
// /Volumes/zpool path, we translate to the FUSE mount root) and adds
// every regular file to the pin registry. The prefetcher daemon picks
// them up and warms the cache.
//
// R-2: the subtree walk can take TENS OF SECONDS on a large reel. Doing it
// synchronously left the menu-bar UI dead (no spinner, no new root) until it
// finished — "pinning looks broken." We now mark the root as scanning, return
// immediately, and run the walk + insert on a background goroutine. The UI
// shows "Scanning <folder>…" (pin.ScanningRoots in /cache-status) the instant
// the click lands, then the real pinned-root row takes over once rows exist.
//
//export NFSServerPin
func NFSServerPin(rootPath *C.char) *C.char {
	globalMu.Lock()
	pinStore := globalPinStore
	mountPath := globalMountPath
	fusePath := globalFUSEPath
	prefetcher := globalPrefetcher
	rc := globalRC
	globalMu.Unlock()

	if pinStore == nil {
		return jsonStr(PinResult{Error: "pin store not initialized"})
	}
	root := C.GoString(rootPath)
	walkPath := translateMountToFUSE(root, mountPath, fusePath)

	// Item 0 GUARD: a pin root inside a scan-filtered internal namespace
	// (.trash/.juicemount) can never be metadata-warmed — reconcileDir #78-
	// filters it — so pinning it is nonsensical. Reject with a clear error
	// rather than warming blocks for a dir that will never list offline.
	if rel := metaRelPath(root, mountPath); metadata.ScanFilteredPath(rel) {
		jmlog.Warn("pin rejected: cannot pin internal namespace", "root", root, "rel", rel)
		return jsonStr(PinResult{Error: "cannot pin internal namespace (.trash/.juicemount)"})
	}

	// Surface the spinner before the (slow) walk starts; return immediately.
	pin.MarkScanning(root)
	go func() {
		// Detached: a panic here must never take down the process, and the
		// scanning marker must always be cleared even on an early error.
		defer func() {
			if rec := recover(); rec != nil {
				jmlog.Warn("pin walk panicked (recovered)", "root", root, "panic", fmt.Sprint(rec))
			}
			pin.ClearScanning(root)
		}()

		entries, err := pin.CountFilesUnder(walkPath)
		if err != nil {
			jmlog.Warn("pin walk failed", "root", root, "error", err.Error())
			return
		}
		var totalBytes int64
		for i := range entries {
			entries[i].Path = translateFUSEToMount(entries[i].Path, fusePath, mountPath)
			entries[i].PinRoot = root
			totalBytes += entries[i].Size
		}
		// Brief "found N files" beat before the rows exist, so a long insert
		// still reads as progress rather than a stall.
		pin.UpdateScanProgress(root, len(entries), totalBytes)
		if err := pinStore.PinMany(entries); err != nil {
			jmlog.Warn("pin insert failed", "root", root, "files", len(entries), "error", err.Error())
			return
		}
		jmlog.Info("pinned folder", "root", root, "files", len(entries), "gb", totalBytes>>30)
		// Drain now rather than on the prefetcher's next 2s tick.
		if prefetcher != nil {
			prefetcher.Wake()
		}

		// Item 0: warm the pinned SUBTREE's metadata into the mirror so the dir
		// (and its whole tree) LIST OFFLINE. PinMany above cached the file
		// BLOCKS but wrote no subtree rows; without this, an offline ListChildren
		// of the pinned dir returns empty. Reuses reconcileDir (durable SQLite
		// `entries` write). Gated on !pin.IsOffline() and JM_PIN_WARM_METADATA
		// inside the metadata calls. Best-effort; a failure leaves today's
		// behavior (blocks cached, metadata warmed by the next full SCAN).
		warmPinnedMetadata(rc, root, mountPath)
	}()

	// FilesPinned/BytesTotal are filled in via /cache-status as the walk lands.
	return jsonStr(PinResult{OK: true, Scanning: true})
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
	// Capacity is the pinned-set-vs-disk verdict (R-1). OverCapacity true means
	// the pinned set can't be kept fully resident; the UI shows a "free disk or
	// unpin" banner with ShortfallBytes.
	Capacity pin.CapacityVerdict `json:"capacity"`
	// CacheUsedBytes (A2) is the TRUE on-disk block-cache size, exposed as an
	// unambiguous top-level field so the app + OpenLoupe stop reporting the
	// pinned-only Aggregate.CachedBytes as "cache used" (the long-standing
	// metric bug: pinned files are a SUBSET of the real LRU block cache).
	// Sourced from JuiceFS's authoritative juicefs_blockcache_bytes gauge
	// (scraped from the FUSE daemon's prometheus /metrics), falling back to a
	// du of the cache dir (Capacity.CacheUsageBytes) when the scrape is
	// unavailable.
	CacheUsedBytes int64 `json:"cache_used_bytes"`
	// Scanning lists pin roots whose subtree is still being enumerated (R-2).
	// The UI renders a "Scanning <folder>…" row until the real pinned root
	// appears in Roots.
	Scanning []pin.ScanningRoot `json:"scanning"`
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
		return jsonStr(CacheStatus{OfflineMode: pin.IsOffline(), Roots: []pin.RootSummary{}, Scanning: []pin.ScanningRoot{}})
	}
	cs := CacheStatus{OfflineMode: pin.IsOffline(), Capacity: pin.Capacity(), Scanning: pin.ScanningRoots()}
	// cache_used_bytes = the TRUE on-disk JuiceFS block-cache size. PREFER the
	// authoritative juicefs_blockcache_bytes gauge scraped LAZILY (≤ once per
	// ~2s, never per-RPC — NFS hot-path discipline) from the FUSE daemon's
	// prometheus endpoint; FALL BACK to the existing cache-dir du
	// (Capacity.CacheUsageBytes) when the scrape is unavailable (addr unknown,
	// endpoint not yet up, transient miss). Nil/err-safe and non-blocking.
	cs.CacheUsedBytes = cs.Capacity.CacheUsageBytes // fallback: cache-dir du
	if bc, ok := pin.BlockCacheBytes(); ok {
		cs.CacheUsedBytes = bc // authoritative block-cache gauge
		// Clamp DOWN to the on-disk cache-dir du. After "Clear Cache" we
		// os.Remove the chunk files behind the JuiceFS daemon's back, so its
		// in-memory juicefs_blockcache_bytes gauge lingers stale-high until the
		// daemon independently rescans — while the du already reflects the
		// emptied dir. Never report more cached than is physically on disk; that
		// stale-high gauge is the "X cached doesn't reset after Clear Cache"
		// symptom. Normal operation: du ≈ gauge, so this is a no-op.
		if du := cs.Capacity.CacheUsageBytes; du >= 0 && du < cs.CacheUsedBytes {
			cs.CacheUsedBytes = du
		}
	}
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

// writeContractJSON marshals v as the JSON body of a control-plane response.
func writeContractJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// metaRelPath maps a user-facing NFS path ("/Volumes/<vol>/a/b") to the
// volume-relative key the metadata mirror + spool store use ("a/b"). The
// `entries` and `spool_entries` tables are anchored at the volume root, NOT the
// mount point. (The pin store, by contrast, is keyed by the full user-facing
// path — do NOT translate pin lookups.)
func metaRelPath(path, mountPoint string) string {
	return strings.TrimPrefix(strings.TrimPrefix(path, mountPoint), "/")
}

// warmPinnedMetadata reconciles a pinned root's ancestor chain and (if it is a
// directory) its whole subtree into the metadata mirror so the pinned dir lists
// OFFLINE (Item 0). The pin store keys on the full user-facing path (e.g.
// "/Volumes/zpool/movies/reel"); the metadata store is keyed volume-relative,
// so we translate once via metaRelPath and resolve the inode via LookupByPath.
//
// Everything here is best-effort and self-gating: the metadata calls no-op when
// JM_PIN_WARM_METADATA=0 or pin.IsOffline(), and a nil RedisClient (pin issued
// before the metadata layer wired, or a CLI build) simply skips warming. A
// failure never breaks pinning — the blocks are already cached and the next
// full SCAN eventually mirrors the subtree.
func warmPinnedMetadata(rc *metadata.RedisClient, root, mountPath string) {
	if rc == nil {
		return
	}
	if !metadata.PinWarmMetadataEnabled() {
		return
	}
	if pin.IsOffline() {
		jmlog.Info("pin warm: skipped (offline)", "root", root)
		return
	}
	rel := metaRelPath(root, mountPath)
	if metadata.ScanFilteredPath(rel) {
		// Already rejected at NFSServerPin entry; defensive double-check.
		jmlog.Warn("pin warm: refusing scan-filtered namespace", "root", root, "rel", rel)
		return
	}

	// Warm the ancestor chain first so the pinned dir is reachable from root,
	// then warm the subtree. ReconcileAncestors also reconciles root inode 1.
	if err := rc.ReconcileAncestors(rel); err != nil {
		jmlog.Warn("pin warm: ReconcileAncestors failed", "root", root, "rel", rel, "error", err.Error())
	}

	ent := rc.Store().LookupByPath(rel)
	if ent == nil {
		jmlog.Warn("pin warm: pinned path not yet in mirror after ancestor warm — subtree left to SCAN",
			"root", root, "rel", rel)
		return
	}
	if ent.IsDir {
		if err := rc.ReconcileSubtree(ent.Inode, 0); err != nil {
			jmlog.Warn("pin warm: ReconcileSubtree failed", "root", root, "inode", ent.Inode, "error", err.Error())
		}
	}
}

// warmPinnedRootsAtBoot re-warms the metadata mirror for every pin root that
// survived from a prior session (Item 0). Runs on a background goroutine at
// boot. Self-gates: no-op when warming is disabled or offline; a nil
// RedisClient (e.g. metadata layer not wired yet) skips silently. Each root is
// warmed via the same warmPinnedMetadata path used at pin time.
func warmPinnedRootsAtBoot(ps *pin.Store, rc *metadata.RedisClient, mountPath string) {
	defer func() {
		if r := recover(); r != nil {
			jmlog.Warn("pin warm (boot) panicked (recovered)", "panic", fmt.Sprint(r))
		}
	}()
	if ps == nil || rc == nil {
		return
	}
	if !metadata.PinWarmMetadataEnabled() {
		return
	}
	if pin.IsOffline() {
		jmlog.Info("pin warm (boot): skipped (offline) — resumes when online")
		return
	}
	roots, err := ps.PinRoots()
	if err != nil {
		jmlog.Warn("pin warm (boot): PinRoots failed", "error", err.Error())
		return
	}
	if len(roots) == 0 {
		return
	}
	jmlog.Info("pin warm (boot): warming pinned roots from prior session", "roots", len(roots))
	for _, r := range roots {
		if r.Root == "" {
			continue
		}
		// Re-check offline between roots: the user may toggle offline mid-pass;
		// each warmPinnedMetadata also self-gates, but bailing early avoids a
		// stack of 30s-timeout dir reconciles.
		if pin.IsOffline() {
			jmlog.Info("pin warm (boot): offline mid-pass — stopping", "remaining_hint", r.Root)
			return
		}
		warmPinnedMetadata(rc, r.Root, mountPath)
	}
	jmlog.Info("pin warm (boot): pinned-root warm pass complete", "roots", len(roots))
}

// handleWhoamiHTTP serves GET /whoami (contract JM-1): JuiceMount identity,
// public version, contract_version, and the DERIVED capability list. Reads the
// identity captured at Start. This is the GUI (cbridge) variant; jm5 has its
// own (cmd/jm5) that reports deployment:"cli" and the smaller capability set.
func handleWhoamiHTTP(w http.ResponseWriter, r *http.Request) {
	globalMu.Lock()
	mp := globalMountPath
	if mp == "" {
		mp = globalWantMountPoint
	}
	addr := globalMetricsAddr
	if globalMetrics != nil {
		addr = globalMetrics.Addr()
	}
	who := cplane.WhoAmI{
		App:             "JuiceMount",
		Version:         version.Version,
		ContractVersion: cplane.ContractVersion,
		InstanceID:      globalInstanceID,
		VolumeName:      globalVolumeName,
		MountPoint:      mp,
		NASRoot:         mp,
		ControlPlane:    "http://" + addr,
		MetadataDBPath:  globalDBPath,
		Deployment:      "gui",
		WireTerms:       cplane.WireTerms,
		Capabilities:    append([]string(nil), globalCapabilities...),
	}
	globalMu.Unlock()
	writeContractJSON(w, who)
}

// residencyResponse is the GET /residency body. Schema:
// contract/spec/schema/residency.schema.json. Pointer fields so inode is
// omitted on exists=false and upload_state serializes as null (not absent).
type residencyResponse struct {
	Path        string  `json:"path"`
	Exists      bool    `json:"exists"`
	Inode       *uint64 `json:"inode,omitempty"`
	Resident    bool    `json:"resident"`
	Pinned      bool    `json:"pinned"`
	BytesCached int64   `json:"bytes_cached"`
	Total       int64   `json:"total"`
	Streaming   bool    `json:"streaming"`
	UploadState *string `json:"upload_state"` // null when no active spool row
	CheckedAt   int64   `json:"checked_at"`
}

// handleResidencyHTTP serves GET /residency?path=<abs> (contract JM-2): the
// honest per-path residency that OpenLoupe's green "resident" badge depends on.
//
// The hard rule: per-byte cache accounting exists ONLY for files with a
// pinned_files row. resident=true is emitted only when that row shows
// bytes_cached >= size; any path with no pin row reports resident=false,
// streaming=true, bytes_cached=0 — the honest under-claim. Never invent cached
// bytes for an unpinned file.
func handleResidencyHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "missing ?path", 400)
		return
	}
	globalMu.Lock()
	store := globalStore
	pinStore := globalPinStore
	spool := globalSpool
	mp := globalMountPath
	if mp == "" {
		mp = globalWantMountPoint
	}
	globalMu.Unlock()

	// entries + spool are keyed VOLUME-RELATIVE; the pin store is keyed by the
	// full user-facing path. Translate once for the relative-keyed stores.
	rel := metaRelPath(path, mp)

	resp := residencyResponse{Path: path, CheckedAt: time.Now().Unix()}

	var entry *metadata.Entry
	if store != nil {
		entry = store.LookupByPath(rel)
	}
	if entry == nil {
		// exists=false ⇒ all flags false, bytes 0, inode omitted (schema rule).
		writeContractJSON(w, resp)
		return
	}
	resp.Exists = true
	in := entry.Inode
	resp.Inode = &in
	resp.Total = entry.Size

	if pinStore != nil {
		if pe, ok := pinStore.Get(path); ok {
			resp.Pinned = true
			resp.BytesCached = pe.BytesCached
			if pe.BytesCached >= entry.Size && entry.Size > 0 {
				resp.Resident = true
			}
		}
	}
	resp.Streaming = resp.Exists && !resp.Resident

	// upload_state mirrors an ACTIVE spool row's drain_state, else null. The
	// spool store only retains writing/ready/draining rows, so a fully-drained
	// file reports null (not "done").
	if spool != nil {
		if row, err := spool.Meta().LookupByPath(rel); err == nil && row != nil {
			s := string(row.DrainState)
			resp.UploadState = &s
		}
	}
	writeContractJSON(w, resp)
}

// lookupResponse is the GET /lookup body. Schema:
// contract/spec/schema/lookup.schema.json. Pointer fields so the per-field
// set collapses to {path, exists:false} when the path is unknown, and is_dir
// serializes even when false.
type lookupResponse struct {
	Path       string  `json:"path"`
	Exists     bool    `json:"exists"`
	Inode      *uint64 `json:"inode,omitempty"`
	NASRelPath *string `json:"nas_rel_path,omitempty"`
	IsDir      *bool   `json:"is_dir,omitempty"`
	Size       *int64  `json:"size,omitempty"`
	Mtime      *int64  `json:"mtime,omitempty"`
}

// handleLookupHTTP serves GET /lookup?path=<abs> (contract JM-4): durable
// identity (inode + nas_rel_path) for a path so OpenLoupe never opens
// JuiceMount's SQLite. nas_rel_path is computed as path minus the mount point.
func handleLookupHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "missing ?path", 400)
		return
	}
	globalMu.Lock()
	store := globalStore
	mp := globalMountPath
	if mp == "" {
		mp = globalWantMountPoint
	}
	globalMu.Unlock()

	// entries are keyed VOLUME-RELATIVE; translate before lookup. nas_rel_path
	// is that same relative key.
	rel := metaRelPath(path, mp)
	resp := lookupResponse{Path: path}
	var entry *metadata.Entry
	if store != nil {
		entry = store.LookupByPath(rel)
	}
	if entry == nil {
		writeContractJSON(w, resp) // {path, exists:false}
		return
	}
	resp.Exists = true
	in := entry.Inode
	resp.Inode = &in
	resp.NASRelPath = &rel
	isDir := entry.IsDir
	resp.IsDir = &isDir
	size := entry.Size
	resp.Size = &size
	mtime := entry.Mtime.Unix()
	resp.Mtime = &mtime
	writeContractJSON(w, resp)
}

// duResponse is the GET /du body (INSTANT-NAV #2): instant recursive folder
// totals from the metadata mirror's incrementally-maintained subtree
// aggregates — no du-walk, no FUSE, no backend round-trip.
type duResponse struct {
	Path  string `json:"path"`
	Bytes int64  `json:"bytes"`
	Files int64  `json:"files"`
	Human string `json:"human"`
}

// duHuman renders a byte count the way the app's UI does elsewhere (binary
// units, one decimal). Kept tiny and local — presentation only.
func duHuman(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// handleDuHTTP serves GET /du?path=<user-path>: O(1) "total bytes + file count
// under this folder" from the mirror's subtree aggregates (metadata.Store.
// SubtreeSize, JM_SUBTREE_SIZES default-on). Path convention matches /lookup:
// entries are keyed VOLUME-RELATIVE, so translate via metaRelPath; the mount
// root maps to "." (root children key under "." in the mirror). A file path
// answers with its own size (files=1), du-style. 503 when the store isn't up
// or the gate is off; 404 when the path isn't in the mirror.
func handleDuHTTP(w http.ResponseWriter, r *http.Request) {
	userPath := r.URL.Query().Get("path")
	if userPath == "" {
		http.Error(w, "missing ?path", 400)
		return
	}
	globalMu.Lock()
	store := globalStore
	mp := globalMountPath
	if mp == "" {
		mp = globalWantMountPoint
	}
	globalMu.Unlock()
	if store == nil {
		http.Error(w, "metadata store not initialized", 503)
		return
	}

	rel := metaRelPath(userPath, mp)
	if rel == "" {
		rel = "." // volume root
	}
	bytes, files, ok := store.SubtreeSize(rel)
	if !ok {
		http.Error(w, "subtree sizes disabled (JM_SUBTREE_SIZES=0)", 503)
		return
	}
	if rel != "." {
		entry := store.LookupByPath(rel)
		if entry == nil {
			http.Error(w, "path not in metadata mirror", 404)
			return
		}
		if !entry.IsDir {
			bytes, files = entry.Size, 1 // du on a file: its own size
		}
	}
	writeContractJSON(w, duResponse{Path: userPath, Bytes: bytes, Files: files, Human: duHuman(bytes)})
}

// derivativesResponse is the GET /derivatives body. Schema:
// contract/spec/schema/derivatives.schema.json. `derivatives` is always a
// non-nil slice (serializes as [] not null — the schema requires an array, and
// exists=false demands maxItems:0). source_hash is null until the farm computes
// it.
type derivativesResponse struct {
	Inode       uint64                 `json:"inode"`
	Exists      bool                   `json:"exists"`
	SourceHash  *string                `json:"source_hash"`
	Derivatives []derivatives.DerivRow `json:"derivatives"`
}

// parseInodeQuery reads the required ?inode=N (unsigned). Returns false (and
// writes a 400) when absent or unparseable.
func parseInodeQuery(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	raw := r.URL.Query().Get("inode")
	if raw == "" {
		http.Error(w, "missing ?inode", 400)
		return 0, false
	}
	inode, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		http.Error(w, "invalid ?inode", 400)
		return 0, false
	}
	return inode, true
}

// handleDerivativesHTTP serves GET /derivatives?inode=N (contract JM-14): the
// derivative MANIFEST for one asset — which machine-derived artifacts exist,
// their status/producer/version/integrity-hash, and (for blob kinds) the
// volume-relative blob path. Read-only + fail-closed: an unknown inode (or a
// torn-down store) reports exists:false with an empty manifest and null hash,
// never a guess. Keyed by the durable JuiceFS inode (from /lookup).
func handleDerivativesHTTP(w http.ResponseWriter, r *http.Request) {
	inode, ok := parseInodeQuery(w, r)
	if !ok {
		return
	}
	globalMu.Lock()
	ds := globalDerivStore
	globalMu.Unlock()

	resp := derivativesResponse{Inode: inode, Derivatives: []derivatives.DerivRow{}}
	if ds == nil {
		// No index wired (pre-Start / post-Stop / open failed) ⇒ fail closed.
		writeContractJSON(w, resp)
		return
	}
	known, srcHash := ds.Known(inode)
	if !known {
		// On-miss lazy reconcile (the JM-15 auto-reconcile bridge): the farm may
		// have written a sidecar on the volume this app hasn't ingested yet (there's
		// no full periodic derivatives reconcile in-app). Try ingesting JUST this
		// inode's sidecar on the fly so navigating to a farm-derived asset surfaces
		// it without a manual `jmfarm -reconcile`. Best-effort + bounded (one inode,
		// only on a miss; ingested rows are cached, so the next query hits the DB).
		//
		// READ VIA FUSE, NOT VIA THE NFS MOUNT. The sidecars live under
		// .juicemount/derivatives, an INTERNAL namespace that is scan-filtered
		// out of the metadata mirror — and the NFS server serves from that
		// mirror. So over /Volumes/zpool the directory is empty while the same
		// path under the FUSE mount has every manifest. Measured 2026-08-17
		// after backfilling 26,447 sidecars: 0 derivative dirs visible via NFS,
		// 37,255 via FUSE.
		//
		// This handler was the ONLY one of the three ReconcileOneSidecar call
		// sites reading globalMountPath; bridge/thumbs.go and the manifest
		// handler below already prefer globalFUSEPath. The failure was silent
		// in the worst way: a sidecar that cannot be seen returns
		// (found=false, err=nil), so there was no error to log and no warning
		// to find — /derivatives simply answered exists:false forever and the
		// consumer concluded the farm had produced nothing.
		globalMu.Lock()
		mount := globalFUSEPath
		if mount == "" {
			mount = globalMountPath
		}
		globalMu.Unlock()
		if mount != "" {
			if found, ferr := farm.ReconcileOneSidecar(ds, mount, inode); ferr != nil {
				jmlog.Warn("derivatives on-miss reconcile failed", "inode", inode, "error", ferr.Error())
			} else if found {
				known, srcHash = ds.Known(inode)
			}
		}
	}
	if !known {
		// exists=false ⇒ empty manifest, source_hash:null (schema rule).
		writeContractJSON(w, resp)
		return
	}
	resp.Exists = true
	resp.SourceHash = srcHash
	rows, err := ds.Manifest(inode)
	if err != nil {
		jmlog.Warn("derivatives manifest query failed", "inode", inode, "error", err.Error())
		// Known asset but the manifest read errored — return the honest
		// exists:true with an empty list rather than a 500; the client
		// fail-closes (regenerates) exactly as it would for status:absent.
		writeContractJSON(w, resp)
		return
	}
	if rows != nil {
		resp.Derivatives = rows
	}
	writeContractJSON(w, resp)
}

// derivativesBatchMaxInodes caps one POST /derivatives/batch request. The
// consumer reconciles tens of thousands of assets; 1000 keeps a single request
// bounded (one indexed point query per inode against a local SQLite DB, no
// filesystem work at all) while cutting the round trips by three orders of
// magnitude.
const derivativesBatchMaxInodes = 1000

// derivativesBatchRequest is the POST /derivatives/batch body (contract JM-22).
type derivativesBatchRequest struct {
	Inodes []uint64 `json:"inodes"`
}

// derivativesBatchEntry is one value in the batch response map, keyed by the
// decimal inode. ready_kinds is ALWAYS a non-nil slice (serializes as [] not
// null) so a consumer can range over it without a nil check; source_hash is
// null for an unknown asset and for a known one whose hash the farm has not
// computed yet.
//
// ready_kinds answers "do we have a READY ROW", NOT "are the bytes on the
// volume" — measured 2026-08-18, 214 of 400 sampled ready rows had no blob.
// The consumer still fails open on X-JM-Blob-Miss: blob-absent at fetch time.
// Contract (AO) Q3 option (a), chosen as the default; the ?verify=1 stat-each-
// blob variant (option (b)) is NOT implemented.
type derivativesBatchEntry struct {
	ReadyKinds []string `json:"ready_kinds"`
	SourceHash *string  `json:"source_hash"`
}

// handleDerivativesBatchHTTP serves POST /derivatives/batch (contract JM-22):
// the bulk form of GET /derivatives?inode=N, for a consumer reconciling
// thousands of inodes at once ("which of these does the farm already have?").
//
//	POST {"inodes":[48899,48908]}
//	200  {"48899":{"ready_kinds":["poster","proxy"],"source_hash":"7f3197..."},
//	      "48908":{"ready_kinds":[],"source_hash":null}}
//
// An inode the index has never heard of is reported as ready_kinds:[] — absence
// is an answer, not an error, so one unknown asset never fails the whole batch.
//
// THIS ROUTE DOES NO ON-MISS RECONCILE. DO NOT ADD ONE. The single-inode route
// deliberately runs farm.ReconcileOneSidecar on a miss (an on-the-fly read of
// <mount>/.juicemount/derivatives/<inode>/manifest.json through FUSE) so that
// navigating to a farm-derived asset surfaces it without a manual sweep. That
// is right for ONE interactive lookup and catastrophic 1000 times: it would
// turn a single indexed query into 1000 filesystem reads through the mount,
// which is the exact cost this route exists to remove. A consumer that needs
// the reconciling behaviour for a specific inode uses GET /derivatives?inode=N
// for that one. Agreed with the consumer in PROVIDER_STATUS (AO) Q3.
func handleDerivativesBatchHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req derivativesBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json body", 400)
		return
	}
	// REJECT, never truncate. A silently dropped tail comes back as an ABSENT
	// key, which a reconciling consumer reads as "the farm has nothing for
	// these" — it would then regenerate thousands of derivatives that already
	// exist, and nothing anywhere would report an error.
	if len(req.Inodes) > derivativesBatchMaxInodes {
		http.Error(w, fmt.Sprintf("too many inodes: %d (max %d per request) — page the "+
			"reconcile. Truncating silently would answer the dropped tail as absent, "+
			"which is indistinguishable from \"no derivatives exist\".",
			len(req.Inodes), derivativesBatchMaxInodes), 400)
		return
	}

	globalMu.Lock()
	ds := globalDerivStore
	globalMu.Unlock()

	out := make(map[string]derivativesBatchEntry, len(req.Inodes))
	for _, inode := range req.Inodes {
		key := strconv.FormatUint(inode, 10)
		if _, dup := out[key]; dup {
			continue // a repeated inode costs one query, not two
		}
		entry := derivativesBatchEntry{ReadyKinds: []string{}}
		// ds == nil is pre-Start / post-Stop / open-failed ⇒ fail closed with
		// the same empty shape an unknown inode gets, never a guess or a 500.
		if ds != nil {
			if known, srcHash := ds.Known(inode); known {
				entry.SourceHash = srcHash
				rows, err := ds.Manifest(inode)
				if err != nil {
					jmlog.Warn("derivatives batch manifest query failed",
						"inode", inode, "error", err.Error())
				}
				for _, row := range rows {
					if row.Status == "ready" {
						entry.ReadyKinds = append(entry.ReadyKinds, row.Kind)
					}
				}
			}
		}
		out[key] = entry
	}
	writeContractJSON(w, out)
}

// handleDerivativesChangesHTTP serves GET /derivatives/changes?since=<unix> — the
// poll-based delta feed: a JSON array [{inode,kind,status,hash,updated_at}] of
// derivative rows with updated_at > since, ascending. OpenLoupe polls this on its
// refresh cadence (NOT SSE — bounded probes), folds it into the badge cache, and
// swaps a farm derivative onto an on-screen asset when it appears. A missing or
// unparseable `since` returns full history (cold-start has no cursor).
func handleDerivativesChangesHTTP(w http.ResponseWriter, r *http.Request) {
	var since int64
	if v := r.URL.Query().Get("since"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			since = n
		}
	}
	var limit int
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	globalMu.Lock()
	ds := globalDerivStore
	globalMu.Unlock()
	out := []derivatives.ChangeRow{}
	if ds != nil {
		if rows, err := ds.ListChangedSince(since, limit); err != nil {
			jmlog.Warn("derivatives changes query failed", "since", since, "error", err.Error())
		} else if rows != nil {
			out = rows
		}
	}
	writeContractJSON(w, out)
}

// metadataResponse is the GET /metadata body. Schema:
// contract/spec/schema/metadata.schema.json. On exists=false only
// {inode,exists,kind,tech:null} is emitted (producer/version/hash omitted);
// `tech` is a verbatim passthrough of the stored ffprobe payload (additive).
type metadataResponse struct {
	Inode    uint64          `json:"inode"`
	Exists   bool            `json:"exists"`
	Kind     string          `json:"kind"`
	Producer string          `json:"producer,omitempty"`
	Version  int             `json:"version,omitempty"`
	Hash     *string         `json:"hash,omitempty"`
	Tech     json.RawMessage `json:"tech"` // nil ⇒ marshals as null
}

// handleMetadataHTTP serves GET /metadata?inode=N&kind=tech (contract JM-14):
// the structured machine-derived metadata for one asset. MVP kind = "tech"
// (ffprobe container/video/audio/timecode/color). Read-only + fail-closed: a
// kind with no produced row reports exists:false, tech:null. exists==true means
// the source is present AND this kind has been produced.
func handleMetadataHTTP(w http.ResponseWriter, r *http.Request) {
	inode, ok := parseInodeQuery(w, r)
	if !ok {
		return
	}
	kind := r.URL.Query().Get("kind")
	if kind == "" {
		kind = "tech" // MVP default; the only kind served today.
	}
	globalMu.Lock()
	ds := globalDerivStore
	globalMu.Unlock()

	resp := metadataResponse{Inode: inode, Kind: kind}
	if ds == nil {
		writeContractJSON(w, resp) // fail closed ⇒ exists:false, tech:null
		return
	}
	tm, err := ds.Metadata(inode, kind)
	if err != nil {
		jmlog.Warn("metadata query failed", "inode", inode, "kind", kind, "error", err.Error())
		writeContractJSON(w, resp)
		return
	}
	if tm == nil {
		// Not produced for this kind ⇒ exists:false, tech:null.
		writeContractJSON(w, resp)
		return
	}
	resp.Exists = true
	resp.Producer = tm.Producer
	resp.Version = tm.Version
	resp.Hash = tm.Hash
	resp.Tech = tm.Payload
	// OUT-8 log_profile LWW-over-detection: a human log_profile assertion
	// (namespace=log_profile) OVERRIDES the ffprobe-DETECTED tech.video.log_format
	// — the user's deliberate "this is clog3" (or force-negative "none") wins over
	// a best-effort tag scrape. Privacy opt-in gate: OFF by default (per-author
	// human assertions don't leak into the shared /metadata view) — enabled with
	// JM_LOG_PROFILE_OVERLAY=1. Read-only overlay; the stored detection is
	// untouched, and a retract (value:null) simply leaves the detected value.
	if kind == "tech" && os.Getenv("JM_LOG_PROFILE_OVERLAY") == "1" {
		resp.Tech = overlayLogProfile(ds, inode, resp.Tech)
	}
	writeContractJSON(w, resp)
}

// overlayLogProfile applies the OUT-8 log_profile LWW-over-detection overlay: if
// an accepted (LWW-resolved, non-retracted) log_profile assertion exists for this
// inode's asset, it replaces tech.video.log_format with the asserted value. A
// retract (value:null) or absent assertion leaves the detected value verbatim. On
// any error the original payload is returned unchanged (fail-open to detection).
func overlayLogProfile(ds *derivatives.Store, inode uint64, tech json.RawMessage) json.RawMessage {
	if len(tech) == 0 {
		return tech
	}
	assetKey, err := ds.AssetKeyForInode(inode)
	if err != nil || assetKey == "" {
		return tech
	}
	raws, err := ds.AssertionsByAssetKey(assetKey)
	if err != nil {
		return tech
	}
	var asserted *string
	for _, a := range raws {
		if a.Namespace != "log_profile" || a.Key != "value" {
			continue
		}
		// value_json is raw JSON: "null" = retract (no override); a string = the pick.
		if a.ValueJSON == "null" {
			return tech // retracted → defer to detection
		}
		var s string
		if json.Unmarshal([]byte(a.ValueJSON), &s) == nil {
			asserted = &s
		}
		break
	}
	if asserted == nil {
		return tech
	}
	// Decode → set tech.video.log_format → re-encode. Tolerant of a null/absent
	// video object (no overlay target then).
	var m map[string]any
	if json.Unmarshal(tech, &m) != nil {
		return tech
	}
	video, ok := m["video"].(map[string]any)
	if !ok {
		return tech
	}
	video["log_format"] = *asserted
	out, err := json.Marshal(m)
	if err != nil {
		return tech
	}
	return out
}

// contributableKind describes one derivative kind a CONSUMER may register.
//
// REGISTER-ROUTE (2026-08-04, founder-prioritized). Before this, the route was
// pinned to kind=="ai", so Tier 1 contribute-back had no commit path for ANY
// other kind — a consumer could write proxy.mp4 through the mount and then had
// no way to make it real, and spec/WRITE_PLACEMENT.md §4 forbids deleting the
// orphan it just made. This table is the widening, and it is deliberately a
// TABLE rather than a permissive check:
//
//   - `file` pins the reserved filename from WRITE_PLACEMENT §2. A consumer
//     cannot choose the path, so it cannot write outside the per-key directory
//     or shadow another kind's artifact.
//   - `mediaType` is assigned SERVER-SIDE from the kind, never echoed from the
//     request. /blob pins its response Content-Type from this row field, so a
//     contributor-supplied media_type would be a contributor-controlled
//     Content-Type served from the control-plane origin.
//
// `filmstrip` is deliberately ABSENT: its reader contract (minimum canvas,
// zero-gutter cell origin, upright-as-displayed, undefined trailing cells) is
// still unspecified, so an edge-written strip is not yet safely renderable by
// another client. See PROVIDER_STATUS 2026-08-04 D.
type contributableKind struct {
	file      string
	mediaType string
}

var contributableKinds = map[string]contributableKind{
	"ai":    {file: "", mediaType: "application/json"}, // "" => resolved dual-name (logger/loupe)
	"proxy": {file: "proxy.mp4", mediaType: "video/mp4"},
	// NOTE the kind is "thumbnail", not "poster" — `poster.jpg` is the FILE.
	// Using the filename as the kind was caught by TestContributableKindsAreValidManifestKinds:
	// the manifest enum has no "poster", so the row would have failed the
	// consumer's own validation of /derivatives.
	"thumbnail": {file: "poster.jpg", mediaType: "image/jpeg"},
	"waveform":  {file: "waveform.json", mediaType: "application/json"},
	// audio_proxy (A8): a small streamable stereo MP4 for audio-only assets, so
	// a remote resolver can prefer it over a 2 GB original. Distinct from proxy
	// because the PK is (inode, kind) and an asset may carry both.
	"audio_proxy": {file: "audio_proxy.mp4", mediaType: "audio/mp4"},
	// filmstrip: OPENED 2026-08-05. It was withheld because its reader contract
	// was unspecified — an edge-written strip was not safely renderable by
	// another client. That was a real gap and the answer is to SPECIFY it, not
	// to keep the kind shut: see filmstripGeometryContract below, now enforced
	// here and written into derivatives.schema.json.
	"filmstrip": {file: "strip.jpg", mediaType: "image/jpeg"},
}

// filmstripGeometryContract is what a contributed strip must satisfy for any
// other client to render it. Enforced at register, so a strip that reaches the
// index is renderable by definition rather than by convention.
//
//	LAYOUT     row-major, origin TOP-LEFT, ZERO gutter. Cell (r,c) occupies
//	           x=[c*cell_w,(c+1)*cell_w), y=[r*cell_h,(r+1)*cell_h).
//	CANVAS     the image is EXACTLY cols*cell_w by rows*cell_h. No padding, no
//	           border — a reader computes a cell rect by multiplication alone.
//	ORIENTATION frames are stored UPRIGHT AS DISPLAYED. Rotation metadata on the
//	           source has already been applied; a reader must not re-apply it.
//	FRAME i    is at row i/cols, col i%cols, for i in [0, frame_count).
//	TRAILING   cols*rows MAY exceed frame_count. Cells at index >= frame_count
//	           are UNDEFINED and must not be rendered — this is the one that
//	           silently produces a duplicated or black last frame if ignored.
//	TIME       frame i covers [i*interval_ms, (i+1)*interval_ms) of a
//	           duration_ms source. interval_ms > 0.
//
// registerConflict is the JSON body for a 409 from /derivatives/register.
//
// WHY IT EXISTS: the route returns 409 for six distinct conditions whose
// remedies CONTRADICT each other — "blob not yet visible" means keep retrying
// with this inode, "synthetic inode" means STOP using this inode, "source
// stale" means recompute the derivative, "farm row wins" is permanent. Every
// one was a bare prose string, so a consumer could only apply one uniform
// retry, which silently masks the stale-source case (the safety-relevant one)
// and burns its whole budget on the permanent ones.
//
// Additive and backward-compatible: the status code is unchanged and a consumer
// that only reads the code and logs the body is unaffected.
type registerConflict struct {
	Code      string `json:"code"`
	Retryable bool   `json:"retryable"`
	Message   string `json:"message"`
	Inode     uint64 `json:"inode,omitempty"`
	// RealInode is the resolved backend inode when the server could determine
	// it, so the consumer can place the blob correctly on the next attempt
	// rather than re-registering into an empty derivative directory.
	RealInode uint64 `json:"real_inode"`
	Path      string `json:"path,omitempty"`
}

// statErrRetryable partitions a source-stat failure into transient vs permanent.
//
// Calling EVERY stat failure permanent discards contributions that would have
// succeeded moments later. entry.Path comes from the RAM mirror with no liveness
// check, so an out-of-band rename (another client, the web UI, the farm) leaves
// it stale until reconcile lands — that ENOENT clears itself. EIO/ENOTCONN/ENXIO
// are transient mount trouble of the same character. A permission or shape error
// is what genuinely cannot improve on retry.
func statErrRetryable(err error) bool {
	switch {
	case errors.Is(err, os.ErrPermission), errors.Is(err, syscall.ENOTDIR),
		errors.Is(err, syscall.ENAMETOOLONG), errors.Is(err, syscall.ELOOP):
		return false
	default:
		return true
	}
}

func writeRegisterConflict(w http.ResponseWriter, c registerConflict) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusConflict)
	_ = json.NewEncoder(w).Encode(c)
}

func validateFilmstripGeometry(g *derivatives.FilmstripGeo) error {
	if g == nil {
		return fmt.Errorf(`kind "filmstrip" requires a "filmstrip" geometry object — ` +
			`without it a strip is an image no other client can index into`)
	}
	switch {
	case g.Cols < 1 || g.Rows < 1:
		return fmt.Errorf("filmstrip cols/rows must both be >= 1 (got %dx%d) — cols==0 is a "+
			"divide-by-zero in any reader's i%%cols", g.Cols, g.Rows)
	case g.CellW < 1 || g.CellH < 1:
		return fmt.Errorf("filmstrip cell_w/cell_h must both be >= 1 (got %dx%d)", g.CellW, g.CellH)
	case g.FrameCount < 1:
		return fmt.Errorf("filmstrip frame_count must be >= 1 (got %d)", g.FrameCount)
	case g.FrameCount > g.Cols*g.Rows:
		return fmt.Errorf("filmstrip frame_count %d exceeds the %dx%d grid (%d cells) — "+
			"the frames do not fit the canvas the geometry describes",
			g.FrameCount, g.Cols, g.Rows, g.Cols*g.Rows)
	case g.Cols > maxStripGridDim || g.Rows > maxStripGridDim:
		return fmt.Errorf("filmstrip grid %dx%d exceeds %d", g.Cols, g.Rows, maxStripGridDim)
	case g.CellW > maxStripCellDim || g.CellH > maxStripCellDim:
		return fmt.Errorf("filmstrip cell %dx%d exceeds %d", g.CellW, g.CellH, maxStripCellDim)
	case g.IntervalMS < 1:
		return fmt.Errorf("filmstrip interval_ms must be >= 1 (got %d) — a reader maps time to "+
			"a cell by dividing by it", g.IntervalMS)
	case g.DurationMS < 0:
		return fmt.Errorf("filmstrip duration_ms must be >= 0 (got %d)", g.DurationMS)
	}
	return nil
}

const (
	maxStripGridDim = 4096
	maxStripCellDim = 8192
)

// registerRequest is the POST /derivatives/register body (OL-1 on-device AI
// contribute-back, widened by REGISTER-ROUTE). The consumer wrote the blob
// through the mount, then vouches with source_size+source_mtime (it cannot
// compute the contract xxh3).
type registerRequest struct {
	Inode       uint64 `json:"inode"`
	Kind        string `json:"kind"`
	Producer    string `json:"producer"`
	Model       string `json:"model"`
	Dim         int    `json:"dim"`
	SourceSize  int64  `json:"source_size"`
	SourceMtime int64  `json:"source_mtime"`
	BlobRelPath string `json:"blob_rel_path"`
	// BlobSize is OPTIONAL and closes the truncated-blob hole (contract (AO)
	// Q1.2). register never reads the consumer's blob bytes — deliberately, on
	// the reasoning that a phantom row degrades gracefully because a failed blob
	// read makes the reader regenerate. That reasoning does not cover a blob
	// truncated at a plausible NON-ZERO length: it passes the exists/regular/
	// non-empty checks and mints a `ready` row over partial data, and nothing
	// downstream detects it. We stat the blob anyway, so comparing the size the
	// consumer vouched is free. A pointer, not an int64: absent must behave
	// EXACTLY as it did before (no check), and 0 is a value a caller could
	// legitimately send by accident — it must not read as "not supplied".
	BlobSize *int64 `json:"blob_size,omitempty"`
	// Codec is REQUIRED for a proxy kind and DECLARATIVE, not gated on a floor
	// (founder decision 2026-08-04). derivatives.schema.json reads an ABSENT
	// codec as h264, so silence would mislabel a richer codec rather than
	// describe it — which is why it stays required even though no value is
	// rejected on quality grounds.
	Codec       string `json:"codec,omitempty"`
	CodecString string `json:"codec_string,omitempty"`
	// Width/Height/BitrateBPS let the server rank a contributed artifact WITHOUT
	// decoding it, so a cheap one can be earmarked for upgrade. A 320px and a
	// 720px poster are otherwise distinguishable on the wire only by blob_size,
	// which is a bad quality proxy (a flat LOG-graded 720 frame can encode
	// smaller than a busy 320).
	Width      int   `json:"width,omitempty"`
	Height     int   `json:"height,omitempty"`
	BitrateBPS int64 `json:"bitrate_bps,omitempty"`
	// Filmstrip is REQUIRED for kind=="filmstrip". A sprite sheet without its
	// geometry is an image no other client can index into — which is exactly why
	// the kind was withheld until the contract below was written down.
	Filmstrip *derivatives.FilmstripGeo `json:"filmstrip,omitempty"`
}

// Bounds for artifact descriptors. Generous enough for 8K-plus, tight enough
// that a forged value cannot walk a reader into an absurd allocation.
const (
	maxArtifactDim     = 65535
	maxArtifactBitrate = int64(10) << 30 // 10 Gbps
)

// knownProxyCodecs mirrors derivatives.schema.json's codec enum. Asserted
// against the vendored contract by a conformance test — a vocabulary narrower
// than the contract rejects legitimate contributions, and one wider writes rows
// that fail the consumer's own validation.
var knownProxyCodecs = map[string]bool{
	"h264": true, "hevc": true, "av1": true, "aac": true, "opus": true,
}

func knownProxyCodecList() []string {
	out := make([]string, 0, len(knownProxyCodecs))
	for c := range knownProxyCodecs {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// registerResponse is the 200 body. Schema: contract/spec/schema/register.schema.json.
type registerResponse struct {
	Inode      uint64               `json:"inode"`
	Registered bool                 `json:"registered"`
	Derivative derivatives.DerivRow `json:"derivative"`
}

// handleDerivativesRegisterHTTP serves POST /derivatives/register (OL-1): the
// on-device AI contribute-back. A consumer (OpenLoupe) that computed expensive,
// identity-bearing AI on-device — and already wrote the ai.loupe.json blob
// through the mount — registers it so JuiceMount indexes it for every other
// client + the web UI.
//
// AI-only. Trust model: the consumer can't produce the contract's xxh3 (it uses
// xxh64), so the SERVER is the source-hash authority — the consumer vouches with
// source_size+source_mtime (the live stat it computed the AI against); we re-stat
// the live source and accept ONLY on a match (else 409 — the AI was computed
// against stale bytes), then stamp the canonical sampled xxh3 ourselves. The blob
// itself the consumer wrote; we only index the manifest row.
func handleDerivativesRegisterHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json body", 400)
		return
	}
	if req.Inode == 0 {
		http.Error(w, "missing inode", 400)
		return
	}
	spec, kindOK := contributableKinds[req.Kind]
	if !kindOK {
		allowed := make([]string, 0, len(contributableKinds))
		for k := range contributableKinds {
			allowed = append(allowed, k)
		}
		sort.Strings(allowed)
		http.Error(w, fmt.Sprintf("kind %q is not consumer-registrable; allowed: %s",
			req.Kind, strings.Join(allowed, ", ")), 400)
		return
	}
	if req.Producer != "on-device" && req.Producer != "macos-node" {
		http.Error(w, "producer must be on-device or macos-node", 400)
		return
	}
	rel := req.BlobRelPath

	globalMu.Lock()
	store := globalStore
	ds := globalDerivStore
	fusePath := globalFUSEPath
	globalMu.Unlock()

	// Wire-term cutover (2026-08-03): when the consumer omits blob_rel_path we
	// resolve it against what is actually on disk — the post-cutover
	// `ai.logger.json` if present, else the legacy `ai.loupe.json`. Defaulting
	// to a fixed name would 404 a blob an older consumer had already written
	// under the other one. An explicit blob_rel_path is honoured as-is; the
	// register schema accepts both names.
	if spec.file != "" {
		// The reserved name is authoritative. A supplied blob_rel_path is only
		// accepted when it MATCHES it — the consumer does not get to choose a
		// path inside our namespace (WRITE_PLACEMENT §2).
		if rel != "" && rel != spec.file {
			http.Error(w, fmt.Sprintf("blob_rel_path %q is not the reserved name for kind %q (expected %q)",
				rel, req.Kind, spec.file), 400)
			return
		}
		rel = spec.file
	} else if rel == "" {
		// kind=="ai" keeps its dual-name resolution (wire-term cutover): the
		// post-cutover `ai.logger.json` if present, else the legacy name.
		// Resolved through a DESCRIPTOR, not a joined path: the path form stat'ed
		// through a consumer-swappable <inode> component, so an ancestor symlink
		// steered the very choice of filename from outside the volume.
		if dir, derr := derivatives.OpenDirUnder(fusePath, derivatives.DerivDirRel(req.Inode)); derr == nil {
			rel = derivatives.AIBlobWriteNameAt(dir)
			dir.Close()
		} else {
			rel = derivatives.AIBlobName
		}
	}

	if store == nil || ds == nil {
		http.Error(w, "control plane not ready", 503)
		return
	}
	// SYNTHETIC INODES ARE REFUSED BEFORE THE LOOKUP, NOT AFTER IT.
	//
	// This check MUST precede LookupByInode. A synthetic inode is not merely an
	// inode that fails to resolve — juiceFS.Create mints one via
	// nextSyntheticInode() and then InsertToCache's it (nfs/handler.go:3247),
	// so it is a LIVE CACHE KEY and the lookup SUCCEEDS. Placing the guard in
	// the `entry == nil` branch therefore left the actual failure wide open: the
	// register returned 200 and committed a manifest row keyed to an identifier
	// that ceases to exist once Redis reconcile assigns the real inode. The blob
	// then lives forever at .juicemount/derivatives/<vanished>/ with nothing
	// pointing at it, and no GC exists for derivative rows. Silent data loss,
	// with a success code — caught by adversarial review, which reproduced it by
	// seeding through the production InsertToCache path.
	//
	// Exposure is widest for files created OFFLINE, which hold a synthetic inode
	// indefinitely rather than for the drain window.
	if req.Inode&(1<<63) != 0 {
		// Resolve to the real inode when we can, so the consumer gets an
		// actionable answer in ONE round trip. It cannot be resolved from the
		// inode alone: the derivative BLOB is addressed by inode too
		// (DerivBlobRel), so a consumer told merely to "retry with the right
		// inode" would re-register against an empty derivative dir and get a
		// second, indistinguishable 409. Handing back real_inode lets it place
		// the blob correctly the first time.
		srcPath, _ := store.SyntheticHandlePath(req.Inode)
		if srcPath == "" {
			if e := store.LookupByInode(req.Inode); e != nil {
				srcPath = e.Path
			}
		}
		var realInode uint64
		if srcPath != "" {
			if e := store.LookupByPath(srcPath); e != nil && e.Inode&(1<<63) == 0 {
				realInode = e.Inode
			}
		}
		writeRegisterConflict(w, registerConflict{
			Code:      "synthetic_inode",
			Retryable: false,
			Inode:     req.Inode,
			RealInode: realInode,
			Path:      srcPath,
			Message: "inode is a transient pre-drain identifier, not a backend inode " +
				"(high bit set). Registering under it would key the derivative to an " +
				"identifier that ceases to exist. If real_inode is set, rewrite the blob " +
				"under that inode's derivative directory and register with it. If it is " +
				"absent the source has not reconciled yet: re-list the parent directory " +
				"to force a fresh READDIR — a cached stat can hold a stale identifier " +
				"indefinitely — then take the inode again.",
		})
		return
	}

	entry := store.LookupByInode(req.Inode)
	if entry == nil {
		http.Error(w, "inode not found", 404)
		return
	}

	// The manifest row is the commit point (WRITE_PLACEMENT §4), so refuse to
	// mint a row for a blob that is not on disk — that would be a phantom row
	// pointing at nothing, and readers fail closed on it rather than
	// regenerating. Cheap: one stat, and it catches a consumer that registered
	// before its own atomic rename landed.
	//
	// ORDER MATTERS: this runs AFTER the inode lookup. Placed before it, an
	// unknown inode reported 409 "blob missing" instead of 404 "inode not
	// found" — a real regression caught by TestDerivativesRegisterOL1.
	// ANCHORED. Lstat on a joined path guards only the LEAF, so an ancestor
	// symlink let an out-of-tree file satisfy this "write it BEFORE registering"
	// gate — minting a `ready` row that the (correctly anchored) serve path then
	// permanently 404s, with the slot occupied. Same walk the serve path uses,
	// so the two cannot disagree about what exists.
	bfi, statErr := derivatives.StatRegularUnder(fusePath, derivatives.DerivBlobRel(req.Inode, rel))
	if statErr != nil {
		// A non-regular file at a reserved blob name is PERMANENT — retrying cannot
		// change it; the consumer must remove it and rewrite. Reporting it as
		// "still draining" made the consumer retry against its own symlink until
		// the budget ran out.
		if errors.Is(statErr, derivatives.ErrNotRegular) {
			writeRegisterConflict(w, registerConflict{
				Code: "blob_not_regular", Retryable: false, Inode: req.Inode,
				Message: fmt.Sprintf("blob %q exists but is not a regular file (%v) — remove it and "+
					"write a regular file; retrying cannot change this", rel, statErr),
			})
			return
		}
		writeRegisterConflict(w, registerConflict{
			Code: "blob_not_visible", Retryable: true, Inode: req.Inode,
			Message: fmt.Sprintf("blob %q not present as a regular file under the derivative dir — "+
				"write it (atomically) BEFORE registering. RETRYABLE with THIS inode: the write may "+
				"still be draining through the spool (~6s). %v", rel, statErr),
		})
		return
	}
	// NON-EMPTY, for the same reason CommitStagedAt refuses to publish an empty
	// staged file: a MISSING blob 404s and the reader regenerates locally, while
	// a 0-byte blob is served with 200 and looks like a real answer. The farm
	// side already enforces this; leaving it off the contribute path made the
	// rule asymmetric, and the consumer is the LESS trusted writer of the two.
	//
	// It also catches the honest mistake this was found by: registering straight
	// after writing, before the bytes are actually visible through the mount.
	if bfi.Size() == 0 {
		writeRegisterConflict(w, registerConflict{
			Code: "blob_empty", Retryable: true, Inode: req.Inode,
			Message: fmt.Sprintf("blob %q is 0 bytes — an empty blob would be served as a real answer "+
				"instead of 404ing so the reader can regenerate. RETRYABLE with THIS inode: the "+
				"drainer creates the destination at its final name and then copies into it, so a blob "+
				"is legitimately 0 bytes for part of the drain window. Let the write land, then "+
				"register.", rel),
		})
		return
	}

	// OPTIONAL blob-size vouch (contract (AO) Q1.2). Only when the consumer
	// supplied one: omitting blob_size behaves exactly as before this existed.
	//
	// NOT RETRYABLE. A size mismatch means the bytes on the volume are not the
	// bytes the consumer thinks it wrote — a short write, an interrupted copy,
	// or a register that raced its own non-atomic write. Retrying the SAME
	// request cannot change either number; the consumer must rewrite the blob
	// (atomically: temp name + rename) and register again. Marking it retryable
	// would burn the whole retry budget and then publish nothing.
	if req.BlobSize != nil && *req.BlobSize != bfi.Size() {
		writeRegisterConflict(w, registerConflict{
			Code: "blob_truncated", Retryable: false, Inode: req.Inode,
			Message: fmt.Sprintf("blob %q is %d bytes on disk but you vouched %d — the row would "+
				"be published over partial data, which no reader can detect. NOT retryable: "+
				"rewrite the blob (write to a temp name and rename() into the reserved name so "+
				"a partial file never appears at a name we stat), then register again.",
				rel, bfi.Size(), *req.BlobSize),
		})
		return
	}

	// Re-stat the live source via the FUSE mount (direct JuiceFS — no NFS
	// self-loop). entry.Path is volume-relative. Gate: the AI must have been
	// computed against the CURRENT bytes.
	src := filepath.Join(fusePath, entry.Path)
	fi, err := os.Stat(src)
	if err != nil {
		writeRegisterConflict(w, registerConflict{
			Code: "source_unreadable", Retryable: statErrRetryable(err), Inode: req.Inode,
			Path:    entry.Path,
			Message: "source unreadable through the mount: " + err.Error(),
		})
		return
	}
	if fi.Size() != req.SourceSize || fi.ModTime().Unix() != req.SourceMtime {
		writeRegisterConflict(w, registerConflict{
			Code: "source_stale", Retryable: false, Inode: req.Inode, Path: entry.Path,
			Message: fmt.Sprintf("live size/mtime (%d/%d) != vouched (%d/%d) — the derivative was "+
				"computed against OLD bytes. NOT retryable: recompute it against the current source, "+
				"then register. Retrying unchanged would publish a derivative that does not describe "+
				"the file.", fi.Size(), fi.ModTime().Unix(), req.SourceSize, req.SourceMtime),
		})
		return
	}

	// Stamp the canonical source_hash (sampled xxh3) — the server is the hash
	// authority. (The consumer wrote the blob; we do not re-verify its bytes,
	// matching OL-1 "the server only indexes it" — a phantom row degrades
	// gracefully: a reader's blob read fails closed → regenerate locally.)
	hash, err := farm.SampleHash(src, fi.Size())
	if err != nil {
		http.Error(w, "hash source: "+err.Error(), 500)
		return
	}

	mt := spec.mediaType // SERVER-assigned from the kind, never from the request
	var modelP *string
	if req.Model != "" {
		modelP = &req.Model
	}
	// PRODUCER PRECEDENCE (2026-08-04 review, HIGH). PutDeriv is a blind upsert
	// on (inode,kind), so before this an on-device register silently replaced a
	// farm-produced row — flipping producer "linux-farm" -> "on-device" and
	// repointing the row at different bytes, with no admin visibility and no
	// self-healing resync. The farm is the higher-trust producer (it hashes the
	// source itself and runs the locked recipe), so a consumer may ADD a kind the
	// farm has not produced, and may replace its OWN earlier contribution, but may
	// not overwrite the farm's.
	if existing, mErr := ds.Manifest(req.Inode); mErr == nil {
		for _, er := range existing {
			if er.Kind != req.Kind {
				continue
			}
			// COORDINATION, NOT AUTHENTICATION. Read this before "hardening" it.
			//
			// On the Mac, EVERY farm row arrives through farm.ReconcileOneSidecar
			// reading manifest.json off the volume — farm generation itself only
			// ever runs server-side in cmd/jmfarm. There is no authentication on
			// that file, so "produced by the farm" is simply NOT an authenticated
			// property here, and any guard built on it is advisory by nature.
			//
			// A previous attempt keyed this on an unforgeable provenance stamp to
			// stop a forged manifest impersonating the farm. That stamp is applied
			// to every reconciled row — including genuine farm rows, since they
			// take the same path — so the predicate was never true in production
			// and this guard silently became a dead branch, re-opening the very
			// clobber it exists to prevent. The test that "proved" it seeded the
			// row with a direct PutDeriv, a state production cannot reach.
			//
			// So: back to the producer string, which is right for the case that
			// actually happens — a well-behaved consumer must not silently replace
			// the farm's work.
			//
			// The forged-row DoS is NOT excused by "they could write the blob bytes
			// anyway". That comparison is access-equivalent but outcome-inequivalent,
			// and the earlier version of this comment had it wrong:
			//   forged BLOB  → the legitimate producer still registers (200), owns
			//                  the row, and can re-register later. Recoverable.
			//   forged ROW   → producer:"linux-farm", status:"failed", NO blob at
			//                  all. Every register for that (inode,kind) 409s
			//                  forever, and "failed" is deliberately accepted
			//                  because it means "this artifact will never appear".
			//                  There is no API path back; recovery is out-of-band.
			// So the forgery is strictly MORE durable than the write it is compared
			// to. Tracked, not dismissed: the fix is an authenticated sidecar, and
			// until then this guard is coordination rather than a trust boundary.
			if er.Producer == "linux-farm" && req.Producer != "linux-farm" {
				writeRegisterConflict(w, registerConflict{
					Code: "producer_conflict", Retryable: false, Inode: req.Inode,
					Message: fmt.Sprintf("kind %q for inode %d already has a farm-produced row; a %q "+
						"contribution may not replace it. PERMANENT — do not retry; the farm row wins "+
						"by precedence, not by timing.", req.Kind, req.Inode, req.Producer),
				})
				return
			}
		}
	}

	// CODEC IS DECLARED, NOT MANDATED (founder decision, 2026-08-04, superseding
	// the H.264 floor this route shipped with hours earlier).
	//
	// The floor made the LEAST capable node set the ceiling for every node. The
	// ruling inverts it: a node that cannot do a thing contributes the things it
	// CAN do, and the fleet's output is the UNION, not the intersection. An older
	// iMac with no HEVC encoder is not a reason to make every Mac emit H.264 — it
	// is a node that contributes posters, filmstrips and waveforms while a
	// capable machine contributes the HEVC proxy.
	//
	// So the codec is still REQUIRED — silence is read as h264 by every
	// downstream reader, and inferring a codec from silence is how an HEVC blob
	// gets mislabelled — but any codec in the closed vocabulary is accepted, and
	// the reader checks the row before fetching.
	var codecP, codecStrP *string
	if req.Kind == "proxy" || req.Kind == "audio_proxy" {
		c := strings.ToLower(strings.TrimSpace(req.Codec))
		if c == "" {
			http.Error(w, fmt.Sprintf(
				`kind %q requires an explicit "codec" — an absent codec is read as h264 by every downstream reader, so silence would mislabel a richer codec rather than describe it`, req.Kind), 400)
			return
		}
		if !knownProxyCodecs[c] {
			http.Error(w, fmt.Sprintf(
				`codec %q is not in the manifest vocabulary %v — a codec a reader cannot recognise is worse than one it cannot decode, because it cannot even choose`, c, knownProxyCodecList()), 400)
			return
		}
		codecP = &c
		if cs := strings.TrimSpace(req.CodecString); cs != "" {
			if len(cs) > 256 {
				http.Error(w, `"codec_string" exceeds 256 bytes`, 400)
				return
			}
			codecStrP = &cs
		}
	}

	// FILMSTRIP GEOMETRY — required and validated, so a strip that reaches the
	// index is renderable by any client by definition (see
	// validateFilmstripGeometry for the contract this enforces).
	var stripGeo *derivatives.FilmstripGeo
	if req.Kind == "filmstrip" {
		if err := validateFilmstripGeometry(req.Filmstrip); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		g := *req.Filmstrip
		stripGeo = &g
	} else if req.Filmstrip != nil {
		http.Error(w, fmt.Sprintf("filmstrip geometry is not meaningful for kind %q", req.Kind), 400)
		return
	}

	// ARTIFACT DESCRIPTORS (consumer ask, founder-endorsed 2026-08-04).
	//
	// If a node's contribution is a FLOOR rather than a finished artifact, the
	// server has to be able to tell a cheap artifact from a good one WITHOUT
	// decoding it, so it can earmark the cheap one for upgrade. Today it cannot:
	// a 320px poster and a 720px poster differ on the wire only by blob_size,
	// which is a bad proxy for quality — a flat LOG-graded 720 frame can encode
	// smaller than a busy 320. `dim` is embedding dimensionality, not image size.
	var widthP, heightP *int
	if req.Width != 0 || req.Height != 0 {
		if req.Kind != "thumbnail" && req.Kind != "proxy" && req.Kind != "audio_proxy" && req.Kind != "filmstrip" {
			http.Error(w, fmt.Sprintf(`width/height are pixel dimensions and are not meaningful for kind %q`, req.Kind), 400)
			return
		}
		if req.Width <= 0 || req.Height <= 0 || req.Width > maxArtifactDim || req.Height > maxArtifactDim {
			http.Error(w, fmt.Sprintf(`width/height must both be in 1..%d (got %dx%d)`, maxArtifactDim, req.Width, req.Height), 400)
			return
		}
		w2, h2 := req.Width, req.Height
		widthP, heightP = &w2, &h2
	}
	var bitrateP *int64
	if req.BitrateBPS != 0 {
		if req.Kind != "proxy" && req.Kind != "audio_proxy" {
			http.Error(w, `bitrate_bps is only meaningful for a proxy`, 400)
			return
		}
		if req.BitrateBPS < 0 || req.BitrateBPS > maxArtifactBitrate {
			http.Error(w, fmt.Sprintf(`bitrate_bps must be in 1..%d`, maxArtifactBitrate), 400)
			return
		}
		b := req.BitrateBPS
		bitrateP = &b
	}

	var dimP *int
	if req.Dim > 0 {
		dimP = &req.Dim
	}
	// Persist the authoritative (re-stat'd) source size+mtime on the row so any
	// other client's read-gate can stat-verify off the manifest directly.
	srcSize := fi.Size()
	srcMtime := fi.ModTime().Unix()
	row := derivatives.DerivRow{
		Kind: req.Kind, Status: "ready", Producer: req.Producer, Version: 1,
		Hash: &hash, BlobRelPath: &rel, MediaType: &mt, Model: modelP, Dim: dimP,
		UpdatedAt:   time.Now().Unix(),
		SourceSize:  &srcSize,
		SourceMtime: &srcMtime,
		Codec:       codecP,
		CodecString: codecStrP,
		Width:       widthP,
		Height:      heightP,
		BitrateBPS:  bitrateP,
		Filmstrip:   stripGeo,
	}
	if err := ds.PutSource(req.Inode, &hash); err != nil {
		http.Error(w, "put source: "+err.Error(), 500)
		return
	}
	if err := ds.PutDeriv(req.Inode, row); err != nil {
		http.Error(w, "put deriv: "+err.Error(), 500)
		return
	}
	jmlog.Info("contribute-back registered",
		"inode", req.Inode, "kind", req.Kind, "blob", rel,
		"producer", req.Producer, "model", req.Model, "hash", hash)
	writeContractJSON(w, registerResponse{Inode: req.Inode, Registered: true, Derivative: row})
}

// handleBlobHTTP serves GET /blob?inode=N&kind=proxy (PROXY-CODEC #50): the
// byte-range delivery of a derivative blob (proxy.mp4 etc.). It resolves the
// blob via the asset's manifest row (blob_rel_path + media_type, fail-closed: a
// kind that isn't a READY blob row → 404), opens it through the FUSE mount, and
// hands it to http.ServeContent, which implements the full Range/206/Accept-
// Ranges contract: a ranged request gets 206 + Content-Range + Content-Length,
// an unranged GET gets 200 + Content-Length, both with Accept-Ranges: bytes.
// That is exactly what a browser <video> and a remote AVPlayer need to seek over
// HTTP, identical for h264/hevc/av1.

// blobVouchHeaders publishes the row's own vouch on a successful /blob response.
//
// The consumer's D3 trust gate compares the manifest's source_size against a
// live stat before applying identity-bearing state (embeddings, transcripts).
// Without these it must fetch /derivatives as well as /blob for every asset —
// two round trips to answer one question, on a link where round trips are the
// cost model. Same values the manifest carries; no new source of truth.
func blobVouchHeaders(w http.ResponseWriter, kind string, row derivatives.DerivRow) {
	w.Header().Set("X-JM-Kind", kind)
	if row.Hash != nil {
		w.Header().Set("X-JM-Hash", *row.Hash)
	}
	if row.SourceSize != nil {
		w.Header().Set("X-JM-Source-Size", strconv.FormatInt(*row.SourceSize, 10))
	}
	if row.SourceMtime != nil {
		w.Header().Set("X-JM-Source-Mtime", strconv.FormatInt(*row.SourceMtime, 10))
	}
}

func handleBlobHTTP(w http.ResponseWriter, r *http.Request) {
	inode, ok := parseInodeQuery(w, r)
	if !ok {
		return
	}
	kind := r.URL.Query().Get("kind")
	if kind == "" {
		kind = "proxy" // the only blob kind /blob serves today.
	}
	globalMu.Lock()
	ds := globalDerivStore
	tc := globalThumbCache
	mount := globalFUSEPath
	if mount == "" {
		mount = globalMountPath
	}
	globalMu.Unlock()
	if ds == nil || mount == "" {
		http.Error(w, "control plane not ready", http.StatusServiceUnavailable)
		return
	}

	// On a miss, try the JM-15 on-demand reconcile so a farm-derived blob this app
	// hasn't ingested yet still resolves (same bridge the /derivatives handler uses).
	if known, _ := ds.Known(inode); !known {
		if found, ferr := farm.ReconcileOneSidecar(ds, mount, inode); ferr == nil && found {
			// reconciled — fall through to the manifest read below.
		}
	}
	rows, err := ds.Manifest(inode)
	if err != nil {
		http.Error(w, "manifest read failed", http.StatusInternalServerError)
		return
	}
	var blobRel, mediaType string
	var row derivatives.DerivRow
	for _, d := range rows {
		if d.Kind != kind || d.Status != "ready" || d.BlobRelPath == nil || *d.BlobRelPath == "" {
			continue
		}
		row = d
		blobRel = *d.BlobRelPath
		if d.MediaType != nil {
			mediaType = *d.MediaType
		}
		break
	}
	if blobRel == "" {
		// WHY A REASON HEADER. Every miss here is a 404 with a prose body, and a
		// consumer cannot tell "you do not serve this kind" from "the row exists
		// but its bytes are gone" — which are opposite problems: the first is an
		// integration bug on their side, the second is a data gap on ours.
		// Measured 2026-08-18: 214 of 400 sampled ready rows point at a
		// blob_rel_path with no file on the volume, so the second case is the
		// COMMON one and was indistinguishable. The status stays 404; only the
		// diagnosis is added.
		w.Header().Set("X-JM-Blob-Miss", "no-ready-row")
		http.Error(w, "no ready "+kind+" blob for this inode", http.StatusNotFound)
		return
	}
	// C5 (data integrity): the row is READY, but derivatives are keyed by INODE
	// and an inode is not a content identity — overwrite this file in place
	// (re-export, re-transcode) or let JuiceFS recycle a deleted inode, and this
	// row (and any blob cached under it) now depicts bytes that are gone. Gate on
	// the row's own vouch, source_size/source_mtime, against the live size/mtime
	// the in-RAM mirror already holds. This is the SAME comparison the AI
	// contribute-back POST path enforces above (409 "computed against old bytes")
	// — it simply was never applied on the way out.
	//
	// liveSourceFor is a mirror map read, deliberately NOT a stat of the source:
	// a stat here would add a full ~500ms cellular round-trip to every thumbnail
	// and to every byte-range request a player makes while scrubbing a proxy.
	// A mismatch is treated as a MISS — same 404 an absent derivative gets, so
	// the caller regenerates — and the persistent thumb-cache copy is dropped so
	// the stale bytes are not served from local disk after a restart either.
	if live := liveSourceFor(inode); derivRowStale(row, live) {
		rejectStaleDeriv(tc, inode, kind, row, live)
		w.Header().Set("X-JM-Blob-Miss", "stale-source")
		http.Error(w, "no ready "+kind+" blob for this inode", http.StatusNotFound)
		return
	}
	if mediaType == "" {
		mediaType = "application/octet-stream"
	}

	// Resolve the blob under the Tier-A per-inode dir and open it through the FUSE
	// mount (direct JuiceFS — no NFS self-loop). filepath.Clean on the rel path
	// keeps it inside the inode dir (the farm only ever writes flat names there).

	// #1 hydration pack: SMALL kinds (everything but proxy) serve LOCAL-FIRST
	// from the thumb cache, populated by the folder-open warmer or a prior
	// read-through here. The manifest row above still authorized the kind,
	// supplied the media type, AND passed the C5 freshness gate, so the
	// fail-closed 404 semantics are identical and the cached bytes are known to
	// match the live source — only the byte source swaps (local SSD instead of
	// a FUSE round-trip). tc was captured with ds above.
	smallKind := kind != "proxy"
	if tc != nil && smallKind {
		if croot, crel, ok := tc.PathParts(inode, kind); ok {
			// Anchored, like the FUSE read below. This branch serves BEFORE the
			// guarded open further down, so leaving it unguarded made that guard
			// bypassable through the very cache a poisoned read populates.
			if lf, lerr := derivatives.OpenRegularUnder(croot, crel); lerr == nil {
				defer lf.Close()
				if lfi, serr := lf.Stat(); serr == nil {
					blobVouchHeaders(w, kind, row)
					w.Header().Set("Content-Type", mediaType)
					http.ServeContent(w, r, "", lfi.ModTime(), lf)
					return
				}
			}
		}
	}

	// Anchored at the MOUNT, not at the per-inode directory: O_NOFOLLOW binds
	// only the final component, so guarding just the blob name left the <inode>
	// directory itself swappable for a symlink to anywhere (round-3 HIGH).
	f, err := derivatives.OpenRegularUnder(mount, derivatives.DerivBlobRel(inode, blobRel))
	if err != nil {
		// The row promised this file and it is not there. Distinct from
		// no-ready-row: the manifest ADVERTISED it, so a consumer that gated on
		// isReadyAndFresh did everything right and still got nothing.
		w.Header().Set("X-JM-Blob-Miss", "blob-absent")
		http.Error(w, "blob unreadable", http.StatusNotFound)
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		http.Error(w, "blob stat failed", http.StatusInternalServerError)
		return
	}
	// Read-through populate: a small blob served from FUSE lands in the local
	// cache so the NEXT request (and offline browsing) is 0-RTT. Bounded by
	// thumbReadThroughCap; the bytes are served from RAM in the same response.
	if tc != nil && smallKind && fi.Size() <= thumbReadThroughCap {
		data, rerr := io.ReadAll(io.LimitReader(f, thumbReadThroughCap))
		if rerr == nil && int64(len(data)) == fi.Size() {
			if _, perr := tc.Put(inode, kind, bytes.NewReader(data)); perr != nil {
				jmlog.Debug("thumb read-through populate failed", "inode", inode, "kind", kind, "error", perr.Error())
			}
			blobVouchHeaders(w, kind, row)
			w.Header().Set("Content-Type", mediaType)
			http.ServeContent(w, r, fi.Name(), fi.ModTime(), bytes.NewReader(data))
			return
		}
		// Short/failed read: fall back to the plain file path from offset 0.
		if _, serr := f.Seek(0, io.SeekStart); serr != nil {
			http.Error(w, "blob read failed", http.StatusInternalServerError)
			return
		}
	}
	// http.ServeContent sets Content-Type (we pin it from the manifest media_type),
	// Accept-Ranges: bytes, and the full 200/206 + Content-Range/Content-Length
	// Range machinery. modtime drives caching validators; the blob's mtime is fine.
	blobVouchHeaders(w, kind, row)
	w.Header().Set("Content-Type", mediaType)
	http.ServeContent(w, r, fi.Name(), fi.ModTime(), f)
}

// assertRequest is the POST /assertions body (JM-ASSERT #51). The consumer
// targets an asset by inode, absolute path, or the portable asset_key/
// content_hash directly; the server resolves it to the content-hash asset_key,
// writes the <media>.loupe.json sidecar (source of truth), and upserts the
// asset_key-keyed Tier-B index. value is json.RawMessage so null (retract) is
// preserved verbatim and distinct from a missing field.
type assertRequest struct {
	Inode       uint64          `json:"inode"`
	Path        string          `json:"path"`
	AssetKey    string          `json:"asset_key"`
	ContentHash string          `json:"content_hash"`
	Namespace   string          `json:"namespace"`
	Key         string          `json:"key"`
	Value       json.RawMessage `json:"value"`
	AssertedBy  string          `json:"asserted_by"`
	AssertedAt  string          `json:"asserted_at"`
}

// assertWriteResponse is the POST /assertions 200 body. Schema:
// contract/spec/schema/assertions.schema.json. inode is echoed (omitempty) only
// when the write resolved/queried an accelerator inode.
type assertWriteResponse struct {
	Accepted          bool   `json:"accepted"`
	WinningAssertedAt string `json:"winning_asserted_at"`
	AssetKey          string `json:"asset_key"`
	Inode             uint64 `json:"inode,omitempty"`
}

// assertReadResponse is the GET /assertions 200 body. Schema:
// contract/spec/schema/assertions-get.schema.json. assertions is ALWAYS a
// non-null array (empty when none).
type assertReadResponse struct {
	AssetKey   string             `json:"asset_key"`
	Inode      uint64             `json:"inode,omitempty"`
	Assertions []assertReadTriple `json:"assertions"`
}

type assertReadTriple struct {
	Namespace  string          `json:"namespace"`
	Key        string          `json:"key"`
	Value      json.RawMessage `json:"value"`
	AssertedBy string          `json:"asserted_by"`
	AssertedAt string          `json:"asserted_at"`
}

// handleAssertionsHTTP serves POST/GET /assertions (JM-ASSERT #51). POST applies
// one namespaced human assertion under last-writer-wins: it resolves the asset to
// a content-hash asset_key (hashing the live source when given inode/path,
// path:<basename> fallback when the bytes aren't hashable), writes the portable
// <media>.loupe.json sidecar (the source of truth — atomic, merge-not-clobber)
// AND upserts the asset_key-keyed Tier-B index, then returns whether it won LWW.
// GET reads the resolved (LWW) set by asset_key, or by inode/path resolved to an
// asset_key; assertions is a non-null array (empty on a miss — fail-closed).
func handleAssertionsHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		handleAssertionsGet(w, r)
	case http.MethodPost:
		handleAssertionsPost(w, r)
	default:
		http.Error(w, "GET or POST only", http.StatusMethodNotAllowed)
	}
}

func handleAssertionsGet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	assetKey := q.Get("asset_key")
	if assetKey == "" {
		assetKey = q.Get("content_hash")
	}
	var inode uint64
	if v := q.Get("inode"); v != "" {
		inode, _ = strconv.ParseUint(v, 10, 64)
	}

	globalMu.Lock()
	ds := globalDerivStore
	store := globalStore
	mp := globalMountPath
	if mp == "" {
		mp = globalWantMountPoint
	}
	globalMu.Unlock()

	resp := assertReadResponse{Assertions: []assertReadTriple{}}
	if ds == nil {
		writeContractJSON(w, resp) // fail closed → empty set, empty asset_key
		return
	}
	// Resolve a path query to an accelerator inode (entries are volume-relative).
	if assetKey == "" && inode == 0 && q.Get("path") != "" && store != nil {
		if e := store.LookupByPath(metaRelPath(q.Get("path"), mp)); e != nil {
			inode = e.Inode
		}
	}
	// inode/path → asset_key via the accelerator column.
	if assetKey == "" && inode != 0 {
		resp.Inode = inode
		if k, err := ds.AssetKeyForInode(inode); err == nil {
			assetKey = k
		}
	}
	if assetKey == "" {
		writeContractJSON(w, resp) // unknown → empty asset_key + empty assertions[]
		return
	}
	raws, err := ds.AssertionsByAssetKey(assetKey)
	if err != nil {
		jmlog.Warn("assertions read failed", "asset_key", assetKey, "error", err.Error())
		writeContractJSON(w, resp)
		return
	}
	resp.AssetKey = assetKey
	for _, a := range raws {
		resp.Assertions = append(resp.Assertions, assertReadTriple{
			Namespace: a.Namespace, Key: a.Key, Value: json.RawMessage(a.ValueJSON),
			AssertedBy: a.AssertedBy, AssertedAt: a.AssertedAt,
		})
	}
	writeContractJSON(w, resp)
}

func handleAssertionsPost(w http.ResponseWriter, r *http.Request) {
	var req assertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json body", 400)
		return
	}
	if req.Namespace == "" || req.Key == "" {
		http.Error(w, "missing namespace/key", 400)
		return
	}
	if req.AssertedBy == "" || req.AssertedAt == "" {
		http.Error(w, "missing asserted_by/asserted_at", 400)
		return
	}
	valueJSON := strings.TrimSpace(string(req.Value))
	if valueJSON == "" {
		valueJSON = "null" // an omitted value is a retract (null)
	}

	globalMu.Lock()
	ds := globalDerivStore
	store := globalStore
	fusePath := globalFUSEPath
	mp := globalMountPath
	if mp == "" {
		mp = globalWantMountPoint
	}
	globalMu.Unlock()
	if ds == nil {
		http.Error(w, "control plane not ready", http.StatusServiceUnavailable)
		return
	}

	// Resolve identity: asset_key (preferred), else inode/path → live source →
	// content-hash asset_key, with a path:<basename> fallback. mediaPath is the
	// FUSE-mount path to the source (for hashing + the sidecar location); empty
	// when only an asset_key was given (a pure asset_key write has no inode/sidecar).
	assetKey := req.AssetKey
	if assetKey == "" {
		assetKey = req.ContentHash
	}
	var entry *metadata.Entry
	if store != nil {
		if req.Inode != 0 {
			entry = store.LookupByInode(req.Inode)
		} else if req.Path != "" {
			entry = store.LookupByPath(metaRelPath(req.Path, mp))
		} else if assetKey != "" {
			// Pure asset_key write: if this key was previously bound to an
			// accelerator inode (an earlier inode/path write), resolve the media
			// so we still write the sidecar (the source of truth) — not just the
			// index. A never-before-seen portable key has no inode → sidecar-less
			// index write, which the response honestly reflects.
			if in, err := ds.InodeForAssetKey(assetKey); err == nil && in != 0 {
				entry = store.LookupByInode(in)
			}
		}
	}
	var mediaPath, mediaName string
	resolvedInode := req.Inode
	if entry != nil {
		resolvedInode = entry.Inode
		mediaName = entry.Name
		if fusePath != "" {
			mediaPath = filepath.Join(fusePath, entry.Path)
		}
	} else if req.Path != "" {
		mediaName = filepath.Base(req.Path)
		mediaPath = req.Path
	}
	// Hash the live source for the portable asset_key when we have no explicit key.
	if assetKey == "" && mediaPath != "" {
		if fi, err := os.Stat(mediaPath); err == nil {
			if h, herr := farm.SampleHash(mediaPath, fi.Size()); herr == nil {
				assetKey = "xxh3:" + h
			}
		}
	}
	if assetKey == "" {
		if mediaName != "" {
			assetKey = derivatives.PathAssetKey(mediaName) // path/name fallback (§2)
		} else {
			http.Error(w, "could not resolve an asset_key (need asset_key, inode, or path)", 400)
			return
		}
	}

	// 1) Write the sidecar (the SOURCE OF TRUTH) when we know where the media is.
	//    The sidecar's LWW result is authoritative; the index mirrors it.
	var value any
	_ = json.Unmarshal([]byte(valueJSON), &value) // null → nil; preserves type
	incoming := farm.SidecarAssertion{
		Namespace: req.Namespace, Key: req.Key, Value: value,
		AssertedBy: req.AssertedBy, AssertedAt: req.AssertedAt,
	}
	var sidecarRes farm.ApplyAssertionResult
	sidecarWritten := false
	if mediaPath != "" {
		if res, err := farm.ApplyAssertion(farm.AssertionSidecarPath(mediaPath), assetKey, mediaName, incoming); err != nil {
			jmlog.Warn("assertion sidecar write failed", "media", mediaPath, "error", err.Error())
		} else {
			sidecarRes = res
			sidecarWritten = true
		}
	}

	// 2) Upsert the rebuildable Tier-B index (the accelerator GET answers from).
	idxRes, err := ds.AssertLWW(assetKey, req.Namespace, req.Key, valueJSON, req.AssertedBy, req.AssertedAt, resolvedInode)
	if err != nil {
		http.Error(w, "assertion index write failed: "+err.Error(), 500)
		return
	}

	// The sidecar is authoritative when written; otherwise the index decides.
	out := assertWriteResponse{AssetKey: assetKey, Inode: resolvedInode}
	if sidecarWritten {
		out.Accepted = sidecarRes.Accepted
		out.WinningAssertedAt = sidecarRes.WinningAssertedAt
	} else {
		out.Accepted = idxRes.Accepted
		out.WinningAssertedAt = idxRes.WinningAssertedAt
	}
	jmlog.Info("JM-ASSERT write",
		"asset_key", assetKey, "namespace", req.Namespace, "key", req.Key,
		"accepted", out.Accepted, "sidecar", sidecarWritten)
	writeContractJSON(w, out)
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
// handlePresenceHTTP serves the live presence snapshot (Tier-1 #3).
func handlePresenceHTTP(w http.ResponseWriter, r *http.Request) {
	globalMu.Lock()
	pt := globalPresence
	globalMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if pt == nil {
		w.Write([]byte(`{"enabled":false}`))
		return
	}
	out := map[string]any{"enabled": true, "host": pt.Host()}
	if r.URL.Query().Get("all") == "1" {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		out["hosts"] = pt.AllSnapshots(ctx)
	} else {
		local := pt.LocalSnapshot()
		out["files"] = local
		out["open_count"] = len(local)
	}
	_ = json.NewEncoder(w).Encode(out)
}

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
			// V2.3 U6 (task #37): show real progress — an opaque
			// multi-minute rebuild reads as "stuck" (field report). The
			// total is the previous sync's entry count, hence the ~.
			scanned, estTotal := rc.SyncProgress()
			switch {
			case estTotal > 0 && scanned > 0:
				pct := scanned * 100 / estTotal
				if pct > 99 {
					pct = 99 // estimate — never claim done before completion
				}
				op.Detail = fmt.Sprintf("Rebuilding index… %d / ~%d (%d%%)", scanned, estTotal, pct)
			case scanned > 0:
				op.Detail = fmt.Sprintf("Rebuilding index… %d entries scanned", scanned)
			default:
				op.Detail = "Rebuilding index…"
			}
			op.Files = int(scanned)
			working = append(working, "rebuilding index")
		} else {
			last := rc.LastSyncTime()
			op.Files = rc.LastSyncEntries()
			switch {
			case rc.SyncDeferredReason() != "":
				// G7 (task #80): the slow-link full rebuild was DEFERRED, not
				// failed — IsSyncing() now reports false for a dead attempt,
				// so say what actually happened instead of the eternal
				// "Rebuilding index…" (live 2026-07-01: 3h of it while the
				// push was engaged and healthy the whole time).
				op.Detail = "Index sync deferred — link too slow for a full rebuild; live updates continue via push"
			case last.IsZero():
				op.Detail = "Index not yet built"
			default:
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

// offlineStartReason is the human-readable reason surfaced (via pin.SetAutoOffline
// → pin.State().Reason and the "started_offline:" start result) when the app
// boots with the metadata backend unreachable (R-4). The UI shows it in the
// "Started offline — showing cached state" banner. Batch-3 review #8: this is
// now only the R-4 DEFAULT for startOfflineReason — the U2 (slow backend) and
// U3 (persisted user intent) offline starts carry their own per-cause reason.
const offlineStartReason = "Started offline — backend unreachable at launch"

// backendReachableQuick does a single cheap TCP dial to the metadata host so
// startup can decide up front whether to take the normal (online) path or the
// start-while-offline path (R-4). It MUST be cheap and short — it gates startup
// latency. Returns true (assume reachable, fall through to the normal connect)
// when the URL can't be parsed, so a parse quirk never forces a spurious
// offline start.
// backendReachableRTT is the boot reachability probe plus the dial RTT it
// was previously performing-and-discarding (V2.3 U2/K2). The RTT seeds
// netprofile (so class-gated behavior is correct from the first moment) and
// lets the boot path treat a reachable-but-terrible link as effectively
// down. rtt is 0 when the address can't be parsed (optimistic-up).
//
// Review fix (MED): DNS resolution happens OUTSIDE the timed window — a cold
// resolver / MagicDNS lookup at wake could otherwise push a healthy link
// past the defer threshold and seed netprofile with an inflated first
// sample. Only the TCP connect against the resolved address is timed.
func backendReachableRTT(redisURL string, timeout time.Duration) (up bool, rtt time.Duration) {
	addr, _, err := metadata.ParseRedisURL(redisURL)
	if err != nil || addr == "" {
		return true, 0
	}
	resolved, rerr := net.ResolveTCPAddr("tcp", addr)
	if rerr != nil {
		// Resolution failure = unreachable for boot purposes (same signal a
		// dial would return, just without burning the timeout on it).
		return false, 0
	}
	t0 := time.Now()
	conn, derr := net.DialTimeout("tcp", resolved.String(), timeout)
	if derr != nil {
		return false, 0
	}
	rtt = time.Since(t0)
	_ = conn.Close()
	netprofile.Default().ObserveRTT(rtt)
	return true, rtt
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

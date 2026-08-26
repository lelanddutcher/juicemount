// Package health — JuiceFS FUSE mount lifecycle management.
//
// FUSEManager mounts JuiceFS, monitors the mount health, and automatically
// remounts if the FUSE process dies or the mount becomes stale.
package health

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// linkAwareJuiceFSPolicy picks the mount-time juicefs prefetch flags
// (--buffer-size / --prefetch) for the current link. JuiceFS's own sequential
// readahead is the dominant prefetcher (validated 2026-06-15: a cold 4 KB read
// pulls the whole file even with our server readahead capped) and these flags
// are mount-time only, so we classify the link HERE, at mount, from a single
// cheap TCP dial to the backend — the same passive philosophy as the
// reachability probe (no synthetic load on a link we may already suspect).
//
// On any parse/dial failure we feed no RTT sample and fall through to the
// profile's default (medium == the historical mount flags), so a transient
// backend hiccup never degrades the mount config.
func (fm *FUSEManager) linkAwareJuiceFSPolicy() netprofile.JuiceFSPolicy {
	if u, err := url.Parse(fm.cfg.RedisURL); err == nil && u.Host != "" {
		start := time.Now()
		conn, derr := net.DialTimeout("tcp", u.Host, 2*time.Second)
		if derr == nil {
			rtt := time.Since(start)
			_ = conn.Close()
			netprofile.Default().ObserveRTT(rtt)
		}
	}
	pol := netprofile.Default().JuiceFS()
	snap := netprofile.Default().Snapshot()

	// Log the DECISION, not just the probe. These flags are mount-time only and
	// never re-track the class afterwards, so this line is the sole record of
	// what the mount actually got and how much the estimator knew when it chose.
	//
	// class_measured matters more than class here. At mount the profile has just
	// been restarted, so it usually has an RTT bootstrap and NO bandwidth
	// sample, and class is BW-driven once samples exist. Measured 2026-08-16
	// across six runs on one 10GbE link: three recorded bandwidth=0 with
	// class_measured=false, and the observed class swung medium/fast (and the
	// bandwidth estimate 0 -> 26 -> 1098 -> 1650 MB/s) while the mount flags
	// stayed frozen at whatever this moment decided. A mount that came up
	// `medium` and a runtime that reports `fast` is not a bug by itself — but it
	// is indistinguishable from one without this line.
	jmlog.Info("link-aware mount policy chosen",
		"class", snap.Class.String(),
		"class_measured", snap.HaveRTT && snap.HaveBW,
		"have_rtt", snap.HaveRTT, "have_bandwidth", snap.HaveBW,
		"rtt_ms", snap.RTT.Milliseconds(),
		"bandwidth_mbps", snap.BytesPerSec/(1024*1024),
		"bootstrapped_from_rtt", snap.BootstrappedRTT,
		"buffer_size_mb", pol.BufferSizeMB,
		"prefetch", pol.Prefetch,
		"nfs_readahead", netprofile.Default().NFSReadahead())
	return pol
}

// redactURLCreds returns a copy of `raw` with any user:password component
// of a parseable URL replaced by `<redacted>`. Used to keep credentials
// out of logs when a user supplies a `--bucket` URL of the form
// `http://user:pass@host:port/bucket`. Non-URL strings pass through
// unchanged so this is safe to apply to anything log-suspect.
func redactURLCreds(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}
	u.User = url.User("<redacted>")
	return u.String()
}

// FUSEConfig holds JuiceFS mount configuration.
type FUSEConfig struct {
	RedisURL   string // e.g. "redis://127.0.0.1:6379/1"
	MountPoint string // e.g. ~/.juicemount/fuse-internal
	CacheDir   string // e.g. ~/.juicefs/cache (empty = JuiceFS default)
	CacheSize  string // e.g. "100000" in MiB (empty = JuiceFS default ~100 GiB)
	// PinnedBytes is the total size of all pinned files, in bytes. The
	// cache-size policy (see Mount) grows the cache to at least this so pinned
	// content never LRU-evicts. 0 = unknown / no pins (cache-size stays at the
	// user's configured value).
	PinnedBytes int64
	JuiceFSBin  string // e.g. /opt/homebrew/bin/juicefs (auto-detected if empty)

	// FreeSpaceRatio is the fraction of cache-volume free space JuiceFS keeps
	// reserved (won't cache when below). Default in JuiceFS is 0.1 (10%) which
	// is hostile to video editors who fill their disks with media — once they
	// drop under 90 GB free on a 1 TB disk, the cache silently disables and
	// every read goes straight to S3 with no warning.
	//
	// Empty = pass nothing (JuiceFS default applies). "0.01" = keep 1% free.
	FreeSpaceRatio string

	// OpenCacheTTL is the --open-cache duration, e.g. "5s". Empty = omit the
	// flag (juicefs default 0s = every open re-validates against Redis).
	//
	// THIS TRAVELS IN THE CONFIG, NOT THE ENVIRONMENT, and that is the whole
	// point. Go snapshots os.Environ at c-archive init, so a host-side setenv()
	// from Swift is invisible to os.Getenv (see ServerController.start). Every
	// JM_* knob in this file is therefore UNREACHABLE from a shipped app — only
	// its default ships. That is harmless for the knobs whose default is the
	// wanted behaviour, and fatal for this one, whose default is OFF: the
	// measured 66x open win (2026-07-29 cellular: first open 1226-5660ms,
	// second 24-36ms) could not be switched on by the person running the test.
	// JM_WAN_MODE died exactly this way — read in six paths, set by nothing.
	OpenCacheTTL string

	// BucketOverride is the S3 endpoint URL the Mac juicefs daemon should
	// use, overriding whatever bucket URL was stored in Redis at format
	// time. Empty = no override; juicefs reads the URL from Redis.
	//
	// Why this exists: JuiceFS stores the bucket URL in Redis when the
	// volume is formatted. On a docker-bridge-networked server-side
	// install the format URL is the docker-internal DNS name (e.g.
	// http://minio:9000/zpool) which the Mac cannot resolve. Setting
	// BucketOverride to http://<truenas-lan-ip>:30151/zpool lets the
	// Mac client reach the server's MinIO directly without /etc/hosts
	// hacks or macvlan networking on the server.
	//
	// Passed as the `--bucket` flag on `juicefs mount`, which wins over
	// the Redis-stored URL.
	BucketOverride string

	// FUSEMetricsAddr is the host:port the JuiceFS mount daemon binds its
	// Prometheus /metrics endpoint to, passed explicitly as `--metrics`. The
	// bridge scrapes `juicefs_blockcache_bytes` from this endpoint to report
	// the TRUE on-disk block-cache size as `cache_used_bytes` in /cache-status
	// (replacing the pinned-only aggregate + the racy cache-dir du).
	//
	// Why explicit, and why NOT :9567: JuiceFS defaults the mount metrics to
	// 127.0.0.1:9567, but the `juicefs sync`/manager path (internal/manager/
	// sync.go) ALSO pins :9567. Relying on the mount's default would collide
	// with a concurrent sync's metrics listener (the documented loopback-port
	// collision). We bind the mount to a distinct default of 127.0.0.1:9568.
	//
	// Empty = pass nothing; JuiceFS falls back to its own :9567 default. The
	// bridge only scrapes the addr it knows about, so an empty value just
	// means cache_used_bytes uses the dir-walk fallback.
	FUSEMetricsAddr string
}

// DefaultFUSEMetricsAddr is the loopback host:port the FUSE mount daemon binds
// its Prometheus metrics to when FUSEConfig.FUSEMetricsAddr is empty. It is
// deliberately :9568 — distinct from the `juicefs sync` path's :9567 — to avoid
// the documented loopback-port collision between a running mount and a
// concurrent sync.
const DefaultFUSEMetricsAddr = "127.0.0.1:9568"

// FUSEManager manages the JuiceFS FUSE mount lifecycle.
type FUSEManager struct {
	cfg    FUSEConfig
	mu     sync.Mutex
	cmd    *exec.Cmd
	stopCh chan struct{}
	done   chan struct{}
	// onRemount fires after a successful WATCHDOG remount (#12): pooled
	// FUSE fds reference the dead mount and must be flushed. Guarded by mu;
	// invoked without mu held. Set via SetOnRemount (bridge wiring).
	onRemount func()
}

// SetOnRemount registers a callback invoked after every successful
// watchdog-driven remount. Safe to call before or after StartMonitor.
func (fm *FUSEManager) SetOnRemount(fn func()) {
	fm.mu.Lock()
	fm.onRemount = fn
	fm.mu.Unlock()
}

// fireOnRemount invokes the registered callback (if any) without holding mu.
func (fm *FUSEManager) fireOnRemount() {
	fm.mu.Lock()
	fn := fm.onRemount
	fm.mu.Unlock()
	if fn != nil {
		fn()
	}
}

// EffectiveCacheSize returns the cache-size string actually passed to the
// JuiceFS daemon (post auto-expansion). Used by callers that want to log
// the effective value rather than the user's original input.
func (fm *FUSEManager) EffectiveCacheSize() string {
	return fm.cfg.CacheSize
}

// EffectiveMetricsAddr returns the host:port the FUSE daemon's Prometheus
// metrics endpoint is bound to (the value passed as `--metrics`), filling in
// DefaultFUSEMetricsAddr when the config left it empty. The bridge uses this to
// tell the block-cache scraper where to find juicefs_blockcache_bytes.
func (fm *FUSEManager) EffectiveMetricsAddr() string {
	if fm.cfg.FUSEMetricsAddr != "" {
		return fm.cfg.FUSEMetricsAddr
	}
	return DefaultFUSEMetricsAddr
}

// lastNonEmptyLine returns the last non-blank line of s, trimmed. juicefs
// prints progress/info lines before the fatal one, so the final non-empty
// line is the actionable reason. Returns "" if s has no non-blank content.
func lastNonEmptyLine(s string) string {
	lines := strings.Split(s, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if t := strings.TrimSpace(lines[i]); t != "" {
			return t
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// macFUSE kext-approval classification (V2.3 G0d, 2026-07-01).
//
// A macOS update silently revokes the macFUSE system-extension approval
// (kernelmanagerd: "Extension io.macfuse.filesystems.macfuse.N not approved
// to load"). From then on EVERY juicefs mount hangs at "Mounting volume…"
// and dies with "mount point is not ready in 10 seconds" — an error
// indistinguishable from a slow backend unless the kext state is checked.
// No amount of retrying can succeed; only the user can fix it (System
// Settings approval). Classify once per failure so the log carries the exact
// remediation and /health can surface it instead of a generic "no FUSE".

// kextBlockedFlag latches "mount failed AND the macFUSE kext is not loaded".
// Cleared on any successful mount. Read by the health monitor's FUSE check.
var kextBlockedFlag atomic.Bool

// KextApprovalBlocked reports whether the last mount failure was classified
// as the macFUSE kext being unloaded (approval loss). Cheap; any goroutine.
func KextApprovalBlocked() bool { return kextBlockedFlag.Load() }

// noteMountFailure classifies a mount failure. Only latches the blocked flag
// on high confidence (kmutil ran and macfuse is absent); on kmutil error or
// timeout it stays quiet — the FUSE identity gate is the safety mechanism,
// this is diagnosis for the human.
func noteMountFailure() {
	if kextLoaded() {
		kextBlockedFlag.Store(false)
		return
	}
	if kextBlockedFlag.CompareAndSwap(false, true) {
		jmlog.Error("macFUSE kext is NOT loaded — juicefs mount cannot succeed until it is approved. " +
			"Likely cause: a macOS update reset the system-extension approval. " +
			"USER ACTION: System Settings → Privacy & Security → Allow \"Benjamin Fleischer\" (macFUSE), then reboot if prompted. " +
			"Until then the FUSE identity gate parks drains and prunes; writes stay safely in the spool.")
	}
}

// kextLoaded reports whether any macFUSE kext is currently loaded, bounded.
// Returns true (= "can't claim blocked") when kmutil fails or times out.
func kextLoaded() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kmutil", "showloaded").Output()
	if err != nil {
		return true
	}
	return strings.Contains(strings.ToLower(string(out)), "macfuse")
}

// NewFUSEManager creates a FUSE mount manager.
func NewFUSEManager(cfg FUSEConfig) *FUSEManager {
	if cfg.JuiceFSBin == "" {
		cfg.JuiceFSBin = findJuiceFSBin()
	}
	return &FUSEManager{
		cfg:    cfg,
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// Mount starts the JuiceFS FUSE mount. Blocks until the mount is verified live.
func (fm *FUSEManager) Mount() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if fm.cfg.JuiceFSBin == "" {
		return fmt.Errorf("juicefs binary not found")
	}

	// Create mount point directory
	if err := os.MkdirAll(fm.cfg.MountPoint, 0755); err != nil {
		return fmt.Errorf("create mount point: %w", err)
	}

	// Check if already mounted
	if fm.isMountedLocked() {
		log.Printf("[fuse] already mounted at %s", fm.cfg.MountPoint)
		return nil
	}

	// Unmount any stale mount first — and VERIFY the kernel actually released
	// the mountpoint before relaunching juicefs onto it. A `juicefs mount`
	// against a still-occupied mountpoint fails with a bare "exit status 1"
	// (mountpoint/device busy). The pre-2026-06-14 code unmounted once without
	// checking, so a watchdog remount after a wedge (kill juicefs → mount
	// entry survives a moment) would fail right here and leave the share down.
	if err := fm.ensureUnmountedLocked(3); err != nil {
		return err
	}

	// Pre-mount: if free space is tight on the cache volume but APFS is
	// holding lots of purgeable (Time Machine local snapshots, mostly),
	// reclaim it so JuiceFS doesn't immediately hit "space not enough on
	// device" warnings on the very first cache write. This is the diff
	// between the conservative statfs view and macOS's "important capacity"
	// view, which can be hundreds of GB on a laptop.
	//
	// Threshold: reclaim if free is below 50 GB. Conservative — won't fire
	// for users with healthy disks; will fire for the typical "97% full
	// video editor's laptop" we shipped this for.
	{
		free, _ := volumeFreeBytes("/")
		if free < 50*(1<<30) {
			if freed, _, _, err := ReclaimPurgeableSpace("/", 0); err != nil {
				jmlog.Warn("auto-reclaim failed (non-fatal)", "error", err.Error())
			} else if freed > 0 {
				jmlog.Info("auto-reclaim succeeded before mount",
					"freed_gb", fmt.Sprintf("%.1f", float64(freed)/(1<<30)))
			}
		}
	}

	// Cache-size policy (2026-06-08). Replaces the prior "max(configured, 85%
	// of disk)" auto-expansion, which IGNORED the user's configured size — it
	// treated the app setting as a FLOOR and grew the cache to fill the disk
	// (observed: 90 GB configured → 787 GB), with --free-space-ratio as the
	// only real limit. New policy:
	//   1. RESPECT the user's configured cache-size.
	//   2. Grow it ONLY as far as needed to keep the pinned set fully cached
	//      (fm.cfg.PinnedBytes) — so a 158 GB pinned project doesn't LRU-evict
	//      the blocks the user paid to download (the original concern), without
	//      blowing the cache past what's actually pinned.
	//   3. NEVER squeeze the cache/boot disk below a hard 10 GiB free floor — a
	//      near-full boot disk causes system-wide instability. The nominal
	//      cache-size is clamped to (total − 10 GiB), and --free-space-ratio is
	//      raised so JuiceFS dynamically keeps ≥10 GiB free at write time (the
	//      REAL guarantee: it also covers OTHER files filling the disk after
	//      mount, which a static cap cannot).
	const cacheFreeFloorBytes = cacheFreeFloorBytesConst
	if total, err := volumeTotalBytes("/"); err == nil && total > 0 {
		var configuredMiB int64
		fmt.Sscanf(fm.cfg.CacheSize, "%d", &configuredMiB)

		effectiveMiB, seededBytes := resolveCacheSizeMiB(configuredMiB, fm.cfg.PinnedBytes, total)
		if effectiveMiB > 0 && effectiveMiB != configuredMiB {
			fm.cfg.CacheSize = fmt.Sprintf("%d", effectiveMiB)
			jmlog.Info("cache-size resolved",
				"configured_gb", seededBytes>>30,
				"pinned_gb", fm.cfg.PinnedBytes>>30,
				"effective_gb", (effectiveMiB<<20)>>30,
				"disk_total_gb", total>>30,
				"reason", "max(configured, pinned) clamped to keep >=10GiB free")
		}

		// R-1 startup diagnostic: if the pinned set is larger than the disk can
		// physically keep resident (free space + the floor it must preserve),
		// it will never fully cache no matter how high cache-size goes —
		// JuiceFS evicts to honor --free-space-ratio. The live verdict (pin.
		// CapacityLoop) surfaces this to the user; logging it here makes the
		// condition obvious at mount time. Compares against FREE disk, not
		// total — the prior policy's blind spot.
		if free, ferr := volumeFreeBytes("/"); ferr == nil {
			if sustainable := free - cacheFreeFloorBytes; sustainable > 0 && fm.cfg.PinnedBytes > sustainable {
				jmlog.Warn("pinned set exceeds free disk — cannot stay fully cached offline",
					"pinned_gb", fm.cfg.PinnedBytes>>30,
					"free_gb", free>>30,
					"sustainable_gb", sustainable>>30,
					"shortfall_gb", (fm.cfg.PinnedBytes-sustainable)>>30,
					"action", "free disk space or unpin a folder")
			}
		}

		// Raise --free-space-ratio so JuiceFS's EVICTION floor sits ABOVE the
		// write spool's ADMISSION floor (B-2 — see resolveFreeSpaceRatio). The
		// old value was cacheFreeFloorBytes/total, i.e. JuiceFS only started
		// evicting below 10 GiB free while the spool already stopped admitting
		// at 20 GiB — so the spool blocked itself first and the cache never gave
		// the space back. resolveFreeSpaceRatio still guarantees the >= 10 GiB
		// free floor this line originally existed for. max() so we never weaken
		// an already-stricter configured ratio.
		if ratioArg, replace := resolveFreeSpaceRatioArg(fm.cfg.FreeSpaceRatio, total); replace {
			fm.cfg.FreeSpaceRatio = ratioArg
			// PUBLISH the floor we are actually mounting with. The pin capacity
			// verdict used to read a hard-coded 10 GiB, so once this ratio moved
			// the floor to ~30 GiB the verdict OVER-stated sustainable capacity by
			// the difference — the over-capacity banner under-warned, and
			// IsOverCapacity() gates the prefetcher re-warm, so the futile-churn
			// guard released late. Both errors were in the unsafe direction.
			pin.SetCacheFreeFloorBytes(int64(resolveFreeSpaceRatio(total) * float64(total)))
			jmlog.Info("free-space-ratio derived to keep the JuiceFS eviction floor above the spool floor",
				"ratio", ratioArg,
				"keeps_free_gb", int64(resolveFreeSpaceRatio(total)*float64(total))>>30,
				"spool_floor_gb", spoolFreeFloorBytesConst>>30,
				"disk_total_gb", total>>30)
		}
	}

	// Build juicefs mount command
	// The FUSE mount is an internal implementation detail — users interact
	// only with the NFS volume at /Volumes/zpool. We hide the FUSE mount:
	//   - Mount point is ~/.juicemount/fuse-internal (hidden dotdir, not /Volumes)
	//   - nobrowse: tells macOS not to show the volume in Finder sidebar
	args := []string{}
	// QA-34 Slice 2 (2026-05-25): --verbose adds debug-level logging
	// to ~/.juicefs/juicefs.log. Useful for investigating write-path
	// wedges. Gated behind the JM_FUSE_VERBOSE env var so production
	// users don't pay the cost (per-FUSE-op log overhead + tens of MB
	// of log data per write burst). Set JM_FUSE_VERBOSE=1 for debug
	// sessions.
	if os.Getenv("JM_FUSE_VERBOSE") != "" {
		args = append(args, "--verbose")
	}
	// Link-aware prefetch (#16 phase 2): juicefs's own sequential readahead is
	// the dominant prefetcher; pick --buffer-size/--prefetch from the measured
	// link so a metered/cellular mount doesn't pull whole files for a 4 KB touch,
	// while a 10GbE mount keeps blocks in flight. Medium == historical defaults.
	jfp := fm.linkAwareJuiceFSPolicy()
	// --buffer-size caps JuiceFS's in-memory DIRTY-data ceiling, which is
	// exactly what a shutdown/remount FlushAll must push to MinIO before the
	// mount worker exits (2026-07-10: a 1 GB slow-class buffer = a 4m17s flush
	// over a cellular tunnel = a ~3-min mount outage on any restart). The class
	// policy already shrinks it on slow/metered links; JM_JFS_BUFFER_MB is the
	// absolute field-override (32-8192; set it to the old per-class value to
	// revert). Writes land on the spool first, so a smaller buffer never risks
	// durability — it only backpressures the drainer to MinIO throughput.
	bufMB := jfp.BufferSizeMB
	if v := os.Getenv("JM_JFS_BUFFER_MB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 32 && n <= 8192 {
			bufMB = n
		}
	}
	args = append(args,
		"mount", fm.cfg.RedisURL, fm.cfg.MountPoint,
		"-d", // daemon mode
		"--no-usage-report",
		"--buffer-size", strconv.Itoa(bufMB),
		"--prefetch", strconv.Itoa(jfp.Prefetch),
	)
	args = append(args, juiceFSMountPlatformOptions(runtime.GOOS)...)
	// S1 (WAVE 1, RC-5): cap JuiceFS session readahead on a WAN link only.
	// juicefs's --max-readahead defaults to 8×BlockSize = 32 MiB, so a single
	// cold 4 KB preview touch pulls up to 32 MiB extra off the backend across
	// the tunnel. On the metered/slow class we set --max-readahead 1M to disable
	// session readahead entirely (~28 MiB less per cold first-touch). Mount-time
	// flag ONLY — zero NFS hot-path impact. Fast/Medium (10GbE/GbE) are left
	// UNSET so they keep the JuiceFS 32 MiB default (LAN behavior unchanged).
	// Kill-switch: JM_JFS_MAX_READAHEAD=0. Auto-reverts on 10GbE by class.
	if cls := netprofile.Default().Class(); cls == netprofile.ClassMetered || cls == netprofile.ClassSlow {
		if os.Getenv("JM_JFS_MAX_READAHEAD") != "0" {
			args = append(args, "--max-readahead", "1M")
		}
	}
	// Bind the Prometheus metrics endpoint EXPLICITLY so the bridge can scrape
	// `juicefs_blockcache_bytes` (the true on-disk block-cache size) for the
	// cache_used_bytes field. We do NOT rely on JuiceFS's :9567 default — that
	// collides with the `juicefs sync`/manager metrics addr (the documented
	// loopback-port collision); DefaultFUSEMetricsAddr is :9568. See FUSEConfig.
	metricsAddr := fm.cfg.FUSEMetricsAddr
	if metricsAddr == "" {
		metricsAddr = DefaultFUSEMetricsAddr
	}
	args = append(args, "--metrics", metricsAddr)
	// DATA-LOSS FIX (2026-06-13): --writeback is DISABLED by default.
	//
	// With --writeback, a close/fsync on a FUSE file returns once the bytes are
	// in JuiceFS's LOCAL writeback cache; the MinIO upload happens async and may
	// lag by seconds. The drainer fsyncs the FUSE dest and then deletes the
	// spool safety-copy + frees capacity (nfs/spool.go MarkDrainComplete),
	// trusting that fsync = durable. It is NOT: at that instant the only durable
	// copy is a dirty writeback-cache block. A crash / power-loss / cache
	// eviction / juicefs SIGKILL in that window destroys an already-"drained"
	// photo with no recovery source — the spool copy is gone. A kill-9 crash
	// test CONFIRMED this: 5 of 50 drained Canon CR3s came back corrupt in MinIO.
	//
	// The write SPOOL already provides the local write buffer + burst absorption
	// that --writeback was added for (writes land on the spool SSD at local
	// speed; the drainer uploads in the background). WITHOUT --writeback the
	// drainer's close waits for the real MinIO PUT, so the data is confirmed
	// durable before the spool copy is deleted. Drain is then honestly
	// upload-bound, which is correct — you cannot acknowledge a photo as stored
	// faster than you can durably store it; the spool + capacity backpressure
	// pace ingest to that rate. Re-enable only if you accept the crash window:
	if os.Getenv("JM_FUSE_WRITEBACK") == "1" {
		args = append(args, "--writeback")
	}

	// Slice H — WAN mode. JuiceFS default --max-uploads is 20; on a
	// high-RTT path (Tailscale, cellular, distant MinIO) 20 concurrent
	// PUTs is the throughput cap and the upload pipe stays
	// underutilized. JM_WAN_MODE=1 bumps it to 64 — bandwidth-delay-
	// product math: at 100 ms RTT, 20×4MB chunks = 80 MB in flight
	// (~6 Gbps if uplink could deliver); 64×4MB = 256 MB in flight,
	// enough headroom for typical home/cellular uplinks where the
	// pipe is the real cap, not concurrency.
	//
	// Off by default. On a LAN, 20 is already over-saturating, more
	// makes no difference and may worsen tail latency.
	if os.Getenv("JM_WAN_MODE") == "1" {
		args = append(args, "--max-uploads", "64")
	}
	// Metadata caching. JuiceFS defaults the attr/entry/dir-entry caches to 1s,
	// so any cold or 1s-expired FUSE metadata access re-validates against Redis
	// — a full RTT, and the dominant navigation cost (≈82% of FUSE op time was
	// `lookup` in profiling). A modest cache (default 5s on LAN, 60s on WAN)
	// cuts that round-trip chatter — which ALSO shrinks the window in which a
	// transient backend/link blip can wedge a FUSE op (the cause of the Finder
	// "not responding" reports). `--negative-entry-cache` caches NEGATIVE
	// lookups so the storm of `._*` AppleDouble probes (~18k ENOENT observed)
	// stops re-hitting Redis on every miss.
	//
	// SAFETY (audited 2026-06-08): JuiceMount serves Stat/ReadDir from its
	// SQLite mirror first — FUSE is fallback-only (ARCHITECTURE:152) — and the
	// kernel NFS client already serves attrs up to actimeo=3600s stale, so a
	// few-seconds FUSE cache changes nothing user-visible. The only consumer of
	// FUSE metadata freshness is the phantom-purge / reconcile-prune Lstat
	// (handler.go phantom-purge, redis.go prune): a longer cache only makes
	// those LAZIER, the strictly-safe direction — those were OVER-aggressive
	// bugs (QA-19/30/32/35), never under-aggressive. JuiceMount's own writes
	// update SQLite directly and don't depend on FUSE-cache freshness. 60s is
	// reserved for WAN to keep the worst-case phantom-linger ≤5s on LAN.
	// Tunable via JM_META_CACHE_SECS.
	// 2026-07-28 — THE NAVIGATION FIX, and why it is unconditional.
	//
	// The 60s branch below used to be gated on JM_WAN_MODE=1. That knob is read
	// in six production paths and SET IN NOTHING BUT TESTS — no Swift setenv, no
	// LSEnvironment entry, no shell wrapper. So every shipping build has run
	// with a 5s TTL on ALL FOUR caches, and the WAN fix written here has never
	// once been active for a user. On a LAN that is invisible (5s revalidation
	// costs ~0.4ms). On cellular each revalidation is a ~250ms round trip, which
	// is exactly the reported symptom: navigating a directory is instant while
	// OFFLINE (never touches the network) and terrible while ONLINE (re-validates
	// against Redis every 5 seconds per entry).
	//
	// It is unconditional rather than link-conditional because these flags are
	// fixed AT MOUNT TIME: juicefs cannot be re-tuned when the link changes, so
	// a class-gated value would just bake in whatever the link looked like at
	// startup and be wrong after the first WiFi<->cellular switch. Picking one
	// value that is correct everywhere beats picking the wrong one adaptively.
	//
	// SAFETY — unchanged from the 2026-06-08 audit above, which already cleared
	// the 60s value: JuiceMount serves Stat/ReadDir from its own mirror first and
	// FUSE is fallback-only, the kernel NFS client already serves attrs up to
	// actimeo=3600s stale, and the only consumer of FUSE metadata freshness is
	// the phantom-purge / reconcile-prune Lstat, which a longer cache makes
	// LAZIER — the strictly-safe direction, and those were over-aggressive bugs
	// (QA-19/30/32/35), never under-aggressive.
	//
	// SPLIT, rather than one scalar for all four (audit P4):
	//   entry / dir-entry 60s — the navigation win; these are what a directory
	//     listing revalidates, and staleness here only delays a NEWLY ADDED file
	//     becoming visible. Leland explicitly accepted that trade for throttled
	//     links ("compromise of delay before new files show up ... is fine").
	//   attr 10s — deliberately NOT 60s. attr-cache backs LiveSize(), the
	//     anti-staleness authority for #85 (OpenLoupe verified-offload reads a
	//     transient short size after a big write, hashes it, and discards a good
	//     file). A 60s attr cache widens that window by 6x; 10s still removes
	//     most revalidation chatter.
	//   negative 5s — unchanged. A stale NEGATIVE is the one users experience as
	//     "I just created this file and it isn't there", which is far more
	//     confusing than a stale positive, and the ._* ENOENT storm this was
	//     added for is already absorbed at 5s.
	//
	// JM_META_CACHE_SECS still overrides ALL FOUR together (documented escape
	// hatch); the per-cache vars below allow finer tuning without it.
	// --fast-statfs: answer statfs from juicefs's LOCAL counters instead of
	// querying the metadata service. Default in juicefs is false, and we have
	// never passed it, so EVERY free-space query has been a Redis round trip.
	//
	// That is not a rare call. Profiling one real cellular session measured
	// StatFS 1,727 times at 230ms average — 397 seconds, 6.6 minutes, spent
	// reporting how much space is free. Finder polls it while a window is open,
	// `df` hits it, and the app's own health surface reads it. On a LAN each
	// call is sub-millisecond and invisible; on cellular it is pure latency
	// tax on an operation nobody is waiting for the exact answer to.
	//
	// SAFETY: the cost is a slightly stale free-space figure. Nothing in
	// JuiceMount makes a correctness decision from statfs — the pin-store
	// capacity guard (R-1) measures the LOCAL cache volume with syscall.Statfs
	// on the Mac's own disk, not the JuiceFS mount, so it is unaffected. A
	// stale byte count in a Finder window is exactly the class of staleness
	// already accepted for throttled links. Kill switch JM_FAST_STATFS=0.
	if os.Getenv("JM_FAST_STATFS") != "0" {
		args = append(args, "--fast-statfs")
	}

	// --open-cache: reuse an open file WITHOUT re-checking for updates.
	//
	// THE PRIZE. Measured on a real cellular link 2026-07-29: the FIRST open of
	// a file costs 1,226-5,660ms while the SECOND costs 24-36ms — a ~66x gap,
	// and the cost is per-FILE, not per-byte (5.7 KB costs the same as 4.4 MB;
	// a directory is 26ms). juicefs defaults this to 0s and we have never
	// passed it, so every open re-validates against Redis. That re-validation
	// IS the gap, and it is redundant for us: we already run a coherence engine
	// (the SQLite/RAM mirror + the Redis keyspace push feed).
	//
	// DEFAULT OFF, DELIBERATELY. Two facts make this opt-in rather than on:
	//
	//  1. juicefs exposes NO external cache invalidation — checked the whole
	//     subcommand surface (format/config/quota/destroy/gc/fsck/restore/dump/
	//     load/status/stats/profile/info/debug/summary/mount/umount/gateway/
	//     webdav/bench/objbench/warmup/rmr/sync). Our push feed can drop OUR
	//     pooled fd (R1) but cannot make juicefs re-check inside its window. The
	//     TTL is therefore a HARD staleness bound with no escape hatch.
	//  2. The exposure is a stale SLICE MAP — wrong bytes from an existing file,
	//     not a stale listing. That is the family this codebase has been burned
	//     by three times (#18 torn reads, #104 zero-tailed black frames, the
	//     membuf stale-partial image). It is NOT covered by the "delay before
	//     new files appear" trade that was accepted for throttled links.
	//
	// So: build the lever, measure the win on the real link, THEN decide the
	// default with data — rather than defaulting it on from reasoning, which is
	// how the metadata-TTL change earned a measurement that showed it did
	// nothing.
	//
	// Sizing when enabled: SHORT. The measured repeat-opens were seconds apart
	// (a browse-and-preview burst), so 5-10s captures nearly all of the 66x
	// while keeping the window too short for a farm/ClipLogger write to land
	// inside it. Do not reach for 30s+ without a multi-writer test.
	//
	// JM_OPEN_CACHE="" (unset) = OFF, today's behavior byte-identically.
	// JM_OPEN_CACHE="5s"       = pass --open-cache 5s.
	if v := fm.openCacheTTL(); v != "" {
		args = append(args, "--open-cache", v)
	}

	attrTTL, entryTTL, dirEntryTTL, negTTL := metaCacheTTLs()
	args = append(args,
		"--attr-cache", attrTTL,
		"--entry-cache", entryTTL,
		"--dir-entry-cache", dirEntryTTL,
		"--negative-entry-cache", negTTL,
	)
	if v := os.Getenv("JM_MAX_UPLOADS"); v != "" {
		// Direct override wins over WAN mode for operators that want
		// to tune themselves.
		args = append(args, "--max-uploads", v)
	}
	if fm.cfg.CacheDir != "" {
		args = append(args, "--cache-dir", fm.cfg.CacheDir)
	}
	if fm.cfg.CacheSize != "" {
		args = append(args, "--cache-size", fm.cfg.CacheSize)
	}
	if fm.cfg.FreeSpaceRatio != "" {
		args = append(args, "--free-space-ratio", fm.cfg.FreeSpaceRatio)
	}
	if fm.cfg.BucketOverride != "" {
		// Override the Redis-stored bucket URL for this Mac client only.
		// See FUSEConfig.BucketOverride comment for rationale.
		//
		// Arg-ordering note (2026-05-26): `--bucket` lands after the
		// positional <meta-url> <mountpoint> args. JuiceFS 1.3.1 accepts
		// flags in this position (verified empirically via direct CLI
		// invocation — `juicefs mount redis://... /mnt/test --bucket
		// http://<server-ip>:30151/zpool` mounts cleanly). If a future
		// JuiceFS version tightens its parser, this is the line to move
		// — relocate the entire conditional-flag block to BEFORE the
		// `mount, fm.cfg.RedisURL, fm.cfg.MountPoint` append above.
		args = append(args, "--bucket", fm.cfg.BucketOverride)
	}

	// Redact secrets from the log line. Currently only the bucket URL
	// can carry creds (user:pass@host form). Build a sanitized copy of
	// args just for logging; the actual exec.Command still gets the
	// real values.
	logArgs := make([]string, len(args))
	copy(logArgs, args)
	for i, a := range logArgs {
		if a == "--bucket" && i+1 < len(logArgs) {
			logArgs[i+1] = redactURLCreds(logArgs[i+1])
		}
	}
	log.Printf("[fuse] mounting JuiceFS: %s %s", fm.cfg.JuiceFSBin, strings.Join(logArgs, " "))
	jmlog.Info("mounting juicefs", "bin", fm.cfg.JuiceFSBin, "args", strings.Join(logArgs, " "))

	// `juicefs mount -d` daemonizes and returns quickly in the happy path,
	// but can hang on certain backend failures (e.g. unreachable Redis
	// during Lua init). Bounded at 30 s so a stuck launch can't park the
	// caller forever.
	launchCtx, launchCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer launchCancel()
	cmd := exec.CommandContext(launchCtx, fm.cfg.JuiceFSBin, args...)
	cmd.Stdout = os.Stdout
	// Tee stderr: still stream to os.Stderr (the log tail) but also capture it
	// so a failed launch's RETURNED error carries juicefs's actual reason
	// (e.g. "mountpoint is not empty", "address already in use") instead of a
	// bare "exit status 1" that tells the user nothing (2026-06-14).
	var stderrBuf bytes.Buffer
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)

	if err := cmd.Run(); err != nil {
		noteMountFailure()
		if launchCtx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("juicefs mount: timed out after 30s (backend unreachable?)")
		}
		if reason := lastNonEmptyLine(stderrBuf.String()); reason != "" {
			return fmt.Errorf("juicefs mount: %w: %s", err, reason)
		}
		return fmt.Errorf("juicefs mount: %w", err)
	}

	// Wait for the mount to become live (juicefs mount -d returns before FUSE is ready)
	if err := fm.waitForMount(fm.mountVerifyTimeout()); err != nil {
		noteMountFailure()
		return fmt.Errorf("mount verification: %w", err)
	}
	kextBlockedFlag.Store(false)

	log.Printf("[fuse] JuiceFS mounted at %s", fm.cfg.MountPoint)

	// Suppress Spotlight indexing on the hidden FUSE mount
	os.WriteFile(fm.cfg.MountPoint+"/.metadata_never_index", nil, 0644)

	// Up-front disk-space sanity check. If the cache volume is so full that
	// JuiceFS will refuse to cache, we want the user to KNOW now — before
	// they try to play media and wonder why everything is slow.
	if msg := checkCacheVolumeHealth(fm.cfg); msg != "" {
		jmlog.Warn("juicefs cache health concern", "detail", msg)
	}

	// Log the purgeable-vs-free breakdown so the user can see at a glance
	// how much APFS is hiding from raw statfs(). On a 1 TB disk that says
	// 38 GB free, macOS often has 280+ GB available for "important use" if
	// the requesting app is willing to take it. JuiceFS uses raw statfs and
	// sees only the conservative number — so we report both.
	logVolumeCapacityBreakdown("/")

	// Log final cache config so any future "why is the cache so small"
	// question is answered by the same log that shows the volume size.
	jmlog.Info("juicefs cache config",
		"cache_size_mb", fm.cfg.CacheSize,
		"free_space_ratio", fm.cfg.FreeSpaceRatio,
		"cache_dir", fm.cfg.CacheDir)

	// Tail JuiceFS's daemon log into our structured logger so warnings like
	// "space not enough on device, upload it directly" are visible to the
	// user instead of buried in ~/.juicefs/juicefs.log. Bound to fm.stopCh
	// so the goroutine exits cleanly when the FUSEManager is stopped
	// (previously leaked on every Stop, holding the open file handle).
	go fm.tailJuiceFSLog()

	return nil
}

// juiceFSMountPlatformOptions returns only options supported by the host FUSE
// implementation. nobrowse maps to macOS's MNT_DONTBROWSE flag; Linux
// fusermount3 rejects it and prevents JuiceFS from mounting at all.
func juiceFSMountPlatformOptions(goos string) []string {
	if goos == "darwin" {
		return []string{"-o", "nobrowse"}
	}
	return nil
}

// checkCacheVolumeHealth returns a non-empty warning string if the volume
// hosting the cache directory is in a state where JuiceFS will silently
// stop caching (free space below ratio threshold, or directory unwritable).
func checkCacheVolumeHealth(cfg FUSEConfig) string {
	cacheDir := cfg.CacheDir
	if cacheDir == "" {
		// JuiceFS default: ~/.juicefs/cache
		home, _ := os.UserHomeDir()
		cacheDir = filepath.Join(home, ".juicefs", "cache")
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(cacheDir, &stat); err != nil {
		// Try the parent if cacheDir doesn't exist yet
		if err := syscall.Statfs(filepath.Dir(cacheDir), &stat); err != nil {
			return fmt.Sprintf("cannot stat cache dir %s: %v", cacheDir, err)
		}
	}
	totalBytes := uint64(stat.Bsize) * stat.Blocks
	freeBytes := uint64(stat.Bsize) * stat.Bavail
	if totalBytes == 0 {
		return ""
	}
	freeRatio := float64(freeBytes) / float64(totalBytes)
	threshold := 0.01
	if cfg.FreeSpaceRatio != "" {
		// Parse our configured ratio as a number — we want to know if the
		// disk is below it. Crude parsing on purpose: invalid → use default.
		var r float64
		_, err := fmt.Sscanf(cfg.FreeSpaceRatio, "%f", &r)
		if err == nil && r > 0 && r < 1 {
			threshold = r
		}
	}
	if freeRatio < threshold {
		return fmt.Sprintf(
			"cache volume %s is %.1f%% free (%.1f GiB), below the %.1f%% threshold "+
				"— JuiceFS will skip caching and read every block from S3. "+
				"Free up disk space or pass a smaller --free-space-ratio.",
			cacheDir, freeRatio*100, float64(freeBytes)/(1<<30),
			threshold*100)
	}
	return ""
}

// tailJuiceFSLog watches the JuiceFS daemon log file for new lines and
// promotes WARNING / ERROR records into jmlog so the user can see them in
// the same JSON stream as everything else. Aggregates the chatty
// "space not enough" message — emit once per minute with a count instead
// of flooding the log with thousands of identical entries.
//
// Exits cleanly when fm.stopCh closes. Previously a package-level
// function with no stop signal — leaked the goroutine + open file handle
// on every FUSEManager.Stop(), accumulating across Stop/Start cycles.
func (fm *FUSEManager) tailJuiceFSLog() {
	home, err := os.UserHomeDir()
	if err != nil {
		return
	}
	logPath := filepath.Join(home, ".juicefs", "juicefs.log")
	// Wait for the file to appear (juicefs daemon may not have created it
	// yet). Interruptible by fm.stopCh so Stop() doesn't have to wait the
	// full 30 s to reap this goroutine.
	for i := 0; i < 30; i++ {
		if _, err := os.Stat(logPath); err == nil {
			break
		}
		select {
		case <-fm.stopCh:
			return
		case <-time.After(1 * time.Second):
		}
	}
	f, err := os.Open(logPath)
	if err != nil {
		jmlog.Debug("juicefs log tail: open failed", "path", logPath, "error", err.Error())
		return
	}
	defer f.Close()
	// Seek to end so we don't replay history.
	if _, err := f.Seek(0, 2); err != nil {
		return
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)

	type aggKey struct{ pattern string }
	type aggState struct {
		count     int
		firstAt   time.Time
		lastFlush time.Time
	}
	agg := map[aggKey]*aggState{}
	const flushEvery = 60 * time.Second
	flushAgg := func() {
		now := time.Now()
		for k, st := range agg {
			if st.count > 0 && now.Sub(st.lastFlush) >= flushEvery {
				jmlog.Warn("juicefs warning (aggregated)",
					"pattern", k.pattern,
					"count_in_window_sec", int(flushEvery.Seconds()),
					"count", st.count,
					"since", st.firstAt.Format(time.RFC3339))
				st.count = 0
				st.lastFlush = now
			}
		}
	}

	for {
		// Check stop before each scan pass so we don't block on a
		// scanner.Scan that's waiting on a quiet log file.
		select {
		case <-fm.stopCh:
			return
		default:
		}
		for scanner.Scan() {
			line := scanner.Text()
			// Two interesting tokens: <WARNING> and <ERROR>
			isWarn := strings.Contains(line, "<WARNING>:")
			isErr := strings.Contains(line, "<ERROR>:")
			if !isWarn && !isErr {
				continue
			}
			// Aggregate the disk-full pattern; surface others immediately.
			if strings.Contains(line, "space not enough on device") {
				k := aggKey{"space not enough on device, upload directly"}
				st := agg[k]
				if st == nil {
					st = &aggState{firstAt: time.Now(), lastFlush: time.Now()}
					agg[k] = st
				}
				st.count++
				continue
			}
			level := jmlog.LevelWarn
			if isErr {
				level = jmlog.LevelError
			}
			if level == jmlog.LevelError {
				jmlog.Error("juicefs", "raw", line)
			} else {
				jmlog.Warn("juicefs", "raw", line)
			}
		}
		flushAgg()
		// scanner exited — file may have rotated or be at EOF; sleep + retry.
		if err := scanner.Err(); err != nil {
			jmlog.Debug("juicefs log tail: scanner error", "error", err.Error())
			return
		}
		// Interruptible sleep so Stop() reaps us within 2 s instead of
		// waiting for the next read attempt.
		select {
		case <-fm.stopCh:
			return
		case <-time.After(2 * time.Second):
		}
	}
}

// StartMonitor begins a background goroutine that checks mount health
// and remounts if the FUSE mount dies.
func (fm *FUSEManager) StartMonitor() {
	go fm.monitorLoop()
}

// Stop unmounts JuiceFS and stops the monitor.
func (fm *FUSEManager) Stop() {
	close(fm.stopCh)

	// Kill juicefs FIRST — lock-free, before the monitor join and before
	// fm.mu.Lock() below. 2026-07-15 teardown-deadlock fix: the watchdog's
	// escalation Mount() holds fm.mu across an un-abortable `juicefs mount` +
	// umount (up to the verify budget, longer on a wedged mount). If the user
	// hits "Stop everything" during a remount, the ONLY juicefs kill (inside
	// unmountLocked) sits behind that contended fm.mu.Lock — so the whole
	// NFSServerShutdown blocked, the daemon stayed alive, and because Swift
	// flips state=.idle only AFTER this cgo call returns, the UI froze,
	// couldn't relaunch, and a force-quit orphaned the mount. Killing the
	// daemon up front makes that in-flight mount/umount return at once, so
	// Mount() releases fm.mu and the Lock() below can't deadlock behind it —
	// and juicefs is guaranteed dead even if the steps below stall. A daemon
	// the watchdog's last tick may re-spawn in the tiny window before it sees
	// stopCh is caught by unmountLocked's own (idempotent) kill.
	fm.killJuiceFSProcesses()

	// Bound the wait for monitorLoop to exit. Even with iteration-2's
	// bounded `mount` syscall (5 s context), one tick can still take that
	// long if it fires while we're trying to stop. Race the join against
	// a 10 s deadline so callers (NFSServerShutdown → fuse.Stop) never
	// park indefinitely. If the monitor doesn't exit cleanly in time, we
	// proceed to unmount anyway — the goroutine will become a zombie
	// rather than blocking the user-visible Stop button.
	select {
	case <-fm.done:
	case <-time.After(10 * time.Second):
		jmlog.Warn("FUSEManager.Stop: monitor goroutine didn't exit in 10s — proceeding with unmount anyway")
	}

	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.unmountLocked()
	log.Printf("[fuse] stopped")
}

// IsMounted returns true if the FUSE mount is live and responsive.
func (fm *FUSEManager) IsMounted() bool {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return fm.isMountedLocked()
}

// isMountedLocked checks if the FUSE mount is live. Must be called with fm.mu held.
//
// CRITICAL: `mount` (which calls `getfsstat()`) hangs in the kernel if the
// mount table contains a wedged entry (server gone, kernel mount retained).
// Without the context timeout, this function — called every 10 s by the
// monitor loop under fm.mu — wedges the FUSEManager lock forever, parking
// every other caller (Stop, IsMounted, Mount) behind it. That was the
// "click menu → app freezes" pattern.
func (fm *FUSEManager) isMountedLocked() bool {
	// Check 1: appears in macOS mount table as a FUSE mount.
	// Hard-bounded at 5 s. On timeout, treat as "unknown" (return false) so
	// the monitor loop doesn't pin fm.mu forever. The cost of a false
	// negative is a respawn attempt the user will see in the log; the cost
	// of hanging is the entire UI.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := mounttable.Output(ctx)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			jmlog.Warn("mount table query timed out — likely a wedged mount entry blocking getfsstat",
				"mount_point", fm.cfg.MountPoint,
				"hint", "treating as not-mounted; reboot or `sudo umount -f -t nfs` may be needed")
		}
		return false
	}
	if !strings.Contains(string(out), fm.cfg.MountPoint) {
		return false
	}

	// Check 2: actually responsive — try listing the directory. A stale FUSE
	// mount (dead daemon) hangs on any fs op. Kept as a root readdir (NOT the
	// .config probe) because this same check gates LAUNCH readiness via
	// waitForMount: right after `juicefs mount -d` the dir is listable before the
	// .config control file is served, so a .config probe here false-fails the
	// launch verification. Pure-read, no side-effect umount (QA-34 Slice 2): the
	// monitorLoop owns the remount decision with its failure tolerance.
	//
	// task #72: this 5s readdir DOES flip to "stale" under backend slowness /
	// cold-start warm, but the DESTRUCTIVE remount is gated by the escalation-
	// confirm probe (mountResponsiveWithin), which reads the in-memory .config and
	// so never trips on data-path slowness — a slow-but-alive mount is NEVER
	// SIGKILLed even though this 5s readdir over-reports "stale" under load. (The
	// residual cosmetic false "degraded" during heavy load is acceptable; the
	// watchdog defers to the .config confirm, never destroys on this 5s probe. See
	// mountResponsiveWithin for why .config beat a generous readdir here.)
	done := make(chan bool, 1)
	go func() {
		_, err := os.ReadDir(fm.cfg.MountPoint)
		done <- (err == nil)
	}()
	select {
	case ok := <-done:
		return ok
	case <-time.After(5 * time.Second):
		log.Printf("[fuse] mount at %s is unresponsive (stale); reporting unhealthy. Remount decision deferred to monitorLoop.", fm.cfg.MountPoint)
		return false
	}
}

// fuseConfirmProbeTimeout is the GENEROUS window used to CONFIRM a mount is
// truly wedged before the destructive last-resort remount. isMountedLocked's
// 5s readdir flips to "stale" whenever the mount is merely SLOW — e.g. while a
// metadata reconcile or a backend blip contends the JuiceFS→Redis path — and
// under sustained slowness that accumulates to FUSEStaleEscalateTicks and
// SIGKILLs a perfectly-alive juicefs, which is exactly what drove the macFUSE
// remount thrash (2026-06-14, surfaced re-deploying mid-reconcile). Confirming
// with a long timeout distinguishes "slow" from "dead": a slow-but-alive mount
// answers within 25s; a genuinely wedged one never does.
const fuseConfirmProbeTimeout = 25 * time.Second

// mountResponsiveWithin returns true (responsive → DEFER the destructive remount)
// if juicefs answers a bounded .config read inside timeout. Safe to call without
// fm.mu (touches no fm state).
//
// DELIBERATELY a .config (LIVENESS) probe, NOT a data-path readdir — empirically
// grounded (2026-06-29). Three iterations were tried as the escalation-confirm:
//
//  1. root readdir (original) — false-SIGKILLs a slow-but-alive mount whenever a
//     readdir exceeds the confirm window under transient load.
//  2. root readdir gated by the GENEROUS 25s/45s/90s class window (the "obvious"
//     fix an adversarial review recommended) — STILL false-escalated: during the
//     cold-start metadata-sync burst the root readdir momentarily exceeded even
//     25s, so the watchdog SIGKILLed a PERFECTLY HEALTHY juicefs (verified
//     seconds later with escalation suppressed: readdir 130ms, reads 13ms, full
//     bytes) and the follow-up remount then failed ("mount not ready after 15s")
//     → a stuck no-mount loop. Destroying healthy mounts is the exact regression
//     tasks #13/#72 exist to PREVENT, so this is strictly worse than (3).
//  3. .config (this) — juicefs serves it from memory with NO backend round-trip,
//     so it answers whenever the process + FUSE session are alive and hangs only
//     on a TOTAL session death. It never false-trips on data-path slowness, so it
//     never SIGKILLs a healthy mount; in the common case .config-healthy tracks
//     data-path-healthy (verified: readdir 130ms / reads 13ms while .config OK).
//
// KNOWN LIMITATION (tracked as a #72 follow-up): a .config probe cannot see the
// RARE "juicefs alive but backend CONNECTION wedged" shape (.config answers while
// readdir hangs — the 2026-06-01 event). There this defers and the mount does not
// auto-recover; manual app restart is the recovery. Accepted for now because
// (a) it is rare, (b) the escalation's remount is itself currently broken ("mount
// not ready after 15s"), so detecting the wedge would not recover it anyway, and
// (c) the alternative (readdir-confirm) destroys HEALTHY mounts in the COMMON
// case. The proper fix is a SEPARATE sustained-data-path-stall counter (escalate
// only after readdir hangs for MINUTES while .config stays live) PLUS a fix for
// the broken remount — both deferred to the follow-up, not rushed into a release.
func (fm *FUSEManager) mountResponsiveWithin(timeout time.Duration) bool {
	// This is the LAST gate before a destructive SIGKILL+remount, so it must
	// return true on ANY sign of life. Race two liveness signals:
	//   1. reading .config (the in-memory control file) — content OR a prompt
	//      error both mean juicefs ANSWERED (alive);
	//   2. a root readdir succeeding — the DATA path serving.
	// Either returning ⇒ alive ⇒ do NOT remount. Only a genuine wedge hangs
	// BOTH past the timeout. 2026-07-15: .config alone false-negatived during
	// cold-warm churn — juicefs serves the data-path readdir before it serves
	// the .config control file, so a .config-only probe could hang and greenlight
	// a SIGKILL of a mount that was listing fine (the remount-thrash trigger on a
	// large-local-cache box). A serving readdir is definitive proof of life.
	done := make(chan bool, 2)
	go func() {
		_, _ = os.ReadFile(fm.cfg.MountPoint + "/.config")
		done <- true
	}()
	go func() {
		if _, err := os.ReadDir(fm.cfg.MountPoint); err == nil {
			done <- true // a successful readdir is unambiguous liveness
		}
	}()
	select {
	case ok := <-done:
		return ok
	case <-time.After(timeout):
		return false
	}
}

// runBoundedCommand fires a shell command with a hard time limit and reaps
// the process. Logs on timeout. Used for fire-and-forget cleanup work that
// previously called `.Start()` without ever calling `.Wait()` — leaking
// zombies and silently failing.
func runBoundedCommand(timeout time.Duration, name string, args ...string) {
	// TRULY bounded (2026-07-10): the old CommandContext(...).Run() killed
	// the child at the deadline but then BLOCKED waiting to reap it — and a
	// child parked in an uninterruptible kernel call (diskutil against a
	// wedged diskarbitrationd, umount of a haunted mountpoint) ignores
	// SIGKILL until its syscall returns, so the "bounded" call itself hung
	// (observed live: 30s-bounded diskutils surviving 20+ minutes while the
	// watchdog's remount crawled at ~65s/attempt). Now: kill at the deadline
	// and RETURN; a reaper goroutine collects the child whenever the kernel
	// finally releases it.
	cmd := exec.Command(name, args...)
	if err := cmd.Start(); err != nil {
		jmlog.Debug("bounded command failed to start",
			"cmd", name, "args", strings.Join(args, " "), "error", err.Error())
		return
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			jmlog.Debug("bounded command failed",
				"cmd", name, "args", strings.Join(args, " "), "error", err.Error())
		}
	case <-timer.C:
		_ = cmd.Process.Kill()
		jmlog.Warn("bounded command timed out — killed, not waiting for reap",
			"cmd", name, "args", strings.Join(args, " "),
			"timeout_sec", int(timeout.Seconds()))
		// The Wait goroutine reaps the child when (if) the kernel releases
		// it; an unkillable D-state child must never pin the caller.
	}
}

// waitForMount polls until the mount is live or timeout expires.
func (fm *FUSEManager) waitForMount(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fm.isMountedLocked() {
			return nil
		}
		// U5 review fix: with class-widened budgets (45s/90s) a failing
		// verify would otherwise pin fm.mu through Mount() and park the
		// user's Stop/quit for the whole window — abort promptly on Stop.
		select {
		case <-fm.stopCh:
			return fmt.Errorf("mount verification aborted: manager stopping")
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("mount not ready after %v", timeout)
}

// killJuiceFSProcesses SIGKILLs every `juicefs mount` process bound to this
// mountpoint. It takes NO lock and each shell-out is time-bounded, so it is safe
// to call from Stop() BEFORE fm.mu is held — killing the daemon is what releases
// the FUSE mount, and doing it lock-free breaks the teardown deadlock (see Stop).
// Returns the number killed. Idempotent: a second call finds nothing.
func (fm *FUSEManager) killJuiceFSProcesses() int {
	procs := juiceFSMountProcessIDs(fm.cfg.MountPoint)
	killed := 0
	for _, pid := range procs {
		if pid != "" {
			killCtx, killCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = exec.CommandContext(killCtx, "kill", "-9", pid).Run()
			killCancel()
			killed++
		}
	}
	if killed > 0 {
		// Visibility into the one destructive action in this subsystem. This
		// pattern also matches juicefs's own `-d` supervisor, so a kill here
		// removes JuiceFS's self-heal too — only acceptable on an explicit
		// remount/stop, never as a reaction to a transient stall.
		jmlog.Warn("unmount: SIGKILL'd juicefs processes for mountpoint",
			"count", killed, "mountpoint", fm.cfg.MountPoint)
	}
	return killed
}

// unmountLocked forcibly unmounts the FUSE mount. Must be called with fm.mu held.
// Every shell-out is time-bounded so a wedged kernel state can't pin fm.mu.
func (fm *FUSEManager) unmountLocked() {
	// Kill any lingering JuiceFS mount processes for this mount point first.
	// Killing the process lets the kernel release the mount.
	fm.killJuiceFSProcesses()
	time.Sleep(500 * time.Millisecond)

	// FAST PATH (2026-07-10): when the mountpoint is NOT in the kernel mount
	// table there is nothing to tear down — the diskutil + umount calls were
	// pure waste that burned their bounds against the (possibly wedged)
	// diskarbitrationd. This was the dominant cost of the 12:01→12:04 organic
	// remount (vs 35s when the teardown had real work): juicefs had died, the
	// mountpoint was already a plain dir, and every ensureUnmounted attempt
	// still crawled through both commands. The juicefs-process kill above
	// already ran (ensureUnmountedLocked depends on it even when the table is
	// clear).
	if !fm.stillInMountTable() {
		jmlog.Debug("unmount: mountpoint not in the kernel mount table — teardown skipped",
			"mountpoint", fm.cfg.MountPoint)
		return
	}

	// QA-34 Slice 2 (2026-05-25): umount is now BLOCKING on this
	// goroutine with a 60s budget. Pre-fix it was fire-and-forget,
	// which created a race window where the caller (typically
	// Mount() about to relaunch juicefs) could attempt to attach
	// to a mount point the kernel had not yet released. At 15 s
	// budget the window was small; at the new 60 s budget required
	// to absorb realistic juicefs fsync durations (15-30 s), the
	// race window would have been catastrophic.
	//
	// Holding fm.mu across the umount is fine: this is the
	// explicit remount path. By definition the world is waiting
	// for the mount transition; no other useful work can complete
	// while the FUSE mount is half-gone.
	//
	// runBoundedCommand handles the timeout + reaping internally.
	done := make(chan struct{})
	go func() {
		// `diskutil unmount force` is more reliable than `umount -f` for a
		// WEDGED macFUSE mount: `umount -f` of a dead-daemon mount can hang in
		// the kernel indefinitely (observed 2026-06-01 — only diskutil cleared
		// it), whereas diskutil tears the FUSE device down cleanly. Try
		// diskutil first; fall back to `umount -f` only if the entry survives.
		runBoundedCommand(30*time.Second, "diskutil", "unmount", "force", fm.cfg.MountPoint)
		if fm.stillInMountTable() {
			runBoundedCommand(30*time.Second, "umount", "-f", fm.cfg.MountPoint)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(65 * time.Second):
		// Safety net: even if a bounded command hangs (shouldn't, each has its
		// own ctx deadline), don't pin fm.mu forever.
		jmlog.Warn("unmountLocked: unmount goroutine did not return in 65s, proceeding anyway")
	}
}

// ensureUnmountedLocked runs unmountLocked and verifies the mountpoint left the
// kernel mount table, retrying up to `attempts` times with a short backoff.
// Returns an error only if the entry survives every attempt — in which case
// relaunching juicefs onto the busy mountpoint would fail with a bare
// "exit status 1", so the caller should surface this descriptive error instead
// of churning on a doomed launch. Must be called with fm.mu held.
//
// Always runs unmountLocked at least once (it also SIGKILLs any lingering
// juicefs processes for this mountpoint, which we want even when the kernel
// table is already clear).
func (fm *FUSEManager) ensureUnmountedLocked(attempts int) error {
	if attempts < 1 {
		attempts = 1
	}
	for i := 0; i < attempts; i++ {
		fm.unmountLocked()
		if !fm.stillInMountTable() {
			if i > 0 {
				jmlog.Info("ensureUnmounted: mountpoint cleared",
					"attempt", i+1, "mountpoint", fm.cfg.MountPoint)
			}
			return nil
		}
		jmlog.Warn("ensureUnmounted: mountpoint still in mount table after unmount",
			"attempt", i+1, "of", attempts, "mountpoint", fm.cfg.MountPoint)
		// Linear backoff: give the kernel a moment to drain in-flight FUSE
		// teardown before the next forced attempt.
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	if fm.stillInMountTable() {
		return fmt.Errorf("mountpoint %s still in kernel mount table after %d unmount attempts; "+
			"refusing to relaunch juicefs onto a busy mountpoint (would fail 'exit status 1')",
			fm.cfg.MountPoint, attempts)
	}
	return nil
}

// stillInMountTable reports whether fm.cfg.MountPoint appears in the kernel
// mount table. Pure-read, bounded; unlike isMountedLocked it does NOT probe
// responsiveness (a wedged mount still appears here), so it's the right check
// for "did the unmount actually remove the entry".
func (fm *FUSEManager) stillInMountTable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "mount").Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(out), fm.cfg.MountPoint)
}

// FUSEStaleEscalateTicks is the number of CONTINUOUS "stale but juicefs alive"
// monitor ticks (10s each) the watchdog tolerates before escalating to a
// last-resort remount. 9 ticks ≈ 90s: long enough that brief flapping never
// reaches it (the counter resets on any recovery), short enough that a
// genuinely stuck backend connection doesn't leave the mount wedged for many
// minutes. Var, not const, so tests can shorten it.
var FUSEStaleEscalateTicks = 9

// fuseWatchdogLinkAware gates the link-aware scaling of the escalation thresholds
// below. Default ON. Set JM_FUSE_WATCHDOG_LINKAWARE=0 (or the master
// JM_NET_ADAPTIVE=0, which pins class=medium) to restore the fixed historical
// thresholds. See docs/TUNING/REVERT_LOG.md (R1).
var fuseWatchdogLinkAware = os.Getenv("JM_FUSE_WATCHDOG_LINKAWARE") != "0"

// fuseOfflineNoEscalate (default ON): when offline, NEVER escalate a stale-but-
// alive mount to an app-side remount. Offline, "stale" is the juicefs daemon
// saturated by in-flight spool drains (busy, not wedged); a remount SIGTERMs a
// healthy daemon and drops the NFS mount (Finder "connection interrupted").
// Set JM_FUSE_OFFLINE_ESCALATE=1 to restore the pre-fix escalate-when-offline
// behavior for debugging (cellular-revert-safety doctrine).
var fuseOfflineNoEscalate = os.Getenv("JM_FUSE_OFFLINE_ESCALATE") != "1"

// fuseSkipEscalateWhileOffline reports whether a stale-but-juicefs-alive FUSE
// tick must be deferred (the escalation counter frozen, NEVER accumulating
// toward the escalate-to-remount threshold) because the user is OFFLINE.
// Offline, a "stale" probe is the juicefs daemon saturated by in-flight spool
// drains (busy, not wedged); escalating would diskutil-unmount-force a HEALTHY
// daemon and drop the NFS mount → Finder "connection interrupted". Gated by the
// JM_FUSE_OFFLINE_ESCALATE=1 kill-switch. Extracted from monitorLoop so the
// release-critical guard is unit-testable (see TestFUSEOfflineGuard*).
func fuseSkipEscalateWhileOffline() bool {
	return fuseOfflineNoEscalate && pin.IsOffline()
}

// V2.3 U4 (task #74/K3, field report "clicking offline doesn't help"): while
// the USER has explicitly engaged offline mode, the gone-branch's app-side
// remount of a dead juicefs is pure churn — a fresh juicefs mount needs the
// backend anyway, and the user has asked us to stand down. Navigation keeps
// serving from the SQLite mirror the whole time. AUTO-offline deliberately
// does NOT gate this (blips must self-heal without user action); the
// backend-reachability check next to the call site covers that shape.
// Set JM_FUSE_OFFLINE_REMOUNT=1 to restore the pre-fix always-retry behavior
// (cellular-revert-safety doctrine).
var fuseOfflineNoRemount = os.Getenv("JM_FUSE_OFFLINE_REMOUNT") != "1"

func fuseSkipRemountWhileUserOffline() bool {
	return fuseOfflineNoRemount && pin.IsUserOffline()
}

// fuseColdStartGrace is how long after the watchdog starts (≈ app start / first
// mount) the escalation to a destructive SIGKILL+remount is SUPPRESSED. During
// cold start the JuiceFS mount is legitimately busy warming caches and serving
// the initial ~200k-entry reconcile, so its FUSE-root readdir can be unresponsive
// to even the generous probe for minutes — NOT wedged, just saturated. Escalating
// then SIGKILLs a healthy-but-busy juicefs and, because the app's NFS server is
// holding the FUSE mount open, the follow-up unmount fails and leaves a ZOMBIE
// mount (kernel entry, no daemon) that breaks all data reads (observed live
// 2026-06-25, post-redeploy cold start). Within the grace window the watchdog
// defers and lets the mount recover on its own — it does, once the reconcile
// settles. A mount that wedges LATER (steady state) still escalates normally.
// Env override JM_FUSE_COLDSTART_GRACE_SEC (0 disables the grace).
var fuseColdStartGrace = coldStartGraceFromEnv()

func coldStartGraceFromEnv() time.Duration {
	if v := os.Getenv("JM_FUSE_COLDSTART_GRACE_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 6 * time.Minute
}

// staleEscalateTicks returns how many CONTINUOUS stale-but-alive ticks to tolerate
// before escalating to the destructive SIGKILL+remount, scaled UP on slow/metered
// links. ROOT CAUSE (2026-06-16, root-caused from juicemount.log): on cellular the
// 5s readdir stale-probe times out tick after tick because the JuiceFS->Redis
// readdir is legitimately SLOW (not wedged); at the fixed 9 ticks the watchdog
// then SIGKILL'd a perfectly-alive juicefs ~469 times in a day, with ~6% remount
// success, leaving FUSE dead for 30-45s per cycle = the "loading bar that never
// cures." medium/fast keep the validated 9 (~90s) BYTE-FOR-BYTE; metered/slow get
// a wide margin so a slow-but-alive mount is never mistaken for a wedge. A truly
// wedged mount still escalates, just later. Tests (class=medium, no signal) are
// unaffected.
func (fm *FUSEManager) staleEscalateTicks() int {
	if !fuseWatchdogLinkAware {
		return FUSEStaleEscalateTicks
	}
	switch netprofile.Default().Class() {
	case netprofile.ClassMetered:
		return FUSEStaleEscalateTicks * 3 // ~270s
	case netprofile.ClassSlow:
		return FUSEStaleEscalateTicks * 2 // ~180s
	default:
		return FUSEStaleEscalateTicks // ~90s — unchanged on LAN/medium/fast
	}
}

// confirmProbeTimeout returns the GENEROUS "is it actually wedged or just slow"
// probe budget before the destructive remount, scaled UP on slow/metered. The
// historical 25s is too short for a cellular FUSE-root readdir under Redis
// contention (so the probe false-fails and the kill proceeds — the bug above).
// medium/fast keep 25s unchanged.
func (fm *FUSEManager) confirmProbeTimeout() time.Duration {
	if !fuseWatchdogLinkAware {
		return fuseConfirmProbeTimeout
	}
	switch netprofile.Default().Class() {
	case netprofile.ClassMetered:
		return 90 * time.Second
	case netprofile.ClassSlow:
		return 45 * time.Second
	default:
		return fuseConfirmProbeTimeout // 25s — unchanged on LAN/medium/fast
	}
}

// mountVerifyTimeout is the launch-time mount-establish budget (V2.3 U5/K1,
// field "mount not ready after 15s" churn). Wider only where a juicefs
// cold-start legitimately needs longer to answer its first readdir.
//
// 2026-07-15 field regression (0.4.0 RC, Leland's box): the old 15s LAN/fast
// base assumed warm-up latency tracks LINK speed. It does NOT — a large LOCAL
// block cache (here ~100GB, --cache-size 102400) makes juicefs scan/index the
// cache dir on mount before its FUSE root readdir answers, ~30s on a FAST LAN.
// The 15s base false-failed ("mount not ready after 15s"); the watchdog then
// SIGKILL'd a HEALTHY-but-warming juicefs, and — because the NFS server holds
// the mount open — the follow-up unmount left a ZOMBIE macFUSE entry that made
// getfsstat (isMountedLocked check 1) hang → perpetual false-"stale" → remount
// thrash. So the base must cover local cache-warm, not just slow links. A real
// mount failure still fails FAST regardless (juicefs's own 10s "mount point is
// not ready" fatal fires from cmd.Run before we ever reach waitForMount — see
// noteMountFailure), so a generous verify budget only helps the slow-warm case
// and never delays genuine-failure detection.
//
// Env override JM_FUSE_VERIFY_SEC (seconds) for field-tuning without a rebuild.
func (fm *FUSEManager) mountVerifyTimeout() time.Duration {
	if v := os.Getenv("JM_FUSE_VERIFY_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	// 60s base covers a large-local-cache cold warm-up on any link class; the
	// slow/metered link classes still widen further (metadata warm-up over the
	// backend stacks on top of local warm-up).
	const base = 60 * time.Second
	if !fuseWatchdogLinkAware {
		return base
	}
	switch netprofile.Default().Class() {
	case netprofile.ClassMetered:
		return 120 * time.Second
	case netprofile.ClassSlow:
		return 90 * time.Second
	default:
		return base
	}
}

// backendReachable does a cheap TCP dial to the metadata (Redis) host to
// decide whether a remount could even succeed. Mirrors the reachability
// monitor's probe so the watchdog does NOT kill+remount juicefs during a real
// backend outage (where a fresh mount would just fail and churn). Best-effort:
// returns true (optimistic — don't block escalation) if the host can't be
// parsed.
func (fm *FUSEManager) backendReachable() bool {
	hostPort := redisHostPort(fm.cfg.RedisURL)
	if hostPort == "" {
		return true
	}
	conn, err := net.DialTimeout("tcp", hostPort, 2*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// redisHostPort extracts "host:port" from a redis URL (redis://host:port/db).
// Returns "" if it can't be parsed.
func redisHostPort(redisURL string) string {
	u, err := url.Parse(redisURL)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Host
}

// monitorLoop checks mount health periodically and remounts ONLY when the
// JuiceFS process tree is genuinely gone.
//
// Design (2026-05-29 audit). There are two layers of self-heal and they
// must not fight:
//
//  1. JuiceFS's own `-d` supervisor: `juicefs mount -d` spawns a watchdog
//     process that restarts the mount child if it crashes. This is the
//     first and best line of recovery — it re-establishes the FUSE session
//     cleanly and knows how to talk to the kernel.
//  2. This loop: the app-side backstop for when JuiceFS's own supervisor has
//     itself exhausted its retries and exited (the whole tree is dead).
//
// The pre-2026-05-29 code remounted after 3 consecutive unhealthy checks
// EVEN WHILE juicefs was still alive. On a flapping backend link that was
// catastrophic: it `kill -9`'d a live-but-slow daemon — and because the kill
// pattern matches `juicefs mount.*<mountpoint>`, it ALSO killed JuiceFS's own
// supervisor — then thrashed mount/unmount until macFUSE wedged with
// "init: 19=operation not supported by device", a kernel state only a full
// app restart clears. Observed live: juicefs SIGKILL'd mid-slow-PUT, never
// recovered, mount dead with no log trace.
//
// New policy: while ANY juicefs process is alive, NEVER remount — report the
// staleness and wait for juicefs's supervisor + backend recovery to clear it.
// Only when the process tree is gone do we own recovery. All decisions log via
// jmlog (the rotating juicemount.log), not log.Printf → the app's /dev/null
// stdout — the old channel made this safety-critical loop invisible.
func (fm *FUSEManager) monitorLoop() {
	defer close(fm.done)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	watchdogStart := time.Now() // for the cold-start escalation grace window
	consecutiveFailures := 0    // ticks where the mount is stale AND juicefs is gone
	staleWhileAliveTicks := 0   // ticks where the mount is stale but juicefs is alive
	remountDeferTicks := 0      // gone-branch ticks deferred (user-offline / backend down) — V2.3 U4
	offlineDeferTicks := 0      // ticks deferred because offline (busy draining, not wedged)
	for {
		select {
		case <-fm.stopCh:
			return
		case <-ticker.C:
			fm.mu.Lock()
			healthy := fm.isMountedLocked()
			fm.mu.Unlock()

			if healthy {
				if consecutiveFailures > 0 || staleWhileAliveTicks > 0 {
					jmlog.Info("fuse mount recovered",
						"after_dead_remount_attempts", consecutiveFailures,
						"stale_while_alive_ticks", staleWhileAliveTicks)
					consecutiveFailures = 0
					staleWhileAliveTicks = 0
				}
				continue
			}

			// The mount is unhealthy. Decide WHY before acting — killing a
			// live-but-slow juicefs is what drove the macFUSE remount thrash.
			if isJuiceFSProcessAliveFn(fm.cfg.MountPoint) {
				// OFFLINE GUARD (release fix — Finder "connection interrupted"):
				// when the user is OFFLINE, a "stale" probe is overwhelmingly the
				// juicefs daemon being SATURATED by in-flight spool drains (writes
				// with nowhere to go offline) — BUSY, not wedged. Escalating to a
				// remount unmounts (diskutil unmount force → SIGTERM) a HEALTHY
				// daemon and rips the FUSE mountpoint out from under the kernel NFS
				// export → Finder "connection interrupted" mid-offline-workflow.
				// juicefs is alive and the user chose offline, so a remount is
				// both unnecessary and harmful. Freeze the escalation counter and
				// defer; the mount recovers when the drain settles / on reconnect.
				// Kill-switch JM_FUSE_OFFLINE_ESCALATE=1 restores the pre-fix
				// behavior (cellular-revert-safety doctrine).
				if fuseSkipEscalateWhileOffline() {
					staleWhileAliveTicks = 0
					consecutiveFailures = 0
					offlineDeferTicks++
					if offlineDeferTicks == 1 || offlineDeferTicks%6 == 0 {
						jmlog.Warn("fuse stale but OFFLINE + juicefs alive — NOT escalating (busy draining, not wedged); an app-side remount here would drop the NFS mount → Finder 'connection interrupted'",
							"offline_defer_ticks", offlineDeferTicks)
					}
					continue
				}
				offlineDeferTicks = 0
				// juicefs (and its own supervisor) is alive — the mount is
				// stale, usually a flapping/blipping backend link. Default to
				// deferring (no kill, no macFUSE thrash). BUT juicefs's own
				// supervisor only restarts the mount child on a CRASH, not on a
				// stuck backend connection: a blip can leave juicefs's
				// connection wedged with FUSE stat hanging indefinitely
				// (observed 2026-06-01 — a 7-minute continuous wedge with zero
				// self-recovery). So after a SUSTAINED continuous wedge,
				// escalate to a remount as a last resort — but ONLY when the
				// backend is actually reachable (a fresh mount can't succeed
				// during a real outage, where deferring is correct). The
				// counter resets the instant the mount recovers, so brief
				// flapping never accumulates to the threshold.
				staleWhileAliveTicks++
				consecutiveFailures = 0
				if staleWhileAliveTicks >= fm.staleEscalateTicks() {
					// COLD-START GRACE: within the warm-up window a busy-but-alive
					// mount must never be SIGKILL'd — the app's NFS server holds the
					// FUSE mount, so the follow-up unmount fails and leaves a zombie
					// (2026-06-25). Defer; the mount recovers on its own once the
					// initial reconcile/cache-warm settles.
					if fuseColdStartGrace > 0 && time.Since(watchdogStart) < fuseColdStartGrace {
						jmlog.Warn("fuse stale at escalation threshold but within cold-start grace — deferring (busy warming, not wedged)",
							"stale_ticks", staleWhileAliveTicks,
							"grace_remaining_sec", int((fuseColdStartGrace - time.Since(watchdogStart)).Seconds()))
						staleWhileAliveTicks = 0
						continue
					}
					// CONFIRM truly-wedged before the destructive SIGKILL+remount.
					// The 5s isMountedLocked probe flips to "stale" under mere
					// SLOWNESS (reconcile/backend contention); accumulating that
					// to the threshold and killing a live juicefs is what drove
					// the macFUSE remount thrash (2026-06-14). A slow-but-alive
					// mount answers a generous probe — if it does, it is NOT
					// wedged; reset and keep deferring instead of remounting.
					confirmTimeout := fm.confirmProbeTimeout()
					if fm.mountResponsiveWithin(confirmTimeout) {
						jmlog.Warn("fuse stale at escalation threshold but a generous probe succeeded — NOT remounting (slow, not wedged)",
							"stale_ticks", staleWhileAliveTicks,
							"confirm_timeout_sec", int(confirmTimeout.Seconds()))
						staleWhileAliveTicks = 0
						continue
					}
					if fm.backendReachable() {
						jmlog.Warn("fuse wedged but juicefs alive for a sustained window + backend reachable — escalating to remount (last resort)",
							"stale_ticks", staleWhileAliveTicks)
						if err := fm.Mount(); err != nil {
							jmlog.Error("fuse escalation remount failed", "error", err.Error())
						} else {
							jmlog.Info("fuse escalation remount succeeded", "after_stale_ticks", staleWhileAliveTicks)
						}
						staleWhileAliveTicks = 0
					} else {
						jmlog.Warn("fuse wedged + juicefs alive, but backend unreachable — deferring (a remount cannot succeed during a real outage)",
							"stale_ticks", staleWhileAliveTicks)
					}
					continue
				}
				// Rate-limit: first stale tick, then ~once a minute.
				if staleWhileAliveTicks == 1 || staleWhileAliveTicks%6 == 0 {
					jmlog.Warn("fuse mount stale but juicefs alive — deferring to juicefs's own supervisor, not remounting",
						"stale_ticks", staleWhileAliveTicks,
						"hint", "transient backend/link stall; an app-side remount here would kill juicefs's supervisor and thrash macFUSE")
				}
				continue
			}

			// juicefs is genuinely GONE — its own supervisor exhausted its
			// retries and exited. App-side recovery is now the only option.
			staleWhileAliveTicks = 0

			// V2.3 U4: suppress the retry loop when it cannot or should not
			// succeed. (a) The user engaged offline — stand down until they
			// come back online (the mirror keeps serving nav). (b) The
			// backend is unreachable — a fresh juicefs mount needs Redis, so
			// each attempt just burns a 30s launch timeout and churns macFUSE
			// state. The next 10s tick re-evaluates both, so recovery is
			// automatic the moment conditions clear. Rate-limited logging.
			if fuseSkipRemountWhileUserOffline() {
				remountDeferTicks++
				if remountDeferTicks == 1 || remountDeferTicks%30 == 0 {
					jmlog.Info("juicefs gone but user is OFFLINE — not remounting (mirror serves nav; remount resumes when back online)",
						"defer_ticks", remountDeferTicks)
				}
				continue
			}
			if !fm.backendReachable() {
				remountDeferTicks++
				if remountDeferTicks == 1 || remountDeferTicks%30 == 0 {
					jmlog.Info("juicefs gone but backend unreachable — not remounting (a mount cannot succeed; retrying on recovery)",
						"defer_ticks", remountDeferTicks)
				}
				continue
			}
			remountDeferTicks = 0
			consecutiveFailures++
			jmlog.Warn("juicefs process tree gone — app-side remount",
				"attempt", consecutiveFailures)

			if err := fm.Mount(); err != nil {
				jmlog.Error("fuse remount failed",
					"attempt", consecutiveFailures, "error", err.Error())
				// A persistent failure here is usually the macFUSE
				// "operation not supported by device" kernel wedge, which
				// only a full app restart clears — back off hard rather than
				// thrash the kernel further.
				if strings.Contains(err.Error(), "operation not supported") {
					jmlog.Error("macFUSE refused the mount — kernel FUSE state is wedged; a full app restart is required to clear it",
						"detail", "repeated mount/unmount churn exhausted macFUSE")
				}
				if consecutiveFailures >= 3 {
					backoff := time.Duration(consecutiveFailures) * 10 * time.Second
					if backoff > 2*time.Minute {
						backoff = 2 * time.Minute
					}
					jmlog.Warn("fuse remount backing off", "backoff_sec", int(backoff.Seconds()))
					select {
					case <-fm.stopCh:
						return
					case <-time.After(backoff):
					}
				}
			} else {
				jmlog.Info("fuse remount succeeded", "after_attempts", consecutiveFailures)
				consecutiveFailures = 0
				// #12: every pooled fd predating this remount references the
				// DEAD mount — notify the bridge so the FDPool flushes them.
				fm.fireOnRemount()
			}
		}
	}
}

// juiceFSMountCommandTargets reports whether one process-table command is a
// JuiceFS mount for the exact mount point. Scoping is release-critical: a test
// mount or a second JuiceMount volume must never convince this manager that its
// own dead daemon is still alive and suppress recovery forever.
func juiceFSMountCommandTargets(command, mountPoint string) bool {
	mountPoint = filepath.Clean(strings.TrimSpace(mountPoint))
	if mountPoint == "." || mountPoint == "" {
		return false
	}
	fields := strings.Fields(command)
	if len(fields) < 2 || filepath.Base(fields[0]) != "juicefs" || fields[1] != "mount" {
		return false
	}
	padded := " " + strings.TrimSpace(command) + " "
	return strings.Contains(padded, " "+mountPoint+" ")
}

// juiceFSMountProcessIDs returns the process IDs for JuiceFS daemons targeting
// one exact mount point. The ps call is bounded so a sick host cannot pin the
// watchdog or shutdown path.
func juiceFSMountProcessIDs(mountPoint string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ps", "-axo", "pid=,command=").Output()
	if err != nil {
		return nil
	}
	var ids []string
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		cut := strings.IndexAny(line, " \t")
		if cut <= 0 {
			continue
		}
		pid := line[:cut]
		command := strings.TrimSpace(line[cut:])
		if juiceFSMountCommandTargets(command, mountPoint) {
			ids = append(ids, pid)
		}
	}
	return ids
}

// isJuiceFSProcessAlive reports whether the daemon for mountPoint is alive.
func isJuiceFSProcessAlive(mountPoint string) bool {
	return len(juiceFSMountProcessIDs(mountPoint)) > 0
}

// findJuiceFSBin locates the juicefs binary.
func findJuiceFSBin() string {
	// Check common locations
	candidates := []string{
		"/opt/homebrew/bin/juicefs",
		"/usr/local/bin/juicefs",
		"/usr/bin/juicefs",
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	// Try PATH
	if p, err := exec.LookPath("juicefs"); err == nil {
		return p
	}
	return ""
}

// DetectJuiceFSCacheDir finds the default JuiceFS cache directory for the volume.
func DetectJuiceFSCacheDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	cacheBase := filepath.Join(home, ".juicefs", "cache")
	entries, err := os.ReadDir(cacheBase)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if e.IsDir() {
			return filepath.Join(cacheBase, e.Name())
		}
	}
	return ""
}

// logVolumeCapacityBreakdown queries macOS's URLResourceKey-equivalent
// volume capacity numbers and logs them. Free (what statfs shows), Important
// (what the system would free for an app declaring important usage —
// includes Time Machine local snapshots, iCloud purgeable, system caches),
// and Opportunistic (lower-priority purgeable). On a typical 1 TB Mac these
// can differ by hundreds of GB; surfacing the gap explains why the JuiceFS
// "space not enough" warning fires when the user thinks they have headroom.
//
// Implementation note: we shell out to `df -k` for free, and parse
// `diskutil info -plist` for the canonical "important" number. Doing this
// in pure Go avoids pulling in CoreFoundation; the cost is two short execs
// at startup, which is fine.
func logVolumeCapacityBreakdown(volume string) {
	type capStats struct {
		totalBytes         int64
		freeBytes          int64
		importantBytes     int64
		opportunisticBytes int64
	}
	var c capStats

	// Free + total via statfs
	var st syscall.Statfs_t
	if err := syscall.Statfs(volume, &st); err == nil {
		c.totalBytes = int64(st.Bsize) * int64(st.Blocks)
		c.freeBytes = int64(st.Bsize) * int64(st.Bavail)
	}

	// We don't try to compute the "important capacity" (purgeable-aware)
	// number from Go — the only reliable source is Foundation's
	// URLResourceKey, and shelling out to swift is fragile in a signed
	// app bundle. The Swift popover queries it directly and displays it
	// to the user. The Go side just logs the conservative statfs view
	// and triggers reclamation when free is tight.

	gb := func(b int64) float64 { return float64(b) / (1 << 30) }
	jmlog.Info("cache volume capacity",
		"volume", volume,
		"total_gb", fmt.Sprintf("%.1f", gb(c.totalBytes)),
		"free_now_gb", fmt.Sprintf("%.1f", gb(c.freeBytes)),
		"hint", "popover shows reclaimable space (Foundation URLResourceKey for important usage); use the Reclaim button or POST /reclaim to free Time Machine local snapshots")
}

// ReclaimPurgeableSpace asks macOS to free purgeable disk space — primarily
// Time Machine local snapshots, which can hoard tens of GB on a typical
// laptop. Returns the bytes freed, count of snapshots thinned, and a
// human-readable source description (e.g. "Time Machine local snapshots").
//
// Mechanism: `tmutil thinlocalsnapshots <vol> <purgeAmountBytes> <urgency>`.
// Urgency 4 is "as much as possible." It's a non-interactive, no-sudo
// operation supported on macOS 10.13+.
//
// We measure the actual freed bytes by sampling the volume's free space
// before and after; the tmutil command's own output is unreliable for this
// (depends on macOS version and which snapshots existed).
func ReclaimPurgeableSpace(volume string, targetBytes int64) (freedBytes int64, snapshotsThinned int, source string, err error) {
	beforeFree, _ := volumeFreeBytes(volume)

	// Urgency 4 = thin as much as possible. tmutil rejects "0" as an invalid
	// amount, so when the caller passes 0 we substitute a high number that
	// effectively means "give me as much as you can." 1 TiB is more than
	// any laptop SSD; tmutil will only free what's actually thinnable.
	var amount string
	if targetBytes > 0 {
		amount = fmt.Sprintf("%d", targetBytes)
	} else {
		amount = fmt.Sprintf("%d", int64(1)<<40) // 1 TiB
	}
	// tmutil can occasionally take a while to walk + delete snapshots; cap
	// at 90 s. If it doesn't return by then, we return what we measured —
	// the user can retry via the Reclaim button.
	tmCtx, tmCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer tmCancel()
	cmd := exec.CommandContext(tmCtx, "tmutil", "thinlocalsnapshots", volume, amount, "4")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return 0, 0, "Time Machine local snapshots", fmt.Errorf("tmutil thinlocalsnapshots: %w (output: %s)",
			err, strings.TrimSpace(string(out)))
	}

	afterFree, _ := volumeFreeBytes(volume)
	freedBytes = afterFree - beforeFree
	if freedBytes < 0 {
		freedBytes = 0 // negative means another process took space concurrently
	}

	// Parse tmutil's output to count thinned snapshots. The format is
	// macOS-version-dependent but the common case has lines like:
	//   "Thinned local snapshots:" followed by one snapshot ID per line
	//   ("2024-01-01-120000" etc.). On macOS without anything to thin
	//   the output is usually empty. We count snapshot-ID lines (the
	//   ones that look like a tmutil snapshot timestamp).
	snapshotsThinned = countTmutilSnapshotLines(string(out))
	source = "Time Machine local snapshots"

	jmlog.Info("reclaimed purgeable space",
		"volume", volume,
		"freed_gb", fmt.Sprintf("%.1f", float64(freedBytes)/(1<<30)),
		"before_free_gb", fmt.Sprintf("%.1f", float64(beforeFree)/(1<<30)),
		"after_free_gb", fmt.Sprintf("%.1f", float64(afterFree)/(1<<30)),
		"snapshots_thinned", snapshotsThinned,
		"tmutil_output", strings.TrimSpace(string(out)))
	return freedBytes, snapshotsThinned, source, nil
}

// countTmutilSnapshotLines extracts the count of thinned snapshots from
// `tmutil thinlocalsnapshots` output. Matches lines that look like a
// tmutil snapshot ID — typically "YYYY-MM-DD-HHMMSS" optionally prefixed
// by some path metadata. Conservative: only counts lines that start with
// 4 digits (a year), to avoid double-counting header lines.
func countTmutilSnapshotLines(out string) int {
	n := 0
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		if len(trimmed) < 10 {
			continue
		}
		// Crude but effective: snapshot lines start with a 4-digit year
		// followed by '-' (e.g. "2026-05-18-..." or full paths ending in
		// such a timestamp). Headers like "Thinned local snapshots:" do
		// not match. False positives are extremely unlikely for tmutil.
		if trimmed[0] >= '0' && trimmed[0] <= '9' &&
			trimmed[1] >= '0' && trimmed[1] <= '9' &&
			trimmed[2] >= '0' && trimmed[2] <= '9' &&
			trimmed[3] >= '0' && trimmed[3] <= '9' &&
			trimmed[4] == '-' {
			n++
		}
	}
	return n
}

func volumeFreeBytes(volume string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(volume, &st); err != nil {
		return 0, err
	}
	return int64(st.Bsize) * int64(st.Bavail), nil
}

func volumeTotalBytes(volume string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(volume, &st); err != nil {
		return 0, err
	}
	return int64(st.Bsize) * int64(st.Blocks), nil
}

// envOr returns the value of environment variable key, or def when unset/empty.
// Used for the per-cache FUSE metadata TTL overrides so each knob has a visible
// default at its call site instead of a bare literal.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// metaCacheTTLs returns the four juicefs FUSE metadata cache TTLs, split by
// role. Extracted from Mount() purely so the values are unit-testable: the
// previous incarnation of this policy sat behind JM_WAN_MODE, which nothing in
// production ever set, so the intended WAN values shipped dead for months with
// no test to notice. See the commentary at the call site for the full rationale.
//
// Returns (attr, entry, dirEntry, negative).
func metaCacheTTLs() (attr, entry, dirEntry, negative string) {
	entry = envOr("JM_META_ENTRY_CACHE", "60s")
	dirEntry = envOr("JM_META_DIR_ENTRY_CACHE", "60s")
	attr = envOr("JM_META_ATTR_CACHE", "10s")
	negative = envOr("JM_META_NEGATIVE_CACHE", "5s")
	// Documented escape hatch: one value for all four.
	if v := os.Getenv("JM_META_CACHE_SECS"); v != "" {
		return v, v, v, v
	}
	return attr, entry, dirEntry, negative
}

// MetaURLForMount returns the metadata URL to use, applying the
// JM_DEBUG_META_ADDR development override when set.
//
// WHY THIS EXISTS. The reported bug — navigation instant offline, awful on
// cellular — is only reproducible at high RTT. Every measurement taken on a LAN
// shows online nav already at parity with offline (3ms/dir vs 6ms/dir at 4.8ms
// RTT), so a fix cannot be validated where it is being written. Iterating on a
// cellular problem while measuring on WiFi is how you convince yourself of a fix
// that does nothing.
//
// The latency lives in juicefs's own Redis round trips, not in this process's
// metadata client, so an in-Go delay would inject latency into the wrong path
// entirely. Pointing juicefs at a latency-injecting TCP proxy (cmd/jmlatency)
// reproduces the real thing: the same syscalls, the same FUSE layer, the same
// gates, with only the metadata round trip slowed.
//
// SAFETY. Unset — the overwhelmingly normal case — this is a pure passthrough
// and the mount is byte-identical to before. It never rewrites the SCHEME, DB
// index, or credentials, only host:port, and only when the override parses as a
// URL; anything malformed leaves the original untouched rather than mounting
// against a half-built address. It is deliberately NOT a persisted setting: an
// env var dies with the process, so a forgotten debug knob cannot survive a
// relaunch and silently degrade a user's mount.
func MetaURLForMount(redisURL string) string {
	addr := os.Getenv("JM_DEBUG_META_ADDR")
	if addr == "" {
		return redisURL
	}
	u, err := url.Parse(redisURL)
	if err != nil || u.Host == "" {
		return redisURL
	}
	u.Host = addr
	return u.String()
}

// openCacheTTL returns the --open-cache duration to pass, or "" to omit the
// flag entirely (juicefs default 0s = disabled = today's behavior). See the
// call site for why this is opt-in rather than on by default.
// Config wins over the environment: the config is the only channel that can
// reach a shipped app (see FUSEConfig.OpenCacheTTL). The env var is retained so
// a CLI/test run can still set it without building a config.
func (fm *FUSEManager) openCacheTTL() string {
	if v := normalizeOpenCacheTTL(fm.cfg.OpenCacheTTL); v != "" {
		return v
	}
	return normalizeOpenCacheTTL(os.Getenv("JM_OPEN_CACHE"))
}

// normalizeOpenCacheTTL treats empty/0/0s as "omit the flag" so an explicitly
// zeroed config reads as OFF rather than as unset.
func normalizeOpenCacheTTL(v string) string {
	if v == "" || v == "0" || v == "0s" {
		return ""
	}
	return v
}

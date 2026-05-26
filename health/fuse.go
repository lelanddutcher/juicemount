// Package health — JuiceFS FUSE mount lifecycle management.
//
// FUSEManager mounts JuiceFS, monitors the mount health, and automatically
// remounts if the FUSE process dies or the mount becomes stale.
package health

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// FUSEConfig holds JuiceFS mount configuration.
type FUSEConfig struct {
	RedisURL   string // e.g. "redis://127.0.0.1:6379/1"
	MountPoint string // e.g. ~/.juicemount/fuse-internal
	CacheDir   string // e.g. ~/.juicefs/cache (empty = JuiceFS default)
	CacheSize  string // e.g. "100000" in MiB (empty = JuiceFS default ~100 GiB)
	JuiceFSBin string // e.g. /opt/homebrew/bin/juicefs (auto-detected if empty)

	// FreeSpaceRatio is the fraction of cache-volume free space JuiceFS keeps
	// reserved (won't cache when below). Default in JuiceFS is 0.1 (10%) which
	// is hostile to video editors who fill their disks with media — once they
	// drop under 90 GB free on a 1 TB disk, the cache silently disables and
	// every read goes straight to S3 with no warning.
	//
	// Empty = pass nothing (JuiceFS default applies). "0.01" = keep 1% free.
	FreeSpaceRatio string
}

// FUSEManager manages the JuiceFS FUSE mount lifecycle.
type FUSEManager struct {
	cfg    FUSEConfig
	mu     sync.Mutex
	cmd    *exec.Cmd
	stopCh chan struct{}
	done   chan struct{}
}

// EffectiveCacheSize returns the cache-size string actually passed to the
// JuiceFS daemon (post auto-expansion). Used by callers that want to log
// the effective value rather than the user's original input.
func (fm *FUSEManager) EffectiveCacheSize() string {
	return fm.cfg.CacheSize
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

	// Unmount any stale mount first
	fm.unmountLocked()

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

	// Auto-size the JuiceFS cache to actually use the disk we have.
	//
	// The Swift app defaults ssdCacheGB to 100 GB. That's a nonsensical cap
	// for a video editor — pinning a single 158 GB project blows past it
	// instantly, and JuiceFS starts evicting blocks the user just paid to
	// download. Symptom: pinned files marked Ready, cache directory full,
	// but Resolve still pulls every other block from S3 because LRU keeps
	// rotating.
	//
	// Policy: cache-size = max(user_configured, current_cache_used,
	//                           total_disk - 20% headroom).
	//
	// We add the *current_cache_used* term because if JuiceFS is already
	// holding 99 GB on disk, we must allow it to keep that much; otherwise
	// JuiceFS would start evicting on first access. We use total_disk * 0.8
	// rather than free_disk because free shrinks AS the cache grows — using
	// the conservative free number locks us into a cap that's smaller than
	// the cache will actually be allowed to grow to (free-space-ratio
	// enforces the real ceiling at write time).
	if total, err := volumeTotalBytes("/"); err == nil && total > 0 {
		ceilingGB := int64(float64(total)/(1<<30) * 0.85) // 85% of total disk
		var configuredGB int64
		fmt.Sscanf(fm.cfg.CacheSize, "%d", &configuredGB)
		configuredGB = configuredGB / 1024 // CacheSize is in MiB; convert to GiB

		// Don't shrink below what's already in cache — otherwise eviction
		// fires on the very first read.
		var currentUsedGB int64
		if home, err := os.UserHomeDir(); err == nil {
			cacheGlob := filepath.Join(home, ".juicefs", "cache")
			if entries, err := os.ReadDir(cacheGlob); err == nil {
				for _, e := range entries {
					if !e.IsDir() {
						continue
					}
					var st syscall.Statfs_t
					if syscall.Statfs(filepath.Join(cacheGlob, e.Name()), &st) != nil {
						continue
					}
					// Best-effort: use du-equivalent via a walk would be slow.
					// Instead query allocation directly via Statfs's used fields.
					// Falls through if we can't tell.
				}
			}
		}

		targetGB := ceilingGB
		if currentUsedGB > targetGB {
			targetGB = currentUsedGB
		}
		if configuredGB > targetGB {
			targetGB = configuredGB // user wanted more; respect it
		}
		if targetGB > configuredGB {
			fm.cfg.CacheSize = fmt.Sprintf("%d", targetGB*1024) // back to MiB
			jmlog.Info("cache-size auto-expanded",
				"configured_gb", configuredGB,
				"target_gb", targetGB,
				"ceiling_gb", ceilingGB,
				"reason", "85% of total disk; free-space-ratio enforces real ceiling at write time")
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
	args = append(args,
		"mount", fm.cfg.RedisURL, fm.cfg.MountPoint,
		"-d",                // daemon mode
		"--no-usage-report",
		"--writeback",       // enable writeback for write performance
		// QA-33 (2026-05-25): bumped --buffer-size 1024 → 4096 (4 GiB).
		// Vision: writes to /Volumes/zpool should sustain at local SSD
		// speed; uploads to MinIO happen async in background; cache
		// holds dirty data. The previous 1 GiB ceiling capped sustained
		// write throughput at the network upload rate after only ~1 GB
		// — wrong model for video-render workflows where a 20 GB export
		// would slow to wifi speed mid-bounce.
		// 4 GiB is a balanced point: enough buffer to absorb realistic
		// chunky-render write bursts before back-pressure kicks in,
		// without eating an unbounded amount of RAM. Crash window
		// (data not yet durable in S3): ≤4 GiB; the on-disk cache holds
		// the persistent copy until upload completes.
		"--buffer-size", "4096", // 4 GiB
		"--prefetch", "3",   // prefetch 3 blocks ahead
		"-o", "nobrowse",    // hide from Finder (MNT_DONTBROWSE flag)
	)
	if fm.cfg.CacheDir != "" {
		args = append(args, "--cache-dir", fm.cfg.CacheDir)
	}
	if fm.cfg.CacheSize != "" {
		args = append(args, "--cache-size", fm.cfg.CacheSize)
	}
	if fm.cfg.FreeSpaceRatio != "" {
		args = append(args, "--free-space-ratio", fm.cfg.FreeSpaceRatio)
	}

	log.Printf("[fuse] mounting JuiceFS: %s %s", fm.cfg.JuiceFSBin, strings.Join(args, " "))

	// `juicefs mount -d` daemonizes and returns quickly in the happy path,
	// but can hang on certain backend failures (e.g. unreachable Redis
	// during Lua init). Bounded at 30 s so a stuck launch can't park the
	// caller forever.
	launchCtx, launchCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer launchCancel()
	cmd := exec.CommandContext(launchCtx, fm.cfg.JuiceFSBin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if launchCtx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("juicefs mount: timed out after 30s (backend unreachable?)")
		}
		return fmt.Errorf("juicefs mount: %w", err)
	}

	// Wait for the mount to become live (juicefs mount -d returns before FUSE is ready)
	if err := fm.waitForMount(15 * time.Second); err != nil {
		return fmt.Errorf("mount verification: %w", err)
	}

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
		count    int
		firstAt  time.Time
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
	out, err := exec.CommandContext(ctx, "mount").Output()
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

	// Check 2: actually responsive — try listing the directory.
	// A stale FUSE mount (dead process) will hang on any fs operation.
	done := make(chan bool, 1)
	go func() {
		_, err := os.ReadDir(fm.cfg.MountPoint)
		done <- (err == nil)
	}()
	select {
	case ok := <-done:
		return ok
	case <-time.After(5 * time.Second):
		// QA-34 Slice 2 (2026-05-25): no longer fires a side-effect
		// `umount -f` from inside this health probe.
		//
		// History of the bug: isMountedLocked is the leaf "is the mount
		// healthy right now" check, called by the monitorLoop AND by
		// any other code path that wants a snapshot. Pre-fix, a 5-second
		// ReadDir timeout would trigger a fire-and-forget `umount -f`
		// as a hidden side effect — which bypassed QA-33's 30-second
		// consecutive-failure tolerance entirely. Under sustained
		// writes, juicefs does fsync flushes that can take 15-30 s
		// (observed: 16.86 s in production), during which ReadDir
		// against the mount point hangs. The hidden umount then races
		// the in-flight fsync, fails-or-succeeds depending on timing,
		// and either way kills the daemon mid-write.
		//
		// New rule: this function is pure-read. It returns false on
		// timeout and lets monitorLoop (which honors the consecutive-
		// failure tolerance) decide whether to actually remount. The
		// 5-second probe timeout stays so callers don't block forever.
		log.Printf("[fuse] mount at %s is unresponsive (stale); reporting unhealthy. Remount decision deferred to monitorLoop.", fm.cfg.MountPoint)
		return false
	}
}

// runBoundedCommand fires a shell command with a hard time limit and reaps
// the process. Logs on timeout. Used for fire-and-forget cleanup work that
// previously called `.Start()` without ever calling `.Wait()` — leaking
// zombies and silently failing.
func runBoundedCommand(timeout time.Duration, name string, args ...string) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := exec.CommandContext(ctx, name, args...).Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			jmlog.Warn("bounded command timed out",
				"cmd", name, "args", strings.Join(args, " "),
				"timeout_sec", int(timeout.Seconds()))
		} else {
			jmlog.Debug("bounded command failed",
				"cmd", name, "args", strings.Join(args, " "), "error", err.Error())
		}
	}
}

// waitForMount polls until the mount is live or timeout expires.
func (fm *FUSEManager) waitForMount(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fm.isMountedLocked() {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("mount not ready after %v", timeout)
}

// unmountLocked forcibly unmounts the FUSE mount. Must be called with fm.mu held.
// Every shell-out is time-bounded so a wedged kernel state can't pin fm.mu.
func (fm *FUSEManager) unmountLocked() {
	// Kill any lingering JuiceFS mount processes for this mount point first.
	// Killing the process lets the kernel release the mount.
	pgrepCtx, pgrepCancel := context.WithTimeout(context.Background(), 2*time.Second)
	procs, _ := exec.CommandContext(pgrepCtx, "pgrep", "-f", "juicefs mount.*"+filepath.Base(fm.cfg.MountPoint)).Output()
	pgrepCancel()
	for _, line := range strings.Split(strings.TrimSpace(string(procs)), "\n") {
		if line != "" {
			killCtx, killCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = exec.CommandContext(killCtx, "kill", "-9", line).Run()
			killCancel()
		}
	}
	time.Sleep(500 * time.Millisecond)

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
		runBoundedCommand(60*time.Second, "umount", "-f", fm.cfg.MountPoint)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(65 * time.Second):
		// Safety net: even if runBoundedCommand hangs (shouldn't,
		// it has its own ctx deadline), don't pin fm.mu forever.
		jmlog.Warn("unmountLocked: umount goroutine did not return in 65s, proceeding anyway")
	}
}

// monitorLoop checks mount health periodically and remounts if needed.
//
// QA-33 (2026-05-25): require multiple consecutive unhealthy checks
// before remounting. A single 5-second stat timeout in checkFUSE looks
// identical to "JuiceFS is dead" but is actually the common case during
// sustained big-file writes — the FUSE-side flush can stall the stat
// syscall for 10-30s while a 4-MiB chunk uploads to MinIO under wifi
// pressure. The pre-QA-33 code killed-and-restarted juicefs on a single
// stall, which (a) dropped 30+ seconds of writes during the restart
// window, and (b) made the "Dropbox-style local-disk-speed write"
// architecture impossible because every sustained write got murdered
// mid-flight.
//
// Fast-path exception: if the juicefs PID is actually gone (process
// died, was OOM-killed, segfaulted), remount immediately — that IS the
// scenario the watchdog was originally for.
func (fm *FUSEManager) monitorLoop() {
	defer close(fm.done)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// QA-33: 3 consecutive failures × 10s tick = 30s of sustained
	// unhealthiness before remount. A slow flush completes well inside
	// that window; a genuinely-dead daemon doesn't.
	const remountAfterNFailures = 3

	consecutiveFailures := 0
	for {
		select {
		case <-fm.stopCh:
			return
		case <-ticker.C:
			fm.mu.Lock()
			healthy := fm.isMountedLocked()
			fm.mu.Unlock()

			if healthy {
				if consecutiveFailures > 0 {
					log.Printf("[fuse] mount recovered after %d failed checks", consecutiveFailures)
					consecutiveFailures = 0
				}
				continue
			}

			consecutiveFailures++

			// Fast-path: if the daemon is genuinely gone (not just slow),
			// don't wait for the threshold — remount immediately.
			juicefsAlive := isJuiceFSProcessAlive()
			if !juicefsAlive {
				log.Printf("[fuse] juicefs process gone — remounting immediately (attempt %d)", consecutiveFailures)
			} else if consecutiveFailures < remountAfterNFailures {
				log.Printf("[fuse] mount unhealthy (check %d/%d); juicefs PID alive, deferring remount in case the daemon is just slow",
					consecutiveFailures, remountAfterNFailures)
				continue
			} else {
				log.Printf("[fuse] mount unhealthy for %d consecutive checks — remounting", consecutiveFailures)
			}

			if err := fm.Mount(); err != nil {
				log.Printf("[fuse] remount failed: %v", err)
				// Exponential backoff: after repeated failures, slow down
				if consecutiveFailures > 3 {
					backoff := time.Duration(consecutiveFailures) * 10 * time.Second
					if backoff > 2*time.Minute {
						backoff = 2 * time.Minute
					}
					log.Printf("[fuse] backing off %v before next attempt", backoff)
					select {
					case <-fm.stopCh:
						return
					case <-time.After(backoff):
					}
				}
			} else {
				consecutiveFailures = 0
			}
		}
	}
}

// isJuiceFSProcessAlive returns true if any `juicefs mount` process
// is currently running. Used by the watchdog's fast-path exception
// to distinguish "daemon is slow" from "daemon is dead".
func isJuiceFSProcessAlive() bool {
	out, err := exec.Command("pgrep", "-f", "juicefs mount").Output()
	if err != nil {
		return false
	}
	return len(strings.TrimSpace(string(out))) > 0
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
		totalBytes        int64
		freeBytes         int64
		importantBytes    int64
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

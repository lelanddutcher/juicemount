// Package health — JuiceFS FUSE mount lifecycle management.
//
// FUSEManager mounts JuiceFS, monitors the mount health, and automatically
// remounts if the FUSE process dies or the mount becomes stale.
package health

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// FUSEConfig holds JuiceFS mount configuration.
type FUSEConfig struct {
	RedisURL   string // e.g. "redis://192.168.0.210:6379/1"
	MountPoint string // e.g. ~/.juicemount/fuse-internal
	CacheDir   string // e.g. ~/.juicefs/cache (empty = JuiceFS default)
	CacheSize  string // e.g. "100G" (empty = JuiceFS default)
	JuiceFSBin string // e.g. /opt/homebrew/bin/juicefs (auto-detected if empty)
}

// FUSEManager manages the JuiceFS FUSE mount lifecycle.
type FUSEManager struct {
	cfg    FUSEConfig
	mu     sync.Mutex
	cmd    *exec.Cmd
	stopCh chan struct{}
	done   chan struct{}
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

	// Build juicefs mount command
	// The FUSE mount is an internal implementation detail — users interact
	// only with the NFS volume at /Volumes/zpool. We hide the FUSE mount:
	//   - Mount point is ~/.juicemount/fuse-internal (hidden dotdir, not /Volumes)
	//   - nobrowse: tells macOS not to show the volume in Finder sidebar
	args := []string{"mount", fm.cfg.RedisURL, fm.cfg.MountPoint,
		"-d",                // daemon mode
		"--no-usage-report",
		"--writeback",       // enable writeback for write performance
		"--buffer-size", "1024", // 1GB buffer
		"--prefetch", "3",   // prefetch 3 blocks ahead
		"-o", "nobrowse",    // hide from Finder (MNT_DONTBROWSE flag)
	}
	if fm.cfg.CacheDir != "" {
		args = append(args, "--cache-dir", fm.cfg.CacheDir)
	}
	if fm.cfg.CacheSize != "" {
		args = append(args, "--cache-size", fm.cfg.CacheSize)
	}

	log.Printf("[fuse] mounting JuiceFS: %s %s", fm.cfg.JuiceFSBin, strings.Join(args, " "))

	cmd := exec.Command(fm.cfg.JuiceFSBin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("juicefs mount: %w", err)
	}

	// Wait for the mount to become live (juicefs mount -d returns before FUSE is ready)
	if err := fm.waitForMount(15 * time.Second); err != nil {
		return fmt.Errorf("mount verification: %w", err)
	}

	log.Printf("[fuse] JuiceFS mounted at %s", fm.cfg.MountPoint)

	// Suppress Spotlight indexing on the hidden FUSE mount
	os.WriteFile(fm.cfg.MountPoint+"/.metadata_never_index", nil, 0644)

	return nil
}

// StartMonitor begins a background goroutine that checks mount health
// and remounts if the FUSE mount dies.
func (fm *FUSEManager) StartMonitor() {
	go fm.monitorLoop()
}

// Stop unmounts JuiceFS and stops the monitor.
func (fm *FUSEManager) Stop() {
	close(fm.stopCh)
	<-fm.done

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
func (fm *FUSEManager) isMountedLocked() bool {
	// Check 1: appears in macOS mount table as a FUSE mount.
	// Do this FIRST (never hangs) before touching the mount point filesystem.
	out, err := exec.Command("mount").Output()
	if err != nil {
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
		log.Printf("[fuse] mount at %s is unresponsive (stale), force unmounting", fm.cfg.MountPoint)
		// Force unmount the dead FUSE — don't leave it hanging
		exec.Command("umount", "-f", fm.cfg.MountPoint).Start()
		return false
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
// Uses non-blocking commands to avoid hanging on dead FUSE mounts.
func (fm *FUSEManager) unmountLocked() {
	// Kill any lingering JuiceFS mount processes for this mount point first.
	// This is critical: a dead FUSE process leaves a kernel mount that hangs
	// on any fs operation. Killing the process lets the kernel release the mount.
	procs, _ := exec.Command("pgrep", "-f", "juicefs mount.*"+filepath.Base(fm.cfg.MountPoint)).Output()
	for _, line := range strings.Split(strings.TrimSpace(string(procs)), "\n") {
		if line != "" {
			exec.Command("kill", "-9", line).Run()
		}
	}
	time.Sleep(500 * time.Millisecond)

	// Non-blocking unmount attempts (Start, not Run — won't hang on dead FUSE)
	exec.Command("umount", "-f", fm.cfg.MountPoint).Start()
	time.Sleep(500 * time.Millisecond)
}

// monitorLoop checks mount health periodically and remounts if needed.
func (fm *FUSEManager) monitorLoop() {
	defer close(fm.done)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

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
			log.Printf("[fuse] mount unhealthy (attempt %d), remounting...", consecutiveFailures)

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

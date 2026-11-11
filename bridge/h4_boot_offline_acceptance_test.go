//go:build h4startupacceptance

package main

import (
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

// Boot-level acceptance for the U2 boot defer (laptop lab 2026-11-11,
// after-8). Runs the real NFSServerStart with the boot probe scripted:
// nothing listens at the Redis address, so a boot that wrongly went online
// would spend about a minute in connect retries before NFS starts.
//
// Bound: an offline start publishes NFS within a few seconds on this harness.
// An online boot here pays at least the 5-attempt connect schedule (1+2+4+8 s
// of sleeps alone), so 12 s separates the two paths.
const bootOfflineStartBound = 12 * time.Second

type scriptedBootProbe struct {
	mu      sync.Mutex
	samples []time.Duration // 0 = failed sample (dial timeout)
	calls   int
}

func (p *scriptedBootProbe) probe(string, time.Duration) (bool, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.calls >= len(p.samples) {
		p.calls++
		return false, 0
	}
	s := p.samples[p.calls]
	p.calls++
	return s > 0, s
}

func h4BootOfflineStart(t *testing.T, seedMirror bool, samples ...time.Duration) (string, time.Duration, *scriptedBootProbe) {
	t.Helper()
	cfg, _ := h4StartupConfig(t)
	cfg.RedisURL = "redis://" + h4UnusedLoopbackAddr(t) + "/0"
	t.Setenv("JM_BOOT_DEFER_RTT_MS", "")
	if seedMirror {
		store := h4OpenFixtureDB(t, cfg.DBPath)
		if err := store.BulkInsert([]*metadata.Entry{
			{Path: "media", Name: "media", ParentPath: ".", IsDir: true, Inode: 2, Mtime: time.Unix(1_700_000_000, 0)},
		}, 10); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
	p := &scriptedBootProbe{samples: samples}
	previous := bootBackendRTT
	bootBackendRTT = p.probe
	t.Cleanup(func() {
		bootBackendRTT = previous
		_ = NFSServerStop()
		pin.SetAutoOffline(false, "")
		pin.SetOffline(false)
	})
	t0 := time.Now()
	got := h4Start(t, cfg)
	took := time.Since(t0)

	globalMu.Lock()
	server := globalServer
	globalMu.Unlock()
	if server == nil || !server.IsRunning() {
		t.Fatalf("NFSServerStart = %q without a running NFS server", got)
	}
	conn, err := net.DialTimeout("tcp", cfg.ListenAddr, time.Second)
	if err != nil {
		t.Fatalf("NFS not accepting at %s: %v", cfg.ListenAddr, err)
	}
	_ = conn.Close()
	return got, took, p
}

// H1: a new install (no saved file list) on a confirmed-slow link takes the
// offline path and reaches NFS server start within the bound, and its status
// says why the volume is empty.
func TestBootOfflineSlowEmptyMirrorReachesNFSStartQuickly(t *testing.T) {
	got, took, p := h4BootOfflineStart(t, false, 900*time.Millisecond, 850*time.Millisecond, 820*time.Millisecond)
	if took > bootOfflineStartBound {
		t.Fatalf("slow empty-mirror boot took %v to reach NFS start (bound %v)", took, bootOfflineStartBound)
	}
	if !strings.HasPrefix(got, "started_offline: ") || !strings.Contains(got, "no saved file list") {
		t.Fatalf("status %q; want started_offline with the no-saved-file-list reason", got)
	}
	if p.calls != bootRTTSamples {
		t.Fatalf("boot probe called %d times; want %d (first + two re-measures)", p.calls, bootRTTSamples)
	}
	if offlineBootAt.Load() == 0 {
		t.Fatal("offline start did not record offlineBootAt for the file-list status")
	}
}

// M1: a slow first sample and two lost re-measures (dial timeouts) is a nearly
// dead link. It must take the offline path, not the online boot.
func TestBootOfflineLostResamplesTakeOfflinePath(t *testing.T) {
	got, took, p := h4BootOfflineStart(t, false, 611*time.Millisecond, 0, 0)
	if took > bootOfflineStartBound {
		t.Fatalf("boot with lost re-measures took %v to reach NFS start (bound %v)", took, bootOfflineStartBound)
	}
	if !strings.HasPrefix(got, "started_offline: ") {
		t.Fatalf("status %q; want an offline start", got)
	}
	if p.calls != bootRTTSamples {
		t.Fatalf("boot probe called %d times; want %d", p.calls, bootRTTSamples)
	}
}

// With a saved file list, the slow-link offline start keeps its own cause.
func TestBootOfflineSlowSavedMirrorStatus(t *testing.T) {
	got, _, _ := h4BootOfflineStart(t, true, 900*time.Millisecond, 880*time.Millisecond, 860*time.Millisecond)
	want := "started_offline: backend responding slowly — started offline; recovering in background"
	if got != want {
		t.Fatalf("status %q; want %q", got, want)
	}
}

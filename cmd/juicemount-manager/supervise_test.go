package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestSupervisorRetriesWithCappedBackoff: a component that never starts is
// retried forever with exponential backoff capped at maxBackoff, and its
// state and error are visible while it waits.
func TestSupervisorRetriesWithCappedBackoff(t *testing.T) {
	clock := time.Unix(1_700_000_000, 0)
	var mu sync.Mutex
	var delays []time.Duration
	var attempts int
	var sup *supervisor
	sup = newSupervisor("test", func(stop <-chan struct{}, ready func(string)) error {
		mu.Lock()
		defer mu.Unlock()
		if attempts > 0 {
			delays = append(delays, sup.status.snapshot().NextRetry.Sub(clock))
		}
		attempts++
		return errors.New("start failed")
	})
	sup.minBackoff, sup.maxBackoff = time.Millisecond, 4*time.Millisecond
	sup.now = func() time.Time { return clock }
	sup.Start()
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := attempts
		mu.Unlock()
		if n >= 7 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("supervisor gave up after %d attempts", n)
		}
		time.Sleep(time.Millisecond)
	}
	st := sup.status.snapshot()
	sup.Stop()
	mu.Lock()
	defer mu.Unlock()
	want := []time.Duration{1, 2, 4, 4, 4, 4}
	for i, d := range want {
		if delays[i] != d*time.Millisecond {
			t.Fatalf("backoff delays = %v, want %v ms then capped", delays, want)
		}
	}
	if st.Restarts < 6 || st.LastError != "start failed" {
		t.Fatalf("status while retrying = %+v", st)
	}
	if got := sup.status.snapshot().State; got != "stopped" {
		t.Fatalf("state after Stop = %q, want stopped", got)
	}
}

// TestSupervisorResetsBackoffAfterStableRun: a component that ran healthily
// for stableAfter restarts at the minimum delay, not the accumulated one.
func TestSupervisorResetsBackoffAfterStableRun(t *testing.T) {
	var mu sync.Mutex
	clock := time.Unix(1_700_000_000, 0)
	var attempts int
	var delays []time.Duration
	var sup *supervisor
	sup = newSupervisor("test", func(stop <-chan struct{}, ready func(string)) error {
		mu.Lock()
		if attempts > 0 {
			delays = append(delays, sup.status.snapshot().NextRetry.Sub(clock))
		}
		attempts++
		n := attempts
		mu.Unlock()
		if n == 4 {
			ready("up")
			mu.Lock()
			clock = clock.Add(time.Hour) // healthy for an hour, then fails
			mu.Unlock()
		}
		return errors.New("crashed")
	})
	sup.minBackoff, sup.maxBackoff, sup.stableAfter = time.Millisecond, 8*time.Millisecond, time.Minute
	sup.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
	sup.Start()
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(delays)
		mu.Unlock()
		if n >= 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("supervisor stopped retrying")
		}
		time.Sleep(time.Millisecond)
	}
	sup.Stop()
	mu.Lock()
	defer mu.Unlock()
	// attempts 1-3 fail fast (1, 2, 4 ms); attempt 4 was stable, so the
	// fourth delay is back to the minimum.
	if delays[0] != time.Millisecond || delays[2] != 4*time.Millisecond || delays[3] != time.Millisecond {
		t.Fatalf("delays = %v, want reset to 1ms after the stable run", delays)
	}
}

// TestHeadscaleSupervisorKeepsRetryingFailedStarts: the old supervisor
// stopped supervising for good when one restart failed, and a failed first
// start disabled Link until the container restarted.
func TestHeadscaleSupervisorKeepsRetryingFailedStarts(t *testing.T) {
	dir := t.TempDir()
	oldBin, oldDir := hsBinary, hsDataDir
	t.Cleanup(func() { hsBinary, hsDataDir = oldBin, oldDir })
	hsDataDir = filepath.Join(dir, "headscale")
	hsBinary = filepath.Join(dir, "headscale-fails")
	if err := os.WriteFile(hsBinary, []byte("#!/bin/sh\necho \"$@\" >> "+dir+"/argv.log\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JM_NET_SERVER_URL", "https://link.example.com:30193")
	t.Setenv("JM_NET_TLS_CERT", "")
	t.Setenv("JM_NET_TLS_KEY", "")
	t.Setenv("JM_NET_HEADSCALE", "on")
	t.Setenv("JM_NET_POLICY", "")
	t.Setenv("JM_NET_NAS_SERVICES", "")
	t.Setenv("JM_NET_NAS_ROUTE", "")

	hs := newHeadscaleSupervisor()
	hs.listen = "127.0.0.1:1" // nothing listens there
	hs.minBackoff, hs.maxBackoff = time.Millisecond, 5*time.Millisecond
	hs.Start()
	deadline := time.Now().Add(10 * time.Second)
	for hs.status.snapshot().Restarts < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("headscale supervisor stopped retrying: %+v", hs.status.snapshot())
		}
		time.Sleep(5 * time.Millisecond)
	}
	st := hs.status.snapshot()
	hs.Stop()
	if !strings.Contains(st.LastError, "exited before listening") {
		t.Fatalf("status = %+v, want the early exit reported", st)
	}
	argv, _ := os.ReadFile(filepath.Join(dir, "argv.log"))
	if strings.Count(string(argv), "serve --config") < 3 {
		t.Fatalf("headscale serve was not retried: %q", argv)
	}
	if _, err := os.Stat(filepath.Join(hsDataDir, "policy.hujson")); err != nil {
		t.Fatalf("managed policy was not written before serve: %v", err)
	}
}

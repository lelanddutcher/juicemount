package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/health"
)

func qualifiedNFSProbeTestDeps() nfsWriteProbeAdmissionDeps {
	session := &cache.Session{}
	owner := &bridgeCacheOwner{session: session}
	return nfsWriteProbeAdmissionDeps{
		current: func() nfsWriteProbeSource { return nfsWriteProbeSource{owner, session, "/fixture/fuse"} },
		offline: func() bool { return false },
		remote:  func(*cache.Session) (uint64, bool) { return 1, true },
		fuse:    func(string) bool { return true },
	}
}

func TestColdStartAlwaysArmsNFSWriteVerification(t *testing.T) {
	raw, err := os.ReadFile("cbridge.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	required := `if cfg.MountPoint != "" {
		generation := globalNFSWriteGeneration.Load()
		go confirmNFSMountReady(globalMonitor, cfg.MountPoint, generation)
	}`
	if !strings.Contains(source, required) {
		t.Fatal("configured NFS write verification is no longer armed independently of boot-time FUSE readiness")
	}
	if strings.Contains(source, "if nfsMountReused {") {
		t.Fatal("cold-start write verification regressed to the boot-time NFS reuse gate")
	}
}

type alwaysHealthyNFSRefresher struct{}

func (alwaysHealthyNFSRefresher) RefreshNFS() health.ComponentStatus {
	return health.ComponentStatus{Healthy: true}
}

func TestRunWriteProbeUsesBoundedHelperAndCleansSentinel(t *testing.T) {
	globalNFSWriteProbeProcesses.enable(true)
	defer globalNFSWriteProbeProcesses.stopAndWait(time.Now().Add(time.Second))
	dir := t.TempDir()
	ok, elapsedMs, hint := runWriteProbe(dir, 2*time.Second)
	if !ok {
		t.Fatalf("runWriteProbe failed after %dms: %s", elapsedMs, hint)
	}
	matches, err := filepath.Glob(filepath.Join(dir, ".juicemount-writetest.*.tmp"))
	if err != nil {
		t.Fatalf("glob sentinels: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("write probe left %d sentinel(s): %v", len(matches), matches)
	}
}

func TestNFSWriteProbeProcessTrackerKillsReapsAndClosesStartGate(t *testing.T) {
	sleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Skip("sleep unavailable")
	}
	trueCmd, err := exec.LookPath("true")
	if err != nil {
		t.Skip("true unavailable")
	}
	tracker := newNFSWriteProbeProcessTracker()
	if !tracker.enable(true) {
		t.Fatal("clean tracker refused initial enable")
	}
	done := make(chan error, 1)
	go func() { done <- tracker.run(30*time.Second, sleep, "30") }()
	deadline := time.Now().Add(time.Second)
	for tracker.activeCount() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := tracker.activeCount(); got != 1 {
		t.Fatalf("tracked helpers = %d, want 1", got)
	}
	if err := tracker.run(time.Second, trueCmd); !errors.Is(err, errNFSWriteProbeHelperActive) {
		t.Fatalf("concurrent helper start = %v, want active-helper refusal", err)
	}
	if tracker.enable(true) {
		t.Fatal("tracker re-enabled while a prior helper was still active")
	}
	if clean, active := tracker.stopAndWait(time.Now().Add(time.Second)); !clean || active != 0 {
		t.Fatalf("helper drain = clean:%t active:%d, want clean", clean, active)
	}
	if err := <-done; err == nil {
		t.Fatal("killed helper unexpectedly succeeded")
	}
	if err := tracker.run(time.Second, trueCmd); !errors.Is(err, errNFSWriteProbeDisarmed) {
		t.Fatalf("post-stop helper start = %v, want disarmed", err)
	}
	if !tracker.enable(true) {
		t.Fatal("clean tracker refused re-enable after helper reap")
	}
	if err := tracker.run(time.Second, trueCmd); err != nil {
		t.Fatalf("helper after clean re-enable: %v", err)
	}
}

func TestNFSWriteSentinelCleanupWaitsForReap(t *testing.T) {
	globalNFSWriteProbeProcesses.enable(true)
	globalNFSWriteVerifyArmed.Store(true)
	defer func() {
		globalNFSWriteVerifyArmed.Store(false)
		globalNFSWriteProbeProcesses.stopAndWait(time.Now().Add(time.Second))
	}()

	sentinel := filepath.Join(t.TempDir(), ".juicemount-writetest.1.tmp")
	if err := os.WriteFile(sentinel, []byte("probe"), 0o600); err != nil {
		t.Fatal(err)
	}
	reaped := make(chan struct{})
	cleanupNFSWriteSentinelAfterReap(sentinel, reaped)
	time.Sleep(25 * time.Millisecond)
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("cleanup ran before child reap: %v", err)
	}
	close(reaped)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(sentinel); errors.Is(err, os.ErrNotExist) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("sentinel was not removed after child reap")
}

func TestSoftStopSerializesAgainstLifecycleStart(t *testing.T) {
	// Holding this mutex models NFSServerStart's whole transition. The soft
	// stop must not disarm, snapshot, or tear down until Start releases it.
	globalLifecycleMu.Lock()
	done := make(chan struct{})
	go func() {
		stopServerLocked()
		close(done)
	}()
	select {
	case <-done:
		globalLifecycleMu.Unlock()
		t.Fatal("soft stop crossed an active Start lifecycle")
	case <-time.After(25 * time.Millisecond):
	}
	globalLifecycleMu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("soft stop did not proceed after Start lifecycle released")
	}
}

func TestNFSWriteVerificationRetryBackoff(t *testing.T) {
	wants := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
	}
	for i, want := range wants {
		if got := nfsWriteVerificationRetryDelay(i + 1); got != want {
			t.Fatalf("attempt %d delay = %v, want %v", i+1, got, want)
		}
	}
}

func TestNFSWriteVerificationSelfHealsAfterTransientFailure(t *testing.T) {
	warmupReset()
	defer warmupReset()
	globalNFSWriteVerifyArmed.Store(true)
	defer globalNFSWriteVerifyArmed.Store(false)
	generation := resetNFSWriteReadiness(true, "waiting")
	globalNFSWriteVerifyGeneration.Store(generation)
	oldWait := nfsWriteVerificationWait
	nfsWriteVerificationWait = func(int, uint64) bool { return true }
	defer func() {
		nfsWriteVerificationWait = oldWait
		globalNFSWriteVerifyGeneration.Store(0)
	}()

	var calls atomic.Int32
	probe := func(string, time.Duration) (bool, int64, string) {
		if calls.Add(1) == 1 {
			return false, 5, "transient startup race"
		}
		return true, 4, ""
	}
	verifyNFSMountReadyLoopWithAdmission(alwaysHealthyNFSRefresher{}, t.TempDir(), generation, probe,
		qualifiedNFSProbeTestDeps(), nfsWriteVerificationWait)

	if got := calls.Load(); got != 2 {
		t.Fatalf("probe calls = %d, want 2", got)
	}
	if got := nfsWriteReadinessSnapshot(); !got.Ready || got.Reason != "verified" {
		t.Fatalf("readiness after retry = %+v, want verified", got)
	}
}

func TestDisarmNFSWriteVerifierDrainsActiveProbeAndPreventsRetry(t *testing.T) {
	globalNFSWriteVerifyArmed.Store(true)
	generation := resetNFSWriteReadiness(true, "waiting")
	globalNFSWriteVerifyGeneration.Store(generation)
	defer func() {
		globalNFSWriteVerifyArmed.Store(false)
		globalNFSWriteVerifyGeneration.Store(0)
	}()

	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	var calls atomic.Int32
	probe := func(string, time.Duration) (bool, int64, string) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return false, 5, "blocked"
	}
	mountPoint := t.TempDir()
	go func() {
		verifyNFSMountReadyLoopWithAdmission(alwaysHealthyNFSRefresher{}, mountPoint, generation, probe,
			qualifiedNFSProbeTestDeps(), nfsWriteVerificationWait)
		close(done)
	}()
	<-entered

	disarmed := make(chan bool, 1)
	go func() {
		disarmed <- disarmNFSWriteVerifier("server shutting down", time.Second)
	}()
	select {
	case <-disarmed:
		t.Fatal("disarm returned before the active write probe retired")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if ok := <-disarmed; !ok {
		t.Fatal("disarm timed out after the active probe was released")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("write verifier did not exit after disarm")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("probe calls = %d, want exactly one with no shutdown retry", got)
	}
	if globalNFSWriteVerifyArmed.Load() {
		t.Fatal("write verifier remained armed after shutdown disarm")
	}
}

func TestNFSWriteVerificationBackoffIsInterruptedByDisarm(t *testing.T) {
	globalNFSWriteVerifyArmed.Store(true)
	generation := resetNFSWriteReadiness(true, "waiting")
	globalNFSWriteVerifyGeneration.Store(generation)
	defer globalNFSWriteVerifyGeneration.Store(0)

	started := time.Now()
	done := make(chan bool, 1)
	go func() { done <- waitForNFSWriteVerifier(generation, 30*time.Second) }()
	time.Sleep(25 * time.Millisecond)
	globalNFSWriteVerifyArmed.Store(false)
	resetNFSWriteReadiness(true, "server stopping")
	if continued := <-done; continued {
		t.Fatal("invalidated verifier treated interrupted backoff as complete")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("shutdown waited behind retry backoff: %v", elapsed)
	}
}

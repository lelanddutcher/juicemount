package health

import (
	"errors"
	"fmt"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TestNFSAbsentDecisionTable drives the pure decision state through tick
// sequences covering the full #93 truth table: mount-table absence,
// juicefs liveness, server-up, offline mode, kill switch, tick counting,
// single-flight and backoff. No real mounts, no probes — inputs only.
func TestNFSAbsentDecisionTable(t *testing.T) {
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	qualifying := nfsAbsentInputs{
		Absent: true, JuiceFSAlive: true, ServerUp: true,
		Enabled: true, Now: base,
	}
	mod := func(f func(*nfsAbsentInputs)) nfsAbsentInputs {
		in := qualifying
		f(&in)
		return in
	}

	type step struct {
		name     string
		in       nfsAbsentInputs
		wantFire bool
	}
	cases := []struct {
		name      string
		threshold int
		pre       func(*nfsAbsentState) // optional state seeding
		steps     []step
	}{
		{
			name:      "fires on Nth consecutive qualifying tick",
			threshold: 3,
			steps: []step{
				{"tick1", qualifying, false},
				{"tick2", qualifying, false},
				{"tick3", qualifying, true},
			},
		},
		{
			name:      "healthy tick fully resets streak",
			threshold: 3,
			steps: []step{
				{"tick1", qualifying, false},
				{"tick2", qualifying, false},
				{"healthy", mod(func(i *nfsAbsentInputs) { i.Absent = false; i.Healthy = true }), false},
				{"tick1'", qualifying, false},
				{"tick2'", qualifying, false},
				{"tick3'", qualifying, true},
			},
		},
		{
			name:      "stale-but-present (not absent) never fires and resets streak",
			threshold: 2,
			steps: []step{
				{"absent1", qualifying, false},
				{"stale", mod(func(i *nfsAbsentInputs) { i.Absent = false }), false},
				{"stale2", mod(func(i *nfsAbsentInputs) { i.Absent = false }), false},
				{"stale3", mod(func(i *nfsAbsentInputs) { i.Absent = false }), false},
				{"absent1'", qualifying, false},
				{"absent2'", qualifying, true},
			},
		},
		{
			name:      "juicefs dead tick resets (legacy path owns the dead case)",
			threshold: 3,
			steps: []step{
				{"tick1", qualifying, false},
				{"tick2", qualifying, false},
				{"dead", mod(func(i *nfsAbsentInputs) { i.JuiceFSAlive = false }), false},
				{"tick3", qualifying, false},
				{"tick4", qualifying, false},
				{"tick5", qualifying, true},
			},
		},
		{
			name:      "server listener down blocks and resets",
			threshold: 2,
			steps: []step{
				{"tick1", qualifying, false},
				{"srv-down", mod(func(i *nfsAbsentInputs) { i.ServerUp = false }), false},
				{"srv-down2", mod(func(i *nfsAbsentInputs) { i.ServerUp = false }), false},
				{"tick1'", qualifying, false},
				{"tick2'", qualifying, true},
			},
		},
		{
			name:      "offline mode blocks and resets",
			threshold: 2,
			steps: []step{
				{"tick1", qualifying, false},
				{"offline", mod(func(i *nfsAbsentInputs) { i.Offline = true }), false},
				{"tick1'", qualifying, false},
				{"tick2'", qualifying, true},
			},
		},
		{
			name:      "kill switch blocks and resets",
			threshold: 2,
			steps: []step{
				{"tick1", qualifying, false},
				{"killed", mod(func(i *nfsAbsentInputs) { i.Enabled = false }), false},
				{"killed2", mod(func(i *nfsAbsentInputs) { i.Enabled = false }), false},
				{"tick1'", qualifying, false},
				{"tick2'", qualifying, true},
			},
		},
		{
			name:      "in-flight attempt suppresses a second fire",
			threshold: 1,
			pre:       func(s *nfsAbsentState) { s.inProgress = true },
			steps: []step{
				{"tick1", qualifying, false},
				{"tick2", qualifying, false},
			},
		},
		{
			name:      "backoff window suppresses, expiry re-fires",
			threshold: 1,
			pre: func(s *nfsAbsentState) {
				s.failures = 1
				s.nextAttempt = base.Add(30 * time.Second)
			},
			steps: []step{
				{"inside-backoff", qualifying, false},
				{"still-inside", mod(func(i *nfsAbsentInputs) { i.Now = base.Add(29 * time.Second) }), false},
				{"expired", mod(func(i *nfsAbsentInputs) { i.Now = base.Add(31 * time.Second) }), true},
			},
		},
		{
			name:      "healthy tick clears backoff too",
			threshold: 1,
			pre: func(s *nfsAbsentState) {
				s.failures = 3
				s.nextAttempt = base.Add(time.Hour)
			},
			steps: []step{
				{"healthy", mod(func(i *nfsAbsentInputs) { i.Absent = false; i.Healthy = true }), false},
				{"absent-again", qualifying, true}, // backoff gone, threshold 1 → immediate
			},
		},
		{
			name:      "stale flap does NOT clear backoff (only healthy does)",
			threshold: 1,
			pre: func(s *nfsAbsentState) {
				s.failures = 1
				s.nextAttempt = base.Add(time.Hour)
			},
			steps: []step{
				{"stale", mod(func(i *nfsAbsentInputs) { i.Absent = false }), false},
				{"absent-again", qualifying, false}, // still backing off
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var s nfsAbsentState
			if tc.pre != nil {
				tc.pre(&s)
			}
			for _, st := range tc.steps {
				fire, reason := s.tick(st.in, tc.threshold)
				if fire != st.wantFire {
					t.Fatalf("step %q: fire = %v (reason %q), want %v (state %+v)",
						st.name, fire, reason, st.wantFire, s)
				}
			}
		})
	}
}

// TestAbsentBackoff verifies the exponential growth, the cap, and
// shift-overflow safety.
func TestAbsentBackoff(t *testing.T) {
	prevBase, prevCap := NFSAbsentRemountBackoffBase, NFSAbsentRemountBackoffCap
	NFSAbsentRemountBackoffBase = 1 * time.Minute
	NFSAbsentRemountBackoffCap = 30 * time.Minute
	t.Cleanup(func() {
		NFSAbsentRemountBackoffBase, NFSAbsentRemountBackoffCap = prevBase, prevCap
	})

	cases := []struct {
		failures int
		want     time.Duration
	}{
		{0, 1 * time.Minute}, // clamped to 1
		{1, 1 * time.Minute},
		{2, 2 * time.Minute},
		{3, 4 * time.Minute},
		{4, 8 * time.Minute},
		{5, 16 * time.Minute},
		{6, 30 * time.Minute},   // 32m capped
		{10, 30 * time.Minute},  // deep into cap
		{40, 30 * time.Minute},  // shift-overflow guard
		{500, 30 * time.Minute}, // absurd
	}
	for _, tc := range cases {
		if got := absentBackoff(tc.failures); got != tc.want {
			t.Errorf("absentBackoff(%d) = %v, want %v", tc.failures, got, tc.want)
		}
	}
}

// TestNFSAbsentRemountProductionDefault prevents the reconnect-latency
// regression where a condition that already has four independent safety
// proofs waited for three 10-second polling ticks before remounting NFS.
func TestNFSAbsentRemountProductionDefault(t *testing.T) {
	if defaultNFSAbsentRemountTicks != 1 {
		t.Fatalf("default absent-remount observations = %d, want 1", defaultNFSAbsentRemountTicks)
	}
	s := nfsAbsentState{}
	fire, reason := s.tick(nfsAbsentInputs{
		Absent:       true,
		JuiceFSAlive: true,
		ServerUp:     true,
		Enabled:      true,
		Now:          time.Now(),
	}, defaultNFSAbsentRemountTicks)
	if !fire {
		t.Fatalf("first fully-qualified observation did not fire: %s", reason)
	}
}

// TestAbsentRemountTicksFromEnv verifies JM_NFS_AUTOREMOUNT_TICKS parsing
// with fallback to the default on unset/garbage/non-positive values.
func TestAbsentRemountTicksFromEnv(t *testing.T) {
	prev := NFSAbsentRemountTicks
	NFSAbsentRemountTicks = 3
	t.Cleanup(func() { NFSAbsentRemountTicks = prev })

	t.Setenv("JM_NFS_AUTOREMOUNT_TICKS", "")
	if got := absentRemountTicksFromEnv(); got != 3 {
		t.Errorf("unset: got %d, want 3", got)
	}
	t.Setenv("JM_NFS_AUTOREMOUNT_TICKS", "5")
	if got := absentRemountTicksFromEnv(); got != 5 {
		t.Errorf("=5: got %d, want 5", got)
	}
	t.Setenv("JM_NFS_AUTOREMOUNT_TICKS", "banana")
	if got := absentRemountTicksFromEnv(); got != 3 {
		t.Errorf("garbage: got %d, want 3", got)
	}
	t.Setenv("JM_NFS_AUTOREMOUNT_TICKS", "0")
	if got := absentRemountTicksFromEnv(); got != 3 {
		t.Errorf("non-positive: got %d, want 3", got)
	}
}

// TestIsMountBusyError covers the EBUSY / "Resource busy" bucketing that
// selects the dedicated haunted-mountpoint log line.
func TestIsMountBusyError(t *testing.T) {
	if isMountBusyError(nil) {
		t.Error("nil should not be busy")
	}
	if !isMountBusyError(syscall.EBUSY) {
		t.Error("EBUSY should be busy")
	}
	if !isMountBusyError(fmt.Errorf("wrap: %w", syscall.EBUSY)) {
		t.Error("wrapped EBUSY should be busy")
	}
	if !isMountBusyError(errors.New("mount_nfs: can't mount / from 127.0.0.1 onto /Volumes/zpool: Resource busy")) {
		t.Error("mount_nfs Resource busy text should be busy")
	}
	if isMountBusyError(errors.New("osascript: user canceled")) {
		t.Error("cancelled prompt is not busy")
	}
}

// TestHandleNFSAbsentRecovery exercises the monitor-level handler with
// every probe injected: it must claim the tick, fire exactly once at the
// threshold, single-flight the attempt, record the failure backoff, and
// respect the kill switch. No real mounts are attempted.
func TestHandleNFSAbsentRecovery(t *testing.T) {
	prevAlive := isJuiceFSProcessAliveFn
	prevServerUp := nfsServerUpFn
	prevBase := NFSAbsentRemountBackoffBase
	isJuiceFSProcessAliveFn = func(string) bool { return true }
	nfsServerUpFn = func(string) bool { return true }
	NFSAbsentRemountBackoffBase = 1 * time.Minute
	t.Cleanup(func() {
		isJuiceFSProcessAliveFn = prevAlive
		nfsServerUpFn = prevServerUp
		NFSAbsentRemountBackoffBase = prevBase
	})
	t.Setenv("JM_NFS_AUTOREMOUNT", "") // enabled

	absentSt := ComponentStatus{Healthy: false, Message: nfsMsgNotMounted}

	t.Run("fires once at threshold and backs off after failure", func(t *testing.T) {
		var calls atomic.Int32
		done := make(chan struct{}, 4)
		m := &HealthMonitor{
			cfg:         Config{NFSMountPoint: "/tmp/jm-test-absent"},
			absentTicks: 2,
		}
		m.SetNFSServerAddr("127.0.0.1:0")
		m.EnableNFSAbsentRemount(func() error {
			calls.Add(1)
			done <- struct{}{}
			return errors.New("mount_nfs: Resource busy") // fail → backoff
		})

		if claimed := m.handleNFSAbsentRecovery(absentSt, true); !claimed {
			t.Fatal("tick 1 should be claimed")
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("fired before threshold: calls=%d", got)
		}
		if claimed := m.handleNFSAbsentRecovery(absentSt, true); !claimed {
			t.Fatal("tick 2 should be claimed")
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("remount fn was not invoked at threshold")
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("calls=%d at threshold, want 1", got)
		}

		// Wait until the failure has been folded into state (inProgress
		// cleared), then verify the backoff suppresses immediate re-fire.
		if err := waitFor(5*time.Second, func() bool {
			m.mu.Lock()
			defer m.mu.Unlock()
			return !m.absentState.inProgress
		}); err != nil {
			t.Fatalf("attempt never completed: %v", err)
		}
		m.mu.Lock()
		failures, next := m.absentState.failures, m.absentState.nextAttempt
		m.mu.Unlock()
		if failures != 1 || next.IsZero() {
			t.Fatalf("failure not recorded: failures=%d nextAttempt=%v", failures, next)
		}
		for i := 0; i < 5; i++ {
			m.handleNFSAbsentRecovery(absentSt, true)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("calls=%d during backoff, want 1", got)
		}
	})

	t.Run("success resets failures and backoff", func(t *testing.T) {
		done := make(chan struct{}, 1)
		m := &HealthMonitor{
			cfg:         Config{NFSMountPoint: "/tmp/jm-test-absent"},
			absentTicks: 1,
		}
		m.SetNFSServerAddr("127.0.0.1:0")
		m.EnableNFSAbsentRemount(func() error {
			done <- struct{}{}
			return nil
		})
		m.handleNFSAbsentRecovery(absentSt, true)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("remount fn was not invoked")
		}
		if err := waitFor(5*time.Second, func() bool {
			m.mu.Lock()
			defer m.mu.Unlock()
			return !m.absentState.inProgress
		}); err != nil {
			t.Fatalf("attempt never completed: %v", err)
		}
		m.mu.Lock()
		st := m.absentState
		m.mu.Unlock()
		if st.failures != 0 || !st.nextAttempt.IsZero() || st.streak != 0 {
			t.Fatalf("success did not reset state: %+v", st)
		}
	})

	t.Run("kill switch disables and un-claims", func(t *testing.T) {
		t.Setenv("JM_NFS_AUTOREMOUNT", "0")
		var calls atomic.Int32
		m := &HealthMonitor{
			cfg:         Config{NFSMountPoint: "/tmp/jm-test-absent"},
			absentTicks: 1,
		}
		m.SetNFSServerAddr("127.0.0.1:0")
		m.EnableNFSAbsentRemount(func() error { calls.Add(1); return nil })
		for i := 0; i < 5; i++ {
			if claimed := m.handleNFSAbsentRecovery(absentSt, true); claimed {
				t.Fatal("kill-switched tick must not be claimed")
			}
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("kill switch ignored: calls=%d", got)
		}
	})

	t.Run("timeout verdict is NOT absence", func(t *testing.T) {
		var calls atomic.Int32
		m := &HealthMonitor{
			cfg:         Config{NFSMountPoint: "/tmp/jm-test-absent"},
			absentTicks: 1,
		}
		m.SetNFSServerAddr("127.0.0.1:0")
		m.EnableNFSAbsentRemount(func() error { calls.Add(1); return nil })
		slow := ComponentStatus{Healthy: false, Message: "unresponsive (stat timeout)"}
		for i := 0; i < 5; i++ {
			if claimed := m.handleNFSAbsentRecovery(slow, true); claimed {
				t.Fatal("stat-timeout tick must not be claimed")
			}
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("timeout misread as absence: calls=%d", got)
		}
	})

	t.Run("no callback registered is inert", func(t *testing.T) {
		m := &HealthMonitor{cfg: Config{NFSMountPoint: "/tmp/jm-test-absent"}}
		for i := 0; i < 5; i++ {
			if claimed := m.handleNFSAbsentRecovery(absentSt, true); claimed {
				t.Fatal("unwired handler must not claim")
			}
		}
	})

	t.Run("raw FUSE unhealthy blocks", func(t *testing.T) {
		var calls atomic.Int32
		m := &HealthMonitor{
			cfg:         Config{NFSMountPoint: "/tmp/jm-test-absent"},
			absentTicks: 1,
		}
		m.SetNFSServerAddr("127.0.0.1:0")
		m.EnableNFSAbsentRemount(func() error { calls.Add(1); return nil })
		for i := 0; i < 3; i++ {
			if claimed := m.handleNFSAbsentRecovery(absentSt, false); claimed {
				t.Fatal("fuse-unhealthy tick must not be claimed")
			}
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("fired with unhealthy FUSE: calls=%d", got)
		}
	})
}

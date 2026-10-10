package health

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// countingRetirer stands in for the privileged unmount helper. It records
// calls only; it never touches a mount.
type countingRetirer struct {
	calls    atomic.Int32
	err      error
	unproven atomic.Int32 // calls without proven Dead/Wedged liveness
}

func (r *countingRetirer) RetireStaleNFSMount(_ context.Context, liveness string) error {
	r.calls.Add(1)
	if liveness != "dead" && liveness != "wedged" {
		r.unproven.Add(1)
	}
	return r.err
}

// Recovery hooks are fully local. These probes exercise the production
// checkNFSWithProbe -> runChecks routing seam without touching a real mount.
// "unmounts" counts helper retirements: the only detach this package may
// request, and never by path.
func nfsObservationRecoveryHooks(t *testing.T) (*atomic.Int32, *atomic.Int32) {
	t.Helper()
	oldThreshold, oldHard := NFSStaleThreshold, NFSStaleHardRemountThreshold
	oldAlive := isJuiceFSProcessAliveFn
	NFSStaleThreshold, NFSStaleHardRemountThreshold = 2, 4
	remounts := new(atomic.Int32)
	isJuiceFSProcessAliveFn = func(string) bool { return false }
	t.Cleanup(func() {
		NFSStaleThreshold, NFSStaleHardRemountThreshold = oldThreshold, oldHard
		isJuiceFSProcessAliveFn = oldAlive
	})
	return new(atomic.Int32), remounts
}

func nfsObservationMonitor(remounts *atomic.Int32) *HealthMonitor {
	m := New(Config{NFSMountPoint: "/owned/probe"})
	m.EnableNFSRemount(func() error { remounts.Add(1); return nil })
	return m
}

// nfsObservationMonitorWithHelper wires a counting helper and returns its
// retirement counter in place of the old path-unmount counter.
func nfsObservationMonitorWithHelper(remounts *atomic.Int32) (*HealthMonitor, *atomic.Int32) {
	m := nfsObservationMonitor(remounts)
	r := &countingRetirer{}
	m.SetStaleMountRetirer(r)
	return m, &r.calls
}

func TestNFSObserverTimeoutUnhealthyButCannotRemount(t *testing.T) {
	unmounts, remounts := nfsObservationRecoveryHooks(t)
	m := nfsObservationMonitor(remounts)
	m.mu.Lock()
	m.nfsStaleStreak = NFSStaleHardRemountThreshold - 1
	m.mu.Unlock()
	entered, release := make(chan struct{}), make(chan struct{})
	status, workerDone := m.checkNFSWithProbe(nfsProbeDeps{
		stat: func(string) (os.FileInfo, error) { return nil, nil },
		mountTable: func(string, time.Duration) (bool, error) {
			close(entered)
			<-release // The original getfsstat-like work remains physically held.
			return true, nil
		},
	}, 25*time.Millisecond)
	select {
	case <-entered:
	default:
		close(release)
		<-workerDone
		t.Fatal("mount-table observer never entered")
	}
	select {
	case <-workerDone:
		close(release)
		t.Fatal("observer worker exited before release")
	default:
	}
	if status.Healthy || status.Message != "unresponsive (mount table timeout)" || !status.nfsObserverUnavailable {
		close(release)
		<-workerDone
		t.Fatalf("uncertain table timeout = %+v", status)
	}
	for i := 0; i < NFSStaleHardRemountThreshold+2; i++ {
		m.routeNFSRecovery(status, true)
	}
	m.mu.Lock()
	streak := m.nfsStaleStreak
	m.mu.Unlock()
	if streak != 0 || unmounts.Load() != 0 || remounts.Load() != 0 {
		close(release)
		<-workerDone
		t.Fatalf("uncertain table triggered recovery: streak=%d unmount=%d remount=%d", streak, unmounts.Load(), remounts.Load())
	}
	close(release)
	<-workerDone
}

func TestNFSObserverResolvedErrorResetsStreakWithoutRemount(t *testing.T) {
	unmounts, remounts := nfsObservationRecoveryHooks(t)
	m := nfsObservationMonitor(remounts)
	m.mu.Lock()
	m.nfsStaleStreak = 1
	m.mu.Unlock()
	status, workerDone := m.checkNFSWithProbe(nfsProbeDeps{
		stat:       func(string) (os.FileInfo, error) { return nil, nil },
		mountTable: func(string, time.Duration) (bool, error) { return false, errors.New("query failed") },
	}, time.Second)
	<-workerDone
	if status.Healthy || status.Message != "stale: mount table unreadable: query failed" || !status.nfsObserverUnavailable {
		t.Fatalf("resolved table error = %+v", status)
	}
	m.routeNFSRecovery(status, true)
	m.mu.Lock()
	streak := m.nfsStaleStreak
	m.mu.Unlock()
	if streak != 0 || unmounts.Load() != 0 || remounts.Load() != 0 {
		t.Fatalf("table query error triggered recovery: streak=%d unmount=%d remount=%d", streak, unmounts.Load(), remounts.Load())
	}
}

func TestNFSObservedStatFailureAndAbsenceStillReachRecovery(t *testing.T) {
	// A present-but-stale mount needs a helper retirement before the new
	// birth; a verified-absent mount needs no detach at all.
	for _, tc := range []struct {
		name        string
		statErr     error
		mounted     bool
		want        string
		wantRetires int32
	}{
		{"stat failure", errors.New("stat failed"), false, "stale: stat failed", 1},
		{"verified absence", nil, false, nfsMsgNotMounted, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, remounts := nfsObservationRecoveryHooks(t)
			m, unmounts := nfsObservationMonitorWithHelper(remounts)
			// The exact JuiceFS child's Wait returned: proven dead.
			m.SetJuiceFSLiveness(func() (bool, bool) { return false, true })
			status, workerDone := m.checkNFSWithProbe(nfsProbeDeps{
				stat:       func(string) (os.FileInfo, error) { return nil, tc.statErr },
				mountTable: func(string, time.Duration) (bool, error) { return tc.mounted, nil },
			}, time.Second)
			<-workerDone
			if status.Healthy || status.Message != tc.want || status.nfsObserverUnavailable {
				t.Fatalf("authoritative failure = %+v", status)
			}
			m.routeNFSRecovery(status, true)
			m.routeNFSRecovery(status, true)
			if unmounts.Load() != tc.wantRetires || remounts.Load() != 1 {
				t.Fatalf("genuine failure lost recovery: retire=%d remount=%d", unmounts.Load(), remounts.Load())
			}
		})
	}
}

func TestNFSObservedStatTimeoutStillReachesHardRecovery(t *testing.T) {
	_, remounts := nfsObservationRecoveryHooks(t)
	isJuiceFSProcessAliveFn = func(string) bool { return true }
	m, unmounts := nfsObservationMonitorWithHelper(remounts)
	entered, release := make(chan struct{}), make(chan struct{})
	status, workerDone := m.checkNFSWithProbe(nfsProbeDeps{
		stat: func(string) (os.FileInfo, error) {
			close(entered)
			<-release
			return nil, nil
		},
		mountTable: func(string, time.Duration) (bool, error) { return true, nil },
	}, 25*time.Millisecond)
	select {
	case <-entered:
	default:
		close(release)
		<-workerDone
		t.Fatal("stat phase not entered")
	}
	if status.Healthy || status.Message != "unresponsive (stat timeout)" || status.nfsObserverUnavailable {
		close(release)
		<-workerDone
		t.Fatalf("stat timeout misclassified: %+v", status)
	}
	for i := 0; i < NFSStaleHardRemountThreshold-1; i++ {
		m.routeNFSRecovery(status, true)
	}
	if unmounts.Load() != 0 || remounts.Load() != 0 {
		close(release)
		<-workerDone
		t.Fatal("live JuiceFS should defer before hard threshold")
	}
	m.routeNFSRecovery(status, true)
	close(release)
	<-workerDone
	// H-3 review: past the hard threshold a live (or unproven) JuiceFS still
	// never reaches the helper. An ordinary unmount of a slow-but-live or
	// hard-mounted wedged volume would eject it or wedge it in MNT_LUNMOUNT.
	// The monitor holds the typed helper-required state instead.
	m.mu.RLock()
	state := m.nfsRecovery
	m.mu.RUnlock()
	if unmounts.Load() != 0 || remounts.Load() != 0 || state != NFSRecoveryHelperRequired {
		t.Fatalf("live JuiceFS reached the helper or remounted over a stale mount: retire=%d remount=%d state=%s", unmounts.Load(), remounts.Load(), state)
	}
}

func TestNFSObserverHealthyAndBusyRemainUnchanged(t *testing.T) {
	m := New(Config{NFSMountPoint: "/owned/probe"})
	healthy, done := m.checkNFSWithProbe(nfsProbeDeps{
		stat:       func(string) (os.FileInfo, error) { return nil, nil },
		mountTable: func(string, time.Duration) (bool, error) { return true, nil },
	}, time.Second)
	<-done
	if !healthy.Healthy || healthy.Message != "ok" || healthy.nfsObserverUnavailable {
		t.Fatalf("healthy observer changed: %+v", healthy)
	}
	m.SetDrainProbe(func() (int64, time.Duration) { return 1, 0 })
	entered, release := make(chan struct{}), make(chan struct{})
	busy, workerDone := m.checkNFSWithProbe(nfsProbeDeps{
		stat: func(string) (os.FileInfo, error) { return nil, nil },
		mountTable: func(string, time.Duration) (bool, error) {
			close(entered)
			<-release
			return true, nil
		},
	}, 25*time.Millisecond)
	select {
	case <-entered:
	default:
		close(release)
		<-workerDone
		t.Fatal("busy table phase not entered")
	}
	close(release)
	<-workerDone
	if !busy.Healthy || busy.Message != "busy (heavy ingest)" || busy.nfsObserverUnavailable {
		t.Fatalf("busy suppression changed: %+v", busy)
	}
}

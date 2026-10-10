package health

import (
	"sync/atomic"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

func TestOwnedFUSEMonitorUsesRetainedEvidenceWithoutProcessTitle(t *testing.T) {
	old := isJuiceFSProcessAliveFn
	var psCalls atomic.Int32
	isJuiceFSProcessAliveFn = func(string) bool { psCalls.Add(1); return true }
	t.Cleanup(func() { isJuiceFSProcessAliveFn = old })
	m := &HealthMonitor{cfg: Config{FUSEPath: "/owned/long-path-that-ps-truncates"}}
	evidence := FUSESteadyEvidence{Ready: true, ChildAlive: true, ChildProven: true}
	m.SetOwnedFUSEProbe(func() FUSESteadyEvidence { return evidence })
	if st := m.checkFUSE(); !st.Healthy {
		t.Fatalf("retained child and exact mount were healthy: %+v", st)
	}
	for _, e := range []FUSESteadyEvidence{
		{},
		{ChildAlive: true, ChildProven: true}, // live child, missing mount proof
		{Ready: true, ChildAlive: false, ChildProven: true},
		{Ready: true, ChildAlive: true, ChildProven: false},
	} {
		evidence = e
		if st := m.checkFUSE(); st.Healthy {
			t.Fatalf("unproven owned FUSE reported healthy: %+v evidence=%+v", st, e)
		}
	}
	if psCalls.Load() != 0 {
		t.Fatalf("owned health fell back to truncated process title %d times", psCalls.Load())
	}
}

func TestOwnedFUSEEvidenceRequiresOriginalWaitAndUnchangedMountIdentity(t *testing.T) {
	fm, receipt, snapshot := ownedReadinessFixture(t)
	if e := fm.SteadyEvidence(); !e.Ready || !e.ChildProven || !e.ChildAlive {
		t.Fatalf("exact long-path mount was not ready: %+v", e)
	}
	snapshot.Entries[0].Source = "JuiceFS:foreign"
	if e := fm.SteadyEvidence(); e.Ready || !e.ChildProven || !e.ChildAlive {
		t.Fatalf("foreign replacement retained healthy mount identity: %+v", e)
	}
	exited := make(chan struct{})
	close(exited)
	receipt.waitDone = exited
	if e := fm.SteadyEvidence(); e.Ready || !e.ChildProven || e.ChildAlive {
		t.Fatalf("exited original child remained healthy or unproven: %+v", e)
	}
}

func TestOwnedFUSEAbsentRecoveryRequiresProvenOriginalWait(t *testing.T) {
	user, auto := pin.IsUserOffline(), pin.IsAutoOffline()
	pin.SetOffline(false)
	pin.SetAutoOffline(false, "owned FUSE monitor test")
	t.Cleanup(func() {
		pin.SetOffline(user)
		pin.SetAutoOffline(auto, "restore owned FUSE monitor test")
	})
	oldPS, oldServer := isJuiceFSProcessAliveFn, nfsServerUpFn
	var psCalls atomic.Int32
	isJuiceFSProcessAliveFn = func(string) bool { psCalls.Add(1); return true }
	nfsServerUpFn = func(string) bool { return true }
	t.Cleanup(func() { isJuiceFSProcessAliveFn, nfsServerUpFn = oldPS, oldServer })
	t.Setenv("JM_NFS_AUTOREMOUNT", "")
	m := &HealthMonitor{cfg: Config{FUSEPath: "/owned/fuse", NFSMountPoint: "/owned/nfs"}, absentTicks: 100}
	m.SetNFSServerAddr("127.0.0.1:11049")
	m.EnableNFSAbsentRemount(func() error { t.Error("remount fired before threshold"); return nil })
	m.SetOwnedFUSEProbe(func() FUSESteadyEvidence { return FUSESteadyEvidence{Ready: true, ChildAlive: true, ChildProven: true} })
	absent := ComponentStatus{Message: nfsMsgNotMounted}
	if m.handleNFSAbsentRecovery(absent, true) {
		t.Fatal("unproven original Wait claimed absent recovery")
	}
	m.SetJuiceFSLiveness(func() (bool, bool) { return false, false })
	if m.handleNFSAbsentRecovery(absent, true) {
		t.Fatal("unknown original Wait claimed absent recovery")
	}
	m.SetJuiceFSLiveness(func() (bool, bool) { return true, true })
	if !m.handleNFSAbsentRecovery(absent, true) {
		t.Fatal("live proven original did not qualify absent recovery")
	}
	if psCalls.Load() != 0 {
		t.Fatalf("owned absence gate used truncated process title %d times", psCalls.Load())
	}
}

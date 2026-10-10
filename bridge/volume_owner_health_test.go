package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

type heldSteadyOwnerManager struct {
	evidence func() health.FUSESteadyEvidence
	calls    atomic.Int32
}

func (*heldSteadyOwnerManager) Mount() error  { return nil }
func (*heldSteadyOwnerManager) StartMonitor() {}
func (*heldSteadyOwnerManager) Stop() health.FUSEStopResult {
	return health.FUSEStopResult{State: health.FUSEStopped}
}
func (*heldSteadyOwnerManager) OwnerSnapshot() health.FUSEOwnerSnapshot {
	return health.FUSEOwnerSnapshot{MountPath: "/owned/exact-fuse"}
}
func (m *heldSteadyOwnerManager) SteadyEvidence() health.FUSESteadyEvidence {
	m.calls.Add(1)
	return m.evidence()
}

func selectedHealthOwner(t *testing.T, manager *heldSteadyOwnerManager) (*volumeOwner, *ownerFacade) {
	t.Helper()
	o, f := policyPair()
	o.manager, o.status = manager, manager
	o.snapshot = manager.OwnerSnapshot()
	o.endpoint = "redis://127.0.0.1:6379"
	f.claimed = o.receiptLocked()
	f.client.path = operationReplyServer(t, o.dispatch)
	return o, f
}

func TestSelectedOwnerFUSEHealthUsesBoundControlSocket(t *testing.T) {
	m := &heldSteadyOwnerManager{evidence: func() health.FUSESteadyEvidence {
		return health.FUSESteadyEvidence{Ready: true, ChildAlive: true, ChildProven: true}
	}}
	o, f := selectedHealthOwner(t, m)
	for i := 0; i < 3; i++ {
		if e := f.fuseEvidence(); !e.Ready || !e.ChildAlive || !e.ChildProven {
			t.Fatalf("selected owner observation %d = %+v", i, e)
		}
	}
	if m.calls.Load() != 3 || len(o.ops) != 0 {
		t.Fatalf("health consumed operation table or missed manager: calls %d, ops %d", m.calls.Load(), len(o.ops))
	}
	request := func(values []string, epoch int) servicecontrol.Response {
		return o.dispatch(servicecontrol.Request{Version: servicecontrol.ProtocolVersion, Build: version.Commit,
			Method: "fuse-health", Strings: values, Ints: []int{epoch}})
	}
	base := []string{f.intent.Instance, f.intent.ConfigDigest, f.intent.Root, f.token}
	for i := range base {
		bad := append([]string(nil), base...)
		bad[i] = "foreign"
		if r := request(bad, int(f.epoch)); r.Error == "" {
			t.Fatalf("identity field %d admitted: %+v", i, r)
		}
	}
	if r := request(base, int(f.epoch)-1); r.Error == "" || m.calls.Load() != 3 {
		t.Fatalf("stale epoch reached original manager: %+v, calls %d", r, m.calls.Load())
	}
	o.mu.Lock()
	o.attachmentEpoch++
	o.attachmentToken = "33333333333333333333333333333333"
	o.mu.Unlock()
	if e := f.fuseEvidence(); e.Ready || e.ChildProven || m.calls.Load() != 3 {
		t.Fatalf("superseded facade accepted owner proof: %+v, calls %d", e, m.calls.Load())
	}
}

func TestSelectedOwnerFUSEHealthRejectsTransitionDuringSnapshot(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	m := &heldSteadyOwnerManager{evidence: func() health.FUSESteadyEvidence {
		close(entered)
		<-release
		return health.FUSESteadyEvidence{Ready: true, ChildAlive: true, ChildProven: true}
	}}
	o, f := selectedHealthOwner(t, m)
	var once sync.Once
	defer once.Do(func() { close(release) })
	done := make(chan health.FUSESteadyEvidence, 1)
	go func() { done <- f.fuseEvidence() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("owner did not enter local snapshot")
	}
	o.mu.Lock()
	o.stopRequested = true
	o.mu.Unlock()
	once.Do(func() { close(release) })
	select {
	case e := <-done:
		if e.Ready || e.ChildProven {
			t.Fatalf("old snapshot promoted after Stop admission: %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("owner health control did not settle")
	}
}

func TestSelectedOwnerFUSEHealthRejectsLostReplyAndFacadeReattachment(t *testing.T) {
	m := &heldSteadyOwnerManager{evidence: func() health.FUSESteadyEvidence {
		return health.FUSESteadyEvidence{Ready: true, ChildAlive: true, ChildProven: true}
	}}
	_, f := selectedHealthOwner(t, m)
	f.policyCall = func(ctx context.Context, path string, req servicecontrol.Request) (servicecontrol.Response, error) {
		resp, err := servicecontrol.Call(ctx, path, req)
		if err == nil {
			f.mu.Lock()
			f.epoch++ // a newer attachment won after the original observation
			f.mu.Unlock()
		}
		return resp, err
	}
	if e := f.fuseEvidence(); e.Ready || e.ChildProven || m.calls.Load() != 1 {
		t.Fatalf("stale post-IPC facade proof accepted: %+v, calls %d", e, m.calls.Load())
	}
	f.mu.Lock()
	f.epoch--
	f.mu.Unlock()
	f.policyCall = func(ctx context.Context, path string, req servicecontrol.Request) (servicecontrol.Response, error) {
		_, _ = servicecontrol.Call(ctx, path, req)
		return servicecontrol.Response{}, context.DeadlineExceeded
	}
	if e := f.fuseEvidence(); e.Ready || e.ChildProven || m.calls.Load() != 2 {
		t.Fatalf("lost post-dispatch owner reply promoted health: %+v, calls %d", e, m.calls.Load())
	}
}

func TestStaleNFSOwnerLivenessUsesOriginalWaitControl(t *testing.T) {
	m := &heldSteadyOwnerManager{evidence: func() health.FUSESteadyEvidence {
		return health.FUSESteadyEvidence{ChildAlive: false, ChildProven: true}
	}}
	_, f := selectedHealthOwner(t, m)
	globalMu.Lock()
	priorFUSE, priorOwner := globalFUSE, globalOwner
	globalFUSE, globalOwner = nil, f
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalFUSE, globalOwner = priorFUSE, priorOwner
		globalMu.Unlock()
	})
	if alive, proven := exactJuiceFSLiveness(); alive || !proven {
		t.Fatalf("owner's original Wait death was not proven: alive %v proven %v", alive, proven)
	}
	m.evidence = func() health.FUSESteadyEvidence { return health.FUSESteadyEvidence{} }
	if alive, proven := exactJuiceFSLiveness(); alive || proven {
		t.Fatalf("unknown owner Wait licensed helper: alive %v proven %v", alive, proven)
	}
	m.evidence = func() health.FUSESteadyEvidence {
		globalMu.Lock()
		globalOwner = nil // selected owner changed while its control reply was in flight
		globalMu.Unlock()
		return health.FUSESteadyEvidence{ChildAlive: false, ChildProven: true}
	}
	if alive, proven := exactJuiceFSLiveness(); alive || proven {
		t.Fatalf("former owner's death proof licensed helper: alive %v proven %v", alive, proven)
	}
}

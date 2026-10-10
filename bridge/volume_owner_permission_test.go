//go:build !serviceclient

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
)

type deniedOwnerManager struct {
	state health.FUSEStopState
	delay time.Duration
}

func (m *deniedOwnerManager) OwnerSnapshot() health.FUSEOwnerSnapshot {
	return health.FUSEOwnerSnapshot{}
}
func (m *deniedOwnerManager) Mount() error  { return errors.New("not used") }
func (m *deniedOwnerManager) StartMonitor() {}
func (m *deniedOwnerManager) Stop() health.FUSEStopResult {
	time.Sleep(m.delay) // keeps the operation pending past the first reply
	return health.FUSEStopResult{State: m.state, Detail: "fixture custodian"}
}

// The engine must receive the owner's typed StopResult on the FIRST failed
// Stop, not "outcome unknown", so it can tell the user what to grant.
func TestSelectedOwnerStopReturnsTypedReceiptOnDefinitiveFailure(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	o.config = "selected-config"
	m := &deniedOwnerManager{state: health.FUSEPermissionDenied, delay: 150 * time.Millisecond}
	o.manager, o.status = m, m
	directory, err := os.MkdirTemp(os.TempDir(), "vo-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "o.sock")
	server, err := servicecontrol.Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(o.dispatch) }()
	defer func() {
		_ = server.Close()
		select {
		case <-served:
		case <-time.After(time.Second):
			t.Error("owner listener not joined")
		}
	}()
	intent := volumeOwnerIntent{Instance: o.instance, Root: o.root, ConfigDigest: o.config}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const id = "00112233445566778899aabbccddeeff"
	r, err := selectedOwnerOperation(ctx, path, intent, "stop", id, nil, nil)
	if err == nil {
		t.Fatal("retained FUSE reported a completed owner Stop")
	}
	if r.OperationState != "error" || !strings.HasPrefix(r.StopResult, string(health.FUSEPermissionDenied)) {
		t.Fatalf("definitive failure lost its typed receipt: state=%q stop=%q err=%v", r.OperationState, r.StopResult, err)
	}
	if o.manager != m {
		t.Fatal("failed Stop released the retained manager")
	}
}

func TestShutdownReportsTypedNetworkVolumesReasonWhenOwnerRetainsFUSE(t *testing.T) {
	globalMu.Lock()
	if globalFUSE != nil || globalOwner != nil || globalServer != nil || globalStopPending || globalMountPath != "" {
		globalMu.Unlock()
		t.Fatal("fixture requires idle bridge")
	}
	owner := &ownerFacade{}
	globalOwner = owner
	globalMu.Unlock()
	oldMode := servingProcessMode.Load()
	servingProcessMode.Store(false)
	oldStop := stopSelectedOwner
	t.Cleanup(func() {
		stopSelectedOwner = oldStop
		servingProcessMode.Store(oldMode)
		globalMu.Lock()
		globalOwner, globalOwnerStopID, globalStopPending = nil, "", false
		globalMu.Unlock()
	})
	for _, tc := range []struct {
		state health.FUSEStopState
		want  string
	}{
		{health.FUSEPermissionDenied, shutdownReasonNetworkVolumesNotGranted},
		{health.FUSEAdmissionPending, shutdownReasonFUSEAdmissionPending},
		{health.FUSEPresent, shutdownReasonFUSERetained},
	} {
		calls := 0
		stopSelectedOwner = func(got *ownerFacade, id string) (volumeOwnerReceipt, error) {
			calls++
			if got != owner || len(id) != 32 {
				t.Fatalf("wrong owner or stop ID: %p %q", got, id)
			}
			return volumeOwnerReceipt{Operation: id, OperationState: "error",
				StopResult: string(tc.state) + ": fixture"}, errors.New("original FUSE stop or mount worker incomplete")
		}
		if shutdownServer(false) != 0 {
			t.Fatalf("%s: shutdown reported success while the owner retained FUSE", tc.state)
		}
		out := lastShutdownOutcome()
		if calls != 1 || out.Complete || out.Reason != tc.want || !strings.Contains(out.Detail, string(tc.state)) {
			t.Fatalf("%s: untyped shutdown outcome %+v calls=%d", tc.state, out, calls)
		}
		globalMu.Lock()
		retained, stopID := globalOwner == owner, globalOwnerStopID
		globalMu.Unlock()
		if !retained || stopID != "" {
			t.Fatalf("%s: owner dropped or definitive stop ID kept: retained=%v id=%q", tc.state, retained, stopID)
		}
	}
}

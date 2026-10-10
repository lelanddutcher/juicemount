package main

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

func ownerShutdownRequest(o *volumeOwner, id, digest string) servicecontrol.Request {
	return servicecontrol.Request{Version: servicecontrol.ProtocolVersion, Build: version.Commit,
		Method: "shutdown", Strings: []string{o.instance, id, digest}}
}

func TestVolumeOwnerTerminalShutdownSelectedAndNoResources(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	o.config = "selected-digest"
	const id = "1234567890abcdef1234567890abcdef"
	for _, req := range []servicecontrol.Request{
		ownerShutdownRequest(o, id, "foreign"),
		{Version: servicecontrol.ProtocolVersion, Build: version.Commit, Method: "shutdown", Strings: []string{"foreign", id, o.config}},
	} {
		if resp := o.dispatch(req); resp.Error == "" {
			t.Fatal("foreign terminal shutdown admitted")
		}
	}
	if o.terminalRequested {
		t.Fatal("foreign request latched terminal admission")
	}
	m := &heldOwnerManager{stopState: health.FUSEInFlight}
	o.manager = m
	if resp := o.dispatch(ownerShutdownRequest(o, id, o.config)); resp.Error == "" {
		t.Fatal("shutdown accepted live manager")
	}
	if o.terminalRequested {
		t.Fatal("live-resource refusal became terminal")
	}
	o.manager = nil
	const retryID = "abcdefabcdefabcdefabcdefabcdefab"
	resp := o.dispatch(ownerShutdownRequest(o, retryID, o.config))
	if resp.Error != "" {
		t.Fatalf("qualified empty owner shutdown refused: %s", resp.Error)
	}
	var receipt volumeOwnerReceipt
	if err := json.Unmarshal([]byte(resp.Text), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Operation != retryID || receipt.OperationState != "complete" || !o.terminalRequested {
		t.Fatalf("terminal original result not retained: %+v", receipt)
	}
	if again := o.dispatch(ownerShutdownRequest(o, retryID, o.config)); again.Error != "" || again.Text != resp.Text {
		t.Fatal("same original shutdown ID did not preserve result")
	}
	if _, err := o.begin("mount", "11111111111111111111111111111111", o.config, func() (volumeOwnerReceipt, error) {
		return volumeOwnerReceipt{}, nil
	}); err == nil {
		t.Fatal("mutation admitted after terminal latch")
	}
}

func TestVolumeOwnerShutdownWaitsForOriginalOperationAndLinkRelease(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	o.config = "selected-digest"
	const activeID = "11111111111111111111111111111111"
	const shutdownID = "22222222222222222222222222222222"
	releaseWork := make(chan struct{})
	var once sync.Once
	if _, err := o.begin("mount", activeID, o.config, func() (volumeOwnerReceipt, error) {
		<-releaseWork
		return volumeOwnerReceipt{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		once.Do(func() { close(releaseWork) })
		o.mu.Lock()
		op := o.ops[activeID]
		o.mu.Unlock()
		select {
		case <-op.done:
		case <-time.After(time.Second):
			t.Error("original operation not joined")
		}
	}()
	if resp := o.dispatch(ownerShutdownRequest(o, shutdownID, o.config)); resp.Error == "" {
		t.Fatal("shutdown accepted active original operation")
	}
	if o.terminalRequested {
		t.Fatal("active-operation refusal latched terminal")
	}
	once.Do(func() { close(releaseWork) })
	o.mu.Lock()
	op := o.ops[activeID]
	o.mu.Unlock()
	select {
	case <-op.done:
	case <-time.After(time.Second):
		t.Fatal("original operation not joined")
	}
	o.mu.Lock()
	o.linkReleasePending = true
	o.mu.Unlock()
	if resp := o.dispatch(ownerShutdownRequest(o, "33333333333333333333333333333333", o.config)); resp.Error == "" {
		t.Fatal("shutdown accepted Link release still in progress")
	}
	o.mu.Lock()
	o.linkReleasePending = false
	o.mu.Unlock()
	if resp := o.dispatch(ownerShutdownRequest(o, "44444444444444444444444444444444", o.config)); resp.Error != "" {
		t.Fatalf("terminal owner remained blocked after original joins: %s", resp.Error)
	}
}

func TestVolumeOwnerFreshUnclaimedShutdown(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	resp := o.dispatch(ownerShutdownRequest(o, "fedcba9876543210fedcba9876543210", ""))
	if resp.Error != "" || !o.terminalRequested {
		t.Fatalf("fresh empty owner could not retire: %s", resp.Error)
	}
}

func TestVolumeOwnerEmptyStopWaitsForRetainedWork(t *testing.T) {
	for _, tc := range []struct {
		name string
		hold func(*volumeOwner)
	}{
		{"status", func(o *volumeOwner) { o.status = &heldOwnerManager{} }},
		{"link-release", func(o *volumeOwner) { o.releaseLink = func() {} }},
		{"release-pending", func(o *volumeOwner) { o.linkReleasePending = true }},
		{"mount-active", func(o *volumeOwner) { o.mountActive = true }},
		{"probe-active", func(o *volumeOwner) { o.probeActive = 1 }},
		{"callback-active", func(o *volumeOwner) { o.enrollActive = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, err := newVolumeOwner(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			o.claimDigest = "selected"
			tc.hold(o)
			const firstID = "11111111111111111111111111111111"
			if _, err := o.begin("stop", firstID, o.claimDigest, o.stop); err != nil {
				t.Fatal(err)
			}
			if op := waitClaimIdentityOperation(t, o, firstID); op.err == nil || o.retired || !o.stopRequested {
				t.Fatalf("retained resource falsely stopped: %v", op.err)
			}
			o.mu.Lock()
			o.status, o.releaseLink, o.linkReleasePending = nil, nil, false
			o.mountActive, o.probeActive, o.enrollActive = false, 0, 0
			o.mu.Unlock()
			const retryID = "22222222222222222222222222222222"
			if _, err := o.begin("stop", retryID, o.claimDigest, o.stop); err != nil {
				t.Fatal(err)
			}
			if op := waitClaimIdentityOperation(t, o, retryID); op.err != nil || !o.retired {
				t.Fatalf("settled empty owner did not stop: %v", op.err)
			}
		})
	}
}

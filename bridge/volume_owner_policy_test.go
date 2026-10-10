package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

const policyTestInstance = "11111111111111111111111111111111"
const policyTestToken = "22222222222222222222222222222222"
const policyTestDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func policyPair() (*volumeOwner, *ownerFacade) {
	o := &volumeOwner{instance: policyTestInstance, root: "private-root", config: policyTestDigest,
		attachmentEpoch: 1, attachmentToken: policyTestToken, ops: make(map[string]*volumeOwnerOperation)}
	f := &ownerFacade{intent: volumeOwnerIntent{Instance: policyTestInstance, Root: "private-root", ConfigDigest: policyTestDigest},
		client: &volumeOwnerClient{path: "owned-policy-socket"}, epoch: 1, token: policyTestToken}
	return o, f
}

func policyReply(r volumeOwnerReceipt) servicecontrol.Response {
	b, _ := json.Marshal(r)
	return servicecontrol.Response{Version: servicecontrol.ProtocolVersion, Text: string(b)}
}

// The production owner has its own process-local offline flags. Preserve the
// simulated engine's flags across this in-process owner call; the owner's
// receipt and policy fields still reflect the dispatched request.
func isolatedPolicyDispatch(o *volumeOwner, req servicecontrol.Request) servicecontrol.Response {
	user, auto, reason := pin.IsUserOffline(), pin.IsAutoOffline(), pin.State().Reason
	r := o.dispatch(req)
	if req.Method == "offline" {
		pin.SetOffline(user)
		pin.SetAutoOffline(auto, reason)
	}
	return r
}

func TestOwnerCostLostReplySettlesOriginalBeforeNewValue(t *testing.T) {
	o, f := policyPair()
	before := o.receiptLocked()
	drop, staleInspect, mutations := true, true, 0
	f.policyCall = func(_ context.Context, _ string, req servicecontrol.Request) (servicecontrol.Response, error) {
		if req.Method == "inspect" && staleInspect {
			staleInspect = false
			return policyReply(before), nil
		}
		if req.Method == "cost" {
			mutations++
		}
		r := isolatedPolicyDispatch(o, req)
		if req.Method == "cost" && drop {
			drop = false // original accepted; client lost only the response
			return servicecontrol.Response{}, context.DeadlineExceeded
		}
		return r, nil
	}
	if err := f.setCost(true, false); err == nil || f.costPending == nil || mutations != 1 {
		t.Fatalf("lost original must remain pending, err=%v pending=%v mutations=%d", err, f.costPending, mutations)
	}
	if err := f.setCost(false, true); err != nil {
		t.Fatalf("settle accepted original then publish newer value: %v", err)
	}
	if mutations != 2 || o.costSequence != 2 || o.costExpensive || !o.costConstrained || f.costPending != nil {
		t.Fatalf("incorrect original/new policy sequence: mutations=%d owner=%+v pending=%v", mutations, o.receiptLocked(), f.costPending)
	}
}

func TestOwnerOfflineLostReplySettlesBeforeNewValue(t *testing.T) {
	o, f := policyPair()
	oldAuto := pin.IsAutoOffline()
	defer func() { pin.SetAutoOffline(oldAuto, "restore policy test") }()
	before := o.receiptLocked()
	drop, staleInspect, mutations := true, true, 0
	f.policyCall = func(_ context.Context, _ string, req servicecontrol.Request) (servicecontrol.Response, error) {
		if req.Method == "inspect" && staleInspect {
			staleInspect = false
			return policyReply(before), nil
		}
		if req.Method == "offline" {
			mutations++
		}
		r := isolatedPolicyDispatch(o, req)
		if req.Method == "offline" && drop {
			drop = false
			return servicecontrol.Response{}, context.DeadlineExceeded
		}
		return r, nil
	}
	pin.SetAutoOffline(true, "policy test")
	if err := f.syncOffline(); err == nil || f.offlinePending == nil || mutations != 1 {
		t.Fatalf("lost offline update must remain pending, err=%v pending=%v mutations=%d", err, f.offlinePending, mutations)
	}
	pin.SetAutoOffline(false, "policy test")
	if err := f.syncOffline(); err != nil {
		t.Fatalf("settle original before new offline value: %v", err)
	}
	if mutations != 2 || o.offlineSequence != 2 || o.autoOffline || f.offlinePending != nil {
		t.Fatalf("incorrect offline sequence: mutations=%d owner=%+v pending=%v", mutations, o.receiptLocked(), f.offlinePending)
	}
}

func TestOwnerPolicyRejectsStaleEpochAndSameSequenceDifferentPayload(t *testing.T) {
	o, f := policyPair()
	_ = f
	req := func(exp, con, epoch, seq int) servicecontrol.Request {
		return servicecontrol.Request{Version: servicecontrol.ProtocolVersion, Build: version.Commit, Method: "cost",
			Strings: []string{policyTestInstance, policyTestDigest, "private-root"}, Ints: []int{exp, con, epoch, seq}}
	}
	first := o.dispatch(req(1, 0, 1, 0))
	if first.Error != "" {
		t.Fatal(first.Error)
	}
	if different := o.dispatch(req(0, 1, 1, 0)); different.Error == "" || o.costSequence != 1 || !o.costExpensive {
		t.Fatalf("same original sequence rebound to different payload: %+v", different)
	}
	o.attachmentEpoch = 2
	o.attachmentToken = "33333333333333333333333333333333"
	if stale := o.dispatch(req(0, 1, 1, 1)); stale.Error == "" || o.costSequence != 1 {
		t.Fatalf("old attachment changed newer owner policy: %+v", stale)
	}
}

func TestOwnerCostUnadmittedTimeoutReplaysOnlyOriginalThenNewValue(t *testing.T) {
	o, f := policyPair()
	missed, attempts := true, 0
	f.policyCall = func(_ context.Context, _ string, req servicecontrol.Request) (servicecontrol.Response, error) {
		if req.Method == "cost" {
			attempts++
			if missed {
				missed = false // request never reached owner
				return servicecontrol.Response{}, context.DeadlineExceeded
			}
		}
		return isolatedPolicyDispatch(o, req), nil
	}
	if err := f.setCost(true, false); err == nil || f.costPending == nil || o.costSequence != 0 {
		t.Fatalf("unadmitted original must remain pending: err=%v pending=%v owner=%+v", err, f.costPending, o.receiptLocked())
	}
	if err := f.setCost(false, true); err != nil {
		t.Fatalf("recovered original then newer cost: %v", err)
	}
	if attempts != 3 || o.costSequence != 2 || o.costExpensive || !o.costConstrained || f.costPending != nil {
		t.Fatalf("newer cost bypassed original: attempts=%d owner=%+v pending=%v", attempts, o.receiptLocked(), f.costPending)
	}
}

func TestOwnerOfflineUnadmittedTimeoutReplaysOnlyOriginalThenNewValue(t *testing.T) {
	o, f := policyPair()
	oldAuto := pin.IsAutoOffline()
	defer pin.SetAutoOffline(oldAuto, "restore policy test")
	missed, attempts := true, 0
	f.policyCall = func(_ context.Context, _ string, req servicecontrol.Request) (servicecontrol.Response, error) {
		if req.Method == "offline" {
			attempts++
			if missed {
				missed = false
				return servicecontrol.Response{}, context.DeadlineExceeded
			}
		}
		return isolatedPolicyDispatch(o, req), nil
	}
	pin.SetAutoOffline(true, "policy test")
	if err := f.syncOffline(); err == nil || f.offlinePending == nil || o.offlineSequence != 0 {
		t.Fatalf("unadmitted original must remain pending: err=%v pending=%v owner=%+v", err, f.offlinePending, o.receiptLocked())
	}
	pin.SetAutoOffline(false, "policy test")
	if err := f.syncOffline(); err != nil {
		t.Fatalf("recovered original then newer offline state: %v", err)
	}
	if attempts != 3 || o.offlineSequence != 2 || o.autoOffline || f.offlinePending != nil {
		t.Fatalf("newer offline state bypassed original: attempts=%d owner=%+v pending=%v", attempts, o.receiptLocked(), f.offlinePending)
	}
}

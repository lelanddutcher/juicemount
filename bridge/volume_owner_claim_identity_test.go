package main

import (
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

func claimIdentityFixture(t *testing.T, cfg ServerConfig) (*volumeOwner, string) {
	t.Helper()
	canonical := canonicalServerConfig(cfg)
	digest, err := servingConfigDigest(canonical)
	if err != nil {
		t.Fatal(err)
	}
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	o.claimDigest = digest
	o.intentBound = true
	o.intent = volumeOwnerIntent{Instance: o.instance, ClaimID: strings.Repeat("1", 32), ConfigDigest: digest}
	raw, err := json.Marshal(canonical)
	if err != nil {
		t.Fatal(err)
	}
	return o, string(raw)
}

func waitClaimIdentityOperation(t *testing.T, o *volumeOwner, id string) *volumeOwnerOperation {
	t.Helper()
	o.mu.Lock()
	op := o.ops[id]
	o.mu.Unlock()
	if op == nil {
		t.Fatal("original operation was not retained")
	}
	select {
	case <-op.done:
	case <-time.After(time.Second):
		t.Fatal("original operation did not settle")
	}
	return op
}

func TestPreboundCredentialClaimKeepsFullReplayIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  ServerConfig
	}{
		{"redis-password", ServerConfig{RedisURL: "redis://:fixture-secret@127.0.0.1:58512/0"}},
		{"link-auth", ServerConfig{RedisURL: "redis://127.0.0.1:58512/0", NetAuthKey: "fixture-secret"}},
		{"object-credentials", ServerConfig{RedisURL: "redis://127.0.0.1:58512/0", ObjectAccessKey: "fixture-id", ObjectSecretKey: "fixture-secret"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, raw := claimIdentityFixture(t, tc.cfg)
			const id = "11111111111111111111111111111111"
			payload := raw + "\x000\x001"
			var executions atomic.Int32
			work := func() (volumeOwnerReceipt, error) {
				executions.Add(1)
				return volumeOwnerReceipt{}, nil
			}
			if _, err := o.begin("claim", id, payload, work); err != nil {
				t.Fatalf("selected credential-bearing claim: %v", err)
			}
			if op := waitClaimIdentityOperation(t, o, id); op.err != nil {
				t.Fatalf("original claim: %v", op.err)
			}
			if _, err := o.begin("claim", id, payload, work); err != nil || executions.Load() != 1 {
				t.Fatalf("exact replay executed twice or failed: calls=%d err=%v", executions.Load(), err)
			}
			changed := canonicalServerConfig(tc.cfg)
			switch tc.name {
			case "redis-password":
				changed.RedisURL = "redis://:different@127.0.0.1:58512/0"
			case "link-auth":
				changed.NetAuthKey = "different"
			case "object-credentials":
				changed.ObjectSecretKey = "different"
			}
			changedRaw, err := json.Marshal(changed)
			if err != nil {
				t.Fatal(err)
			}
			for _, rebound := range []string{string(changedRaw) + "\x000\x001", raw + "\x001\x001", raw + "\x000\x002"} {
				if _, err := o.begin("claim", id, rebound, work); err == nil || !strings.Contains(err.Error(), "ID rebound") {
					t.Fatalf("changed original payload accepted: %v", err)
				}
			}
			if executions.Load() != 1 {
				t.Fatal("rebound claim executed work")
			}
		})
	}
}

func TestPreboundClaimRejectsDifferentNonsecretConfigBeforeWork(t *testing.T) {
	o, _ := claimIdentityFixture(t, ServerConfig{RedisURL: "redis://:fixture-secret@127.0.0.1:58512/0"})
	changed, err := json.Marshal(canonicalServerConfig(ServerConfig{RedisURL: "redis://:fixture-secret@127.0.0.1:58513/0"}))
	if err != nil {
		t.Fatal(err)
	}
	var executions atomic.Int32
	_, err = o.begin("claim", o.intent.ClaimID, string(changed)+"\x000\x001", func() (volumeOwnerReceipt, error) {
		executions.Add(1)
		return volumeOwnerReceipt{}, nil
	})
	if err == nil || executions.Load() != 0 || len(o.ops) != 0 {
		t.Fatalf("different nonsecret config reached original work: err=%v calls=%d ops=%d", err, executions.Load(), len(o.ops))
	}
}

func TestDispatchCredentialClaimFailureCanStopEmptyOwner(t *testing.T) {
	o, raw := claimIdentityFixture(t, ServerConfig{RedisURL: "redis://:fixture-secret@127.0.0.1:58512/0"})
	claim := o.dispatch(servicecontrol.Request{Version: servicecontrol.ProtocolVersion, Build: version.Commit,
		Method: "claim", Strings: []string{o.instance, o.intent.ClaimID, raw}, Ints: []int{-1, 1}})
	if claim.Error != "" {
		t.Fatalf("selected claim was rejected before original operation: %s", claim.Error)
	}
	if op := waitClaimIdentityOperation(t, o, o.intent.ClaimID); op.err == nil {
		t.Fatal("invalid cache budget did not fail original claim")
	}
	const stopID = "22222222222222222222222222222222"
	stop := o.dispatch(servicecontrol.Request{Version: servicecontrol.ProtocolVersion, Build: version.Commit,
		Method: "stop", Strings: []string{o.instance, stopID, o.intent.ConfigDigest}})
	if stop.Error != "" {
		t.Fatalf("empty selected Stop was rejected: %s", stop.Error)
	}
	if op := waitClaimIdentityOperation(t, o, stopID); op.err != nil || !o.retired {
		t.Fatalf("empty original Stop did not complete: %v", op.err)
	}
	shutdownID := ownerShutdownID(o.intent)
	if response := o.dispatch(ownerShutdownRequest(o, shutdownID, o.intent.ConfigDigest)); response.Error != "" || !o.terminalRequested {
		t.Fatalf("empty stopped owner did not terminalize: %+v", response)
	}
}

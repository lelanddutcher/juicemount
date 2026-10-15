package health

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

type recoveryTestLease struct{ op string }

func (l recoveryTestLease) OpID() string          { return l.op }
func (l recoveryTestLease) Lost() <-chan struct{} { return make(chan struct{}) }
func (l recoveryTestLease) Err() error            { return nil }
func (l recoveryTestLease) Close()                {}

type recoveryTestHelper struct {
	op                                     string
	status                                 client.Outcome
	reclaim                                client.ReclaimResult
	retire                                 client.Outcome
	reclaimErr, retireErr                  error
	statusCalls, reclaimCalls, retireCalls int
}

func (h *recoveryTestHelper) Status(context.Context, string) (client.Outcome, error) {
	h.statusCalls++
	return h.status, nil
}
func (h *recoveryTestHelper) Reclaim(context.Context, string) (UnmountLease, client.ReclaimResult, error) {
	h.reclaimCalls++
	if h.reclaimErr != nil {
		return nil, client.ReclaimResult{}, h.reclaimErr
	}
	return recoveryTestLease{h.op}, h.reclaim, nil
}
func (h *recoveryTestHelper) Retire(_ context.Context, _ string, rr client.RetireRequest, _ time.Duration) (client.Outcome, error) {
	h.retireCalls++
	if rr.Liveness != client.LivenessUnknown || rr.EjectNow {
		panic("recovery requested force or non-unknown liveness")
	}
	return h.retire, h.retireErr
}

func recoveryGenerationFixture(t *testing.T) (RetainedGenerationRecovery, *recoveryTestHelper, func(context.Context) (mounttable.Snapshot, error)) {
	t.Helper()
	logical := generationTestPath(t)
	r, err := reserveFUSEGeneration(logical, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", emptyGenerationSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.markObserved([2]int32{19, 29}); err != nil {
		t.Fatal(err)
	}
	spec := RetainedGenerationRecovery{Root: filepath.Dir(logical), OwnerInstance: "owner", Build: "old-build",
		ConfigDigest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		LogicalPath:  logical, OpID: FUSEHelperOpID(r.Path)}
	path := r.Path
	r.close() // original owner is absent
	h := &recoveryTestHelper{op: spec.OpID,
		status:  client.Outcome{OpID: spec.OpID, State: "REGISTERED"},
		reclaim: client.ReclaimResult{Reclaimed: true, Outcome: client.Outcome{OpID: spec.OpID, State: "REGISTERED"}},
		retire:  client.Outcome{OpID: spec.OpID, State: "RETIRED", Syscalls: 1}}
	calls := 0
	observe := func(context.Context) (mounttable.Snapshot, error) {
		calls++
		entries := []mounttable.MountEntry{}
		if calls == 1 {
			entries = append(entries, mounttable.MountEntry{Path: path, Source: "inert", Type: "macfuse", FSID: [2]int32{19, 29}})
		}
		return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: entries}, nil
	}
	return spec, h, observe
}

func TestRetainedGenerationExactOrdinaryRecovery(t *testing.T) {
	spec, h, observe := recoveryGenerationFixture(t)
	got := RecoverRetainedGeneration(context.Background(), spec, h, observe)
	if !got.Retired || got.Forced || got.State != "RECOVERED_RESOURCE_RETIRED" ||
		h.statusCalls != 1 || h.reclaimCalls != 1 || h.retireCalls != 1 {
		t.Fatalf("exact ordinary recovery: %+v helper %+v", got, h)
	}
	eventsPath := spec.LogicalPath + ".generations/ledger.jsonl"
	data, err := os.ReadFile(eventsPath)
	if err != nil || len(data) == 0 {
		t.Fatalf("ledger unavailable: %v", err)
	}
	rerun := RecoverRetainedGeneration(context.Background(), spec, h, observe)
	if rerun.Retired || h.reclaimCalls != 1 || h.retireCalls != 1 {
		t.Fatalf("rerun replayed helper: %+v helper %+v", rerun, h)
	}
}

func TestRetainedGenerationLiveOrUnsafeHelperHolds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*recoveryTestHelper)
	}{
		{"live-reclaim", func(h *recoveryTestHelper) { h.reclaim.Reclaimed = false }},
		{"forced-status", func(h *recoveryTestHelper) { h.status.Forced = true }},
		{"forced-retire", func(h *recoveryTestHelper) { h.retire.Forced = true }},
		{"lost-reclaim-reply", func(h *recoveryTestHelper) { h.reclaimErr = errors.New("lost reply") }},
		{"busy-retire", func(h *recoveryTestHelper) { h.retire.State = "BACKOFF" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec, h, observe := recoveryGenerationFixture(t)
			tc.mutate(h)
			got := RecoverRetainedGeneration(context.Background(), spec, h, observe)
			if got.Retired {
				t.Fatalf("unsafe helper state retired: %+v", got)
			}
			before := h.reclaimCalls
			if before == 0 {
				return
			} // no request was dispatched
			h.status = client.Outcome{OpID: spec.OpID, State: "REGISTERED"}
			again := RecoverRetainedGeneration(context.Background(), spec, h, observe)
			if again.Retired || h.reclaimCalls != before || again.ErrorCode != "previous_helper_attempt_uncertain" {
				t.Fatalf("uncertain attempt replayed: %+v helper %+v", again, h)
			}
		})
	}
}

func TestRetainedGenerationIdentityAndInventoryHoldsBeforeHelper(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(RetainedGenerationRecovery) RetainedGenerationRecovery
	}{
		{"wrong-op", func(s RetainedGenerationRecovery) RetainedGenerationRecovery { s.OpID += "x"; return s }},
		{"wrong-logical", func(s RetainedGenerationRecovery) RetainedGenerationRecovery { s.LogicalPath += "x"; return s }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec, h, observe := recoveryGenerationFixture(t)
			got := RecoverRetainedGeneration(context.Background(), tc.change(spec), h, observe)
			if got.Retired || h.statusCalls != 0 || h.reclaimCalls != 0 {
				t.Fatalf("bad identity reached helper: %+v", got)
			}
		})
	}
}

func TestRetainedGenerationChangedAnchorAndSidecarHold(t *testing.T) {
	for _, leaf := range []string{"anchor", "sidecar"} {
		t.Run(leaf, func(t *testing.T) {
			spec, h, observe := recoveryGenerationFixture(t)
			var path string
			if leaf == "anchor" {
				path = spec.LogicalPath + ".generations.anchor"
			} else {
				nonce, _, _ := parseRecoveryGenerationID(spec.OpID[len("fuse-"):])
				path = filepath.Join(spec.LogicalPath+".generations", nonce, spec.OpID[len(spec.OpID)-16:], observedSidecarName)
			}
			if err := os.WriteFile(path, []byte("{}\n"), 0600); err != nil {
				t.Fatal(err)
			}
			got := RecoverRetainedGeneration(context.Background(), spec, h, observe)
			if got.Retired || h.statusCalls != 0 {
				t.Fatalf("changed %s reached helper: %+v", leaf, got)
			}
		})
	}
}

// Causal guard: removing the explicit-attempt check from ordinary reservation
// changes this named refusal and lets the old automatic resolver touch helper
// state that may already have acted after a lost reply.
func TestRetainedRecoveryAttemptBlocksOrdinarySuccessor(t *testing.T) {
	spec, h, observe := recoveryGenerationFixture(t)
	h.reclaimErr = errors.New("lost reply")
	got := RecoverRetainedGeneration(context.Background(), spec, h, observe)
	if got.Retired || got.ErrorCode != "reclaim_outcome_unknown" {
		t.Fatalf("lost reply reported terminal: %+v", got)
	}
	if _, err := reserveFUSEGeneration(spec.LogicalPath, "new-digest", emptyGenerationSnapshot); err == nil || !strings.Contains(err.Error(), "explicit recovery unresolved") {
		t.Fatalf("uncertain explicit attempt admitted ordinary successor or bypassed guard: %v", err)
	}
}

func TestRetainedGenerationCrossBootAndForeignNamespaceHold(t *testing.T) {
	for _, tc := range []struct {
		name     string
		snapshot mounttable.Snapshot
	}{
		{"cross-boot", mounttable.Snapshot{Version: 1, BootID: "different", Entries: []mounttable.MountEntry{}}},
		{"foreign-descendant", mounttable.Snapshot{Version: 1, BootID: "test-boot",
			Entries: []mounttable.MountEntry{{Path: "/will-be-replaced", Source: "foreign", Type: "macfuse", FSID: [2]int32{40, 50}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec, h, _ := recoveryGenerationFixture(t)
			snapshot := tc.snapshot
			if tc.name == "foreign-descendant" {
				snapshot.Entries[0].Path = spec.LogicalPath + ".generations/foreign"
			}
			observe := func(context.Context) (mounttable.Snapshot, error) { return snapshot, nil }
			got := RecoverRetainedGeneration(context.Background(), spec, h, observe)
			if got.Retired || h.reclaimCalls != 0 || h.retireCalls != 0 {
				t.Fatalf("unsafe inventory reached mutating helper: %+v %+v", got, h)
			}
		})
	}
}

func TestRetainedGenerationHelperCompleteLocalSettlementRetry(t *testing.T) {
	spec, h, _ := recoveryGenerationFixture(t)
	nonce, seq, ok := parseRecoveryGenerationID(spec.OpID[len("fuse-"):])
	if !ok {
		t.Fatal("fixture op invalid")
	}
	path := filepath.Join(spec.LogicalPath+".generations", nonce, fmt.Sprintf("%016x", seq), "mount")
	observations := 0
	observe := func(context.Context) (mounttable.Snapshot, error) {
		observations++
		if observations == 2 {
			return mounttable.Snapshot{}, errors.New("post-helper observer unavailable")
		}
		entries := []mounttable.MountEntry{}
		if observations == 1 {
			entries = append(entries, mounttable.MountEntry{Path: path, Source: "inert", Type: "macfuse", FSID: [2]int32{19, 29}})
		}
		return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: entries}, nil
	}
	first := RecoverRetainedGeneration(context.Background(), spec, h, observe)
	if first.Retired || first.State != "HELPER_RETIRED_LOCAL_SETTLEMENT_PENDING" ||
		first.ErrorCode != "post_retirement_inventory_unavailable" || h.reclaimCalls != 1 || h.retireCalls != 1 {
		t.Fatalf("failed post-observation overclaimed: %+v helper %+v", first, h)
	}
	f, err := os.Open(spec.LogicalPath + ".generations/ledger.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	events, readErr := generationEvents(f)
	f.Close()
	if readErr != nil || events[len(events)-1].State == "retired" {
		t.Fatalf("failed local settlement tombstoned generation: %v %+v", readErr, events)
	}
	h.status = client.Outcome{OpID: spec.OpID, State: "RETIRED", Syscalls: 1}
	second := RecoverRetainedGeneration(context.Background(), spec, h, observe)
	if !second.Retired || h.reclaimCalls != 1 || h.retireCalls != 1 {
		t.Fatalf("terminal status rerun replayed helper or did not settle: %+v helper %+v", second, h)
	}
}

func TestRetainedGenerationHeldFenceRefusesHelperMutation(t *testing.T) {
	logical := generationTestPath(t)
	r, err := reserveFUSEGeneration(logical, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", emptyGenerationSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	if err := r.markObserved([2]int32{19, 29}); err != nil {
		t.Fatal(err)
	}
	spec := RetainedGenerationRecovery{Root: filepath.Dir(logical), OwnerInstance: "owner", Build: "old-build",
		ConfigDigest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		LogicalPath:  logical, OpID: FUSEHelperOpID(r.Path)}
	h := &recoveryTestHelper{op: spec.OpID, status: client.Outcome{OpID: spec.OpID, State: "REGISTERED"}}
	got := RecoverRetainedGeneration(context.Background(), spec, h, emptyGenerationSnapshot)
	if got.Retired || h.statusCalls != 0 || h.reclaimCalls != 0 || h.retireCalls != 0 {
		t.Fatalf("held generation fence reached helper: %+v %+v", got, h)
	}
}

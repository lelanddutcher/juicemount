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

type recoveryTestLease struct {
	op   string
	lost chan struct{}
}

func (l recoveryTestLease) OpID() string          { return l.op }
func (l recoveryTestLease) Lost() <-chan struct{} { return l.lost }
func (l recoveryTestLease) Err() error            { return nil }
func (l recoveryTestLease) Close()                {}

type recoveryTestHelper struct {
	op                                                  string
	status                                              client.Outcome
	reclaim                                             client.ReclaimResult
	retire                                              client.Outcome
	cancelResult                                        client.Outcome
	statusSequence                                      []client.Outcome
	lease                                               UnmountLease
	allowEject                                          bool
	retireRequest                                       client.RetireRequest
	reclaimErr, retireErr, cancelErr                    error
	statusCalls, reclaimCalls, retireCalls, cancelCalls int
}

func (h *recoveryTestHelper) Status(context.Context, string) (client.Outcome, error) {
	h.statusCalls++
	if len(h.statusSequence) > 0 {
		out := h.statusSequence[0]
		h.statusSequence = h.statusSequence[1:]
		return out, nil
	}
	return h.status, nil
}
func (h *recoveryTestHelper) Reclaim(context.Context, string) (UnmountLease, client.ReclaimResult, error) {
	h.reclaimCalls++
	if h.reclaimErr != nil {
		return nil, client.ReclaimResult{}, h.reclaimErr
	}
	if h.lease != nil {
		return h.lease, h.reclaim, nil
	}
	return recoveryTestLease{op: h.op}, h.reclaim, nil
}
func (h *recoveryTestHelper) Cancel(context.Context, string) (client.Outcome, error) {
	h.cancelCalls++
	return h.cancelResult, h.cancelErr
}
func (h *recoveryTestHelper) Retire(_ context.Context, _ string, rr client.RetireRequest, _ time.Duration) (client.Outcome, error) {
	h.retireCalls++
	h.retireRequest = rr
	if rr.Liveness != client.LivenessUnknown || rr.EjectNow != h.allowEject {
		panic("recovery requested force or non-unknown liveness")
	}
	return h.retire, h.retireErr
}

func publishOrdinaryTestAttempt(t *testing.T, spec RetainedGenerationRecovery) {
	publishTestAttempt(t, spec, recoveryAttemptName)
}

func publishTestAttempt(t *testing.T, spec RetainedGenerationRecovery, name string) {
	t.Helper()
	r, og, install, err := openRetainedGeneration(spec)
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	defer install.Close()
	attempt := generationRecoveryAttempt{Version: 1, Root: spec.Root, OwnerInstance: spec.OwnerInstance,
		Build: spec.Build, ConfigDigest: spec.ConfigDigest, OpID: spec.OpID, Path: og.path, BootID: og.boot, FSID: og.fsid}
	if err := publishRecoveryAttemptNamed(install, r.ID, name, attempt); err != nil {
		t.Fatal(err)
	}
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

func TestRetainedGenerationEjectRequiresOrdinaryAttempt(t *testing.T) {
	spec, h, observe := recoveryGenerationFixture(t)
	spec.EjectNow = true
	h.allowEject = true
	got := RecoverRetainedGeneration(context.Background(), spec, h, observe)
	if got.Retired || got.ErrorCode != "ordinary_attempt_required_for_stage" || h.statusCalls != 0 || h.reclaimCalls != 0 || h.retireCalls != 0 {
		t.Fatalf("eject without ordinary attempt reached helper: %+v %+v", got, h)
	}
}

func mountedRecoverySnapshot(spec RetainedGenerationRecovery) func(context.Context) (mounttable.Snapshot, error) {
	nonce, seq, _ := parseRecoveryGenerationID(spec.OpID[len("fuse-"):])
	path := filepath.Join(spec.LogicalPath+".generations", nonce, fmt.Sprintf("%016x", seq), "mount")
	return func(context.Context) (mounttable.Snapshot, error) {
		return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: []mounttable.MountEntry{
			{Path: path, Source: "inert", Type: "macfuse", FSID: [2]int32{19, 29}},
		}}, nil
	}
}

func TestRetainedGenerationWithdrawBackoffFalseReclaimed(t *testing.T) {
	spec, h, _ := recoveryGenerationFixture(t)
	publishOrdinaryTestAttempt(t, spec)
	spec.Withdraw = true
	h.statusSequence = []client.Outcome{{OpID: spec.OpID, State: "BACKOFF"}, {OpID: spec.OpID, State: "REGISTERED"}}
	h.reclaim.Outcome.State = "BACKOFF"
	h.reclaim.Reclaimed = false
	h.cancelResult = client.Outcome{OpID: spec.OpID, State: "CANCELLED"}
	got := RecoverRetainedGeneration(context.Background(), spec, h, mountedRecoverySnapshot(spec))
	if !got.Withdrawn || got.Retired || h.cancelCalls != 1 || h.reclaimCalls != 1 || h.retireCalls != 0 {
		t.Fatalf("withdrawal did not bind and cancel once: %+v %+v", got, h)
	}
	again := RecoverRetainedGeneration(context.Background(), spec, h, mountedRecoverySnapshot(spec))
	if again.Withdrawn || h.reclaimCalls != 1 || h.cancelCalls != 1 {
		t.Fatalf("withdrawal replayed: %+v %+v", again, h)
	}
}

func TestRetainedGenerationWithdrawLostReplyNeverReplays(t *testing.T) {
	spec, h, _ := recoveryGenerationFixture(t)
	publishOrdinaryTestAttempt(t, spec)
	spec.Withdraw = true
	h.status.State = "BACKOFF"
	h.reclaim.Outcome.State = "BACKOFF"
	h.reclaimErr = errors.New("lost reply")
	first := RecoverRetainedGeneration(context.Background(), spec, h, mountedRecoverySnapshot(spec))
	if first.Withdrawn || first.ErrorCode != "stage_reclaim_unknown" || h.reclaimCalls != 1 {
		t.Fatalf("lost withdraw reclaim misreported: %+v %+v", first, h)
	}
	second := RecoverRetainedGeneration(context.Background(), spec, h, mountedRecoverySnapshot(spec))
	if second.Withdrawn || second.ErrorCode != "previous_stage_attempt_uncertain" || h.reclaimCalls != 1 || h.cancelCalls != 0 {
		t.Fatalf("uncertain withdraw replayed: %+v %+v", second, h)
	}
}

func TestRetainedGenerationExplicitEjectAfterWithdraw(t *testing.T) {
	spec, h, _ := recoveryGenerationFixture(t)
	publishOrdinaryTestAttempt(t, spec)
	publishTestAttempt(t, spec, recoveryWithdrawAttemptName)
	publishTestAttempt(t, spec, recoveryWithdrawCompleteName)
	spec.EjectNow, h.allowEject = true, true
	h.reclaim.Reclaimed = false
	h.retire.Forced = true
	reads := 0
	mounted := mountedRecoverySnapshot(spec)
	observe := func(ctx context.Context) (mounttable.Snapshot, error) {
		reads++
		if reads == 3 {
			return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: []mounttable.MountEntry{}}, nil
		}
		return mounted(ctx)
	}
	got := RecoverRetainedGeneration(context.Background(), spec, h, observe)
	if !got.Retired || !got.Forced || !h.retireRequest.EjectNow || h.reclaimCalls != 1 || h.retireCalls != 1 || h.cancelCalls != 0 {
		t.Fatalf("explicit eject failed: %+v %+v", got, h)
	}
}

func TestRetainedGenerationEjectRejectsOrphanCompleteMarker(t *testing.T) {
	spec, h, _ := recoveryGenerationFixture(t)
	publishOrdinaryTestAttempt(t, spec)
	publishTestAttempt(t, spec, recoveryWithdrawCompleteName)
	spec.EjectNow = true
	got := RecoverRetainedGeneration(context.Background(), spec, h, mountedRecoverySnapshot(spec))
	if got.Retired || got.ErrorCode != "withdraw_complete_required_for_eject" || h.statusCalls != 0 || h.reclaimCalls != 0 {
		t.Fatalf("orphan complete marker admitted eject: %+v %+v", got, h)
	}
}

func TestRetainedGenerationEjectLostRetireReplySettlesWithoutReplay(t *testing.T) {
	spec, h, _ := recoveryGenerationFixture(t)
	publishOrdinaryTestAttempt(t, spec)
	publishTestAttempt(t, spec, recoveryWithdrawAttemptName)
	publishTestAttempt(t, spec, recoveryWithdrawCompleteName)
	spec.EjectNow, h.allowEject = true, true
	h.retireErr = errors.New("lost reply")
	reads := 0
	mounted := mountedRecoverySnapshot(spec)
	observe := func(ctx context.Context) (mounttable.Snapshot, error) {
		reads++
		if reads > 2 {
			return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: []mounttable.MountEntry{}}, nil
		}
		return mounted(ctx)
	}
	first := RecoverRetainedGeneration(context.Background(), spec, h, observe)
	if first.Retired || first.ErrorCode != "eject_retire_unknown" || h.retireCalls != 1 {
		t.Fatalf("lost reply reported terminal: %+v %+v", first, h)
	}
	second := RecoverRetainedGeneration(context.Background(), spec, h, observe)
	if second.Retired || second.ErrorCode != "previous_stage_attempt_uncertain" || h.retireCalls != 1 || h.reclaimCalls != 1 {
		t.Fatalf("eject replayed before proof: %+v %+v", second, h)
	}
	h.status = client.Outcome{OpID: spec.OpID, State: "RETIRED", Forced: true, Syscalls: 2}
	third := RecoverRetainedGeneration(context.Background(), spec, h, observe)
	if !third.Retired || !third.Forced || h.retireCalls != 1 || h.reclaimCalls != 1 {
		t.Fatalf("terminal proof failed local settlement: %+v %+v", third, h)
	}
}

func TestRetainedGenerationStagedLostLeaseRefusesMutation(t *testing.T) {
	spec, h, _ := recoveryGenerationFixture(t)
	publishOrdinaryTestAttempt(t, spec)
	spec.Withdraw = true
	lost := make(chan struct{})
	close(lost)
	h.lease = recoveryTestLease{op: spec.OpID, lost: lost}
	got := RecoverRetainedGeneration(context.Background(), spec, h, mountedRecoverySnapshot(spec))
	if got.Withdrawn || h.reclaimCalls != 1 || h.cancelCalls != 0 || h.retireCalls != 0 {
		t.Fatalf("lost lease reached mutation: %+v %+v", got, h)
	}
}

func TestRetainedGenerationEjectRefusesKnownOrStaleHolders(t *testing.T) {
	for _, stale := range []bool{false, true} {
		spec, h, _ := recoveryGenerationFixture(t)
		publishOrdinaryTestAttempt(t, spec)
		publishTestAttempt(t, spec, recoveryWithdrawAttemptName)
		publishTestAttempt(t, spec, recoveryWithdrawCompleteName)
		spec.EjectNow = true
		if stale {
			h.status.HoldersStale = true
		} else {
			h.status.Holders = []client.Holder{{PID: 42, UID: 501, Name: "inert"}}
		}
		got := RecoverRetainedGeneration(context.Background(), spec, h, mountedRecoverySnapshot(spec))
		if got.Retired || h.reclaimCalls != 0 || h.retireCalls != 0 {
			t.Fatalf("known/stale holders reached force: %+v %+v", got, h)
		}
	}
}

func TestRetainedGenerationWithdrawPendingCancelHolds(t *testing.T) {
	spec, h, _ := recoveryGenerationFixture(t)
	publishOrdinaryTestAttempt(t, spec)
	spec.Withdraw = true
	h.cancelResult = client.Outcome{OpID: spec.OpID, State: "IN_KERNEL"}
	got := RecoverRetainedGeneration(context.Background(), spec, h, mountedRecoverySnapshot(spec))
	if got.Withdrawn || got.ErrorCode != "withdraw_cancel_pending" || h.cancelCalls != 1 || h.retireCalls != 0 {
		t.Fatalf("pending cancel overclaimed: %+v %+v", got, h)
	}
}

func TestRetainedGenerationForcedTerminalRequiresEjectMarker(t *testing.T) {
	spec, h, observe := recoveryGenerationFixture(t)
	publishOrdinaryTestAttempt(t, spec)
	publishTestAttempt(t, spec, recoveryWithdrawAttemptName)
	publishTestAttempt(t, spec, recoveryWithdrawCompleteName)
	spec.EjectNow = true
	h.status.State, h.status.Forced = "RETIRED", true
	got := RecoverRetainedGeneration(context.Background(), spec, h, observe)
	if got.Retired || h.reclaimCalls != 0 || h.retireCalls != 0 {
		t.Fatalf("ordinary marker settled forced terminal: %+v %+v", got, h)
	}
}

func TestRetainedGenerationForcedTerminalSettlesOnlyWithExactEjectMarker(t *testing.T) {
	spec, h, _ := recoveryGenerationFixture(t)
	publishOrdinaryTestAttempt(t, spec)
	publishTestAttempt(t, spec, recoveryWithdrawAttemptName)
	publishTestAttempt(t, spec, recoveryWithdrawCompleteName)
	publishTestAttempt(t, spec, recoveryEjectAttemptName)
	spec.EjectNow = true
	h.status = client.Outcome{OpID: spec.OpID, State: "RETIRED", Forced: true, Syscalls: 2}
	observe := func(context.Context) (mounttable.Snapshot, error) {
		return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: []mounttable.MountEntry{}}, nil
	}
	got := RecoverRetainedGeneration(context.Background(), spec, h, observe)
	if !got.Retired || !got.Forced || h.reclaimCalls != 0 || h.retireCalls != 0 {
		t.Fatalf("exact terminal settlement: %+v %+v", got, h)
	}
}

func TestRetainedGenerationStagedMarkerAndLeaseIdentityHolds(t *testing.T) {
	for _, tc := range []string{"changed-marker", "wrong-lease"} {
		t.Run(tc, func(t *testing.T) {
			spec, h, _ := recoveryGenerationFixture(t)
			publishOrdinaryTestAttempt(t, spec)
			spec.Withdraw = true
			h.status.State = "BACKOFF"
			if tc == "changed-marker" {
				bad := spec
				bad.OwnerInstance = "other"
				publishTestAttempt(t, bad, recoveryWithdrawAttemptName)
				got := RecoverRetainedGeneration(context.Background(), spec, h, mountedRecoverySnapshot(spec))
				if got.Withdrawn || h.statusCalls != 0 || h.reclaimCalls != 0 {
					t.Fatalf("changed marker reached helper: %+v %+v", got, h)
				}
			} else {
				h.lease = recoveryTestLease{op: "wrong"}
				got := RecoverRetainedGeneration(context.Background(), spec, h, mountedRecoverySnapshot(spec))
				if got.Withdrawn || h.cancelCalls != 0 || h.reclaimCalls != 1 {
					t.Fatalf("wrong lease reached cancel: %+v %+v", got, h)
				}
			}
		})
	}
}

func TestRetainedGenerationEjectPostbindingChangeHolds(t *testing.T) {
	spec, h, _ := recoveryGenerationFixture(t)
	publishOrdinaryTestAttempt(t, spec)
	publishTestAttempt(t, spec, recoveryWithdrawAttemptName)
	publishTestAttempt(t, spec, recoveryWithdrawCompleteName)
	spec.EjectNow = true
	h.allowEject = true
	h.statusSequence = []client.Outcome{
		{OpID: spec.OpID, State: "REGISTERED"},
		{OpID: spec.OpID, State: "BACKOFF"},
	}
	got := RecoverRetainedGeneration(context.Background(), spec, h, mountedRecoverySnapshot(spec))
	if got.Retired || got.ErrorCode != "eject_postbinding_unsafe" || h.retireCalls != 0 {
		t.Fatalf("postbinding drift reached force: %+v %+v", got, h)
	}
}

func TestRetainedGenerationEjectRefusesChangedIdentityAndDescendant(t *testing.T) {
	spec, h, _ := recoveryGenerationFixture(t)
	publishOrdinaryTestAttempt(t, spec)
	publishTestAttempt(t, spec, recoveryWithdrawAttemptName)
	publishTestAttempt(t, spec, recoveryWithdrawCompleteName)
	spec.EjectNow = true
	h.allowEject = true
	bad := spec
	bad.OwnerInstance = "other"
	if got := RecoverRetainedGeneration(context.Background(), bad, h, emptyGenerationSnapshot); got.Retired || h.statusCalls != 0 {
		t.Fatalf("changed owner reached helper: %+v %+v", got, h)
	}
	observe := func(context.Context) (mounttable.Snapshot, error) {
		return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: []mounttable.MountEntry{
			{Path: spec.LogicalPath + ".generations/foreign", Source: "foreign", Type: "macfuse", FSID: [2]int32{40, 50}},
		}}, nil
	}
	if got := RecoverRetainedGeneration(context.Background(), spec, h, observe); got.Retired || h.reclaimCalls != 0 || h.retireCalls != 0 {
		t.Fatalf("descendant reached mutating helper: %+v %+v", got, h)
	}
}

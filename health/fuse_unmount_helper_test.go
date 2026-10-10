package health

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

type fakeFUSELease struct {
	op     string
	lost   chan struct{}
	mu     sync.Mutex
	closed bool
}

func (l *fakeFUSELease) OpID() string          { return l.op }
func (l *fakeFUSELease) Lost() <-chan struct{} { return l.lost }
func (l *fakeFUSELease) Err() error            { return nil }
func (l *fakeFUSELease) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
}

type fuseRetireCall struct {
	op string
	rr client.RetireRequest
}

// fakeFUSEUnmountd records calls; onRetire models the kernel effect.
type fakeFUSEUnmountd struct {
	mu         sync.Mutex
	registers  []client.RegisterRequest
	retires    []fuseRetireCall
	reclaims   []string
	reclaimed  bool
	reclaimErr error
	reclaimOut client.Outcome
	retireErr  error
	onRetire   func()
	cancels    []string
}

func (f *fakeFUSEUnmountd) Register(_ context.Context, rr client.RegisterRequest) (UnmountLease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registers = append(f.registers, rr)
	return &fakeFUSELease{op: rr.OpID, lost: make(chan struct{})}, nil
}
func (f *fakeFUSEUnmountd) Reclaim(_ context.Context, op string) (UnmountLease, client.ReclaimResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reclaims = append(f.reclaims, op)
	if f.reclaimErr != nil {
		return nil, client.ReclaimResult{Outcome: f.reclaimOut}, f.reclaimErr
	}
	return &fakeFUSELease{op: op, lost: make(chan struct{})}, client.ReclaimResult{Reclaimed: f.reclaimed}, nil
}
func (f *fakeFUSEUnmountd) Retire(_ context.Context, op string, rr client.RetireRequest, _ time.Duration) (client.Outcome, error) {
	f.mu.Lock()
	f.retires = append(f.retires, fuseRetireCall{op, rr})
	err, hook := f.retireErr, f.onRetire
	f.mu.Unlock()
	if err != nil {
		return client.Outcome{OpID: op, State: "ORDINARY"}, err
	}
	if hook != nil {
		hook()
	}
	return client.Outcome{OpID: op, State: "RETIRED"}, nil
}
func (f *fakeFUSEUnmountd) Cancel(_ context.Context, op string) (client.Outcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancels = append(f.cancels, op)
	return client.Outcome{OpID: op, State: "CANCELLED"}, nil
}
func (f *fakeFUSEUnmountd) cancelled() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.cancels...)
}
func (f *fakeFUSEUnmountd) calls() ([]client.RegisterRequest, []fuseRetireCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]client.RegisterRequest(nil), f.registers...), append([]fuseRetireCall(nil), f.retires...)
}

const helperTestNonce = "00112233445566778899aabbccddeeff"

// helperStopFixture is a permission fixture whose mount disappears only
// when the fake helper retires it (or the custodian detaches it). The
// receipt holds a registered helper lease.
func helperStopFixture(t *testing.T, keeper fuseCustodian, fake *fakeFUSEUnmountd) (*permissionFixture, *bool) {
	t.Helper()
	f := newPermissionFixture(t, keeper)
	var mu sync.Mutex
	helperGone := false
	f.present = func() bool {
		mu.Lock()
		defer mu.Unlock()
		return !helperGone && !f.receipt.custody.Detached()
	}
	fake.onRetire = func() { mu.Lock(); helperGone = true; mu.Unlock() }
	f.fm.unmountd = fake
	f.receipt.custodyStartedAt = fuseNow()
	f.receipt.helper = &fuseHelperReceipt{op: "fuse-" + helperTestNonce + "-0000000000000001",
		lease: &fakeFUSELease{lost: make(chan struct{})}, state: FUSEHelperRegistered}
	return f, &helperGone
}

func TestFUSERegisterReceiptCarriesGenerationIdentity(t *testing.T) {
	root := generationTestPath(t)
	path := filepath.Join(root+".generations", helperTestNonce, "000000000000002a", "mount")
	fake := &fakeFUSEUnmountd{}
	fm := NewFUSEManager(FUSEConfig{MountPoint: path})
	fm.generation = &GenerationReservation{Path: path}
	fm.unmountd = fake
	r := &MountReceipt{owner: fm}
	fm.registerWithHelperLocked(r, path, "JuiceFS:zpool", [2]int32{0x2a, 0x1b})
	regs, _ := fake.calls()
	want := client.RegisterRequest{OpID: "fuse-" + helperTestNonce + "-000000000000002a", RegisterArgs: client.RegisterArgs{
		Type: "macfuse", FSID: [2]int32{0x2a, 0x1b}, Token: helperTestNonce, MntOnName: path, MntFromName: "JuiceFS:zpool", Generation: 0x2a}}
	if len(regs) != 1 || regs[0] != want {
		t.Fatalf("receipt claim:\n got %+v\nwant %+v", regs, want)
	}
	if op, state, _ := fm.UnmountHelperStatus(); op != want.OpID || state != FUSEHelperRegistered {
		t.Fatalf("lease not held/exposed: %s %s", op, state)
	}
	if FUSEHelperOpID(path) != want.OpID || FUSEHelperOpID(root) != "" || FUSEHelperOpID(filepath.Join(root+".generations", "xyz", "000000000000002a", "mount")) != "" {
		t.Fatal("op id derivation accepts a non-generation path")
	}
}

func TestFUSERegisterWithoutHelperKeepsMountAndReportsUnavailable(t *testing.T) {
	path := filepath.Join(generationTestPath(t)+".generations", helperTestNonce, "0000000000000001", "mount")
	fm := NewFUSEManager(FUSEConfig{MountPoint: path})
	fm.generation = &GenerationReservation{Path: path}
	r := &MountReceipt{owner: fm}
	fm.registerWithHelperLocked(r, path, "src", [2]int32{1, 2})
	if _, state, _ := fm.UnmountHelperStatus(); state != FUSEHelperUnavailable {
		t.Fatalf("state %q", state)
	}
}

func TestFUSEPermissionDeniedHandsOffToHelperAtOnce(t *testing.T) {
	denied := &permissionCustodian{denied: true, joined: true}
	fake := &fakeFUSEUnmountd{}
	f, _ := helperStopFixture(t, denied, fake)
	old := startFUSECustodian
	t.Cleanup(func() { startFUSECustodian = old })
	startFUSECustodian = func(string, string, int, int) (fuseCustodian, error) {
		return &permissionCustodian{denied: true, joined: true}, nil // still denied
	}
	got := f.fm.stopWithBounds(2*time.Second, time.Second)
	if !got.Complete() {
		t.Fatalf("denied custody not retired through the helper: %+v", got)
	}
	_, retires := fake.calls()
	if len(retires) != 1 || retires[0].op != f.receipt.helper.op || retires[0].rr.EjectNow || retires[0].rr.Liveness != client.LivenessAlive {
		t.Fatalf("handoff was not one ordinary retire with proven liveness: %+v", retires)
	}
	if denied.detachCalls != 0 || f.fm.activeReceipt != nil {
		t.Fatal("denied custodian was asked to detach or the receipt was kept")
	}
}

func TestFUSEPendingHandsOffOnlyAfterBound(t *testing.T) {
	keeper := &abandonableCustodian{permissionCustodian: permissionCustodian{approve: make(chan struct{}), uuid: permissionTestUUID, fsid: [2]int32{7, 9}}}
	fake := &fakeFUSEUnmountd{}
	f, _ := helperStopFixture(t, keeper, fake)
	forbidCustodianStart(t)
	if got := f.fm.stopWithBounds(50*time.Millisecond, time.Second); got.State != FUSEAdmissionPending {
		t.Fatalf("pending within bound: %+v", got)
	}
	if _, retires := fake.calls(); len(retires) != 0 {
		t.Fatalf("pending within %v handed off: %+v", FUSEPendingHelperHandoff, retires)
	}
	f.receipt.custodyStartedAt = fuseNow().Add(-FUSEPendingHelperHandoff - time.Second)
	got := f.fm.stopWithBounds(200*time.Millisecond, time.Second)
	if !got.Complete() {
		t.Fatalf("pending past the bound not retired: %+v", got)
	}
	if _, retires := fake.calls(); len(retires) != 1 || retires[0].rr.EjectNow {
		t.Fatalf("pending handoff: %+v", retires)
	}
	if !keeper.abandoned || keeper.detachCalls != 0 || keeper.releaseCalls != 0 {
		t.Fatalf("pending custodian not abandoned after proof: %+v", keeper)
	}
}

// abandonableCustodian models a custodian whose exec handle can end it.
type abandonableCustodian struct {
	permissionCustodian
	abandoned bool
}

func (c *abandonableCustodian) Abandon(time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.abandoned = true
	return nil
}
func (c *abandonableCustodian) Settled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.released || c.abandoned
}

func TestFUSEBusyDescriptorDetachFallsBackToHelper(t *testing.T) {
	keeper := &busyCustodian{permissionCustodian{ready: true, uuid: permissionTestUUID, fsid: [2]int32{7, 9}}}
	fake := &fakeFUSEUnmountd{}
	f, _ := helperStopFixture(t, keeper, fake)
	if got := f.fm.stopWithBounds(500*time.Millisecond, time.Second); got.State == FUSEStopped {
		// busyCustodian has no Abandon: the custodian holding a dead
		// descriptor must not be silently dropped.
		t.Fatalf("custodian dropped without a join: %+v", got)
	} else if !strings.Contains(got.Detail, "no Abandon") {
		t.Fatalf("unexpected result: %+v", got)
	}
	if _, retires := fake.calls(); len(retires) != 1 || retires[0].rr.Liveness != client.LivenessAlive || retires[0].rr.EjectNow {
		t.Fatalf("busy detach was not one ordinary (non-forcing) handoff: %+v", retires)
	}
}

type busyCustodian struct{ permissionCustodian }

func (c *busyCustodian) Detach(time.Duration) error { return syscall.EBUSY }

func TestFUSEHelperFailureKeepsTypedState(t *testing.T) {
	denied := &permissionCustodian{denied: true, joined: true}
	fake := &fakeFUSEUnmountd{retireErr: client.ErrPending}
	f, _ := helperStopFixture(t, denied, fake)
	old := startFUSECustodian
	t.Cleanup(func() { startFUSECustodian = old })
	startFUSECustodian = func(string, string, int, int) (fuseCustodian, error) {
		return &permissionCustodian{denied: true, joined: true}, nil
	}
	got := f.fm.stopWithBounds(200*time.Millisecond, time.Second)
	if got.State != FUSEPermissionDenied || !strings.Contains(got.Detail, "unmount helper") || f.fm.activeReceipt != f.receipt {
		t.Fatalf("unproven helper retirement changed state: %+v", got)
	}
	// The ordinary job this Stop asked for must not outlive it.
	if c := fake.cancelled(); len(c) != 1 || c[0] != f.receipt.helper.op {
		t.Fatalf("failed Stop left its ordinary helper job running: %v", c)
	}
}

func TestFUSEDeadChildRetiresWithLivenessDead(t *testing.T) {
	keeper := &permissionCustodian{ready: true, uuid: permissionTestUUID, fsid: [2]int32{7, 9}}
	fake := &fakeFUSEUnmountd{}
	f, _ := helperStopFixture(t, keeper, fake)
	if err := f.fm.RetireDeadChildMount(time.Second); err == nil {
		t.Fatal("retired while the exact child is alive")
	}
	// While the exact child runs its liveness is proven ALIVE; a source that
	// reported "proven dead" here would license a force against a live server.
	if alive, proven := f.fm.ExactChildLiveness(); !alive || !proven {
		t.Fatalf("exact liveness of a running child: alive=%v proven=%v", alive, proven)
	}
	var none *FUSEManager
	if _, proven := none.ExactChildLiveness(); proven {
		t.Fatal("a manager with no child receipt claimed proof")
	}
	// End the exact child; its own Wait returning is the proof of death.
	_ = f.receipt.process.Kill()
	<-f.receipt.waitDone
	if err := f.fm.RetireDeadChildMount(time.Second); err != nil {
		t.Fatalf("dead child mount not retired: %v", err)
	}
	if _, retires := fake.calls(); len(retires) != 1 || retires[0].rr.Liveness != client.LivenessDead {
		t.Fatalf("dead retire liveness: %+v", retires)
	}
	if alive, proven := f.fm.ExactChildLiveness(); alive || !proven {
		t.Fatalf("exact liveness after the child's Wait: alive=%v proven=%v", alive, proven)
	}
	old := isJuiceFSProcessAliveFn
	isJuiceFSProcessAliveFn = func(string) bool { return true } // a stale ps title
	t.Cleanup(func() { isJuiceFSProcessAliveFn = old })
	if f.fm.juiceFSAlive() {
		t.Fatal("watchdog liveness ignores the exact child's Wait")
	}
}

// After a RETIRED proof the exact JuiceFS child gets FUSEChildKillAfterRetire
// to exit; then SIGKILL (never SIGTERM) to that unreaped child only.
func TestFUSEChildKilledOnlyAfterGraceAndOnlyWithSIGKILL(t *testing.T) {
	denied := &permissionCustodian{denied: true, joined: true}
	fake := &fakeFUSEUnmountd{}
	f, _ := helperStopFixture(t, denied, fake)
	forbidCustodianStartAllowDenied(t)
	// The fixture child exits when observation releases it; keep it alive
	// to model a JuiceFS that did not notice the closed channel.
	f.fm.observeMount = func(context.Context) (mounttable.Snapshot, error) {
		entries := []mounttable.MountEntry{}
		if f.present() {
			entries = append(entries, mounttable.MountEntry{Path: f.path, Type: "macfuse", Source: f.source, FSID: f.fsid})
		}
		return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: entries}, nil
	}
	now := time.Now()
	oldNow := fuseNow
	fuseNow = func() time.Time { return now }
	t.Cleanup(func() { fuseNow = oldNow })
	got := f.fm.stopWithBounds(100*time.Millisecond, time.Second)
	if got.State != FUSEInFlight || !strings.Contains(got.Detail, "waiting for the exact JuiceFS child") {
		t.Fatalf("child signalled inside the grace: %+v", got)
	}
	if waitFUSEChannel(f.receipt.waitDone, 0) {
		t.Fatal("child ended inside the grace")
	}
	now = now.Add(FUSEChildKillAfterRetire)
	if got := f.fm.stopWithBounds(2*time.Second, time.Second); !got.Complete() {
		t.Fatalf("Stop after grace: %+v", got)
	}
	ws, ok := f.receipt.exitErrState()
	if !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("exact child not ended by SIGKILL: %+v", ws)
	}
}

func forbidCustodianStartAllowDenied(t *testing.T) {
	t.Helper()
	old := startFUSECustodian
	startFUSECustodian = func(string, string, int, int) (fuseCustodian, error) {
		return &permissionCustodian{denied: true, joined: true}, nil
	}
	t.Cleanup(func() { startFUSECustodian = old })
}

func (r *MountReceipt) exitErrState() (syscall.WaitStatus, bool) {
	ee, ok := r.exitErr.(interface{ Sys() any })
	if !ok {
		return 0, false
	}
	ws, ok := ee.Sys().(syscall.WaitStatus)
	return ws, ok
}

// A restarted owner finds a same-boot generation whose owner died. With the
// helper's RETIRED proof (after a real reclaim) the tombstone is written and
// a new generation is reserved; without it the new child stays refused.
func TestGenerationReclaimsDeadOwnersReceiptBeforeNewChild(t *testing.T) {
	cases := []struct {
		name    string
		fake    *fakeFUSEUnmountd
		helper  bool
		allowed bool
	}{
		{"reclaimed-and-retired", &fakeFUSEUnmountd{reclaimed: true}, true, true},
		{"orphan-sweep-already-retired", &fakeFUSEUnmountd{reclaimErr: client.ErrNoReceipt}, true, true},
		{"live-owner-holds-receipt", &fakeFUSEUnmountd{reclaimed: false}, true, false},
		{"no-helper", nil, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logical := generationTestPath(t)
			a, err := reserveFUSEGeneration(logical, "d", emptyGenerationSnapshot)
			if err != nil {
				t.Fatal(err)
			}
			for _, step := range []func() error{a.markLaunching, func() error { return a.markChild(4242) }, func() error { return a.markObserved([2]int32{5, 6}) }} {
				if err := step(); err != nil {
					t.Fatal(err)
				}
			}
			a.close() // the owner died with the generation mounted
			op := FUSEHelperOpID(a.Path)
			if tc.fake != nil && tc.fake.reclaimErr != nil {
				tc.fake.reclaimOut = client.Outcome{OpID: op, State: "RETIRED"}
			}
			if tc.helper {
				SetDefaultUnmountHelper(tc.fake)
			} else {
				SetDefaultUnmountHelper(nil)
			}
			t.Cleanup(func() { SetDefaultUnmountHelper(nil) })
			b, err := reserveFUSEGeneration(logical, "d", emptyGenerationSnapshot)
			if !tc.allowed {
				if err == nil || !strings.Contains(err.Error(), "unresolved") {
					t.Fatalf("new child admitted without proof: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("reclaim+retire did not resolve the prior generation: %v", err)
			}
			defer b.close()
			if tc.fake.reclaims[0] != op || b.ID == a.ID {
				t.Fatalf("reclaimed %v for %s", tc.fake.reclaims, op)
			}
			events, err := generationEvents(b.ledger)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, e := range events {
				if e.ID == a.ID && e.State == "retired" {
					found = true
				}
			}
			if !found {
				t.Fatal("prior generation tombstone not written")
			}
		})
	}
}

func TestGenerationObservedEventPersistsFSID(t *testing.T) {
	a, err := reserveFUSEGeneration(generationTestPath(t), "d", emptyGenerationSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	_ = a.markLaunching()
	_ = a.markChild(4242)
	if err := a.markObserved([2]int32{5, 6}); err != nil {
		t.Fatal(err)
	}
	// Downgrade safety: the ledger grammar is unchanged (no new state).
	events, _ := generationEvents(a.ledger)
	if last := events[len(events)-1]; last.State != "born" {
		t.Fatalf("observed identity leaked into the ledger: %+v", last)
	}
	nonce := strings.SplitN(a.ID, "-", 2)[0]
	install, err := os.Open(filepath.Dir(filepath.Dir(a.Path)))
	if err != nil {
		t.Fatal(err)
	}
	defer install.Close()
	og, ok := readObservedSidecar(install, nonce, a.ID)
	if !ok || og.fsid != [2]int32{5, 6} || og.boot != "test-boot" || og.path != a.Path {
		t.Fatalf("observed fsid not persisted: %+v %v", og, ok)
	}
	if err := a.retire(); err != nil {
		t.Fatalf("born → retired refused: %v", err)
	}
}

// The ledger keeps the pre-H-3 grammar, so an earlier 0.5 RC still parses it
// after a rollback; the sidecar never blocks a later reservation.
func TestGenerationLedgerStaysParseableByOlderGrammar(t *testing.T) {
	logical := generationTestPath(t)
	a, err := reserveFUSEGeneration(logical, "d", emptyGenerationSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	_ = a.markLaunching()
	_ = a.markChild(4242)
	if err := a.markObserved([2]int32{5, 6}); err != nil {
		t.Fatal(err)
	}
	if err := a.retire(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(logical+".generations", "ledger.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	events, err := generationEvents(f)
	if err != nil {
		t.Fatal(err)
	}
	old := map[string]bool{"install": true, "intent": true, "ready": true, "launching": true, "born": true, "custody": true, "retired": true}
	for _, e := range events {
		if !old[e.State] {
			t.Fatalf("ledger state %q unknown to earlier 0.5 builds", e.State)
		}
	}
	b, err := reserveFUSEGeneration(logical, "d", emptyGenerationSnapshot)
	if err != nil {
		t.Fatalf("sidecar broke the next reservation: %v", err)
	}
	b.close()
}

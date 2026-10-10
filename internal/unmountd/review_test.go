package unmountd

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Review fix 1: proof of death is sticky. A joining orphan/reconnect with
// LivenessUnknown must not downgrade the job to an ordinary unmount, which
// would park in VFS_SYNC against the dead server and wedge convergence.
func TestProvenDeathIsStickyAcrossJoins(t *testing.T) {
	child := Entry{FSID: FSID{0x2e000040, 0x2e}, Owner: 0, Type: "smbfs", MntOnName: testPath + "/child", MntFromName: "//nas/c"}
	k := newFake(rootEntry(), ourEntry(), child)
	k.dead[ourFSID] = true
	svc := parkedNestedService(t, k)
	mustRegister(t, svc, nfsRegister("op1"))
	// Dead job parks in WAIT_NESTED (no syscall) behind the nested mount.
	waitParkedNested(t, svc, k, RetireRequest{Liveness: LivenessDead})
	// A reconnect that knows less joins the job.
	_, _ = retire(t, svc, "op1", RetireRequest{Liveness: LivenessUnknown}, time.Millisecond)
	k.setEntries(rootEntry(), ourEntry())
	// This join also wakes the parked job, so the next pass sees no nested
	// mount and picks the flavor from the merged request.
	out, err := retire(t, svc, "op1", RetireRequest{Liveness: LivenessUnknown}, 2*time.Second)
	if err != nil || out.State != StateRetired {
		t.Fatalf("after downgrade attempt: %+v err %v calls %q (an ordinary call on a dead server never returns)", out, err, flagsOf(k.recorded()))
	}
	if got := flagsOf(k.recorded()); got != "F" {
		t.Fatalf("calls %q, want a single MNT_FORCE", got)
	}
}

// parkedNestedService is fastPolicy with a NestedRetry no test outlives, so a
// job in WAIT_NESTED stays there until a join wakes it. The setup check then
// observes a stable state instead of sampling a VERIFY⇄WAIT_NESTED cycle
// (sampling that cycle at a fixed instant failed ~20% of runs under -race).
func parkedNestedService(t *testing.T, k Kernel) *Service {
	t.Helper()
	st, err := OpenStore(testRoot(t), 0)
	if err != nil {
		t.Fatal(err)
	}
	pol := fastPolicy()
	pol.NestedRetry = time.Hour
	svc, err := NewService(Config{Store: st, Kernel: k, Policy: pol})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Close)
	return svc
}

// waitParkedNested starts op1 with req and waits until it is parked in
// WAIT_NESTED having issued no syscall.
func waitParkedNested(t *testing.T, svc *Service, k *fakeKernel, req RetireRequest) {
	t.Helper()
	_, _ = retire(t, svc, "op1", req, time.Millisecond)
	if !waitFor(t, 5*time.Second, func() bool {
		out, err := svc.Status(service(), "op1")
		return err == nil && out.State == StateWaitNested
	}) {
		out, _ := svc.Status(service(), "op1")
		t.Fatalf("setup: job never parked in WAIT_NESTED: %+v", out)
	}
	if got := flagsOf(k.recorded()); got != "" {
		t.Fatalf("setup: calls %q before the nested mount left, want none", got)
	}
}

// Review fix 4: EjectNow is sticky. The Quit policy escalates once at 30 s;
// a later plain Retire (e.g. a reconnect) must not cancel it.
func TestEjectNowIsSticky(t *testing.T) {
	child := Entry{FSID: FSID{0x2e000040, 0x2e}, Owner: 0, Type: "smbfs", MntOnName: testPath + "/child", MntFromName: "//nas/c"}
	k := newFake(rootEntry(), ourEntry(), child)
	k.busy[ourFSID] = true
	svc := parkedNestedService(t, k)
	mustRegister(t, svc, nfsRegister("op1"))
	waitParkedNested(t, svc, k, RetireRequest{Liveness: LivenessAlive, EjectNow: true})
	_, _ = retire(t, svc, "op1", RetireRequest{Liveness: LivenessAlive}, time.Millisecond)
	k.setEntries(rootEntry(), ourEntry())
	out, err := retire(t, svc, "op1", RetireRequest{Liveness: LivenessAlive}, 2*time.Second)
	if err != nil || out.State != StateRetired || !out.Forced {
		t.Fatalf("EjectNow lost on join: %+v err %v calls %q", out, err, flagsOf(k.recorded()))
	}
}

// A join that proves death AFTER a pass chose an ordinary unmount but BEFORE
// it issued the call must stop that call. Otherwise the ordinary unmount
// enters the kernel after the proof was recorded, sets MNT_LUNMOUNT, parks in
// VFS_SYNC against the dead server, and every later MNT_FORCE gets EBUSY.
// The beforeIssue hook places the join exactly in that window.
func TestJoinProvingDeathBeforeIssueStopsOrdinaryCall(t *testing.T) {
	for _, tc := range []struct {
		name string
		join RetireRequest
	}{
		{"dead", RetireRequest{Liveness: LivenessDead}},
		{"wedged", RetireRequest{Liveness: LivenessWedged}},
		{"eject-now", RetireRequest{Liveness: LivenessAlive, EjectNow: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := newFake(rootEntry(), ourEntry())
			k.dead[ourFSID] = true // an ordinary call would never return
			svc, _ := newTestService(t, k, nil)
			mustRegister(t, svc, nfsRegister("op1"))
			var joined atomic.Bool
			svc.beforeIssue = func(op string, flags int) {
				if flags == FlagNoBlock && joined.CompareAndSwap(false, true) {
					if _, err := svc.startOrJoin(service(), op, tc.join); err != nil {
						t.Errorf("join: %v", err)
					}
				}
			}
			out, err := retire(t, svc, "op1", RetireRequest{Liveness: LivenessAlive}, 2*time.Second)
			if !joined.Load() {
				t.Fatal("hook never saw an ordinary decision; the window was not exercised")
			}
			if got := flagsOf(k.recorded()); got != "F" {
				t.Fatalf("calls %q after the join proved death, want only MNT_FORCE (state %s err %v)", got, out.State, err)
			}
			if err != nil || out.State != StateRetired {
				t.Fatalf("%+v err %v", out, err)
			}
		})
	}
}

// Review fix 2: success-but-present backs off and ends as a surfaced
// ANOMALY instead of spinning.
func TestSuccessButPresentIsBoundedAnomaly(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.noop[ourFSID] = true
	svc, _ := newTestService(t, k, nil)
	mustRegister(t, svc, nfsRegister("op1"))
	out, err := retire(t, svc, "op1", RetireRequest{Liveness: LivenessAlive}, 2*time.Second)
	if err != nil || out.State != StateAnomaly || !errors.Is(out.Err, ErrPostState) {
		t.Fatalf("outcome %+v err %v, want ANOMALY", out, err)
	}
	if n := len(k.recorded()); n != 3 {
		t.Fatalf("%d syscalls, want exactly MaxZeroPresent=3", n)
	}
}

// And between those attempts it waits (no hot loop).
func TestSuccessButPresentBacksOff(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.noop[ourFSID] = true
	st, _ := OpenStore(testRoot(t), 0)
	pol := fastPolicy()
	pol.BackoffMin, pol.BackoffMax, pol.MaxZeroPresent = 40*time.Millisecond, 40*time.Millisecond, 1000
	svc, _ := NewService(Config{Store: st, Kernel: k, Policy: pol})
	defer svc.Close()
	mustRegister(t, svc, nfsRegister("op1"))
	_, _ = retire(t, svc, "op1", RetireRequest{Liveness: LivenessAlive}, 200*time.Millisecond)
	if n := len(k.recorded()); n > 8 {
		t.Fatalf("%d syscalls in 200ms with a 40ms backoff: spinning", n)
	}
}

type stuckHolders struct {
	calls   atomic.Int32
	release chan struct{}
}

func (h *stuckHolders) Holders(uint32, FSID) ([]Holder, error) {
	h.calls.Add(1)
	<-h.release
	return nil, nil
}

// Review fix 3: a holder listing stuck in the kernel (vn_stat on a wedged
// volume) pins an OS thread. Only one may exist per fsid, however many EBUSY
// rounds pass.
func TestStuckHolderListingIsSingleFlight(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.busy[ourFSID] = true
	h := &stuckHolders{release: make(chan struct{})}
	defer close(h.release)
	st, _ := OpenStore(testRoot(t), 0)
	pol := fastPolicy()
	pol.HolderWatch = 2 * time.Millisecond
	svc, _ := NewService(Config{Store: st, Kernel: k, Holders: h, Policy: pol})
	defer svc.Close()
	mustRegister(t, svc, nfsRegister("op1"))
	before := runtime.NumGoroutine()
	out, _ := retire(t, svc, "op1", RetireRequest{Liveness: LivenessAlive}, 400*time.Millisecond)
	rounds := len(k.recorded())
	if rounds < 5 {
		t.Fatalf("only %d EBUSY rounds; test did not exercise repetition", rounds)
	}
	if n := h.calls.Load(); n != 1 {
		t.Fatalf("%d concurrent stuck holder listings after %d EBUSY rounds, want 1", n, rounds)
	}
	if !out.HoldersStale {
		t.Fatalf("stuck listing not reported: %+v", out)
	}
	if g := runtime.NumGoroutine() - before; g > 4 {
		t.Fatalf("goroutines grew by %d", g)
	}
}

// Review fix 4: the issue gate allows one unmount syscall per fsid even
// across two jobs (two receipts naming one mount).
func TestIssueGateAcrossJobs(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	release := make(chan struct{})
	k.block = release
	svc, _ := newTestService(t, k, nil)
	if _, started := svc.issue(ourFSID, FlagNoBlock); !started {
		t.Fatal("first issue refused")
	}
	if _, started := svc.issue(ourFSID, FlagForce); started {
		t.Fatal("second issue for an fsid with a call in the kernel was started")
	}
	time.Sleep(20 * time.Millisecond)
	if got := flagsOf(k.recorded()); got != "N" {
		t.Fatalf("kernel calls %q, want only the first", got)
	}
	close(release)
}

func TestTwoJobsOneFSIDIssueOneCall(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	release := make(chan struct{})
	k.block = release
	svc, _ := newTestService(t, k, nil)
	mustRegister(t, svc, nfsRegister("op1"))
	mustRegister(t, svc, nfsRegister("op2"))
	_, _ = retire(t, svc, "op1", RetireRequest{Liveness: LivenessAlive}, 50*time.Millisecond)
	_, _ = retire(t, svc, "op2", RetireRequest{Liveness: LivenessDead, EjectNow: true}, 200*time.Millisecond)
	if got := flagsOf(k.recorded()); got != "N" {
		t.Fatalf("calls %q while op1's call was in the kernel, want only op1's", got)
	}
	// op2 must WAIT on the in-flight call (InKernelPoll cadence), not spin
	// against the issue gate re-taking snapshots.
	k.mu.Lock()
	snaps := k.snaps
	k.mu.Unlock()
	t.Logf("snapshots: %d", snaps)
	if snaps > 40 {
		t.Fatalf("%d snapshots in ~250ms (normal is ~16): op2 is spinning", snaps)
	}
	k.mu.Lock()
	k.block = nil
	k.mu.Unlock()
	close(release)
}

// Review fix 4: registration proves the live entry, not just the strings.
func TestRegisterVerifiesLiveEntry(t *testing.T) {
	e := ourEntry()
	e.Owner = 0
	k := newFake(rootEntry(), e)
	svc, st := newTestService(t, k, nil)
	if _, err := svc.Register(service(), nfsRegister("op1")); !errors.Is(err, ErrIdentityRefused) {
		t.Fatalf("register over a root-owned entry: %v", err)
	}
	if rs, _ := st.List(testUID); len(rs) != 0 {
		t.Fatalf("receipt stored: %+v", rs)
	}
}

func TestStatusRequiresRole(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	svc, _ := newTestService(t, k, nil)
	mustRegister(t, svc, nfsRegister("op1"))
	if _, err := svc.Status(Caller{UID: testUID, Role: RoleNone}, "op1"); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("unsigned peer status: %v", err)
	}
}

func TestStoreRefusesGroupOrOtherAccess(t *testing.T) {
	open := filepath.Join(t.TempDir(), "r")
	if err := os.Mkdir(open, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(open, 0); err == nil || !strings.Contains(err.Error(), "group/other") {
		t.Fatalf("0755 root accepted: %v", err)
	}
	root := testRoot(t)
	st, err := OpenStore(root, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "501"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(root, "501"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := st.Put(validReceipt("op1")); err == nil || !strings.Contains(err.Error(), "group/other") {
		t.Fatalf("0755 uid dir accepted: %v", err)
	}
}

func TestStoreRefusesForeignOwnedRoot(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("needs a non-root runner")
	}
	// /Library exists and is root-owned; Mkdir returns EEXIST, nothing is written.
	if _, err := OpenStore("/Library", 0); err == nil || !strings.Contains(err.Error(), "owned by uid 0") {
		t.Fatalf("root-owned dir accepted by a uid %d store: %v", os.Geteuid(), err)
	}
}

func TestReceiptLoadDoesNotFollowSymlinks(t *testing.T) {
	root := testRoot(t)
	st, _ := OpenStore(root, 0)
	if err := st.Put(validReceipt("op1")); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "501")
	elsewhere := filepath.Join(t.TempDir(), "planted.json")
	data, _ := os.ReadFile(filepath.Join(dir, "op1.json"))
	if err := os.WriteFile(elsewhere, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "op1.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, filepath.Join(dir, "op1.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(testUID, "op1"); !errors.Is(err, syscall.ELOOP) {
		t.Fatalf("symlinked receipt load: %v, want ELOOP", err)
	}
}

// Review fix 4: an orphan timer that fired but lost the race to Reclaim must
// not act.
func TestOrphanTimerLosingRaceToReclaimDoesNothing(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	svc, _ := newTestService(t, k, nil)
	mustRegister(t, svc, nfsRegister("op1"))
	fired, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	svc.orphanFired = func(uint32, string) { once.Do(func() { close(fired) }); <-proceed }
	if err := svc.Disconnected(testUID, "op1", LivenessUnknown); err != nil {
		t.Fatal(err)
	}
	<-fired
	if ok, err := svc.Reclaim(service(), "op1"); !ok || err != nil {
		t.Fatalf("reclaim %v %v", ok, err)
	}
	close(proceed)
	time.Sleep(100 * time.Millisecond)
	if n := len(k.recorded()); n != 0 {
		t.Fatalf("reclaimed op got %d syscalls from a fired timer", n)
	}
}

// Review fix 5: an op id is not reusable while the daemon remembers it, so a
// stale RETIRED result can never be returned for a new receipt.
func TestOpIDReuseIsRefused(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	svc, _ := newTestService(t, k, nil)
	mustRegister(t, svc, nfsRegister("op1"))
	if out, err := retire(t, svc, "op1", RetireRequest{Liveness: LivenessAlive}, 2*time.Second); err != nil || out.State != StateRetired {
		t.Fatalf("%+v %v", out, err)
	}
	k.setEntries(rootEntry(), ourEntry())
	if _, err := svc.Register(service(), nfsRegister("op1")); !errors.Is(err, ErrOpIDReused) {
		t.Fatalf("reused op id: %v", err)
	}
}

// Review fix 5: overlapping registrations share one in-flight snapshot.
func TestRegisterJoinsInFlightSnapshot(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	gate := make(chan struct{})
	k.snapBlock = gate
	svc, _ := newTestService(t, k, nil)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, op := range []string{"op1", "op2"} {
		wg.Add(1)
		go func(i int, op string) {
			defer wg.Done()
			_, errs[i] = svc.Register(service(), nfsRegister(op))
		}(i, op)
		time.Sleep(20 * time.Millisecond)
	}
	close(gate)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("register %d: %v", i, err)
		}
	}
	if k.snaps != 1 {
		t.Fatalf("%d kernel snapshots for two overlapping registrations, want 1", k.snaps)
	}
}

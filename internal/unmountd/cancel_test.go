package unmountd

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// slowBackoffService parks a busy ordinary job in BACKOFF after exactly one
// EBUSY (backoff 1 h), so every assertion below is deterministic.
func slowBackoffService(t *testing.T, k *fakeKernel) *Service {
	t.Helper()
	st, err := OpenStore(testRoot(t), 0)
	if err != nil {
		t.Fatal(err)
	}
	pol := fastPolicy()
	pol.BackoffMin, pol.BackoffMax, pol.ForceConfirmGap = time.Hour, time.Hour, time.Hour
	svc, err := NewService(Config{Store: st, Kernel: k, Policy: pol})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Close)
	return svc
}

func stateIs(svc *Service, op string, want State) func() bool {
	return func() bool { o, _ := svc.Status(service(), op); return o.State == want }
}

func TestCancelWithdrawsOrdinaryBackoffAndKeepsReceipt(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.busy[ourFSID] = true
	svc := slowBackoffService(t, k)
	mustRegister(t, svc, nfsRegister("op-1"))
	if _, err := retire(t, svc, "op-1", RetireRequest{Liveness: LivenessAlive}, 50*time.Millisecond); err == nil {
		t.Fatal("busy ordinary retirement finished")
	}
	if !waitFor(t, 2*time.Second, stateIs(svc, "op-1", StateBackoff)) {
		t.Fatal("job never reached BACKOFF")
	}
	out, err := svc.Cancel(service(), "op-1")
	if err != nil || out.State != StateCancelled {
		t.Fatalf("cancel: %+v %v", out, err)
	}
	if got := flagsOf(k.recorded()); got != "N" {
		t.Fatalf("calls %q, want exactly one ordinary", got)
	}
	if st, err := svc.Status(service(), "op-1"); err != nil || st.State != StateRegistered {
		t.Fatalf("cancelled op not REGISTERED with its receipt: %+v %v", st, err)
	}
	// A later "Eject now" starts a fresh job on the same receipt.
	k.mu.Lock()
	k.busy[ourFSID] = false
	k.mu.Unlock()
	if out, err := retire(t, svc, "op-1", RetireRequest{Liveness: LivenessAlive}, 2*time.Second); err != nil || out.State != StateRetired {
		t.Fatalf("retire after cancel: %+v %v", out, err)
	}
}

func TestCancelRefusesForcePathAndCallInKernel(t *testing.T) {
	t.Run("eject-now requested", func(t *testing.T) {
		k := newFake(rootEntry(), ourEntry())
		svc := slowBackoffService(t, k)
		mustRegister(t, svc, nfsRegister("op-f"))
		_, _ = retire(t, svc, "op-f", RetireRequest{Liveness: LivenessAlive, EjectNow: true}, 50*time.Millisecond)
		if !waitFor(t, 2*time.Second, stateIs(svc, "op-f", StateForceConfirm)) {
			t.Fatal("force path not reached")
		}
		if _, err := svc.Cancel(service(), "op-f"); !errors.Is(err, ErrNotCancellable) {
			t.Fatalf("force path cancelled: %v", err)
		}
	})
	t.Run("proven dead waiting on a nested mount", func(t *testing.T) {
		nested := Entry{FSID: otherFSID, Owner: testUID, Type: "smbfs", MntOnName: testPath + "/inner", MntFromName: "//x/y"}
		k := newFake(rootEntry(), ourEntry(), nested)
		svc := slowBackoffService(t, k)
		mustRegister(t, svc, nfsRegister("op-d"))
		_, _ = retire(t, svc, "op-d", RetireRequest{Liveness: LivenessDead}, 50*time.Millisecond)
		if !waitFor(t, 2*time.Second, stateIs(svc, "op-d", StateWaitNested)) {
			t.Fatal("force path did not wait on the nested mount")
		}
		if _, err := svc.Cancel(service(), "op-d"); !errors.Is(err, ErrNotCancellable) {
			t.Fatalf("proven-dead job cancelled: %v", err)
		}
	})
	t.Run("call in kernel is withdrawn, the call itself finishes", func(t *testing.T) {
		k := newFake(rootEntry(), ourEntry())
		k.block = make(chan struct{})
		svc := slowBackoffService(t, k)
		mustRegister(t, svc, nfsRegister("op-k"))
		_, _ = retire(t, svc, "op-k", RetireRequest{Liveness: LivenessAlive}, 50*time.Millisecond)
		if !waitFor(t, 2*time.Second, func() bool { return len(k.recorded()) == 1 }) {
			t.Fatal("no call issued")
		}
		res := make(chan error, 1)
		go func() { _, err := svc.Cancel(service(), "op-k"); res <- err }()
		if !waitFor(t, 2*time.Second, func() bool { return jobCancelled(svc, "op-k") }) {
			t.Fatal("cancel with an ordinary call in the kernel was refused")
		}
		// The ordinary call already in the kernel is not interruptible; the
		// volume was idle, so it unmounts. Nothing further is issued.
		close(k.block)
		if err := <-res; err != nil {
			t.Fatalf("cancel: %v", err)
		}
		if got := flagsOf(k.recorded()); got != "N" {
			t.Fatalf("calls %q, want the one ordinary call", got)
		}
	})
	t.Run("gui role", func(t *testing.T) {
		svc := slowBackoffService(t, newFake(rootEntry(), ourEntry()))
		if _, err := svc.Cancel(Caller{UID: testUID, Role: RoleGUI}, "op-g"); !errors.Is(err, ErrNotAuthorized) {
			t.Fatalf("gui cancelled: %v", err)
		}
	})
}

func jobOf(svc *Service, op string) *job {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	return svc.jobs[jobKey{testUID, op}]
}

func jobCancelled(svc *Service, op string) bool {
	j := jobOf(svc, op)
	return j != nil && j.isCancelled()
}

func entryPresent(k *fakeKernel) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, e := range k.snap.Entries {
		if e.FSID == ourFSID {
			return true
		}
	}
	return false
}

// A join after Cancel was accepted but before the job ended revives it, and
// the revived job really runs to completion.
func TestJoinRevivesWithdrawnJob(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.busy[ourFSID] = true
	svc := slowBackoffService(t, k)
	mustRegister(t, svc, nfsRegister("op-r"))
	_, _ = retire(t, svc, "op-r", RetireRequest{Liveness: LivenessAlive}, 50*time.Millisecond)
	if !waitFor(t, 2*time.Second, stateIs(svc, "op-r", StateBackoff)) {
		t.Fatal("no BACKOFF")
	}
	j := jobOf(svc, "op-r")
	j.mu.Lock()
	j.cancelled = true // Cancel accepted, job not yet woken
	j.mu.Unlock()
	k.mu.Lock()
	k.busy[ourFSID] = false // the apps closed their files
	k.mu.Unlock()
	out, err := retire(t, svc, "op-r", RetireRequest{Liveness: LivenessAlive}, 2*time.Second)
	if err != nil || out.State != StateRetired {
		t.Fatalf("revived job did not run to completion: %+v %v", out, err)
	}
	if got := flagsOf(k.recorded()); got != "NN" || entryPresent(k) {
		t.Fatalf("calls %q present=%v", got, entryPresent(k))
	}
}

// Reviewer race: converge has returned CANCELLED but the job has not left
// s.jobs. A request arriving now must be run, not answered with the stale
// CANCELLED (nil error).
func TestJoinAfterConvergeReturnedCancelledIsRun(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.busy[ourFSID] = true
	svc := slowBackoffService(t, k)
	svc.pol.ForceConfirmGap = 5 * time.Millisecond
	mustRegister(t, svc, nfsRegister("op-w"))
	gate := make(chan struct{})
	var hit atomic.Bool
	svc.afterConverge = func(op string, st State) {
		if st == StateCancelled && hit.CompareAndSwap(false, true) {
			<-gate
		}
	}
	_, _ = retire(t, svc, "op-w", RetireRequest{Liveness: LivenessAlive}, 50*time.Millisecond)
	if !waitFor(t, 2*time.Second, stateIs(svc, "op-w", StateBackoff)) {
		t.Fatal("no BACKOFF")
	}
	cancelDone := make(chan struct{})
	t.Cleanup(func() { <-cancelDone })
	go func() { defer close(cancelDone); _, _ = svc.Cancel(service(), "op-w") }()
	if !waitFor(t, 2*time.Second, hit.Load) {
		t.Fatal("converge never returned CANCELLED")
	}
	res := make(chan Outcome, 1)
	errs := make(chan error, 1)
	go func() {
		o, err := retire(t, svc, "op-w", RetireRequest{Liveness: LivenessDead}, 3*time.Second)
		res <- o
		errs <- err
	}()
	if !waitFor(t, 2*time.Second, func() bool { j := jobOf(svc, "op-w"); return j != nil && !j.isCancelled() }) {
		t.Fatal("the Dead request did not reach the job")
	}
	close(gate)
	out, err := <-res, <-errs
	if err != nil || out.State != StateRetired || !out.Forced {
		t.Fatalf("Dead-proof request swallowed by the withdrawn job: %+v %v", out, err)
	}
}

// After the job is marked ended (before removal) a request starts a fresh job.
func TestJoinAfterJobEndedStartsFreshJob(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.busy[ourFSID] = true
	svc := slowBackoffService(t, k)
	svc.pol.ForceConfirmGap = 5 * time.Millisecond
	mustRegister(t, svc, nfsRegister("op-e"))
	gate := make(chan struct{})
	var hit atomic.Bool
	svc.beforeRemove = func(op string) {
		if hit.CompareAndSwap(false, true) {
			<-gate
		}
	}
	_, _ = retire(t, svc, "op-e", RetireRequest{Liveness: LivenessAlive}, 50*time.Millisecond)
	if !waitFor(t, 2*time.Second, stateIs(svc, "op-e", StateBackoff)) {
		t.Fatal("no BACKOFF")
	}
	cancelDone := make(chan struct{})
	t.Cleanup(func() { <-cancelDone })
	go func() { defer close(cancelDone); _, _ = svc.Cancel(service(), "op-e") }()
	if !waitFor(t, 2*time.Second, hit.Load) {
		t.Fatal("job never ended")
	}
	res := make(chan Outcome, 1)
	errs := make(chan error, 1)
	go func() {
		o, err := retire(t, svc, "op-e", RetireRequest{Liveness: LivenessDead}, 3*time.Second)
		res <- o
		errs <- err
	}()
	time.Sleep(50 * time.Millisecond) // let the request find the ended job
	close(gate)
	out, err := <-res, <-errs
	if err != nil || out.State != StateRetired || !out.Forced {
		t.Fatalf("request joined an ended job: %+v %v", out, err)
	}
}

// Stress: a Dead-proof Retire racing a Cancel always retires (forced). The
// Cancel may land first (job withdrawn, then a fresh or revived job) or
// second (refused: force requested); never may it swallow the request.
func TestDeadRetireVersusCancelStress(t *testing.T) {
	const iters = 400
	for i := 0; i < iters; i++ {
		k := newFake(rootEntry(), ourEntry())
		k.busy[ourFSID] = true
		svc, _ := newTestService(t, k, nil)
		if i%2 == 1 {
			// Widen the revive window (converge returned CANCELLED, run not
			// yet decided) so the race reaches it, not only the outer edges:
			// without the revive the Dead request joins a job that then
			// ends CANCELLED and is answered by the old withdrawal.
			svc.afterConverge = func(_ string, st State) {
				if st == StateCancelled {
					time.Sleep(2 * time.Millisecond)
				}
			}
		}
		mustRegister(t, svc, nfsRegister("op-s"))
		_, _ = retire(t, svc, "op-s", RetireRequest{Liveness: LivenessAlive}, time.Millisecond)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := svc.Cancel(service(), "op-s")
			if err != nil && !errors.Is(err, ErrNotCancellable) {
				t.Errorf("iter %d: cancel: %v", i, err)
			}
		}()
		var out Outcome
		var rerr error
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			out, rerr = svc.Retire(ctx, service(), "op-s", RetireRequest{Liveness: LivenessDead})
		}()
		wg.Wait()
		if rerr != nil || out.State != StateRetired || entryPresent(k) {
			t.Fatalf("iter %d: Dead retire lost to Cancel: %+v %v present=%v calls=%q", i, out, rerr, entryPresent(k), flagsOf(k.recorded()))
		}
		svc.Close()
	}
}

// Reviewer probe: Cancel while an ordinary call is IN_KERNEL. The call
// returns EBUSY; the job must then end with no further call, and the
// volume must stay mounted (no later ordinary call may unmount it).
func TestCancelInKernelLeavesNoOrphanJob(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.block = make(chan struct{})
	var n atomic.Int32
	k.blockFn = func(int) error {
		if n.Add(1) == 1 {
			return syscall.EBUSY // the first call: apps still busy
		}
		return nil // any later call would unmount the now-idle volume
	}
	svc := slowBackoffService(t, k)
	mustRegister(t, svc, nfsRegister("op-i"))
	_, _ = retire(t, svc, "op-i", RetireRequest{Liveness: LivenessAlive}, 50*time.Millisecond)
	if !waitFor(t, 2*time.Second, stateIs(svc, "op-i", StateInKernel)) {
		t.Fatal("ordinary call never went IN_KERNEL")
	}
	res := make(chan Outcome, 1)
	errs := make(chan error, 1)
	go func() { o, err := svc.Cancel(service(), "op-i"); res <- o; errs <- err }()
	if !waitFor(t, 2*time.Second, func() bool { return jobCancelled(svc, "op-i") }) {
		t.Fatalf("cancel refused while IN_KERNEL: %v", <-errs)
	}
	// The IN_KERNEL loop may already have consumed Cancel's wake; take it
	// either way so the job cannot rely on it. The EBUSY backoff (1 h here)
	// must still end at once for a withdrawn job.
	time.Sleep(100 * time.Millisecond) // Cancel sends its wake after the durable mark
	if j := jobOf(svc, "op-i"); j != nil {
		select {
		case <-j.wake:
		default:
		}
	}
	close(k.block)
	if out, err := <-res, <-errs; err != nil || out.State != StateCancelled {
		t.Fatalf("cancel: %+v %v", out, err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := flagsOf(k.recorded()); got != "N" || !entryPresent(k) {
		t.Fatalf("withdrawn job kept going: calls %q present=%v", got, entryPresent(k))
	}
	if st, err := svc.Status(service(), "op-i"); err != nil || st.State != StateRegistered {
		t.Fatalf("status %+v %v", st, err)
	}
}

// C2: a Cancel accepted after a pass chose its ordinary call (state
// ORDINARY) but before issue stops that call.
func TestCancelBeforeIssueStopsChosenOrdinaryCall(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	svc := slowBackoffService(t, k)
	mustRegister(t, svc, nfsRegister("op-c"))
	var hit, accepted atomic.Bool
	cancelDone := make(chan struct{})
	t.Cleanup(func() { <-cancelDone }) // its store write must finish before TempDir cleanup
	svc.beforeIssue = func(op string, flags int) {
		if flags == FlagNoBlock && hit.CompareAndSwap(false, true) {
			go func() { defer close(cancelDone); _, _ = svc.Cancel(service(), op) }()
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) && !jobCancelled(svc, op) {
				time.Sleep(time.Millisecond)
			}
			accepted.Store(jobCancelled(svc, op))
		}
	}
	out, err := retire(t, svc, "op-c", RetireRequest{Liveness: LivenessAlive}, 3*time.Second)
	if !hit.Load() {
		t.Fatal("no ordinary decision observed")
	}
	if !accepted.Load() {
		t.Fatal("cancel in ORDINARY was refused")
	}
	if err != nil || out.State != StateCancelled {
		t.Fatalf("%+v %v", out, err)
	}
	if got := flagsOf(k.recorded()); got != "" || !entryPresent(k) {
		t.Fatalf("an ordinary call was issued after the withdrawal: %q", got)
	}
}

// The orphan timer must not restart an op whose owner's last request was
// Cancel: neither on a connection drop nor after a daemon restart (the
// launch sweep arms the same timer).
func TestOrphanTimerSkipsWithdrawnOp(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.busy[ourFSID] = true
	root := testRoot(t)
	st, err := OpenStore(root, 0)
	if err != nil {
		t.Fatal(err)
	}
	pol := fastPolicy()
	pol.BackoffMin, pol.BackoffMax = time.Hour, time.Hour
	svc, err := NewService(Config{Store: st, Kernel: k, Policy: pol})
	if err != nil {
		t.Fatal(err)
	}
	mustRegister(t, svc, nfsRegister("op-o"))
	_, _ = retire(t, svc, "op-o", RetireRequest{Liveness: LivenessAlive}, 50*time.Millisecond)
	if !waitFor(t, 2*time.Second, stateIs(svc, "op-o", StateBackoff)) {
		t.Fatal("no BACKOFF")
	}
	if out, err := svc.Cancel(service(), "op-o"); err != nil || out.State != StateCancelled {
		t.Fatalf("cancel: %+v %v", out, err)
	}
	k.mu.Lock()
	k.busy[ourFSID] = false // a restarted ordinary job would now unmount
	k.mu.Unlock()
	if err := svc.Disconnected(testUID, "op-o", LivenessUnknown); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * pol.OrphanReclaim)
	if got := flagsOf(k.recorded()); got != "N" || !entryPresent(k) {
		t.Fatalf("orphan timer restarted a withdrawn op: calls %q", got)
	}
	svc.Close()

	// Daemon restart: memory is gone; the durable mark still holds.
	st2, err := OpenStore(root, 0)
	if err != nil {
		t.Fatal(err)
	}
	svc2, err := NewService(Config{Store: st2, Kernel: k, Policy: pol})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc2.Close)
	rep, err := svc2.SweepAtLaunch()
	if err != nil || len(rep.Pending) != 1 {
		t.Fatalf("sweep: %v %v", rep, err)
	}
	if n := ArmLaunchOrphans(svc2, rep, t.Logf); n != 1 {
		t.Fatalf("armed %d", n)
	}
	time.Sleep(5 * pol.OrphanReclaim)
	if got := flagsOf(k.recorded()); got != "N" || !entryPresent(k) {
		t.Fatalf("launch orphan restarted a withdrawn op: calls %q", got)
	}
	// A new owner request clears the mark and runs.
	if out, err := retire(t, svc2, "op-o", RetireRequest{Liveness: LivenessAlive}, 2*time.Second); err != nil || out.State != StateRetired {
		t.Fatalf("retire after withdrawal: %+v %v", out, err)
	}
	if on, err := st2.Withdrawn(testUID, "op-o"); on || err != nil {
		t.Fatalf("withdrawn mark survived retirement: %v %v", on, err)
	}
}

// Control for the test above: without a Cancel the orphan timer does run.
func TestOrphanTimerRunsWhenNotWithdrawn(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	svc, _ := newTestService(t, k, nil)
	mustRegister(t, svc, nfsRegister("op-p"))
	if err := svc.Disconnected(testUID, "op-p", LivenessUnknown); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return !entryPresent(k) }) {
		t.Fatalf("orphan timer never retired: %q", flagsOf(k.recorded()))
	}
}

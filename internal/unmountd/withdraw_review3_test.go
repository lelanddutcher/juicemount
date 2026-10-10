package unmountd

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Review #3 finding 1: a withdrawn mark from an EARLIER given-up Stop stays on
// the op. When the owner later dies and the orphan timer fires with nobody
// reconnected, the timer skips the op (withdrawn) — but it must keep the op
// reclaimable, or a successor that starts after the window gets
// Reclaimed=false ("still held by a live owner") and the mount is stranded
// until reboot.
func TestStaleWithdrawnMarkKeepsOrphanReclaimable(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.busy[ourFSID] = true
	svc := slowBackoffService(t, k)
	mustRegister(t, svc, nfsRegister("op-x"))
	_, _ = retire(t, svc, "op-x", RetireRequest{Liveness: LivenessAlive}, 50*time.Millisecond)
	if !waitFor(t, 2*time.Second, stateIs(svc, "op-x", StateBackoff)) {
		t.Fatal("no BACKOFF")
	}
	if _, err := svc.Cancel(service(), "op-x"); err != nil {
		t.Fatal(err)
	}
	k.mu.Lock()
	k.busy[ourFSID] = false
	k.mu.Unlock()
	if err := svc.Disconnected(testUID, "op-x", LivenessUnknown); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * svc.pol.OrphanReclaim)
	if got := flagsOf(k.recorded()); got != "N" || !entryPresent(k) {
		t.Fatalf("orphan timer restarted a withdrawn op: calls %q", got)
	}
	if svc.idle() != true {
		t.Fatal("a parked op must not keep the daemon from going idle")
	}
	reclaimed, err := svc.Reclaim(service(), "op-x")
	if err != nil || !reclaimed {
		t.Fatalf("STRANDED: successor Reclaim after the window: reclaimed=%v err=%v", reclaimed, err)
	}
	// The successor's own retirement (a new owner request) runs.
	if out, err := retire(t, svc, "op-x", RetireRequest{Liveness: LivenessUnknown}, 2*time.Second); err != nil || out.State != StateRetired {
		t.Fatalf("successor retire: %+v %v", out, err)
	}
	// Taken once: a second claimant finds nothing.
	if again, _ := svc.Reclaim(service(), "op-x"); again {
		t.Fatal("parked op reclaimed twice")
	}
}

// The same strand after a daemon restart: the launch sweep arms the orphan
// timer for a still-mounted, durably withdrawn receipt; once it fires (and
// skips), a late successor must still reclaim it.
func TestLaunchSweepWithdrawnOrphanStaysReclaimable(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	root := testRoot(t)
	st, err := OpenStore(root, 0)
	if err != nil {
		t.Fatal(err)
	}
	pol := fastPolicy()
	svc, err := NewService(Config{Store: st, Kernel: k, Policy: pol})
	if err != nil {
		t.Fatal(err)
	}
	mustRegister(t, svc, nfsRegister("op-l"))
	if _, err := svc.Cancel(service(), "op-l"); err != nil { // not running: marks withdrawn
		t.Fatal(err)
	}
	svc.Close()
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
	ArmLaunchOrphans(svc2, rep, t.Logf)
	time.Sleep(5 * pol.OrphanReclaim)
	if got := flagsOf(k.recorded()); got != "" || !entryPresent(k) {
		t.Fatalf("launch orphan restarted a withdrawn op: %q", got)
	}
	if reclaimed, err := svc2.Reclaim(service(), "op-l"); err != nil || !reclaimed {
		t.Fatalf("STRANDED after daemon restart: reclaimed=%v err=%v", reclaimed, err)
	}
}

// Review #3 finding 3: Cancel on a job that has ENDED without any force must
// not answer ErrNotCancellable ("a force was requested or issued"). It
// returns the real final state.
func TestCancelOnEndedOrdinaryJobReportsFinalState(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	svc := slowBackoffService(t, k)
	mustRegister(t, svc, nfsRegister("op-n"))
	gate := make(chan struct{})
	var hit atomic.Bool
	svc.beforeRemove = func(op string) {
		if hit.CompareAndSwap(false, true) {
			<-gate
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = retire(t, svc, "op-n", RetireRequest{Liveness: LivenessAlive}, 3*time.Second)
	}()
	if !waitFor(t, 2*time.Second, hit.Load) {
		t.Fatal("job never ended")
	}
	res := make(chan Outcome, 1)
	errs := make(chan error, 1)
	go func() { o, err := svc.Cancel(service(), "op-n"); res <- o; errs <- err }()
	time.Sleep(30 * time.Millisecond) // Cancel finds the ended job
	close(gate)
	<-done
	out, err := <-res, <-errs
	if err != nil || out.State != StateRetired || out.Forced {
		t.Fatalf("cancel on an ended ordinary job: state=%s forced=%v err=%v (want RETIRED, no error)", out.State, out.Forced, err)
	}
}

// N8: a new owner Retire clears the withdrawn mark, in memory and durably,
// while its job is still running (so Store.Delete has not removed it).
func TestRetireClearsWithdrawnMarkBeforeItEnds(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.busy[ourFSID] = true
	svc := slowBackoffService(t, k)
	mustRegister(t, svc, nfsRegister("op-m"))
	if _, err := svc.Cancel(service(), "op-m"); err != nil { // not running: marks withdrawn
		t.Fatal(err)
	}
	if !svc.isWithdrawn(jobKey{testUID, "op-m"}) {
		t.Fatal("Cancel did not mark the op withdrawn")
	}
	if _, err := retire(t, svc, "op-m", RetireRequest{Liveness: LivenessAlive}, 50*time.Millisecond); err == nil {
		t.Fatal("busy retire finished")
	}
	if !waitFor(t, 2*time.Second, stateIs(svc, "op-m", StateBackoff)) {
		t.Fatal("no BACKOFF")
	}
	if on, err := svc.store.Withdrawn(testUID, "op-m"); on || err != nil {
		t.Fatalf("durable mark survived a new Retire: %v %v", on, err)
	}
	if svc.isWithdrawn(jobKey{testUID, "op-m"}) {
		t.Fatal("op still withdrawn after a new Retire")
	}
}

// N12: Cancel with no running job still marks the op withdrawn, so a later
// connection drop does not restart it.
func TestCancelWithoutJobMarksWithdrawn(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	svc, st := newTestService(t, k, nil)
	mustRegister(t, svc, nfsRegister("op-w"))
	out, err := svc.Cancel(service(), "op-w")
	if err != nil || out.State != StateRegistered {
		t.Fatalf("cancel: %+v %v", out, err)
	}
	if on, err := st.Withdrawn(testUID, "op-w"); !on || err != nil {
		t.Fatalf("no durable mark: %v %v", on, err)
	}
	if err := svc.Disconnected(testUID, "op-w", LivenessUnknown); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * svc.pol.OrphanReclaim)
	if got := flagsOf(k.recorded()); got != "" || !entryPresent(k) {
		t.Fatalf("orphan timer ran a withdrawn op: %q", got)
	}
}

// N9: deleting a receipt removes its withdrawn mark.
func TestStoreDeleteRemovesWithdrawnMark(t *testing.T) {
	st, err := OpenStore(testRoot(t), 0)
	if err != nil {
		t.Fatal(err)
	}
	r := validReceipt("op-d")
	if err := st.Put(r); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkWithdrawn(r.UID, r.OpID); err != nil {
		t.Fatal(err)
	}
	if err := st.Delete(r.UID, r.OpID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(st.withdrawnPath(r.UID, r.OpID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("withdrawn mark left behind by Delete: %v", err)
	}
}

// N10: a new receipt starts un-withdrawn even if a stale mark for the same
// op id was left on disk (an older helper build, or a crash between the
// receipt removal and the mark removal).
func TestStorePutClearsStaleWithdrawnMark(t *testing.T) {
	st, err := OpenStore(testRoot(t), 0)
	if err != nil {
		t.Fatal(err)
	}
	r := validReceipt("op-p")
	if err := st.Put(r); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkWithdrawn(r.UID, r.OpID); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(st.path(r.UID, r.OpID)); err != nil { // the receipt only
		t.Fatal(err)
	}
	if err := st.Put(r); err != nil {
		t.Fatal(err)
	}
	if on, err := st.Withdrawn(r.UID, r.OpID); on || err != nil {
		t.Fatalf("new receipt inherited a stale withdrawn mark: %v %v", on, err)
	}
}

// N20: two starters racing past the receipt read must end up on ONE job.
// The loser joins; it never replaces the winner's job in s.jobs (two jobs
// for one receipt, and the first to end deletes the other's entry).
func TestRacingStartersShareOneJob(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.busy[ourFSID] = true
	svc := slowBackoffService(t, k)
	mustRegister(t, svc, nfsRegister("op-r2"))
	release := make(chan struct{})
	var held atomic.Int32
	svc.beforeStart = func(string) {
		if held.Add(1) == 1 {
			<-release // the first starter stalls between the read and the publish
		}
	}
	var wg sync.WaitGroup
	var j1, j2 *job
	wg.Add(1)
	go func() {
		defer wg.Done()
		j, err := svc.startOrJoin(service(), "op-r2", RetireRequest{Liveness: LivenessAlive})
		if err != nil {
			t.Error(err)
		}
		j1 = j
	}()
	if !waitFor(t, 2*time.Second, func() bool { return held.Load() == 1 }) {
		t.Fatal("first starter never reached the seam")
	}
	j, err := svc.startOrJoin(service(), "op-r2", RetireRequest{Liveness: LivenessAlive})
	if err != nil {
		t.Fatal(err)
	}
	j2 = j
	close(release)
	wg.Wait()
	if j1 != j2 {
		t.Fatalf("racing starters got two jobs for one receipt (%p, %p)", j1, j2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _ = svc.Cancel(service(), "op-r2")
	select {
	case <-j2.done:
	case <-ctx.Done():
		t.Fatal("job did not end after Cancel")
	}
}

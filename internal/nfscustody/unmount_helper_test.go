package nfscustody

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

// fakeLease is a held receipt binding.
type fakeLease struct {
	op     string
	lost   chan struct{}
	mu     sync.Mutex
	closed bool
}

func (l *fakeLease) OpID() string          { return l.op }
func (l *fakeLease) Lost() <-chan struct{} { return l.lost }
func (l *fakeLease) Err() error            { return nil }
func (l *fakeLease) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
}
func (l *fakeLease) isClosed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed
}

type retireCall struct {
	op string
	rr client.RetireRequest
}

// fakeUnmountd records calls. onRetire models the kernel effect of a
// helper retirement (the test removes the mounted marker).
type fakeUnmountd struct {
	mu          sync.Mutex
	registers   []client.RegisterRequest
	retires     []retireCall
	leases      []*fakeLease
	registerErr error
	retireState string // final state to report; "" = RETIRED
	retireErr   error
	onRetire    func(retireCall)
	cancels     []string
	cancelErr   error
	// duringRetire runs on every Retire before it answers (e.g. to advance
	// a fake clock by the wait the real helper would take).
	duringRetire func()
}

func (f *fakeUnmountd) Cancel(_ context.Context, op string) (client.Outcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancels = append(f.cancels, op)
	if f.cancelErr != nil {
		return client.Outcome{OpID: op, State: "FORCE"}, f.cancelErr
	}
	return client.Outcome{OpID: op, State: "CANCELLED"}, nil
}

func (f *fakeUnmountd) cancelled() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.cancels...)
}

func (f *fakeUnmountd) Register(_ context.Context, rr client.RegisterRequest) (UnmountLease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registers = append(f.registers, rr)
	if f.registerErr != nil {
		return nil, f.registerErr
	}
	l := &fakeLease{op: rr.OpID, lost: make(chan struct{})}
	f.leases = append(f.leases, l)
	return l, nil
}

func (f *fakeUnmountd) Retire(_ context.Context, op string, rr client.RetireRequest, _ time.Duration) (client.Outcome, error) {
	f.mu.Lock()
	call := retireCall{op, rr}
	f.retires = append(f.retires, call)
	state, err, hook, during := f.retireState, f.retireErr, f.onRetire, f.duringRetire
	f.mu.Unlock()
	if during != nil {
		during()
	}
	if err != nil {
		return client.Outcome{OpID: op, State: "ORDINARY"}, err
	}
	if hook != nil {
		hook(call)
	}
	if state == "" {
		state = "RETIRED"
	}
	return client.Outcome{OpID: op, State: state}, nil
}

func (f *fakeUnmountd) snapshot() ([]client.RegisterRequest, []retireCall, []*fakeLease) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]client.RegisterRequest(nil), f.registers...), append([]retireCall(nil), f.retires...), append([]*fakeLease(nil), f.leases...)
}

// helperFixture runs the real Start/Stop against an owned scripted
// custodian. detach selects the custodian's answer to "detach": "detached"
// (descriptor tier works) or "retained_retry" (busy: the descriptor cannot
// detach). Nothing mounts; "mounted" is a marker file the observer reads.
func helperFixture(t *testing.T, detach string) (*Controller, string) {
	t.Helper()
	root := t.TempDir()
	marker := filepath.Join(root, "mounted")
	c, _, spec := harmlessMountChild(t, fmt.Sprintf("touch %q", marker))
	c.creator.Observe = func(context.Context) (mounttable.Snapshot, error) {
		rows := []mounttable.MountEntry{}
		if _, err := os.Stat(marker); err == nil {
			rows = append(rows, mounttable.MountEntry{Path: spec.MountPath, Source: spec.Source, Type: "nfs", FSID: [2]int32{31, 41}})
		}
		return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: rows}, nil
	}
	onDetach := fmt.Sprintf(`rm -f %q
printf '{"version":1,"session":"%%s","id":2,"state":"detached","boot":"%%s","operation":"%%s","path":"%%s","source":"%%s"}\n' "$session" "$boot" "$operation" "$path" "$source"
IFS= read -r releaseRequest || exit 67
printf '{"version":1,"session":"%%s","id":3,"state":"released","boot":"%%s","operation":"%%s","path":"%%s","source":"%%s"}\n' "$session" "$boot" "$operation" "$path" "$source"`, marker)
	if detach == "retained_retry" {
		// Busy: the descriptor keeps the mount; the custodian then holds,
		// reading stdin, exactly like the real one after a failed funmount.
		onDetach = `printf '{"version":1,"session":"%s","id":2,"state":"retained_retry","boot":"%s","operation":"%s","path":"%s","source":"%s"}\n' "$session" "$boot" "$operation" "$path" "$source"
while IFS= read -r more; do :; done
sleep 600`
	}
	helper := filepath.Join(root, "owned-custodian.sh")
	script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--nfs-custodian-capability" ]; then
  printf '%%s\n' '{"version":1,"id":0,"state":"available"}'
  exit 0
fi
[ "$1" = "--nfs-custodian" ] || exit 64
session="$2"; boot="$3"; operation="$4"; path="$5"; source="$6"
printf '{"version":1,"session":"%%s","id":0,"state":"root_admitted","boot":"%%s","operation":"%%s","path":"%%s","source":"%%s","fsid":[31,41]}\n' "$session" "$boot" "$operation" "$path" "$source"
IFS= read -r arm || exit 65
printf '{"version":1,"session":"%%s","id":1,"state":"armed","boot":"%%s","operation":"%%s","path":"%%s","source":"%%s","fsid":[31,41]}\n' "$session" "$boot" "$operation" "$path" "$source"
IFS= read -r detach || exit 66
%s
`, onDetach)
	if err := os.WriteFile(helper, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	c.helper = helper
	return c, marker
}

func journalState(t *testing.T, c *Controller) mountbroker.State {
	t.Helper()
	r, err := mountbroker.ReadRecordReadonly(c.creator.Directory, c.spec.MountPath)
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	return r.State
}

func TestNFSStartRegistersExactReceiptAndHoldsLease(t *testing.T) {
	c, _ := helperFixture(t, "detached")
	fake := &fakeUnmountd{}
	c.SetUnmountHelper(fake)
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	regs, _, leases := fake.snapshot()
	rec := c.Record()
	if len(regs) != 1 || rec == nil || rec.Mount == nil {
		t.Fatalf("mount birth did not register exactly one receipt: %+v", regs)
	}
	want := client.RegisterRequest{OpID: "nfs-" + rec.OperationID, RegisterArgs: client.RegisterArgs{
		Type: "nfs", FSID: [2]int32{31, 41}, Token: strings.TrimPrefix(c.spec.ExportPath, "/jm-"),
		MntOnName: c.spec.MountPath, MntFromName: c.spec.Source}}
	if regs[0] != want {
		t.Fatalf("receipt claim differs from the observed mount:\n got %+v\nwant %+v", regs[0], want)
	}
	if st := c.HelperStatus(); st.State != HelperStateRegistered || st.OpID != want.OpID || leases[0].isClosed() {
		t.Fatalf("lease not held after mount: %+v", st)
	}
	if err := c.Stop(context.Background(), 5*time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	_, retires, _ := fake.snapshot()
	// The descriptor detached it; the helper only clears the absent receipt.
	if len(retires) != 1 || retires[0].op != want.OpID || retires[0].rr.EjectNow || retires[0].rr.Liveness != client.LivenessAlive {
		t.Fatalf("receipt not cleared after descriptor detach: %+v", retires)
	}
	if !leases[0].isClosed() || journalState(t, c) != mountbroker.Retired {
		t.Fatal("lease kept or journal not retired after Stop")
	}
}

func TestNFSRegisterFailureKeepsDescriptorBehaviour(t *testing.T) {
	c, _ := helperFixture(t, "detached")
	c.SetUnmountHelper(&fakeUnmountd{registerErr: client.ErrUnavailable})
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("an unavailable helper must not fail the mount: %v", err)
	}
	if st := c.HelperStatus(); st.State != HelperStateRegisterFailed || !strings.Contains(st.Error, "unavailable") {
		t.Fatalf("register failure not exposed: %+v", st)
	}
	if err := c.Stop(context.Background(), 5*time.Second); err != nil {
		t.Fatalf("descriptor Stop: %v", err)
	}
}

// A busy volume: the descriptor detach is retained. Stop hands the
// registered fsid to the helper; after the helper's RETIRED proof and a
// fresh absence it kills only the held custodian and retires the journal.
func TestNFSStopFallsBackToHelperWhenDescriptorDetachRetained(t *testing.T) {
	c, marker := helperFixture(t, "retained_retry")
	fake := &fakeUnmountd{}
	fake.onRetire = func(call retireCall) { _ = os.Remove(marker) } // the helper's kernel effect
	c.SetUnmountHelper(fake)
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := c.Stop(context.Background(), 5*time.Second); err != nil {
		t.Fatalf("Stop did not complete through the helper: %v", err)
	}
	_, retires, _ := fake.snapshot()
	if len(retires) != 1 || retires[0].op != c.HelperOpID() || retires[0].rr.Liveness != client.LivenessAlive || retires[0].rr.EjectNow {
		t.Fatalf("Stop fallback was not one ordinary retirement: %+v", retires)
	}
	if !c.Retired() || journalState(t, c) != mountbroker.Retired || c.HelperStatus().State != HelperStateRetired {
		t.Fatalf("helper proof did not complete durable retirement: %+v", c.HelperStatus())
	}
	c.mu.Lock()
	s, abandoned := c.session, c.abandonedKeeper
	c.mu.Unlock()
	if !abandoned || !keeperJoined(s) {
		t.Fatal("custodian holding a dead descriptor was not joined")
	}
}

// Without proof (helper busy, pending, or absent) Stop retains everything.
func TestNFSStopWithoutHelperProofRetainsBusyMount(t *testing.T) {
	for name, fake := range map[string]*fakeUnmountd{
		"pending": {retireErr: client.ErrPending},
		"refused": {retireState: "REFUSED_IDENTITY"},
	} {
		t.Run(name, func(t *testing.T) {
			c, marker := helperFixture(t, "retained_retry")
			c.SetUnmountHelper(fake)
			if err := c.Start(context.Background()); err != nil {
				t.Fatalf("Start: %v", err)
			}
			if err := c.Stop(context.Background(), 3*time.Second); err == nil || c.Retired() {
				t.Fatal("Stop retired without helper proof")
			}
			if _, err := os.Stat(marker); err != nil || journalState(t, c) != mountbroker.Observed {
				t.Fatal("retained mount or journal changed")
			}
			// The ordinary job this Stop started must not outlive it.
			if got := fake.cancelled(); len(got) != 1 || got[0] != c.HelperOpID() {
				t.Fatalf("failed Stop left its ordinary helper job running: cancels=%v", got)
			}
			// Cleanup: the owned scripted custodian is ours; end it.
			c.mu.Lock()
			s := c.session
			c.mu.Unlock()
			_ = c.abandonKeeper(s, 3*time.Second)
		})
	}
}

// Eject-now while a Stop is blocked: RetireWithHelper does not take the
// controller gate and the next Stop consumes its proof.
func TestNFSEjectNowProofCompletesLaterStop(t *testing.T) {
	c, marker := helperFixture(t, "retained_retry")
	fake := &fakeUnmountd{retireErr: client.ErrPending}
	c.SetUnmountHelper(fake)
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := c.Stop(context.Background(), 2*time.Second); err == nil {
		t.Fatal("busy Stop completed without proof")
	}
	fake.mu.Lock()
	fake.retireErr = nil
	fake.onRetire = func(retireCall) { _ = os.Remove(marker) }
	fake.mu.Unlock()
	out, err := c.RetireWithHelper(context.Background(), client.RetireRequest{Liveness: client.LivenessAlive, EjectNow: true}, time.Minute)
	if err != nil || out.State != "RETIRED" {
		t.Fatalf("eject-now: %+v %v", out, err)
	}
	_, retires, _ := fake.snapshot()
	if last := retires[len(retires)-1]; !last.rr.EjectNow {
		t.Fatalf("eject-now did not reach the helper: %+v", last)
	}
	if err := c.Stop(context.Background(), 5*time.Second); err != nil {
		t.Fatalf("Stop after eject-now: %v", err)
	}
	if journalState(t, c) != mountbroker.Retired {
		t.Fatal("journal not retired")
	}
}

func TestNFSRetireWithHelperRefusesUnregistered(t *testing.T) {
	c, _ := helperFixture(t, "detached")
	c.SetUnmountHelper(&fakeUnmountd{})
	if _, err := c.RetireWithHelper(context.Background(), client.RetireRequest{}, time.Second); err == nil {
		t.Fatal("retire without a registered receipt")
	}
	var nilc *Controller
	if _, err := nilc.RetireWithHelper(context.Background(), client.RetireRequest{}, time.Second); err == nil {
		t.Fatal("nil controller accepted")
	}
}

// /eject-now's 30 s floor is measured from the first ordinary retire that
// came back still busy; a Stop's cancel clears it.
func TestNFSOrdinaryBusyTrackedAndClearedByCancel(t *testing.T) {
	c, _ := helperFixture(t, "retained_retry")
	fake := &fakeUnmountd{retireErr: client.ErrPending}
	c.SetUnmountHelper(fake)
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		c.mu.Lock()
		s := c.session
		c.mu.Unlock()
		_ = c.abandonKeeper(s, 3*time.Second)
	})
	base := time.Unix(1_000_000, 0)
	now := base
	old := helperNow
	helperNow = func() time.Time { return now }
	t.Cleanup(func() { helperNow = old })
	for i := 0; i < 2; i++ {
		_, _ = c.RetireWithHelper(context.Background(), client.RetireRequest{Liveness: client.LivenessAlive}, time.Second)
		now = now.Add(20 * time.Second)
	}
	if since, last := c.OrdinaryBusy(); !since.Equal(base) || !last.Equal(base.Add(20*time.Second)) {
		t.Fatalf("busy window %v..%v", since, last)
	}
	// After a gap longer than OrdinaryBusyMaxGap the window restarts.
	now = now.Add(OrdinaryBusyMaxGap + time.Second)
	gapAt := now
	_, _ = c.RetireWithHelper(context.Background(), client.RetireRequest{Liveness: client.LivenessAlive}, time.Second)
	if since, _ := c.OrdinaryBusy(); !since.Equal(gapAt) {
		t.Fatalf("stale busy window carried over: since=%v", since)
	}
	// A forcing request is not an ordinary busy observation.
	_, _ = c.RetireWithHelper(context.Background(), client.RetireRequest{Liveness: client.LivenessAlive, EjectNow: true}, time.Second)
	if _, last := c.OrdinaryBusy(); !last.Equal(gapAt) {
		t.Fatal("eject-now counted as ordinary busy")
	}
	c.CancelHelperRetire()
	if since, _ := c.OrdinaryBusy(); !since.IsZero() || len(fake.cancelled()) != 1 {
		t.Fatalf("cancel did not clear the window / reach the helper: %v %v", since, fake.cancelled())
	}
}

// The eject-now busy window starts when the first ordinary request ARRIVES.
// Each ordinary retire waits ~3 s in the helper; with POSTs at t=0 and t=30
// the floor must be met at the second POST's return (t=33), not land on a
// coin flip of the two round trips.
func TestNFSOrdinaryBusyWindowStartsAtArrival(t *testing.T) {
	c, _ := helperFixture(t, "retained_retry")
	base := time.Unix(1_000_000, 0)
	now := base
	var clockMu sync.Mutex
	old := helperNow
	helperNow = func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
	t.Cleanup(func() { helperNow = old })
	fake := &fakeUnmountd{retireErr: client.ErrPending, duringRetire: func() {
		clockMu.Lock()
		now = now.Add(3 * time.Second) // the helper's ordinary wait
		clockMu.Unlock()
	}}
	c.SetUnmountHelper(fake)
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		c.mu.Lock()
		s := c.session
		c.mu.Unlock()
		_ = c.abandonKeeper(s, 3*time.Second)
	})
	_, _ = c.RetireWithHelper(context.Background(), client.RetireRequest{Liveness: client.LivenessAlive}, 3*time.Second)
	clockMu.Lock()
	now = base.Add(30 * time.Second)
	clockMu.Unlock()
	_, _ = c.RetireWithHelper(context.Background(), client.RetireRequest{Liveness: client.LivenessAlive}, 3*time.Second)
	since, last := c.OrdinaryBusy()
	if !since.Equal(base) {
		t.Fatalf("busy window starts at %v, want the first request's arrival %v", since.Sub(base), time.Duration(0))
	}
	if !last.Equal(base.Add(33*time.Second)) || last.Sub(since) < 30*time.Second {
		t.Fatalf("busy window %v..%v does not span the 30 s floor", since.Sub(base), last.Sub(base))
	}
}

// DELETE /eject-now's controller half: the withdrawal reaches the helper,
// clears the busy window, and a force-path refusal is returned, not hidden.
func TestNFSWithdrawHelperRetireReportsRefusal(t *testing.T) {
	c, _ := helperFixture(t, "retained_retry")
	fake := &fakeUnmountd{retireErr: client.ErrPending}
	c.SetUnmountHelper(fake)
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		c.mu.Lock()
		s := c.session
		c.mu.Unlock()
		_ = c.abandonKeeper(s, 3*time.Second)
	})
	_, _ = c.RetireWithHelper(context.Background(), client.RetireRequest{Liveness: client.LivenessAlive}, time.Second)
	if since, _ := c.OrdinaryBusy(); since.IsZero() {
		t.Fatal("no busy window recorded")
	}
	out, err := c.WithdrawHelperRetire(context.Background())
	if err != nil || out.State != "CANCELLED" || len(fake.cancelled()) != 1 {
		t.Fatalf("withdraw: %+v %v %v", out, err, fake.cancelled())
	}
	if since, _ := c.OrdinaryBusy(); !since.IsZero() {
		t.Fatal("withdraw kept the busy window")
	}
	fake.mu.Lock()
	fake.cancelErr = client.ErrNotCancellable
	fake.mu.Unlock()
	if _, err := c.WithdrawHelperRetire(context.Background()); !errors.Is(err, client.ErrNotCancellable) {
		t.Fatalf("force-path refusal hidden: %v", err)
	}
}

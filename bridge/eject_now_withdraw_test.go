package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/nfscustody"
	"github.com/lelanddutcher/juicemount/internal/unmountd"
	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

func TestEjectNowDeleteWithdrawsOrdinaryRetire(t *testing.T) {
	useFakeServiceHelper(t, &fakeServiceHelper{})
	ctl := &helperNFSCustody{fakeNFSCustody: fakeNFSCustody{path: "/Volumes/zpool"}}
	publishNFSController(t, ctl)
	if code, _ := doEjectNow(t, http.MethodDelete, false); code != http.StatusForbidden || ctl.withdraws != 0 {
		t.Fatalf("headerless DELETE: %d withdraws=%d", code, ctl.withdraws)
	}
	code, body := doEjectNow(t, http.MethodDelete, true)
	if code != http.StatusOK || body["ok"] != true || body["state"] != "CANCELLED" || ctl.withdraws != 1 {
		t.Fatalf("DELETE: %d %v withdraws=%d", code, body, ctl.withdraws)
	}
	if len(ctl.calls) != 0 {
		t.Fatalf("DELETE sent a retire: %+v", ctl.calls)
	}
	ctl.withdrawErr = client.ErrNotCancellable
	code, body = doEjectNow(t, http.MethodDelete, true)
	if code != http.StatusConflict || body["ok"] != false || body["state"] != "force_in_progress" {
		t.Fatalf("DELETE on a force: %d %v", code, body)
	}
	publishNFSController(t, nil)
	if code, _ := doEjectNow(t, http.MethodDelete, true); code != http.StatusConflict {
		t.Fatalf("DELETE with no owned mount: %d", code)
	}
}

// The t=30 POST forces deterministically when the controller measures the
// busy window from request arrival: POST at t=0 (its wait returns at t=3),
// POST at t=30 (returns at t=33) -> 33 s >= 30 s floor.
func TestEjectNowSecondPostAtThirtySecondsForces(t *testing.T) {
	useFakeServiceHelper(t, &fakeServiceHelper{})
	now := fakeEjectClock(t)
	ctl := &helperNFSCustody{fakeNFSCustody: fakeNFSCustody{path: "/Volumes/zpool"}, ordinaryErr: client.ErrPending}
	publishNFSController(t, ctl)
	start := *now
	// Model the real controller: window starts at arrival, the wait takes 3 s.
	ctl.onOrdinary = func() { *now = now.Add(3 * time.Second) }
	if code, _ := doEjectNow(t, http.MethodPost, true); code != http.StatusAccepted {
		t.Fatalf("first POST: %d", code)
	}
	*now = start.Add(30 * time.Second)
	code, body := doEjectNow(t, http.MethodPost, true)
	if code != http.StatusOK || forced(ctl.calls) != 1 {
		t.Fatalf("POST at t=30 did not force: %d %v %+v", code, body, ctl.calls)
	}
}

// --- end to end against the real helper core -------------------------------

const e2eUID = uint32(501)

var (
	e2eToken  = strings.Repeat("0123456789abcdef", 2)
	e2eFSID   = unmountd.FSID{0x1a000031, 0x31}
	e2eSource = "127.0.0.1:/jm-" + e2eToken
)

// e2eKernel is a minimal XNU model: ordinary unmounts of a busy mount return
// EBUSY; any successful unmount removes the entry.
type e2eKernel struct {
	mu    sync.Mutex
	busy  bool
	there bool
	calls []int
}

func (k *e2eKernel) Snapshot() (unmountd.Snapshot, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	es := []unmountd.Entry{{FSID: unmountd.FSID{0x1000004, 0x1a}, Owner: 0, Type: "apfs", MntOnName: "/", MntFromName: "/dev/disk3s1s1"}}
	if k.there {
		es = append(es, unmountd.Entry{FSID: e2eFSID, Owner: e2eUID, Type: "nfs", MntOnName: "/Volumes/zpool", MntFromName: e2eSource})
	}
	return unmountd.Snapshot{Boot: "11111111-2222-3333-4444-555555555555", Entries: es}, nil
}

func (k *e2eKernel) UnmountByFSID(fsid unmountd.FSID, flags int) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.calls = append(k.calls, flags)
	if !k.there {
		return syscall.ENOENT
	}
	if k.busy && flags&unmountd.FlagForce == 0 {
		return syscall.EBUSY
	}
	k.there = false
	return nil
}

func (k *e2eKernel) state() (calls int, present bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.calls), k.there
}

// coreBackedNFS speaks the bridge's controller surface over a real helper
// core (the same Retire/Cancel the XPC transport dispatches to).
type coreBackedNFS struct {
	fakeNFSCustody
	svc  *unmountd.Service
	op   string
	once sync.Once
	gate nfscustody.HelperRetireGate
	// beforeHelper runs after the gate admitted a request and before it
	// reaches the helper core (probes interleave a DELETE there).
	beforeHelper func(rr client.RetireRequest)
}

func e2eLiveness(s string) unmountd.Liveness {
	switch s {
	case client.LivenessAlive:
		return unmountd.LivenessAlive
	case client.LivenessDead:
		return unmountd.LivenessDead
	case client.LivenessWedged:
		return unmountd.LivenessWedged
	}
	return unmountd.LivenessUnknown
}

func (c *coreBackedNFS) caller() unmountd.Caller {
	return unmountd.Caller{UID: e2eUID, Role: unmountd.RoleService, SigningID: "com.juicemount.service"}
}

// g is the production ordering gate on the e2e clock.
func (c *coreBackedNFS) g() *nfscustody.HelperRetireGate {
	c.once.Do(func() { c.gate.Now = func() time.Time { return ejectNowNow() } })
	return &c.gate
}

func (c *coreBackedNFS) RetireWithHelper(ctx context.Context, rr client.RetireRequest, wait time.Duration) (client.Outcome, error) {
	return c.RetireWithHelperAt(ctx, c.HelperWithdrawGeneration(), rr, wait)
}
func (c *coreBackedNFS) HelperWithdrawGeneration() uint64 { return c.g().Generation() }
func (c *coreBackedNFS) RetireWithHelperAt(ctx context.Context, gen uint64, rr client.RetireRequest, wait time.Duration) (client.Outcome, error) {
	return c.retireAt(ctx, gen, rr, wait, c.g().Retire)
}

// StartRetireWithHelperAt runs the gate's start path, like the controller's.
func (c *coreBackedNFS) StartRetireWithHelperAt(ctx context.Context, gen uint64, rr client.RetireRequest, wait time.Duration) (client.Outcome, error) {
	return c.retireAt(ctx, gen, rr, wait, c.g().RetireStart)
}

func (c *coreBackedNFS) retireAt(ctx context.Context, gen uint64, rr client.RetireRequest, wait time.Duration,
	gateRetire func(context.Context, uint64, client.RetireRequest,
		func(context.Context) (client.Outcome, error), func(context.Context) (client.Outcome, error)) (client.Outcome, error)) (client.Outcome, error) {
	out, err := gateRetire(ctx, gen, rr, func(ctx context.Context) (client.Outcome, error) {
		if c.beforeHelper != nil {
			c.beforeHelper(rr)
		}
		wctx, cancel := context.WithTimeout(ctx, wait)
		defer cancel()
		out, err := c.svc.Retire(wctx, c.caller(), c.op, unmountd.RetireRequest{Liveness: e2eLiveness(rr.Liveness), EjectNow: rr.EjectNow})
		res := client.Outcome{OpID: out.OpID, State: string(out.State), Forced: out.Forced}
		if errors.Is(err, context.DeadlineExceeded) {
			return res, client.ErrPending
		}
		return res, err
	}, c.helperCancel)
	if err == nil && out.State != string(unmountd.StateRetired) {
		err = errors.New("ended " + out.State)
	}
	return out, err
}
func (c *coreBackedNFS) helperCancel(context.Context) (client.Outcome, error) {
	out, err := c.svc.Cancel(c.caller(), c.op)
	if errors.Is(err, unmountd.ErrNotCancellable) {
		err = client.ErrNotCancellable
	}
	return client.Outcome{OpID: out.OpID, State: string(out.State), Forced: out.Forced}, err
}
func (c *coreBackedNFS) HelperStatus() nfscustody.HelperStatus {
	return nfscustody.HelperStatus{OpID: c.op, State: nfscustody.HelperStateRegistered}
}

// CancelHelperRetire is JuiceMount's own withdrawal, like the controller's.
func (c *coreBackedNFS) CancelHelperRetire() {
	_, _ = c.g().WithdrawInternal(context.Background(), c.helperCancel)
}
func (c *coreBackedNFS) WithdrawHelperRetire(ctx context.Context) (client.Outcome, error) {
	return c.g().Withdraw(ctx, c.helperCancel)
}
func (c *coreBackedNFS) OrdinaryBusy() (time.Time, time.Time) { return c.g().Busy() }

// "POST then DELETE": the user clicks Eject now (the ordinary job starts and
// stays busy), then Cancel Quit. After the DELETE, even once the apps let go,
// no further unmount may happen: the volume stays mounted.
func TestEjectNowPostThenDeleteNoUnmountAfterwards(t *testing.T) {
	useFakeServiceHelper(t, &fakeServiceHelper{})
	oldWait := ejectNowOrdinaryWait
	ejectNowOrdinaryWait = 100 * time.Millisecond
	t.Cleanup(func() { ejectNowOrdinaryWait = oldWait })
	k := &e2eKernel{busy: true, there: true}
	st, err := unmountd.OpenStore(filepath.Join(t.TempDir(), "unmountd"), 0)
	if err != nil {
		t.Fatal(err)
	}
	pol := unmountd.DefaultPolicy()
	pol.BackoffMin, pol.BackoffMax = 5*time.Millisecond, 20*time.Millisecond
	pol.SnapshotWatch, pol.Watch, pol.InKernelPoll = time.Second, time.Second, 20*time.Millisecond
	svc, err := unmountd.NewService(unmountd.Config{Store: st, Kernel: k, Policy: pol})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Close)
	ctl := &coreBackedNFS{fakeNFSCustody: fakeNFSCustody{path: "/Volumes/zpool"}, svc: svc, op: "nfs-" + e2eToken}
	if _, err := svc.Register(ctl.caller(), unmountd.RegisterRequest{OpID: ctl.op, Type: unmountd.TypeNFS, FSID: e2eFSID,
		Token: e2eToken, MntOnName: "/Volumes/zpool", MntFromName: e2eSource}); err != nil {
		t.Fatal(err)
	}
	publishNFSController(t, ctl)
	if code, body := doEjectNow(t, http.MethodPost, true); code != http.StatusAccepted {
		t.Fatalf("POST on a busy volume: %d %v", code, body)
	}
	if n, _ := k.state(); n == 0 {
		t.Fatal("POST started no ordinary job")
	}
	code, body := doEjectNow(t, http.MethodDelete, true)
	if code != http.StatusOK || body["ok"] != true {
		t.Fatalf("DELETE: %d %v", code, body)
	}
	after, _ := k.state()
	k.mu.Lock()
	k.busy = false // the apps close their files
	k.mu.Unlock()
	time.Sleep(300 * time.Millisecond) // many backoff periods
	if n, present := k.state(); n != after || !present {
		t.Fatalf("unmount after DELETE: calls %d -> %d, present=%v", after, n, present)
	}
}

// C14: the server start wires the exact-child liveness into the monitor.
func TestStaleNFSRecoveryWiresExactLiveness(t *testing.T) {
	calls := 0
	old := juiceFSLivenessSource
	juiceFSLivenessSource = func() (bool, bool) { calls++; return false, true }
	t.Cleanup(func() { juiceFSLivenessSource = old })
	m := health.New(health.Config{RedisURL: "127.0.0.1:1"})
	if _, proven := m.JuiceFSLiveness(); proven {
		t.Fatal("unwired monitor claims proof")
	}
	wireStaleNFSRecovery(m)
	if alive, proven := m.JuiceFSLiveness(); alive || !proven || calls != 1 {
		t.Fatalf("monitor liveness not the exact-child source: alive=%v proven=%v calls=%d", alive, proven, calls)
	}
	// And the production source is the exact child's own Wait.
	juiceFSLivenessSource = old
	globalMu.Lock()
	prev := globalFUSE
	globalFUSE = nil
	globalMu.Unlock()
	t.Cleanup(func() { globalMu.Lock(); globalFUSE = prev; globalMu.Unlock() })
	if _, proven := m.JuiceFSLiveness(); proven {
		t.Fatal("no FUSE manager in this process, yet liveness proven")
	}
}

// C14 grep gate: the NFS server start calls the wiring exactly once.
func TestServerStartCallsStaleNFSRecoveryWiring(t *testing.T) {
	src, err := os.ReadFile("cbridge.go")
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`(?m)^\s*wireStaleNFSRecovery\(globalMonitor\)\s*$`)
	if n := len(re.FindAll(src, -1)); n != 1 {
		t.Fatalf("cbridge.go calls wireStaleNFSRecovery(globalMonitor) %d times, want 1", n)
	}
}

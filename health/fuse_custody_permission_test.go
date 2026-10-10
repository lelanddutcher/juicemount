package health

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/fusecustody"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

// permissionCustodian models the custodian's root open under the macOS
// Network Volumes (TCC) prompt: held until approved, or refused.
type permissionCustodian struct {
	mu           sync.Mutex
	denied       bool          // root open failed with EPERM
	joined       bool          // the denied helper exited and was reaped
	approve      chan struct{} // closed when the prompt is approved
	uuid         string
	fsid         [2]int32
	ready        bool
	detached     bool
	released     bool
	waits        int
	detachCalls  int
	releaseCalls int
}

func (c *permissionCustodian) WaitReady(timeout time.Duration) error {
	c.mu.Lock()
	c.waits++
	denied, ready, approve := c.denied, c.ready, c.approve
	c.mu.Unlock()
	if denied {
		return fusecustody.ErrAdmissionPermissionDenied
	}
	if ready {
		return nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-approve:
		c.mu.Lock()
		c.ready = true
		c.mu.Unlock()
		return nil
	case <-timer.C:
		return fusecustody.ErrAdmissionPending
	}
}
func (c *permissionCustodian) Identity() (string, bool, [2]int32, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.uuid, false, c.fsid, c.ready
}
func (c *permissionCustodian) Detach(time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.detachCalls++
	c.detached = true
	return nil
}
func (c *permissionCustodian) Release(time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.releaseCalls++
	c.released = true
	return nil
}
func (c *permissionCustodian) Detached() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.detached
}
func (c *permissionCustodian) PendingDetach() bool { return false }
func (c *permissionCustodian) ReconcileDetach(time.Duration) error {
	return errors.New("no pending detach")
}
func (c *permissionCustodian) NeverAdmittedSettled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.denied && c.joined
}
func (c *permissionCustodian) Settled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.released
}
func (c *permissionCustodian) PID() int { return 4242 }

const permissionTestUUID = "12345678-1234-1234-1234-123456789012"

type permissionFixture struct {
	fm      *FUSEManager
	receipt *MountReceipt
	path    string
	source  string
	fsid    [2]int32
	present func() bool
}

// The mount stays present until a custodian of this receipt detaches it.
// The JuiceFS child is a harmless held test process, released after detach.
func newPermissionFixture(t *testing.T, keeper fuseCustodian) *permissionFixture {
	t.Helper()
	f := &permissionFixture{path: t.TempDir(), source: "owned-source", fsid: [2]int32{7, 9}}
	f.fm = NewFUSEManager(FUSEConfig{MountPoint: f.path})
	f.receipt = &MountReceipt{owner: f.fm, custody: keeper, custodyBoot: "test-boot",
		custodySource: f.source, custodyFSID: f.fsid}
	release := heldFUSETestChild(t, f.receipt)
	f.fm.activeReceipt = f.receipt
	f.present = func() bool { return !f.receipt.custody.Detached() }
	f.fm.observeMount = func(context.Context) (mounttable.Snapshot, error) {
		entries := []mounttable.MountEntry{}
		if f.present() {
			entries = append(entries, mounttable.MountEntry{Path: f.path, Type: "macfuse", Source: f.source, FSID: f.fsid})
		} else {
			release() // detach lets the original JuiceFS child exit on its own
		}
		return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: entries}, nil
	}
	return f
}

func forbidCustodianStart(t *testing.T) *int {
	t.Helper()
	old := startFUSECustodian
	calls := 0
	startFUSECustodian = func(string, string, int, int) (fuseCustodian, error) {
		calls++
		return nil, errors.New("fresh custodian not expected")
	}
	t.Cleanup(func() { startFUSECustodian = old })
	return &calls
}

func TestFUSEStopReportsNeutralAdmissionPendingWithinBoundAndRetainsMount(t *testing.T) {
	keeper := &permissionCustodian{approve: make(chan struct{}), uuid: permissionTestUUID, fsid: [2]int32{7, 9}}
	f := newPermissionFixture(t, keeper)
	starts := forbidCustodianStart(t)
	const bound = 200 * time.Millisecond
	began := time.Now()
	got := f.fm.stopWithBounds(bound, time.Second)
	if elapsed := time.Since(began); elapsed > bound+2*time.Second {
		t.Fatalf("pending Stop exceeded its bound: %v", elapsed)
	}
	if got.State != FUSEAdmissionPending || got.Complete() {
		t.Fatalf("held prompt not reported as admission_pending: %+v", got)
	}
	// A hung daemon parks the same open: no permission-specific claim.
	if !strings.Contains(got.Detail, "unresponsive filesystem") || strings.Contains(got.Detail, "not granted") {
		t.Fatalf("pending admission claimed a permission cause without evidence: %q", got.Detail)
	}
	if keeper.detachCalls != 0 || keeper.releaseCalls != 0 || f.fm.activeReceipt != f.receipt || *starts != 0 {
		t.Fatalf("pending prompt released or replaced custody: keeper=%+v starts=%d", keeper, *starts)
	}
	if waitFUSEChannel(f.receipt.waitDone, 0) {
		t.Fatal("pending Stop ended the original JuiceFS child")
	}
}

// A slow-but-legitimate first admission is never abandoned: approval that
// arrives inside one Stop's bound, or between two Stops, completes retirement
// through the SAME custodian.
func TestFUSEStopCompletesWhenPromptApprovedWithinOrAfterBound(t *testing.T) {
	t.Run("within bound", func(t *testing.T) {
		keeper := &permissionCustodian{approve: make(chan struct{}), uuid: permissionTestUUID, fsid: [2]int32{7, 9}}
		f := newPermissionFixture(t, keeper)
		starts := forbidCustodianStart(t)
		go func() { time.Sleep(50 * time.Millisecond); close(keeper.approve) }()
		if got := f.fm.stopWithBounds(2*time.Second, time.Second); !got.Complete() {
			t.Fatalf("admission inside the bound was abandoned: %+v", got)
		}
		if keeper.detachCalls != 1 || keeper.releaseCalls != 1 || *starts != 0 || f.fm.activeReceipt != nil {
			t.Fatalf("late admission did not retire through the original custodian: %+v starts=%d", keeper, *starts)
		}
	})
	t.Run("after bound", func(t *testing.T) {
		keeper := &permissionCustodian{approve: make(chan struct{}), uuid: permissionTestUUID, fsid: [2]int32{7, 9}}
		f := newPermissionFixture(t, keeper)
		starts := forbidCustodianStart(t)
		if got := f.fm.stopWithBounds(50*time.Millisecond, time.Second); got.State != FUSEAdmissionPending {
			t.Fatalf("first Stop: %+v", got)
		}
		close(keeper.approve)
		if got := f.fm.stopWithBounds(2*time.Second, time.Second); !got.Complete() {
			t.Fatalf("approval after the bound was abandoned: %+v", got)
		}
		if keeper.detachCalls != 1 || keeper.releaseCalls != 1 || *starts != 0 {
			t.Fatalf("retirement used a different custodian: %+v starts=%d", keeper, *starts)
		}
	})
}

func TestFUSEStopReportsPermissionDeniedAndRenewsOnlyJoinedCustodian(t *testing.T) {
	denied := &permissionCustodian{denied: true, joined: true}
	f := newPermissionFixture(t, denied)
	old := startFUSECustodian
	t.Cleanup(func() { startFUSECustodian = old })
	var fresh []*permissionCustodian
	type call struct {
		root, source  string
		child, parent int
	}
	var calls []call
	startFUSECustodian = func(root, source string, child, parent int) (fuseCustodian, error) {
		calls = append(calls, call{root, source, child, parent})
		c := &permissionCustodian{denied: true, joined: true}
		fresh = append(fresh, c)
		return c, nil
	}
	got := f.fm.stopWithBounds(200*time.Millisecond, time.Second)
	if got.State != FUSEPermissionDenied || got.Complete() {
		t.Fatalf("denied access not reported as permission_denied: %+v", got)
	}
	want := call{f.path, f.source, f.receipt.process.Pid, os.Getpid()}
	if len(calls) != 1 || calls[0] != want {
		t.Fatalf("renewal was not one exact-handle start: %+v want %+v", calls, want)
	}
	if denied.detachCalls != 0 || denied.releaseCalls != 0 || f.fm.activeReceipt != f.receipt || waitFUSEChannel(f.receipt.waitDone, 0) {
		t.Fatal("denied Stop detached, released or ended something it does not hold")
	}
	// A still-unjoined denied helper is never replaced: no leak, no second helper.
	f.receipt.custody.(*permissionCustodian).mu.Lock()
	f.receipt.custody.(*permissionCustodian).joined = false
	f.receipt.custody.(*permissionCustodian).mu.Unlock()
	if got := f.fm.stopWithBounds(200*time.Millisecond, time.Second); got.State != FUSEPermissionDenied || len(calls) != 1 {
		t.Fatalf("unjoined denied custodian was replaced: %+v calls=%d", got, len(calls))
	}
}

func TestFUSEStopFinishesAfterLaterGrantViaFreshCustodian(t *testing.T) {
	denied := &permissionCustodian{denied: true, joined: true}
	f := newPermissionFixture(t, denied)
	old := startFUSECustodian
	t.Cleanup(func() { startFUSECustodian = old })
	approved := make(chan struct{})
	close(approved) // the user granted access in System Settings
	var granted *permissionCustodian
	startFUSECustodian = func(string, string, int, int) (fuseCustodian, error) {
		granted = &permissionCustodian{approve: approved, uuid: permissionTestUUID, fsid: [2]int32{7, 9}}
		return granted, nil
	}
	if got := f.fm.stopWithBounds(2*time.Second, time.Second); !got.Complete() {
		t.Fatalf("later grant did not let Stop finish: %+v", got)
	}
	if granted == nil || granted.detachCalls != 1 || granted.releaseCalls != 1 || denied.detachCalls != 0 || f.fm.activeReceipt != nil {
		t.Fatalf("retirement did not go through the fresh exact custodian: granted=%+v denied=%+v", granted, denied)
	}
	if !f.receipt.custodyRecorded || f.receipt.custodyUUID != permissionTestUUID {
		t.Fatal("fresh admission was not bound to the receipt")
	}
}

func TestFUSEDeniedCustodyNeverRenewsForExitedChild(t *testing.T) {
	denied := &permissionCustodian{denied: true, joined: true}
	fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir()})
	exited := make(chan struct{})
	close(exited)
	r := &MountReceipt{owner: fm, custody: denied, waitDone: exited, process: &os.Process{Pid: 99999}}
	starts := forbidCustodianStart(t)
	if err := fm.renewDeniedCustodyLocked(r); err == nil || *starts != 0 || r.custody != denied {
		t.Fatalf("renewed custody for an exited child: %v starts=%d", err, *starts)
	}
}

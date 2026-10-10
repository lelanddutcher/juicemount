package health

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

type testCustodian struct {
	uuid              string
	fsid              [2]int32
	detached          bool
	settled           bool
	detachCalls       int
	releaseCalls      int
	detachErr         error
	neverAdmitted     bool
	readyAttempts     int
	firstReadyPending bool
	pendingDetach     bool
	reconcileCalls    int
}

func (c *testCustodian) WaitReady(time.Duration) error {
	c.readyAttempts++
	if c.neverAdmitted {
		return errors.New("admission refused")
	}
	if c.firstReadyPending && c.readyAttempts == 1 {
		return errors.New("admission pending")
	}
	return nil
}
func (c *testCustodian) Identity() (string, bool, [2]int32, bool) { return c.uuid, false, c.fsid, true }
func (c *testCustodian) Detach(time.Duration) error {
	c.detachCalls++
	if c.detachErr != nil {
		return c.detachErr
	}
	c.detached = true
	return nil
}
func (c *testCustodian) Release(time.Duration) error { c.releaseCalls++; c.settled = true; return nil }
func (c *testCustodian) Detached() bool              { return c.detached }
func (c *testCustodian) PendingDetach() bool         { return c.pendingDetach }
func (c *testCustodian) ReconcileDetach(time.Duration) error {
	c.reconcileCalls++
	if !c.pendingDetach {
		return errors.New("no pending detach")
	}
	c.pendingDetach = false
	c.detached = true
	return nil
}
func (c *testCustodian) NeverAdmittedSettled() bool { return c.neverAdmitted }
func (c *testCustodian) Settled() bool              { return c.settled }
func (c *testCustodian) PID() int                   { return 12345 }

// The test owns the only Wait and releases this harmless child through stdin.
// Cleanup always joins it, including when a Stop assertion fails.
func heldFUSETestChild(t *testing.T, receipt *MountReceipt) func() {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command("/bin/sh", "-c", "IFS= read -r line")
	child.Stdin = reader
	if err := child.Start(); err != nil {
		_ = reader.Close()
		_ = writer.Close()
		t.Fatal(err)
	}
	_ = reader.Close()
	receipt.process = child.Process
	_ = startFUSEChildWaiter(child, nil, receipt)
	done := receipt.waitDone
	var once sync.Once
	release := func() {
		once.Do(func() {
			_, _ = writer.Write([]byte("done\n"))
			_ = writer.Close()
		})
	}
	t.Cleanup(func() {
		release()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			// This is exclusively the test's own fixture child, never a FUSE process.
			_ = child.Process.Kill()
			<-done
			t.Error("held test child did not exit after stdin release")
		}
	})
	return release
}

func delayedFUSETestRelease(t *testing.T, afterDetach <-chan struct{}, release func()) {
	t.Helper()
	cancel := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		select {
		case <-afterDetach:
		case <-cancel:
			release()
			return
		}
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-cancel:
		}
		release()
	}()
	t.Cleanup(func() { close(cancel); <-finished })
}

func TestFUSEStopJoinsOriginalChildWithinExistingBoundAfterDetach(t *testing.T) {
	path := t.TempDir()
	const source = "owned-source"
	fsid := [2]int32{7, 9}
	keeper := &testCustodian{uuid: "12345678-1234-1234-1234-123456789012", fsid: fsid}
	fm := NewFUSEManager(FUSEConfig{MountPoint: path})
	receipt := &MountReceipt{owner: fm, custody: keeper,
		custodyBoot: "test-boot", custodySource: source, custodyFSID: fsid, custodyUUID: keeper.uuid}
	release := heldFUSETestChild(t, receipt)
	fm.activeReceipt = receipt
	afterDetach := make(chan struct{})
	delayedFUSETestRelease(t, afterDetach, release)
	observations := 0
	fm.observeMount = func(context.Context) (mounttable.Snapshot, error) {
		observations++
		entries := []mounttable.MountEntry{}
		if observations == 1 {
			entries = []mounttable.MountEntry{{Path: path, Type: "macfuse", Source: source, FSID: fsid}}
		} else if observations == 2 {
			close(afterDetach)
		}
		return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: entries}, nil
	}
	if got := fm.stopWithBounds(time.Second, time.Second); !got.Complete() {
		t.Fatalf("first Stop did not join naturally exiting original child: %+v", got)
	}
	if observations != 3 || keeper.detachCalls != 1 || keeper.releaseCalls != 1 || fm.activeReceipt != nil {
		t.Fatalf("retirement proof incomplete: observations=%d keeper=%+v receipt=%p", observations, keeper, fm.activeReceipt)
	}
}

func TestFUSEStopChildJoinTimeoutRetainsAndRetriesSameReceipt(t *testing.T) {
	path := t.TempDir()
	fsid := [2]int32{7, 9}
	keeper := &testCustodian{uuid: "12345678-1234-1234-1234-123456789012", fsid: fsid}
	fm := NewFUSEManager(FUSEConfig{MountPoint: path})
	receipt := &MountReceipt{owner: fm, custody: keeper,
		custodyBoot: "test-boot", custodySource: "owned-source", custodyFSID: fsid, custodyUUID: keeper.uuid}
	release := heldFUSETestChild(t, receipt)
	done := receipt.waitDone
	fm.activeReceipt = receipt
	fm.observeMount = func(context.Context) (mounttable.Snapshot, error) {
		entries := []mounttable.MountEntry{}
		if !keeper.detached {
			entries = []mounttable.MountEntry{{Path: path, Type: "macfuse", Source: "owned-source", FSID: fsid}}
		}
		return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: entries}, nil
	}
	if got := fm.stopWithBounds(20*time.Millisecond, time.Second); got.State != FUSEInFlight {
		t.Fatalf("held original child was retired: %+v", got)
	}
	select {
	case <-done:
		t.Fatal("test child exited without release")
	default:
	}
	if fm.activeReceipt != receipt || keeper.detachCalls != 1 || keeper.releaseCalls != 0 {
		t.Fatalf("timeout lost exact custody: keeper=%+v receipt=%p", keeper, fm.activeReceipt)
	}
	release()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("test child failed to exit after release")
	}
	if got := fm.stopWithBounds(time.Second, time.Second); !got.Complete() {
		t.Fatalf("same-receipt retry failed: %+v", got)
	}
	if keeper.detachCalls != 1 || keeper.releaseCalls != 1 || fm.activeReceipt != nil {
		t.Fatalf("retry duplicated detach or lost retirement: keeper=%+v receipt=%p", keeper, fm.activeReceipt)
	}
}

func TestFUSEStopRefusesChangedMountAfterOriginalChildJoin(t *testing.T) {
	for _, tc := range []struct {
		name        string
		want        FUSEStopState
		boot        string
		replacement bool
	}{
		{name: "replacement", want: FUSEPresent, boot: "test-boot", replacement: true},
		{name: "boot changed", want: FUSEUnknown, boot: "next-boot"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir()
			fsid := [2]int32{7, 9}
			keeper := &testCustodian{uuid: "12345678-1234-1234-1234-123456789012", fsid: fsid}
			fm := NewFUSEManager(FUSEConfig{MountPoint: path})
			receipt := &MountReceipt{owner: fm, custody: keeper,
				custodyBoot: "test-boot", custodySource: "owned-source", custodyFSID: fsid, custodyUUID: keeper.uuid}
			release := heldFUSETestChild(t, receipt)
			fm.activeReceipt = receipt
			afterDetach := make(chan struct{})
			delayedFUSETestRelease(t, afterDetach, release)
			observations := 0
			fm.observeMount = func(context.Context) (mounttable.Snapshot, error) {
				observations++
				snapshot := mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: []mounttable.MountEntry{}}
				switch observations {
				case 1:
					snapshot.Entries = []mounttable.MountEntry{{Path: path, Type: "macfuse", Source: "owned-source", FSID: fsid}}
				case 2:
					close(afterDetach)
				default:
					snapshot.BootID = tc.boot
					if tc.replacement {
						snapshot.Entries = []mounttable.MountEntry{{Path: path, Type: "macfuse", Source: "foreign", FSID: [2]int32{8, 10}}}
					}
				}
				return snapshot, nil
			}
			if got := fm.stopWithBounds(time.Second, time.Second); got.State != tc.want {
				t.Fatalf("changed mount/boot accepted after original join: %+v", got)
			}
			if observations != 3 || fm.activeReceipt != receipt || keeper.detachCalls != 1 || keeper.releaseCalls != 0 {
				t.Fatalf("changed identity released custody: observations=%d keeper=%+v receipt=%p", observations, keeper, fm.activeReceipt)
			}
		})
	}
}

func TestFUSEStopDetachesOnlyExactRetainedMountAndJoinsCustodian(t *testing.T) {
	path := t.TempDir()
	const source = "owned-source"
	fsid := [2]int32{7, 9}
	keeper := &testCustodian{uuid: "12345678-1234-1234-1234-123456789012", fsid: fsid}
	fm := NewFUSEManager(FUSEConfig{MountPoint: path})
	childDone := make(chan struct{})
	close(childDone)
	receipt := &MountReceipt{owner: fm, waitDone: childDone, custody: keeper,
		custodyBoot: "test-boot", custodySource: source, custodyFSID: fsid, custodyUUID: keeper.uuid}
	fm.activeReceipt = receipt
	fm.observeMount = func(context.Context) (mounttable.Snapshot, error) {
		entries := []mounttable.MountEntry{}
		if !keeper.detached {
			entries = append(entries, mounttable.MountEntry{Path: path, Type: "macfuse", Source: source, FSID: fsid})
		}
		return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: entries}, nil
	}
	if result := fm.stopWithBounds(time.Second, time.Second); !result.Complete() {
		t.Fatalf("exact retained mount did not retire: %+v", result)
	}
	if keeper.detachCalls != 1 || keeper.releaseCalls != 1 || fm.activeReceipt != nil {
		t.Fatalf("detach/release/receipt mismatch: %+v", keeper)
	}
}

func TestFUSEFailedBirthNeverAdmittedHelperCanSettleAndRollbackAgain(t *testing.T) {
	path := t.TempDir()
	fm := NewFUSEManager(FUSEConfig{MountPoint: path})
	fm.observeMount = testEmptyFUSESnapshot
	done := make(chan struct{})
	close(done)
	keeper := &testCustodian{neverAdmitted: true}
	receipt := &MountReceipt{owner: fm, waitDone: done, custody: keeper, custodyBoot: "test-boot"}
	fm.activeReceipt = receipt
	if got := fm.stopWithBounds(time.Second, time.Second); !got.Complete() {
		t.Fatalf("definite failed birth retained: %+v", got)
	}
	if err := receipt.Rollback(); err != nil {
		t.Fatalf("repeated rollback rejected settled failed birth: %v", err)
	}
}

func TestFUSELateAdmissionFreezesOnlyLiveOriginalChild(t *testing.T) {
	keeper := &testCustodian{uuid: "12345678-1234-1234-1234-123456789012", fsid: [2]int32{7, 9}, firstReadyPending: true}
	fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir()})
	done := make(chan struct{})
	r := &MountReceipt{owner: fm, waitDone: done, custody: keeper, custodyFSID: keeper.fsid}
	if err := fm.finishCustodyAdmission(r, time.Millisecond); err == nil || r.custodyUUID != "" {
		t.Fatalf("pending admission frozen: %v", err)
	}
	if err := fm.finishCustodyAdmission(r, time.Second); err != nil || r.custodyUUID != keeper.uuid {
		t.Fatalf("late admission not frozen: %v", err)
	}
	close(done)
	if err := fm.finishCustodyAdmission(r, time.Second); err != nil {
		t.Fatalf("already-bound identity changed after child exit: %v", err)
	}
	other := &testCustodian{uuid: keeper.uuid, fsid: keeper.fsid}
	exited := make(chan struct{})
	close(exited)
	r2 := &MountReceipt{owner: fm, waitDone: exited, custody: other, custodyFSID: other.fsid}
	if err := fm.finishCustodyAdmission(r2, time.Second); err == nil || other.readyAttempts != 0 || r2.custodyUUID != "" {
		t.Fatalf("exited original authorized admission: %v", err)
	}
}

func TestFUSEAdmittedMountPublishesResolvedBudget(t *testing.T) {
	previous := pin.CacheBudgetBytes()
	defer pin.SetCacheBudgetBytes(previous)
	pin.SetCacheBudgetBytes(1 << 30) // configured pre-mount baseline
	keeper := &testCustodian{uuid: "12345678-1234-1234-1234-123456789012",
		fsid: [2]int32{7, 9}, firstReadyPending: true}
	fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir(), CacheSize: "512"})
	done := make(chan struct{})
	defer close(done)
	r := &MountReceipt{owner: fm, waitDone: done, custody: keeper, custodyFSID: keeper.fsid}
	if err := fm.admitMountBudget(r, time.Millisecond); err == nil {
		t.Fatal("pending custody was admitted")
	}
	if got := pin.CacheBudgetBytes(); got != 1<<30 {
		t.Fatalf("failed custody published budget %d", got)
	}
	if err := fm.admitMountBudget(r, time.Second); err != nil {
		t.Fatalf("settled custody: %v", err)
	}
	if got := pin.CacheBudgetBytes(); got != 512<<20 {
		t.Fatalf("admitted mount budget = %d, want 512MiB", got)
	}
}

func TestFUSEAdmittedUnknownBudgetClearsPreviousBudget(t *testing.T) {
	previous := pin.CacheBudgetBytes()
	defer pin.SetCacheBudgetBytes(previous)
	for _, size := range []string{"", "0", "-1", "invalid", "8796093022208"} {
		t.Run(size, func(t *testing.T) {
			pin.SetCacheBudgetBytes(1 << 30)
			keeper := &testCustodian{uuid: "12345678-1234-1234-1234-123456789012", fsid: [2]int32{7, 9}}
			fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir(), CacheSize: size})
			done := make(chan struct{})
			defer close(done)
			r := &MountReceipt{owner: fm, waitDone: done, custody: keeper, custodyFSID: keeper.fsid}
			if err := fm.admitMountBudget(r, time.Second); err != nil {
				t.Fatal(err)
			}
			if got := pin.CacheBudgetBytes(); got != 0 {
				t.Fatalf("unknown current budget inherited %d", got)
			}
		})
	}
}

func TestFUSEStopReconcilesOnlyPriorDetachWhenPathAlreadyAbsent(t *testing.T) {
	fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir()})
	fm.observeMount = testEmptyFUSESnapshot
	done := make(chan struct{})
	close(done)
	keeper := &testCustodian{uuid: "12345678-1234-1234-1234-123456789012", fsid: [2]int32{7, 9}, pendingDetach: true}
	fm.activeReceipt = &MountReceipt{owner: fm, waitDone: done, custody: keeper, custodyBoot: "test-boot", custodyUUID: keeper.uuid, custodyFSID: keeper.fsid}
	if got := fm.stopWithBounds(time.Second, time.Second); !got.Complete() {
		t.Fatalf("late detach result not reconciled: %+v", got)
	}
	if keeper.detachCalls != 0 || keeper.reconcileCalls != 1 || keeper.releaseCalls != 1 {
		t.Fatalf("duplicate/new detach or no release: %+v", keeper)
	}
}

func TestFUSEStopRetainsCustodyOnForeignReplacementOrDetachFailure(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		detachErr    error
		want         FUSEStopState
	}{
		{"foreign source", "replacement", nil, FUSEPresent},
		{"detach retry", "owned-source", errors.New("busy"), FUSEInFlight},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir()
			fsid := [2]int32{7, 9}
			keeper := &testCustodian{uuid: "12345678-1234-1234-1234-123456789012", fsid: fsid, detachErr: tc.detachErr}
			fm := NewFUSEManager(FUSEConfig{MountPoint: path})
			fm.observeMount = func(context.Context) (mounttable.Snapshot, error) {
				return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: []mounttable.MountEntry{{Path: path, Type: "macfuse", Source: tc.source, FSID: fsid}}}, nil
			}
			childDone := make(chan struct{})
			close(childDone)
			receipt := &MountReceipt{owner: fm, waitDone: childDone, custody: keeper,
				custodyBoot: "test-boot", custodySource: "owned-source", custodyFSID: fsid, custodyUUID: keeper.uuid}
			fm.activeReceipt = receipt
			got := fm.stopWithBounds(time.Second, time.Second)
			if got.State != tc.want || fm.activeReceipt != receipt || keeper.releaseCalls != 0 {
				t.Fatalf("unsafe retirement: %+v, keeper=%+v", got, keeper)
			}
			if tc.source != "owned-source" && keeper.detachCalls != 0 {
				t.Fatal("foreign mount detached")
			}
		})
	}
}

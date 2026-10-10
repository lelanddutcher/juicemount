package health

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

func occupiedFUSESnapshot(path string) mounttable.Snapshot {
	return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: []mounttable.MountEntry{{Path: path, Source: "foreign", Type: "fuse", FSID: [2]int32{4, 5}}}}
}

func TestFUSEStopUnknownAndForeignDescendantNeverSignal(t *testing.T) {
	for _, tc := range []struct {
		name    string
		observe func(context.Context) (mounttable.Snapshot, error)
		want    FUSEStopState
	}{
		{"observer error", func(context.Context) (mounttable.Snapshot, error) {
			return mounttable.Snapshot{}, errors.New("observer failed")
		}, FUSEUnknown},
		{"incomplete", func(context.Context) (mounttable.Snapshot, error) {
			return mounttable.Snapshot{Version: 1, BootID: "test-boot"}, nil
		}, FUSEUnknown},
		{"foreign exact", nil, FUSEPresent},
		{"foreign descendant", nil, FUSEPresent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir()
			fm := NewFUSEManager(FUSEConfig{MountPoint: path})
			if tc.observe != nil {
				fm.observeMount = tc.observe
			} else {
				occupant := path
				if tc.name == "foreign descendant" {
					occupant = filepath.Join(path, "child")
				}
				fm.observeMount = func(context.Context) (mounttable.Snapshot, error) { return occupiedFUSESnapshot(occupant), nil }
			}
			child := exec.Command("sleep", "30")
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			waitDone := make(chan struct{})
			go func() { _ = child.Wait(); close(waitDone) }()
			t.Cleanup(func() { _ = child.Process.Kill(); <-waitDone })
			receipt := &MountReceipt{owner: fm, process: child.Process, waitDone: waitDone}
			fm.activeReceipt = receipt
			result := fm.stopWithBounds(20*time.Millisecond, 20*time.Millisecond)
			if result.State != tc.want || fm.activeReceipt != receipt {
				t.Fatalf("unsafe stop: %+v", result)
			}
			if err := child.Process.Signal(syscall.Signal(0)); err != nil {
				t.Fatalf("foreign/unknown observation signaled child: %v", err)
			}
		})
	}
}

func TestFUSEStopQuiescesCallbackWithoutLifecycleMutex(t *testing.T) {
	fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir()})
	fm.observeMount = testEmptyFUSESnapshot
	entered, release, callbackDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	fm.SetOnRemount(func() { close(entered); <-release })
	go func() { fm.fireOnRemount(); close(callbackDone) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("callback did not enter")
	}
	result := fm.stopWithBounds(20*time.Millisecond, 20*time.Millisecond)
	if result.State != FUSEInFlight {
		t.Fatalf("held callback settled Stop: %+v", result)
	}
	close(release)
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("callback did not finish")
	}
	if result := fm.stopWithBounds(time.Second, time.Second); !result.Complete() {
		t.Fatalf("retry failed: %+v", result)
	}
	var called atomic.Bool
	fm.SetOnRemount(func() { called.Store(true) })
	fm.fireOnRemount()
	if called.Load() {
		t.Fatal("callback admitted after Stop")
	}
}

func TestFUSEStopJoinsSpotlightHelperAndPreservesReadiness(t *testing.T) {
	fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir()})
	fm.observeMount = testEmptyFUSESnapshot
	entered, release := make(chan struct{}), make(chan struct{})
	fm.spotlightWrite = func(string, []byte, os.FileMode) error { close(entered); <-release; return nil }
	fm.scheduleSpotlightIndexing()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("marker helper did not enter")
	}
	if result := fm.stopWithBounds(20*time.Millisecond, 20*time.Millisecond); result.State != FUSEInFlight {
		t.Fatalf("active marker helper lost: %+v", result)
	}
	close(release)
	if result := fm.stopWithBounds(time.Second, time.Second); !result.Complete() {
		t.Fatalf("marker retry failed: %+v", result)
	}
}

func TestFUSEStopRepeatConcurrentAndLateMountAdmission(t *testing.T) {
	fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir(), JuiceFSBin: "/missing"})
	var observations atomic.Int32
	fm.observeMount = func(ctx context.Context) (mounttable.Snapshot, error) {
		observations.Add(1)
		return testEmptyFUSESnapshot(ctx)
	}
	fm.mu.Lock()
	const contenders = 8
	results := make(chan FUSEStopResult, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- fm.stopWithBounds(20*time.Millisecond, 20*time.Millisecond) }()
	}
	wg.Wait()
	close(results)
	for result := range results {
		if result.State != FUSEInFlight {
			t.Fatalf("held mutex admitted stop: %+v", result)
		}
	}
	fm.mu.Unlock()
	if _, err := fm.MountWithReceipt(); err == nil {
		t.Fatal("late mount admitted after Stop")
	}
	if observations.Load() != 0 {
		t.Fatal("late mount reached observer")
	}
	if result := fm.stopWithBounds(time.Second, time.Second); !result.Complete() {
		t.Fatalf("retry failed: %+v", result)
	}
}

func TestFUSEStopRetainsLiveExactChildAndReobservesAfterExit(t *testing.T) {
	fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir()})
	var observations atomic.Int32
	fm.observeMount = func(ctx context.Context) (mounttable.Snapshot, error) {
		observations.Add(1)
		return testEmptyFUSESnapshot(ctx)
	}
	child := exec.Command("sleep", "30")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan struct{})
	go func() { _ = child.Wait(); close(waitDone) }()
	t.Cleanup(func() { _ = child.Process.Kill(); <-waitDone })
	fm.activeReceipt = &MountReceipt{owner: fm, process: child.Process, waitDone: waitDone}
	result := fm.stopWithBounds(time.Second, time.Second)
	if result.State != FUSEInFlight || observations.Load() != 1 || fm.activeReceipt == nil {
		t.Fatalf("live child lost before safe retirement: result=%+v observations=%d", result, observations.Load())
	}
	if err := child.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("Stop signaled child whose TERM could unmount a replacement: %v", err)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("test child did not exit")
	}
	result = fm.stopWithBounds(time.Second, time.Second)
	if !result.Complete() || observations.Load() != 3 || fm.activeReceipt != nil {
		t.Fatalf("exited child not settled with two fresh absence proofs: result=%+v observations=%d", result, observations.Load())
	}
}

func TestFUSEMountAdmissionRejectsPresentAndUnknownBeforeStart(t *testing.T) {
	for _, tc := range []struct {
		name    string
		observe func(context.Context) (mounttable.Snapshot, error)
	}{
		{"present", func(context.Context) (mounttable.Snapshot, error) { return occupiedFUSESnapshot("/fixture/fuse"), nil }},
		{"unknown", func(context.Context) (mounttable.Snapshot, error) {
			return mounttable.Snapshot{}, errors.New("unknown")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "fuse")
			fm := NewFUSEManager(FUSEConfig{MountPoint: path, JuiceFSBin: "/definitely-not-an-executable"})
			fm.observeMount = tc.observe
			if tc.name == "present" {
				fm.observeMount = func(context.Context) (mounttable.Snapshot, error) { return occupiedFUSESnapshot(path), nil }
			}
			if receipt, err := fm.MountWithReceipt(); err == nil || receipt != nil || fm.activeReceipt != nil {
				t.Fatalf("unsafe mount admission: receipt=%v err=%v", receipt, err)
			}
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unsafe admission traversed mountpoint: %v", err)
			}
		})
	}
}

func TestFUSECanReuseRequiresLiveOwnedReceiptAndMatchingConfig(t *testing.T) {
	fm := NewFUSEManager(FUSEConfig{MountPoint: "/fixture/fuse", RedisURL: "redis://fixture"})
	if fm.CanReuse("/fixture/fuse", "redis://fixture") {
		t.Fatal("receiptless manager adopted")
	}
	waitDone := make(chan struct{})
	fm.activeReceipt = &MountReceipt{owner: fm, process: &os.Process{Pid: 12345}, waitDone: waitDone}
	if !fm.CanReuse("/fixture/fuse", "redis://fixture") || fm.CanReuse("/fixture/other", "redis://fixture") || fm.CanReuse("/fixture/fuse", "redis://other") {
		t.Fatal("reuse config/receipt check failed")
	}
	close(waitDone)
	if fm.CanReuse("/fixture/fuse", "redis://fixture") {
		t.Fatal("exited child reused")
	}
}

func TestFUSERemountCallbackQuiesceThenRearmSoftSession(t *testing.T) {
	fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir()})
	entered, release := make(chan struct{}), make(chan struct{})
	fm.SetOnRemount(func() { close(entered); <-release })
	oldDone := make(chan struct{})
	go func() { fm.fireOnRemount(); close(oldDone) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("old callback did not enter")
	}
	if fm.QuiesceRemountCallbacks(20 * time.Millisecond) {
		t.Fatal("active old callback was declared quiescent")
	}
	var fresh atomic.Int32
	fm.SetOnRemount(func() { fresh.Add(1) })
	fm.fireOnRemount()
	if fresh.Load() != 0 {
		t.Fatal("new callback admitted over active old callback")
	}
	close(release)
	select {
	case <-oldDone:
	case <-time.After(time.Second):
		t.Fatal("old callback did not settle")
	}
	if !fm.QuiesceRemountCallbacks(time.Second) {
		t.Fatal("old callback remained active")
	}
	fm.SetOnRemount(func() { fresh.Add(1) })
	fm.fireOnRemount()
	if fresh.Load() != 1 {
		t.Fatal("soft-stop-preserved manager could not rearm callback")
	}
}

func TestFUSEMountAdmissionWaitsForPriorSpotlightHelper(t *testing.T) {
	fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir()})
	fm.observeMount = testEmptyFUSESnapshot
	entered, release := make(chan struct{}), make(chan struct{})
	fm.spotlightWrite = func(string, []byte, os.FileMode) error { close(entered); <-release; return nil }
	fm.scheduleSpotlightIndexing()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("prior helper did not enter")
	}
	fm.mu.Lock()
	err := fm.prepareMountLocked()
	fm.mu.Unlock()
	if err == nil {
		t.Fatal("new mount admitted while prior marker could still write into path")
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for {
		fm.helperMu.Lock()
		active := fm.helperActive
		fm.helperMu.Unlock()
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("prior marker helper did not settle")
		}
		time.Sleep(time.Millisecond)
	}
	fm.mu.Lock()
	err = fm.prepareMountLocked()
	fm.mu.Unlock()
	if err != nil {
		t.Fatalf("settled helper still blocked creation: %v", err)
	}
}

func TestFUSEMonitorStartStopAdmissionRace(t *testing.T) {
	for i := 0; i < 32; i++ {
		fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir()})
		fm.observeMount = testEmptyFUSESnapshot
		started := make(chan struct{})
		startDone := make(chan struct{})
		go func() { defer close(startDone); <-started; fm.StartMonitor() }()
		close(started)
		result := fm.stopWithBounds(time.Second, time.Second)
		select {
		case <-startDone:
		case <-time.After(time.Second):
			t.Fatal("monitor start caller did not settle")
		}
		if !result.Complete() {
			t.Fatalf("iteration %d: raced monitor Start escaped Stop: %+v", i, result)
		}
		if state := fm.monitorState.Load(); state == 1 {
			select {
			case <-fm.done:
			default:
				t.Fatalf("iteration %d: monitor still running after complete Stop", i)
			}
		}
	}
}

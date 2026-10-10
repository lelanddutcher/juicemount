package health

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestMountWithReceiptStartAndReadinessFailures(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	mountPoint := filepath.Join(t.TempDir(), "fuse")
	missing := NewFUSEManager(FUSEConfig{JuiceFSBin: filepath.Join(t.TempDir(), "missing"), MountPoint: mountPoint})
	missing.observeMount = testEmptyFUSESnapshot
	missing.custodyPreflight = func() error { return nil }
	if receipt, err := missing.MountWithReceipt(); !errors.Is(err, os.ErrNotExist) || receipt != nil {
		t.Fatalf("unstarted child has receipt %v, error %v", receipt, err)
	}

	fake := filepath.Join(t.TempDir(), "fake-juicefs")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexit 7\n"), 0700); err != nil {
		t.Fatal(err)
	}
	fm := NewFUSEManager(FUSEConfig{JuiceFSBin: fake, MountPoint: mountPoint})
	fm.observeMount = testEmptyFUSESnapshot
	fm.custodyPreflight = func() error { return nil }
	receipt, err := fm.MountWithReceipt()
	if err == nil || receipt == nil || fm.activeReceipt != receipt || fm.stopping.Load() {
		t.Fatalf("failed launch must retain receipt without stopping manager: receipt=%v error=%v", receipt, err)
	}
	select {
	case <-receipt.waitDone:
	case <-time.After(time.Second):
		t.Fatal("failed child waiter did not settle")
	}
	if err := receipt.Rollback(); err != nil {
		t.Fatalf("failed readiness rollback: %v", err)
	}
	if fm.activeReceipt != nil {
		t.Fatal("complete rollback retained child receipt")
	}
}

func TestCustodyCapabilityRefusalPreventsChildBirth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "child-started")
	bin := filepath.Join(t.TempDir(), "fake-juicefs")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\ntouch '"+path+"'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	fm := NewFUSEManager(FUSEConfig{JuiceFSBin: bin, MountPoint: filepath.Join(t.TempDir(), "mount")})
	fm.observeMount = testEmptyFUSESnapshot
	fm.custodyPreflight = func() error { return errors.New("unsupported") }
	if receipt, err := fm.MountWithReceipt(); err == nil || receipt != nil || fm.activeReceipt != nil {
		t.Fatalf("capability refusal launched child: receipt=%v err=%v", receipt, err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("child start marker: %v", err)
	}
}

func TestMountReceiptRollbackOnlyOwnsItsChild(t *testing.T) {
	owned := exec.Command("sleep", "30")
	if err := owned.Start(); err != nil {
		t.Fatal(err)
	}
	ownedDone := make(chan struct{})
	go func() { _ = owned.Wait(); close(ownedDone) }()
	fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir()})
	fm.observeMount = testEmptyFUSESnapshot
	receipt := &MountReceipt{owner: fm, process: owned.Process, waitDone: ownedDone}
	fm.activeReceipt = receipt
	t.Cleanup(func() { _ = owned.Process.Kill(); <-ownedDone })
	if err := receipt.Rollback(); err == nil {
		t.Fatal("live JuiceFS child falsely settled")
	}
	if fm.activeReceipt != receipt || !fm.stopping.Load() {
		t.Fatal("incomplete rollback lost child or stop admission")
	}
	if err := owned.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("rollback signaled live child: %v", err)
	}
	if err := owned.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ownedDone:
	case <-time.After(time.Second):
		t.Fatal("test child did not exit")
	}
	if err := receipt.Rollback(); err != nil {
		t.Fatalf("already exited/reaped child rollback: %v", err)
	}
	if fm.activeReceipt != nil {
		t.Fatal("settled child receipt retained")
	}

	unrelated := exec.Command("sleep", "30")
	if err := unrelated.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unrelated.Process.Kill(); _ = unrelated.Wait() }()
	if err := receipt.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := unrelated.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatal("rollback stopped unrelated process")
	}
}

func TestMountReceiptStaleRollbackCannotStopSuccessor(t *testing.T) {
	fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir()})
	fm.observeMount = testEmptyFUSESnapshot
	oldDone := make(chan struct{})
	close(oldDone)
	old := &MountReceipt{owner: fm, waitDone: oldDone}
	successor := exec.Command("sleep", "30")
	if err := successor.Start(); err != nil {
		t.Fatal(err)
	}
	successorDone := make(chan struct{})
	go func() { _ = successor.Wait(); close(successorDone) }()
	t.Cleanup(func() { _ = successor.Process.Kill(); <-successorDone })
	fresh := &MountReceipt{owner: fm, process: successor.Process, waitDone: successorDone}
	fm.activeReceipt = fresh
	if err := old.Rollback(); err != nil {
		t.Fatalf("settled old receipt: %v", err)
	}
	if fm.stopping.Load() || fm.activeReceipt != fresh {
		t.Fatal("stale receipt changed successor admission or custody")
	}
	if err := successor.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("stale receipt signaled successor: %v", err)
	}
}

func TestMountReceiptRollbackStopsLogTail(t *testing.T) {
	tailStop := make(chan struct{})
	tailDone := make(chan struct{})
	waitDone := make(chan struct{})
	close(waitDone)
	go func() { <-tailStop; close(tailDone) }()
	fm := NewFUSEManager(FUSEConfig{MountPoint: t.TempDir()})
	fm.observeMount = testEmptyFUSESnapshot
	receipt := &MountReceipt{owner: fm, waitDone: waitDone, tailStop: tailStop, tailDone: tailDone}
	fm.activeReceipt = receipt
	if err := receipt.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-tailDone:
	default:
		t.Fatal("log tail did not stop")
	}
	if err := receipt.Rollback(); err != nil {
		t.Fatal(err)
	}
}

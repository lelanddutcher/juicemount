package health

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

func earlyCustodyMountFixture(t *testing.T, start func(string, string, int, int) (fuseCustodian, error)) (*FUSEManager, *fakeFUSEUnmountd, string) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	logical := generationTestPath(t)
	generation, err := reserveFUSEGeneration(logical, "early-custody", emptyGenerationSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(generation.close)
	marker := filepath.Join(t.TempDir(), "child-exit")
	bin := filepath.Join(t.TempDir(), "fake-juicefs")
	if err := os.WriteFile(bin, []byte(fmt.Sprintf("#!/bin/sh\nwhile [ ! -f %q ]; do sleep 0.05; done\n", marker)), 0700); err != nil {
		t.Fatal(err)
	}
	fm := NewFUSEManager(FUSEConfig{MountPoint: generation.Path, JuiceFSBin: bin, RedisURL: "redis://127.0.0.1:1", CacheSize: "1"})
	fm.generation = generation
	fm.custodyPreflight = func() error { return nil }
	fake := &fakeFUSEUnmountd{}
	fm.unmountd = fake
	retired := false
	fm.observeMount = func(context.Context) (mounttable.Snapshot, error) {
		if r := fm.activeReceipt; !retired && r != nil && r.custody != nil && r.custody.Detached() {
			retired = true // the exact descriptor detach removed this fake mount
			if err := os.WriteFile(marker, nil, 0600); err != nil {
				t.Error(err)
			}
		}
		entries := []mounttable.MountEntry{}
		if fm.activeReceipt != nil && !retired {
			entries = append(entries, mounttable.MountEntry{Path: generation.Path, Type: "macfuse", Source: "JuiceFS:owned", FSID: [2]int32{7, 9}})
		}
		return mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: entries}, nil
	}
	fake.onRetire = func() {
		retired = true
		if err := os.WriteFile(marker, nil, 0600); err != nil {
			t.Error(err)
		}
	}
	old := startFUSECustodian
	startFUSECustodian = start
	t.Cleanup(func() {
		startFUSECustodian = old
		_ = os.WriteFile(marker, nil, 0600)
		if r := fm.activeReceipt; r != nil {
			_ = waitFUSEChannel(r.waitDone, 2*time.Second)
		}
	})
	return fm, fake, marker
}

func TestEarlyCustodyRetainedWhenDataPlaneFails(t *testing.T) {
	keeper := &permissionCustodian{approve: make(chan struct{}), uuid: permissionTestUUID, fsid: [2]int32{7, 9}}
	starts := 0
	fm, fake, _ := earlyCustodyMountFixture(t, func(string, string, int, int) (fuseCustodian, error) {
		starts++
		return keeper, nil
	})
	var spotlightWrites atomic.Int32
	fm.spotlightWrite = func(string, []byte, os.FileMode) error {
		spotlightWrites.Add(1)
		return nil
	}
	probed := false
	fm.dataPlaneProbe = func(path string, _ time.Duration) bool {
		probed = true
		if path != fm.cfg.MountPoint || fm.activeReceipt == nil || fm.activeReceipt.custody != keeper {
			t.Fatal("root probe ran before original custodian was retained")
		}
		if _, err := os.ReadFile(filepath.Join(filepath.Dir(path), observedSidecarName)); err != nil {
			t.Fatalf("observed FSID not durable before root probe: %v", err)
		}
		registers, _ := fake.calls()
		if len(registers) != 1 || registers[0].FSID != [2]int32{7, 9} {
			t.Fatalf("helper registration did not precede root probe: %+v", registers)
		}
		return false
	}
	r, err := fm.MountWithReceipt()
	if err == nil || !strings.Contains(err.Error(), "data plane did not become stable") || !probed || r == nil || r.custody != keeper {
		t.Fatalf("failed readiness lost original custody: receipt=%v err=%v probed=%v", r, err, probed)
	}
	if r.custodyRecorded || r.observerDone != nil || r.tailDone != nil {
		t.Fatal("failed root probe published successful FUSE startup")
	}
	if !fm.quiesceSpotlight(time.Second) {
		t.Fatal("Spotlight completion did not join after failed readiness")
	}
	if spotlightWrites.Load() != 0 {
		t.Fatalf("Spotlight wrote before stable data-plane proof: %d", spotlightWrites.Load())
	}
	if got := fm.stopWithBounds(20*time.Millisecond, time.Second); got.State != FUSEAdmissionPending || fm.activeReceipt != r {
		t.Fatalf("pending original custodian was discarded or declared stopped: %+v", got)
	}
	_, retires := fake.calls()
	if len(retires) != 0 {
		t.Fatalf("pending original session handed to helper before its handoff bound: %+v", retires)
	}
	lease, ok := r.helper.lease.(*fakeFUSELease)
	if !ok {
		t.Fatal("original helper lease missing before descriptor retirement")
	}
	close(keeper.approve)
	if got := fm.stopWithBounds(2*time.Second, time.Second); !got.Complete() {
		t.Fatalf("late original admission did not complete failed-start Stop: %+v", got)
	}
	if starts != 1 || keeper.detachCalls != 1 || keeper.releaseCalls != 1 ||
		!r.custodyRecorded || !fm.generation.retired || fm.activeReceipt != nil || !lease.closed || !waitFUSEChannel(r.waitDone, 0) {
		t.Fatalf("original session/child was not joined after descriptor retirement: starts=%d keeper=%+v", starts, keeper)
	}
}

func TestEarlyCustodyRetainedWhenObservedSidecarFails(t *testing.T) {
	keeper := &permissionCustodian{approve: make(chan struct{}), uuid: permissionTestUUID, fsid: [2]int32{7, 9}}
	fm, fake, _ := earlyCustodyMountFixture(t, func(string, string, int, int) (fuseCustodian, error) { return keeper, nil })
	// The sidecar name is occupied before the child starts, so the normal
	// durable write refuses; the same original session must still be retained.
	if err := os.WriteFile(filepath.Join(filepath.Dir(fm.cfg.MountPoint), observedSidecarName), []byte("occupied"), 0600); err != nil {
		t.Fatal(err)
	}
	fm.dataPlaneProbe = func(string, time.Duration) bool { t.Fatal("probe ran after sidecar refusal"); return false }
	r, err := fm.MountWithReceipt()
	if err == nil || !strings.Contains(err.Error(), "persist observed FUSE identity") || r == nil || r.custody != keeper {
		t.Fatalf("sidecar failure lost descriptor custodian: receipt=%v err=%v", r, err)
	}
	registers, retires := fake.calls()
	if len(registers) != 0 || len(retires) != 0 {
		t.Fatalf("sidecar failure invented helper authority: registers=%+v retires=%+v", registers, retires)
	}
	if got := fm.stopWithBounds(20*time.Millisecond, time.Second); got.State != FUSEAdmissionPending || fm.activeReceipt != r {
		t.Fatalf("pending original custodian was discarded: %+v", got)
	}
}

func TestEarlyJournalAndCustodianFailureRetainsUnresolvedMount(t *testing.T) {
	fm, fake, _ := earlyCustodyMountFixture(t, func(string, string, int, int) (fuseCustodian, error) {
		return nil, errors.New("custodian spawn refused")
	})
	if err := os.WriteFile(filepath.Join(filepath.Dir(fm.cfg.MountPoint), observedSidecarName), []byte("occupied"), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := fm.MountWithReceipt()
	if err == nil || !strings.Contains(err.Error(), "persist observed FUSE identity") ||
		!strings.Contains(err.Error(), "custodian spawn refused") || r == nil || r.custody != nil {
		t.Fatalf("dual failure lost exact retained child or either error: receipt=%v err=%v", r, err)
	}
	if got := fm.stopWithBounds(20*time.Millisecond, time.Second); got.Complete() || fm.activeReceipt != r {
		t.Fatalf("dual failure reported retirement without authority: %+v", got)
	}
	registers, retires := fake.calls()
	if len(registers) != 0 || len(retires) != 0 {
		t.Fatalf("dual failure invented helper authority: registers=%+v retires=%+v", registers, retires)
	}
}

func TestEarlyCustodianStartFailureUsesExactRegisteredHelper(t *testing.T) {
	fm, fake, _ := earlyCustodyMountFixture(t, func(string, string, int, int) (fuseCustodian, error) {
		return nil, errors.New("custodian spawn refused")
	})
	fm.dataPlaneProbe = func(string, time.Duration) bool { t.Fatal("probe ran after custodian start failed"); return false }
	r, err := fm.MountWithReceipt()
	if err == nil || !strings.Contains(err.Error(), "custodian spawn refused") || r == nil || r.custody != nil || r.helper == nil {
		t.Fatalf("failed start did not retain helper receipt: receipt=%v err=%v", r, err)
	}
	if got := fm.stopWithBounds(2*time.Second, time.Second); !got.Complete() {
		t.Fatalf("registered helper did not finish exact failed start: %+v", got)
	}
	registers, retires := fake.calls()
	if len(registers) != 1 || len(retires) != 1 || retires[0].op != registers[0].OpID ||
		retires[0].rr.EjectNow || retires[0].rr.Liveness != client.LivenessAlive || fm.activeReceipt != nil || !fm.generation.retired {
		t.Fatalf("helper-only retirement was not exact and ordinary: registers=%+v retires=%+v", registers, retires)
	}
}

func TestEarlyHelperPendingDoesNotReportRetirement(t *testing.T) {
	fm, fake, _ := earlyCustodyMountFixture(t, func(string, string, int, int) (fuseCustodian, error) {
		return nil, errors.New("custodian spawn refused")
	})
	fake.retireErr = client.ErrPending
	r, err := fm.MountWithReceipt()
	if err == nil || r == nil || r.helper == nil {
		t.Fatalf("failed start did not retain helper receipt: %v", err)
	}
	if got := fm.stopWithBounds(50*time.Millisecond, time.Second); got.Complete() || fm.activeReceipt != r || fm.generation.retired {
		t.Fatalf("pending helper retirement reported complete: %+v", got)
	}
	_, retires := fake.calls()
	if len(retires) != 1 || retires[0].rr.EjectNow {
		t.Fatalf("pending helper route was not one ordinary attempt: %+v", retires)
	}
}

func TestEarlyHelperStopRefusesMissingOrChangedAuthority(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*FUSEManager, *MountReceipt, *mounttable.Snapshot)
	}{
		{"missing receipt", func(fm *FUSEManager, _ *MountReceipt, _ *mounttable.Snapshot) { fm.activeReceipt = nil }},
		{"missing helper", func(_ *FUSEManager, r *MountReceipt, _ *mounttable.Snapshot) { r.helper = nil }},
		{"missing lease", func(_ *FUSEManager, r *MountReceipt, _ *mounttable.Snapshot) { r.helper.lease = nil }},
		{"lost lease", func(_ *FUSEManager, r *MountReceipt, _ *mounttable.Snapshot) { close(r.helper.lease.(*fakeFUSELease).lost) }},
		{"wrong helper op", func(_ *FUSEManager, r *MountReceipt, _ *mounttable.Snapshot) { r.helper.op = "fuse-foreign" }},
		{"wrong receipt owner", func(_ *FUSEManager, r *MountReceipt, _ *mounttable.Snapshot) { r.owner = nil }},
		{"missing child process", func(_ *FUSEManager, r *MountReceipt, _ *mounttable.Snapshot) { r.process = nil }},
		{"missing child waiter", func(_ *FUSEManager, r *MountReceipt, _ *mounttable.Snapshot) { r.waitDone = nil }},
		{"wrong generation", func(_ *FUSEManager, r *MountReceipt, _ *mounttable.Snapshot) { r.generation = &GenerationReservation{} }},
		{"changed FSID", func(_ *FUSEManager, _ *MountReceipt, s *mounttable.Snapshot) { s.Entries[0].FSID = [2]int32{8, 10} }},
		{"changed source", func(_ *FUSEManager, _ *MountReceipt, s *mounttable.Snapshot) { s.Entries[0].Source = "foreign" }},
		{"changed boot", func(_ *FUSEManager, _ *MountReceipt, s *mounttable.Snapshot) { s.BootID = "other" }},
		{"foreign descendant", func(_ *FUSEManager, _ *MountReceipt, s *mounttable.Snapshot) {
			s.Entries = append(s.Entries, mounttable.MountEntry{Path: s.Entries[0].Path + "/foreign", Type: "nfs", Source: "foreign", FSID: [2]int32{8, 10}})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(generationTestPath(t)+".generations", helperTestNonce, "0000000000000001", "mount")
			fm := NewFUSEManager(FUSEConfig{MountPoint: path})
			fm.generation = &GenerationReservation{Path: path, bootID: "test-boot"}
			fake := &fakeFUSEUnmountd{}
			fm.unmountd = fake
			op := FUSEHelperOpID(path)
			r := &MountReceipt{owner: fm, generation: fm.generation, process: &os.Process{Pid: 82918}, waitDone: make(chan struct{}),
				readinessBoot: "test-boot", readinessSource: "JuiceFS:owned", readinessFSID: [2]int32{7, 9},
				custodyBoot: "test-boot", custodySource: "JuiceFS:owned", custodyFSID: [2]int32{7, 9},
				helper: &fuseHelperReceipt{op: op, state: FUSEHelperRegistered, lease: &fakeFUSELease{op: op, lost: make(chan struct{})}}}
			fm.activeReceipt = r
			snapshot := &mounttable.Snapshot{Version: 1, BootID: "test-boot", Entries: []mounttable.MountEntry{
				{Path: path, Type: "macfuse", Source: "JuiceFS:owned", FSID: [2]int32{7, 9}},
			}}
			fm.observeMount = func(context.Context) (mounttable.Snapshot, error) { return *snapshot, nil }
			tc.change(fm, r, snapshot)
			if got := fm.stopWithBounds(20*time.Millisecond, time.Second); got.Complete() {
				t.Fatalf("unproven helper-only Stop completed: %+v", got)
			}
			_, retires := fake.calls()
			if len(retires) != 0 {
				t.Fatalf("unproven helper-only Stop dispatched retirement: %+v", retires)
			}
		})
	}
}

func TestEarlyHelperRetiredUnknownThenChangedBootStaysRetained(t *testing.T) {
	path := filepath.Join(generationTestPath(t)+".generations", helperTestNonce, "0000000000000001", "mount")
	fm := NewFUSEManager(FUSEConfig{MountPoint: path})
	fm.generation = &GenerationReservation{Path: path, bootID: "test-boot"}
	op := FUSEHelperOpID(path)
	r := &MountReceipt{owner: fm, generation: fm.generation, process: &os.Process{Pid: 82918}, waitDone: make(chan struct{}),
		readinessBoot: "test-boot", readinessSource: "JuiceFS:owned", readinessFSID: [2]int32{7, 9},
		custodyBoot: "test-boot", custodySource: "JuiceFS:owned", custodyFSID: [2]int32{7, 9},
		helper: &fuseHelperReceipt{op: op, state: FUSEHelperRegistered, lease: &fakeFUSELease{op: op, lost: make(chan struct{})}}}
	fm.activeReceipt = r
	fake := &fakeFUSEUnmountd{}
	fm.unmountd = fake
	retired, observations := false, 0
	fake.onRetire = func() { retired = true }
	fm.observeMount = func(context.Context) (mounttable.Snapshot, error) {
		observations++
		if observations == 2 {
			return mounttable.Snapshot{}, errors.New("post-retirement snapshot unavailable")
		}
		boot := "test-boot"
		if observations > 2 {
			boot = "changed-boot"
		}
		entries := []mounttable.MountEntry{}
		if !retired {
			entries = append(entries, mounttable.MountEntry{Path: path, Type: "macfuse", Source: "JuiceFS:owned", FSID: [2]int32{7, 9}})
		}
		return mounttable.Snapshot{Version: 1, BootID: boot, Entries: entries}, nil
	}
	if got := fm.stopWithBounds(20*time.Millisecond, time.Second); got.State != FUSEUnknown || !r.helper.proven() {
		t.Fatalf("unavailable post-retirement proof was treated as terminal: %+v", got)
	}
	if got := fm.stopWithBounds(20*time.Millisecond, time.Second); got.State != FUSEUnknown || fm.activeReceipt != r || fm.generation.retired {
		t.Fatalf("changed boot completed retained helper-only Stop: %+v", got)
	}
	_, retires := fake.calls()
	if len(retires) != 1 {
		t.Fatalf("uncertain retry dispatched another retirement: %+v", retires)
	}
}

package health

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

func ownedReadinessFixture(t *testing.T) (*FUSEManager, *MountReceipt, *mounttable.Snapshot) {
	t.Helper()
	// ps truncates a command title before this generation path on macOS. The
	// retained child handle and kernel snapshot must still admit readiness.
	path := filepath.Join(t.TempDir(), strings.Repeat("a", 110), strings.Repeat("b", 110),
		"fuse-internal.generations", "00112233445566778899aabbccddeeff", "0000000000000001", "mount")
	if len(path) <= 256 {
		t.Fatalf("test path too short: %d", len(path))
	}
	fm := NewFUSEManager(FUSEConfig{MountPoint: path})
	generation := &GenerationReservation{Path: path, bootID: "owned-boot"}
	fm.generation = generation
	r := &MountReceipt{owner: fm, generation: generation, process: &os.Process{Pid: 82918}, waitDone: make(chan struct{})}
	fm.activeReceipt = r
	snapshot := &mounttable.Snapshot{Version: 1, BootID: "owned-boot", Entries: []mounttable.MountEntry{
		{Path: path, Type: "macfuse", Source: "JuiceFS:owned", FSID: [2]int32{7, 9}},
	}}
	fm.observeMount = func(context.Context) (mounttable.Snapshot, error) { return *snapshot, nil }
	return fm, r, snapshot
}

func TestOwnedReadinessLongPathUsesRetainedChildAndSnapshot(t *testing.T) {
	fm, r, snapshot := ownedReadinessFixture(t)
	if err := fm.waitForMountProcess(100*time.Millisecond, nil); err != nil {
		t.Fatalf("exact owned mount should be ready despite truncated ps title: %v", err)
	}
	if r.readinessBoot != snapshot.BootID || r.readinessSource != snapshot.Entries[0].Source ||
		r.readinessFSID != snapshot.Entries[0].FSID {
		t.Fatalf("initial mount identity not retained: %+v", r)
	}
	if !fm.isMountedSteadyLocked() {
		t.Fatal("steady health lost the directly owned child and unchanged kernel mount")
	}
}

func TestOwnedReadinessRejectsMissingOrExitedChildAndUnknownSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*FUSEManager, *MountReceipt)
	}{
		{"missing receipt", func(fm *FUSEManager, _ *MountReceipt) { fm.activeReceipt = nil }},
		{"foreign receipt owner", func(_ *FUSEManager, r *MountReceipt) { r.owner = NewFUSEManager(FUSEConfig{}) }},
		{"missing process", func(_ *FUSEManager, r *MountReceipt) { r.process = nil }},
		{"missing waiter", func(_ *FUSEManager, r *MountReceipt) { r.waitDone = nil }},
		{"exited child", func(_ *FUSEManager, r *MountReceipt) {
			exited := make(chan struct{})
			close(exited)
			r.waitDone = exited
		}},
		{"wrong generation", func(_ *FUSEManager, r *MountReceipt) { r.generation = &GenerationReservation{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fm, r, _ := ownedReadinessFixture(t)
			tc.change(fm, r)
			if fm.isMountedSteadyLocked() {
				t.Fatal("unowned or exited child admitted as healthy")
			}
		})
	}
	for _, tc := range []struct {
		name string
		obs  func(context.Context) (mounttable.Snapshot, error)
	}{
		{"observer error", func(context.Context) (mounttable.Snapshot, error) {
			return mounttable.Snapshot{}, errors.New("unavailable")
		}},
		{"incomplete snapshot", func(context.Context) (mounttable.Snapshot, error) {
			return mounttable.Snapshot{Version: 1, BootID: "owned-boot"}, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fm, _, _ := ownedReadinessFixture(t)
			fm.observeMount = tc.obs
			if fm.isMountedSteadyLocked() {
				t.Fatal("unknown kernel mount admitted as healthy")
			}
		})
	}
}

func TestOwnedReadinessRejectsWrongMountIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*FUSEManager, *mounttable.Snapshot)
	}{
		{"wrong boot", func(_ *FUSEManager, s *mounttable.Snapshot) { s.BootID = "other-boot" }},
		{"missing root", func(_ *FUSEManager, s *mounttable.Snapshot) { s.Entries = []mounttable.MountEntry{} }},
		{"case-changed root", func(_ *FUSEManager, s *mounttable.Snapshot) { s.Entries[0].Path = strings.ToUpper(s.Entries[0].Path) }},
		{"wrong type", func(_ *FUSEManager, s *mounttable.Snapshot) { s.Entries[0].Type = "nfs" }},
		{"zero fsid", func(_ *FUSEManager, s *mounttable.Snapshot) { s.Entries[0].FSID = [2]int32{} }},
		{"duplicate root", func(_ *FUSEManager, s *mounttable.Snapshot) { s.Entries = append(s.Entries, s.Entries[0]) }},
		{"mounted descendant", func(_ *FUSEManager, s *mounttable.Snapshot) {
			s.Entries = append(s.Entries, mounttable.MountEntry{Path: s.Entries[0].Path + "/foreign", Type: "nfs", Source: "foreign", FSID: [2]int32{8, 10}})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fm, r, snapshot := ownedReadinessFixture(t)
			tc.change(fm, snapshot)
			if fm.isMountedSteadyLocked() || r.readinessBoot != "" {
				t.Fatal("invalid first kernel observation established readiness")
			}
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*mounttable.Snapshot)
	}{
		{"replaced source", func(s *mounttable.Snapshot) { s.Entries[0].Source = "JuiceFS:foreign" }},
		{"replaced fsid", func(s *mounttable.Snapshot) { s.Entries[0].FSID = [2]int32{11, 12} }},
		{"new boot", func(s *mounttable.Snapshot) { s.BootID = "new-boot" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fm, _, snapshot := ownedReadinessFixture(t)
			if !fm.isMountedSteadyLocked() {
				t.Fatal("baseline owned mount did not establish")
			}
			tc.change(snapshot)
			if fm.isMountedSteadyLocked() {
				t.Fatal("changed kernel mount retained healthy status")
			}
		})
	}
}

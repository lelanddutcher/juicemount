//go:build !serviceclient

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

// Power loss or a reboot while serving leaves desired=running and an owner
// that ended without retirement. The service-start recovery worker used to
// hold at "held-original-owner-unavailable"; it now starts with a successor.
func TestRecoveryWorkerReplacesAbandonedOwner(t *testing.T) {
	_, owner, revision := confirmedServingFixture(t)
	oldObserve, oldInspect, oldStart, oldAbandoned := observePriorServingNFS, inspectRecoveryOwner, startRecoveryEngine, selectedOwnerAbandoned
	t.Cleanup(func() {
		observePriorServingNFS, inspectRecoveryOwner, startRecoveryEngine, selectedOwnerAbandoned = oldObserve, oldInspect, oldStart, oldAbandoned
	})
	observePriorServingNFS = func(context.Context, servingDesiredRecord) (bool, error) { return true, nil }
	inspectRecoveryOwner = func(ServerConfig) (volumeOwnerIntent, error) {
		return volumeOwnerIntent{}, errors.New("selected original owner unavailable; no replacement launch")
	}
	for _, abandoned := range []bool{false, true} {
		starts := 0
		selectedOwnerAbandoned = func(got volumeOwnerIntent) bool {
			if got != owner {
				t.Error("abandonment checked for another owner")
			}
			return abandoned
		}
		startRecoveryEngine = func(got servingDesiredRecord) error {
			starts++
			if got.Revision != revision || got.Desired != "recovering" || got.Owner != owner {
				t.Error("recovery admission changed identity")
			}
			return nil
		}
		resumeConfirmedServingOnce(context.Background())
		if !abandoned {
			if starts != 0 || recoveryStatus() != "held-original-owner-unavailable" {
				t.Fatalf("live-but-silent owner was replaced: starts=%d status=%s", starts, recoveryStatus())
			}
			continue
		}
		if starts != 1 || recoveryStatus() != "resumed" {
			t.Fatalf("abandoned owner held recovery: starts=%d status=%s", starts, recoveryStatus())
		}
	}
}

// After a reboot the exact NFS operation of the previous boot cannot be
// marked Retired, but kernel mounts do not survive a reboot. Recovery accepts
// it when nothing is mounted at the path or source now; shutdown and legacy
// upgrade keep the strict same-boot rule.
func TestRecoveryAcceptsPriorBootNFSOperation(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "mount-transactions")
	mountPoint := filepath.Join(root, "volume")
	for _, path := range []string{directory, mountPoint} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := &mounttable.Snapshot{Version: 1, BootID: "boot-before-power-loss", Entries: []mounttable.MountEntry{}}
	spec := mountbroker.Spec{MountPath: mountPoint, Source: "127.0.0.1:/jm-0123456789abcdef0123456789abcdef", Endpoint: "127.0.0.1:11049", ExportPath: "/jm-0123456789abcdef0123456789abcdef", MetadataPath: filepath.Join(root, "metadata.db")}
	creator := mountbroker.Creator{Directory: directory, Observe: func(context.Context) (mounttable.Snapshot, error) { return *snapshot, nil }}
	result := creator.CreateBound(context.Background(), spec, func(mountbroker.Record) error {
		snapshot.Entries = []mounttable.MountEntry{{Path: mountPoint, Source: spec.Source, Type: "nfs", FSID: [2]int32{21, 22}}}
		return nil
	})
	if err := result.Failure(); err != nil {
		t.Fatal(err)
	}
	desired := servingDesiredRecord{Config: ServerConfig{MountPoint: mountPoint}, NFSPhase: "bound", NFSRecord: result.Record}
	oldObserve := observeOwnedNFS
	t.Cleanup(func() { observeOwnedNFS = oldObserve })
	observeOwnedNFS = func(context.Context) (mounttable.Snapshot, error) { return *snapshot, nil }

	// Same boot, still mounted: not retired on either path.
	if ok, err := priorServingNFSRetiredAcrossBoots(context.Background(), desired, directory, true); ok || err != nil {
		t.Fatalf("live same-boot mount admitted: %v %v", ok, err)
	}
	// Reboot: new boot ID, the kernel mount table is empty.
	snapshot.BootID, snapshot.Entries = "boot-after-power-loss", []mounttable.MountEntry{}
	if ok, err := priorServingNFSRetiredAcrossBoots(context.Background(), desired, directory, true); !ok || err != nil {
		t.Fatalf("prior-boot operation held recovery: %v %v", ok, err)
	}
	if ok, _ := priorServingNFSRetiredInDirectory(context.Background(), desired, directory); ok {
		t.Fatal("strict shutdown path relaxed")
	}
	// Something else now sits at the path or exports the same source: hold.
	// The prior boot's FSID is ignored; kernels reuse FSIDs after a reboot.
	for _, entry := range []mounttable.MountEntry{
		{Path: mountPoint, Source: "foreign:/", Type: "nfs", FSID: [2]int32{1, 2}},
		{Path: filepath.Join(root, "elsewhere"), Source: spec.Source, Type: "nfs", FSID: [2]int32{1, 2}},
	} {
		snapshot.Entries = []mounttable.MountEntry{entry}
		if ok, err := priorServingNFSRetiredAcrossBoots(context.Background(), desired, directory, true); ok || err != nil {
			t.Fatalf("occupied path or source admitted: %+v %v %v", entry, ok, err)
		}
	}
	snapshot.Entries = []mounttable.MountEntry{{Path: filepath.Join(root, "elsewhere"), Source: "other:/", Type: "nfs", FSID: [2]int32{21, 22}}}
	if ok, err := priorServingNFSRetiredAcrossBoots(context.Background(), desired, directory, true); !ok || err != nil {
		t.Fatalf("reused prior-boot FSID held recovery: %v %v", ok, err)
	}
}

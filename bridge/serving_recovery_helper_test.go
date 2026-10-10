//go:build !serviceclient

package main

import (
	"context"
	"path/filepath"
	"testing"
)

// Service start reclaims the dead owner's helper receipt for the desired
// mount point before it waits for NFS retirement (H-3 L4 orphan sweep).
func TestRecoveryWorkerReclaimsOrphanNFSAtServiceStart(t *testing.T) {
	base := servingDesiredFixture(t)
	cfg := ServerConfig{DBPath: filepath.Join(base, "metadata.db"), MountPoint: filepath.Join(base, "volume")}
	revision, err := beginServingDesired(cfg)
	if err != nil {
		t.Fatal(err)
	}
	oldOrphan, oldObserve, oldInspect, oldStart := retireOrphanNFS, observePriorServingNFS, inspectRecoveryOwner, startRecoveryEngine
	t.Cleanup(func() {
		retireOrphanNFS, observePriorServingNFS, inspectRecoveryOwner, startRecoveryEngine = oldOrphan, oldObserve, oldInspect, oldStart
	})
	var swept []string
	observedBeforeSweep := false
	observations := 0
	retireOrphanNFS = func(_ context.Context, mountPoint string) (bool, error) {
		swept = append(swept, mountPoint)
		observedBeforeSweep = observations > 0
		return true, nil
	}
	observePriorServingNFS = func(context.Context, servingDesiredRecord) (bool, error) { observations++; return false, nil }
	inspectRecoveryOwner = func(ServerConfig) (volumeOwnerIntent, error) { return volumeOwnerIntent{}, nil }
	startRecoveryEngine = func(servingDesiredRecord) error { return nil }
	// Not confirmed running: the sweep still runs, nothing else does.
	resumeConfirmedServingOnce(context.Background())
	if len(swept) != 1 || swept[0] != cfg.MountPoint || observedBeforeSweep {
		t.Fatalf("service start did not sweep the orphan first: %v", swept)
	}
	// A pending admission has no Observed row and therefore no receipt.
	if err := markServingNFSPending(revision); err != nil {
		t.Fatal(err)
	}
	resumeConfirmedServingOnce(context.Background())
	if len(swept) != 1 {
		t.Fatalf("pending admission was swept: %v", swept)
	}
}

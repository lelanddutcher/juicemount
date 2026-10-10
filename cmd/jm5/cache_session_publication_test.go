package main

import (
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

func TestCLICacheLateRecoveryCannotClearLossOrStop(t *testing.T) {
	pin.SetAutoOffline(true, "test")
	t.Cleanup(func() { pin.SetAutoOffline(false, "") })
	o := newCLICacheNetworkOwner(nil, nil)
	o.qualificationChanged(true)
	epoch, ready := o.recoveryCandidate()
	if !ready {
		t.Fatal("qualification did not create a recovery candidate")
	}
	// Recovery has completed its remote/FUSE checks but has not published.
	o.qualificationChanged(false)
	if o.clearAutoOfflineIfCurrent(epoch) || !pin.IsAutoOffline() {
		t.Fatal("late recovery cleared a completed qualification loss")
	}
	o.qualificationChanged(true)
	epoch, ready = o.recoveryCandidate()
	if !ready || !o.clearAutoOfflineIfCurrent(epoch) || pin.IsAutoOffline() {
		t.Fatal("fresh qualified recovery did not clear automatic offline")
	}
	// Interface reset uses the same false qualification callback.
	o.qualificationChanged(false)
	if !pin.IsAutoOffline() || o.clearAutoOfflineIfCurrent(epoch) {
		t.Fatal("interface loss allowed the previous recovery")
	}
	o.qualificationChanged(true)
	epoch, _ = o.recoveryCandidate()
	o.closePublication()
	if o.clearAutoOfflineIfCurrent(epoch) || !pin.IsAutoOffline() {
		t.Fatal("late recovery cleared Stop")
	}
	o.qualificationChanged(true)
	if _, ready := o.recoveryCandidate(); ready {
		t.Fatal("late qualification reopened a stopped owner")
	}
}

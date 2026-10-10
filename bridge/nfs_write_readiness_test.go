package main

import "testing"

func TestNFSWriteReadinessIsFailClosedAndGenerationScoped(t *testing.T) {
	first := resetNFSWriteReadiness(true, "waiting")
	if got := nfsWriteReadinessSnapshot(); got.Ready || !got.Required || got.Reason != "waiting" {
		t.Fatalf("initial readiness = %+v, want required and not ready", got)
	}
	publishNFSWriteReadiness(first, true, "verified")
	if got := nfsWriteReadinessSnapshot(); !got.Ready || got.Reason != "verified" || got.CheckedAt == "" {
		t.Fatalf("verified readiness = %+v", got)
	}

	second := resetNFSWriteReadiness(true, "new start")
	publishNFSWriteReadiness(first, true, "stale success")
	if got := nfsWriteReadinessSnapshot(); got.Ready || got.Reason != "new start" {
		t.Fatalf("stale generation changed readiness: %+v", got)
	}
	publishNFSWriteReadiness(second, false, "write failed")
	if got := nfsWriteReadinessSnapshot(); got.Ready || got.Reason != "write failed" {
		t.Fatalf("failure readiness = %+v", got)
	}
}

func TestNFSWriteReadinessNotRequiredForServerOnlyMode(t *testing.T) {
	resetNFSWriteReadiness(false, "ignored")
	if got := nfsWriteReadinessSnapshot(); !got.Ready || got.Required {
		t.Fatalf("server-only readiness = %+v, want ready and not required", got)
	}
}

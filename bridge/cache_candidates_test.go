//go:build redisoptionsacceptance

package main

import (
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

// The bridge buffers a boot hint before handler attachment. Activating the
// handler after the spool-ready boundary must not consume that hint while the
// real Session's auto-offline recovery gate is still closed. This fixture
// supplies a stale hint, so consumption tests delivery, not byte authority;
// the NFS reconcile wire test separately covers accepted content.
func TestBridgeCacheCandidateBootBufferRecoveryDelivery(t *testing.T) {
	raw, rdb, store, rc := bridgeCacheIntegrationFixture(t)
	o := bridgeCacheIntegrationOwner(t, raw, rdb, store, rc, "")
	if !pin.IsAutoOffline() {
		t.Fatal("new Session did not establish boot auto-offline gate")
	}
	o.offerCandidate(metadata.CacheCandidate{Identity: metadata.CachedFileIdentity{Path: "._boot"}})
	if len(o.candidateHints) != 1 {
		t.Fatal("pre-attach boot hint was lost")
	}
	bridgeCacheIntegrationQualify(t, o)
	if !pin.IsAutoOffline() {
		t.Fatal("qualification prematurely cleared auto-offline")
	}
	h := jmnfs.NewHandler(store, t.TempDir())
	t.Cleanup(func() {
		if !h.StopHandler() {
			t.Error("metadata worker did not join")
		}
	})
	paths, err := metadata.OpenCachePathEligibility(store)
	if err != nil {
		t.Fatal(err)
	}
	o.attach(rc, h, paths)
	if len(o.candidateHints) != 1 {
		t.Fatal("handler attachment consumed boot hint before spool-ready activation")
	}
	// Production calls this only after successful spool recovery. The test
	// controls that boundary directly; it does not construct a real spool.
	o.startCandidatePreparation()
	<-time.After(250 * time.Millisecond)
	if len(o.candidateHints) != 1 {
		t.Fatal("auto-offline consumed buffered boot hint")
	}
	if !o.recoverIfSafe(func() bool { return true }, "") || pin.IsAutoOffline() {
		t.Fatal("qualified owner did not clear auto-offline through safe recovery")
	}
	deadline := time.After(3 * time.Second)
	for len(o.candidateHints) != 0 {
		select {
		case <-deadline:
			t.Fatal("original buffered hint was not delivered after safe recovery")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

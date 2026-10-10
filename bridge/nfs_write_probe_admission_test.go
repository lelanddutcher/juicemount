package main

import (
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
)

func armedNFSProbeTestGeneration(t *testing.T) uint64 {
	t.Helper()
	oldArmed := globalNFSWriteVerifyArmed.Load()
	oldActive := globalNFSWriteVerifyGeneration.Load()
	globalNFSWriteVerifyArmed.Store(true)
	generation := resetNFSWriteReadiness(true, "waiting")
	globalNFSWriteVerifyGeneration.Store(generation)
	t.Cleanup(func() {
		globalNFSWriteVerifyArmed.Store(oldArmed)
		globalNFSWriteVerifyGeneration.Store(oldActive)
	})
	return generation
}

func TestNFSWriteProbeDefersWithoutCurrentQualifiedAuthority(t *testing.T) {
	for _, condition := range []string{"user-offline", "auto-offline", "unqualified", "missing-owner", "missing-session", "stopped-session", "invalid-fuse", "missing-fuse-path"} {
		t.Run(condition, func(t *testing.T) {
			generation := armedNFSProbeTestGeneration(t)
			session := &cache.Session{}
			owner := &bridgeCacheOwner{session: session}
			source := nfsWriteProbeSource{owner, session, "/fixture/fuse"}
			offline, ready, fuseOK := false, true, true
			switch condition {
			case "user-offline", "auto-offline":
				offline = true
			case "unqualified", "stopped-session":
				ready = false
			case "missing-owner":
				source.owner = nil
			case "missing-session":
				source.session = nil
			case "invalid-fuse":
				fuseOK = false
			case "missing-fuse-path":
				source.fusePath = ""
			}
			deps := nfsWriteProbeAdmissionDeps{
				current: func() nfsWriteProbeSource { return source },
				offline: func() bool { return offline },
				remote:  func(*cache.Session) (uint64, bool) { return 9, ready },
				fuse:    func(string) bool { return fuseOK },
			}
			probes, waits := 0, 0
			verifyNFSMountReadyLoopWithAdmission(alwaysHealthyNFSRefresher{}, "/fixture/nfs", generation,
				func(string, time.Duration) (bool, int64, string) { probes++; return true, 1, "" }, deps,
				func(int, uint64) bool { waits++; return waits < 3 })
			if probes != 0 || waits != 3 {
				t.Fatalf("unqualified verifier mutated: probes=%d waits=%d", probes, waits)
			}
			state := nfsWriteReadinessSnapshot()
			if !state.Required || state.Ready || !strings.Contains(state.Reason, "deferred") {
				t.Fatalf("deferred write readiness misreported: %+v", state)
			}
		})
	}
}

func TestNFSWriteProbeResumesOnlyAfterRequalification(t *testing.T) {
	generation := armedNFSProbeTestGeneration(t)
	session := &cache.Session{}
	owner := &bridgeCacheOwner{session: session}
	offline, ready := true, false
	deps := nfsWriteProbeAdmissionDeps{
		current: func() nfsWriteProbeSource { return nfsWriteProbeSource{owner, session, "/fixture/fuse"} },
		offline: func() bool { return offline },
		remote:  func(*cache.Session) (uint64, bool) { return 10, ready },
		fuse:    func(string) bool { return true },
	}
	probes, waits := 0, 0
	verifyNFSMountReadyLoopWithAdmission(alwaysHealthyNFSRefresher{}, "/fixture/nfs", generation,
		func(string, time.Duration) (bool, int64, string) { probes++; return true, 1, "" }, deps,
		func(int, uint64) bool {
			waits++
			if waits == 1 {
				offline = false
			} // reachable, but still unqualified
			if waits == 2 {
				ready = true
			} // same session now qualified
			return waits <= 2
		})
	if probes != 1 || waits != 2 {
		t.Fatalf("requalification did not admit exactly one probe: probes=%d waits=%d", probes, waits)
	}
	if state := nfsWriteReadinessSnapshot(); !state.Ready || state.Reason != "verified" {
		t.Fatalf("successful current probe not published: %+v", state)
	}
}

func TestNFSWriteProbeHeldSuccessCannotCrossChangedAuthority(t *testing.T) {
	for _, change := range []string{"user-offline", "auto-offline", "epoch", "owner", "session", "fuse", "generation"} {
		t.Run(change, func(t *testing.T) {
			generation := armedNFSProbeTestGeneration(t)
			session := &cache.Session{}
			owner := &bridgeCacheOwner{session: session}
			source := nfsWriteProbeSource{owner, session, "/fixture/fuse"}
			offline, fuseOK, epoch := false, true, uint64(3)
			deps := nfsWriteProbeAdmissionDeps{
				current: func() nfsWriteProbeSource { return source },
				offline: func() bool { return offline },
				remote:  func(*cache.Session) (uint64, bool) { return epoch, true },
				fuse:    func(string) bool { return fuseOK },
			}
			probes := 0
			verifyNFSMountReadyLoopWithAdmission(alwaysHealthyNFSRefresher{}, "/fixture/nfs", generation,
				func(string, time.Duration) (bool, int64, string) {
					probes++
					switch change {
					case "user-offline", "auto-offline":
						offline = true
					case "epoch":
						epoch++
					case "owner":
						source.owner = &bridgeCacheOwner{session: session}
					case "session":
						source.session = &cache.Session{}
						source.owner = &bridgeCacheOwner{session: source.session}
					case "fuse":
						fuseOK = false
					case "generation":
						resetNFSWriteReadiness(true, "superseded")
					}
					return true, 1, ""
				}, deps, func(int, uint64) bool { return false })
			if probes != 1 {
				t.Fatalf("held probe count=%d, want one", probes)
			}
			if state := nfsWriteReadinessSnapshot(); state.Ready || state.Reason == "verified" {
				t.Fatalf("stale success published after %s: %+v", change, state)
			}
		})
	}
}

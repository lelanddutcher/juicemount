package nfs

import (
	"sync"
	"testing"
	"time"
)

// Measured on a real cellular link 2026-07-29, AND with the mount OFFLINE where
// the warmer had nothing reachable to fetch:
//
//	sidecar_warm  calls=264  timeouts=258 (98%)  mean=791ms
//	sidecar_warm_populated = 2
//
// 264 bounded FUSE opens, 258 spending the full 800ms timeout, to populate two
// entries — repeated indefinitely because nothing in the warmer consulted its
// own outcome. That is the reported "latency spikes hidden to the user ... even
// when offline". A warmer exists to make the next access cheap; when it is
// failing it is burning the resource it means to save.
func newBreakerHandler() *JuiceMountHandler {
	return &JuiceMountHandler{
		sidecarWarmed:  make(map[string]time.Time),
		sidecarWarmSem: make(chan struct{}, 3),
		sidecarWarmMu:  sync.Mutex{},
	}
}

func TestSidecarWarmBreaker_TripsAfterFutilePasses(t *testing.T) {
	t.Setenv("JM_SIDECAR_WARM_BREAKER", "")
	h := newBreakerHandler()

	if !h.sidecarWarmAllowed() {
		t.Fatal("breaker blocked warming before any failure")
	}
	for i := 0; i < sidecarWarmFailStreakMax; i++ {
		h.noteSidecarWarmResult(200, 0) // attempted 200, populated none
	}
	if h.sidecarWarmAllowed() {
		t.Error("breaker still allows warming after consecutive futile passes — " +
			"this is the 800ms-per-read burn that never stops")
	}
}

// Any success must clear it instantly: a link that recovers resumes warming
// without needing a restart.
func TestSidecarWarmBreaker_SuccessResets(t *testing.T) {
	t.Setenv("JM_SIDECAR_WARM_BREAKER", "")
	h := newBreakerHandler()
	for i := 0; i < sidecarWarmFailStreakMax; i++ {
		h.noteSidecarWarmResult(200, 0)
	}
	if h.sidecarWarmAllowed() {
		t.Fatal("precondition: breaker did not trip")
	}
	h.noteSidecarWarmResult(200, 5) // link recovered
	if !h.sidecarWarmAllowed() {
		t.Error("breaker stayed tripped after a successful pass — a recovered link " +
			"must resume warming without a restart")
	}
}

// A pass that attempted nothing carries no information and must not count as a
// failure, or a run of empty directories would trip the breaker spuriously.
func TestSidecarWarmBreaker_EmptyPassIsNotFailure(t *testing.T) {
	t.Setenv("JM_SIDECAR_WARM_BREAKER", "")
	h := newBreakerHandler()
	for i := 0; i < sidecarWarmFailStreakMax*3; i++ {
		h.noteSidecarWarmResult(0, 0) // dirs with no `._` children
	}
	if !h.sidecarWarmAllowed() {
		t.Error("directories with no sidecars tripped the breaker")
	}
}

func TestSidecarWarmBreaker_KillSwitch(t *testing.T) {
	t.Setenv("JM_SIDECAR_WARM_BREAKER", "0")
	h := newBreakerHandler()
	for i := 0; i < sidecarWarmFailStreakMax*2; i++ {
		h.noteSidecarWarmResult(200, 0)
	}
	if !h.sidecarWarmAllowed() {
		t.Error("JM_SIDECAR_WARM_BREAKER=0 did not restore always-warm behavior")
	}
}

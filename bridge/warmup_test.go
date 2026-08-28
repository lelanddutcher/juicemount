package main

import "testing"

func TestWarmupServingStateSelfHealsFromVerifiedNFS(t *testing.T) {
	savedSince := warmupServingSince.Load()
	warmupServingSince.Store(0)
	t.Cleanup(func() { warmupServingSince.Store(savedSince) })

	since, serving := warmupServingState(true, true)
	if !serving || since == 0 {
		t.Fatalf("warmupServingState(running=true, nfsHealthy=true) = (%d, %v), want a repaired serving marker", since, serving)
	}
}

func TestWarmupServingStateDoesNotTrustUnverifiedMount(t *testing.T) {
	savedSince := warmupServingSince.Load()
	warmupServingSince.Store(0)
	t.Cleanup(func() { warmupServingSince.Store(savedSince) })

	if since, serving := warmupServingState(true, false); serving || since != 0 {
		t.Fatalf("warmupServingState(running=true, nfsHealthy=false) = (%d, %v), want not serving", since, serving)
	}
	if since, serving := warmupServingState(false, true); serving || since != 0 {
		t.Fatalf("warmupServingState(running=false, nfsHealthy=true) = (%d, %v), want not serving", since, serving)
	}
}

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

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

func TestWarmupHTTPStartingResponseCannotBeCached(t *testing.T) {
	globalMu.Lock()
	savedRC := globalRC
	savedServer := globalServer
	savedMonitor := globalMonitor
	globalRC = nil
	globalServer = nil
	globalMonitor = nil
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalRC = savedRC
		globalServer = savedServer
		globalMonitor = savedMonitor
		globalMu.Unlock()
	})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/warmup", nil)
	handleWarmupHTTP(recorder, request)

	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store for live startup state", got)
	}
	var response warmupResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode /warmup response: %v", err)
	}
	if response.Phase != "starting" || response.Progress != 5 || response.Serving {
		t.Fatalf("starting response = %+v, want truthful 5%% non-serving state", response)
	}
}

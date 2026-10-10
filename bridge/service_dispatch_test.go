//go:build !serviceclient

package main

import (
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

func TestServingDispatchRefusesInvalidRequestsBeforeEngine(t *testing.T) {
	old := pin.IsOffline()
	defer pin.SetOffline(old)
	pin.SetOffline(false)
	for _, req := range []servicecontrol.Request{
		{Version: 2, Build: version.Commit, Method: "set-offline", Ints: []int{1}},
		{Version: 1, Build: "other-build", Method: "set-offline", Ints: []int{1}},
		{Version: 1, Build: version.Commit, Method: "set-offline", Ints: []int{1, 1}},
		{Version: 1, Build: version.Commit, Method: "start"},
		{Version: 1, Build: version.Commit, Method: "start", Strings: []string{"{}\x00{}"}},
		{Version: 1, Build: version.Commit, Method: "search", Strings: []string{"a", ""}, Ints: []int{1 << 32}},
		{Version: 1, Build: version.Commit, Method: "unknown"},
	} {
		if got := dispatchServingRequest(req); got.Error == "" {
			t.Errorf("invalid request admitted: %+v", req)
		}
		if pin.IsOffline() {
			t.Fatal("rejected request changed engine state")
		}
	}
	got := dispatchServingRequest(servicecontrol.Request{Version: 1, Build: version.Commit, Method: "set-offline", Ints: []int{1}})
	if got.Error != "" || !pin.IsOffline() {
		t.Fatalf("valid operation did not reach engine: %+v", got)
	}
}

func TestServingReadyDoesNotStartStorage(t *testing.T) {
	if globalServer != nil {
		t.Fatal("test requires idle engine")
	}
	got := dispatchServingRequest(servicecontrol.Request{Version: 1, Build: version.Commit, Method: "ready"})
	if got.Int != 1 || got.Error != "" || globalServer != nil {
		t.Fatalf("readiness altered runtime: %+v", got)
	}
	if err := runServingProcess(nil); err == nil {
		t.Fatal("missing explicit service role accepted")
	}
}

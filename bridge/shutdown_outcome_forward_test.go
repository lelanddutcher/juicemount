//go:build serviceclient

package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

// The desktop archive never owns the engine, so its shutdown outcome must be
// read from the serving process, and a lost reply must read as an error, not
// as an empty (and therefore "unknown but fine") outcome.
func TestServingClientForwardsShutdownOutcome(t *testing.T) {
	var calls []servicecontrol.Request
	installControlFake(t, func(ctx context.Context, path string, req servicecontrol.Request) (servicecontrol.Response, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("unbounded GUI call")
		}
		calls = append(calls, req)
		return servicecontrol.Response{Version: 1, Text: `{"complete":false,"reason":"network_volumes_permission_denied"}`}, nil
	})
	req := servicecontrol.Request{Version: 1, Build: version.Commit, Method: "shutdown-outcome"}
	got := dispatchServingRequest(req)
	if len(calls) != 1 || calls[0].Method != "shutdown-outcome" || len(calls[0].Strings) != 0 || len(calls[0].Ints) != 0 {
		t.Fatalf("shutdown outcome not forwarded exactly once: %+v", calls)
	}
	if got.Text != `{"complete":false,"reason":"network_volumes_permission_denied"}` {
		t.Fatalf("forwarded outcome altered: %q", got.Text)
	}

	installControlFake(t, func(context.Context, string, servicecontrol.Request) (servicecontrol.Response, error) {
		return servicecontrol.Response{}, errors.New("socket gone")
	})
	lost := dispatchServingRequest(req)
	if !strings.HasPrefix(lost.Text, "error:") {
		t.Fatalf("lost reply must surface as error text, got %q", lost.Text)
	}
}

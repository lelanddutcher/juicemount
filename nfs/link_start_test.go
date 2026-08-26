package nfs

import (
	"context"
	"net/netip"
	"strings"
	"testing"
	"time"

	"tailscale.com/ipn/ipnstate"
)

func TestWaitForLinkRunningPollsCurrentState(t *testing.T) {
	statuses := []*ipnstate.Status{
		{BackendState: "NoState", Health: []string{"starting"}},
		{BackendState: "Starting", Health: []string{"starting"}},
		{BackendState: "Running", TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.11")}},
	}
	call := 0
	got, err := waitForLinkRunning(context.Background(), func(context.Context) (*ipnstate.Status, error) {
		idx := call
		call++
		if idx >= len(statuses) {
			idx = len(statuses) - 1
		}
		return statuses[idx], nil
	}, time.Microsecond)
	if err != nil {
		t.Fatalf("waitForLinkRunning: %v", err)
	}
	if got.BackendState != "Running" || len(got.TailscaleIPs) != 1 {
		t.Fatalf("unexpected status: %#v", got)
	}
	if call < 3 {
		t.Fatalf("status was not polled through Running: %d calls", call)
	}
}

func TestWaitForLinkRunningRequiresAssignedAddress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	_, err := waitForLinkRunning(ctx, func(context.Context) (*ipnstate.Status, error) {
		return &ipnstate.Status{BackendState: "Running", Health: []string{"address pending"}}, nil
	}, time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout without a tailnet address")
	}
	if !strings.Contains(err.Error(), `last state "Running"`) || !strings.Contains(err.Error(), "address pending") {
		t.Fatalf("error did not preserve actionable state: %v", err)
	}
}

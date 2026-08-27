package nfs

import (
	"context"
	"errors"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tsnet"
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

func TestDeferredReadinessRetriesTransientConfigurationFailure(t *testing.T) {
	node := &LinkNode{srv: &tsnet.Server{}, readyCh: make(chan struct{})}
	t.Cleanup(func() {
		node.mu.Lock()
		cancel := node.readyCancel
		node.srv = nil
		node.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	})

	releaseStatus := make(chan struct{})
	var statusCalls atomic.Int64
	status := func(ctx context.Context) (*ipnstate.Status, error) {
		statusCalls.Add(1)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-releaseStatus:
			return &ipnstate.Status{
				BackendState: "Running",
				TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.22")},
			}, nil
		}
	}
	var configureCalls atomic.Int64
	configure := func(st *ipnstate.Status) error {
		if configureCalls.Add(1) == 1 {
			return errors.New("LocalAPI preferences still starting")
		}
		node.mu.Lock()
		node.addrs = []string{st.TailscaleIPs[0].String()}
		node.mu.Unlock()
		return nil
	}

	node.startDeferredReadinessWith(status, time.Millisecond, configure)
	earlyCtx, earlyCancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer earlyCancel()
	if _, err := node.WaitReady(earlyCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitReady before control plane recovery = %v, want deadline", err)
	}
	close(releaseStatus)
	readyCtx, readyCancel := context.WithTimeout(context.Background(), time.Second)
	defer readyCancel()
	addrs, err := node.WaitReady(readyCtx)
	if err != nil {
		t.Fatalf("WaitReady after recovery: %v", err)
	}
	if len(addrs) != 1 || addrs[0] != "100.64.0.22" {
		t.Fatalf("ready addresses = %v", addrs)
	}
	if got := configureCalls.Load(); got < 2 {
		t.Fatalf("configuration attempts = %d, want retry after transient failure", got)
	}
	if got := statusCalls.Load(); got < 2 {
		t.Fatalf("status attempts = %d, want readiness revalidated before retry", got)
	}
}

func TestDeferredReadinessCancellationReleasesWaiters(t *testing.T) {
	node := &LinkNode{srv: &tsnet.Server{}, readyCh: make(chan struct{})}
	status := func(ctx context.Context) (*ipnstate.Status, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	node.startDeferredReadinessWith(status, time.Millisecond, func(*ipnstate.Status) error {
		return nil
	})

	node.mu.Lock()
	cancel := node.readyCancel
	node.srv = nil
	node.mu.Unlock()
	if cancel == nil {
		t.Fatal("deferred readiness did not install cancellation")
	}
	cancel()

	ctx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if _, err := node.WaitReady(ctx); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitReady after cancellation = %v, want context canceled", err)
	}
}

func TestDeferredReadinessLetsControlClientRecoverWithoutRestart(t *testing.T) {
	node := &LinkNode{srv: &tsnet.Server{}, readyCh: make(chan struct{})}
	t.Cleanup(func() {
		node.mu.Lock()
		cancel := node.readyCancel
		node.srv = nil
		node.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	})

	var statusCalls atomic.Int64
	status := func(context.Context) (*ipnstate.Status, error) {
		// A slow initial map is expected on a cold control plane. The readiness
		// loop must keep observing the existing control client instead of calling
		// LocalBackend.Start, which disconnects it and begins the map request over.
		if statusCalls.Add(1) <= 20 {
			return &ipnstate.Status{BackendState: "NoState", Health: []string{"control fetch failed"}}, nil
		}
		return &ipnstate.Status{
			BackendState: "Running",
			TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.23")},
		}, nil
	}
	configure := func(st *ipnstate.Status) error {
		node.mu.Lock()
		node.addrs = []string{st.TailscaleIPs[0].String()}
		node.mu.Unlock()
		return nil
	}

	node.startDeferredReadinessWith(status, time.Millisecond, configure)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	addrs, err := node.WaitReady(ctx)
	if err != nil {
		t.Fatalf("WaitReady after control-client recovery: %v", err)
	}
	if len(addrs) != 1 || addrs[0] != "100.64.0.23" {
		t.Fatalf("ready addresses = %v", addrs)
	}
	if got := statusCalls.Load(); got < 21 {
		t.Fatalf("status calls = %d, want existing control attempt to be observed through recovery", got)
	}
}

package nfs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"tailscale.com/ipn"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tsnet"
)

type fakeLinkControlClient struct {
	startErr error
	loginErr error
	options  []ipn.Options
	calls    []string
}

func (f *fakeLinkControlClient) Start(_ context.Context, opts ipn.Options) error {
	f.calls = append(f.calls, "start")
	f.options = append(f.options, opts)
	return f.startErr
}

func (f *fakeLinkControlClient) StartLoginInteractive(context.Context) error {
	f.calls = append(f.calls, "login")
	return f.loginErr
}

func TestRecoverLinkNoStateRestartsThenRequestsLogin(t *testing.T) {
	client := &fakeLinkControlClient{}
	if err := recoverLinkNoState(context.Background(), client, "test-auth-key"); err != nil {
		t.Fatalf("recoverLinkNoState: %v", err)
	}
	if got := strings.Join(client.calls, ","); got != "start,login" {
		t.Fatalf("calls = %q, want start,login", got)
	}
	if len(client.options) != 1 || client.options[0].AuthKey != "test-auth-key" {
		t.Fatalf("restart did not receive saved authorization")
	}
	if client.options[0].UpdatePrefs != nil {
		t.Fatalf("restart unexpectedly replaced persisted preferences")
	}
}

func TestRecoverLinkNoStateStopsAfterRestartFailure(t *testing.T) {
	client := &fakeLinkControlClient{startErr: errors.New("restart failed")}
	err := recoverLinkNoState(context.Background(), client, "test-auth-key")
	if err == nil || !strings.Contains(err.Error(), "restart control client") {
		t.Fatalf("recoverLinkNoState error = %v", err)
	}
	if got := strings.Join(client.calls, ","); got != "start" {
		t.Fatalf("calls = %q, want start only", got)
	}
}

func TestLinkTSNetDiagnosticNeverReturnsRawErrors(t *testing.T) {
	sensitiveURL := "https://control.example.invalid/register?auth=do-not-log"
	event, attrs, ok := linkTSNetDiagnostic("[v1] TryLogin: %v", errors.New("request "+sensitiveURL+": context deadline exceeded"))
	if !ok || event != "auth-try-login-error" {
		t.Fatalf("diagnostic = %q %v %v", event, attrs, ok)
	}
	got := fmt.Sprint(attrs)
	if strings.Contains(got, sensitiveURL) || !strings.Contains(got, "timeout") {
		t.Fatalf("diagnostic attributes leaked raw error or lost class: %q", got)
	}
}

func TestLinkTSNetDiagnosticVersionedLoginWrapperNeverReturnsRawErrors(t *testing.T) {
	sensitiveURL := "https://control.example.invalid/key?auth=do-not-log"
	event, attrs, ok := linkTSNetDiagnostic("[v1] %s: %v", "TryLogin", errors.New("GET "+sensitiveURL+": operation not permitted"))
	if !ok || event != "auth-try-login-error" {
		t.Fatalf("diagnostic = %q %v %v", event, attrs, ok)
	}
	got := fmt.Sprint(attrs)
	if strings.Contains(got, sensitiveURL) || !strings.Contains(got, "permission") {
		t.Fatalf("diagnostic attributes leaked raw error or lost class: %q", got)
	}
	if _, _, ok := linkTSNetDiagnostic("[v1] %s: %v", "RegisterReq", errors.New("payload "+sensitiveURL)); ok {
		t.Fatal("unapproved versioned operation unexpectedly passed diagnostic allowlist")
	}
}

func TestLinkDiagnosticErrorKind(t *testing.T) {
	tests := map[string]string{
		"dial tcp: connection reset by peer":     "reset",
		"parse request: missing protocol scheme": "malformed-url",
		"control returned status code 403":       "http-4xx",
		"control returned HTTP 503":              "http-5xx",
	}
	for raw, want := range tests {
		if got := linkDiagnosticErrorKind([]any{errors.New(raw)}); got != want {
			t.Errorf("linkDiagnosticErrorKind(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestPreflightLinkControl(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	var dialed string
	err := preflightLinkControl(context.Background(), "http://192.0.2.10:30193", func(_ context.Context, network, address string) (net.Conn, error) {
		dialed = network + " " + address
		return client, nil
	})
	if err != nil || dialed != "tcp 192.0.2.10:30193" {
		t.Fatalf("preflight = %q, %v", dialed, err)
	}
	if err := preflightLinkControl(context.Background(), "not-a-url", nil); err == nil || !strings.Contains(err.Error(), "http://") {
		t.Fatalf("malformed URL error = %v", err)
	}
}

func TestPreflightLinkControlNamesLocalNetworkRemedy(t *testing.T) {
	dial := func(context.Context, string, string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.EHOSTUNREACH}
	}
	err := preflightLinkControl(context.Background(), "http://192.0.2.10:30193", dial)
	if err == nil || !strings.Contains(err.Error(), "Privacy & Security > Local Network") {
		t.Fatalf("local-network preflight error = %v", err)
	}
}

func TestLinkTSNetDiagnosticAllowlist(t *testing.T) {
	event, attrs, ok := linkTSNetDiagnostic("Authkey is set; but state is %v. Ignoring authkey.", ipn.NoState)
	if !ok || event != "initial-auth-trigger-skipped" || !strings.Contains(fmt.Sprint(attrs), "NoState") {
		t.Fatalf("diagnostic = %q %v %v", event, attrs, ok)
	}
	if _, _, ok := linkTSNetDiagnostic("RegisterRequest: %s", "sensitive payload"); ok {
		t.Fatal("raw registration payload unexpectedly passed diagnostic allowlist")
	}
}

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

func TestWaitForLinkRunningRecoversPersistentNoStateOnce(t *testing.T) {
	var recoveryCalls atomic.Int64
	recovered := false
	status := func(context.Context) (*ipnstate.Status, error) {
		if !recovered {
			return &ipnstate.Status{BackendState: "NoState", Health: []string{"login trigger pending"}}, nil
		}
		return &ipnstate.Status{
			BackendState: "Running",
			TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.24")},
		}, nil
	}
	recover := func(context.Context) error {
		recoveryCalls.Add(1)
		recovered = true
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	st, err := waitForLinkRunningWithRecovery(ctx, status, time.Millisecond, time.Millisecond, recover)
	if err != nil {
		t.Fatalf("waitForLinkRunningWithRecovery: %v", err)
	}
	if st.BackendState != "Running" {
		t.Fatalf("backend state = %q, want Running", st.BackendState)
	}
	if got := recoveryCalls.Load(); got != 1 {
		t.Fatalf("recovery calls = %d, want exactly 1", got)
	}
}

func TestWaitForLinkRunningDoesNotRecoverBriefNoState(t *testing.T) {
	statuses := []*ipnstate.Status{
		{BackendState: "NoState"},
		{BackendState: "Starting"},
		{BackendState: "Running", TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.25")}},
	}
	call := 0
	status := func(context.Context) (*ipnstate.Status, error) {
		st := statuses[call]
		call++
		return st, nil
	}
	var recoveryCalls atomic.Int64
	recover := func(context.Context) error {
		recoveryCalls.Add(1)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := waitForLinkRunningWithRecovery(ctx, status, time.Millisecond, 100*time.Millisecond, recover); err != nil {
		t.Fatalf("waitForLinkRunningWithRecovery: %v", err)
	}
	if got := recoveryCalls.Load(); got != 0 {
		t.Fatalf("recovery calls = %d, want 0 for a normal transition", got)
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

package nfs

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"tailscale.com/tsnet"
)

func TestProbeHTTPUsesHealthPathAndRequiresSuccess(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		if r.URL.Path != "/minio/health/live" {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if _, err := probeHTTP(srv.URL+"/zpool?secret=no", "/minio/health/live", time.Second, (&net.Dialer{}).DialContext); err != nil {
		t.Fatalf("probeHTTP: %v", err)
	}
	if gotPath != "/minio/health/live" {
		t.Fatalf("probe path = %q", gotPath)
	}

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}))
	defer bad.Close()
	if _, err := probeHTTP(bad.URL+"/bucket", "/minio/health/live", time.Second, (&net.Dialer{}).DialContext); err == nil {
		t.Fatal("non-2xx HTTP response was accepted as ready")
	}
}

func TestProxyEndpointTargetPreservesEndpointShape(t *testing.T) {
	tests := []struct {
		raw, defaultPort, wantTarget, wantLoopback string
	}{
		{
			raw:          "redis://:secret@192.168.0.197:30179/1?dial_timeout=2s",
			defaultPort:  "6379",
			wantTarget:   "192.168.0.197:30179",
			wantLoopback: "redis://:secret@127.0.0.1:4567/1?dial_timeout=2s",
		},
		{
			raw:          "http://192.168.0.197:30151/zpool",
			defaultPort:  "80",
			wantTarget:   "192.168.0.197:30151",
			wantLoopback: "http://127.0.0.1:4567/zpool",
		},
		{
			raw:          "192.168.0.197:30179/1",
			defaultPort:  "6379",
			wantTarget:   "192.168.0.197:30179",
			wantLoopback: "127.0.0.1:4567/1",
		},
		{
			raw:          "redis://nas.example/1",
			defaultPort:  "6379",
			wantTarget:   "nas.example:6379",
			wantLoopback: "redis://127.0.0.1:4567/1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			target, rewrite, err := proxyEndpointTarget(tt.raw, tt.defaultPort)
			if err != nil {
				t.Fatalf("proxyEndpointTarget: %v", err)
			}
			if target != tt.wantTarget {
				t.Errorf("target = %q, want %q", target, tt.wantTarget)
			}
			if got := rewrite("127.0.0.1:4567"); got != tt.wantLoopback {
				t.Errorf("rewritten endpoint = %q, want %q", got, tt.wantLoopback)
			}
		})
	}
}

func TestProxyEndpointTargetRejectsTLSIdentityBreakingRewrite(t *testing.T) {
	for _, raw := range []string{
		"https://object.example.test/zpool",
		"rediss://metadata.example.test:6379/1",
	} {
		t.Run(raw, func(t *testing.T) {
			if _, _, err := proxyEndpointTarget(raw, ""); err == nil || !strings.Contains(err.Error(), "not supported") {
				t.Fatalf("proxyEndpointTarget(%q) error = %v, want actionable rejection", raw, err)
			}
		})
	}
}

func TestProxyEndpointReusesStableListener(t *testing.T) {
	node := &LinkNode{srv: &tsnet.Server{}}
	t.Cleanup(func() {
		node.mu.Lock()
		proxies := append([]*tcpProxy(nil), node.proxies...)
		node.srv = nil
		node.proxies = nil
		node.proxyEndpoints = nil
		node.mu.Unlock()
		for _, proxy := range proxies {
			_ = proxy.Close()
		}
	})

	const raw = "redis://nas.example:6379/1"
	first, err := node.ProxyEndpoint(raw, "6379")
	if err != nil {
		t.Fatal(err)
	}
	second, err := node.ProxyEndpoint(raw, "6379")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("proxy endpoint changed across reuse: %q != %q", first, second)
	}
	node.mu.Lock()
	count := len(node.proxies)
	node.mu.Unlock()
	if count != 1 {
		t.Fatalf("proxy listener count = %d, want 1", count)
	}
	adaptive, err := node.AdaptiveProxyEndpoint(raw, "6379")
	if err != nil {
		t.Fatal(err)
	}
	if adaptive == first {
		t.Fatal("adaptive endpoint reused the Link-only diagnostic listener")
	}
	node.mu.Lock()
	count = len(node.proxies)
	node.mu.Unlock()
	if count != 2 {
		t.Fatalf("strict + adaptive proxy listener count = %d, want 2", count)
	}
}

func TestDirectLANEndpointMatchesOnlyFreshPrivatePeerAddress(t *testing.T) {
	direct := LinkTransportStatus{Mode: "direct", Endpoint: "192.168.0.197:41641"}
	for _, tc := range []struct {
		name      string
		address   string
		transport LinkTransportStatus
		want      bool
	}{
		{"matching private IPv4", "192.168.0.197:30179", direct, true},
		{"mismatched private IPv4", "192.168.0.198:30179", direct, false},
		{"hostname fails closed", "nas.example:30179", direct, false},
		{"public target fails closed", "203.0.113.7:30179", LinkTransportStatus{Mode: "direct", Endpoint: "203.0.113.7:41641"}, false},
		{"DERP fails closed", "192.168.0.197:30179", LinkTransportStatus{Mode: "derp", Relay: "mia"}, false},
		{"malformed peer endpoint", "192.168.0.197:30179", LinkTransportStatus{Mode: "direct", Endpoint: "192.168.0.197"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := directLANEndpointMatches(tc.address, tc.transport); got != tc.want {
				t.Fatalf("directLANEndpointMatches(%q, %+v) = %v, want %v", tc.address, tc.transport, got, tc.want)
			}
		})
	}
}

func TestProxyTransportChangeHookFiresOnlyOnTransitions(t *testing.T) {
	node := &LinkNode{}
	var got []string
	node.SetOnProxyTransportChanged(func(mode string) {
		got = append(got, mode)
	})

	node.noteProxyMode(proxyModeDirectLAN, "192.168.0.197:6379")
	node.noteProxyMode(proxyModeDirectLAN, "192.168.0.197:9000")
	node.noteProxyMode(proxyModeEncryptedLink, "192.168.0.197:6379")
	node.noteProxyMode(proxyModeEncryptedLink, "192.168.0.197:9000")

	want := []string{"encrypted-link"}
	if len(got) != len(want) {
		t.Fatalf("transport notifications = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("transport notification %d = %q, want %q", i, got[i], want[i])
		}
	}

	node.SetOnProxyTransportChanged(nil)
	node.noteProxyMode(proxyModeDirectLAN, "192.168.0.197:6379")
	if len(got) != len(want) {
		t.Fatalf("cleared hook received a notification: %v", got)
	}
}

func TestAdaptiveBackendDialSelectsVerifiedLANAndClosesLinkProbe(t *testing.T) {
	linkConn, linkPeer := net.Pipe()
	directConn, directPeer := net.Pipe()
	defer linkPeer.Close()
	defer directPeer.Close()
	got, err := adaptiveBackendDial(context.Background(), "tcp", "192.168.0.197:30151",
		func(context.Context, string, string) (net.Conn, error) { return linkConn, nil },
		func(context.Context) (LinkTransportStatus, error) {
			return LinkTransportStatus{Mode: "direct", Endpoint: "192.168.0.197:41641"}, nil
		},
		func(context.Context, string, string) (net.Conn, error) { return directConn, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	if got.Conn != directConn || got.mode != proxyModeDirectLAN {
		t.Fatalf("selected connection = (%T, %d), want direct LAN", got.Conn, got.mode)
	}
	_ = linkPeer.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := linkPeer.Read(make([]byte, 1)); err == nil {
		t.Fatal("encrypted proof connection remained open after LAN selection")
	}
}

func TestAdaptiveBackendDialKeepsEncryptedConnectionOffLAN(t *testing.T) {
	linkConn, linkPeer := net.Pipe()
	defer linkPeer.Close()
	directCalls := 0
	got, err := adaptiveBackendDial(context.Background(), "tcp", "192.168.0.197:30151",
		func(context.Context, string, string) (net.Conn, error) { return linkConn, nil },
		func(context.Context) (LinkTransportStatus, error) {
			return LinkTransportStatus{Mode: "derp", Relay: "mia"}, nil
		},
		func(context.Context, string, string) (net.Conn, error) {
			directCalls++
			return nil, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	if got.Conn != linkConn || got.mode != proxyModeEncryptedLink || directCalls != 0 {
		t.Fatalf("off-LAN selection = (%T, %d, direct calls %d), want encrypted/no direct attempt", got.Conn, got.mode, directCalls)
	}
}

func TestAdaptiveBackendDialFallsBackToEstablishedLinkWhenLANDialFails(t *testing.T) {
	linkConn, linkPeer := net.Pipe()
	defer linkPeer.Close()
	got, err := adaptiveBackendDial(context.Background(), "tcp", "192.168.0.197:30179",
		func(context.Context, string, string) (net.Conn, error) { return linkConn, nil },
		func(context.Context) (LinkTransportStatus, error) {
			return LinkTransportStatus{Mode: "direct", Endpoint: "192.168.0.197:41641"}, nil
		},
		func(context.Context, string, string) (net.Conn, error) {
			return nil, context.DeadlineExceeded
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	if got.Conn != linkConn || got.mode != proxyModeEncryptedLink {
		t.Fatalf("failed LAN dial selected (%T, %d), want established Link", got.Conn, got.mode)
	}
}

func TestTCPProxyForwardsAndCloses(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	backendDone := make(chan struct{})
	go func() {
		defer close(backendDone)
		conn, err := backend.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(conn, conn)
	}()

	p, err := newTCPProxy(backend.Addr().String(), (&net.Dialer{}).DialContext)
	if err != nil {
		t.Fatal(err)
	}
	proxyAddr := p.listener.Addr().String()
	conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("through-link")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len("through-link"))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "through-link" {
		t.Errorf("reply = %q", buf)
	}
	_ = conn.Close()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if c, err := (&net.Dialer{}).DialContext(ctx, "tcp", proxyAddr); err == nil {
		_ = c.Close()
		t.Fatal("proxy accepted a connection after Close")
	}
	select {
	case <-backendDone:
	case <-time.After(time.Second):
		t.Fatal("backend did not finish")
	}
}

func TestObjectProxyDropsStalledFlowBeforeClientTimeout(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	backendDone := make(chan struct{})
	go func() {
		defer close(backendDone)
		conn, err := backend.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(io.Discard, conn)
	}()

	// Model a userspace connection whose socket deadline methods report
	// success but do not interrupt a blocked Read. The proxy's independent
	// activity watchdog must still close the flow.
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &deadlineIgnoringConn{Conn: conn}, nil
	}
	p, err := newTCPProxyWithIdleTimeout(backend.Addr().String(), dial, 75*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	conn, err := net.DialTimeout("tcp", p.listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("request-with-no-response")); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("stalled backend connection remained open")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("stalled proxy closed after %v, expected well before client timeout", elapsed)
	}
	select {
	case <-backendDone:
	case <-time.After(time.Second):
		t.Fatal("stalled backend connection was not closed")
	}
}

type deadlineIgnoringConn struct{ net.Conn }

func (c *deadlineIgnoringConn) SetDeadline(time.Time) error      { return nil }
func (c *deadlineIgnoringConn) SetReadDeadline(time.Time) error  { return nil }
func (c *deadlineIgnoringConn) SetWriteDeadline(time.Time) error { return nil }

func TestObjectProxyRefreshesDeadlineOnProgress(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	go func() {
		conn, err := backend.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		for _, b := range []byte("alive") {
			_, _ = conn.Write([]byte{b})
			time.Sleep(20 * time.Millisecond)
		}
	}()

	p, err := newTCPProxyWithIdleTimeout(backend.Addr().String(), (&net.Dialer{}).DialContext, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	conn, err := net.DialTimeout("tcp", p.listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	got := make([]byte, len("alive"))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("progressing flow was closed: %v", err)
	}
	if string(got) != "alive" {
		t.Fatalf("progressing flow = %q", got)
	}
}

func TestProxyIdleTimeoutOnlyAppliesToHTTP(t *testing.T) {
	if got := proxyIdleTimeout("http://nas.example/bucket"); got != objectProxyIdleTimeout {
		t.Fatalf("HTTP timeout = %v", got)
	}
	if got := proxyIdleTimeout("https://nas.example/bucket"); got != objectProxyIdleTimeout {
		t.Fatalf("HTTPS timeout = %v", got)
	}
	if got := proxyIdleTimeout("redis://nas.example/1"); got != 0 {
		t.Fatalf("Redis timeout = %v, want zero", got)
	}
}

func TestProxyResponseTimeoutOnlyAppliesToAdaptiveRedis(t *testing.T) {
	if got := proxyResponseTimeout("redis://nas.example/1", "6379", "adaptive"); got != adaptiveRedisResponseTimeout {
		t.Fatalf("adaptive Redis response timeout = %v", got)
	}
	for _, tc := range []struct {
		raw, port, mode string
	}{
		{"redis://nas.example/1", "6379", "encrypted"},
		{"http://nas.example/bucket", "80", "adaptive"},
		{"nas.example:9000", "9000", "adaptive"},
	} {
		if got := proxyResponseTimeout(tc.raw, tc.port, tc.mode); got != 0 {
			t.Fatalf("proxyResponseTimeout(%q, %q, %q) = %v, want zero", tc.raw, tc.port, tc.mode, got)
		}
	}
}

func TestRedisResponseWatchdogPreservesIdleThenClosesStalledRequest(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	accepted := make(chan struct{})
	requestSeen := make(chan struct{})
	go func() {
		conn, err := backend.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		close(accepted)
		buf := make([]byte, 4)
		if _, err := io.ReadFull(conn, buf); err == nil && string(buf) == "PING" {
			close(requestSeen)
		}
		// Deliberately never reply. The response watchdog must close this flow.
		_, _ = io.Copy(io.Discard, conn)
	}()

	p, err := newTCPProxyWithTimeouts(backend.Addr().String(), (&net.Dialer{}).DialContext, 0, 75*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	conn, err := net.DialTimeout("tcp", p.listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("proxy did not establish backend flow")
	}
	// The request-armed watchdog must not churn a healthy idle subscription.
	time.Sleep(150 * time.Millisecond)
	started := time.Now()
	if _, err := conn.Write([]byte("PING")); err != nil {
		t.Fatalf("idle Redis flow was closed before a request: %v", err)
	}
	select {
	case <-requestSeen:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("backend did not receive request after idle period")
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("stalled Redis response remained open")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("stalled Redis response closed after %v, expected under 500ms", elapsed)
	}
}

func TestRedisResponseWatchdogDisarmsAfterReply(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	go func() {
		conn, err := backend.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(conn, conn)
	}()

	p, err := newTCPProxyWithTimeouts(backend.Addr().String(), (&net.Dialer{}).DialContext, 0, 75*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	conn, err := net.DialTimeout("tcp", p.listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for i := 0; i < 2; i++ {
		if _, err := conn.Write([]byte("PONG")); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 4)
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := io.ReadFull(conn, got); err != nil || string(got) != "PONG" {
			t.Fatalf("reply %d = %q, %v", i, got, err)
		}
		// A completed response disarms the watchdog until the next request.
		time.Sleep(150 * time.Millisecond)
	}
}

func TestProxyEndpointTargetRejectsMissingHost(t *testing.T) {
	for _, raw := range []string{"", "://nope", "redis:///1"} {
		t.Run(strings.ReplaceAll(raw, "/", "_"), func(t *testing.T) {
			if _, _, err := proxyEndpointTarget(raw, "6379"); err == nil {
				t.Fatalf("proxyEndpointTarget(%q) succeeded", raw)
			}
		})
	}
}

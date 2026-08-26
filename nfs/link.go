package nfs

// JuiceMount Link client node (Tier-2 T2.2): an embedded Tailscale/tsnet
// endpoint inside the Mac app. When the user pairs (code from the NAS
// manager), cbridge brings this node up BEFORE mounting, so the NFS loopback
// server's backend traffic — JuiceFS→Redis, MinIO bucket override — can
// target the NAS over the tailnet from anywhere.
//
// tsnet uses a userspace network stack. The Go Redis client and the external
// juicefs process use the host network stack, so they cannot dial a tailnet
// route directly. Link therefore exposes short-lived loopback TCP proxies:
// local clients dial 127.0.0.1 and the proxy's outbound leg dials through
// tsnet. The NAS node advertises the existing LAN subnet, so the original
// Redis and S3 endpoint addresses remain the route targets.

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"tailscale.com/ipn"
	"tailscale.com/tsnet"
)

type LinkNode struct {
	srv     *tsnet.Server
	mu      sync.Mutex
	proxies []*tcpProxy
	addrs   []string
}

// contextDialer is deliberately small so proxy behavior can be tested with a
// normal TCP dialer; production passes tsnet.Server.Dial.
type contextDialer func(context.Context, string, string) (net.Conn, error)

// tcpProxy accepts only on loopback and forwards each connection through its
// supplied dialer. It owns active connections so Link shutdown cannot leave
// background proxy goroutines or sockets behind after Stop → Start cycles.
type tcpProxy struct {
	listener net.Listener
	target   string
	dial     contextDialer
	// idleTimeout is used for request/response transports such as object
	// storage. A dead tailnet flow must close before JuiceFS exhausts its own
	// 30-second S3 attempt and aborts the mount. Zero preserves long-lived idle
	// connections such as Redis subscriptions.
	idleTimeout time.Duration

	mu     sync.Mutex
	closed bool
	conns  map[net.Conn]struct{}
}

// StartLinkNode brings up the embedded tailnet node and waits (bounded) for
// registration. Returns the node + this Mac's tailnet addresses.
func StartLinkNode(controlURL, authKey, hostname, stateDir string) (*LinkNode, []string, error) {
	if controlURL == "" || authKey == "" {
		return nil, nil, fmt.Errorf("link: control URL and auth key required")
	}
	if hostname == "" {
		hostname = "juicemount-mac"
	}
	if stateDir == "" {
		stateDir = filepath.Join("/tmp", "jm-tsnet-"+hostname)
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, nil, fmt.Errorf("link: create state directory: %w", err)
	}
	s := &tsnet.Server{
		Hostname:   hostname,
		ControlURL: controlURL,
		AuthKey:    authKey,
		Dir:        stateDir,
		Ephemeral:  false, // silent rejoin on every launch is the product behavior
		Logf:       func(string, ...any) {},
	}
	if err := s.Start(); err != nil {
		return nil, nil, fmt.Errorf("link: start: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	st, err := s.Up(ctx)
	if err != nil {
		s.Close()
		return nil, nil, fmt.Errorf("link: up: %w", err)
	}
	addrs := make([]string, 0, len(st.Self.TailscaleIPs))
	for _, a := range st.Self.TailscaleIPs {
		addrs = append(addrs, a.String())
	}
	node := &LinkNode{srv: s, addrs: append([]string(nil), addrs...)}
	if err := node.setHostname(hostname); err != nil {
		s.Close()
		return nil, nil, err
	}
	node.acceptRoutes()
	return node, addrs, nil
}

func (l *LinkNode) setHostname(hostname string) error {
	lc, err := l.srv.LocalClient()
	if err != nil {
		return fmt.Errorf("link: hostname preferences unavailable: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := lc.EditPrefs(ctx, &ipn.MaskedPrefs{
		Prefs:       ipn.Prefs{Hostname: hostname},
		HostnameSet: true,
	}); err != nil {
		return fmt.Errorf("link: apply hostname %q: %w", hostname, err)
	}
	return nil
}

// Addresses returns the node's tailnet addresses captured at successful Up.
// A copy keeps callers from mutating LinkNode state.
func (l *LinkNode) Addresses() []string {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.addrs...)
}

// DialContext opens a connection through the embedded tailnet. It is exposed
// for protocol-level readiness checks: accepting a TCP connection is not proof
// that Redis will authenticate or that object storage will answer requests.
func (l *LinkNode) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if l == nil {
		return nil, fmt.Errorf("link: node is nil")
	}
	l.mu.Lock()
	s := l.srv
	l.mu.Unlock()
	if s == nil {
		return nil, fmt.Errorf("link: node is stopped")
	}
	return s.Dial(ctx, network, address)
}

// ProxyEndpoint returns an equivalent URL whose host is a loopback listener.
// Connections accepted there are dialed to raw's original host:port through
// the Link node. The URL scheme, credentials, path (including Redis DB), and
// query are unchanged. raw may also be a bare host:port value.
func (l *LinkNode) ProxyEndpoint(raw, defaultPort string) (string, error) {
	target, rewrite, err := proxyEndpointTarget(raw, defaultPort)
	if err != nil {
		return raw, err
	}

	l.mu.Lock()
	s := l.srv
	l.mu.Unlock()
	if s == nil {
		return raw, fmt.Errorf("link: node is stopped")
	}

	p, err := newTCPProxyWithIdleTimeout(target, s.Dial, proxyIdleTimeout(raw))
	if err != nil {
		return raw, err
	}
	l.mu.Lock()
	if l.srv == nil {
		l.mu.Unlock()
		_ = p.Close()
		return raw, fmt.Errorf("link: node is stopped")
	}
	l.proxies = append(l.proxies, p)
	l.mu.Unlock()
	return rewrite(p.listener.Addr().String()), nil
}

// ProbeEndpoint verifies the route from tsnet itself. A successful dial to a
// loopback proxy would only prove that the local listener accepted a socket;
// this direct probe correctly drives the startup online/offline decision.
func (l *LinkNode) ProbeEndpoint(raw, defaultPort string, timeout time.Duration) (time.Duration, error) {
	target, _, err := proxyEndpointTarget(raw, defaultPort)
	if err != nil {
		return 0, err
	}
	l.mu.Lock()
	s := l.srv
	l.mu.Unlock()
	if s == nil {
		return 0, fmt.Errorf("link: node is stopped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	started := time.Now()
	conn, err := s.Dial(ctx, "tcp", target)
	if err != nil {
		return 0, err
	}
	_ = conn.Close()
	return time.Since(started), nil
}

// ProbeHTTP verifies an application-level HTTP endpoint through tsnet. The
// request keeps the original hostname, so HTTPS certificate verification and
// Host routing behave exactly as they will for the real object-store client.
// probePath replaces any bucket path in raw (for MinIO use
// "/minio/health/live"). Only a 2xx response is considered ready.
func (l *LinkNode) ProbeHTTP(raw, probePath string, timeout time.Duration) (time.Duration, error) {
	if l == nil {
		return 0, fmt.Errorf("link: node is nil")
	}
	return probeHTTP(raw, probePath, timeout, l.DialContext)
}

func probeHTTP(raw, probePath string, timeout time.Duration, dial contextDialer) (time.Duration, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") {
		if err == nil {
			err = fmt.Errorf("expected an http or https URL with a host")
		}
		return 0, fmt.Errorf("link: parse HTTP endpoint %q: %w", raw, err)
	}
	u.Path = probePath
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	transport := &http.Transport{
		Proxy:       nil,
		DialContext: dial,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: timeout}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, fmt.Errorf("link: build HTTP probe: %w", err)
	}
	started := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("link: HTTP probe returned %s", resp.Status)
	}
	return time.Since(started), nil
}

// proxyEndpointTarget parses a URL or bare host:port and returns the address
// the tsnet leg should target plus a function that rewrites the original URL
// to a supplied loopback address.
func proxyEndpointTarget(raw, defaultPort string) (string, func(string) string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", nil, fmt.Errorf("link: empty endpoint")
	}
	bare := !strings.Contains(raw, "://")
	parseRaw := raw
	if bare {
		parseRaw = "//" + raw
	}
	u, err := url.Parse(parseRaw)
	if err != nil || u.Hostname() == "" {
		if err == nil {
			err = fmt.Errorf("missing host")
		}
		return "", nil, fmt.Errorf("link: parse endpoint %q: %w", raw, err)
	}
	port := u.Port()
	if port == "" {
		port = defaultPort
		if port == "" {
			switch strings.ToLower(u.Scheme) {
			case "http":
				port = "80"
			case "https":
				port = "443"
			}
		}
	}
	if port == "" {
		return "", nil, fmt.Errorf("link: endpoint %q has no port", raw)
	}
	target := net.JoinHostPort(u.Hostname(), port)
	return target, func(loopback string) string {
		u2 := *u
		u2.Host = loopback
		out := u2.String()
		if bare {
			out = strings.TrimPrefix(out, "//")
		}
		return out
	}, nil
}

func newTCPProxy(target string, dial contextDialer) (*tcpProxy, error) {
	return newTCPProxyWithIdleTimeout(target, dial, 0)
}

const objectProxyIdleTimeout = 10 * time.Second

func proxyIdleTimeout(raw string) time.Duration {
	u, err := url.Parse(raw)
	if err != nil {
		return 0
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		return objectProxyIdleTimeout
	default:
		return 0
	}
}

func newTCPProxyWithIdleTimeout(target string, dial contextDialer, idleTimeout time.Duration) (*tcpProxy, error) {
	if dial == nil {
		return nil, fmt.Errorf("link: proxy dialer is nil")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("link: listen loopback proxy: %w", err)
	}
	p := &tcpProxy{
		listener:    listener,
		target:      target,
		dial:        dial,
		idleTimeout: idleTimeout,
		conns:       make(map[net.Conn]struct{}),
	}
	go p.serve()
	return p, nil
}

func (p *tcpProxy) serve() {
	for {
		local, err := p.listener.Accept()
		if err != nil {
			return
		}
		if !p.track(local) {
			_ = local.Close()
			return
		}
		go p.forward(local)
	}
}

func (p *tcpProxy) forward(local net.Conn) {
	defer p.untrackAndClose(local)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	remote, err := p.dial(ctx, "tcp", p.target)
	cancel()
	if err != nil {
		return
	}
	if !p.track(remote) {
		_ = remote.Close()
		return
	}
	defer p.untrackAndClose(remote)

	remoteReader := io.Reader(remote)
	remoteWriter := io.Writer(remote)
	localWriter := io.Writer(local)
	if p.idleTimeout > 0 {
		// Deadline each individual blocking operation, refreshing on progress.
		// A single absolute deadline would kill a healthy large transfer; these
		// wrappers only close a flow that makes no progress for the full window.
		remoteReader = &readDeadlineConn{Conn: remote, timeout: p.idleTimeout}
		remoteWriter = &writeDeadlineConn{Conn: remote, timeout: p.idleTimeout}
		localWriter = &writeDeadlineConn{Conn: local, timeout: p.idleTimeout}
	}
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(remoteWriter, local); done <- struct{}{} }()
	go func() { _, _ = io.Copy(localWriter, remoteReader); done <- struct{}{} }()
	<-done
	_ = local.Close()
	_ = remote.Close()
	<-done
}

type readDeadlineConn struct {
	net.Conn
	timeout time.Duration
}

func (c *readDeadlineConn) Read(p []byte) (int, error) {
	if err := c.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Read(p)
}

type writeDeadlineConn struct {
	net.Conn
	timeout time.Duration
}

func (c *writeDeadlineConn) Write(p []byte) (int, error) {
	if err := c.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}

func (p *tcpProxy) track(conn net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.conns[conn] = struct{}{}
	return true
}

func (p *tcpProxy) untrackAndClose(conn net.Conn) {
	p.mu.Lock()
	delete(p.conns, conn)
	p.mu.Unlock()
	_ = conn.Close()
}

func (p *tcpProxy) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	conns := make([]net.Conn, 0, len(p.conns))
	for conn := range p.conns {
		conns = append(conns, conn)
	}
	p.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	return p.listener.Close()
}

// acceptRoutes flips RouteAll so this node USES subnet routes advertised by
// peers (the NAS LAN prefix). tsnet runs in userspace networking mode — there
// is no utun interface and no OS route table to populate — so acceptance is a
// backend-prefs operation and routed destinations are only reachable from
// dials made through this node's netstack. Best effort: a failure degrades to
// LAN-only behavior, never blocks the join.
func (l *LinkNode) acceptRoutes() {
	lc, err := l.srv.LocalClient()
	if err != nil {
		log.Printf("[link] accept-routes unavailable: %v", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := lc.EditPrefs(ctx, &ipn.MaskedPrefs{
		Prefs:       ipn.Prefs{RouteAll: true},
		RouteAllSet: true,
	}); err != nil {
		log.Printf("[link] accept-routes failed (LAN-only mode): %v", err)
	}
}

// LinkEndpointOverride is retained for callers that need a pure URL rewrite.
// Deprecated for Link transport: changing a host to a tailnet address does
// not help a normal OS socket reach tsnet's userspace network stack. The
// desktop bridge uses ProxyEndpoint instead.
//
// Scheme, port, userinfo, path and query survive untouched — only the host
// swaps. Empty nasAddr or an unparseable input returns the input unchanged,
// so callers need no special-casing.
func LinkEndpointOverride(nasAddr, redisURL, minioURL string) (string, string) {
	return swapEndpointHost(redisURL, nasAddr), swapEndpointHost(minioURL, nasAddr)
}

// swapEndpointHost replaces the host of raw (a URL or bare host:port) with
// nasAddr, preserving any port. Bare host:port inputs are handled by
// prefixing "//" before parsing (url.Parse would otherwise read the host as
// a scheme).
func swapEndpointHost(raw, nasAddr string) string {
	if raw == "" || nasAddr == "" {
		return raw
	}
	bare := false
	p := raw
	if !strings.Contains(p, "://") {
		p = "//" + p
		bare = true
	}
	u, err := url.Parse(p)
	if err != nil || u.Host == "" {
		return raw
	}
	host := nasAddr
	if _, port, err := net.SplitHostPort(u.Host); err == nil && port != "" {
		host = net.JoinHostPort(nasAddr, port)
	}
	u.Host = host
	out := u.String()
	if bare {
		out = strings.TrimPrefix(out, "//")
	}
	return out
}

// Stop tears the node down at shutdown.
func (l *LinkNode) Stop() {
	if l == nil {
		return
	}
	l.mu.Lock()
	s := l.srv
	proxies := l.proxies
	l.srv = nil
	l.proxies = nil
	l.addrs = nil
	l.mu.Unlock()
	for _, proxy := range proxies {
		_ = proxy.Close()
	}
	if s != nil {
		_ = s.Close()
	}
}

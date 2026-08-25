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
	node := &LinkNode{srv: s}
	node.acceptRoutes()
	return node, addrs, nil
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

	p, err := newTCPProxy(target, s.Dial)
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
	if dial == nil {
		return nil, fmt.Errorf("link: proxy dialer is nil")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("link: listen loopback proxy: %w", err)
	}
	p := &tcpProxy{
		listener: listener,
		target:   target,
		dial:     dial,
		conns:    make(map[net.Conn]struct{}),
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

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(remote, local); done <- struct{}{} }()
	go func() { _, _ = io.Copy(local, remote); done <- struct{}{} }()
	<-done
	_ = local.Close()
	_ = remote.Close()
	<-done
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
	l.mu.Unlock()
	for _, proxy := range proxies {
		_ = proxy.Close()
	}
	if s != nil {
		_ = s.Close()
	}
}

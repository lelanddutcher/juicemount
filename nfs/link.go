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
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"

	"tailscale.com/ipn"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/net/tsaddr"
	"tailscale.com/tsnet"
)

type LinkNode struct {
	srv             *tsnet.Server
	mu              sync.Mutex
	hostname        string
	proxies         []*tcpProxy
	proxyEndpoints  map[string]string
	addrs           []string
	readyCh         chan struct{}
	readyOnce       sync.Once
	readyErr        error
	readyCancel     context.CancelFunc
	readyState      string
	readyHealth     []string
	readyAttemptErr error
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

// newLinkNode starts tsnet without waiting for the control plane or subnet
// route. status is separated so synchronous Apply/Test and deferred app startup
// can share one constructor while choosing their own readiness lifecycle.
func newLinkNode(controlURL, authKey, hostname, stateDir string) (*LinkNode, linkStatusFunc, error) {
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
	lc, err := s.LocalClient()
	if err != nil {
		s.Close()
		return nil, nil, fmt.Errorf("link: local client: %w", err)
	}
	node := &LinkNode{
		srv:      s,
		hostname: hostname,
		readyCh:  make(chan struct{}),
	}
	return node, lc.StatusWithoutPeers, nil
}

// StartLinkNode brings up the embedded tailnet node and waits (bounded) for
// registration. Apply/Test uses this synchronous contract so success means the
// encrypted route is ready now, not merely retrying in the background.
func StartLinkNode(controlURL, authKey, hostname, stateDir string) (*LinkNode, []string, error) {
	node, status, err := newLinkNode(controlURL, authKey, hostname, stateDir)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	st, err := waitForLinkRunning(ctx, status, 200*time.Millisecond)
	if err == nil {
		err = node.configureReady(st)
	}
	if err != nil {
		node.Stop()
		return nil, nil, err
	}
	node.completeReady(nil)
	return node, node.Addresses(), nil
}

// StartLinkNodeDeferred starts the encrypted-only data plane immediately and
// completes registration in the background. The returned node can create
// loopback proxies before the route is online; their outbound DialContext waits
// for a Tailscale route and never falls back to the ordinary LAN. This lets the
// app serve its offline metadata mirror during a cold cellular/LAN transition
// instead of turning a transient control-key fetch into a fatal startup modal.
func StartLinkNodeDeferred(controlURL, authKey, hostname, stateDir string) (*LinkNode, error) {
	node, status, err := newLinkNode(controlURL, authKey, hostname, stateDir)
	if err != nil {
		return nil, err
	}
	node.startDeferredReadiness(status, 200*time.Millisecond)
	return node, nil
}

type linkStatusFunc func(context.Context) (*ipnstate.Status, error)

func (l *LinkNode) startDeferredReadiness(status linkStatusFunc, pollEvery time.Duration) {
	l.startDeferredReadinessWith(status, pollEvery, l.configureReady)
}

// startDeferredReadinessWith separates the long-lived retry loop from tsnet's
// preference calls. Tests inject configure so they can prove that a transient
// Running -> EditPrefs failure is retried instead of permanently poisoning the
// saved Link identity.
func (l *LinkNode) startDeferredReadinessWith(status linkStatusFunc, pollEvery time.Duration, configure func(*ipnstate.Status) error) {
	if pollEvery <= 0 {
		pollEvery = 200 * time.Millisecond
	}
	l.mu.Lock()
	s := l.srv
	l.mu.Unlock()
	if s == nil {
		l.completeReady(fmt.Errorf("link: node is stopped"))
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.mu.Lock()
	if l.readyCancel != nil {
		l.readyCancel()
	}
	l.readyCancel = cancel
	l.mu.Unlock()
	go func() {
		lastState := ""
		for {
			st, err := waitForLinkRunning(ctx, func(ctx context.Context) (*ipnstate.Status, error) {
				current, statusErr := status(ctx)
				l.noteReadinessAttempt(current, statusErr)
				if statusErr == nil && current != nil && current.BackendState != lastState {
					lastState = current.BackendState
					jmlog.Info("link control state changed", "state", lastState)
				}
				return current, statusErr
			}, pollEvery)
			if err != nil {
				l.completeReady(err)
				return
			}
			if err := configure(st); err == nil {
				l.completeReady(nil)
				return
			} else {
				l.noteReadinessAttempt(st, err)
			}
			// BackendState can become Running before LocalAPI preference edits
			// are available during a cold boot. Keep the node pending and retry;
			// Apply/Test remains bounded by its caller's WaitReady context.
			select {
			case <-ctx.Done():
				l.completeReady(ctx.Err())
				return
			case <-time.After(pollEvery):
			}
		}
	}()
}

func (l *LinkNode) noteReadinessAttempt(st *ipnstate.Status, err error) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if st != nil {
		l.readyState = st.BackendState
		l.readyHealth = append(l.readyHealth[:0], st.Health...)
	}
	if err != nil {
		l.readyAttemptErr = err
	} else if st != nil && st.BackendState == ipn.Running.String() {
		l.readyAttemptErr = nil
	}
}

func (l *LinkNode) configureReady(st *ipnstate.Status) error {
	if st == nil {
		return fmt.Errorf("link: ready status is missing")
	}
	if err := l.setHostname(l.hostname); err != nil {
		return err
	}
	if err := l.acceptRoutes(); err != nil {
		return err
	}
	addrs := make([]string, 0, len(st.TailscaleIPs))
	for _, a := range st.TailscaleIPs {
		addrs = append(addrs, a.String())
	}
	l.mu.Lock()
	if l.srv == nil {
		l.mu.Unlock()
		return fmt.Errorf("link: node stopped during readiness")
	}
	l.addrs = append([]string(nil), addrs...)
	l.mu.Unlock()
	return nil
}

func (l *LinkNode) completeReady(err error) {
	if l == nil {
		return
	}
	l.readyOnce.Do(func() {
		l.mu.Lock()
		l.readyErr = err
		if err == nil {
			l.readyAttemptErr = nil
		}
		readyCh := l.readyCh
		l.mu.Unlock()
		if readyCh != nil {
			close(readyCh)
		}
	})
}

// WaitReady waits for deferred registration and route acceptance. It never
// starts or stops the node and therefore can be used by diagnostics without
// racing the data-plane lifecycle.
func (l *LinkNode) WaitReady(ctx context.Context) ([]string, error) {
	if l == nil {
		return nil, fmt.Errorf("link: node is nil")
	}
	l.mu.Lock()
	readyCh := l.readyCh
	l.mu.Unlock()
	if readyCh == nil {
		return nil, fmt.Errorf("link: readiness is unavailable")
	}
	select {
	case <-ctx.Done():
		l.mu.Lock()
		state := l.readyState
		health := append([]string(nil), l.readyHealth...)
		attemptErr := l.readyAttemptErr
		l.mu.Unlock()
		detail := ""
		if state != "" {
			detail = fmt.Sprintf("; last state %q", state)
		}
		if len(health) > 0 {
			detail += "; health: " + strings.Join(health, "; ")
		}
		if attemptErr != nil {
			detail += "; last attempt: " + attemptErr.Error()
		}
		return nil, fmt.Errorf("link: readiness is still pending%s: %w", detail, ctx.Err())
	case <-readyCh:
		l.mu.Lock()
		err := l.readyErr
		addrs := append([]string(nil), l.addrs...)
		l.mu.Unlock()
		return addrs, err
	}
}

func waitForLinkRunning(ctx context.Context, status linkStatusFunc, pollEvery time.Duration) (*ipnstate.Status, error) {
	if pollEvery <= 0 {
		pollEvery = 200 * time.Millisecond
	}
	var (
		lastState  = "unknown"
		lastHealth []string
		lastErr    error
	)
	for {
		st, err := status(ctx)
		if err != nil {
			lastErr = err
		} else if st != nil {
			lastState = st.BackendState
			lastHealth = append(lastHealth[:0], st.Health...)
			if st.BackendState == ipn.Running.String() && len(st.TailscaleIPs) > 0 {
				return st, nil
			}
		}

		select {
		case <-ctx.Done():
			detail := ""
			if len(lastHealth) > 0 {
				detail = "; health: " + strings.Join(lastHealth, "; ")
			}
			if lastErr != nil {
				detail += "; last status error: " + lastErr.Error()
			}
			return nil, fmt.Errorf("link: backend did not become ready (last state %q%s): %w", lastState, detail, ctx.Err())
		case <-time.After(pollEvery):
		}
	}
}

func (l *LinkNode) setHostname(hostname string) error {
	l.mu.Lock()
	s := l.srv
	l.mu.Unlock()
	if s == nil {
		return fmt.Errorf("link: node is stopped")
	}
	lc, err := s.LocalClient()
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
	// UserDial deliberately falls back to the ordinary system network when
	// no tailnet route matches. That is useful for a general SOCKS proxy but
	// violates JuiceMount Link's contract: backend traffic must never escape
	// the encrypted route merely because a subnet router is still converging
	// or has gone offline. Wait for an explicitly Tailscale-routed plan and
	// dial the resolved address from that plan.
	dialer := s.Sys().Dialer.Get()
	planned, err := waitForTailscaleDialPlan(ctx, network, address,
		func(ctx context.Context, network, address string) (netip.AddrPort, bool, error) {
			return dialer.UserDialPlan(ctx, network, address)
		})
	if err != nil {
		return nil, err
	}
	conn, err := s.Dial(ctx, network, planned.String())
	if err != nil {
		return nil, err
	}
	if !linkConnUsesTailnet(conn) {
		_ = conn.Close()
		return nil, fmt.Errorf("link: refused non-tailnet connection to %s", address)
	}
	return conn, nil
}

type tailscaleDialPlanner func(context.Context, string, string) (netip.AddrPort, bool, error)

func waitForTailscaleDialPlan(ctx context.Context, network, address string, plan tailscaleDialPlanner) (netip.AddrPort, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var last netip.AddrPort
	for {
		planned, viaTailscale, err := plan(ctx, network, address)
		if err != nil {
			return netip.AddrPort{}, fmt.Errorf("link: route plan for %s: %w", address, err)
		}
		last = planned
		if viaTailscale {
			return planned, nil
		}
		select {
		case <-ctx.Done():
			return netip.AddrPort{}, fmt.Errorf("link: no encrypted route to %s (last plan %s): %w", address, last, ctx.Err())
		case <-ticker.C:
		}
	}
}

func linkConnUsesTailnet(conn net.Conn) bool {
	if conn == nil || conn.LocalAddr() == nil {
		return false
	}
	host, _, err := net.SplitHostPort(conn.LocalAddr().String())
	if err != nil {
		return false
	}
	ip, err := netip.ParseAddr(strings.Trim(host, "[]"))
	return err == nil && tsaddr.IsTailscaleIP(ip)
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

	proxyKey := target + "\x00" + proxyIdleTimeout(raw).String()
	l.mu.Lock()
	s := l.srv
	if addr := l.proxyEndpoints[proxyKey]; addr != "" {
		l.mu.Unlock()
		return rewrite(addr), nil
	}
	l.mu.Unlock()
	if s == nil {
		return raw, fmt.Errorf("link: node is stopped")
	}

	p, err := newTCPProxyWithIdleTimeout(target, l.DialContext, proxyIdleTimeout(raw))
	if err != nil {
		return raw, err
	}
	l.mu.Lock()
	if l.srv == nil {
		l.mu.Unlock()
		_ = p.Close()
		return raw, fmt.Errorf("link: node is stopped")
	}
	if addr := l.proxyEndpoints[proxyKey]; addr != "" {
		l.mu.Unlock()
		_ = p.Close()
		return rewrite(addr), nil
	}
	if l.proxyEndpoints == nil {
		l.proxyEndpoints = make(map[string]string)
	}
	l.proxies = append(l.proxies, p)
	l.proxyEndpoints[proxyKey] = p.listener.Addr().String()
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
	conn, err := l.DialContext(ctx, "tcp", target)
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
	// A loopback URL cannot preserve certificate identity: rewriting
	// https://nas.example to https://127.0.0.1 makes JuiceFS validate the
	// certificate against 127.0.0.1. Refuse encrypted upstream schemes until
	// the proxy can retain the original hostname/SNI instead of allowing a
	// preflight probe to pass and the real mount to fail later.
	switch strings.ToLower(u.Scheme) {
	case "https", "rediss":
		return "", nil, fmt.Errorf("link: %s endpoints are not supported by the loopback data proxy; use HTTP/Redis on the private NAS route", strings.ToLower(u.Scheme))
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

	localReader := io.Reader(local)
	remoteReader := io.Reader(remote)
	remoteWriter := io.Writer(remote)
	localWriter := io.Writer(local)
	var watchdogStop chan struct{}
	if p.idleTimeout > 0 {
		// tsnet connections can remain blocked past a socket deadline while a
		// userspace path is wedged. Track byte-level progress and close both legs
		// from an independent watchdog, so the external JuiceFS process receives
		// a retryable connection error before its 30-second S3 attempt aborts the
		// mount. Healthy large transfers refresh the timer on every read/write.
		activity := &proxyActivity{}
		activity.touch()
		localReader = &activityReader{Reader: localReader, activity: activity}
		remoteReader = &activityReader{Reader: remoteReader, activity: activity}
		remoteWriter = &activityWriter{Writer: remoteWriter, activity: activity}
		localWriter = &activityWriter{Writer: localWriter, activity: activity}
		watchdogStop = make(chan struct{})
		go closeProxyAfterIdle(local, remote, activity, p.idleTimeout, watchdogStop)
	}
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(remoteWriter, localReader); done <- struct{}{} }()
	go func() { _, _ = io.Copy(localWriter, remoteReader); done <- struct{}{} }()
	<-done
	if watchdogStop != nil {
		close(watchdogStop)
	}
	_ = local.Close()
	_ = remote.Close()
	<-done
}

type proxyActivity struct {
	lastProgress atomic.Int64
}

func (a *proxyActivity) touch() {
	a.lastProgress.Store(time.Now().UnixNano())
}

type activityReader struct {
	io.Reader
	activity *proxyActivity
}

func (r *activityReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		r.activity.touch()
	}
	return n, err
}

type activityWriter struct {
	io.Writer
	activity *proxyActivity
}

func (w *activityWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if n > 0 {
		w.activity.touch()
	}
	return n, err
}

func closeProxyAfterIdle(local, remote net.Conn, activity *proxyActivity, idleTimeout time.Duration, stop <-chan struct{}) {
	interval := idleTimeout / 4
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	if interval > time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-ticker.C:
			last := time.Unix(0, activity.lastProgress.Load())
			if now.Sub(last) < idleTimeout {
				continue
			}
			_ = local.Close()
			_ = remote.Close()
			return
		}
	}
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
// dials made through this node's netstack. A failure is fatal: silently
// degrading to the ordinary LAN would defeat Link's data-plane guarantee.
func (l *LinkNode) acceptRoutes() error {
	l.mu.Lock()
	s := l.srv
	l.mu.Unlock()
	if s == nil {
		return fmt.Errorf("link: node is stopped")
	}
	lc, err := s.LocalClient()
	if err != nil {
		return fmt.Errorf("link: accept-routes unavailable: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := lc.EditPrefs(ctx, &ipn.MaskedPrefs{
		Prefs:       ipn.Prefs{RouteAll: true},
		RouteAllSet: true,
	}); err != nil {
		return fmt.Errorf("link: accept-routes: %w", err)
	}
	return nil
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
	readyCancel := l.readyCancel
	l.srv = nil
	l.proxies = nil
	l.proxyEndpoints = nil
	l.addrs = nil
	l.readyCancel = nil
	l.mu.Unlock()
	if readyCancel != nil {
		readyCancel()
	}
	for _, proxy := range proxies {
		_ = proxy.Close()
	}
	if s != nil {
		_ = s.Close()
	}
	l.completeReady(fmt.Errorf("link: node is stopped"))
}

package nfs

// JuiceMount Link client node (Tier-2 T2.2): an embedded Tailscale/tsnet
// endpoint inside the Mac app. When the user pairs (code from the NAS
// manager), cbridge brings this node up BEFORE mounting, so the NFS loopback
// server's backend traffic — JuiceFS→Redis, MinIO bucket override — can
// target the NAS over the tailnet from anywhere.
//
// v1 scope (this file): lifecycle + address discovery. The Swift layer owns
// the pairing UI; it passes control URL + auth key + NAS tailnet IP through
// ServerConfig. Endpoint routing for Redis/MinIO over the tailnet rides the
// NAS subnet-router deployment step (documented in SPRINT-T1-T2.md).

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"tailscale.com/ipn"
	"tailscale.com/tsnet"
)

type LinkNode struct {
	srv *tsnet.Server
	mu  sync.Mutex
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

// LinkEndpointOverride re-points NAS endpoints at the NAS tailnet address
// once Link is up (T2.2): the bridge calls this with ServerConfig's
// redis_url / bucket_override right after StartLinkNode succeeds, so the
// metadata client and the juicefs mount target tunnel-routed addresses when
// away from the home LAN instead of unroutable 192.168.x.x ones.
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
	l.mu.Unlock()
	if s != nil {
		s.Close()
	}
}

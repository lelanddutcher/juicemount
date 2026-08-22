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
	"path/filepath"
	"sync"
	"time"

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
	return &LinkNode{srv: s}, addrs, nil
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

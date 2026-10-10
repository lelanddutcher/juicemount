package nfs

import (
	"context"
	"fmt"

	"tailscale.com/ipn/ipnstate"
)

// LinkTransportStatus describes the actual peer path carrying the NAS subnet
// route. A successful control-plane join is not enough: DERP-only bulk traffic
// can pass tiny health probes while missing JuiceFS's object request budget.
type LinkTransportStatus struct {
	Mode     string
	Peer     string
	Endpoint string
	Relay    string
}

// TransportStatus reads live magicsock peer state and identifies the peer
// advertising a primary subnet route (the embedded NAS node).
func (l *LinkNode) TransportStatus(ctx context.Context) (LinkTransportStatus, error) {
	if l == nil {
		return LinkTransportStatus{}, fmt.Errorf("link: node is nil")
	}
	l.mu.Lock()
	s := l.srv
	l.mu.Unlock()
	if s == nil {
		return LinkTransportStatus{}, fmt.Errorf("link: node is stopped")
	}
	lc, err := s.LocalClient()
	if err != nil {
		return LinkTransportStatus{}, fmt.Errorf("link: local status unavailable: %w", err)
	}
	status, err := lc.Status(ctx)
	if err != nil {
		return LinkTransportStatus{}, fmt.Errorf("link: transport status: %w", err)
	}
	return linkTransportFromStatus(status)
}

func linkTransportFromStatus(status *ipnstate.Status) (LinkTransportStatus, error) {
	if status == nil {
		return LinkTransportStatus{}, fmt.Errorf("link: empty transport status")
	}
	for _, peer := range status.Peer {
		if peer == nil || peer.PrimaryRoutes == nil || peer.PrimaryRoutes.Len() == 0 {
			continue
		}
		result := LinkTransportStatus{
			Peer:     peer.HostName,
			Endpoint: peer.CurAddr,
			Relay:    peer.Relay,
			Mode:     "unresolved",
		}
		switch {
		case peer.CurAddr != "":
			result.Mode = "direct"
		case peer.PeerRelay != "":
			result.Mode = "peer-relay"
			result.Relay = peer.PeerRelay
		case peer.Relay != "":
			result.Mode = "derp"
		}
		return result, nil
	}
	return LinkTransportStatus{}, fmt.Errorf("link: no peer is advertising the NAS subnet route")
}

package nfs

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"

	"tailscale.com/ipn/ipnstate"
	"tailscale.com/net/tsaddr"
)

// LinkTransportStatus describes the actual peer path to the NAS node (the
// peer carrying the NAS subnet route, or the tag:jm-nas peer serving the
// JuiceMount ports on its tailnet address). A successful control-plane join
// is not enough: DERP-only bulk traffic can pass tiny health probes while
// missing JuiceFS's object request budget.
type LinkTransportStatus struct {
	Mode     string
	Peer     string
	Endpoint string
	Relay    string
}

// TransportStatus reads live magicsock peer state and identifies the NAS
// node: the NAS service peer (linkNASServicePeer), else the peer
// advertising a primary subnet route.
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
	// The NAS service peer first, as linkDialPlanner dials it first.
	if peer := linkNASServicePeer(status); peer != nil {
		return linkTransportForPeer(peer), nil
	}
	for _, peer := range status.Peer {
		if peer == nil || peer.PrimaryRoutes == nil || peer.PrimaryRoutes.Len() == 0 {
			continue
		}
		return linkTransportForPeer(peer), nil
	}
	return LinkTransportStatus{}, fmt.Errorf("link: no peer is advertising the NAS subnet route or serving as the NAS node")
}

func linkTransportForPeer(peer *ipnstate.PeerStatus) LinkTransportStatus {
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
	return result
}

// linkNASTag marks the NAS node that serves the JuiceMount ports on its own
// tailnet address (the Manager's service exposure). Tags are assigned by the
// control server: the Manager gives this one only to the NAS node it runs,
// found by node key, and its own policy lets no node request it. That does
// not make the tag unique. An operator's own policy may let a node claim it,
// and an earlier NAS identity keeps it in a stale record, so the Manager's
// status reports any second holder, and linkNASServicePeer below never
// guesses between two online ones.
const linkNASTag = "tag:jm-nas"

// linkNASServicePeer returns the one tag:jm-nas peer, preferring online
// peers when a stale record lingers. Anything ambiguous returns nil: Link
// must never guess which machine receives the volume's traffic.
func linkNASServicePeer(status *ipnstate.Status) *ipnstate.PeerStatus {
	if status == nil {
		return nil
	}
	var tagged, online []*ipnstate.PeerStatus
	for _, peer := range status.Peer {
		if peer == nil || peer.Tags == nil || !peer.Tags.ContainsFunc(func(tag string) bool { return tag == linkNASTag }) || len(peer.TailscaleIPs) == 0 {
			continue
		}
		tagged = append(tagged, peer)
		if peer.Online {
			online = append(online, peer)
		}
	}
	switch {
	case len(tagged) == 1:
		return tagged[0]
	case len(online) == 1:
		return online[0]
	}
	return nil
}

// linkServiceDialTarget maps a backend address with no tailnet route (the
// NAS's LAN or public address) to the same port on the NAS service peer's
// tailnet address. The NAS node serves each JuiceMount service on the port
// the NAS publishes on the LAN, so only the host changes.
func linkServiceDialTarget(status *ipnstate.Status, address string) (netip.AddrPort, bool) {
	host, portRaw, err := net.SplitHostPort(address)
	if err != nil {
		return netip.AddrPort{}, false
	}
	port, err := strconv.ParseUint(portRaw, 10, 16)
	if err != nil || port == 0 {
		return netip.AddrPort{}, false
	}
	if ip, err := netip.ParseAddr(strings.Trim(host, "[]")); err == nil && tsaddr.IsTailscaleIP(ip) {
		return netip.AddrPort{}, false
	}
	peer := linkNASServicePeer(status)
	if peer == nil {
		return netip.AddrPort{}, false
	}
	addr := peer.TailscaleIPs[0]
	for _, ip := range peer.TailscaleIPs {
		if ip.Is4() {
			addr = ip
			break
		}
	}
	return netip.AddrPortFrom(addr, uint16(port)), true
}

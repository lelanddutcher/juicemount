package nfs

import (
	"net/netip"
	"testing"

	"tailscale.com/ipn/ipnstate"
	"tailscale.com/types/key"
	"tailscale.com/types/views"
)

func TestLinkTransportFromStatus(t *testing.T) {
	routes := views.SliceOf([]netip.Prefix{netip.MustParsePrefix("192.168.0.0/24")})
	keyA := key.NewNode().Public()
	status := &ipnstate.Status{Peer: map[key.NodePublic]*ipnstate.PeerStatus{
		keyA: {HostName: "juicemount-nas", PrimaryRoutes: &routes, CurAddr: "192.168.0.197:41641", Relay: "nyc"},
	}}
	got, err := linkTransportFromStatus(status)
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode != "direct" || got.Peer != "juicemount-nas" || got.Endpoint != "192.168.0.197:41641" {
		t.Fatalf("direct status = %+v", got)
	}

	status.Peer[keyA].CurAddr = ""
	got, err = linkTransportFromStatus(status)
	if err != nil || got.Mode != "derp" || got.Relay != "nyc" {
		t.Fatalf("DERP status = %+v, %v", got, err)
	}
}

func TestLinkTransportFromStatusRequiresSubnetRouter(t *testing.T) {
	if _, err := linkTransportFromStatus(&ipnstate.Status{}); err == nil {
		t.Fatal("status without a subnet router was accepted")
	}
}

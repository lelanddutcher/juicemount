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

func nasServicePeer(hostname string, ips []string, online bool, tags ...string) *ipnstate.PeerStatus {
	var addrs []netip.Addr
	for _, ip := range ips {
		addrs = append(addrs, netip.MustParseAddr(ip))
	}
	tagView := views.SliceOf(tags)
	return &ipnstate.PeerStatus{HostName: hostname, TailscaleIPs: addrs, Online: online, Tags: &tagView}
}

// TestLinkTransportFromStatusFindsNASServicePeer: a NAS that serves the
// JuiceMount ports on its tailnet address advertises no subnet route; its
// tag identifies it for transport reporting and the LAN fast path.
func TestLinkTransportFromStatusFindsNASServicePeer(t *testing.T) {
	nas := nasServicePeer("juicemount-nas", []string{"fd7a:115c:a1e0::1", "100.64.0.1"}, true, "tag:jm-nas")
	nas.CurAddr = "10.0.0.2:41641" // private: the LAN fast path takes no other kind
	status := &ipnstate.Status{Peer: map[key.NodePublic]*ipnstate.PeerStatus{
		key.NewNode().Public(): nasServicePeer("other-mac", []string{"100.64.0.9"}, true, "tag:jm-mac"),
		key.NewNode().Public(): nas,
	}}
	got, err := linkTransportFromStatus(status)
	if err != nil || got.Mode != "direct" || got.Peer != "juicemount-nas" || got.Endpoint != "10.0.0.2:41641" {
		t.Fatalf("service peer transport = %+v, %v", got, err)
	}
	if !directLANEndpointMatches("10.0.0.2:30179", got) {
		t.Fatal("LAN fast path rejected the NAS service peer's verified endpoint")
	}
}

func TestLinkServiceDialTarget(t *testing.T) {
	nasKey := key.NewNode().Public()
	status := &ipnstate.Status{Peer: map[key.NodePublic]*ipnstate.PeerStatus{
		nasKey:                 nasServicePeer("juicemount-nas", []string{"fd7a:115c:a1e0::1", "100.64.0.1"}, true, "tag:jm-nas"),
		key.NewNode().Public(): nasServicePeer("juicemount-nas", []string{"100.64.0.2"}, true), // untagged look-alike
	}}
	for addr, want := range map[string]string{
		"192.0.2.10:30179":      "100.64.0.1:30179",
		"nas.example.com:30151": "100.64.0.1:30151",
		"[2001:db8::5]:30190":   "100.64.0.1:30190",
	} {
		got, ok := linkServiceDialTarget(status, addr)
		if !ok || got.String() != want {
			t.Errorf("linkServiceDialTarget(%q) = %v, %v; want %s", addr, got, ok, want)
		}
	}
	for _, addr := range []string{"100.64.0.5:30179", "192.0.2.10", "192.0.2.10:http", "192.0.2.10:0"} {
		if got, ok := linkServiceDialTarget(status, addr); ok {
			t.Errorf("linkServiceDialTarget(%q) = %v, want no rewrite", addr, got)
		}
	}

	// Two tagged NAS nodes: only the online one may receive traffic, and two
	// online ones are ambiguous, so nothing is rewritten.
	stale := key.NewNode().Public()
	status.Peer[stale] = nasServicePeer("juicemount-nas-1", []string{"100.64.0.7"}, false, "tag:jm-nas")
	if got, ok := linkServiceDialTarget(status, "192.0.2.10:30179"); !ok || got.String() != "100.64.0.1:30179" {
		t.Fatalf("stale twin: got %v, %v; want the online NAS", got, ok)
	}
	status.Peer[stale].Online = true
	if got, ok := linkServiceDialTarget(status, "192.0.2.10:30179"); ok {
		t.Fatalf("ambiguous NAS peers rewrote to %v", got)
	}
	if _, ok := linkServiceDialTarget(&ipnstate.Status{}, "192.0.2.10:30179"); ok {
		t.Fatal("rewrite without a NAS service peer")
	}
}

package nfs

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"testing"
	"time"

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

// TestLinkPrefersNASServicePeerOverRoutes: another node's route covering
// the NAS's LAN address must not take a Mac's traffic, or its transport
// report, from the tag:jm-nas peer; without that peer the route is used.
func TestLinkPrefersNASServicePeerOverRoutes(t *testing.T) {
	routes := views.SliceOf([]netip.Prefix{netip.MustParsePrefix("192.0.2.0/24")})
	rogue := &ipnstate.PeerStatus{HostName: "rogue", PrimaryRoutes: &routes, CurAddr: "203.0.113.9:41641"}
	nas := nasServicePeer("juicemount-nas", []string{"100.64.0.1"}, true, "tag:jm-nas")
	nas.CurAddr = "10.0.0.2:41641"
	status := &ipnstate.Status{Peer: map[key.NodePublic]*ipnstate.PeerStatus{
		key.NewNode().Public(): rogue,
		key.NewNode().Public(): nas,
	}}
	if got, err := linkTransportFromStatus(status); err != nil || got.Peer != "juicemount-nas" {
		t.Fatalf("transport with a rogue route = %+v, %v; want the NAS service peer", got, err)
	}

	viaRoute := func(context.Context, string, string) (netip.AddrPort, bool, error) {
		return netip.MustParseAddrPort("192.0.2.10:30179"), true, nil
	}
	plan := linkDialPlanner(viaRoute, func(_ context.Context, address string) (netip.AddrPort, bool) {
		return linkServiceDialTarget(status, address)
	})
	if got, ok, err := plan(context.Background(), "tcp", "192.0.2.10:30179"); err != nil || !ok || got.String() != "100.64.0.1:30179" {
		t.Fatalf("dial plan with a rogue route = %v, %v, %v; want the NAS service peer", got, ok, err)
	}

	noPeer := linkDialPlanner(viaRoute, func(context.Context, string) (netip.AddrPort, bool) { return netip.AddrPort{}, false })
	if got, ok, err := noPeer(context.Background(), "tcp", "192.0.2.10:30179"); err != nil || !ok || got.String() != "192.0.2.10:30179" {
		t.Fatalf("dial plan without a NAS service peer = %v, %v, %v; want the route", got, ok, err)
	}
}

// TestLinkStaleOfflineNASPeerDoesNotBeatLiveRoute: subnet exposure after
// the NAS node's state was reset. Its earlier registration (tagged during a
// service-exposure period) is offline; the current NAS node is untagged and
// serves the approved route. The Mac follows the route and reports its
// router, not the offline node.
func TestLinkStaleOfflineNASPeerDoesNotBeatLiveRoute(t *testing.T) {
	routes := views.SliceOf([]netip.Prefix{netip.MustParsePrefix("192.0.2.0/24")})
	stale := nasServicePeer("juicemount-nas", []string{"100.64.0.9"}, false, linkNASTag)
	live := &ipnstate.PeerStatus{HostName: "juicemount-nas-1", TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.10")}, Online: true, PrimaryRoutes: &routes, CurAddr: "10.0.0.2:41641"}
	st := &ipnstate.Status{Peer: map[key.NodePublic]*ipnstate.PeerStatus{key.NewNode().Public(): stale, key.NewNode().Public(): live}}
	plan := linkDialPlanner(func(_ context.Context, _, address string) (netip.AddrPort, bool, error) {
		return netip.MustParseAddrPort(address), true, nil
	}, func(_ context.Context, address string) (netip.AddrPort, bool) {
		return linkServiceDialTarget(st, address)
	})
	if got, ok, err := plan(context.Background(), "tcp", "192.0.2.2:30179"); err != nil || !ok || got.String() != "192.0.2.2:30179" {
		t.Fatalf("dial plan = %v, %v, %v; want the live route, not the offline tag:jm-nas node", got, ok, err)
	}
	if tr, err := linkTransportFromStatus(st); err != nil || tr.Peer != "juicemount-nas-1" || tr.Mode != "direct" {
		t.Fatalf("transport = %+v, %v; want the live route's router", tr, err)
	}
}

// TestPeerStatusCacheReusesForOneSecond: the dial path asks for the peer
// status every 100 ms while it waits; one fetch serves a second, for the
// same tsnet server only, errors are not kept, and concurrent use is safe.
func TestPeerStatusCacheReusesForOneSecond(t *testing.T) {
	var mu sync.Mutex
	clock := time.Unix(1000, 0)
	fetches := 0
	fail := false
	fetch := func(context.Context) (*ipnstate.Status, error) {
		mu.Lock()
		defer mu.Unlock()
		fetches++
		if fail {
			return nil, errors.New("local API down")
		}
		return &ipnstate.Status{}, nil
	}
	c := &peerStatusCache{now: func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }}
	count := func() int { mu.Lock(); defer mu.Unlock(); return fetches }
	advance := func(d time.Duration) { mu.Lock(); clock = clock.Add(d); mu.Unlock() }
	srvA, srvB := new(int), new(int)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = c.get(context.Background(), srvA, fetch) }()
	}
	wg.Wait()
	first := count()
	advance(900 * time.Millisecond)
	if c.get(context.Background(), srvA, fetch) == nil || count() != first {
		t.Fatalf("within the TTL: %d fetches, want %d", count(), first)
	}
	if c.get(context.Background(), srvB, fetch) == nil || count() != first+1 {
		t.Fatalf("another server reused the cached status: %d fetches", count())
	}
	advance(1100 * time.Millisecond)
	mu.Lock()
	fail = true
	mu.Unlock()
	if c.get(context.Background(), srvB, fetch) != nil {
		t.Fatal("an expired status served after a failed fetch")
	}
	mu.Lock()
	fail = false
	mu.Unlock()
	before := count()
	if c.get(context.Background(), srvB, fetch) == nil || count() != before+1 {
		t.Fatal("a failed fetch was cached")
	}
}

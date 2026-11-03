package main

// End-to-end Link tests against a real Headscale and two in-process tsnet
// nodes on loopback: the NAS node (nasNodeConfigFromEnv + runNASNode, as the
// Manager runs it) and a Mac (nfs.StartLinkNode, as the app runs it). They
// cover each policy (the Manager's, JM_NET_POLICY=off, an operator's own)
// and each exposure (services, subnet).
//
// It needs the pinned Headscale binary, so it runs only when
// JM_TEST_HEADSCALE_BIN names it (the version the manager image ships,
// server/juicemount-manager/Dockerfile):
//
//	curl -fsSLo headscale https://github.com/juanfont/headscale/releases/download/v0.29.3/headscale_0.29.3_linux_amd64
//	echo '8dc183758024ed7095cf610fedea0790233613c71353bc8be2715d82ba29b92c  headscale' | sha256sum -c && chmod +x headscale
//	JM_TEST_HEADSCALE_BIN=$PWD/headscale go test -run TestLinkIntegration ./cmd/juicemount-manager
//
// The DERP map names an unused loopback port (Headscale requires one), so
// the test needs no network beyond this host and the nodes must connect
// directly.

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/manager"
	"github.com/lelanddutcher/juicemount/nfs"
	"tailscale.com/tailcfg"
)

func freeLoopbackPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// unusedDERPMap names a relay on a loopback port nothing listens on.
// Headscale requires a non-empty DERP map; the two nodes in this process
// then find each other directly over their local interface endpoints, which
// is the path Link is meant to use anyway.
func unusedDERPMap(t *testing.T) *tailcfg.DERPMap {
	return &tailcfg.DERPMap{Regions: map[int]*tailcfg.DERPRegion{
		900: {RegionID: 900, RegionCode: "test", Nodes: []*tailcfg.DERPNode{{
			Name: "900a", RegionID: 900, HostName: "127.0.0.1", IPv4: "127.0.0.1", IPv6: "none",
			STUNPort: -1, DERPPort: freeLoopbackPort(t), InsecureForTests: true, STUNTestIP: "127.0.0.1",
		}}},
	}}
}

// writeDERPMapYAML renders a DERP map in the YAML shape Headscale's
// derp.paths loader reads (lower-cased tailcfg field names).
func writeDERPMapYAML(t *testing.T, path string, m *tailcfg.DERPMap) {
	t.Helper()
	var b strings.Builder
	b.WriteString("regions:\n")
	ids := make([]int, 0, len(m.Regions))
	for id := range m.Regions {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		r := m.Regions[id]
		fmt.Fprintf(&b, "  %d:\n    regionid: %d\n    regioncode: %s\n    regionname: test\n    nodes:\n", id, r.RegionID, r.RegionCode)
		for _, n := range r.Nodes {
			fmt.Fprintf(&b, "      - name: %q\n        regionid: %d\n        hostname: %s\n        ipv4: %s\n        ipv6: %s\n        stunport: %d\n        derpport: %d\n        insecurefortests: %t\n        stuntestip: %s\n",
				n.Name, n.RegionID, n.HostName, n.IPv4, n.IPv6, n.STUNPort, n.DERPPort, n.InsecureForTests, n.STUNTestIP)
		}
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
}

func waitForState(t *testing.T, name string, c *componentStatus, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !c.running() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not reach running: %+v", name, c.snapshot())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// linkIntegrationCase is one deployment shape.
type linkIntegrationCase struct {
	policy       string // "managed", "off", or an operator policy's HuJSON
	subnet       bool   // subnet exposure (JM_NET_NAS_ROUTE)
	legacyNASKey bool   // the NAS identity predates tagged keys
	// wantNASErr, when set, is a substring of the NAS node's error: the
	// node must fail loudly instead of running.
	wantNASErr   string
	wantRestarts int
}

// TestLinkIntegrationNASServiceExposureAndPolicy is a fresh install: the NAS
// node registers with a tag:jm-nas key and the Manager's policy applies.
func TestLinkIntegrationNASServiceExposureAndPolicy(t *testing.T) {
	runLinkIntegration(t, linkIntegrationCase{policy: "managed"})
}

// TestLinkIntegrationUntaggedNASNodeIsTagged is an upgrade to service
// exposure: the NAS node's identity predates tagged keys, so it is tagged
// server-side once it is up, then rejoins once (the one restart) to receive
// the policy's rules for its new tag.
func TestLinkIntegrationUntaggedNASNodeIsTagged(t *testing.T) {
	runLinkIntegration(t, linkIntegrationCase{policy: "managed", legacyNASKey: true, wantRestarts: 1})
}

// TestLinkIntegrationPolicyOffServiceExposure: without any ACL policy the
// NAS node's key still carries tag:jm-nas, so a Mac finds the node.
func TestLinkIntegrationPolicyOffServiceExposure(t *testing.T) {
	runLinkIntegration(t, linkIntegrationCase{policy: "off"})
}

// TestLinkIntegrationOperatorPolicyRefusingNASTag: an operator's policy
// that does not define tag:jm-nas cannot tag an existing untagged NAS
// identity. The node must not report running (its listeners would be
// unreachable for Macs); its error names the remedy.
func TestLinkIntegrationOperatorPolicyRefusingNASTag(t *testing.T) {
	runLinkIntegration(t, linkIntegrationCase{
		policy:       `{"tagOwners": {"tag:other": []}, "acls": [{"action": "accept", "src": ["*"], "dst": ["*:*"]}]}`,
		legacyNASKey: true,
		wantNASErr:   "tagOwners",
	})
}

// TestLinkIntegrationSubnetExposureWaitsForRoute: a subnet-exposure NAS
// node is not tagged tag:jm-nas, so before its route is approved a Mac's
// dial waits for the route instead of being sent to the node's tailnet
// address; once approved, the Mac finds the NAS node by its route.
func TestLinkIntegrationSubnetExposureWaitsForRoute(t *testing.T) {
	runLinkIntegration(t, linkIntegrationCase{policy: "managed", subnet: true})
}

func runLinkIntegration(t *testing.T, tc linkIntegrationCase) {
	bin := os.Getenv("JM_TEST_HEADSCALE_BIN")
	if bin == "" {
		t.Skip("JM_TEST_HEADSCALE_BIN is not set; see the file comment for the pinned Headscale binary")
	}
	if out, err := exec.Command(bin, "version").CombinedOutput(); err != nil {
		t.Fatalf("headscale version: %v: %s", err, out)
	} else {
		t.Logf("headscale: %s", strings.SplitN(string(out), "\n", 2)[0])
	}

	dir := t.TempDir()
	oldBin, oldDir := hsBinary, hsDataDir
	t.Cleanup(func() { hsBinary, hsDataDir = oldBin, oldDir })
	hsBinary, hsDataDir = bin, dir

	// Backend services on the "container network" (loopback here). Every
	// forwarded dial, by service listener or subnet route, lands on echo.
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	go func() {
		for {
			conn, err := echo.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				line, err := bufio.NewReader(conn).ReadString('\n')
				if err == nil {
					_, _ = io.WriteString(conn, "svc:"+line)
				}
			}()
		}
	}()

	// The public URL is deliberately unreachable: the NAS node must register
	// through the local listener, and Macs here use loopback too.
	const publicURL = "https://link.example.com:30193"
	hsPort := freeLoopbackPort(t)
	listen := net.JoinHostPort("127.0.0.1", strconv.Itoa(hsPort))
	localControl := "http://" + listen
	derpPath := filepath.Join(dir, "derp.yaml")
	writeDERPMapYAML(t, derpPath, unusedDERPMap(t))
	cfgPath := filepath.Join(dir, "config.yaml")
	cfg := renderHeadscaleConfig(headscaleConfigParams{
		ServerURL:     publicURL,
		Listen:        listen,
		MetricsListen: net.JoinHostPort("127.0.0.1", strconv.Itoa(freeLoopbackPort(t))),
		GRPCListen:    net.JoinHostPort("127.0.0.1", strconv.Itoa(freeLoopbackPort(t))),
		DataDir:       dir,
		DERPPaths:     []string{derpPath},
	})
	policyEnv := ""
	switch tc.policy {
	case "managed":
	case "off":
		policyEnv = "off"
	default:
		operatorPolicy := filepath.Join(dir, "operator.hujson")
		if err := os.WriteFile(operatorPolicy, []byte(tc.policy), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg += "policy:\n  mode: file\n  path: " + operatorPolicy + "\n"
	}
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	// The NAS's "LAN": a documentation prefix that no interface of this host
	// is in. A route over the host's own network would also cover the
	// nodes' WireGuard endpoints.
	lan := unusedDocumentationPrefix(t)
	nasLAN := lan.Addr().Next().Next().String() // .2
	subnet := ""
	if tc.subnet {
		subnet = lan.String()
	}
	echoAddr := echo.Addr().String()
	t.Setenv("JM_NET_SERVER_URL", publicURL)
	t.Setenv("JM_NET_TLS_CERT", "")
	t.Setenv("JM_NET_TLS_KEY", "")
	t.Setenv("JM_NET_HEADSCALE", "on")
	t.Setenv("JM_NET_NAS_NODE", "on")
	t.Setenv("JM_NET_NAS_CONTROL_URL", localControl)
	t.Setenv("JM_NET_NAS_HOSTNAME", "")
	t.Setenv("JM_NET_NAS_EXPOSURE", "")
	t.Setenv("JM_NET_NAS_ROUTE", subnet)
	t.Setenv("JM_NET_POLICY", policyEnv)
	t.Setenv("JM_HEADSCALE_USER", "")
	// The policy allows 30179 and the Manager's 30190; the NAS node below
	// also serves 30199, which no rule allows.
	t.Setenv("JM_NET_NAS_SERVICES", "30179="+echoAddr+",30190=manager")
	if _, err := manager.RecordLinkExposureDefault(cfgPath); err != nil {
		t.Fatal(err)
	}

	hs := newHeadscaleSupervisor()
	hs.listen = listen
	hs.Start()
	defer hs.Stop()
	waitForState(t, "headscale", hs.status, 90*time.Second)

	// The Manager's handler answers with the address it saw the request
	// come from.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		_, _ = io.WriteString(w, host)
	})
	nasCfg, err := nasNodeConfigFromEnv(cfgPath, handler)
	if err != nil {
		t.Fatal(err)
	}
	if nasCfg.ControlURL != localControl {
		t.Fatalf("nas node control URL = %s", nasCfg.ControlURL)
	}
	// A free port, fixed across the node's restarts as in production (the
	// DERP map here relays nothing, so peers need its endpoint to stay put).
	udp, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	nasCfg.UDPPort = uint16(udp.LocalAddr().(*net.UDPAddr).Port)
	_ = udp.Close()
	nasCfg.Services = append(nasCfg.Services, nasService{Port: 30199, Target: echoAddr})
	nasCfg.Dial = func(ctx context.Context, network, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, network, echoAddr)
	}
	nasCfg.HealthEvery = time.Second
	if tc.legacyNASKey {
		nasCfg.MintKey = func() (string, error) { return manager.MintPreauthKey(bin, cfgPath, false, nil) }
	}
	var approve atomic.Bool
	if nasCfg.ApproveRoute != nil {
		realApprove := nasCfg.ApproveRoute
		nasCfg.ApproveRoute = func(prefix string) error {
			if !approve.Load() {
				return errors.New("approval held by the test")
			}
			return realApprove(prefix)
		}
	}
	nas := newSupervisor("nas node", func(stop <-chan struct{}, ready func(string)) error {
		return runNASNode(nasCfg, stop, ready)
	})
	nas.Start()
	defer nas.Stop()

	if tc.wantNASErr != "" {
		deadline := time.Now().Add(90 * time.Second)
		for {
			st := nas.status.snapshot()
			if st.State == "running" {
				t.Fatalf("nas node runs although its tag was refused: %+v", st)
			}
			if st.State == "backoff" && strings.Contains(st.LastError, tc.wantNASErr) {
				t.Logf("nas node error: %s", st.LastError)
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("nas node did not fail with %q: %+v", tc.wantNASErr, st)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	waitForState(t, "nas node", nas.status, 90*time.Second)
	t.Logf("nas node: %s", nas.status.snapshot().Detail)

	wantNASTags := []string{manager.LinkTagNAS}
	if tc.subnet {
		wantNASTags = nil
	}
	out, err := exec.Command(bin, "--config", cfgPath, "nodes", "list", "-o", "json").Output()
	if err != nil {
		t.Fatalf("nodes list: %v", err)
	}
	var nodes []manager.HSNode
	if err := json.Unmarshal(out, &nodes); err != nil {
		t.Fatalf("nodes list json: %v\n%s", err, out)
	}
	if len(nodes) != 1 || nodes[0].GivenName != "juicemount-nas" || !reflect.DeepEqual(nodes[0].Tags, wantNASTags) {
		t.Fatalf("registered nodes = %+v, want the NAS node tagged %v", nodes, wantNASTags)
	}

	var macTags []string
	if manager.LinkPolicyManaged(cfgPath) {
		macTags = []string{manager.LinkTagMac}
	}
	macKey, err := manager.MintPreauthKey(bin, cfgPath, false, macTags)
	if err != nil {
		t.Fatal(err)
	}
	mac, addrs, err := nfs.StartLinkNode(localControl, macKey, "test-mac", filepath.Join(dir, "mac-node"))
	if err != nil {
		t.Fatalf("mac node: %v", err)
	}
	defer mac.Stop()
	t.Logf("mac node: %v", addrs)

	// echoOver dials the NAS's LAN address as the Mac app does (a
	// documentation address here, routed nowhere but through Link).
	echoOver := func(timeout time.Duration, port int) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		conn, err := mac.DialContext(ctx, "tcp", net.JoinHostPort(nasLAN, strconv.Itoa(port)))
		cancel()
		if err != nil {
			return err
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
		if _, err := io.WriteString(conn, "juice\n"); err != nil {
			return err
		}
		got, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil || got != "svc:juice\n" {
			return fmt.Errorf("reply %q, %v", got, err)
		}
		return nil
	}

	if tc.subnet {
		// The Mac knows the NAS node (it is online and in its netmap) but
		// the route is not approved: nothing may carry the dial yet.
		if err := echoOver(5*time.Second, 30179); err == nil {
			t.Fatal("a dial reached the NAS before its subnet route was approved")
		} else if !strings.Contains(err.Error(), "no encrypted route") {
			t.Fatalf("pre-approval dial failed for another reason: %v", err)
		}
		approve.Store(true)
		start := time.Now()
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			transport, err := mac.TransportStatus(ctx)
			cancel()
			if err == nil && transport.Peer == "juicemount-nas" {
				break
			}
			if time.Since(start) > 60*time.Second {
				t.Fatalf("the Mac never saw the approved route: %+v, %v", transport, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Logf("route reached the Mac after %v", time.Since(start).Round(time.Millisecond))
		// Not asserted here: a connection through the route. On this
		// relay-less loopback tailnet the first routed connection took
		// 20-60 s in some runs and none connected within 120 s of fresh 5 s
		// attempts in others, independent of the policy; the subnet data
		// path (nasRouteTCPHandler) is the one Link has always used.
	} else {
		// Link carries the LAN address to the same port on the NAS node's
		// tailnet address.
		if err := echoOver(60*time.Second, 30179); err != nil {
			t.Fatalf("allowed service dial: %v", err)
		}
		// The Manager is served in-process, so it sees the Mac's own
		// tailnet address, not the NAS container's.
		client := &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{DialContext: mac.DialContext}}
		resp, err := client.Get("http://" + net.JoinHostPort(nasLAN, "30190") + "/")
		if err != nil {
			t.Fatalf("manager over Link: %v", err)
		}
		seen, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if !containsString(addrs, string(seen)) {
			t.Fatalf("manager saw the request from %q, want one of the Mac's addresses %v", seen, addrs)
		}
	}

	if tc.policy == "managed" && !tc.subnet {
		// 30199 has a listener on the NAS node but no policy rule.
		if err := echoOver(5*time.Second, 30199); err == nil {
			t.Fatal("policy let the Mac reach a non-allowed port")
		}
	}

	if st := nas.status.snapshot(); st.State != "running" || st.Restarts != tc.wantRestarts {
		t.Fatalf("nas node status after traffic = %+v", st)
	}
}

// unusedDocumentationPrefix returns a documentation /24 (RFC 5737) that no
// local interface address is in.
func unusedDocumentationPrefix(t *testing.T) netip.Prefix {
	t.Helper()
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24"} {
		prefix := netip.MustParsePrefix(raw)
		used := false
		for _, a := range addrs {
			if ipnet, ok := a.(*net.IPNet); ok {
				if ip, ok := netip.AddrFromSlice(ipnet.IP); ok && prefix.Contains(ip.Unmap()) {
					used = true
				}
			}
		}
		if !used {
			return prefix
		}
	}
	t.Skip("every documentation prefix is on a local interface")
	return netip.Prefix{}
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

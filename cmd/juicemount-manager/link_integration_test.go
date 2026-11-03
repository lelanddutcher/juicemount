package main

// End-to-end Link test against a real Headscale and two in-process tsnet
// nodes on loopback: the NAS node (runNASNode, as the Manager runs it) and a
// Mac (nfs.StartLinkNode, as the app runs it).
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
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
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

// TestLinkIntegrationNASServiceExposureAndPolicy is a fresh install: the NAS
// node registers with a tag:jm-nas key.
func TestLinkIntegrationNASServiceExposureAndPolicy(t *testing.T) {
	runLinkIntegration(t, []string{manager.LinkTagNAS}, 0)
}

// TestLinkIntegrationUntaggedNASNodeIsTagged is an upgrade: the NAS node's
// identity predates tagged keys, so its key carries no tag and the node is
// tagged server-side once it is up, then rejoins once (the one restart) to
// receive the policy's rules for its new tag.
func TestLinkIntegrationUntaggedNASNodeIsTagged(t *testing.T) {
	runLinkIntegration(t, nil, 1)
}

func runLinkIntegration(t *testing.T, nasKeyTags []string, wantNASRestarts int) {
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

	// Backend services on the "container network" (loopback here).
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
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	echoAddr := echo.Addr().String()
	t.Setenv("JM_NET_SERVER_URL", publicURL)
	t.Setenv("JM_NET_TLS_CERT", "")
	t.Setenv("JM_NET_TLS_KEY", "")
	t.Setenv("JM_NET_HEADSCALE", "on")
	t.Setenv("JM_NET_POLICY", "")
	t.Setenv("JM_NET_NAS_ROUTE", "")
	t.Setenv("JM_HEADSCALE_USER", "")
	// The policy allows only 30179; the NAS node below also serves 30199.
	t.Setenv("JM_NET_NAS_SERVICES", "30179="+echoAddr)

	hs := newHeadscaleSupervisor()
	hs.listen = listen
	hs.Start()
	defer hs.Stop()
	waitForState(t, "headscale", hs.status, 90*time.Second)

	nasTags := []string{manager.LinkTagNAS}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	nasCfg := nasNodeConfig{
		ControlURL: localControl,
		Hostname:   "juicemount-nas",
		Dir:        filepath.Join(dir, "nas-node"),
		Services: []nasService{
			{Port: 30179, Target: echoAddr},
			{Port: 30199, Target: echoAddr},
		},
		MintKey: func() (string, error) {
			return manager.MintPreauthKey(bin, cfgPath, false, nasKeyTags)
		},
		EnsureTags: func() (bool, error) {
			return manager.EnsureNodeTags(bin, cfgPath, "juicemount-nas", nasTags)
		},
		Dial:        dialer.DialContext,
		UpTimeout:   60 * time.Second,
		HealthEvery: time.Second,
	}
	nas := newSupervisor("nas node", func(stop <-chan struct{}, ready func(string)) error {
		return runNASNode(nasCfg, stop, ready)
	})
	nas.Start()
	defer nas.Stop()
	waitForState(t, "nas node", nas.status, 90*time.Second)
	t.Logf("nas node: %s", nas.status.snapshot().Detail)

	out, err := exec.Command(bin, "--config", cfgPath, "nodes", "list", "-o", "json").Output()
	if err != nil {
		t.Fatalf("nodes list: %v", err)
	}
	var nodes []manager.HSNode
	if err := json.Unmarshal(out, &nodes); err != nil {
		t.Fatalf("nodes list json: %v\n%s", err, out)
	}
	if len(nodes) != 1 || nodes[0].GivenName != "juicemount-nas" || !reflect.DeepEqual(nodes[0].Tags, nasTags) {
		t.Fatalf("registered nodes = %+v, want the NAS node tagged %v", nodes, nasTags)
	}

	macKey, err := manager.MintPreauthKey(bin, cfgPath, false, []string{manager.LinkTagMac})
	if err != nil {
		t.Fatal(err)
	}
	mac, addrs, err := nfs.StartLinkNode(localControl, macKey, "test-mac", filepath.Join(dir, "mac-node"))
	if err != nil {
		t.Fatalf("mac node: %v", err)
	}
	defer mac.Stop()
	t.Logf("mac node: %v", addrs)

	// The Mac keeps dialing the NAS's LAN address (a documentation address
	// here, routed nowhere); Link carries it to the same port on the NAS
	// node's tailnet address.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	conn, err := mac.DialContext(ctx, "tcp", "192.0.2.10:30179")
	cancel()
	if err != nil {
		t.Fatalf("allowed service dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	if _, err := io.WriteString(conn, "juice\n"); err != nil {
		t.Fatal(err)
	}
	got, err := bufio.NewReader(conn).ReadString('\n')
	_ = conn.Close()
	if err != nil || got != "svc:juice\n" {
		t.Fatalf("allowed service reply = %q, %v", got, err)
	}

	// 30199 has a listener on the NAS node but no policy rule.
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	conn, err = mac.DialContext(ctx, "tcp", "192.0.2.10:30199")
	cancel()
	if err == nil {
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		_, _ = io.WriteString(conn, "juice\n")
		reply, readErr := bufio.NewReader(conn).ReadString('\n')
		_ = conn.Close()
		if readErr == nil {
			t.Fatalf("policy let the Mac reach a non-allowed port: reply %q", reply)
		}
	}

	if st := nas.status.snapshot(); st.State != "running" || st.Restarts != wantNASRestarts {
		t.Fatalf("nas node status after traffic = %+v", st)
	}
}

package main

// Link integration cases from the round-2 review, against the pinned
// Headscale (see link_integration_test.go for JM_TEST_HEADSCALE_BIN): a node
// asking for tag:jm-nas itself, an operator policy that does not define the
// tag, and an install switched from subnet to service exposure.

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/manager"
	"github.com/lelanddutcher/juicemount/nfs"
	"tailscale.com/ipn"
	"tailscale.com/tsnet"
)

// testControl is a running Headscale for one test.
type testControl struct {
	bin, dir, cfgPath, listen string
	hs                        *headscaleSupervisor
}

func (c *testControl) url() string { return "http://" + c.listen }

func (c *testControl) nodes(t *testing.T) []manager.HSNode {
	t.Helper()
	out, err := exec.Command(c.bin, "--config", c.cfgPath, "nodes", "list", "-o", "json").Output()
	if err != nil {
		t.Fatalf("nodes list: %v", err)
	}
	var nodes []manager.HSNode
	if err := json.Unmarshal(out, &nodes); err != nil {
		t.Fatalf("nodes list json: %v\n%s", err, out)
	}
	return nodes
}

// startTestControl starts Headscale the way the Manager does. env is the
// Link environment on top of an embedded control server and NAS node with
// no exposure chosen; operatorPolicy, when set, is written to config.yaml
// as the operator's own policy.
func startTestControl(t *testing.T, operatorPolicy string, env map[string]string) *testControl {
	t.Helper()
	bin := os.Getenv("JM_TEST_HEADSCALE_BIN")
	if bin == "" {
		t.Skip("JM_TEST_HEADSCALE_BIN is not set; see link_integration_test.go")
	}
	c := &testControl{bin: bin, dir: t.TempDir()}
	oldBin, oldDir := hsBinary, hsDataDir
	t.Cleanup(func() { hsBinary, hsDataDir = oldBin, oldDir })
	hsBinary, hsDataDir = bin, c.dir
	const publicURL = "https://link.example.com:30193"
	c.listen = net.JoinHostPort("127.0.0.1", strconv.Itoa(freeLoopbackPort(t)))
	derpPath := filepath.Join(c.dir, "derp.yaml")
	writeDERPMapYAML(t, derpPath, unusedDERPMap(t))
	c.cfgPath = filepath.Join(c.dir, "config.yaml")
	cfg := renderHeadscaleConfig(headscaleConfigParams{
		ServerURL:     publicURL,
		Listen:        c.listen,
		MetricsListen: net.JoinHostPort("127.0.0.1", strconv.Itoa(freeLoopbackPort(t))),
		GRPCListen:    net.JoinHostPort("127.0.0.1", strconv.Itoa(freeLoopbackPort(t))),
		DataDir:       c.dir,
		DERPPaths:     []string{derpPath},
	})
	if operatorPolicy != "" {
		p := filepath.Join(c.dir, "operator.hujson")
		if err := os.WriteFile(p, []byte(operatorPolicy), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg += "policy:\n  mode: file\n  path: " + p + "\n"
	}
	if err := os.WriteFile(c.cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	base := map[string]string{
		"JM_NET_SERVER_URL": publicURL, "JM_NET_TLS_CERT": "", "JM_NET_TLS_KEY": "",
		"JM_NET_HEADSCALE": "on", "JM_NET_NAS_NODE": "on", "JM_NET_NAS_CONTROL_URL": c.url(),
		"JM_NET_NAS_HOSTNAME": "", "JM_NET_NAS_EXPOSURE": "", "JM_NET_NAS_ROUTE": "", "JM_NET_NAS_LAN_ADDR": "",
		"JM_NET_POLICY": "", "JM_HEADSCALE_USER": "", "JM_NET_NAS_SERVICES": "",
	}
	for k, v := range env {
		base[k] = v
	}
	for k, v := range base {
		t.Setenv(k, v)
	}
	c.hs = newHeadscaleSupervisor()
	c.hs.listen = c.listen
	c.hs.Start()
	t.Cleanup(func() { c.hs.Stop() })
	waitForState(t, "headscale", c.hs.status, 90*time.Second)
	return c
}

// TestLinkIntegrationNodeCannotTagItselfNAS: under the Manager's policy no
// tag has an owner, so an untagged Link-user node (a Mac paired before keys
// were tagged) that asks for tag:jm-nas does not get it.
func TestLinkIntegrationNodeCannotTagItselfNAS(t *testing.T) {
	c := startTestControl(t, "", nil)
	if _, err := manager.RecordLinkExposureDefault(c.cfgPath); err != nil {
		t.Fatal(err)
	}
	key, err := manager.MintPreauthKey(c.bin, c.cfgPath, false, nil) // a key from before tagging
	if err != nil {
		t.Fatal(err)
	}
	s := &tsnet.Server{Hostname: "legacy-mac", ControlURL: c.url(), AuthKey: key, Dir: filepath.Join(c.dir, "legacy"), Logf: func(string, ...any) {}}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := s.Up(ctx); err != nil {
		t.Fatal(err)
	}
	lc, err := s.LocalClient()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lc.EditPrefs(ctx, &ipn.MaskedPrefs{Prefs: ipn.Prefs{AdvertiseTags: []string{manager.LinkTagNAS}}, AdvertiseTagsSet: true}); err != nil {
		t.Logf("EditPrefs refused the tag request: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		nodes := c.nodes(t)
		if len(nodes) != 1 {
			t.Fatalf("nodes = %+v, want the one Mac", nodes)
		}
		if containsString(nodes[0].Tags, manager.LinkTagNAS) {
			t.Fatalf("an untagged node gave itself %s: %+v", manager.LinkTagNAS, nodes[0])
		}
		time.Sleep(time.Second)
	}
}

// TestLinkIntegrationOperatorPolicyMintsTaggedKeys: an operator policy that
// does not define the Link tags still lets the Manager mint tagged keys, so
// a fresh NAS node registers with tag:jm-nas (only an identity from before
// tagged keys needs the tag defined: TestLinkIntegrationOperatorPolicyRefusingNASTag).
func TestLinkIntegrationOperatorPolicyMintsTaggedKeys(t *testing.T) {
	c := startTestControl(t, `{"tagOwners": {"tag:other": []}, "acls": [{"action": "accept", "src": ["*"], "dst": ["*:*"]}]}`, nil)
	for _, tag := range []string{manager.LinkTagNAS, manager.LinkTagMac} {
		if _, err := manager.MintPreauthKey(c.bin, c.cfgPath, false, []string{tag}); err != nil {
			t.Fatalf("mint %s under an operator policy: %v", tag, err)
		}
	}
}

// TestLinkIntegrationSwitchSubnetToServices*: an install switched from
// subnet to service exposure (the CHANGELOG's upgrade path) keeps its node
// identity, whose old route stays approved but is no longer advertised; a
// current Mac reaches the NAS on its tailnet address.
func TestLinkIntegrationSwitchSubnetToServicesManaged(t *testing.T) {
	runSwitchSubnetToServices(t, "")
}

func TestLinkIntegrationSwitchSubnetToServicesPolicyOff(t *testing.T) {
	runSwitchSubnetToServices(t, "off")
}

func runSwitchSubnetToServices(t *testing.T, policy string) {
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
				if line, err := bufio.NewReader(conn).ReadString('\n'); err == nil {
					_, _ = io.WriteString(conn, "svc:"+line)
				}
			}()
		}
	}()
	echoAddr := echo.Addr().String()
	lan := unusedDocumentationPrefix(t)
	nasLAN := lan.Addr().Next().Next().String()
	c := startTestControl(t, "", map[string]string{
		"JM_NET_NAS_EXPOSURE": "subnet", "JM_NET_NAS_ROUTE": lan.String(),
		"JM_NET_POLICY": policy, "JM_NET_NAS_SERVICES": "30179=" + echoAddr,
	})
	udp, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	udpPort := uint16(udp.LocalAddr().(*net.UDPAddr).Port)
	_ = udp.Close()
	startNAS := func() *supervisor {
		t.Helper()
		cfg, err := nasNodeConfigFromEnv(c.cfgPath, http.NotFoundHandler())
		if err != nil {
			t.Fatal(err)
		}
		cfg.UDPPort = udpPort
		cfg.HealthEvery = time.Second
		cfg.Dial = func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, echoAddr)
		}
		var s *supervisor
		cfg.Identity = func(key string) { s.status.setNodeKey(key) }
		s = newSupervisor("nas node", func(stop <-chan struct{}, ready func(string)) error { return runNASNode(cfg, stop, ready) })
		s.Start()
		waitForState(t, "nas node", s.status, 90*time.Second)
		return s
	}

	// The existing install: subnet exposure, its route approved.
	nas := startNAS()
	deadline := time.Now().Add(90 * time.Second)
	for {
		nodes := c.nodes(t)
		if len(nodes) == 1 && containsString(nodes[0].ApprovedRoutes, lan.String()) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("subnet route never approved: %+v", nodes)
		}
		time.Sleep(time.Second)
	}
	key := nas.status.snapshot().NodeKey
	nas.Stop()

	// The operator switches to services and restarts the Manager.
	t.Setenv("JM_NET_NAS_EXPOSURE", "services")
	t.Setenv("JM_NET_NAS_ROUTE", "")
	c.hs.Stop()
	c.hs = newHeadscaleSupervisor()
	c.hs.listen = c.listen
	c.hs.Start()
	waitForState(t, "headscale", c.hs.status, 90*time.Second)
	nas = startNAS()
	defer nas.Stop()
	if got := nas.status.snapshot().NodeKey; got != key {
		t.Fatalf("the NAS node changed identity across the switch: %s -> %s", key, got)
	}
	nodes := c.nodes(t)
	if len(nodes) != 1 || !containsString(nodes[0].Tags, manager.LinkTagNAS) {
		t.Fatalf("after the switch nodes = %+v, want the NAS node tagged %s", nodes, manager.LinkTagNAS)
	}

	var macTags []string
	if manager.LinkPolicyManaged(c.cfgPath) {
		macTags = []string{manager.LinkTagMac}
	}
	macKey, err := manager.MintPreauthKey(c.bin, c.cfgPath, false, macTags)
	if err != nil {
		t.Fatal(err)
	}
	mac, _, err := nfs.StartLinkNode(c.url(), macKey, "test-mac", filepath.Join(c.dir, "mac"))
	if err != nil {
		t.Fatal(err)
	}
	defer mac.Stop()
	start := time.Now()
	var lastErr error
	for time.Since(start) < 90*time.Second {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		conn, err := mac.DialContext(ctx, "tcp", net.JoinHostPort(nasLAN, "30179"))
		cancel()
		if err == nil {
			_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
			_, _ = io.WriteString(conn, "juice\n")
			got, rerr := bufio.NewReader(conn).ReadString('\n')
			conn.Close()
			if got == "svc:juice\n" {
				t.Logf("the Mac reached the NAS after %v", time.Since(start).Round(time.Second))
				return
			}
			err = rerr
		}
		lastErr = err
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("after switching to services the Mac never reached the NAS: %v", lastErr)
}

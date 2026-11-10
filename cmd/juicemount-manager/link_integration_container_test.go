package main

// End-to-end test of the NAS node in its own container
// (JM_NET_NAS_NODE=container), against a real Headscale, as an upgrade:
//
//  1. today's layout: the NAS node runs in the Manager and registers;
//  2. the update: the Manager serves the node socket, the node runs as
//     `--link-node` would (newLinkNodeSupervisor), takes over the same
//     identity, and a Mac reaches a service and the Manager through it,
//     the Manager seeing the Mac's own tailnet address;
//  3. the rollback: the in-Manager node starts again with the same identity.
//
// Needs JM_TEST_HEADSCALE_BIN, as link_integration_test.go.

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/manager"
	"github.com/lelanddutcher/juicemount/nfs"
)

func TestLinkIntegrationNodeContainer(t *testing.T) {
	bin := os.Getenv("JM_TEST_HEADSCALE_BIN")
	if bin == "" {
		t.Skip("JM_TEST_HEADSCALE_BIN is not set; see link_integration_test.go for the pinned Headscale binary")
	}
	dir := t.TempDir()
	oldBin, oldDir := hsBinary, hsDataDir
	t.Cleanup(func() { hsBinary, hsDataDir = oldBin, oldDir })
	hsBinary, hsDataDir = bin, dir

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

	const publicURL = "https://link.example.com:30193"
	derpPath := filepath.Join(dir, "derp.yaml")
	cfgPath := filepath.Join(dir, "config.yaml")
	lan := unusedDocumentationPrefix(t)
	nasLAN := lan.Addr().Next().Next().String()
	for k, v := range map[string]string{
		"JM_NET_SERVER_URL": publicURL, "JM_NET_TLS_CERT": "", "JM_NET_TLS_KEY": "",
		"JM_NET_HEADSCALE": "on", "JM_NET_NAS_NODE": manager.NASNodeInManager,
		"JM_NET_NAS_HOSTNAME": "", "JM_NET_NAS_EXPOSURE": "", "JM_NET_NAS_ROUTE": "",
		"JM_NET_POLICY": "", "JM_HEADSCALE_USER": "", "JM_NET_NAS_LAN_ADDR": "",
		"JM_NET_NAS_SERVICES": "30179=" + echoAddr + ",30190=manager",
	} {
		t.Setenv(k, v)
	}
	hs, listen := startLoopbackHeadscale(t, func(listen string) {
		writeDERPMapYAML(t, derpPath, localDERPMap(t, dir, listen))
		cfg := renderHeadscaleConfig(headscaleConfigParams{
			ServerURL: publicURL, Listen: listen, MetricsListen: loopbackAnyPort, GRPCListen: loopbackAnyPort,
			DataDir: dir, DERPPaths: []string{derpPath},
		})
		if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("JM_NET_NAS_CONTROL_URL", "http://"+listen)
		if _, err := manager.RecordLinkExposureDefault(cfgPath); err != nil {
			t.Fatal(err)
		}
	})
	defer hs.Stop()
	localControl := "http://" + listen

	udp, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	udpPort := udp.LocalAddr().(*net.UDPAddr).Port
	_ = udp.Close()
	t.Setenv("JM_NET_NAS_UDP_PORT", strconv.Itoa(udpPort))

	// The Manager's API answers with the address it saw.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		_, _ = io.WriteString(w, host)
	})
	echoDial := func(ctx context.Context, network, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, network, echoAddr)
	}

	runInManager := func() (*supervisor, string) {
		t.Helper()
		cfg, err := nasNodeConfigFromEnv(cfgPath, handler)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Dial = echoDial
		cfg.HealthEvery = time.Second
		var nas *supervisor
		cfg.Identity = func(key string) { nas.status.setNodeKey(key) }
		nas = newSupervisor("nas node", func(stop <-chan struct{}, ready func(string)) error {
			return runNASNode(cfg, stop, ready)
		})
		nas.Start()
		waitForState(t, "in-Manager nas node", nas.status, 90*time.Second)
		return nas, nas.status.snapshot().NodeKey
	}
	nasNodes := func() []manager.HSNode {
		t.Helper()
		out, err := exec.Command(bin, "--config", cfgPath, "nodes", "list", "-o", "json").Output()
		if err != nil {
			t.Fatalf("nodes list: %v", err)
		}
		var nodes, nas []manager.HSNode
		if err := json.Unmarshal(out, &nodes); err != nil {
			t.Fatal(err)
		}
		for _, n := range nodes {
			if n.Name == manager.DefaultNasHostname {
				nas = append(nas, n)
			}
		}
		return nas
	}

	// 1. Today's layout.
	inManager, key1 := runInManager()
	inManager.Stop()
	t.Logf("in-Manager node: %s", key1)

	// 2. The update. The node container dials control at another URL (the
	// host's published port instead of the Manager's own listener); a
	// different URL to the same server keeps the identity.
	t.Setenv("JM_NET_NAS_NODE", manager.NASNodeInContainer)
	_, controlPort, _ := net.SplitHostPort(listen)
	t.Setenv("JM_NET_NAS_CONTROL_URL", "http://localhost:"+controlPort)
	sockDir, err := os.MkdirTemp("", "jmli")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(sockDir)
	broker := newLinkNodeBroker(cfgPath, hs, handler)
	stopBroker := make(chan struct{})
	brokerDone := make(chan struct{})
	go func() { defer close(brokerDone); serveLinkNodeSockets(sockDir, broker, stopBroker) }()
	defer func() { close(stopBroker); <-brokerDone }()

	client := newLinkNodeClient(sockDir)
	nodeDir := filepath.Join(dir, "link-node-state")
	node := newLinkNodeSupervisor(client, nodeDir)
	node.Start()
	waitForState(t, "container nas node", node.status, 90*time.Second)
	// Drop the Manager's copy, as on an install that started with the node
	// container: the node's reports must give it back, or a rollback would
	// register a second NAS node.
	managerCopy := filepath.Join(dir, "nas-node", linkNodeStateFile)
	if err := os.Remove(managerCopy); err != nil {
		t.Fatal(err)
	}
	stopReport := make(chan struct{})
	reportDone := make(chan struct{})
	go func() { defer close(reportDone); reportLinkNode(client, node.status, nodeDir, "", stopReport) }()
	if key2 := node.status.snapshot().NodeKey; key2 != key1 {
		t.Fatalf("the node container registered as %s, not the in-Manager identity %s", key2, key1)
	}
	if nodes := nasNodes(); len(nodes) != 1 || nodes[0].NodeKey != key1 || !reflect.DeepEqual(nodes[0].Tags, []string{manager.LinkTagNAS}) {
		t.Fatalf("NAS nodes after the update = %+v, want one, tagged, with key %s", nodes, key1)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		st := broker.snapshot()
		if st.State == "running" && st.NodeKey == key1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the Manager's view of the node container: %+v", st)
		}
		time.Sleep(200 * time.Millisecond)
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
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	conn, err := mac.DialContext(ctx, "tcp", net.JoinHostPort(nasLAN, "30179"))
	cancel()
	if err != nil {
		t.Fatalf("service dial through the node container: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	_, _ = io.WriteString(conn, "juice\n")
	got, err := bufio.NewReader(conn).ReadString('\n')
	conn.Close()
	if err != nil || got != "svc:juice\n" {
		t.Fatalf("service reply %q, %v", got, err)
	}
	macV4 := ""
	for _, a := range addrs {
		if ip, err := netip.ParseAddr(a); err == nil && ip.Is4() {
			macV4 = a
		}
	}
	hc := &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{DialContext: mac.DialContext}}
	resp, err := hc.Get("http://" + net.JoinHostPort(nasLAN, "30190") + "/")
	if err != nil {
		t.Fatalf("Manager over the node container: %v", err)
	}
	seen, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(seen) != macV4 {
		t.Fatalf("the Manager saw %q, want the Mac's tailnet address %s", seen, macV4)
	}

	// The node's control API is not reachable from the Mac: every
	// /link-node/ path through tailnet port 30190 is a 404, and no key is
	// minted or identity handed out.
	keysBefore, _ := exec.Command(bin, "--config", cfgPath, "preauthkeys", "list", "-o", "json").Output()
	for _, p := range []string{"/link-node/v1/identity", "/link-node/v1/key", "/link-node/v1/config", "/link-node/v1/ensure-tags", "/link-node/v1/approve-route", "/link-node/v1/conflicts", "/link-node/v1/status"} {
		for _, m := range []string{http.MethodGet, http.MethodPost} {
			req, _ := http.NewRequest(m, "http://"+net.JoinHostPort(nasLAN, "30190")+p, strings.NewReader(`{"node_key":"x"}`))
			resp, err := hc.Do(req)
			if err != nil {
				t.Fatalf("%s %s from the Mac: %v", m, p, err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound || strings.Contains(string(body), "tailscaled_state") || strings.Contains(string(body), "hskey") {
				t.Fatalf("%s %s from the Mac: %d %q, want 404", m, p, resp.StatusCode, body)
			}
		}
	}
	if keysAfter, _ := exec.Command(bin, "--config", cfgPath, "preauthkeys", "list", "-o", "json").Output(); string(keysAfter) != string(keysBefore) {
		t.Fatal("a request from the Mac changed the pre-auth keys")
	}
	// Even on the control socket, the Mac's node key is not the NAS node's.
	macNode := ""
	if out, err := exec.Command(bin, "--config", cfgPath, "nodes", "list", "-o", "json").Output(); err == nil {
		var nodes []manager.HSNode
		_ = json.Unmarshal(out, &nodes)
		for _, n := range nodes {
			if n.Name == "test-mac" {
				macNode = n.NodeKey
			}
		}
	}
	if macNode == "" {
		t.Fatal("the Mac is not listed")
	}
	err = client.call(context.Background(), http.MethodPost, "ensure-tags", linkNodeKeyRequest{NodeKey: macNode}, &linkNodeTagsResponse{})
	if he := (*linkNodeHTTPError)(nil); !errors.As(err, &he) || he.Code != http.StatusForbidden {
		t.Fatalf("ensure-tags for the Mac's key: %v, want 403", err)
	}

	node.Stop()
	close(stopReport)
	<-reportDone
	_ = client.call(context.Background(), http.MethodPost, "status", node.status.snapshot(), nil)
	if st := broker.snapshot(); st.State != "stopped" {
		t.Fatalf("after the node container stops the Manager shows %+v", st)
	}

	// The node gave its identity back to the Manager's copy.
	if got, err := os.ReadFile(managerCopy); err != nil || len(got) == 0 {
		t.Fatalf("the Manager has no copy of the node's identity: %v", err)
	}

	// 3. The rollback: the Manager's copy is still there.
	t.Setenv("JM_NET_NAS_NODE", manager.NASNodeInManager)
	t.Setenv("JM_NET_NAS_CONTROL_URL", localControl)
	back, key3 := runInManager()
	defer back.Stop()
	if key3 != key1 {
		t.Fatalf("after the rollback the node is %s, want %s", key3, key1)
	}
	if nodes := nasNodes(); len(nodes) != 1 {
		t.Fatalf("NAS nodes after the rollback = %+v, want one", nodes)
	}
	if !strings.HasPrefix(key1, "nodekey:") {
		t.Fatalf("odd node key %q", key1)
	}
}

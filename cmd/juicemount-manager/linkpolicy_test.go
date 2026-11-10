package main

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/manager"
)

func TestParseNASServices(t *testing.T) {
	got, err := parseNASServices(defaultNASServices)
	if err != nil {
		t.Fatal(err)
	}
	want := []nasService{
		{Port: 30151, Target: "minio:9000"},
		{Port: 30179, Target: "respproxy:6379"},
		{Port: 30190, Target: nasManagerTarget},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("default services = %+v, want %+v", got, want)
	}
	if got, err := parseNASServices("30190=127.0.0.1:8080"); err != nil || got[0].Target != "127.0.0.1:8080" {
		t.Fatalf("forwarded Manager target = %+v, %v", got, err)
	}
	for _, bad := range []string{"", "30179", "0=redis:6379", "70000=redis:6379", "30179=redis", "30179=:6379", "1=a:1,1=b:2", "30190=managerx"} {
		if _, err := parseNASServices(bad); err == nil {
			t.Errorf("parseNASServices(%q) accepted", bad)
		}
	}
}

type testPolicy struct {
	TagOwners map[string][]string `json:"tagOwners"`
	ACLs      []struct {
		Action string   `json:"action"`
		Src    []string `json:"src"`
		Dst    []string `json:"dst"`
	} `json:"acls"`
}

// TestLinkPolicyAllowsOnlyServicePorts: Macs and farm nodes reach the
// JuiceMount ports on the NAS node and nothing else; no rule reaches the
// router, the rest of the LAN, or other Macs. No tag has an owner, so no
// node can give itself one.
func TestLinkPolicyAllowsOnlyServicePorts(t *testing.T) {
	services, _ := parseNASServices(defaultNASServices)
	raw, err := linkPolicy("jm", services, "", "")
	if err != nil {
		t.Fatal(err)
	}
	var p testPolicy
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("policy is not JSON: %v\n%s", err, raw)
	}
	if len(p.ACLs) != 1 {
		t.Fatalf("policy has %d rules, want 1:\n%s", len(p.ACLs), raw)
	}
	rule := p.ACLs[0]
	if rule.Action != "accept" ||
		!reflect.DeepEqual(rule.Src, []string{"tag:jm-mac", "tag:jm-farm", "jm@"}) ||
		!reflect.DeepEqual(rule.Dst, []string{"tag:jm-nas:30151,30179,30190"}) {
		t.Fatalf("rule = %+v", rule)
	}
	for _, tag := range []string{"tag:jm-mac", "tag:jm-nas", "tag:jm-farm"} {
		if owners, ok := p.TagOwners[tag]; !ok || owners == nil || len(owners) != 0 {
			t.Fatalf("tagOwners[%s] = %v (defined %v), want defined and empty", tag, owners, ok)
		}
	}
	if strings.Contains(string(raw), "*") {
		t.Fatalf("policy contains a wildcard:\n%s", raw)
	}

	raw, err = linkPolicy("jm", services, "192.0.2.7/24", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	if want := []string{"tag:jm-nas:30151,30179,30190", "192.0.2.0/24:30151,30179,30190"}; !reflect.DeepEqual(p.ACLs[0].Dst, want) {
		t.Fatalf("subnet dst = %v, want %v", p.ACLs[0].Dst, want)
	}
	if _, err := linkPolicy("jm", services, "not-a-prefix", ""); err == nil {
		t.Fatal("bad subnet accepted")
	}
	// The NAS's own address, when known, replaces the route as destination.
	raw, err = linkPolicy("jm", services, "192.0.2.7/24", "192.0.2.10/32")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	if want := []string{"tag:jm-nas:30151,30179,30190", "192.0.2.10/32:30151,30179,30190"}; !reflect.DeepEqual(p.ACLs[0].Dst, want) {
		t.Fatalf("subnet dst with the NAS address = %v, want %v", p.ACLs[0].Dst, want)
	}
	for _, bad := range []string{"198.51.100.1/32", "192.0.2.0/25", "nas"} {
		if _, err := linkPolicy("jm", services, "192.0.2.0/24", bad); err == nil {
			t.Fatalf("NAS address %q accepted", bad)
		}
	}
}

// TestLinkComposeOverlayMatchesCodeDefaults: the opt-in overlay publishes
// the listener the code uses, runs the NAS node in its own host-network
// container with the code's socket, state directory and WireGuard port (and
// publishes no UDP port for it), and the base compose keeps Link off without
// claiming either host port.
func TestLinkComposeOverlayMatchesCodeDefaults(t *testing.T) {
	overlay, err := os.ReadFile("../../server/docker-compose.link.yml")
	if err != nil {
		t.Fatal(err)
	}
	_, hsPort, _ := net.SplitHostPort(hsListen)
	udp := strconv.Itoa(defaultNASUDPPort)
	for _, want := range []string{
		`- "30193:` + hsPort + `"`,
		`JM_NET_NAS_UDP_PORT: "${JM_NET_NAS_UDP_PORT:-` + udp + `}"`,
		`JM_NET_HEADSCALE: "on"`,
		`JM_NET_NAS_NODE: "` + manager.NASNodeInContainer + `"`,
		`JM_NET_SERVER_URL: "${JM_NET_SERVER_URL:?`,
		`network_mode: host`,
		`entrypoint: ["/usr/local/bin/juicemount-manager", "--link-node"]`,
		`juicemount-link-run:` + filepath.Dir(defaultLinkNodeSocket),
		`juicemount-link-state:` + defaultLinkNodeStateDir,
		`- ALL`,
		`read_only: true`,
	} {
		if !strings.Contains(string(overlay), want) {
			t.Errorf("docker-compose.link.yml lacks %s", want)
		}
	}
	if strings.Contains(string(overlay), "/udp") || strings.Contains(string(overlay), "cap_add") {
		t.Error("docker-compose.link.yml publishes a UDP port or adds a capability")
	}
	base, err := os.ReadFile("../../server/docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`JM_NET_HEADSCALE: "off"`, `JM_NET_NAS_NODE: "off"`} {
		if !strings.Contains(string(base), want) {
			t.Errorf("base compose lacks %s", want)
		}
	}
	for _, line := range strings.Split(string(base), "\n") {
		mapping := strings.TrimSpace(line)
		if !strings.HasPrefix(mapping, `- "`) {
			continue
		}
		if strings.Contains(mapping, ":"+hsPort+`"`) || strings.Contains(mapping, udp) {
			t.Errorf("base compose publishes a Link port while Link is off: %s", mapping)
		}
	}
}

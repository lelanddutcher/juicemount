package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/manager"
)

func setNASNodeEnv(t *testing.T) string {
	t.Helper()
	t.Setenv("JM_NET_HEADSCALE", "on")
	t.Setenv("JM_NET_NAS_NODE", "on")
	t.Setenv("JM_NET_TLS_CERT", "")
	t.Setenv("JM_NET_TLS_KEY", "")
	t.Setenv("JM_NET_CONTROL_URL", "")
	t.Setenv("JM_NET_NAS_CONTROL_URL", "")
	t.Setenv("JM_NET_POLICY", "")
	t.Setenv("JM_NET_NAS_ROUTE", "")
	t.Setenv("JM_NET_NAS_EXPOSURE", "")
	t.Setenv("JM_NET_NAS_SERVICES", "")
	t.Setenv("JM_NET_NAS_LAN_ADDR", "")
	return filepath.Join(t.TempDir(), "config.yaml")
}

// TestNASNodeTagFollowsExposure: tag:jm-nas is how a Mac finds a NAS node
// that serves its ports, so service exposure tags the node under every
// policy (off and operator-owned included), and subnet exposure leaves it
// untagged so a Mac waits for the route rather than dialing the node's
// tailnet address before the route converges.
func TestNASNodeTagFollowsExposure(t *testing.T) {
	for _, tc := range []struct {
		name       string
		policy     string // JM_NET_POLICY
		config     string // persisted config.yaml ("" = not generated yet)
		route      string // JM_NET_NAS_ROUTE
		upgrade    bool   // a NAS node state directory already exists
		wantTags   []string
		wantSubnet string
	}{
		{name: "managed services", wantTags: []string{manager.LinkTagNAS}},
		{name: "policy off services", policy: "off", wantTags: []string{manager.LinkTagNAS}},
		{name: "operator policy services", config: "policy:\n  path: /etc/acl.hujson\n", wantTags: []string{manager.LinkTagNAS}},
		{name: "managed subnet", route: "192.0.2.0/24", wantSubnet: "192.0.2.0/24"},
		{name: "policy off subnet", policy: "off", route: "192.0.2.0/24", wantSubnet: "192.0.2.0/24"},
		{name: "upgrade keeps the subnet route", upgrade: true, wantSubnet: manager.NasLanSubnet},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfgPath := setNASNodeEnv(t)
			t.Setenv("JM_NET_POLICY", tc.policy)
			t.Setenv("JM_NET_NAS_ROUTE", tc.route)
			if tc.config != "" {
				if err := os.WriteFile(cfgPath, []byte(tc.config), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.upgrade {
				if err := os.MkdirAll(filepath.Join(filepath.Dir(cfgPath), "nas-node"), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			cfg, err := nasNodeConfigFromEnv(cfgPath, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cfg.Tags, tc.wantTags) || (cfg.EnsureTags != nil) != (tc.wantTags != nil) {
				t.Fatalf("tags = %v (ensure %v), want %v", cfg.Tags, cfg.EnsureTags != nil, tc.wantTags)
			}
			if cfg.Subnet != tc.wantSubnet {
				t.Fatalf("subnet = %q, want %q", cfg.Subnet, tc.wantSubnet)
			}
			if want := filepath.Join(filepath.Dir(cfgPath), "nas-node"); cfg.Dir != want {
				t.Fatalf("state dir = %q, want %q", cfg.Dir, want)
			}
		})
	}
}

// TestNASNodeExposureSurvivesItsOwnState: a fresh install records service
// exposure before its node first runs, so the state directory that run
// creates does not turn the next start into an upgrade.
func TestNASNodeExposureSurvivesItsOwnState(t *testing.T) {
	cfgPath := setNASNodeEnv(t)
	if mode, err := manager.RecordLinkExposureDefault(cfgPath); err != nil || mode != manager.LinkExposureServices {
		t.Fatalf("fresh default = %q, %v", mode, err)
	}
	if err := os.MkdirAll(filepath.Join(filepath.Dir(cfgPath), "nas-node"), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg, err := nasNodeConfigFromEnv(cfgPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Subnet != "" || !reflect.DeepEqual(cfg.Tags, []string{manager.LinkTagNAS}) {
		t.Fatalf("restart of a fresh install = subnet %q tags %v, want service exposure", cfg.Subnet, cfg.Tags)
	}
}

// TestLinkPolicyFileFollowsExposure: the generated policy names the subnet
// only in subnet exposure, including the upgrade default.
func TestLinkPolicyFileFollowsExposure(t *testing.T) {
	cfgPath := setNASNodeEnv(t)
	read := func() string {
		t.Helper()
		path, err := writeLinkPolicy(cfgPath)
		if err != nil {
			t.Fatal(err)
		}
		if path != filepath.Join(filepath.Dir(cfgPath), "policy.hujson") {
			t.Fatalf("policy path = %s", path)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	if got := read(); strings.Contains(got, manager.NasLanSubnet) {
		t.Fatalf("service exposure policy names the subnet:\n%s", got)
	}
	if err := os.MkdirAll(filepath.Join(filepath.Dir(cfgPath), "nas-node"), 0o700); err != nil {
		t.Fatal(err)
	}
	if got := read(); !strings.Contains(got, manager.NasLanSubnet+":30151,30179,30190") {
		t.Fatalf("upgrade policy lacks the subnet ports:\n%s", got)
	}
	t.Setenv("JM_NET_NAS_EXPOSURE", "services")
	if got := read(); strings.Contains(got, manager.NasLanSubnet) {
		t.Fatalf("explicit service exposure policy names the subnet:\n%s", got)
	}
}

package main

// Link configurations an upgrade can arrive in: an operator's own subnet
// router (no embedded NAS node), a contradictory exposure, an exposure
// default that cannot be recorded, and a NAS address that narrows the
// policy.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/manager"
)

// TestHeadscaleServeEnvWithoutEmbeddedNASNode: with JM_NET_NAS_NODE off the
// NAS is reached through the operator's own router, whose services the
// Manager does not know. Before the managed policy every such install
// allowed all Link traffic; a policy naming only tag:jm-nas (which no node
// holds) would deny all of it while status still called the route ready.
func TestHeadscaleServeEnvWithoutEmbeddedNASNode(t *testing.T) {
	for _, route := range []string{"", "192.0.2.0/24"} {
		cfgPath := setNASNodeEnv(t)
		t.Setenv("JM_NET_NAS_NODE", "off")
		t.Setenv("JM_NET_NAS_ROUTE", route)
		env, err := headscaleServeEnv(cfgPath)
		if err != nil || env != nil {
			t.Fatalf("route %q: headscaleServeEnv = %v, %v; want no policy", route, env, err)
		}
		if _, err := os.Stat(filepath.Join(filepath.Dir(cfgPath), "policy.hujson")); !os.IsNotExist(err) {
			t.Fatalf("route %q: a policy file was written (%v)", route, err)
		}
	}
}

// TestHeadscaleServeEnvSurvivesMisconfiguredExposure: services exposure
// with a subnet route is contradictory. The NAS node refuses to start and
// says why; the control server (pairing, every Mac's coordination) keeps
// running.
func TestHeadscaleServeEnvSurvivesMisconfiguredExposure(t *testing.T) {
	cfgPath := setNASNodeEnv(t)
	t.Setenv("JM_NET_NAS_EXPOSURE", "services")
	t.Setenv("JM_NET_NAS_ROUTE", "192.0.2.0/24")
	env, err := headscaleServeEnv(cfgPath)
	if err != nil || len(env) == 0 {
		t.Fatalf("headscaleServeEnv = %v, %v; want the policy env", env, err)
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(cfgPath), "policy.hujson"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "192.0.2.0") || !strings.Contains(string(raw), "tag:jm-nas:") {
		t.Fatalf("policy for a misconfigured exposure:\n%s", raw)
	}
	if _, err := nasNodeConfigFromEnv(cfgPath, nil); err == nil || !strings.Contains(err.Error(), "JM_NET_NAS_ROUTE") {
		t.Fatalf("NAS node config with services and a route: %v", err)
	}
}

// TestNASNodeStartNeedsTheRecordedExposure: a fresh install whose default
// cannot be recorded must not start its node, because the state directory
// that start creates would make the next start look like an upgrade and
// switch it to subnet exposure.
func TestNASNodeStartNeedsTheRecordedExposure(t *testing.T) {
	cfgPath := setNASNodeEnv(t)
	dir := filepath.Dir(cfgPath)
	blocker := filepath.Join(dir, "nas-exposure.tmp")
	if err := os.MkdirAll(filepath.Join(blocker, "x"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := nasNodeConfigFromEnv(cfgPath, nil); err == nil {
		t.Fatal("NAS node configured although its exposure default could not be recorded")
	}
	if err := os.RemoveAll(blocker); err != nil {
		t.Fatal(err)
	}
	if _, err := nasNodeConfigFromEnv(cfgPath, nil); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "nas-node"), 0o700); err != nil {
		t.Fatal(err)
	}
	if mode, _, err := manager.LinkExposure(cfgPath); err != nil || mode != manager.LinkExposureServices {
		t.Fatalf("after the node's first run, exposure = %q, %v; want services", mode, err)
	}
}

// TestLinkPolicyNarrowsToNASAddress: with the NAS's own LAN address known,
// subnet exposure's rule reaches the JuiceMount ports on that address only,
// not on every host in the route.
func TestLinkPolicyNarrowsToNASAddress(t *testing.T) {
	cfgPath := setNASNodeEnv(t)
	t.Setenv("JM_NET_NAS_ROUTE", "192.0.2.0/24")
	t.Setenv("JM_NET_NAS_LAN_ADDR", "192.0.2.10")
	path, err := writeLinkPolicy(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"192.0.2.10/32:30151,30179,30190"`) || strings.Contains(string(raw), "192.0.2.0/24") {
		t.Fatalf("policy with the NAS address:\n%s", raw)
	}
}

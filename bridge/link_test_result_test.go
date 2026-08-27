package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

func TestLinkTestResultAlwaysCarriesRequiredFields(t *testing.T) {
	raw, err := json.Marshal(linkTestResult{Addresses: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"ok", "authorized", "online", "backend_reachable", "redis_reachable", "object_store_reachable", "addresses"} {
		if _, ok := got[key]; !ok {
			t.Fatalf("required Link test field %q omitted from %s", key, raw)
		}
	}
}

func TestValidateLinkConfigRequiresEveryDataPlaneEndpoint(t *testing.T) {
	if configured, err := validateLinkConfig(ServerConfig{}); configured || err != nil {
		t.Fatalf("disabled config = (%v, %v), want (false, nil)", configured, err)
	}
	base := ServerConfig{
		NetControlURL:  "https://link.example.test",
		NetAuthKey:     "tskey-auth-test",
		RedisURL:       "redis://nas:6379/1",
		BucketOverride: "http://nas:9000/zpool",
	}
	if configured, err := validateLinkConfig(base); !configured || err != nil {
		t.Fatalf("complete config = (%v, %v), want enabled", configured, err)
	}

	cases := []struct {
		name string
		edit func(*ServerConfig)
		want string
	}{
		{"missing control URL", func(c *ServerConfig) { c.NetControlURL = "" }, "both the server URL"},
		{"missing key", func(c *ServerConfig) { c.NetAuthKey = "" }, "both the server URL"},
		{"missing Redis", func(c *ServerConfig) { c.RedisURL = "" }, "Redis endpoint"},
		{"missing object store", func(c *ServerConfig) { c.BucketOverride = "" }, "object-store endpoint"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.edit(&cfg)
			configured, err := validateLinkConfig(cfg)
			if !configured || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validate = (%v, %v), want enabled error containing %q", configured, err, tc.want)
			}
		})
	}
}

func TestObjectStoreHealthBaseStripsBucketAndQuery(t *testing.T) {
	if got := objectStoreHealthBase("https://127.0.0.1:4567/zpool?token=secret"); got != "https://127.0.0.1:4567" {
		t.Fatalf("health base = %q", got)
	}
	if got := objectStoreHealthBase("not-a-url"); got != "" {
		t.Fatalf("invalid health base = %q, want empty", got)
	}
}

func TestLinkIdentityChangesWithEveryPairingInput(t *testing.T) {
	base := ServerConfig{
		NetControlURL: "https://link.example.test/",
		NetAuthKey:    "one-off-key-a",
		NetHostname:   "editing-mac",
		DBPath:        "/tmp/jm-test/metadata.db",
	}
	want := identityForLink(base)
	if want.ControlURL != "https://link.example.test" || want.StateDir != "/tmp/jm-test/link" {
		t.Fatalf("normalized identity = %+v", want)
	}
	for _, edit := range []func(*ServerConfig){
		func(c *ServerConfig) { c.NetControlURL = "https://other.example.test" },
		func(c *ServerConfig) { c.NetAuthKey = "one-off-key-b" },
		func(c *ServerConfig) { c.NetHostname = "color-mac" },
		func(c *ServerConfig) { c.DBPath = "/tmp/other/metadata.db" },
	} {
		changed := base
		edit(&changed)
		if got := identityForLink(changed); got == want {
			t.Fatalf("pairing input change reused identity: %+v", changed)
		}
	}
}

func TestReusableLinkNodeRefusesIdentityChangeWithoutStoppingActiveDataPlane(t *testing.T) {
	base := ServerConfig{
		NetControlURL: "https://link.example.test",
		NetAuthKey:    "one-off-key-a",
		NetHostname:   "editing-mac",
		DBPath:        "/tmp/jm-test/metadata.db",
	}
	node := &jmnfs.LinkNode{}
	linkMu.Lock()
	previousNode, previousID := globalLinkNode, globalLinkID
	globalLinkNode, globalLinkID = node, identityForLink(base)
	linkMu.Unlock()
	t.Cleanup(func() {
		linkMu.Lock()
		globalLinkNode, globalLinkID = previousNode, previousID
		linkMu.Unlock()
	})

	got, err := reusableLinkNode(base)
	if err != nil || got != node {
		t.Fatalf("same identity reuse = (%p, %v), want (%p, nil)", got, err, node)
	}
	changed := base
	changed.NetAuthKey = "one-off-key-b"
	if got, err := reusableLinkNode(changed); err == nil || got != nil || !strings.Contains(err.Error(), "stop everything") {
		t.Fatalf("changed identity reuse = (%p, %v), want nil actionable error", got, err)
	}
	linkMu.Lock()
	stillPublished := globalLinkNode
	linkMu.Unlock()
	if stillPublished != node {
		t.Fatal("identity change replaced or detached the live Link node before FUSE teardown")
	}
}

func TestStartupDefersLinkReadinessAndProbesFarSideThroughLink(t *testing.T) {
	raw, err := os.ReadFile("cbridge.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	start := strings.Index(source, "func startLinkIfConfigured")
	end := strings.Index(source, "func validateLinkConfig")
	if start < 0 || end <= start {
		t.Fatal("could not locate Link startup implementation")
	}
	startup := source[start:end]
	for _, required := range []string{
		"StartLinkNodeDeferred",
		"reachURL = linkProbeURL",
		"health.WithDialContext(linkNode.DialContext)",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("startup wiring no longer contains %q", required)
		}
	}
	if strings.Contains(startup, "StartLinkNode(") {
		t.Fatal("app startup reverted to a synchronous Link readiness wait")
	}
}

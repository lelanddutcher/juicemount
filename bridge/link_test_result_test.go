package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

func TestLinkTestDoesNotClaimAuthorizationOrOnlineBeforeFarSideProof(t *testing.T) {
	result := linkTestResult{Addresses: []string{"100.64.0.12"}}
	wantErr := errors.New("revoked route")
	if _, err := proveLinkRedisReadiness(&result, func() (time.Duration, error) {
		return 0, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("failed readiness probe error = %v, want %v", err, wantErr)
	}
	if result.Authorized || result.Online || result.RedisReachable {
		t.Fatalf("failed far-side proof published stale-positive state: %+v", result)
	}

	wantRTT := 37 * time.Millisecond
	gotRTT, err := proveLinkRedisReadiness(&result, func() (time.Duration, error) {
		return wantRTT, nil
	})
	if err != nil || gotRTT != wantRTT {
		t.Fatalf("successful readiness proof = (%v, %v), want (%v, nil)", gotRTT, err, wantRTT)
	}
	if !result.Authorized || !result.Online || !result.RedisReachable {
		t.Fatalf("successful far-side proof did not publish ready state: %+v", result)
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
	if want.ControlURL != "https://link.example.test" || !strings.HasPrefix(want.StateDir, "/tmp/jm-test/link/identities/") {
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
	renamed := base
	renamed.NetHostname = "color-mac"
	if linkStateDir(renamed) != linkStateDir(base) {
		t.Fatal("hostname-only edit required a new one-time pairing identity")
	}
}

func TestPrepareLinkStateDirMigratesLegacyOnceAndIsolatesNewPairing(t *testing.T) {
	root := t.TempDir()
	cfg := ServerConfig{
		NetControlURL: "https://link.example.test/",
		NetAuthKey:    "one-off-key-a",
		NetHostname:   "editing-mac",
		DBPath:        filepath.Join(root, "metadata.db"),
	}
	legacyRoot := linkStateRoot(cfg)
	if err := os.MkdirAll(legacyRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"tailscaled.state":    "saved-profile",
		"tailscaled.log.conf": "saved-log-config",
	} {
		if err := os.WriteFile(filepath.Join(legacyRoot, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate an interrupted first launch that made the destination directory
	// but had not yet moved the legacy state. Preparation must resume safely.
	if err := os.MkdirAll(linkStateDir(cfg), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := prepareLinkStateDir(cfg); err != nil {
		t.Fatalf("prepareLinkStateDir: %v", err)
	}
	first := linkStateDir(cfg)
	if got, err := os.ReadFile(filepath.Join(first, "tailscaled.state")); err != nil || string(got) != "saved-profile" {
		t.Fatalf("migrated state = %q, %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(legacyRoot, "tailscaled.state")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy state still active after migration: %v", err)
	}

	changed := cfg
	changed.NetAuthKey = "one-off-key-b"
	if err := prepareLinkStateDir(changed); err != nil {
		t.Fatalf("prepare changed pairing: %v", err)
	}
	second := linkStateDir(changed)
	if second == first {
		t.Fatal("new pairing code reused the previous tsnet identity directory")
	}
	if _, err := os.Stat(filepath.Join(second, "tailscaled.state")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("new pairing unexpectedly inherited old state: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(first, "tailscaled.state")); err != nil || string(got) != "saved-profile" {
		t.Fatalf("previous identity was not preserved: %q, %v", got, err)
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

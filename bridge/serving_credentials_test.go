//go:build !serviceclient

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/credstore"
)

const (
	// Decoded password with every character that Swift and Go encode
	// differently in userinfo (+ = $ & , ;) plus : and @.
	h8bRedisPassword = "s3cr+et=$a&b,c;d:e@f"
	// The app's spelling (RedisURLCredential.withPassword: all but unreserved).
	h8bRedisSecret = "s3cr%2Bet%3D%24a%26b%2Cc%3Bd%3Ae%40f"
	h8bLinkSecret  = "tskey-auth-s3cret"
)

func h8bSecretConfig(base string) ServerConfig {
	return ServerConfig{
		DBPath:         filepath.Join(base, "metadata.db"),
		RedisURL:       "redis://mac-edit2:" + h8bRedisSecret + "@nas.local:30179/1",
		BucketOverride: "http://nas.local:30151/zpool",
		NetControlURL:  "https://hs.example/",
		NetAuthKey:     h8bLinkSecret,
	}
}

// serving-desired.json lives in Application Support, which Time Machine backs
// up. The saved config must carry the username but no secret.
func TestH8BServingDesiredPersistsNoSecrets(t *testing.T) {
	base := servingDesiredFixture(t)
	cfg := h8bSecretConfig(base)
	if _, err := beginServingDesired(cfg); err != nil {
		t.Fatal(err)
	}
	scope, _, err := selectedOwnerScope()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(scope.Root, servingDesiredLeaf))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{h8bRedisSecret, "s3cr", h8bLinkSecret} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("serving-desired.json persists a secret (%q)", secret)
		}
	}
	saved, err := readServingDesired()
	if err != nil {
		t.Fatal(err)
	}
	if saved.Config.RedisURL != "redis://mac-edit2@nas.local:30179/1" || saved.Config.NetAuthKey != "" {
		t.Fatalf("saved config = %q / %q, want the username kept and both secrets removed",
			saved.Config.RedisURL, saved.Config.NetAuthKey)
	}
	// The owner intent digest is checked against the SAVED config by Stop and
	// recovery, and against the live config by Start: both must agree.
	live, err := intendedOwner(cfg, scope)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := intendedOwner(saved.Config, scope)
	if err != nil {
		t.Fatal(err)
	}
	if live.ConfigDigest != stored.ConfigDigest {
		t.Fatal("owner config digest differs between the live and the saved config")
	}
}

// launchd may restart the service with no app running; recovery must put the
// secrets back from the store, and only for the endpoints they were saved for.
func TestH8BServingRecoveryRehydratesCredentials(t *testing.T) {
	cfg := h8bSecretConfig(t.TempDir())
	saved := cfg
	saved.RedisURL, saved.NetAuthKey = "redis://mac-edit2@nas.local:30179/1", ""
	old := loadServingCredential
	t.Cleanup(func() { loadServingCredential = old })
	loadServingCredential = func(slot credstore.Slot, account string) (string, error) {
		switch {
		case slot == credstore.RedisPassword && account == "redis://mac-edit2@nas.local:30179":
			return h8bRedisPassword, nil
		case slot == credstore.LinkAuthKey && account == "https://hs.example":
			return h8bLinkSecret, nil
		}
		return "", credstore.ErrNotFound
	}
	got := hydrateServingConfig(saved)
	if got != cfg {
		t.Fatalf("hydrated config = %+v, want the original", got)
	}
	if persistableServerConfig(got) != saved {
		t.Fatal("recovery's re-submitted config no longer matches the saved record")
	}
	other := saved
	other.RedisURL = "redis://mac-edit2@evil.example:30179/1"
	if hydrateServingConfig(other).RedisURL != other.RedisURL {
		t.Fatal("password attached to an endpoint it was not saved for")
	}
}

// The owner compares claim payloads byte for byte. The app's Start and
// recovery's replay of it (launchd restart, no app) must therefore produce the
// same payload, however each side spelled the password. Before the canonical
// form, "%2B" from the app came back from recovery as "+": "operation ID
// rebound", recovery held, never retried.
func TestH8BRecoveryClaimMatchesAppClaim(t *testing.T) {
	old := loadServingCredential
	t.Cleanup(func() { loadServingCredential = old })
	loadServingCredential = func(slot credstore.Slot, account string) (string, error) {
		switch slot {
		case credstore.RedisPassword:
			return h8bRedisPassword, nil
		case credstore.LinkAuthKey:
			return h8bLinkSecret, nil
		}
		return "", credstore.ErrNotFound
	}
	claim := func(cfg ServerConfig) string {
		raw, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		// What the service sends and the owner re-serializes (volume_owner.go).
		var requested ServerConfig
		if err := json.Unmarshal(raw, &requested); err != nil {
			t.Fatal(err)
		}
		out, err := json.Marshal(canonicalServerConfig(requested))
		if err != nil {
			t.Fatal(err)
		}
		return string(out)
	}
	app := canonicalServerConfig(h8bSecretConfig(t.TempDir())) // service ingest
	recovered := hydrateServingConfig(persistableServerConfig(app))
	if claim(recovered) != claim(app) {
		t.Fatalf("recovery claim differs from the app's:\n app %s\n rec %s", app.RedisURL, recovered.RedisURL)
	}
	// A user-typed URL (raw sub-delims) is the same claim as the app's spelling.
	typed := h8bSecretConfig(t.TempDir())
	typed.DBPath = app.DBPath
	typed.RedisURL = "redis://mac-edit2:" + strings.ReplaceAll(h8bRedisPassword, "@", "%40") + "@nas.local:30179/1"
	if claim(canonicalServerConfig(typed)) != claim(app) {
		t.Fatal("a raw-typed password is a different claim from the app's encoded one")
	}
}

// H-8B object key: recovery (launchd restart, no app) must give the juicefs
// child the same object key the app would, read from the credential store
// for the Redis endpoint account, and the saved record must never hold it.
func TestH8BServingRecoveryLoadsObjectKey(t *testing.T) {
	const (
		accessKey = "jm-client"
		secretKey = "objsecret-0123456789abcdef"
	)
	base := servingDesiredFixture(t)
	cfg := h8bSecretConfig(base)
	cfg.ObjectAccessKey, cfg.ObjectSecretKey = accessKey, secretKey
	cfg = canonicalServerConfig(cfg)

	if _, err := beginServingDesired(cfg); err != nil {
		t.Fatal(err)
	}
	scope, _, err := selectedOwnerScope()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(scope.Root, servingDesiredLeaf))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), secretKey) || strings.Contains(string(raw), "object_access_key") {
		t.Fatalf("serving-desired.json persists the object key: %s", raw)
	}
	saved, err := readServingDesired()
	if err != nil {
		t.Fatal(err)
	}

	old := loadServingCredential
	t.Cleanup(func() { loadServingCredential = old })
	loadServingCredential = func(slot credstore.Slot, account string) (string, error) {
		switch {
		case slot == credstore.RedisPassword && account == "redis://mac-edit2@nas.local:30179":
			return h8bRedisPassword, nil
		case slot == credstore.LinkAuthKey && account == "https://hs.example":
			return h8bLinkSecret, nil
		case slot == credstore.ObjectKey && account == "redis://mac-edit2@nas.local:30179":
			return accessKey + "\n" + secretKey, nil
		}
		return "", credstore.ErrNotFound
	}
	got := hydrateServingConfig(saved.Config)
	if got.ObjectAccessKey != accessKey || got.ObjectSecretKey != secretKey {
		t.Fatalf("recovery object key = %q/%q, want the stored key", got.ObjectAccessKey, got.ObjectSecretKey)
	}
	if got != cfg {
		t.Fatalf("recovered config differs from the app's:\n got %+v\nwant %+v", persistableServerConfig(got), persistableServerConfig(cfg))
	}
	// Bound to the endpoint account: another server gets no key.
	other := saved.Config
	other.RedisURL = "redis://mac-edit2@evil.example:30179/1"
	if k := hydrateServingConfig(other); k.ObjectAccessKey != "" || k.ObjectSecretKey != "" {
		t.Fatal("object key attached to an endpoint it was not saved for")
	}
	// An app-supplied key is never replaced by the stored one.
	live := cfg
	live.ObjectAccessKey, live.ObjectSecretKey = "jm-client-2", "newer"
	if k := hydrateServingConfig(live); k.ObjectSecretKey != "newer" {
		t.Fatal("recovery overwrote the object key the app supplied")
	}
}

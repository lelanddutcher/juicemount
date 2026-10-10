package credstore

import (
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/deviceid"
)

type vectors struct {
	RedisAccounts []struct{ URL, Account string }             `json:"redis_accounts"`
	LinkAccounts  []struct{ URL, Account string }             `json:"link_accounts"`
	PasswordURLs  []struct{ URL, Password, Canonical string } `json:"password_urls"`
	CanonicalURLs []struct{ URL, Canonical string }           `json:"canonical_urls"`
}

// The same file is checked by the app's RedisCredentialMigrationProbe, so the
// endpoint key the app stores under is the one the service looks up.
func TestEndpointKeysMatchSharedVectors(t *testing.T) {
	data, err := os.ReadFile("testdata/endpoint_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v vectors
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	if len(v.RedisAccounts) == 0 || len(v.LinkAccounts) == 0 || len(v.PasswordURLs) == 0 || len(v.CanonicalURLs) == 0 {
		t.Fatal("vector file has an empty section")
	}
	for _, c := range v.RedisAccounts {
		if got, ok := RedisAccount(c.URL); !ok || got != c.Account {
			t.Errorf("RedisAccount(%q) = %q, want %q", c.URL, got, c.Account)
		}
	}
	for _, c := range v.LinkAccounts {
		if got := LinkAccount(c.URL); got != c.Account {
			t.Errorf("LinkAccount(%q) = %q, want %q", c.URL, got, c.Account)
		}
	}
	for _, c := range v.PasswordURLs {
		if got := WithPassword(c.URL, c.Password); got != c.Canonical {
			t.Errorf("WithPassword(%q) = %q, want %q", c.URL, got, c.Canonical)
		}
		u, err := url.Parse(c.Canonical)
		if err != nil {
			t.Fatal(err)
		}
		if pw, _ := u.User.Password(); pw != c.Password {
			t.Errorf("canonical %q decodes to %q", c.Canonical, pw)
		}
	}
	for _, c := range v.CanonicalURLs {
		if got := CanonicalRedisURL(c.URL); got != c.Canonical {
			t.Errorf("CanonicalRedisURL(%q) = %q, want %q", c.URL, got, c.Canonical)
		}
		if again := CanonicalRedisURL(c.Canonical); again != c.Canonical {
			t.Errorf("CanonicalRedisURL is not idempotent for %q: %q", c.Canonical, again)
		}
	}
	if _, ok := RedisAccount("not a url"); ok {
		t.Error("RedisAccount accepted a URL without scheme and host")
	}
}

func TestIsolatedCredentialIsEndpointBoundAndPrivate(t *testing.T) {
	path := filepath.Join(t.TempDir(), isolatedLeaf(RedisPassword))
	if _, err := loadIsolated(path, "redis://u@h:1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing file: %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"account":"redis://u@h:1","secret":"pw"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := loadIsolated(path, "redis://u@h:1"); err != nil || got != "pw" {
		t.Fatalf("bound read = %q, %v", got, err)
	}
	if _, err := loadIsolated(path, "redis://u@other:1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("secret returned for another endpoint: %v", err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadIsolated(path, "redis://u@h:1"); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("world-readable credential file accepted: %v", err)
	}
}

func stampedFile(t *testing.T, device string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), isolatedLeaf(RedisPassword))
	e := isolatedEntry{Account: "redis://mac-0123456789ab@h:1", Secret: "pw", Device: device}
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func onMac(t *testing.T, uuid string) {
	t.Helper()
	t.Cleanup(deviceid.SetSourceForTest(func() (string, error) { return uuid, nil }))
}

// A secret copied from another Mac (Migration Assistant, Time Machine, clone)
// is another principal's: the service must not send it.
func TestServiceReadRefusesSecretFromAnotherMac(t *testing.T) {
	onMac(t, "BBBBBBBB-0000-4000-8000-00000000000B")
	path := stampedFile(t, deviceid.Digest("AAAAAAAA-0000-4000-8000-00000000000A"))
	got, err := loadIsolated(path, "redis://mac-0123456789ab@h:1")
	if !errors.Is(err, ErrOtherDevice) || got != "" {
		t.Fatalf("copied secret read = %q, %v; want ErrOtherDevice", got, err)
	}
	// Keychain path: the same check on the item's comment attribute.
	if err := checkDevice(deviceFromComment(DeviceStampPrefix + deviceid.Digest("AAAAAAAA-0000-4000-8000-00000000000A"))); !errors.Is(err, ErrOtherDevice) {
		t.Fatalf("keychain stamp from another Mac = %v", err)
	}
}

func TestServiceReadKeepsSecretOnSameMac(t *testing.T) {
	const mac = "AAAAAAAA-0000-4000-8000-00000000000A"
	onMac(t, mac)
	path := stampedFile(t, deviceid.Digest(mac))
	if got, err := loadIsolated(path, "redis://mac-0123456789ab@h:1"); err != nil || got != "pw" {
		t.Fatalf("same-Mac read = %q, %v", got, err)
	}
	if err := checkDevice(deviceFromComment(DeviceStampPrefix + deviceid.Digest(mac))); err != nil {
		t.Fatalf("keychain stamp from this Mac = %v", err)
	}
}

// Secrets saved before the binding carry no stamp; they are adopted (the app
// stamps them), not dropped, so an upgrade does not force re-enrollment.
func TestServiceReadAdoptsUnstampedLegacySecret(t *testing.T) {
	onMac(t, "AAAAAAAA-0000-4000-8000-00000000000A")
	if got, err := loadIsolated(stampedFile(t, ""), "redis://mac-0123456789ab@h:1"); err != nil || got != "pw" {
		t.Fatalf("legacy read = %q, %v", got, err)
	}
	for _, comment := range []string{"", "a note the user typed"} {
		if err := checkDevice(deviceFromComment(comment)); err != nil {
			t.Fatalf("unstamped comment %q = %v", comment, err)
		}
	}
}

// Load's Keychain path applies the device check (not only checkDevice in
// isolation). The Keychain read is substituted; the real Keychain is never
// touched.
func TestLoadKeychainPathAppliesDeviceCheck(t *testing.T) {
	if _, isolated := os.LookupEnv("JM_APP_ISOLATION"); isolated {
		t.Skip("isolation env set; this test covers the Keychain path")
	}
	const account = "redis://mac-0123456789ab@h:1"
	stamp := ""
	prev := keychainRead
	keychainRead = func(service, slot, acct string) (string, string, error) {
		if acct != account {
			return "", "", ErrNotFound
		}
		return "pw", stamp, nil
	}
	t.Cleanup(func() { keychainRead = prev })
	onMac(t, "BBBBBBBB-0000-4000-8000-00000000000B")

	stamp = DeviceStampPrefix + deviceid.Digest("AAAAAAAA-0000-4000-8000-00000000000A")
	if got, err := Load(RedisPassword, account); !errors.Is(err, ErrOtherDevice) || got != "" {
		t.Fatalf("Load returned a Keychain secret saved on another Mac: %q, %v", got, err)
	}
	stamp = DeviceStampPrefix + deviceid.Digest("BBBBBBBB-0000-4000-8000-00000000000B")
	if got, err := Load(RedisPassword, account); err != nil || got != "pw" {
		t.Fatalf("Load on the saving Mac = %q, %v", got, err)
	}
}

// With no fingerprint (IOKit refused, non-darwin) nothing can be compared:
// the secret is kept, never refused, even when stamped for another Mac.
func TestServiceReadKeepsSecretWhenFingerprintUnavailable(t *testing.T) {
	t.Cleanup(deviceid.SetSourceForTest(func() (string, error) { return "", deviceid.ErrUnavailable }))
	path := stampedFile(t, deviceid.Digest("AAAAAAAA-0000-4000-8000-00000000000A"))
	if got, err := loadIsolated(path, "redis://mac-0123456789ab@h:1"); err != nil || got != "pw" {
		t.Fatalf("read with no fingerprint = %q, %v; want the secret kept", got, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("credential file touched: %v", err)
	}
}

package credstore

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/deviceid"
)

// agentTokenEnv points the off-darwin agent token file at a temporary
// config directory, on a Mac with the given hardware UUID.
func agentTokenEnv(t *testing.T, uuid *string) string {
	t.Helper()
	if runtime.GOOS == "darwin" {
		t.Skip("the agent token is kept in the Keychain on darwin")
	}
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	t.Setenv("HOME", dir)
	os.Unsetenv("JM_APP_ISOLATION")
	os.Unsetenv("JM_APP_ISOLATION_ROOT")
	t.Cleanup(deviceid.SetSourceForTest(func() (string, error) { return *uuid, nil }))
	return filepath.Join(dir, "juicemount", "credential-agent-token.json")
}

func TestAgentTokenFileRoundTrip(t *testing.T) {
	uuid := "11111111-2222-3333-4444-555555555555"
	path := agentTokenEnv(t, &uuid)
	if _, err := LoadAgentToken(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("nothing stored: %v", err)
	}
	if err := SaveAgentToken(`{"token":"one"}`, "1111111111111111"); err != nil {
		t.Fatal(err)
	}
	if err := SaveAgentToken(`{"token":"two"}`, "2222222222222222"); err != nil {
		t.Fatal(err)
	}
	if got, err := LoadAgentToken(); err != nil || got != `{"token":"two"}` {
		t.Fatalf("load: %q %v", got, err)
	}
	st, err := os.Stat(path)
	if err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("file mode: %v %v", st.Mode(), err)
	}
	if dst, err := os.Stat(filepath.Dir(path)); err != nil || dst.Mode().Perm() != 0o700 {
		t.Fatalf("directory mode: %v %v", dst.Mode(), err)
	}
	if left, _ := filepath.Glob(filepath.Join(filepath.Dir(path), ".credential-*")); len(left) != 0 {
		t.Fatalf("temporary files left: %v", left)
	}
	// Another Mac (a copied home folder) does not get it.
	uuid = "99999999-2222-3333-4444-555555555555"
	if _, err := LoadAgentToken(); !errors.Is(err, ErrOtherDevice) {
		t.Fatalf("other device: %v", err)
	}
	uuid = "11111111-2222-3333-4444-555555555555"
	// A file others can read is refused.
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAgentToken(); err == nil {
		t.Fatal("a 0644 token file was read")
	}
	if err := DeleteAgentToken(); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAgentToken(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after delete: %v", err)
	}
	if err := DeleteAgentToken(); err != nil {
		t.Fatalf("deleting nothing: %v", err)
	}
	if err := SaveAgentToken("", ""); err == nil {
		t.Fatal("an empty record was saved")
	}
}

func TestLoadEnrollmentChecksDevice(t *testing.T) {
	uuid := "11111111-2222-3333-4444-555555555555"
	agentTokenEnv(t, &uuid)
	here, err := deviceid.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	stamp := DeviceStampPrefix + here
	prev := keychainReadInteractive
	t.Cleanup(func() { keychainReadInteractive = prev })
	keychainReadInteractive = func(service, slot, account string) (string, string, error) {
		if service != keychainService || slot != "enrollment" || account != "manager-enrollment" {
			t.Errorf("read %q %q %q", service, slot, account)
		}
		return `{"poll_token":"p"}`, stamp, nil
	}
	if got, err := LoadEnrollment(); err != nil || got != `{"poll_token":"p"}` {
		t.Fatalf("load: %q %v", got, err)
	}
	stamp = DeviceStampPrefix + "00000000000000000000000000000000"
	if _, err := LoadEnrollment(); !errors.Is(err, ErrOtherDevice) {
		t.Fatalf("other device: %v", err)
	}
}

// The app's store is the item juicemount-mcp reads: what the app saves,
// LoadAgentToken (serve) returns, with the same device binding, and the
// other way round.
func TestAppAgentTokenIsTheCLIItem(t *testing.T) {
	uuid := "11111111-2222-3333-4444-555555555555"
	path := agentTokenEnv(t, &uuid)
	app := AppAgentToken{Trusted: []string{"/Applications/JuiceMount.app/Contents/MacOS/juicemount-mcp"}}
	if _, err := app.Load(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("nothing stored: %v", err)
	}
	if err := app.Save(`{"token":"from-app"}`, "aaaaaaaaaaaaaaaa"); err != nil {
		t.Fatal(err)
	}
	if got, err := LoadAgentToken(); err != nil || got != `{"token":"from-app"}` {
		t.Fatalf("serve's read: %q %v", got, err)
	}
	if st, err := os.Stat(path); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("file: %v", err)
	}
	if err := SaveAgentToken(`{"token":"from-cli"}`, "cccccccccccccccc"); err != nil {
		t.Fatal(err)
	}
	if got, err := app.Load(); err != nil || got != `{"token":"from-cli"}` {
		t.Fatalf("app's read: %q %v", got, err)
	}
	uuid = "99999999-2222-3333-4444-555555555555"
	if _, err := app.Load(); !errors.Is(err, ErrOtherDevice) {
		t.Fatalf("other device: %v", err)
	}
	uuid = "11111111-2222-3333-4444-555555555555"
	if err := app.Delete(); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAgentToken(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after the app's delete: %v", err)
	}
	if err := app.Delete(); err != nil {
		t.Fatalf("deleting nothing: %v", err)
	}
	if err := app.Save("", ""); err == nil {
		t.Fatal("an empty record was saved")
	}
}

// The stored token's ID is kept beside it (the Keychain label, the file's
// id) and told without reading the token; an item from before has none and
// reads as unknown ("").
func TestStoredAgentTokenID(t *testing.T) {
	uuid := "11111111-2222-3333-4444-555555555555"
	path := agentTokenEnv(t, &uuid)
	if id, err := StoredAgentTokenID(); err != nil || id != "" {
		t.Fatalf("nothing stored: %q %v", id, err)
	}
	if err := (AppAgentToken{}).Save(`{"token":"t"}`, "fedcba9876543210"); err != nil {
		t.Fatal(err)
	}
	if id, err := (AppAgentToken{}).StoredID(); err != nil || id != "fedcba9876543210" {
		t.Fatalf("app's: %q %v", id, err)
	}
	if err := SaveAgentToken(`{"token":"t"}`, "0123456789abcdef"); err != nil {
		t.Fatal(err)
	}
	if id, err := StoredAgentTokenID(); err != nil || id != "0123456789abcdef" {
		t.Fatalf("CLI's: %q %v", id, err)
	}
	// An entry written before the ID was kept: unknown, not an error.
	if err := saveFile(path, isolatedEntry{Account: agentTokenAccount, Secret: `{"token":"old"}`}); err != nil {
		t.Fatal(err)
	}
	if id, err := StoredAgentTokenID(); err != nil || id != "" {
		t.Fatalf("legacy: %q %v", id, err)
	}
	// Not an ID: not kept.
	if err := SaveAgentToken(`{"token":"t"}`, "../x"); err != nil {
		t.Fatal(err)
	}
	if id, _ := StoredAgentTokenID(); id != "" {
		t.Fatalf("a bad ID was kept: %q", id)
	}
	uuid = "99999999-2222-3333-4444-555555555555"
	if _, err := StoredAgentTokenID(); !errors.Is(err, ErrOtherDevice) {
		t.Fatalf("other device: %v", err)
	}
}

func TestAgentTokenLabel(t *testing.T) {
	if l := AgentTokenLabel("fedcba9876543210"); l != "JuiceMount agent token fedcba9876543210 (juicemount-mcp)" || agentTokenIDFromLabel(l) != "fedcba9876543210" {
		t.Fatalf("label %q", l)
	}
	for _, l := range []string{AgentTokenLabel(""), AgentTokenLabel("x y"), "JuiceMount agent token (juicemount-mcp)", "JuiceMount agent token FEDCBA9876543210 (juicemount-mcp)", ""} {
		if id := agentTokenIDFromLabel(l); id != "" {
			t.Errorf("%q names %q", l, id)
		}
	}
}

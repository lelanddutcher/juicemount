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
	if err := SaveAgentToken(`{"token":"one"}`); err != nil {
		t.Fatal(err)
	}
	if err := SaveAgentToken(`{"token":"two"}`); err != nil {
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
	if err := SaveAgentToken(""); err == nil {
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

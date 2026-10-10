package credstore

import (
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
)

// The separate Swift process writes the five actual production slot files
// before this selected Go read. No test writer or device fingerprint override
// is involved, and HOME remains the account home.
func TestSwiftIsolatedCredentialWire(t *testing.T) {
	root := os.Getenv("JM_SCOPE_WIRE_ROOT")
	if root == "" {
		t.Skip("JM_SCOPE_WIRE_ROOT not set")
	}
	account, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("HOME") != account.HomeDir {
		t.Fatal("wire read requires unchanged account HOME")
	}
	t.Setenv("JM_APP_ISOLATION", "1")
	t.Setenv("JM_APP_ISOLATION_ROOT", root)
	t.Setenv("TMPDIR", filepath.Join(root, "tmp"))
	scope, isolated, err := appisolation.Current()
	if err != nil || !isolated || scope.Root != root {
		t.Fatalf("Swift-owned private scope unavailable: isolated=%t err=%v", isolated, err)
	}
	for _, slot := range []Slot{RedisPassword, LinkAuthKey, ObjectKey, "enrollment", "metadata-previous"} {
		t.Run(string(slot), func(t *testing.T) {
			got, err := Load(slot, "scope-account")
			if err != nil || got != "scope-"+string(slot) {
				t.Fatalf("Swift-written slot read failed: err=%v matched=%t", err, got == "scope-"+string(slot))
			}
			if got, err := Load(slot, "other-account"); got != "" || !errors.Is(err, ErrNotFound) {
				t.Fatalf("other endpoint accepted: err=%v nonempty=%t", err, got != "")
			}
		})
	}
}

func TestIsolatedCredentialHardLinkRefused(t *testing.T) {
	root := t.TempDir()
	original := filepath.Join(root, "original")
	linked := filepath.Join(root, "linked")
	if err := os.WriteFile(original, []byte(`{"account":"scope-account","secret":"sentinel"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(original, linked); err != nil {
		t.Fatal(err)
	}
	if secret, err := loadIsolated(linked, "scope-account"); err == nil || secret != "" {
		t.Fatal("hard-linked credential admitted")
	}
}

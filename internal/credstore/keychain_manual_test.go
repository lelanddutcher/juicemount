//go:build credstore_keychain && darwin && cgo

package credstore

import (
	"os"
	"testing"
)

// By hand only (touches the login keychain; never shows UI because reads run
// with user interaction disabled). With JM_CREDSTORE_SERVICE naming an item
// made by RedisCredentialMigrationProbe `keychain-acl create`:
// JM_CREDSTORE_EXPECT=readable when this test binary is on its access list,
// JM_CREDSTORE_EXPECT=refused when it is not.
func TestManualKeychainAccessList(t *testing.T) {
	service := os.Getenv("JM_CREDSTORE_SERVICE")
	if service == "" {
		t.Skip("JM_CREDSTORE_SERVICE unset")
	}
	got, _, err := keychainLoad(service, string(RedisPassword), "redis://probe@127.0.0.1:1")
	switch os.Getenv("JM_CREDSTORE_EXPECT") {
	case "readable":
		if err != nil || got != "acl-probe" {
			t.Fatalf("trusted read = %q, %v", got, err)
		}
	case "refused":
		if err == nil {
			t.Fatalf("untrusted binary read the item: %q", got)
		}
		t.Logf("untrusted read refused without UI: %v", err)
	default:
		t.Fatal("set JM_CREDSTORE_EXPECT=readable|refused")
	}
	if _, _, err := keychainLoad(service, string(RedisPassword), "redis://probe@127.0.0.2:1"); err == nil {
		t.Fatal("item returned for another endpoint")
	}
}

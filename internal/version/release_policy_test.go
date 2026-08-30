package version

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readReleasePolicyFile(t *testing.T, parts ...string) string {
	t.Helper()
	path := filepath.Join(append([]string{"..", ".."}, parts...)...)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func TestMacUpdateChecksAreUserInitiatedOnly(t *testing.T) {
	plist := readReleasePolicyFile(t, "app", "JuiceMount", "Resources", "Info.plist")
	if !strings.Contains(plist, "<key>SUEnableAutomaticChecks</key>\n\t<false/>") {
		t.Fatal("Info.plist must disable Sparkle automatic checks")
	}
	if strings.Contains(plist, "SUScheduledCheckInterval") {
		t.Fatal("manual-only update policy must not carry a scheduled check interval")
	}

	controller := readReleasePolicyFile(t, "app", "JuiceMount", "Sources", "JuiceMount", "UI", "MenuBarController.swift")
	if got := strings.Count(controller, "updaterController.startUpdater()"); got != 1 {
		t.Fatalf("Sparkle start count = %d, want exactly one user-initiated start", got)
	}
	checkStart := strings.Index(controller, "func checkForUpdates()")
	updaterStart := strings.Index(controller, "updaterController.startUpdater()")
	if checkStart < 0 || updaterStart < checkStart {
		t.Fatal("Sparkle must only start inside the explicit checkForUpdates action")
	}
	if strings.Contains(controller[:checkStart], "DispatchQueue.main.asyncAfter") {
		t.Fatal("background-delayed Sparkle start reintroduced")
	}
}

func TestMacWriteSpoolRemainsDefaultOn(t *testing.T) {
	preferences := readReleasePolicyFile(t, "app", "JuiceMount", "Sources", "JuiceMount", "Core", "Preferences.swift")
	if !strings.Contains(preferences, "spoolEnabled: Bool = true") {
		t.Fatal("write spool must default on unless a user has explicitly disabled it")
	}
}

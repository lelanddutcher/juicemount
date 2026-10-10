package perf

import (
	"os"
	"strings"
	"testing"
)

func TestSwiftOfflineRecoveryClearsStartupBanner(t *testing.T) {
	controller, err := os.ReadFile("../../app/JuiceMount/Sources/JuiceMount/Core/ServerController.swift")
	if err != nil {
		t.Fatal(err)
	}
	menu, err := os.ReadFile("../../app/JuiceMount/Sources/JuiceMount/UI/MenuPopoverView.swift")
	if err != nil {
		t.Fatal(err)
	}
	controllerText := string(controller)
	menuText := string(menu)
	for _, marker := range []string{
		"public private(set) var offlineStartupReason: String?",
		// cacheStatus() became optional in 5e5f9f6: nil means "unavailable,
		// keep the last confirmed state". The banner may clear only on a
		// fetched (non-nil) online status, so the guard binds s first.
		"if let s, !s.offline_mode,",
		"hp?.coreHealthy == true",
		"self.offlineStartupReason = nil",
		"if !o.auto_offline && self.offlineStartupReason != nil",
	} {
		if !strings.Contains(controllerText, marker) {
			t.Fatalf("ServerController recovery contract missing %q", marker)
		}
	}
	if !strings.Contains(menuText, "Started offline — showing the cached file list.") {
		t.Fatal("MenuPopoverView no longer exposes the startup-offline state")
	}
}

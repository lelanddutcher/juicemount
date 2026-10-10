package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// The packaged app's Info.plist LSEnvironment reaches the GUI process, and
// servingConfiguration refuses any JM_ variable outside servingGUIEnvAllowed.
// An entry here that the service rejects makes every packaged launch fail
// with "GUI environment override ... is not supported" (H-12, 2026-10-06).
func TestInfoPlistEnvironmentIsAcceptedByServingService(t *testing.T) {
	raw, err := os.ReadFile("../app/JuiceMount/Resources/Info.plist")
	if err != nil {
		t.Fatal(err)
	}
	plist := string(raw)
	start := strings.Index(plist, "<key>LSEnvironment</key>")
	if start < 0 {
		return
	}
	end := strings.Index(plist[start:], "</dict>")
	if end < 0 {
		t.Fatal("unterminated LSEnvironment dict")
	}
	for _, m := range regexp.MustCompile(`<key>([^<]+)</key>`).FindAllStringSubmatch(plist[start:start+end], -1) {
		key := m[1]
		if key == "LSEnvironment" || !strings.HasPrefix(key, "JM_") {
			continue
		}
		if !servingGUIEnvAllowed[key] {
			t.Errorf("Info.plist LSEnvironment sets %s, which the serving service refuses; every packaged Start would fail", key)
		}
	}
}

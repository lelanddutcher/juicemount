package main

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/juicefsbin"
)

// The serving process receives the shared bundled resolver's answer; the
// no-fallback proof lives in internal/juicefsbin.
func TestServingJuiceFSResolverIsTheBundledResolver(t *testing.T) {
	restore := juicefsbin.SetForTest("/Applications/JuiceMount.app/Contents/MacOS/juicefs")
	defer restore()
	if got := resolveServingJuiceFS(); got != "/Applications/JuiceMount.app/Contents/MacOS/juicefs" {
		t.Fatalf("resolveServingJuiceFS() = %q", got)
	}
}

// An explicit juicefs_bin in the GUI's configuration is forwarded only when
// it is the bundled executable; anything else is a typed refusal.
func TestServingConfigurationRefusesUnbundledJuiceFS(t *testing.T) {
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		if strings.HasPrefix(key, "JM_") {
			t.Setenv(key, "")
		}
	}
	restore := juicefsbin.SetForTest("/Applications/JuiceMount.app/Contents/MacOS/juicefs")
	defer restore()
	_, err := servingConfiguration(`{"juicefs_bin":"/opt/homebrew/bin/juicefs"}`)
	var refused *juicefsbin.NotBundledError
	if !errors.As(err, &refused) || refused.Path != "/opt/homebrew/bin/juicefs" {
		t.Fatalf("servingConfiguration = %v, want *juicefsbin.NotBundledError", err)
	}
	raw, err := servingConfiguration(`{"juicefs_bin":"/Applications/JuiceMount.app/Contents/MacOS/juicefs"}`)
	if err != nil || !strings.Contains(raw, `"juicefs_bin":"/Applications/JuiceMount.app/Contents/MacOS/juicefs"`) {
		t.Fatalf("bundled path not forwarded: %v %s", err, raw)
	}
}

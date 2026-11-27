package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/enginehost"
)

func TestEngineLevelConfigWinsOverEnvironment(t *testing.T) {
	t.Setenv("JM_ENGINE", "")
	if got := engineLevel(ServerConfig{}); got != enginehost.LevelFUSE {
		t.Fatalf("unset: %v", got)
	}
	t.Setenv("JM_ENGINE", "shadow")
	if got := engineLevel(ServerConfig{}); got != enginehost.LevelShadow {
		t.Fatalf("environment shadow: %v", got)
	}
	if got := engineLevel(ServerConfig{EngineLevel: "fuse"}); got != enginehost.LevelFUSE {
		t.Fatalf("config fuse over environment shadow: %v", got)
	}
	t.Setenv("JM_ENGINE", "")
	for _, bad := range []string{"direct", "reads", "nonsense"} {
		if got := engineLevel(ServerConfig{EngineLevel: bad}); got != enginehost.LevelFUSE {
			t.Fatalf("%q: %v, want fuse", bad, got)
		}
	}
}

// Unset, the switch starts nothing and the config encodes as before.
func TestEngineShadowOffByDefault(t *testing.T) {
	t.Setenv("JM_ENGINE", "")
	globalMu.Lock()
	startEngineShadowLocked(ServerConfig{JuiceFSBin: "/nonexistent/juicefs"}, "")
	started := globalEngine != nil
	globalMu.Unlock()
	if started {
		stopEngineShadow()
		t.Fatal("a helper started with the level unset")
	}
	b, err := json.Marshal(ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "engine_level") {
		t.Fatalf("an unset level changes the config encoding: %s", b)
	}
}

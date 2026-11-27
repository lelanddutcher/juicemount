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
	if got := engineLevel(ServerConfig{EngineLevel: "reads"}); got != enginehost.LevelReads {
		t.Fatalf("reads: %v", got)
	}
	for _, bad := range []string{"direct", "control", "nonsense"} {
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

// Step-1 review F4: a helper whose inputs changed (a key rotation, another
// bucket, the level) is replaced, not kept for the serving lifetime.
func TestEngineShadowReplacedWhenInputsChange(t *testing.T) {
	t.Setenv("JM_ENGINE", "")
	cfg := ServerConfig{JuiceFSBin: "/nonexistent/juicefs", EngineLevel: "shadow",
		RedisURL: "redis://mac@127.0.0.1:1/1", ObjectAccessKey: "AK1", ObjectSecretKey: "SK1"}
	globalMu.Lock()
	startEngineShadowLocked(cfg, "")
	first := globalEngine
	startEngineShadowLocked(cfg, "")
	same := globalEngine
	rotated := cfg
	rotated.ObjectAccessKey, rotated.ObjectSecretKey = "AK2", "SK2"
	startEngineShadowLocked(rotated, "")
	afterRotation := globalEngine
	off := rotated
	off.EngineLevel = "fuse"
	startEngineShadowLocked(off, "")
	afterOff := globalEngine
	globalMu.Unlock()
	defer stopEngineShadow()
	if first == nil || same != first {
		t.Fatalf("unchanged inputs replaced the helper (%p, %p)", first, same)
	}
	if afterRotation == nil || afterRotation == first {
		t.Fatal("a key rotation kept the old helper")
	}
	if afterOff != nil {
		t.Fatal("level fuse kept a shadow helper running")
	}
}

// Only level reads changes the FUSE child (the mount opens the socket).
func TestEngineFUSEKey(t *testing.T) {
	t.Setenv("JM_ENGINE", "")
	for level, want := range map[string]string{"": "", "fuse": "", "shadow": "", "reads": "reads", "bogus": ""} {
		if got := engineFUSEKey(ServerConfig{EngineLevel: level}); got != want {
			t.Fatalf("%q: %q, want %q", level, got, want)
		}
	}
	if d := engineDoorForMount(ServerConfig{EngineLevel: "shadow"}); d != nil {
		t.Fatal("a door below level reads")
	}
	d := engineDoorForMount(ServerConfig{EngineLevel: "reads"})
	if d == nil || d != engineDoorForMount(ServerConfig{EngineLevel: "reads"}) {
		t.Fatal("level reads must reuse one door")
	}
	closeEngineDoor()
}

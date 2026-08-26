package health

import (
	"slices"
	"strings"
	"testing"
)

func TestJuiceFSChildEnvironmentDropsParentSecrets(t *testing.T) {
	t.Setenv("HOME", "/tmp/jm-safe-home")
	t.Setenv("PATH", "/usr/bin:/bin")
	t.Setenv("JM_TEST_TOKEN", "must-not-leak")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "must-not-leak")
	t.Setenv("UNRELATED_VALUE", "must-not-leak")

	env := juiceFSChildEnvironment()
	if !slices.Contains(env, "HOME=/tmp/jm-safe-home") {
		t.Fatalf("safe HOME missing from child environment: %v", env)
	}
	if !slices.Contains(env, "PATH=/usr/bin:/bin") {
		t.Fatalf("safe PATH missing from child environment: %v", env)
	}
	for _, entry := range env {
		if strings.Contains(entry, "must-not-leak") {
			t.Fatalf("secret/unrelated parent value leaked to JuiceFS child: %q", entry)
		}
	}
}

func TestDesktopJuiceFSHeartbeatKeepsSessionForFiveMinutes(t *testing.T) {
	if desktopJuiceFSHeartbeat != "60s" {
		t.Fatalf("desktop heartbeat = %q, want 60s (five-minute JuiceFS expiry)", desktopJuiceFSHeartbeat)
	}
	want := []string{"--heartbeat", "60s", "--max-uploads", "4"}
	if got := desktopJuiceFSSessionArgs(); !slices.Equal(got, want) {
		t.Fatalf("desktop session args = %v, want %v", got, want)
	}
}

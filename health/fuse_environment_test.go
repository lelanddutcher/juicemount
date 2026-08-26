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

func TestJuiceFSDirectEnvironmentSelectsOneExplicitSupervisor(t *testing.T) {
	t.Setenv("HOME", "/tmp/jm-safe-home")
	t.Setenv("JFS_SUPERVISOR", "inherited-value-must-not-win")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "must-not-leak")

	env := juiceFSDirectEnvironment(4242)
	if !slices.Contains(env, "JFS_SUPERVISOR=4242") {
		t.Fatalf("direct service selector missing from child environment: %v", env)
	}
	for _, entry := range env {
		if entry == "JFS_SUPERVISOR=inherited-value-must-not-win" || strings.Contains(entry, "must-not-leak") {
			t.Fatalf("untrusted parent value leaked to direct JuiceFS service: %q", entry)
		}
	}
}

func TestDesktopJuiceFSMountArgsNeverStartUpstreamDaemonSupervisor(t *testing.T) {
	got := desktopJuiceFSMountArgs("redis://127.0.0.1:6379/1", "/tmp/jm", 32, 1)
	want := []string{
		"mount", "redis://127.0.0.1:6379/1", "/tmp/jm",
		"--no-usage-report", "--buffer-size", "32", "--prefetch", "1",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("desktop mount args = %v, want %v", got, want)
	}
	if slices.Contains(got, "-d") || slices.Contains(got, "--background") {
		t.Fatalf("desktop mount args enabled upstream watchdog supervisor: %v", got)
	}
}

func TestTailCaptureRetainsBoundedActionableSuffix(t *testing.T) {
	b := newTailCapture(8)
	if n, err := b.Write([]byte("progress-")); err != nil || n != 9 {
		t.Fatalf("first write = (%d, %v), want (9, nil)", n, err)
	}
	if n, err := b.Write([]byte("fatal")); err != nil || n != 5 {
		t.Fatalf("second write = (%d, %v), want (5, nil)", n, err)
	}
	if got := b.String(); got != "ss-fatal" {
		t.Fatalf("bounded stderr tail = %q, want %q", got, "ss-fatal")
	}
	if len(b.String()) > 8 {
		t.Fatalf("bounded stderr tail grew past cap: %d", len(b.String()))
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

func TestDesktopJuiceFSReadAheadDoesNotFreezeStartupClass(t *testing.T) {
	t.Setenv("JM_JFS_MAX_READAHEAD", "")
	want := []string{"--max-readahead", "1M"}
	if got := desktopJuiceFSReadAheadArgs(); !slices.Equal(got, want) {
		t.Fatalf("desktop read-ahead args = %v, want %v", got, want)
	}

	// The rollback switch restores the upstream JuiceFS default without a
	// rebuild if a field deployment finds a fast-link throughput regression.
	t.Setenv("JM_JFS_MAX_READAHEAD", "0")
	if got := desktopJuiceFSReadAheadArgs(); got != nil {
		t.Fatalf("desktop read-ahead rollback args = %v, want nil", got)
	}
}

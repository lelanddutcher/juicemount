package health

import (
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
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

	env := juiceFSDirectEnvironment(4242, "", ObjectKey{})
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
	got, _ := desktopJuiceFSMountArgs("redis://127.0.0.1:6379/1", "/tmp/jm", 32, 1)
	want := []string{
		"mount", "redis://127.0.0.1:6379/1", "/tmp/jm",
		"--no-usage-report", "--no-bgjob", "--max-deletes", "0",
		"--buffer-size", "32", "--prefetch", "1",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("desktop mount args = %v, want %v", got, want)
	}
	if slices.Contains(got, "-d") || slices.Contains(got, "--background") {
		t.Fatalf("desktop mount args enabled upstream watchdog supervisor: %v", got)
	}
}

func TestReadFileTailSinceRetainsBoundedLaunchSuffix(t *testing.T) {
	path := t.TempDir() + "/juicefs.log"
	if err := os.WriteFile(path, []byte("old-progress-fatal"), 0600); err != nil {
		t.Fatal(err)
	}
	if got := readFileTailSince(path, int64(len("old-")), 8); got != "ss-fatal" {
		t.Fatalf("bounded launch log tail = %q, want %q", got, "ss-fatal")
	}
	if got := readFileTailSince(path, int64(len("old-progress-fatal")), 8); got != "" {
		t.Fatalf("tail at EOF = %q, want empty", got)
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

func TestDesktopJuiceFSSmallUploadSlots(t *testing.T) {
	for v, want := range map[string]string{"": "2", "0": "0", "4": "4", "5": "2", "-1": "2", "x": "2"} {
		t.Setenv("JM_JFS_SMALL_UPLOADS", v)
		if got := desktopJuiceFSSmallUploadArgs(); !slices.Equal(got, []string{"--small-uploads", want}) {
			t.Fatalf("JM_JFS_SMALL_UPLOADS=%q: args %v, want --small-uploads %s", v, got, want)
		}
	}
}

func TestDesktopJuiceFSObjectTimeoutsKeepCellularBlocksViableAndBounded(t *testing.T) {
	t.Setenv("JM_JFS_OBJECT_TIMEOUTS", "")
	tests := []struct {
		class netprofile.LinkClass
		want  []string
	}{
		{netprofile.ClassMetered, []string{"--get-timeout", "3m", "--put-timeout", "5m", "--io-retries", "2"}},
		{netprofile.ClassSlow, []string{"--get-timeout", "2m", "--put-timeout", "3m", "--io-retries", "3"}},
		{netprofile.ClassMedium, nil},
		{netprofile.ClassFast, nil},
	}
	for _, tt := range tests {
		if got := desktopJuiceFSObjectIOArgs(tt.class); !slices.Equal(got, tt.want) {
			t.Fatalf("object I/O args for %s = %v, want %v", tt.class, got, tt.want)
		}
	}

	// Field rollback restores the exact upstream defaults without a rebuild.
	t.Setenv("JM_JFS_OBJECT_TIMEOUTS", "0")
	if got := desktopJuiceFSObjectIOArgs(netprofile.ClassMetered); got != nil {
		t.Fatalf("object timeout rollback args = %v, want nil", got)
	}
}

func TestDesktopJuiceFSDemandFirstOnSlowAndMeteredOnly(t *testing.T) {
	t.Setenv("JM_JFS_DEMAND_FIRST", "")
	tests := []struct {
		class netprofile.LinkClass
		want  []string
	}{
		{netprofile.ClassMetered, []string{"--demand-first"}},
		{netprofile.ClassSlow, []string{"--demand-first"}},
		{netprofile.ClassMedium, nil},
		{netprofile.ClassFast, nil},
	}
	for _, tt := range tests {
		if got := desktopJuiceFSDemandFirstArgs(tt.class); !slices.Equal(got, tt.want) {
			t.Fatalf("demand-first args for %s = %v, want %v", tt.class, got, tt.want)
		}
	}
	t.Setenv("JM_JFS_DEMAND_FIRST", "0")
	if got := desktopJuiceFSDemandFirstArgs(netprofile.ClassSlow); got != nil {
		t.Fatalf("demand-first rollback args = %v, want nil", got)
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

// This control preserves the actual account HOME; the namespace is carried
// separately, and external JuiceFS receives explicit paths through arguments.
func TestJuiceFSDirectEnvironmentPreservesAccountHome(t *testing.T) {
	original := os.Getenv("HOME")
	if original == "" {
		t.Fatal("account home unavailable")
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "synthetic-inherited-key")
	t.Setenv("MINIO_SECRET_KEY", "synthetic-inherited-secret")
	env := juiceFSDirectEnvironment(os.Getpid(), "", ObjectKey{})
	if !slices.Contains(env, "HOME="+original) {
		t.Fatal("account HOME changed")
	}
	for _, item := range juiceFSNoAWSCredentialChain {
		if !slices.Contains(env, item) {
			t.Fatal("ambient AWS credential source enabled")
		}
	}
	for _, item := range env {
		if strings.HasPrefix(item, "AWS_ACCESS_KEY_ID=") || strings.HasPrefix(item, "MINIO_SECRET_KEY=") {
			t.Fatal("ambient credential propagated")
		}
	}
}

// A far, thin link (300 ms, 2 Mbps, no cost flag) is class slow by the
// slow-RTT floor but keeps the metered object deadlines, which the mount
// takes from ReadPolicyClass: unflagged cellular keeps cellular-safe PUTs.
func TestDesktopJuiceFSObjectIOArgsThinFarLinkStaysMetered(t *testing.T) {
	t.Setenv("JM_JFS_OBJECT_TIMEOUTS", "")
	t.Setenv("JM_NET_SLOW_RTT_FLOOR", "")
	p := netprofile.New()
	p.ObserveRTT(300 * time.Millisecond)
	for i := 0; i < 8; i++ {
		p.ObserveThroughput(4<<20, 16*time.Second) // 2 Mbps
	}
	if p.Class() != netprofile.ClassSlow {
		t.Fatalf("precondition: class %s, want slow", p.Class())
	}
	got := desktopJuiceFSObjectIOArgs(p.ReadPolicyClass())
	if want := desktopJuiceFSObjectIOArgs(netprofile.ClassMetered); !slices.Equal(got, want) {
		t.Fatalf("object IO args %v, want the metered %v", got, want)
	}
}

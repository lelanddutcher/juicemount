package health

import (
	"slices"
	"strings"
	"testing"
)

// H-8 option B, lane L3. The Mac juicefs mount is an ordinary writer under
// the per-project Redis proxy: the server's bg principal owns trash, slice
// and deleted-file cleanup, so the Mac runs no background jobs and deletes no
// objects. Its Redis username rides in the URL; its password reaches juicefs
// only through META_PASSWORD (pkg/meta/redis.go), never argv, which every
// local user can read with ps.
func TestH8BMacArgvHasNoBgjobAndNoSecret(t *testing.T) {
	const secret = "s3cr:et@/pa%ss"
	configured := "redis://mac-edit2:s3cr%3Aet%40%2Fpa%25ss@nas.local:30179/1"
	t.Setenv("META_PASSWORD", "inherited-must-not-win")
	t.Setenv("REDIS_PASSWORD", "inherited-must-not-win")

	args, password := desktopJuiceFSMountArgs(configured, "/mount", 32, 1)
	if password != secret {
		t.Fatalf("password for the child environment = %q, want %q", password, secret)
	}
	if !slices.Contains(args, "--no-bgjob") {
		t.Fatalf("Mac mount runs background cleanup jobs: %v", args)
	}
	i := slices.Index(args, "--max-deletes")
	if i < 0 || i+1 >= len(args) || args[i+1] != "0" {
		t.Fatalf("Mac mount deletes objects (want --max-deletes 0): %v", args)
	}
	if args[1] != "redis://mac-edit2@nas.local:30179/1" {
		t.Fatalf("metadata URL in argv = %q, want the username without the password", args[1])
	}
	argv := strings.Join(args, " ")
	for _, leak := range []string{secret, "s3cr", "%3Aet", "pa%25ss"} {
		if strings.Contains(argv, leak) {
			t.Fatalf("argv exposes the Redis password (%q): %v", leak, args)
		}
	}

	env := juiceFSDirectEnvironment(4242, password, ObjectKey{})
	var metaPasswords []string
	for _, entry := range env {
		if strings.HasPrefix(entry, "META_PASSWORD=") {
			metaPasswords = append(metaPasswords, entry)
		}
		if strings.Contains(entry, "inherited-must-not-win") {
			t.Fatalf("inherited credential reached the juicefs child: %q", entry)
		}
	}
	if !slices.Equal(metaPasswords, []string{"META_PASSWORD=" + secret}) {
		t.Fatalf("child META_PASSWORD entries = %v, want exactly the configured password", metaPasswords)
	}

	// No password configured: nothing extra in the environment, URL unchanged.
	args, password = desktopJuiceFSMountArgs("redis://nas.local:30179/1", "/mount", 32, 1)
	if password != "" || args[1] != "redis://nas.local:30179/1" {
		t.Fatalf("password-less URL changed: %q / %q", args[1], password)
	}
	for _, entry := range juiceFSDirectEnvironment(4242, "", ObjectKey{}) {
		if strings.HasPrefix(entry, "META_PASSWORD=") || strings.HasPrefix(entry, "REDIS_PASSWORD=") {
			t.Fatalf("password-less mount got a credential variable: %q", entry)
		}
	}
}

// Under H-8 the proxy refuses an unauthenticated (or wrong-password) client on
// every command, reads included. The bundled juicefs exits at once with a
// FATAL naming JMDENY AUTH_REQUIRED (internal/bundledjuicefs
// TestH8BAuthRequiredMountFailsCleanly). The mount failure must say what the
// user can do about it, not just echo the proxy's token line.
func TestH8BAuthRequiredMountFailsWithClearError(t *testing.T) {
	// Captured from the bundled juicefs against a proxy stub, 2026-10-06.
	tail := `2026/10/06 16:21:00.783248 juicefs[15188] <INFO>: Meta address: redis://mac-edit2@127.0.0.1:58693/14 [NewClient@interface.go:584]
2026/10/06 16:21:00.785555 juicefs[15188] <WARNING>: Deleting object will be disabled since max-deletes is 0 [SelfCheck@config.go:62]
2026/10/06 16:21:00.788951 juicefs[15188] <WARNING>: parse info: JMDENY AUTH_REQUIRED proj=0 holder=-@- epoch=0 [checkServerConfig@redis.go:3941]
2026/10/06 16:21:00.789741 juicefs[15188] <FATAL>: JMDENY AUTH_REQUIRED proj=0 holder=-@- epoch=0 [main@main.go:31]
`
	reason := juiceFSStartupFailureReason(tail)
	for _, want := range []string{"AUTH_REQUIRED", "password", "Settings", "Manager"} {
		if !strings.Contains(reason, want) {
			t.Fatalf("mount failure reason %q lacks %q", reason, want)
		}
	}
	// Any other startup refusal names its reason; ordinary failures are
	// reported unchanged.
	if r := juiceFSStartupFailureReason("x <FATAL>: JMDENY ADMIN_ONLY proj=1 holder=a@b epoch=1 [main]\n"); !strings.Contains(r, "ADMIN_ONLY") || !strings.Contains(r, "refused") {
		t.Fatalf("other refusal reason = %q", r)
	}
	if r := juiceFSStartupFailureReason("info\nfatal: mountpoint is not empty\n"); r != "fatal: mountpoint is not empty" {
		t.Fatalf("ordinary failure reason changed: %q", r)
	}
}

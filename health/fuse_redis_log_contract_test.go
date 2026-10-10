package health

import (
	"strings"
	"testing"
)

func TestSavedRedisURLMountLogArgsRedactAllCredentials(t *testing.T) {
	args, _ := desktopJuiceFSMountArgs("redis://probe:synthetic-secret@nas.tailnet:6379/2?token=synthetic-secret", "/mount", 32, 1)
	args = append(args, "--bucket", "https://user:bucket-secret@storage.tailnet/path?key=bucket-secret")
	got := mountLogArgs(args)
	text := strings.Join(got, " ")
	for _, secret := range []string{"synthetic-secret", "bucket-secret", "token=", "key="} {
		if strings.Contains(text, secret) {
			t.Errorf("log args expose %q", secret)
		}
	}
	// The exec URL keeps everything but the password, which travels in the
	// child environment (TestH8BMacArgvHasNoBgjobAndNoSecret); logging must
	// not alter what is executed.
	if args[1] != "redis://probe@nas.tailnet:6379/2?token=synthetic-secret" {
		t.Fatalf("exec args = %q, want the configured URL without its password", args[1])
	}
	if !strings.Contains(text, "redis://nas.tailnet:6379") {
		t.Fatal("safe Redis destination missing")
	}
	verboseArgs := append([]string{"--verbose"}, args...)
	verboseText := strings.Join(mountLogArgs(verboseArgs), " ")
	if strings.Contains(verboseText, "synthetic-secret") || strings.Contains(verboseText, "token=") {
		t.Fatal("verbose mount log exposed saved Redis URL")
	}
}

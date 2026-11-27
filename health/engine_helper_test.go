package health

import (
	"strings"
	"testing"
)

func TestEngineHelperArgsKeepSecretsOut(t *testing.T) {
	t.Setenv("META_PASSWORD", "inherited")
	t.Setenv("MINIO_SECRET_KEY", "inherited")
	args, pw := EngineHelperArgs("redis://mac-a:s3cret@nas.local:6379/1", "http://nas.local:9000/zpool")
	if pw != "s3cret" {
		t.Fatalf("password %q", pw)
	}
	joined := strings.Join(args, " ")
	if strings.Contains(joined, "s3cret") {
		t.Fatalf("password in argv: %s", joined)
	}
	for _, want := range []string{"jm-engine redis://mac-a@nas.local:6379/1", "--read-only", "--cache-size 0",
		"--no-bgjob", "--max-deletes 0", "--bucket http://nas.local:9000/zpool"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("args %q lack %q", joined, want)
		}
	}
	if args, _ := EngineHelperArgs("redis://nas.local:6379/1", ""); strings.Contains(strings.Join(args, " "), "--bucket") {
		t.Fatal("--bucket without an override")
	}
	for _, kv := range EngineHelperEnvironment() {
		if strings.HasPrefix(kv, "META_PASSWORD=") || strings.HasPrefix(kv, "MINIO_") || strings.HasPrefix(kv, "JFS_SUPERVISOR=") {
			t.Fatalf("helper environment carries %s", strings.SplitN(kv, "=", 2)[0])
		}
	}
}

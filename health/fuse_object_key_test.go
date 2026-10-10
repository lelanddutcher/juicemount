package health

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestH8BJuiceFSChildGetsObjectKeyInEnvNotArgv runs the real mount launch
// with a stand-in juicefs that records its argv and environment. The
// enrollment object key must reach the child as MINIO_ACCESS_KEY /
// MINIO_SECRET_KEY (stock juicefs reads them once the volume `setting` holds
// no key) and must never appear in argv, which any local user can read.
func TestH8BJuiceFSChildGetsObjectKeyInEnvNotArgv(t *testing.T) {
	const (
		accessKey = "jm-client"
		secretKey = "objsecret-0123456789abcdef"
		password  = "redispw-0123456789"
	)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MINIO_ACCESS_KEY", "inherited-must-not-win")
	t.Setenv("MINIO_SECRET_KEY", "inherited-must-not-win")
	dir := t.TempDir()
	record := filepath.Join(dir, "child.txt")
	fake := filepath.Join(dir, "fake-juicefs")
	script := "#!/bin/sh\n{ echo ARGV: \"$@\"; env; } > '" + record + "'\nexit 7\n"
	if err := os.WriteFile(fake, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	fm := NewFUSEManager(FUSEConfig{
		JuiceFSBin: fake,
		MountPoint: filepath.Join(dir, "fuse"),
		RedisURL:   "redis://mac-0123456789ab:" + password + "@nas.local:30179/1",
		ObjectKey:  ObjectKey{AccessKey: accessKey, SecretKey: secretKey},
	})
	fm.observeMount = testEmptyFUSESnapshot
	fm.custodyPreflight = func() error { return nil }
	receipt, err := fm.MountWithReceipt()
	if receipt == nil {
		t.Fatalf("child never started: %v", err)
	}
	select {
	case <-receipt.waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stand-in juicefs did not exit")
	}
	_ = receipt.Rollback()
	data, err := os.ReadFile(record)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(data), "\n")
	argv := lines[0]
	for _, leak := range []string{secretKey, password} {
		if strings.Contains(argv, leak) {
			t.Fatalf("argv exposes a secret: %s", argv)
		}
	}
	env := map[string][]string{}
	for _, l := range lines[1:] {
		if k, v, ok := strings.Cut(l, "="); ok {
			env[k] = append(env[k], v)
		}
	}
	if got := env["MINIO_ACCESS_KEY"]; len(got) != 1 || got[0] != accessKey {
		t.Fatalf("child MINIO_ACCESS_KEY = %q, want exactly the enrollment key", got)
	}
	if got := env["MINIO_SECRET_KEY"]; len(got) != 1 || got[0] != secretKey {
		t.Fatalf("child MINIO_SECRET_KEY = %q, want exactly the enrollment key", got)
	}
	if got := env["META_PASSWORD"]; len(got) != 1 || got[0] != password {
		t.Fatalf("child META_PASSWORD = %q", got)
	}
}

// Without an object key the child gets no MINIO_* at all (today's behavior:
// juicefs uses the key in the volume `setting`), and an inherited value never
// reaches it.
func TestH8BNoObjectKeyLeavesChildWithoutMinioEnv(t *testing.T) {
	t.Setenv("MINIO_ACCESS_KEY", "inherited-must-not-win")
	t.Setenv("MINIO_SECRET_KEY", "inherited-must-not-win")
	for _, key := range []ObjectKey{{}, {AccessKey: "jm-client"}, {SecretKey: "only-secret"}} {
		for _, entry := range juiceFSDirectEnvironment(1, "", key) {
			if strings.HasPrefix(entry, "MINIO_") {
				t.Fatalf("object key %+v: child env has %q", key.AccessKey, entry)
			}
		}
	}
}

// generationConfigDigest hashes the config; the object key must not be part
// of anything journaled, hashed or serialized.
func TestH8BObjectKeyNeverSerialized(t *testing.T) {
	a := FUSEConfig{MountPoint: "/m", ObjectKey: ObjectKey{AccessKey: "a", SecretKey: "b"}}
	b := FUSEConfig{MountPoint: "/m"}
	if generationConfigDigest(a) != generationConfigDigest(b) {
		t.Fatal("the object key changes the journaled generation config digest")
	}
}

// TestH8BChildEnvironmentClosesAWSCredentialChain: with no object key the
// juicefs minio backend falls back to the AWS SDK default chain (review
// 2026-10-06 finding 2: a probe juicefs signed with ~/.aws/credentials). The
// child must always get the file sources pointed at /dev/null and the EC2
// metadata service off, whatever the object key, and no inherited AWS_*
// variable may reach it.
func TestH8BChildEnvironmentClosesAWSCredentialChain(t *testing.T) {
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE",
		"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"} {
		t.Setenv(k, "inherited-must-not-win")
	}
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/Users/someone/.aws/credentials")
	t.Setenv("AWS_CONFIG_FILE", "/Users/someone/.aws/config")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "false")
	for _, key := range []ObjectKey{{}, {AccessKey: "jm-client", SecretKey: "clientsecret0123"}} {
		env := map[string][]string{}
		for _, entry := range juiceFSDirectEnvironment(1, "", key) {
			k, v, _ := strings.Cut(entry, "=")
			env[k] = append(env[k], v)
		}
		want := map[string]string{
			"AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
			"AWS_CONFIG_FILE":             "/dev/null",
			"AWS_EC2_METADATA_DISABLED":   "true",
		}
		for k, v := range want {
			if got := env[k]; len(got) != 1 || got[0] != v {
				t.Fatalf("object key %q: child %s = %q, want exactly %q", key.AccessKey, k, got, v)
			}
		}
		for k := range env {
			if strings.HasPrefix(k, "AWS_") && want[k] == "" {
				t.Fatalf("object key %q: inherited %s reached the child", key.AccessKey, k)
			}
		}
	}
}

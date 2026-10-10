package health

import (
	"errors"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
)

func TestIsolatedFUSEChildPathsAndLog(t *testing.T) {
	account, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("HOME") != account.HomeDir {
		t.Fatal("test requires unchanged account HOME")
	}
	base := filepath.Join(account.HomeDir, "Developer", ".state")
	if st, err := os.Stat(base); err != nil || !st.IsDir() {
		t.Skip("private fixture base unavailable")
	}
	root, err := os.MkdirTemp(base, "jm-fi-")
	if err != nil {
		t.Fatal(err)
	}
	retainRoot := false
	t.Cleanup(func() {
		if !retainRoot {
			_ = os.RemoveAll(root)
		}
	})
	keepReceipt := func(r *MountReceipt) {
		if r == nil {
			return
		}
		t.Cleanup(func() {
			err := r.Rollback()
			if err != nil || !waitFUSEChannel(r.waitDone, 2*time.Second) {
				retainRoot = true
				t.Errorf("owned child retirement incomplete; retaining %s: %v", root, err)
			}
		})
	}
	if err := os.Mkdir(filepath.Join(root, "tmp"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, appisolation.Marker), []byte("JuiceMount isolated state v1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JM_APP_ISOLATION", "1")
	t.Setenv("JM_APP_ISOLATION_ROOT", root)
	t.Setenv("TMPDIR", filepath.Join(root, "tmp"))
	t.Setenv("JM_CACHE_DIR", "")
	t.Setenv("JM_INFLIGHT_DUMP_DIR", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "synthetic-ambient-key")
	paths, isolated, err := appisolation.ResolvePaths()
	if err != nil || !isolated {
		t.Fatalf("private FUSE paths: isolated=%t err=%v", isolated, err)
	}
	if err := os.MkdirAll(paths.CacheBase, 0700); err != nil {
		t.Fatal(err)
	}

	argsPath := filepath.Join(root, "child-args")
	envPath := filepath.Join(root, "child-env")
	startedPath := filepath.Join(root, "child-started")
	bin := filepath.Join(root, "fake-juicefs")
	quote := func(value string) string {
		return "\"" + strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "$", "\\$", "`", "\\`").Replace(value) + "\""
	}
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$@\" > " + quote(argsPath) + "\n" +
		"printf 'HOME=%s\\nTMPDIR=%s\\nSTATE=%s\\nAWS_FILE=%s\\nAWS_CONFIG=%s\\nAWS_METADATA=%s\\nAMBIENT=%s\\n' \"$HOME\" \"$TMPDIR\" \"${_FUSE_STATE_PATH-}\" \"${AWS_SHARED_CREDENTIALS_FILE-}\" \"${AWS_CONFIG_FILE-}\" \"${AWS_EC2_METADATA_DISABLED-}\" \"${AWS_ACCESS_KEY_ID+present}\" > " + quote(envPath) + "\n" +
		"printf 'started\\n' > " + quote(startedPath) + "\n" +
		"printf 'private-log-marker\\n'\nexit 7\n"
	if err := os.WriteFile(bin, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	config := FUSEConfig{JuiceFSBin: bin, RedisURL: "redis://127.0.0.1:1/0", MountPoint: filepath.Join(root, "fuse-internal"), CacheDir: paths.CacheBase}
	fm := NewFUSEManager(config)
	fm.observeMount = testEmptyFUSESnapshot
	fm.custodyPreflight = func() error { return nil }
	receipt, err := fm.MountWithReceipt()
	keepReceipt(receipt)
	if err == nil || receipt == nil || fm.activeReceipt != receipt {
		t.Fatalf("fake child receipt not retained: receipt=%v err=%v", receipt, err)
	}
	select {
	case <-receipt.waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("fake child original waiter did not settle")
	}
	var exit *exec.ExitError
	if !errors.As(receipt.exitErr, &exit) || exit.ExitCode() != 7 {
		t.Fatalf("fake child original exit = %v, want 7", receipt.exitErr)
	}
	if err := receipt.Rollback(); err != nil {
		t.Fatalf("joined fake child rollback: %v", err)
	}
	if fm.activeReceipt != nil {
		t.Fatal("settled fake child receipt retained")
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "--cache-dir\n"+paths.CacheBase+"\n") {
		t.Fatalf("private --cache-dir absent from argv: %q", args)
	}
	env, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		"HOME=" + account.HomeDir, "TMPDIR=" + filepath.Join(root, "tmp"),
		"AWS_FILE=/dev/null", "AWS_CONFIG=/dev/null", "AWS_METADATA=true", "AMBIENT=",
	} {
		if !strings.Contains(string(env), field+"\n") {
			t.Fatalf("child environment missing %q: %q", field, env)
		}
	}
	state := ""
	for _, line := range strings.Split(string(env), "\n") {
		if strings.HasPrefix(line, "STATE=") {
			state = strings.TrimPrefix(line, "STATE=")
		}
	}
	if filepath.Dir(state) != root || !strings.HasPrefix(filepath.Base(state), "fuse-recovery-") || !strings.HasSuffix(state, ".json") {
		t.Fatalf("private recovery pathname absent: %q", state)
	}
	logData, err := os.ReadFile(paths.FUSELog)
	if err != nil || !strings.Contains(string(logData), "private-log-marker\n") {
		t.Fatalf("private child log absent: err=%v", err)
	}
	if st, err := os.Lstat(paths.FUSELog); err != nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 {
		t.Fatalf("private log type/mode: %v, %v", st, err)
	}

	// A planted log link is refused before a second child can execute.
	if err := os.Remove(paths.FUSELog); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(root, "log-sentinel")
	if err := os.WriteFile(outside, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, paths.FUSELog); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(startedPath); err != nil {
		t.Fatal(err)
	}
	fm = NewFUSEManager(config)
	fm.observeMount = testEmptyFUSESnapshot
	fm.custodyPreflight = func() error { return nil }
	refused, refuseErr := fm.MountWithReceipt()
	keepReceipt(refused)
	if refused != nil {
		select {
		case <-refused.waitDone:
		case <-time.After(2 * time.Second):
			t.Fatal("unexpected child did not join; fixture custody incomplete")
		}
		if err := refused.Rollback(); err != nil {
			t.Fatalf("unexpected child rollback: %v", err)
		}
	}
	if refuseErr == nil || refused != nil {
		t.Fatalf("linked private log admitted: receipt=%v err=%v", refused, refuseErr)
	}
	if _, err := os.Stat(startedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refused child executed: %v", err)
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "keep" {
		t.Fatalf("linked log target changed: %q, %v", data, err)
	}
}

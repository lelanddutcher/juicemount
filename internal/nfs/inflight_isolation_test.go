package nfs

import (
	"context"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
)

type isolatedDumpLogger struct {
	DefaultLogger
	done chan struct{}
}

func (l *isolatedDumpLogger) Errorf(format string, args ...interface{}) {
	l.DefaultLogger.Errorf(format, args...)
	if strings.Contains(format, "goroutine dump written to") {
		select {
		case l.done <- struct{}{}:
		default:
		}
	}
}

// The long-lived watchdog is confined to a fresh process. Its original wait
// joins the entire fixture, so no watchdog can outlive restored test env vars.
func TestIsolatedStallDumpPrivateProcess(t *testing.T) {
	if os.Getenv("JM_ISOLATED_DUMP_CHILD") == "1" {
		scope, isolated, err := appisolation.Current()
		if err != nil || !isolated {
			t.Fatalf("private child scope: %v", err)
		}
		paths, _, err := appisolation.ResolvePaths()
		if err != nil {
			t.Fatal(err)
		}
		logger := &isolatedDumpLogger{done: make(chan struct{}, 1)}
		SetLogger(logger)
		inflightDumpAfter = time.Millisecond
		id := inflightRegister("isolation-fixture")
		defer inflightDone(id)
		select {
		case <-logger.done:
		case <-time.After(10 * time.Second):
			t.Fatal("private watchdog did not finish dump")
		}
		entries, err := os.ReadDir(paths.StallDumps)
		if err != nil || len(entries) != 1 {
			t.Fatalf("private dump count=%d err=%v", len(entries), err)
		}
		path := filepath.Join(paths.StallDumps, entries[0].Name())
		st, err := os.Lstat(path)
		if err != nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 {
			t.Fatalf("dump mode/type: %v %v", st, err)
		}
		data, err := os.ReadFile(path)
		if err != nil || !strings.Contains(string(data), "op=isolation-fixture") || !strings.Contains(string(data), "goroutine") {
			t.Fatal("private dump is incomplete")
		}
		if err := scope.RequirePrivateChild(path, false); err != nil {
			t.Fatal(err)
		}
		t.Log("DUMP-COMPLETE private0600 after production writer close")
		return
	}
	account, err := user.Current()
	if err != nil || os.Getenv("HOME") != account.HomeDir {
		t.Fatal("account HOME must remain unchanged")
	}
	base, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Isolation forbids generic temporary storage and mounted volumes. Use
	// the checkout when the runner's temp directory is one of those paths.
	if base == "/tmp" || strings.HasPrefix(base, "/tmp/") ||
		base == "/private/tmp" || strings.HasPrefix(base, "/private/tmp/") ||
		base == "/var/tmp" || strings.HasPrefix(base, "/var/tmp/") ||
		base == "/Volumes" || strings.HasPrefix(base, "/Volumes/") {
		base, err = os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		base, err = filepath.EvalSymlinks(base)
		if err != nil {
			t.Fatal(err)
		}
	}
	root, err := os.MkdirTemp(base, "jm-dump-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	if err := os.Mkdir(filepath.Join(root, "tmp"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, appisolation.Marker), []byte("JuiceMount isolated state v1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestIsolatedStallDumpPrivateProcess$", "-test.v")
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		if key != "TMPDIR" && !strings.HasPrefix(key, "JM_") {
			cmd.Env = append(cmd.Env, item)
		}
	}
	cmd.Env = append(cmd.Env, "TMPDIR="+filepath.Join(root, "tmp"), "JM_APP_ISOLATION=1", "JM_APP_ISOLATION_ROOT="+root, "JM_ISOLATED_DUMP_CHILD=1")
	output, err := cmd.CombinedOutput()
	t.Logf("complete owned child transcript:\n%s", output)
	if err != nil || !strings.Contains(string(output), "DUMP-COMPLETE") {
		t.Fatalf("owned watchdog child failed: %v", err)
	}
	if !cmd.ProcessState.Exited() || cmd.ProcessState.ExitCode() != 0 {
		t.Fatal("original child wait did not join successfully")
	}
}

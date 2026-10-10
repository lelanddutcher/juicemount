package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

const b1RunQueueChildEnv = "JM_FARM_B1_RUNQUEUE_CHILD"

// B-1 is an EXIT-CODE bug: `docker stop` sent SIGTERM while the worker was in
// capability admission, runQueue returned 1, and the supervisor recorded a
// failed stop. The exit status is what a supervisor sees, so this runs the
// real runQueue in a child process and signals it, rather than asserting a
// helper's return value.
func TestRunQueueSignalExitStatus(t *testing.T) {
	if mode := os.Getenv(b1RunQueueChildEnv); mode != "" {
		ready := os.Getenv("JM_FARM_B1_READY")
		cfg := queueConfig{
			meta: os.Getenv("JM_FARM_TEST_REDIS_URL"), dbPath: os.Getenv("JM_FARM_B1_DB"),
			mount: os.Getenv("JM_FARM_B1_MOUNT"), producer: "b1-runqueue", version: 1,
			conc: 1, pConc: 1, vcodec: "libx264", preset: "ultrafast", role: farmqueue.QueueClassServer,
		}
		cfg.probeProfile = func(ctx context.Context, _ queueConfig, _ *derivatives.Store) (workerProfile, error) {
			if mode == "hardware" {
				return workerProfile{}, errors.New("render hardware probe failed")
			}
			_ = os.WriteFile(ready, []byte("probing"), 0o644)
			<-ctx.Done()
			return workerProfile{}, ctx.Err()
		}
		os.Exit(runQueue(cfg))
	}
	metaURL := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if metaURL == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_REDIS_URL to isolated Redis and JM_FARM_TEST_REDIS_FLUSH=1")
	}

	run := func(t *testing.T, mode string, signal bool) int {
		t.Helper()
		dir := t.TempDir()
		ready := filepath.Join(dir, "ready")
		cmd := exec.Command(os.Args[0], "-test.run=^TestRunQueueSignalExitStatus$")
		cmd.Env = append(os.Environ(),
			b1RunQueueChildEnv+"="+mode,
			"JM_FARM_B1_READY="+ready,
			"JM_FARM_B1_DB="+filepath.Join(dir, "derivatives.db"),
			"JM_FARM_B1_MOUNT="+dir,
		)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		if signal {
			deadline := time.Now().Add(20 * time.Second)
			for {
				if _, err := os.Stat(ready); err == nil {
					break
				}
				if time.Now().After(deadline) {
					_ = cmd.Process.Kill()
					t.Fatal("child never reached the capability probe")
				}
				time.Sleep(20 * time.Millisecond)
			}
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
		}
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
			t.Fatal("child did not exit")
		}
		return cmd.ProcessState.ExitCode()
	}

	t.Run("SIGTERM during probe exits 0", func(t *testing.T) {
		if code := run(t, "block", true); code != 0 {
			t.Fatalf("signalled stop during admission exited %d; want 0 so docker stop is clean", code)
		}
	})
	t.Run("probe failure without a signal exits 1", func(t *testing.T) {
		if code := run(t, "hardware", false); code != 1 {
			t.Fatalf("hardware admission failure exited %d; want 1", code)
		}
	})
}

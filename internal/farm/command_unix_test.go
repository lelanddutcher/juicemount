//go:build darwin || linux

package farm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCommandContextCancellationKillsToolProcessGroup(t *testing.T) {
	dir := t.TempDir()
	pidPath := dir + "/child.pid"
	toolPath := dir + "/fake-ffmpeg"
	// Publish only a complete PID: shell redirection creates an empty file
	// before echo writes, so file existence alone is not a readiness signal.
	script := fmt.Sprintf("#!/bin/sh\ntrap '' TERM\nsleep 60 &\nprintf '%%s\\n' \"$!\" > %q\nmv %q %q\nwait\n", pidPath+".tmp", pidPath+".tmp", pidPath)
	if err := os.WriteFile(toolPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var childPID int
	joined := false
	disappeared := false
	t.Cleanup(func() {
		cleanupOK := true
		cancel()
		join := func() {
			if joined {
				return
			}
			select {
			case <-done:
				joined = true
			case <-time.After(3 * time.Second):
			}
		}
		join()
		// A compiled parent-only cancellation negative control may leave this
		// exact test-owned descendant running after its wrapper has exited.
		if childPID > 1 && !disappeared {
			if err := syscall.Kill(childPID, 0); errors.Is(err, syscall.ESRCH) {
				disappeared = true
			} else {
				if err != nil {
					cleanupOK = false
					t.Errorf("probe descendant %d during cleanup: %v", childPID, err)
				} else if err := syscall.Kill(childPID, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
					cleanupOK = false
					t.Errorf("kill descendant %d during cleanup: %v", childPID, err)
				}
				deadline := time.Now().Add(time.Second)
				for time.Now().Before(deadline) {
					err := syscall.Kill(childPID, 0)
					if errors.Is(err, syscall.ESRCH) {
						disappeared = true
						break
					}
					if err != nil {
						cleanupOK = false
						t.Errorf("probe descendant %d after cleanup kill: %v", childPID, err)
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				if !disappeared {
					cleanupOK = false
					t.Errorf("descendant %d did not disappear during cleanup", childPID)
				}
			}
		}
		// If the first join timed out, retry after the owned-child fallback.
		join()
		if !joined {
			cleanupOK = false
			t.Error("fake media tool did not return during cleanup")
		}
		if cleanupOK && joined && disappeared {
			t.Logf("tool fixture retired: descendant=%d", childPID)
		}
	})
	go func() {
		done <- ThumbnailContext(ctx, toolPath, "source.mov", dir+"/poster.jpg", 720, 1_000)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(pidPath)
		if err == nil {
			childPID, err = strconv.Atoi(strings.TrimSpace(string(raw)))
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if childPID == 0 {
		t.Fatal("fake media tool did not start its child")
	}
	if childPID <= 1 {
		t.Fatalf("invalid fake media tool descendant PID %d", childPID)
	}
	if err := syscall.Kill(childPID, 0); err != nil {
		t.Fatalf("fake media tool descendant %d not live before cancel: %v", childPID, err)
	}
	t.Logf("live tool descendant witnessed: pid=%d", childPID)

	cancel()
	select {
	case err := <-done:
		joined = true
		if err == nil {
			t.Fatal("cancelled media tool returned nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled media tool did not return within shutdown bound")
	}

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(childPID, 0)
		if errors.Is(err, syscall.ESRCH) {
			disappeared = true
			return
		}
		if err != nil {
			t.Fatalf("probe descendant %d after cancel: %v", childPID, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("descendant process %d survived context cancellation", childPID)
}

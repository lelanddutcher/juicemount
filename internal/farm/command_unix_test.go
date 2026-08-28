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
	script := fmt.Sprintf("#!/bin/sh\ntrap '' TERM\nsleep 60 &\necho $! > %q\nwait\n", pidPath)
	if err := os.WriteFile(toolPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ThumbnailContext(ctx, toolPath, "source.mov", dir+"/poster.jpg", 720, 1_000)
	}()

	deadline := time.Now().Add(2 * time.Second)
	var childPID int
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

	cancel()
	select {
	case err := <-done:
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
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("descendant process %d survived context cancellation", childPID)
}

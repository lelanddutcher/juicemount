package mounttable

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestOutputReturnsCompletedSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := output(ctx, make(chan struct{}, 1), func() *exec.Cmd {
		return exec.Command("/bin/sh", "-c", "printf 'source on /mount (nfs)'")
	})
	if err != nil {
		t.Fatalf("output: %v", err)
	}
	if string(got) != "source on /mount (nfs)" {
		t.Fatalf("output = %q", got)
	}
}

func TestOutputDoesNotWaitPastDeadlineForBusyGate(t *testing.T) {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := output(ctx, gate, func() *exec.Cmd {
		t.Fatal("command must not start while a prior query owns the gate")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "previous query still running") {
		t.Fatalf("error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("gate wait exceeded deadline: %v", elapsed)
	}
}

func TestOutputReturnsAtCommandDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := output(ctx, make(chan struct{}, 1), func() *exec.Cmd {
		return exec.Command("/bin/sleep", "5")
	})
	if err == nil || !strings.Contains(err.Error(), "query timed out") {
		t.Fatalf("error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("command wait exceeded deadline: %v", elapsed)
	}
}

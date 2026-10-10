package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunProcessBoundedReturnsAtDeadline(t *testing.T) {
	start := time.Now()
	err := runProcessBounded(30*time.Millisecond, "sleep", "5")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runProcessBounded error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("runProcessBounded returned after %v, want a hard deadline", elapsed)
	}
}

func TestRunProcessBoundedReportsSuccessfulExit(t *testing.T) {
	if err := runProcessBounded(time.Second, "true"); err != nil {
		t.Fatalf("runProcessBounded(true) = %v", err)
	}
}

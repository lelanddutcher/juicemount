package health

import (
	"testing"
	"time"
)

// TestRunBoundedCommandReturnsAtDeadline pins the 2026-07-10 fix: a child
// that outlives the bound must not pin the caller past it (the old
// CommandContext.Run() blocked reaping an unkillable child; observed live as
// 30s-bounded diskutils surviving 20+ minutes while the remount crawled).
func TestRunBoundedCommandReturnsAtDeadline(t *testing.T) {
	start := time.Now()
	runBoundedCommand(200*time.Millisecond, "sleep", "10")
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("runBoundedCommand pinned the caller %v past a 200ms bound", el)
	}
}

// TestRunBoundedCommandFastPath: a completing command returns promptly.
func TestRunBoundedCommandFastPath(t *testing.T) {
	start := time.Now()
	runBoundedCommand(5*time.Second, "true")
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("fast command took %v", el)
	}
}

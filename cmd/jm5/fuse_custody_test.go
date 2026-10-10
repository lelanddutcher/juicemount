package main

import (
	"errors"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestCLIStartupRollbackFailureRetainsResources(t *testing.T) {
	closed := 0
	var cleanup cliStartupCleanup
	cleanup.add(func() { closed++ })
	initial := errors.New("listener failed")
	err := failCLIStartup(initial, func() error { return nil }, func() bool { return true }, func() bool { return true }, func() error {
		return errors.New("FUSE still mounted")
	}, &cleanup)
	if !errors.Is(err, initial) || !errors.Is(err, errCLICleanupIncomplete) || closed != 0 || len(cleanup.steps) != 1 {
		t.Fatalf("incomplete rollback discarded custody: err=%v closed=%d remaining=%d", err, closed, len(cleanup.steps))
	}
	err = failCLIStartup(initial, func() error { return nil }, func() bool { return true }, func() bool { return true }, func() error { return nil }, &cleanup)
	if !errors.Is(err, initial) || errors.Is(err, errCLICleanupIncomplete) || closed != 1 || len(cleanup.steps) != 0 {
		t.Fatalf("settled rollback did not retire once: err=%v closed=%d remaining=%d", err, closed, len(cleanup.steps))
	}
}

func TestCLICleanupSignalsRetryWithoutAbandoningResources(t *testing.T) {
	signals := make(chan os.Signal, 1)
	entered := make(chan int, 2)
	results := make(chan error)
	finished := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		calls := 0
		finished <- waitForCLICleanup(func() error {
			calls++
			entered <- calls
			return <-results
		}, signals, time.Hour)
	}()
	// Join the owned retry goroutine even if an assertion fails.
	t.Cleanup(func() {
		select {
		case <-done:
			return
		default:
		}
		select {
		case signals <- syscall.SIGTERM:
		default:
		}
		select {
		case results <- nil:
		case <-done:
			return
		case <-time.After(time.Second):
			t.Error("cleanup attempt not waiting")
			return
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("cleanup did not finish")
		}
	})
	if got := <-entered; got != 1 {
		t.Fatalf("first attempt %d", got)
	}
	results <- errCLICleanupIncomplete
	select {
	case err := <-finished:
		t.Fatalf("incomplete attempt exited: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	signals <- syscall.SIGTERM
	select {
	case got := <-entered:
		if got != 2 {
			t.Fatalf("signal attempt %d", got)
		}
	case <-time.After(time.Second):
		t.Fatal("signal did not wake next bounded attempt")
	}
	select {
	case err := <-finished:
		t.Fatalf("signal authorized exit: %v", err)
	default:
	}
	// Cleanup releases the second attempt and joins successful completion.
}

func TestCLICleanupRetriesAutomaticallyUntilSettled(t *testing.T) {
	calls := 0
	err := waitForCLICleanup(func() error {
		calls++
		if calls < 3 {
			return errCLICleanupIncomplete
		}
		return nil
	}, nil, time.Millisecond)
	if err != nil || calls != 3 {
		t.Fatalf("automatic cleanup: %v calls=%d", err, calls)
	}
}

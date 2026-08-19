package nfs

import (
	"sync"
	"testing"
	"time"
)

// One winner per interval under a 128-goroutine burst — the concurrency this
// exists to survive, a read with 88 RPCs in flight and all of them slow.
//
// WHAT THIS DOES NOT PROVE: that the CAS is load-bearing. Replacing it with a
// plain Store still passes, under -race and repeated runs, because the first
// writer closes the window faster than a second reader can get through it. The
// CAS is kept because a lost update here would let an unbounded number of
// goroutines log in one interval, but that failure is not reachable by a test
// and this one should not be read as evidence for it.
func TestSlowRPCLogAdmitsOneWinnerPerInterval(t *testing.T) {
	old := slowRPCLogInterval
	slowRPCLogInterval = time.Hour
	defer func() { slowRPCLogInterval = old }()
	slowRPCLogNext.Store(0)

	const n = 128
	var start, done sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	start.Add(1)
	done.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer done.Done()
			start.Wait()
			if allowSlowRPCLog() {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	start.Done()
	done.Wait()

	if wins != 1 {
		t.Fatalf("%d goroutines logged in one interval, want exactly 1 — every extra "+
			"winner is another acquisition of the process-wide logger lock", wins)
	}
}

// The gate must reopen, or a stall that outlives one interval goes unreported.
func TestSlowRPCLogReopensAfterTheInterval(t *testing.T) {
	old := slowRPCLogInterval
	slowRPCLogInterval = 10 * time.Millisecond
	defer func() { slowRPCLogInterval = old }()
	slowRPCLogNext.Store(0)

	if !allowSlowRPCLog() {
		t.Fatal("the first call must be allowed")
	}
	if allowSlowRPCLog() {
		t.Fatal("a second call inside the interval must be refused")
	}
	time.Sleep(15 * time.Millisecond)
	if !allowSlowRPCLog() {
		t.Fatal("the gate never reopened — a stall longer than one interval would go silent")
	}
}

// Zero disables the limit outright, for anyone debugging who wants every line.
func TestSlowRPCLogIntervalZeroDisablesTheLimit(t *testing.T) {
	old := slowRPCLogInterval
	slowRPCLogInterval = 0
	defer func() { slowRPCLogInterval = old }()
	for i := 0; i < 5; i++ {
		if !allowSlowRPCLog() {
			t.Fatal("interval 0 must allow every call")
		}
	}
}

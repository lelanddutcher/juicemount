// Package jmsignal is JuiceMount's signal policy for the JuiceFS mount
// process. It is not upstream JuiceFS code.
//
// Upstream v1.3.1 answers SIGTERM and SIGINT by running `umount -f <path>`:
// the unmount stops new FUSE requests, then the process flushes and closes its
// session. JuiceMount owns the kernel mount. It is the only party that may
// remove the mount, and it does so by exact identity, because a path can name
// a newer, different mount by the time a signal is handled.
//
// This package keeps upstream's ordering without the unmount:
//
//  1. Stop: the FUSE server stops reading new requests and waits for in-flight
//     ones to finish (go-fuse Server.Shutdown). Requests that arrive later wait
//     in the kernel and are never acknowledged by this process.
//  2. Flush buffered data, so every write acknowledged before step 1 ends is
//     written to object storage and metadata.
//  3. Close the metadata session, release object storage, exit.
//
// Stop is attempted ONCE. go-fuse Server.Shutdown already waits up to 10 s
// for in-flight requests and then gives up and resumes serving; retrying it
// spent the whole deadline on Stop and never flushed (2026-10-06: a request
// stuck >10 s at SIGTERM gave three failed Stops, exit 2 at 30 s, no session
// close). If Stop fails the process flushes and closes its session anyway,
// while the server is still serving: a write acknowledged after that point is
// lost when the process exits, exactly as for any crash, and JuiceMount's spool
// keeps its copy because the drainer's close never returned.
//
// The flush has a budget that ends FlushReserve before the deadline. A flush
// that cannot finish (the object store refuses or is unreachable, so the data
// can never be written) is abandoned at the budget and the session is closed
// anyway, so Redis does not keep a dead session for the stale-session sweep;
// the unflushed data is lost either way (the 2026-10-06 repro: an unwritable
// bucket; nothing a longer wait could fix).
//
// The whole sequence has a deadline (upstream's 30 s), kept as the backstop
// for a session close that itself hangs. When it expires the process exits
// with ExitDeadline without closing the session. Repeated
// signals during the sequence are logged and ignored, so a supervisor retrying
// SIGTERM cannot skip the flush; the deadline bounds the wait and SIGKILL
// remains the owner's escalation.
package jmsignal

import (
	"os"
	"time"
)

// Exit codes. None equals upstream meta.UmountCode, which tells an upstream
// supervisor to unmount the mount point.
const (
	ExitFlushed     = 0
	ExitFlushFailed = 1
	ExitDeadline    = 2
)

// DefaultDeadline matches upstream's 30 s force-exit timer.
const DefaultDeadline = 30 * time.Second

// FlushReserve is the part of the deadline kept for closing the session after
// an abandoned flush.
const FlushReserve = 5 * time.Second

// Actions are the only effects the handler can have. None unmounts.
type Actions struct {
	Stop         func() bool  // stop taking FUSE requests and drain in-flight ones; never unmount
	Flush        func() error // write buffered data to object storage and metadata
	CloseSession func() error
	Shutdown     func() // release object-storage clients
	Exit         func(code int)
	Logf         func(format string, args ...interface{})

	Deadline time.Duration                      // zero means DefaultDeadline
	After    func(time.Duration) <-chan time.Time // nil means time.After; tests replace it
}

// Handle waits for the first signal on signals, then stops, flushes and
// exits. It returns without effects if signals is closed first.
func Handle(signals <-chan os.Signal, a Actions) {
	sig, ok := <-signals
	if !ok {
		return
	}
	deadline := a.Deadline
	if deadline <= 0 {
		deadline = DefaultDeadline
	}
	after := a.After
	if after == nil {
		after = time.After
	}
	a.Logf("received signal %s: stopping FUSE requests, flushing and exiting within %s; the kernel mount is left to its owner", sig, deadline)
	expired := after(deadline)
	budget := deadline - FlushReserve
	if budget <= 0 {
		budget = deadline / 2
	}
	flushBy := after(budget)
	done := make(chan int, 1)
	go func() { done <- a.drain(flushBy, budget) }()
	for {
		select {
		case code := <-done:
			a.Exit(code)
			return
		case later, ok := <-signals:
			if !ok {
				signals = nil
				continue
			}
			a.Logf("received signal %s while stopping; still flushing (deadline %s from the first signal)", later, deadline)
		case <-expired:
			a.Logf("stop and flush did not finish within %s; exiting without closing the session (unflushed data is lost)", deadline)
			a.Exit(ExitDeadline)
			return
		}
	}
}

func (a Actions) drain(flushBy <-chan time.Time, budget time.Duration) int {
	if !a.Stop() {
		a.Logf("FUSE server did not stop (not serving yet, or a request stayed busy); flushing and closing the session while it still serves")
	}
	code := ExitFlushed
	flushed := make(chan error, 1)
	go func() { flushed <- a.Flush() }()
	select {
	case err := <-flushed:
		if err != nil {
			a.Logf("flush buffered data failed: %s", err)
			code = ExitFlushFailed
		}
	case <-flushBy:
		a.Logf("flush did not finish within %s of the signal; closing the session without it (unflushed data is lost)", budget)
		code = ExitFlushFailed
	}
	if err := a.CloseSession(); err != nil {
		a.Logf("close session: %s", err)
	}
	a.Shutdown()
	return code
}

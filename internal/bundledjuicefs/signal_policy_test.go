package bundledjuicefs

import (
	"errors"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/jmsignal"
	"github.com/juicedata/juicefs/pkg/meta"
)

type recorder struct {
	mu     sync.Mutex
	events []string
	exited chan int
}

func (r *recorder) add(event string) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}

func (r *recorder) joined() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.events, ",")
}

func (r *recorder) actions(flushErr error) jmsignal.Actions {
	return jmsignal.Actions{
		Stop:         func() bool { r.add("stop"); return true },
		Flush:        func() error { r.add("flush"); return flushErr },
		CloseSession: func() error { r.add("close-session"); return nil },
		Shutdown:     func() { r.add("shutdown") },
		Exit:         func(code int) { r.add("exit"); r.exited <- code },
		Logf:         func(string, ...interface{}) {},
	}
}

func waitExit(t *testing.T, exited <-chan int) int {
	t.Helper()
	select {
	case code := <-exited:
		return code
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not reach the exit action")
		return -1
	}
}

// A real signal delivered to this process takes the owner policy: stop FUSE
// requests, flush, close the session, release storage, exit. The policy has
// no unmount effect to call.
func TestSignalStopsFlushesAndExitsWithoutUnmount(t *testing.T) {
	for _, tc := range []struct {
		sig      syscall.Signal
		flushErr error
		code     int
	}{
		{syscall.SIGTERM, nil, jmsignal.ExitFlushed},
		{syscall.SIGINT, nil, jmsignal.ExitFlushed},
		{syscall.SIGHUP, nil, jmsignal.ExitFlushed},
		{syscall.SIGTERM, errors.New("object store refused"), jmsignal.ExitFlushFailed},
	} {
		t.Run(tc.sig.String(), func(t *testing.T) {
			signals := make(chan os.Signal, 10)
			signal.Notify(signals, tc.sig)
			defer signal.Stop(signals)
			r := &recorder{exited: make(chan int, 1)}
			done := make(chan struct{})
			go func() { jmsignal.Handle(signals, r.actions(tc.flushErr)); close(done) }()
			if err := syscall.Kill(os.Getpid(), tc.sig); err != nil {
				t.Fatal(err)
			}
			if code := waitExit(t, r.exited); code != tc.code {
				t.Fatalf("exit code = %d, want %d", code, tc.code)
			}
			<-done
			if got := r.joined(); got != "stop,flush,close-session,shutdown,exit" {
				t.Fatalf("effects = %s, want stop,flush,close-session,shutdown,exit", got)
			}
		})
	}
}

// Upstream's supervisor answers meta.UmountCode with `umount -f <path>`.
func TestExitCodesNeverRequestUnmount(t *testing.T) {
	for _, code := range []int{jmsignal.ExitFlushed, jmsignal.ExitFlushFailed, jmsignal.ExitDeadline} {
		if code == meta.UmountCode {
			t.Fatalf("exit code %d is upstream's unmount request", code)
		}
	}
	if jmsignal.DefaultDeadline != 30*time.Second {
		t.Fatalf("deadline = %s, want upstream's 30s", jmsignal.DefaultDeadline)
	}
}

// fakeServer stands in for the go-fuse server and the VFS write buffer. A
// write is acknowledged only while the server accepts requests; Flush makes
// every acknowledged write durable.
type fakeServer struct {
	mu        sync.Mutex
	accepting bool
	acked     int
	durable   int
	afterStop int // writes acknowledged after Stop returned
	stopped   bool
}

func (f *fakeServer) write() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.accepting {
		return // waits in the kernel; never acknowledged by this process
	}
	f.acked++
	if f.stopped {
		f.afterStop++
	}
}

func (f *fakeServer) stop() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.accepting = false
	f.stopped = true
	return true
}

func (f *fakeServer) flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.durable = f.acked
	return nil
}

// No write may be acknowledged after the stop step, and every acknowledged
// write must be flushed before exit. Writers keep hammering the server through
// the whole sequence, as Finder or an NLE would.
func TestNoWriteAcceptedAfterStop(t *testing.T) {
	server := &fakeServer{accepting: true}
	quit := make(chan struct{})
	var writers sync.WaitGroup
	for i := 0; i < 8; i++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for {
				select {
				case <-quit:
					return
				default:
					server.write()
				}
			}
		}()
	}
	time.Sleep(20 * time.Millisecond)
	signals := make(chan os.Signal, 1)
	exited := make(chan int, 1)
	go jmsignal.Handle(signals, jmsignal.Actions{
		Stop:  server.stop,
		Flush: server.flush,
		CloseSession: func() error {
			time.Sleep(10 * time.Millisecond) // writers still running
			return nil
		},
		Shutdown: func() {},
		Exit: func(code int) {
			server.mu.Lock()
			if server.acked != server.durable {
				t.Errorf("exit with %d acknowledged writes, %d durable: %d writes lost", server.acked, server.durable, server.acked-server.durable)
			}
			server.mu.Unlock()
			exited <- code
		},
		Logf: func(string, ...interface{}) {},
	})
	signals <- syscall.SIGTERM
	code := waitExit(t, exited)
	close(quit)
	writers.Wait()
	if code != jmsignal.ExitFlushed {
		t.Fatalf("exit code = %d", code)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.afterStop != 0 {
		t.Fatalf("%d writes acknowledged after the stop step", server.afterStop)
	}
	if server.acked == 0 {
		t.Fatal("no write was acknowledged; the test exercised nothing")
	}
}

// A Stop that fails (server not serving yet, or a request busy past go-fuse's
// own 10 s limit) is not retried: retrying spent the whole 30 s deadline on
// Stop, so the process exited 2 without flushing or closing its session. One
// attempt, then flush and close the session while the server still serves.
func TestFailedStopFlushesAndClosesAfterOneAttempt(t *testing.T) {
	r := &recorder{exited: make(chan int, 1)}
	a := r.actions(nil)
	a.Stop = func() bool { r.add("stop"); return false }
	signals := make(chan os.Signal, 1)
	go jmsignal.Handle(signals, a)
	signals <- syscall.SIGTERM
	if code := waitExit(t, r.exited); code != jmsignal.ExitFlushed {
		t.Fatalf("exit code = %d, want ExitFlushed", code)
	}
	if got := r.joined(); got != "stop,flush,close-session,shutdown,exit" {
		t.Fatalf("effects = %s, want one stop then flush and session close", got)
	}
}

// A flush that cannot finish (offline object store) ends at the deadline with
// ExitDeadline and without closing the session. A second signal during the
// flush is ignored; it neither skips the flush nor exits early.
func TestDeadlineAndRepeatedSignal(t *testing.T) {
	r := &recorder{exited: make(chan int, 1)}
	release := make(chan struct{})
	defer close(release)
	flushing := make(chan struct{})
	expire := make(chan time.Time)
	a := r.actions(nil)
	a.Flush = func() error { r.add("flush"); close(flushing); <-release; return nil }
	a.CloseSession = func() error { r.add("close-session"); <-release; return nil }
	var deadline time.Duration
	a.After = func(d time.Duration) <-chan time.Time {
		if d == jmsignal.DefaultDeadline {
			deadline = d
			return expire
		}
		return nil // the flush budget never fires here: this is the backstop
	}
	signals := make(chan os.Signal, 2)
	go jmsignal.Handle(signals, a)
	signals <- syscall.SIGTERM
	<-flushing
	signals <- syscall.SIGTERM
	select {
	case code := <-r.exited:
		t.Fatalf("second signal exited early with %d", code)
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case expire <- time.Now():
	case <-time.After(5 * time.Second):
		t.Fatal("handler does not wait on its deadline: a stuck flush would never exit")
	}
	if code := waitExit(t, r.exited); code != jmsignal.ExitDeadline {
		t.Fatalf("exit code = %d, want ExitDeadline", code)
	}
	if deadline != jmsignal.DefaultDeadline {
		t.Fatalf("deadline = %s", deadline)
	}
	if got := r.joined(); got != "stop,flush,exit" {
		t.Fatalf("effects = %s, want stop,flush,exit (no session close after the deadline)", got)
	}
}

// A flush that cannot finish (an object store that refuses every upload) is
// abandoned at its budget, FlushReserve before the deadline; the session is
// still closed and the exit reports the failed flush.
func TestFlushBudgetStillClosesSession(t *testing.T) {
	r := &recorder{exited: make(chan int, 1)}
	release := make(chan struct{})
	defer close(release)
	budgetFires := make(chan time.Time)
	a := r.actions(nil)
	a.Stop = func() bool { r.add("stop"); return false }
	flushing := make(chan struct{})
	a.Flush = func() error { r.add("flush"); close(flushing); <-release; return nil }
	var budget time.Duration
	a.After = func(d time.Duration) <-chan time.Time {
		if d == jmsignal.DefaultDeadline {
			return nil
		}
		budget = d
		return budgetFires
	}
	signals := make(chan os.Signal, 1)
	go jmsignal.Handle(signals, a)
	signals <- syscall.SIGTERM
	<-flushing
	select {
	case budgetFires <- time.Now():
	case <-time.After(5 * time.Second):
		t.Fatal("flush has no budget: an unflushable write holds the process to the deadline")
	}
	if code := waitExit(t, r.exited); code != jmsignal.ExitFlushFailed {
		t.Fatalf("exit code = %d, want ExitFlushFailed", code)
	}
	if budget != jmsignal.DefaultDeadline-jmsignal.FlushReserve {
		t.Fatalf("flush budget = %s", budget)
	}
	if got := r.joined(); got != "stop,flush,close-session,shutdown,exit" {
		t.Fatalf("effects = %s, want the session closed after the abandoned flush", got)
	}
}

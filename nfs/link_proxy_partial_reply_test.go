package nfs

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// This covers incomplete-frame resilience in addition to the retained
// any-response-byte request acknowledgement rule. It uses only owned TCP
// loopback sockets; it does not start Link, Redis, FUSE, or an NFS server.
func TestAdaptiveRedisProxyPartialReplyRecovery(t *testing.T) {
	timeout := proxyResponseTimeout("redis://127.0.0.1/0", "6379", "adaptive")
	if timeout != adaptiveRedisResponseTimeout || timeout <= 0 {
		t.Fatalf("adaptive Redis watchdog timeout = %v", timeout)
	}
	watchdogBound := timeout + timeout/4 + 500*time.Millisecond
	const request = "*1\r\n$4\r\nPING\r\n"
	const reply = "+PONG\r\n"

	t.Run("partial_reply_then_same_listener_reconnect", func(t *testing.T) {
		const prefix = "$8\r\npar"
		transmitted := make(chan time.Time, 1)
		f := newPartialReplyFixture(t, timeout,
			func(f *partialReplyFixture, c net.Conn) error {
				if err := partialReplyReadExact(c, request); err != nil {
					return err
				}
				if _, err := io.WriteString(c, prefix); err != nil {
					return err
				}
				transmitted <- time.Now()
				// Keep this incomplete response open until the proxy or test
				// closes it. No backend timer manufactures the expected EOF.
				return partialReplyExpectEOF(c)
			},
			func(f *partialReplyFixture, c net.Conn) error {
				if err := partialReplyReadExact(c, request); err != nil {
					return err
				}
				if _, err := io.WriteString(c, reply); err != nil {
					return err
				}
				return partialReplyExpectEOF(c)
			})
		address := f.proxy.listener.Addr().String()
		c := f.dial(t)
		if _, err := io.WriteString(c, request); err != nil {
			t.Fatal(err)
		}
		if err := partialReplyReadExact(c, prefix); err != nil {
			t.Fatal(err)
		}
		observed := time.Now()
		sent := <-transmitted
		// Allow one watchdog tick plus scheduler headroom after actual
		// prefix observation. A client deadline is failure, never closure.
		bound := watchdogBound
		if err := c.SetReadDeadline(observed.Add(bound)); err != nil {
			t.Fatal(err)
		}
		n, err := c.Read(make([]byte, 1))
		elapsed := time.Since(observed)
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			t.Errorf("partial RESP reply remained open beyond watchdog bound: observed=%v transmitted=%v bound=%v", elapsed, time.Since(sent), bound)
		} else if n != 0 || !errors.Is(err, io.EOF) {
			t.Errorf("partial RESP reply closure = (%d, %v), want EOF", n, err)
		} else if elapsed > bound {
			t.Errorf("partial RESP reply closed after watchdog bound: %v > %v", elapsed, bound)
		}
		_ = c.Close()
		f.waitNoFlows(t)

		// Even on the expected baseline failure, complete the owned retry
		// and custody checks through exactly the original listener.
		if got := f.proxy.listener.Addr().String(); got != address {
			t.Fatalf("proxy listener changed: %s != %s", got, address)
		}
		retry := f.dial(t)
		if _, err := io.WriteString(retry, request); err != nil {
			t.Fatal(err)
		}
		if err := partialReplyReadExact(retry, reply); err != nil {
			t.Fatalf("same-listener reconnect: %v", err)
		}
		_ = retry.Close()
		f.finish(t)
	})

	t.Run("fragmented_progress_and_completed_idle", func(t *testing.T) {
		const body = "abcdefghijklmnop"
		const complete = "$16\r\n" + body + "\r\n"
		progress := make(chan []time.Time, 1)
		f := newPartialReplyFixture(t, timeout, func(f *partialReplyFixture, c net.Conn) error {
			if err := partialReplyReadExact(c, request); err != nil {
				return err
			}
			var writes []time.Time
			write := func(s string) error {
				_, err := io.WriteString(c, s)
				writes = append(writes, time.Now())
				return err
			}
			if err := write("$16\r\n"); err != nil {
				return err
			}
			for _, b := range []byte(body) {
				if err := f.pause(timeout / 8); err != nil {
					return err
				}
				if err := write(string(b)); err != nil {
					return err
				}
			}
			if err := write("\r\n"); err != nil {
				return err
			}
			progress <- writes
			// No unsolicited bytes keep the completed connection alive.
			if err := partialReplyReadExact(c, request); err != nil {
				return err
			}
			if err := write(reply); err != nil {
				return err
			}
			return partialReplyExpectEOF(c)
		})
		c := f.dial(t)
		if _, err := io.WriteString(c, request); err != nil {
			t.Fatal(err)
		}
		if err := partialReplyReadExact(c, complete); err != nil {
			t.Fatalf("healthy fragmented reply: %v", err)
		}
		writes := <-progress
		if span := writes[len(writes)-1].Sub(writes[0]); span <= watchdogBound {
			t.Errorf("fragmented reply duration %v did not exceed watchdog bound %v", span, watchdogBound)
		}
		for i := 1; i < len(writes); i++ {
			if gap := writes[i].Sub(writes[i-1]); gap >= timeout/2 {
				t.Errorf("fixture progress gap %v is not safely below watchdog timeout %v", gap, timeout)
			}
		}
		idleStart := time.Now() // begins after the complete reply is observed
		if err := f.pause(timeout + timeout/2); err != nil {
			t.Fatal(err)
		}
		if time.Since(idleStart) <= timeout {
			t.Fatal("completed-idle control did not exceed watchdog timeout")
		}
		if _, err := io.WriteString(c, request); err != nil {
			t.Fatalf("completed idle connection: %v", err)
		}
		if err := partialReplyReadExact(c, reply); err != nil {
			t.Fatalf("completed idle connection reply: %v", err)
		}
		_ = c.Close()
		f.finish(t)
	})

	for _, tc := range []struct{ name, initial, pong string }{
		{"resp2_pubsub_completed_idle", "*3\r\n$9\r\nsubscribe\r\n$6\r\nevents\r\n:1\r\n*3\r\n$7\r\nmessage\r\n$6\r\nevents\r\n$2\r\nok\r\n", "*2\r\n$4\r\npong\r\n$0\r\n\r\n"},
		{"resp3_push_completed_idle", ">3\r\n$9\r\nsubscribe\r\n$6\r\nevents\r\n:1\r\n>3\r\n$7\r\nmessage\r\n$6\r\nevents\r\n$2\r\nok\r\n", reply},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const subscribe = "*2\r\n$9\r\nSUBSCRIBE\r\n$6\r\nevents\r\n"
			f := newPartialReplyFixture(t, timeout, func(f *partialReplyFixture, c net.Conn) error {
				if err := partialReplyReadExact(c, subscribe); err != nil {
					return err
				}
				if _, err := io.WriteString(c, tc.initial); err != nil {
					return err
				}
				if err := partialReplyReadExact(c, request); err != nil {
					return err
				}
				if _, err := io.WriteString(c, tc.pong); err != nil {
					return err
				}
				return partialReplyExpectEOF(c)
			})
			c := f.dial(t)
			if _, err := io.WriteString(c, subscribe); err != nil {
				t.Fatal(err)
			}
			if err := partialReplyReadExact(c, tc.initial); err != nil {
				t.Fatal(err)
			}
			if err := f.pause(timeout + timeout/2); err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(c, request); err != nil {
				t.Fatal(err)
			}
			if err := partialReplyReadExact(c, tc.pong); err != nil {
				t.Fatalf("completed pub/sub idle: %v", err)
			}
			_ = c.Close()
			f.finish(t)
		})
	}
}

// These deterministic pipe controls exercise the same production framing and
// watchdog functions. One backend Write is read as one chunk, so the buffered
// complete+partial boundary does not depend on TCP segmentation. The gate
// models a blocked local writer without filling platform-dependent socket buffers.
func TestRedisIncompleteReplyWatchdogControls(t *testing.T) {
	for _, gated := range []bool{false, true} {
		name := "buffered_complete_and_partial_next_frame"
		if gated {
			name = "blocked_local_writer_then_fresh_backend_interval"
		}
		t.Run(name, func(t *testing.T) {
			const timeout = 100 * time.Millisecond
			wire := "+OK\r\n$8\r\npar"
			if gated {
				wire = "$8\r\npar"
			}
			local, client := net.Pipe()
			remote, backend := net.Pipe()
			activity := &proxyResponseActivity{}
			activity.requestGen.Store(1)
			activity.lastRequest.Store(time.Now().UnixNano())
			gate := &partialReplyGateWriter{Writer: local, entered: make(chan struct{}), release: make(chan struct{}), cancel: make(chan struct{})}
			var dst io.Writer = local
			if gated {
				dst = gate
			}
			copyDone, backendDone, watchDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			stop := make(chan struct{})
			var copyErr, backendErr error
			go func() { defer close(copyDone); copyErr = copyRedisProxyResponses(dst, remote, activity) }()
			go func() { defer close(watchDone); closeProxyAfterResponseStall(local, remote, activity, timeout, stop) }()
			go func() {
				defer close(backendDone)
				if _, backendErr = io.WriteString(backend, wire); backendErr == nil {
					backendErr = partialReplyExpectEOF(backend)
				}
			}()
			t.Cleanup(func() {
				close(gate.cancel)
				close(stop)
				_ = local.Close()
				_ = client.Close()
				_ = remote.Close()
				_ = backend.Close()
				<-copyDone
				<-backendDone
				<-watchDone
			})
			if gated {
				select {
				case <-gate.entered:
				case <-time.After(time.Second):
					t.Fatal("forwarding writer was not entered")
				}
				select {
				case <-copyDone:
					t.Fatal("blocked client writer was classified as stalled backend")
				case <-watchDone:
					t.Fatal("watchdog retired a client-backpressured flow")
				case <-time.After(3 * timeout):
				}
				close(gate.release)
			}
			_ = client.SetReadDeadline(time.Now().Add(time.Second))
			if err := partialReplyReadExact(client, wire); err != nil {
				t.Fatal(err)
			}
			observed := time.Now()
			if gated {
				select {
				case <-watchDone:
					t.Fatal("no fresh backend interval after client write resumed")
				case <-time.After(timeout / 2):
				}
			}
			n, err := client.Read(make([]byte, 1))
			if n != 0 || !errors.Is(err, io.EOF) {
				t.Fatalf("incomplete frame closure = (%d, %v), want EOF", n, err)
			}
			if time.Since(observed) > 5*timeout {
				t.Error("incomplete frame exceeded bounded retirement interval")
			}
			<-copyDone
			<-backendDone
			<-watchDone
			if !errors.Is(copyErr, io.EOF) && !errors.Is(copyErr, io.ErrClosedPipe) {
				t.Errorf("framing completion: %v", copyErr)
			}
			if backendErr != nil {
				t.Errorf("backend closure: %v", backendErr)
			}
		})
	}
}

type partialReplyGateWriter struct {
	io.Writer
	entered, release, cancel chan struct{}
	once                     sync.Once
}

func (w *partialReplyGateWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	select {
	case <-w.release:
		return w.Writer.Write(p)
	case <-w.cancel:
		return 0, net.ErrClosed
	}
}

type partialReplyStep func(*partialReplyFixture, net.Conn) error

type partialReplyFixture struct {
	backend net.Listener
	proxy   *tcpProxy
	stop    chan struct{}
	done    chan struct{}
	mu      sync.Mutex
	active  net.Conn
	closing bool
	err     error // read only after done closes
}

func newPartialReplyFixture(t *testing.T, timeout time.Duration, steps ...partialReplyStep) *partialReplyFixture {
	t.Helper()
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p, err := newTCPProxyWithTimeouts(backend.Addr().String(), (&net.Dialer{Timeout: time.Second}).DialContext, 0, timeout)
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	f := &partialReplyFixture{backend: backend, proxy: p, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(f.done)
		for _, step := range steps {
			c, err := backend.Accept()
			if err != nil {
				f.err = err
				return
			}
			f.mu.Lock()
			if f.closing {
				f.mu.Unlock()
				_ = c.Close()
				return
			}
			f.active = c
			f.mu.Unlock()
			_ = c.SetDeadline(time.Now().Add(20 * time.Second))
			err = step(f, c)
			_ = c.Close()
			f.mu.Lock()
			f.active = nil
			f.mu.Unlock()
			if err != nil {
				f.err = err
				return
			}
		}
	}()
	t.Cleanup(func() {
		f.mu.Lock()
		f.closing = true
		close(f.stop)
		if f.active != nil {
			_ = f.active.Close()
		}
		f.mu.Unlock()
		_ = f.proxy.Close()
		_ = f.backend.Close()
		<-f.done // closing sockets and stop releases every fixture operation
		f.waitNoFlows(t)
	})
	return f
}

func (f *partialReplyFixture) dial(t *testing.T) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", f.proxy.listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return c
}

func (f *partialReplyFixture) pause(d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-f.stop:
		return net.ErrClosed
	}
}

func (f *partialReplyFixture) waitNoFlows(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		f.proxy.mu.Lock()
		n := len(f.proxy.conns)
		f.proxy.mu.Unlock()
		if n == 0 {
			// forward joins both io.Copy workers before its final untrack.
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("proxy forward flows retained %d connections after closure", n)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (f *partialReplyFixture) finish(t *testing.T) {
	t.Helper()
	select {
	case <-f.done:
		if f.err != nil {
			t.Errorf("backend fixture: %v", f.err)
		}
	case <-time.After(2 * time.Second):
		t.Error("backend fixture did not finish after client closure")
	}
	f.waitNoFlows(t)
}

func partialReplyReadExact(c net.Conn, want string) error {
	got := make([]byte, len(want))
	if _, err := io.ReadFull(c, got); err != nil {
		return err
	}
	if string(got) != want {
		return fmt.Errorf("ordinary reply bytes = %q, want %q", got, want)
	}
	return nil
}

func partialReplyExpectEOF(c net.Conn) error {
	n, err := c.Read(make([]byte, 1))
	if n != 0 || !errors.Is(err, io.EOF) {
		return fmt.Errorf("backend closure = (%d, %v), want EOF", n, err)
	}
	return nil
}

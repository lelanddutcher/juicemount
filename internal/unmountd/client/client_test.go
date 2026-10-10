package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeConn answers each request with fn. fn may block (to model a slow
// daemon) or return an error (to model libxpc events).
type fakeConn struct {
	fn     func(Request) (Response, error)
	events chan Event

	mu     sync.Mutex
	sent   []Request
	closed bool
}

func newFakeConn(fn func(Request) (Response, error)) *fakeConn {
	return &fakeConn{fn: fn, events: make(chan Event, 4)}
}

func (f *fakeConn) RoundTrip(ctx context.Context, v int64, body []byte) (int64, []byte, error) {
	req, err := DecodeRequest(v, body)
	if err != nil {
		return 0, nil, err
	}
	f.mu.Lock()
	f.sent = append(f.sent, req)
	f.mu.Unlock()
	type res struct {
		r   Response
		err error
	}
	ch := make(chan res, 1)
	go func() { r, err := f.fn(req); ch <- res{r, err} }()
	select {
	case x := <-ch:
		if x.err != nil {
			return 0, nil, x.err
		}
		b, err := EncodeResponse(x.r)
		return ProtocolVersion, b, err
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (f *fakeConn) Events() <-chan Event { return f.events }
func (f *fakeConn) Close() {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
}

func (f *fakeConn) requests() []Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Request(nil), f.sent...)
}

func clientWith(conns ...*fakeConn) (*Client, *int) {
	n := 0
	var mu sync.Mutex
	return New(Options{CallTimeout: 100 * time.Millisecond, ReclaimWindow: 300 * time.Millisecond,
		Dial: func(service, req string) (Conn, error) {
			mu.Lock()
			defer mu.Unlock()
			if service != MachService || req != DaemonRequirement() {
				return nil, errors.New("dialed the wrong service or without the daemon requirement")
			}
			c := conns[n]
			if n < len(conns)-1 {
				n++
			}
			return c, nil
		}}), &n
}

func TestDaemonCodesAreTypedErrors(t *testing.T) {
	for code, sentinel := range codeSentinel {
		conn := newFakeConn(func(Request) (Response, error) { return Response{Code: code, Message: "m"}, nil })
		c, _ := clientWith(conn)
		_, err := c.Status(context.Background(), "op1")
		var de *DaemonError
		if !errors.Is(err, sentinel) || !errors.As(err, &de) || de.Code != code {
			t.Errorf("code %s: err = %v, want %v as *DaemonError", code, err, sentinel)
		}
	}
}

func TestCallTimeoutIsBounded(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	conn := newFakeConn(func(Request) (Response, error) { <-block; return Response{Code: CodeOK}, nil })
	c, _ := clientWith(conn)
	start := time.Now()
	_, err := c.Status(context.Background(), "op1")
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("err = %v, want ErrTimeout", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("status took %s with a 100ms call timeout", d)
	}
	// A caller's own cancellation is reported as such, not as a timeout.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Status(ctx, "op1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ctx: err = %v", err)
	}
}

func TestRetireWaitExtendsTheDeadlineAndPendingIsTyped(t *testing.T) {
	conn := newFakeConn(func(r Request) (Response, error) {
		time.Sleep(time.Duration(r.WaitMillis) * time.Millisecond)
		return Response{Code: CodePending, Outcome: &Outcome{OpID: r.OpID, State: "BACKOFF",
			Holders: []Holder{{PID: 7, Name: "Finder"}}}}, nil
	})
	c, _ := clientWith(conn)
	out, err := c.Retire(context.Background(), "op1", RetireRequest{Liveness: LivenessAlive}, 300*time.Millisecond)
	if !errors.Is(err, ErrPending) {
		t.Fatalf("err = %v, want ErrPending (the 300ms wait must extend the 100ms call timeout)", err)
	}
	if out.State != "BACKOFF" || len(out.Holders) != 1 {
		t.Fatalf("pending outcome not returned: %+v", out)
	}
	if got := conn.requests()[0]; got.WaitMillis != 300 || got.Retire.Liveness != LivenessAlive {
		t.Fatalf("sent %+v", got)
	}
}

func TestRetireForQuitEscalatesToEjectNow(t *testing.T) {
	conn := newFakeConn(func(r Request) (Response, error) {
		if r.Retire.EjectNow {
			return Response{Code: CodeOK, Outcome: &Outcome{OpID: r.OpID, State: "RETIRED", Forced: true}}, nil
		}
		return Response{Code: CodePending, Outcome: &Outcome{OpID: r.OpID, State: "BACKOFF"}}, nil
	})
	c, _ := clientWith(conn)
	out, err := c.RetireForQuit(context.Background(), "op1", LivenessAlive, 10*time.Millisecond)
	if err != nil || out.State != "RETIRED" {
		t.Fatalf("quit: %+v, %v", out, err)
	}
	reqs := conn.requests()
	if len(reqs) != 2 || reqs[0].Retire.EjectNow || !reqs[1].Retire.EjectNow || reqs[0].WaitMillis != 10 {
		t.Fatalf("quit sequence: %+v", reqs)
	}
}

func TestSharedConnRedialsAfterUnavailable(t *testing.T) {
	dead := newFakeConn(func(Request) (Response, error) { return Response{}, ErrUnavailable })
	live := newFakeConn(func(Request) (Response, error) {
		return Response{Code: CodeOK, Outcome: &Outcome{State: "REGISTERED"}}, nil
	})
	c, _ := clientWith(dead, live)
	if _, err := c.Status(context.Background(), "op1"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("first: %v", err)
	}
	if _, err := c.Status(context.Background(), "op1"); err != nil {
		t.Fatalf("second call did not redial: %v", err)
	}
}

func TestLeaseRebindsAfterDaemonRestart(t *testing.T) {
	var mu sync.Mutex
	interrupted := true
	conn := newFakeConn(func(r Request) (Response, error) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Op {
		case OpRegister:
			return Response{Code: CodeOK, Receipt: &Receipt{OpID: r.OpID}}, nil
		case OpReclaim:
			if interrupted { // the daemon is still relaunching
				interrupted = false
				return Response{}, ErrInterrupted
			}
			return Response{Code: CodeOK, Reclaimed: true, Outcome: &Outcome{State: "REGISTERED"}}, nil
		}
		return Response{Code: CodeBadRequest}, nil
	})
	c, _ := clientWith(conn)
	reg := validRegister()
	l, rec, err := c.Register(context.Background(), RegisterRequest{OpID: reg.OpID, RegisterArgs: *reg.Register})
	if err != nil || rec.OpID != reg.OpID {
		t.Fatalf("register: %v %+v", err, rec)
	}
	defer l.Close()
	conn.events <- EventInterrupted
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n := 0
		for _, r := range conn.requests() {
			if r.Op == OpReclaim {
				n++
			}
		}
		if n >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case <-l.Lost():
		t.Fatalf("lease lost after a recoverable restart: %v", l.Err())
	case <-time.After(50 * time.Millisecond):
	}
	var reclaims int
	for _, r := range conn.requests() {
		if r.Op == OpReclaim && r.OpID == reg.OpID {
			reclaims++
		}
	}
	if reclaims != 2 {
		t.Fatalf("reclaims sent = %d, want 2 (one interrupted, one bound)", reclaims)
	}
}

func TestLeaseLostWhenReceiptGone(t *testing.T) {
	conn := newFakeConn(func(r Request) (Response, error) {
		if r.Op == OpReclaim && len(r.OpID) > 0 {
			return Response{Code: CodeOK, Outcome: &Outcome{State: "REGISTERED"}}, nil
		}
		return Response{Code: CodeBadRequest}, nil
	})
	c, _ := clientWith(conn)
	l, _, err := c.Reclaim(context.Background(), "op1")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	conn.fn = func(Request) (Response, error) { return Response{Code: CodeNoReceipt}, nil }
	conn.events <- EventInterrupted
	select {
	case <-l.Lost():
		if !errors.Is(l.Err(), ErrNoReceipt) {
			t.Fatalf("lost with %v, want ErrNoReceipt", l.Err())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lease not lost after the daemon reported no receipt")
	}
}

func TestReclaimOfFinishedOpReturnsNoLease(t *testing.T) {
	conn := newFakeConn(func(r Request) (Response, error) {
		return Response{Code: CodeNoReceipt, Outcome: &Outcome{OpID: r.OpID, State: "RETIRED"}}, nil
	})
	c, _ := clientWith(conn)
	l, res, err := c.Reclaim(context.Background(), "op1")
	if l != nil || !errors.Is(err, ErrNoReceipt) || res.Outcome.State != "RETIRED" {
		t.Fatalf("lease=%v res=%+v err=%v", l, res, err)
	}
	conn.mu.Lock()
	closed := conn.closed
	conn.mu.Unlock()
	if !closed {
		t.Fatal("unbound connection left open")
	}
}

func TestLeaseCloseIsNotLoss(t *testing.T) {
	conn := newFakeConn(func(r Request) (Response, error) {
		return Response{Code: CodeOK, Outcome: &Outcome{State: "REGISTERED"}}, nil
	})
	c, _ := clientWith(conn)
	l, _, err := c.Reclaim(context.Background(), "op1")
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	conn.events <- EventInvalid
	select {
	case <-l.Lost():
		t.Fatalf("a deliberate Close reported loss: %v", l.Err())
	case <-time.After(50 * time.Millisecond):
	}
}

func TestPeerRejectedEventLosesLease(t *testing.T) {
	conn := newFakeConn(func(r Request) (Response, error) {
		return Response{Code: CodeOK, Outcome: &Outcome{State: "REGISTERED"}}, nil
	})
	c, _ := clientWith(conn)
	l, _, err := c.Reclaim(context.Background(), "op1")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	conn.events <- EventPeerRejected
	select {
	case <-l.Lost():
		if !errors.Is(l.Err(), ErrPeerRejected) {
			t.Fatalf("lost with %v", l.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("lease survived a daemon that failed its requirement")
	}
}

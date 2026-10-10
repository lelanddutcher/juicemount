package nfscustody

// Round 7: the busy-Quit START request (HelperRetireGate.RetireStart) never
// forces and restarts the busy window at its own arrival.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

type r7Clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *r7Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *r7Clock) set(t time.Time) {
	c.mu.Lock()
	c.now = t
	c.mu.Unlock()
}

func r7Pending(context.Context) (client.Outcome, error) {
	return client.Outcome{OpID: "op", State: "BACKOFF"}, client.ErrPending
}

func r7NoCancel(context.Context) (client.Outcome, error) { return client.Outcome{OpID: "op"}, nil }

// The measured carry-over: a busy window nobody withdrew (the earlier
// panel died, or its DELETE got no answer) used to keep running, so the
// next panel's first request met the floor at once. A start request ends it
// and starts a new one at its own arrival.
func TestRetireGateR7StartRestartsCarriedOverWindow(t *testing.T) {
	t0 := time.Unix(3_000_000, 0)
	clk := &r7Clock{now: t0}
	g := HelperRetireGate{Now: clk.Now}
	for _, at := range []time.Duration{0, 20 * time.Second} { // the old panel's requests
		clk.set(t0.Add(at))
		if _, err := g.Retire(context.Background(), g.Generation(), ordinaryRR, r7Pending, r7NoCancel); !errors.Is(err, client.ErrPending) {
			t.Fatal(err)
		}
	}
	if since, _ := g.Busy(); !since.Equal(t0) {
		t.Fatalf("setup: busy since %v, want %v", since, t0)
	}
	start := t0.Add(40 * time.Second)
	clk.set(start)
	if _, err := g.RetireStart(context.Background(), g.Generation(), ordinaryRR, r7Pending, r7NoCancel); !errors.Is(err, client.ErrPending) {
		t.Fatal(err)
	}
	if since, _ := g.Busy(); !since.Equal(start) {
		t.Fatalf("CARRY-OVER: after a start at %v the busy window still begins at %v; the floor would already be met", start, since)
	}
	// Escalated requests continue the start's window.
	clk.set(start.Add(30 * time.Second))
	if _, err := g.Retire(context.Background(), g.Generation(), ordinaryRR, r7Pending, r7NoCancel); !errors.Is(err, client.ErrPending) {
		t.Fatal(err)
	}
	if since, _ := g.Busy(); !since.Equal(start) {
		t.Fatalf("escalated request restarted the window: since %v, want %v", since, start)
	}
}

// A start request never forces: a force-wanting request is refused before
// anything reaches the helper.
func TestRetireGateR7StartRefusesForce(t *testing.T) {
	var g HelperRetireGate
	for _, rr := range []client.RetireRequest{
		{Liveness: client.LivenessAlive, EjectNow: true},
		{Liveness: client.LivenessDead},
		{Liveness: client.LivenessWedged},
	} {
		sent := false
		_, err := g.RetireStart(context.Background(), g.Generation(), rr, func(context.Context) (client.Outcome, error) {
			sent = true
			return client.Outcome{OpID: "op", State: "RETIRED", Forced: true}, nil
		}, r7NoCancel)
		if !errors.Is(err, ErrHelperRetireStartForce) || sent {
			t.Fatalf("start with %+v: err %v, sent to helper %v (want refused, nothing sent)", rr, err, sent)
		}
	}
}

// An older request still in flight when a start arrives must not push the
// window back before the start: here the start's own retire fails (no busy
// observation of its own), and the old request then comes back busy with
// its earlier arrival.
func TestRetireGateR7OlderInFlightRequestCannotPredateStart(t *testing.T) {
	t0 := time.Unix(3_000_000, 0)
	clk := &r7Clock{now: t0}
	g := HelperRetireGate{Now: clk.Now}
	entered, hold := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := g.Retire(context.Background(), g.Generation(), ordinaryRR, func(context.Context) (client.Outcome, error) {
			close(entered)
			<-hold
			return r7Pending(context.Background())
		}, r7NoCancel)
		done <- err
	}()
	<-entered // the old request arrived at t0 and is with the helper
	start := t0.Add(40 * time.Second)
	clk.set(start)
	if _, err := g.RetireStart(context.Background(), g.Generation(), ordinaryRR, func(context.Context) (client.Outcome, error) {
		return client.Outcome{OpID: "op", State: "FAILED"}, errors.New("helper refused")
	}, r7NoCancel); err == nil {
		t.Fatal("start: want the helper's error")
	}
	clk.set(start.Add(2 * time.Second))
	close(hold)
	if err := <-done; !errors.Is(err, client.ErrPending) {
		t.Fatal(err)
	}
	since, _ := g.Busy()
	if since.Before(start) {
		t.Fatalf("busy window begins at %v, before the start at %v: an escalation now would force %v early", since, start, start.Sub(since))
	}
}

// The controller's start entry point runs the gate's start path: it
// restarts a carried-over window at its arrival and never sends a force.
func TestNFSStartRetireWithHelperRestartsWindowAndNeverForces(t *testing.T) {
	c, _ := helperFixture(t, "retained_retry")
	fake := &fakeUnmountd{retireErr: client.ErrPending}
	c.SetUnmountHelper(fake)
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		c.mu.Lock()
		s := c.session
		c.mu.Unlock()
		_ = c.abandonKeeper(s, 3*time.Second)
	})
	base := time.Unix(1_000_000, 0)
	now := base
	old := helperNow
	helperNow = func() time.Time { return now }
	t.Cleanup(func() { helperNow = old })
	alive := client.RetireRequest{Liveness: client.LivenessAlive}
	for i := 0; i < 2; i++ { // an earlier panel's requests, never withdrawn
		_, _ = c.RetireWithHelperAt(context.Background(), c.HelperWithdrawGeneration(), alive, time.Second)
		now = now.Add(20 * time.Second)
	}
	start := now
	if _, err := c.StartRetireWithHelperAt(context.Background(), c.HelperWithdrawGeneration(), alive, time.Second); !errors.Is(err, client.ErrPending) {
		t.Fatalf("start: %v", err)
	}
	if since, _ := c.OrdinaryBusy(); !since.Equal(start) {
		t.Fatalf("after a start at +%v the window begins at +%v", start.Sub(base), since.Sub(base))
	}
	n := len(fake.retires)
	_, err := c.StartRetireWithHelperAt(context.Background(), c.HelperWithdrawGeneration(),
		client.RetireRequest{Liveness: client.LivenessAlive, EjectNow: true}, time.Second)
	if !errors.Is(err, ErrHelperRetireStartForce) || len(fake.retires) != n {
		t.Fatalf("forcing start: err %v, helper calls %d -> %d (want refused, none sent)", err, n, len(fake.retires))
	}
}

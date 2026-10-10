package nfscustody

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

// gateHelper is a scripted helper for the gate: each retire can be held.
type gateHelper struct {
	mu       sync.Mutex
	retires  []client.RetireRequest
	cancels  int
	retireOK bool // answer RETIRED instead of ErrPending
	hold     chan struct{}
	entered  chan struct{}
}

func (h *gateHelper) retire(rr client.RetireRequest) func(context.Context) (client.Outcome, error) {
	return func(context.Context) (client.Outcome, error) {
		h.mu.Lock()
		h.retires = append(h.retires, rr)
		hold, entered, ok := h.hold, h.entered, h.retireOK
		h.mu.Unlock()
		if entered != nil {
			entered <- struct{}{}
		}
		if hold != nil {
			<-hold
		}
		if ok {
			return client.Outcome{OpID: "op", State: "RETIRED", Forced: rr.EjectNow}, nil
		}
		return client.Outcome{OpID: "op", State: "BACKOFF"}, client.ErrPending
	}
}

func (h *gateHelper) cancel(context.Context) (client.Outcome, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cancels++
	return client.Outcome{OpID: "op", State: "CANCELLED"}, nil
}

func (h *gateHelper) counts() (int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.retires), h.cancels
}

var (
	ordinaryRR = client.RetireRequest{Liveness: client.LivenessAlive}
	forceRR    = client.RetireRequest{Liveness: client.LivenessAlive, EjectNow: true}
)

// PB1 (review #3): a withdrawal between a POST's arrival and its EjectNow
// retire wins: the force is never sent.
func TestGateWithdrawBeforeForceAdmissionRefusesForce(t *testing.T) {
	var g HelperRetireGate
	h := &gateHelper{retireOK: true}
	gen := g.Generation() // the POST arrives
	if _, err := g.Withdraw(context.Background(), h.cancel); err != nil {
		t.Fatal(err)
	}
	_, err := g.Retire(context.Background(), gen, forceRR, h.retire(forceRR), h.cancel)
	if !errors.Is(err, ErrHelperRetireWithdrawn) {
		t.Fatalf("force after a withdrawal: %v", err)
	}
	if r, _ := h.counts(); r != 0 {
		t.Fatalf("a retire reached the helper after the withdrawal: %d", r)
	}
}

// PB1, other order: once the force is admitted, the withdrawal is refused
// (409 force_in_progress), and no Cancel is sent for it.
func TestGateForceInFlightRefusesWithdraw(t *testing.T) {
	var g HelperRetireGate
	h := &gateHelper{retireOK: true, hold: make(chan struct{}), entered: make(chan struct{}, 1)}
	gen := g.Generation()
	done := make(chan error, 1)
	go func() {
		_, err := g.Retire(context.Background(), gen, forceRR, h.retire(forceRR), h.cancel)
		done <- err
	}()
	<-h.entered
	if _, err := g.Withdraw(context.Background(), h.cancel); !errors.Is(err, client.ErrNotCancellable) {
		t.Fatalf("withdraw during an admitted force: %v", err)
	}
	close(h.hold)
	if err := <-done; err != nil {
		t.Fatalf("force: %v", err)
	}
	if _, c := h.counts(); c != 0 {
		t.Fatalf("a Cancel was sent against the force: %d", c)
	}
	// After the force returned, a withdrawal is accepted again.
	if _, err := g.Withdraw(context.Background(), h.cancel); err != nil {
		t.Fatalf("withdraw after the force: %v", err)
	}
}

// PB2 (review #3): an ordinary request in flight when the withdrawal lands
// may start a fresh job after the withdrawal's Cancel. On return it must
// withdraw that job again, report withdrawn, and not re-arm the busy window.
func TestGateOrdinaryCrossingWithdrawCancelsAgainNoRearm(t *testing.T) {
	var g HelperRetireGate
	h := &gateHelper{hold: make(chan struct{}), entered: make(chan struct{}, 1)}
	gen := g.Generation()
	done := make(chan error, 1)
	go func() {
		_, err := g.Retire(context.Background(), gen, ordinaryRR, h.retire(ordinaryRR), h.cancel)
		done <- err
	}()
	<-h.entered
	if _, err := g.Withdraw(context.Background(), h.cancel); err != nil {
		t.Fatal(err)
	}
	close(h.hold) // the helper answers "still busy" after the withdrawal
	if err := <-done; !errors.Is(err, ErrHelperRetireWithdrawn) {
		t.Fatalf("crossed ordinary request: %v", err)
	}
	if _, c := h.counts(); c != 2 {
		t.Fatalf("cancels=%d, want 2 (the withdrawal's and the re-withdrawal)", c)
	}
	if since, last := g.Busy(); !since.IsZero() || !last.IsZero() {
		t.Fatalf("busy window re-armed after the withdrawal: %v..%v", since, last)
	}
}

// A withdrawal never swallows a NEWER request: a request that arrives while
// the withdrawal's Cancel is still in flight reaches the helper only after
// that Cancel is done.
func TestGateWithdrawDoesNotSwallowLaterRequest(t *testing.T) {
	var g HelperRetireGate
	var cancelDone atomic.Bool
	release := make(chan struct{})
	inCancel := make(chan struct{})
	wd := make(chan struct{})
	go func() {
		defer close(wd)
		_, _ = g.Withdraw(context.Background(), func(context.Context) (client.Outcome, error) {
			close(inCancel)
			<-release
			cancelDone.Store(true)
			return client.Outcome{State: "CANCELLED"}, nil
		})
	}()
	<-inCancel
	gen := g.Generation() // arrives after the withdrawal began
	var sawCancelDone atomic.Bool
	res := make(chan error, 1)
	go func() {
		_, err := g.Retire(context.Background(), gen, ordinaryRR, func(context.Context) (client.Outcome, error) {
			sawCancelDone.Store(cancelDone.Load())
			return client.Outcome{State: "RETIRED"}, nil
		}, func(context.Context) (client.Outcome, error) { return client.Outcome{}, nil })
		res <- err
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)
	<-wd
	if err := <-res; err != nil {
		t.Fatalf("newer request refused: %v", err)
	}
	if !sawCancelDone.Load() {
		t.Fatal("the newer request reached the helper before the older withdrawal's Cancel finished")
	}
}

// DELETE on a job the helper already retired: the controller keeps the
// RETIRED proof, so a later Stop completes without the descriptor.
func TestNFSWithdrawOfRetiredJobKeepsProof(t *testing.T) {
	requireDarwinNativeTarget(t)
	c, _ := helperFixture(t, "retained_retry")
	fake := &retiredCancelHelper{fakeUnmountd: &fakeUnmountd{retireErr: client.ErrPending}}
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
	op := c.HelperOpID()
	out, err := c.WithdrawHelperRetire(context.Background())
	if err != nil || out.State != "RETIRED" || out.OpID != op {
		t.Fatalf("withdraw: %+v %v", out, err)
	}
	if p := c.helperProof(); p == nil || p.OpID != op {
		t.Fatalf("RETIRED from the withdraw not kept as proof: %+v", p)
	}
	if st := c.HelperStatus(); st.State != HelperStateRetired {
		t.Fatalf("helper state %s", st.State)
	}
}

type retiredCancelHelper struct{ *fakeUnmountd }

func (f *retiredCancelHelper) Cancel(_ context.Context, op string) (client.Outcome, error) {
	return client.Outcome{OpID: op, State: "RETIRED"}, nil
}

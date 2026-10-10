package nfscustody

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

// R4-G1 (review #4, I4): a crossed ordinary request (A) must not re-withdraw
// after a NEWER request (B, admitted after the withdrawal) reached the
// helper: B shares the job, and A's second Cancel would swallow it.
func TestProbeR4CrossedReCancelSwallowsNewerRequest(t *testing.T) {
	var g HelperRetireGate
	h := &gateHelper{hold: make(chan struct{}), entered: make(chan struct{}, 4)}
	genA := g.Generation()
	doneA := make(chan error, 1)
	go func() {
		_, err := g.Retire(context.Background(), genA, ordinaryRR, h.retire(ordinaryRR), h.cancel)
		doneA <- err
	}()
	<-h.entered // A is at the helper
	if _, err := g.Withdraw(context.Background(), h.cancel); err != nil {
		t.Fatal(err)
	}
	// B: the user's NEW request, after the withdrawal. It reaches the helper
	// (starting/joining the job) and comes back still busy.
	genB := g.Generation()
	hB := &gateHelper{}
	_, errB := g.Retire(context.Background(), genB, ordinaryRR, hB.retire(ordinaryRR), h.cancel)
	if !errors.Is(errB, client.ErrPending) {
		t.Fatalf("B: %v", errB)
	}
	_, cancelsBeforeA := h.counts()
	close(h.hold) // A returns
	<-doneA
	_, cancelsAfterA := h.counts()
	if cancelsAfterA != cancelsBeforeA {
		t.Fatalf("SWALLOWED: crossed request A sent %d Cancel(s) AFTER newer request B (gen %d) reached the helper; B's job is withdrawn",
			cancelsAfterA-cancelsBeforeA, genB)
	}
}

// R4-G2: generation arithmetic. Many withdrawals between arrival and retire
// still refuse (no wrap at realistic counts), and a request that arrived
// after N withdrawals is admitted.
func TestProbeR4GenerationManyWithdrawals(t *testing.T) {
	var g HelperRetireGate
	h := &gateHelper{retireOK: true}
	gen0 := g.Generation()
	for i := 0; i < 100000; i++ {
		if _, err := g.Withdraw(context.Background(), h.cancel); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := g.Retire(context.Background(), gen0, forceRR, h.retire(forceRR), h.cancel); !errors.Is(err, ErrHelperRetireWithdrawn) {
		t.Fatalf("old arrival admitted: %v", err)
	}
	if _, err := g.Retire(context.Background(), g.Generation(), forceRR, h.retire(forceRR), h.cancel); err != nil {
		t.Fatalf("fresh arrival refused: %v", err)
	}
}

// R4-G3: a Withdraw with no helper job (DELETE with no job) still bumps the
// generation and clears busy; the next request is admitted normally.
func TestProbeR4WithdrawNoJobThenPost(t *testing.T) {
	var g HelperRetireGate
	h := &gateHelper{}
	if _, err := g.Withdraw(context.Background(), func(context.Context) (client.Outcome, error) {
		return client.Outcome{OpID: "op", State: "REGISTERED"}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Retire(context.Background(), g.Generation(), ordinaryRR, h.retire(ordinaryRR), h.cancel); !errors.Is(err, client.ErrPending) {
		t.Fatalf("post after no-job delete: %v", err)
	}
	if since, _ := g.Busy(); since.IsZero() {
		t.Fatal("busy window not armed by the post after the delete")
	}
	if _, c := h.counts(); c != 0 {
		t.Fatalf("unexpected cancels: %d", c)
	}
}

// R5: with no newer request, a crossed request still withdraws its job again
// (the PB2 behaviour), and that Cancel runs under the ordering lock: a
// request arriving meanwhile reaches the helper only after it.
func TestGateCrossedReCancelHoldsOrderingLock(t *testing.T) {
	var g HelperRetireGate
	h := &gateHelper{hold: make(chan struct{}), entered: make(chan struct{}, 4)}
	genA := g.Generation()
	inCancel, release := make(chan struct{}), make(chan struct{})
	var reCancelDone atomic.Bool
	n := 0
	cancelA := func(ctx context.Context) (client.Outcome, error) {
		n++
		if n == 2 { // A's second Cancel
			close(inCancel)
			<-release
			reCancelDone.Store(true)
		}
		return h.cancel(ctx)
	}
	doneA := make(chan error, 1)
	go func() {
		_, err := g.Retire(context.Background(), genA, ordinaryRR, h.retire(ordinaryRR), cancelA)
		doneA <- err
	}()
	<-h.entered
	if _, err := g.Withdraw(context.Background(), cancelA); err != nil {
		t.Fatal(err)
	}
	close(h.hold)
	<-inCancel
	genB := g.Generation()
	var sawDone atomic.Bool
	resB := make(chan error, 1)
	go func() {
		_, err := g.Retire(context.Background(), genB, ordinaryRR, func(context.Context) (client.Outcome, error) {
			sawDone.Store(reCancelDone.Load())
			return client.Outcome{State: "RETIRED"}, nil
		}, h.cancel)
		resB <- err
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)
	if err := <-doneA; !errors.Is(err, ErrHelperRetireWithdrawn) {
		t.Fatalf("A: %v", err)
	}
	if err := <-resB; err != nil {
		t.Fatalf("B: %v", err)
	}
	if !sawDone.Load() {
		t.Fatal("B reached the helper while A's second Cancel was still in flight")
	}
}

// R5 (review #4, I7): an internal withdrawal never refuses a request and
// never bumps the user's generation. A force arriving before it is still
// admitted; an ordinary request whose job it ended is interrupted, not
// withdrawn; one whose job is still running stays pending; and none re-arms
// the busy window.
func TestGateInternalWithdrawNeverRefusesUser(t *testing.T) {
	var g HelperRetireGate
	h := &gateHelper{retireOK: true}
	gen := g.Generation() // the user's POST arrives
	if _, err := g.WithdrawInternal(context.Background(), h.cancel); err != nil {
		t.Fatal(err)
	}
	if g.Generation() != gen {
		t.Fatal("internal withdrawal bumped the user's generation")
	}
	if out, err := g.Retire(context.Background(), gen, forceRR, h.retire(forceRR), h.cancel); err != nil || out.State != "RETIRED" {
		t.Fatalf("user force after an internal withdrawal: %+v %v", out, err)
	}

	// Ordinary request crossed by an internal withdrawal that ends its job.
	entered, hold := make(chan struct{}), make(chan struct{})
	ended := func(context.Context) (client.Outcome, error) {
		close(entered)
		<-hold
		return client.Outcome{OpID: "op", State: "CANCELLED"}, nil
	}
	res := make(chan error, 1)
	go func() {
		_, err := g.Retire(context.Background(), g.Generation(), ordinaryRR, ended, h.cancel)
		res <- err
	}()
	<-entered
	if _, err := g.WithdrawInternal(context.Background(), h.cancel); err != nil {
		t.Fatal(err)
	}
	close(hold)
	if err := <-res; !errors.Is(err, ErrHelperRetireInterrupted) || errors.Is(err, ErrHelperRetireWithdrawn) {
		t.Fatalf("ordinary request whose job an internal withdrawal ended: %v", err)
	}

	// Ordinary request crossed by an internal withdrawal; its job lives on.
	hp := &gateHelper{hold: make(chan struct{}), entered: make(chan struct{}, 1)}
	go func() {
		_, err := g.Retire(context.Background(), g.Generation(), ordinaryRR, hp.retire(ordinaryRR), hp.cancel)
		res <- err
	}()
	<-hp.entered
	if _, err := g.WithdrawInternal(context.Background(), hp.cancel); err != nil {
		t.Fatal(err)
	}
	_, before := hp.counts()
	close(hp.hold)
	if err := <-res; !errors.Is(err, client.ErrPending) {
		t.Fatalf("ordinary request with a still-running job: %v", err)
	}
	if _, after := hp.counts(); after != before {
		t.Fatalf("internal crossing sent %d extra Cancel(s): the user's job must stand", after-before)
	}
	if since, _ := g.Busy(); !since.IsZero() {
		t.Fatalf("busy window armed by a request an internal withdrawal crossed: %v", since)
	}
}

// R5 (review #4, I7): the production controller routes a Stop's give-up
// (CancelHelperRetire) through the internal withdrawal and DELETE
// (WithdrawHelperRetire) through the user's: only the latter refuses a
// request that arrived before it.
func TestNFSInternalCancelKeepsUserGeneration(t *testing.T) {
	c, _ := helperFixture(t, "retained_retry")
	c.SetUnmountHelper(&fakeUnmountd{retireErr: client.ErrPending})
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		c.mu.Lock()
		s := c.session
		c.mu.Unlock()
		_ = c.abandonKeeper(s, 3*time.Second)
	})
	gen := c.HelperWithdrawGeneration()
	c.CancelHelperRetire()
	if got := c.HelperWithdrawGeneration(); got != gen {
		t.Fatalf("CancelHelperRetire bumped the user withdraw generation %d -> %d", gen, got)
	}
	if _, err := c.WithdrawHelperRetire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := c.HelperWithdrawGeneration(); got == gen {
		t.Fatal("DELETE's WithdrawHelperRetire did not bump the user withdraw generation")
	}
}

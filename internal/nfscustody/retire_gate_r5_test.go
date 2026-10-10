package nfscustody

// Review #5 regression tests (ported from the review's probes, asserting).

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

// jobModel is a one-job helper: a retire that lands starts (or joins) the
// job; a cancel ends it.
type jobModel struct {
	mu    sync.Mutex
	alive bool
}

func (m *jobModel) land() (client.Outcome, error) {
	m.mu.Lock()
	m.alive = true
	m.mu.Unlock()
	return client.Outcome{OpID: "op", State: "BACKOFF"}, client.ErrPending
}

func (m *jobModel) cancel(context.Context) (client.Outcome, error) {
	m.mu.Lock()
	m.alive = false
	m.mu.Unlock()
	return client.Outcome{OpID: "op", State: "CANCELLED"}, nil
}

func (m *jobModel) isAlive() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.alive
}

// V1 (I1/I3): crossed user POST A; the user's DELETE D; then JuiceMount's
// own ordinary request S (a Stop) is admitted, and its owner gives up
// (WithdrawInternal). S's admission makes A skip its re-cancel, although
// the internal withdrawal already ended S's job. A's retire, landing late,
// starts a job that no request stands behind.
func TestReviewR5AdmittedThenInternallyWithdrawnStrandsCrossedJob(t *testing.T) {
	var g HelperRetireGate
	m := &jobModel{}
	entered, hold := make(chan struct{}), make(chan struct{})
	genA := g.Generation()
	doneA := make(chan error, 1)
	go func() {
		_, err := g.Retire(context.Background(), genA, ordinaryRR, func(context.Context) (client.Outcome, error) {
			close(entered)
			<-hold
			return m.land() // A's retire reaches the helper late
		}, m.cancel)
		doneA <- err
	}()
	<-entered
	if _, err := g.Withdraw(context.Background(), m.cancel); err != nil { // user: Cancel Quit
		t.Fatal(err)
	}
	// S: JuiceMount's own ordinary retire (Stop), admitted after D.
	if _, err := g.Retire(context.Background(), g.Generation(), ordinaryRR,
		func(context.Context) (client.Outcome, error) { return m.land() }, m.cancel); !errors.Is(err, client.ErrPending) {
		t.Fatalf("S: %v", err)
	}
	if _, err := g.WithdrawInternal(context.Background(), m.cancel); err != nil { // Stop gives up
		t.Fatal(err)
	}
	close(hold)
	if err := <-doneA; !errors.Is(err, ErrHelperRetireWithdrawn) {
		t.Fatalf("A: %v", err)
	}
	if m.isAlive() {
		t.Fatal("STRANDED: the user withdrew (A answered 409), S's owner withdrew too, yet an ordinary job is still running with no request behind it")
	}
}

// V2 (founder floor): an internal withdrawal clears the busy window, so the
// force floor restarts. Measures the restart (expected by design).
func TestReviewR5InternalWithdrawRestartsFloor(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	g := HelperRetireGate{Now: func() time.Time { return now }}
	h := &gateHelper{}
	for i := 0; i < 3; i++ {
		_, _ = g.Retire(context.Background(), g.Generation(), ordinaryRR, h.retire(ordinaryRR), h.cancel)
		now = now.Add(10 * time.Second)
	}
	since, _ := g.Busy()
	if now.Sub(since) < 30*time.Second {
		t.Fatalf("floor not reached: %v", now.Sub(since))
	}
	_, _ = g.WithdrawInternal(context.Background(), h.cancel)
	if since, _ := g.Busy(); !since.IsZero() {
		t.Fatalf("internal withdrawal kept the window: %v", since)
	}
	t.Logf("an internal withdrawal at a met floor resets it: the user's Eject now waits another 30 s")
}

// V3 (deadlock / lock ordering): random interleavings of ordinary and force
// requests, user and internal withdrawals, with a slow helper. Everything
// must finish, and no force may be admitted at a generation a completed user
// withdrawal had already superseded.
func TestReviewR5StressNoDeadlock(t *testing.T) {
	var g HelperRetireGate
	var forcedAfterWithdraw atomic.Int32
	var mu sync.Mutex
	withdrawnGens := map[uint64]bool{} // generations superseded by a COMPLETED user withdrawal
	slow := func(ctx context.Context) (client.Outcome, error) {
		time.Sleep(time.Duration(rand.Intn(300)) * time.Microsecond)
		return client.Outcome{OpID: "op", State: "BACKOFF"}, client.ErrPending
	}
	cancel := func(context.Context) (client.Outcome, error) {
		time.Sleep(time.Duration(rand.Intn(300)) * time.Microsecond)
		return client.Outcome{OpID: "op", State: "CANCELLED"}, nil
	}
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < 300; i++ {
				switch r.Intn(4) {
				case 0:
					_, _ = g.Retire(context.Background(), g.Generation(), ordinaryRR, slow, cancel)
				case 1:
					gen := g.Generation()
					_, err := g.Retire(context.Background(), gen, forceRR, func(ctx context.Context) (client.Outcome, error) {
						mu.Lock()
						bad := withdrawnGens[gen]
						mu.Unlock()
						if bad {
							forcedAfterWithdraw.Add(1)
						}
						return slow(ctx)
					}, cancel)
					_ = err
				case 2:
					gen := g.Generation()
					if _, err := g.Withdraw(context.Background(), cancel); err == nil {
						mu.Lock()
						withdrawnGens[gen] = true
						mu.Unlock()
					}
				case 3:
					_, _ = g.WithdrawInternal(context.Background(), cancel)
				}
			}
		}(w)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("DEADLOCK: gate operations did not finish in 60 s")
	}
	if n := forcedAfterWithdraw.Load(); n != 0 {
		t.Fatalf("%d force(s) reached the helper at a generation a completed user withdrawal had superseded", n)
	}
}

// V4 (I4): a newer request B admitted after the user's DELETE finishes
// immediately (RETIRED is not needed; any answer) while its job keeps
// running; the crossed request A must still not cancel it.
func TestReviewR5NewerRequestFinishedBeforeCrossedReturns(t *testing.T) {
	var g HelperRetireGate
	m := &jobModel{}
	entered, hold := make(chan struct{}), make(chan struct{})
	genA := g.Generation()
	doneA := make(chan error, 1)
	go func() {
		_, err := g.Retire(context.Background(), genA, ordinaryRR, func(context.Context) (client.Outcome, error) {
			close(entered)
			<-hold
			return m.land()
		}, m.cancel)
		doneA <- err
	}()
	<-entered
	if _, err := g.Withdraw(context.Background(), m.cancel); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Retire(context.Background(), g.Generation(), ordinaryRR,
		func(context.Context) (client.Outcome, error) { return m.land() }, m.cancel); !errors.Is(err, client.ErrPending) {
		t.Fatalf("B: %v", err)
	}
	close(hold)
	<-doneA
	if !m.isAlive() {
		t.Fatal("SWALLOWED: B (202, job running) was cancelled by crossed A")
	}
}

// V5 (I5): an internal withdrawal never stops an admitted force, and does
// not clear the busy window while one runs.
func TestReviewR5InternalWithdrawDuringForceRefused(t *testing.T) {
	var g HelperRetireGate
	h := &gateHelper{retireOK: true, hold: make(chan struct{}), entered: make(chan struct{}, 1)}
	_, _ = g.Retire(context.Background(), g.Generation(), ordinaryRR, (&gateHelper{}).retire(ordinaryRR), h.cancel)
	before, _ := g.Busy()
	done := make(chan error, 1)
	go func() {
		_, err := g.Retire(context.Background(), g.Generation(), forceRR, h.retire(forceRR), h.cancel)
		done <- err
	}()
	<-h.entered
	if _, err := g.WithdrawInternal(context.Background(), h.cancel); !errors.Is(err, client.ErrNotCancellable) {
		t.Fatalf("internal withdraw during force: %v", err)
	}
	if after, _ := g.Busy(); !after.Equal(before) {
		t.Fatalf("refused internal withdrawal changed the busy window %v -> %v", before, after)
	}
	close(h.hold)
	if err := <-done; err != nil {
		t.Fatalf("force: %v", err)
	}
	if _, c := h.counts(); c != 0 {
		t.Fatalf("cancel sent during a force: %d", c)
	}
}

// V6 (I4, guards any fix for V1): B is admitted after the user's DELETE and
// is still in flight when an internal withdrawal lands; B's retire then
// revives the job and B answers 202. Crossed A must not cancel B's job.
func TestReviewR5NewerInFlightAcrossInternalWithdrawNotSwallowed(t *testing.T) {
	var g HelperRetireGate
	m := &jobModel{}
	enteredA, holdA := make(chan struct{}), make(chan struct{})
	doneA := make(chan error, 1)
	genA := g.Generation()
	go func() {
		_, err := g.Retire(context.Background(), genA, ordinaryRR, func(context.Context) (client.Outcome, error) {
			close(enteredA)
			<-holdA
			return m.land()
		}, m.cancel)
		doneA <- err
	}()
	<-enteredA
	if _, err := g.Withdraw(context.Background(), m.cancel); err != nil {
		t.Fatal(err)
	}
	enteredB, holdB := make(chan struct{}), make(chan struct{})
	doneB := make(chan error, 1)
	genB := g.Generation()
	go func() {
		_, err := g.Retire(context.Background(), genB, ordinaryRR, func(context.Context) (client.Outcome, error) {
			close(enteredB)
			<-holdB
			return m.land()
		}, m.cancel)
		doneB <- err
	}()
	<-enteredB
	if _, err := g.WithdrawInternal(context.Background(), m.cancel); err != nil {
		t.Fatal(err)
	}
	close(holdB)
	if err := <-doneB; !errors.Is(err, client.ErrPending) {
		t.Fatalf("B: %v", err)
	}
	close(holdA)
	<-doneA
	if !m.isAlive() {
		t.Fatal("SWALLOWED: B answered 202 (its job revived after the internal withdrawal), but crossed A cancelled it")
	}
}

// M9 (review #5): the crossed request reads the admission count only once it
// holds the ordering lock. B is admitted (and answered 202, its job running)
// while A waits for that lock; A must then see B and skip its re-cancel.
// Read before the lock, A would cancel B's job.
func TestReviewR5RecancelReadsAdmissionUnderOrderingLock(t *testing.T) {
	var g HelperRetireGate
	m := &jobModel{}
	entered, hold := make(chan struct{}), make(chan struct{})
	genA := g.Generation()
	doneA := make(chan error, 1)
	go func() {
		_, err := g.Retire(context.Background(), genA, ordinaryRR, func(context.Context) (client.Outcome, error) {
			close(entered)
			<-hold
			return m.land()
		}, m.cancel)
		doneA <- err
	}()
	<-entered
	if _, err := g.Withdraw(context.Background(), m.cancel); err != nil {
		t.Fatal(err)
	}
	var errB error
	fired := false
	g.beforeRecancelLock = func() {
		if fired {
			return
		}
		fired = true
		// B, a new request, is admitted while A waits for the lock.
		_, errB = g.Retire(context.Background(), g.Generation(), ordinaryRR,
			func(context.Context) (client.Outcome, error) { return m.land() }, m.cancel)
	}
	close(hold)
	if err := <-doneA; !errors.Is(err, ErrHelperRetireWithdrawn) {
		t.Fatalf("A: %v", err)
	}
	if !fired || !errors.Is(errB, client.ErrPending) {
		t.Fatalf("B never ran during A's lock wait (fired=%v, B=%v)", fired, errB)
	}
	if !m.isAlive() {
		t.Fatal("SWALLOWED: B was admitted while A waited for the ordering lock and answered 202, yet A's re-cancel ended B's job")
	}
}

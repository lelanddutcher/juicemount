package nfscustody

// Review #7 probes adopted as regression tests (round 8): repeated starts,
// an older crossed request against a start across JuiceMount's own
// withdrawal, and randomized interleavings of the request counters.

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

// RV7-G1: repeated starts. The floor counts from the MOST RECENT start.
func TestRV7RepeatedStartRestartsFloor(t *testing.T) {
	t0 := time.Unix(4_000_000, 0)
	clk := &r7Clock{now: t0}
	g := HelperRetireGate{Now: clk.Now}
	_, _ = g.RetireStart(context.Background(), g.Generation(), ordinaryRR, r7Pending, r7NoCancel)
	clk.set(t0.Add(10 * time.Second))
	_, _ = g.Retire(context.Background(), g.Generation(), ordinaryRR, r7Pending, r7NoCancel)
	s2 := t0.Add(25 * time.Second)
	clk.set(s2)
	_, _ = g.RetireStart(context.Background(), g.Generation(), ordinaryRR, r7Pending, r7NoCancel)
	clk.set(t0.Add(31 * time.Second))
	_, _ = g.Retire(context.Background(), g.Generation(), ordinaryRR, r7Pending, r7NoCancel)
	if since, _ := g.Busy(); !since.Equal(s2) {
		t.Fatalf("second start at +25s: window begins at +%v (want +25s)", since.Sub(t0))
	}
}

// RV7-G3 (rounds 6 + 7): an older request A crossed by the user's DELETE; a
// newer START S admitted after it is still in flight when JuiceMount's own
// withdrawal lands. S may revive the job and is the user's newer word, so A
// must not re-cancel. If S had already finished before the internal
// withdrawal, A must re-cancel.
func TestRV7CrossedOlderVsStartAcrossInternalWithdraw(t *testing.T) {
	for _, sFinishesFirst := range []bool{false, true} {
		var g HelperRetireGate
		var cancels atomic.Int32
		cancel := func(context.Context) (client.Outcome, error) { cancels.Add(1); return client.Outcome{OpID: "op"}, nil }
		aIn, aHold := make(chan struct{}), make(chan struct{})
		aDone := make(chan error, 1)
		go func() {
			_, err := g.Retire(context.Background(), g.Generation(), ordinaryRR, func(context.Context) (client.Outcome, error) {
				close(aIn)
				<-aHold
				return r7Pending(context.Background())
			}, cancel)
			aDone <- err
		}()
		<-aIn
		if _, err := g.Withdraw(context.Background(), cancel); err != nil {
			t.Fatal(err)
		}
		sIn, sHold := make(chan struct{}), make(chan struct{})
		sDone := make(chan error, 1)
		go func() {
			_, err := g.RetireStart(context.Background(), g.Generation(), ordinaryRR, func(context.Context) (client.Outcome, error) {
				close(sIn)
				<-sHold
				return r7Pending(context.Background())
			}, cancel)
			sDone <- err
		}()
		<-sIn
		if sFinishesFirst {
			close(sHold)
			<-sDone
		}
		if _, err := g.WithdrawInternal(context.Background(), cancel); err != nil {
			t.Fatal(err)
		}
		if !sFinishesFirst {
			close(sHold)
			<-sDone
		}
		close(aHold)
		if err := <-aDone; !errors.Is(err, ErrHelperRetireWithdrawn) {
			t.Fatalf("A: %v", err)
		}
		want := int32(2) // W1 + internal
		if sFinishesFirst {
			want = 3 // + A's own re-cancel
		}
		if got := cancels.Load(); got != want {
			t.Fatalf("sFinishesFirst=%v: cancels %d, want %d", sFinishesFirst, got, want)
		}
	}
}

// RV7-G4: randomized interleavings of start / escalate-ordinary / user and
// internal withdrawals. Invariants once quiescent: cur == 0 (no leak, no
// uint64 wrap); and Busy() never begins before the most recent admitted
// start.
func TestRV7RandomInterleavingsCounters(t *testing.T) {
	for iter := 0; iter < 200; iter++ {
		rnd := rand.New(rand.NewSource(int64(iter)))
		var clockMu sync.Mutex
		crnd := rand.New(rand.NewSource(int64(iter) + 99))
		now := time.Unix(5_000_000, 0)
		g := HelperRetireGate{Now: func() time.Time {
			clockMu.Lock()
			defer clockMu.Unlock()
			now = now.Add(time.Duration(crnd.Intn(5)) * time.Second)
			return now
		}}
		var wg sync.WaitGroup
		for i := 0; i < 12; i++ {
			kind := rnd.Intn(4)
			d := time.Duration(rnd.Intn(300)) * time.Microsecond
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(d)
				slow := func(context.Context) (client.Outcome, error) {
					time.Sleep(d)
					return r7Pending(context.Background())
				}
				switch kind {
				case 0:
					_, _ = g.RetireStart(context.Background(), g.Generation(), ordinaryRR, slow, r7NoCancel)
				case 1:
					_, _ = g.Retire(context.Background(), g.Generation(), ordinaryRR, slow, r7NoCancel)
				case 2:
					_, _ = g.Withdraw(context.Background(), r7NoCancel)
				case 3:
					_, _ = g.WithdrawInternal(context.Background(), r7NoCancel)
				}
			}()
		}
		wg.Wait()
		g.mu.Lock()
		cur, startAt, bs := g.cur, g.startAt, g.busySince
		g.mu.Unlock()
		if cur != 0 {
			t.Fatalf("iter %d: cur=%d after quiescence (leak or wrap)", iter, cur)
		}
		since, _ := g.Busy()
		if !since.IsZero() && since.Before(startAt) {
			t.Fatalf("iter %d: Busy since %v before startAt %v (raw %v)", iter, since, startAt, bs)
		}
	}
}

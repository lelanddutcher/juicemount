package nfscustody

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

// ErrHelperRetireWithdrawn: the user's withdrawal (DELETE /eject-now, the
// busy-Quit panel's Cancel Quit) arrived after this retire request did. The
// request did not force, and any ordinary job it started has been withdrawn
// again: the user's last word stands.
var ErrHelperRetireWithdrawn = errors.New("eject withdrawn while this request was in flight")

// ErrHelperRetireInterrupted: an internal withdrawal (a Stop or the stale
// retirer giving up; never the user) ended the ordinary job this request had
// joined. Nothing was forced. The request was not refused: the caller asks
// again, and the next request starts a fresh job (review #4, finding 2).
var ErrHelperRetireInterrupted = errors.New("JuiceMount withdrew its own eject attempt while this request was in flight")

// ErrHelperRetireStartForce: RetireStart was handed a force-wanting request.
// A start request never forces (round 7), so it is refused before anything
// reaches the helper.
var ErrHelperRetireStartForce = errors.New("a start eject request never forces")

// helperWithdrawTimeout bounds one helper Cancel.
const helperWithdrawTimeout = 5 * time.Second

// HelperRetireGate orders helper retire requests against withdrawals of the
// same op (review #3, finding 2). The two race in the service: the busy-Quit
// POST /eject-now and its DELETE run on separate HTTP goroutines, and a POST
// already past its checks can reach the helper after the DELETE did:
//
//   - a POST between its floor check and its EjectNow retire would force a
//     volume whose eject the DELETE had just reported withdrawn;
//   - an ordinary POST reaching the helper after the DELETE's Cancel would
//     start a fresh job nothing withdraws (it ejects the volume once the
//     apps let go, while JuiceMount keeps running), and re-arm the busy
//     window the withdrawal cleared.
//
// The gate linearises them with a generation number. A request reads the
// generation when it ARRIVES and is admitted only while it is unchanged;
// Withdraw increments it. A force-wanting request (EjectNow, or proven
// Dead/Wedged) is counted while it runs, and Withdraw refuses during it with
// client.ErrNotCancellable: the force is committed, and the helper would
// refuse the Cancel anyway. So exactly one of "the force runs and the
// withdrawal is refused" or "the withdrawal wins and the force is never
// sent" happens. An ordinary request that finishes after a withdrawal
// withdraws its job again and never re-arms the busy window.
//
// Withdraw holds the ordering lock across its helper Cancel, so a request
// that arrives after a withdrawal is admitted only once that Cancel is done:
// the Cancel can never swallow the newer request. The crossed request's own
// second Cancel runs under the same lock and is skipped once any request was
// admitted after the withdrawal (review #4, finding 1): that newer request is
// now the last word, and it shares the helper job. An internal withdrawal
// ends that job, so after one only a newer request still in flight counts
// (review #5, finding 1): one that already finished stands behind nothing.
//
// Withdraw is the USER's withdrawal. JuiceMount's own withdrawals (a Stop or
// the stale retirer giving up on the job it started) use WithdrawInternal:
// they cancel the job and clear the busy window, but never invalidate a user
// request in flight (review #4, finding 2). A user request whose job an
// internal withdrawal ended gets ErrHelperRetireInterrupted, never
// ErrHelperRetireWithdrawn, so no reply claims the user cancelled.
//
// The gate also owns the eject-now busy window (the founder's 30 s floor).
// RetireStart is the busy-Quit panel's START request (its countdown began).
// It never forces, and it restarts the window at its own arrival: no busy
// observation from before the most recent start counts toward the floor, so
// the user always gets the full countdown they were shown (round 7: a window
// nobody withdrew used to carry over, and the next panel's first request
// forced at once).
type HelperRetireGate struct {
	// Now is the clock for the busy window; nil uses helperNow.
	Now func() time.Time

	order sync.Mutex // admission vs. withdrawal; held across Withdraw's Cancel

	mu  sync.Mutex
	gen uint64 // user withdrawals
	// admitted counts the requests that may stand behind the op's job
	// since the last user withdrawal; a crossed request skips its second
	// Cancel while it is non-zero. A user withdrawal zeroes it. An internal
	// withdrawal ends the job, so it keeps only the requests still in
	// flight (cur): those may revive the job, the finished ones cannot
	// (review #5, finding 1).
	admitted uint64
	// cur counts requests admitted at the current user generation that are
	// still in flight.
	cur       uint64
	internal  uint64 // internal withdrawals; they never refuse a request
	forcing   int
	busySince time.Time
	busyLast  time.Time
	// startAt is the arrival of the most recent admitted start request.
	// Busy never reports a window that began before it.
	startAt time.Time

	// beforeRecancelLock is a test seam (see lockOrderForRecancel).
	beforeRecancelLock func()
}

// lockOrderForRecancel takes the ordering lock for a crossed request's
// second Cancel. beforeRecancelLock (tests only) runs just before it blocks,
// standing in for the time spent waiting for the lock.
func (g *HelperRetireGate) lockOrderForRecancel() {
	if g.beforeRecancelLock != nil {
		g.beforeRecancelLock()
	}
	g.order.Lock()
}

func (g *HelperRetireGate) now() time.Time {
	if g.Now != nil {
		return g.Now()
	}
	return helperNow()
}

// Generation is read when a request arrives and passed to Retire.
func (g *HelperRetireGate) Generation() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.gen
}

// Busy reports when an ordinary retire first and most recently came back
// still busy (zero: never, or since withdrawn). The window never begins
// before the most recent start request arrived.
func (g *HelperRetireGate) Busy() (since, last time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	since = g.busySince
	if !since.IsZero() && since.Before(g.startAt) {
		// The window began before the most recent start request (a window
		// nobody withdrew, or an older request still in flight at the
		// start): the floor counts from the start.
		since = g.startAt
	}
	return since, g.busyLast
}

func wantsForce(rr client.RetireRequest) bool {
	return rr.EjectNow || rr.Liveness == client.LivenessDead || rr.Liveness == client.LivenessWedged
}

// Retire runs one helper retire for a request that arrived at generation
// gen. retire issues it; cancel withdraws the op's ordinary job. It returns
// ErrHelperRetireWithdrawn (sending nothing, or after withdrawing the job it
// started) when a withdrawal came after the request's arrival, unless the
// helper had already retired the volume (that is reported as is).
func (g *HelperRetireGate) Retire(ctx context.Context, gen uint64, rr client.RetireRequest,
	retire func(context.Context) (client.Outcome, error),
	cancel func(context.Context) (client.Outcome, error)) (client.Outcome, error) {
	return g.retire(ctx, gen, rr, false, retire, cancel)
}

// RetireStart is Retire for the busy-Quit panel's start request: an
// ordinary retire after which Busy never reports a window that began before
// this request arrived, so the floor counts from it. A force-wanting request is refused with
// ErrHelperRetireStartForce and nothing is sent.
func (g *HelperRetireGate) RetireStart(ctx context.Context, gen uint64, rr client.RetireRequest,
	retire func(context.Context) (client.Outcome, error),
	cancel func(context.Context) (client.Outcome, error)) (client.Outcome, error) {
	if wantsForce(rr) {
		return client.Outcome{}, ErrHelperRetireStartForce
	}
	return g.retire(ctx, gen, rr, true, retire, cancel)
}

func (g *HelperRetireGate) retire(ctx context.Context, gen uint64, rr client.RetireRequest, start bool,
	retire func(context.Context) (client.Outcome, error),
	cancel func(context.Context) (client.Outcome, error)) (client.Outcome, error) {
	force := wantsForce(rr)
	g.order.Lock()
	g.mu.Lock()
	admitted := g.gen == gen
	internalAt := g.internal
	arrived := g.now()
	if admitted {
		g.admitted++
		g.cur++
		if force {
			g.forcing++
		}
		if start {
			// A new countdown: nothing observed before it counts (Busy).
			g.startAt = arrived
		}
	}
	g.mu.Unlock()
	g.order.Unlock()
	if !admitted {
		return client.Outcome{}, ErrHelperRetireWithdrawn
	}
	out, err := retire(ctx)
	g.mu.Lock()
	if force {
		g.forcing--
	}
	withdrawn := g.gen != gen
	if !withdrawn {
		g.cur-- // a user withdrawal already zeroed cur for older requests
	}
	interrupted := g.internal != internalAt
	if !force && !withdrawn && !interrupted && errors.Is(err, client.ErrPending) {
		// Measured from ARRIVAL: two POSTs 30 s apart always span 30 s,
		// whatever each wait's round trip took. Busy never reports a window
		// older than the most recent start request. A gap longer than
		// OrdinaryBusyMaxGap restarts the window too: that still matters
		// for escalated requests whose panel's start never reached this
		// service (it got no answer), so an old busy observation never
		// counts toward a later force floor.
		if g.busySince.IsZero() || arrived.Sub(g.busyLast) > OrdinaryBusyMaxGap {
			g.busySince = arrived
		}
		g.busyLast = g.now()
	}
	g.mu.Unlock()
	if force || (!withdrawn && !interrupted) {
		// A force cannot have been overtaken: both withdrawals refuse while
		// it runs.
		return out, err
	}
	if err == nil && out.State == mountbroker.HelperRetiredState {
		return out, nil // the volume is gone either way; report the truth
	}
	if !withdrawn {
		// Only JuiceMount's own withdrawal crossed this request; the user did
		// not cancel. A still-running (revived) job is reported as is; one the
		// withdrawal ended is reported as interrupted, and the caller asks
		// again. No busy observation was recorded for it.
		if errors.Is(err, client.ErrPending) {
			return out, err
		}
		return out, ErrHelperRetireInterrupted
	}
	// This ordinary request may have started (or revived) the job after the
	// user's withdrawal Cancel. Withdraw it again, under the ordering lock,
	// unless a request was admitted after that withdrawal: that request is
	// the user's newer word and shares the job, so cancelling would swallow
	// it (I4). A request arriving later waits for the lock and reaches the
	// helper only after this Cancel. The count is read only once the lock
	// is held: a request admitted while this one waited for the lock is
	// newer too (review #5, M9).
	g.lockOrderForRecancel()
	g.mu.Lock()
	newer := g.admitted > 0
	g.mu.Unlock()
	if newer {
		g.order.Unlock()
		return out, ErrHelperRetireWithdrawn
	}
	cctx, ccancel := context.WithTimeout(context.Background(), helperWithdrawTimeout)
	cout, cerr := cancel(cctx)
	ccancel()
	g.order.Unlock()
	if cerr == nil && cout.State == mountbroker.HelperRetiredState {
		return cout, nil
	}
	if cerr != nil {
		log.Printf("[nfs] unmount helper re-withdraw %s after a crossed withdrawal failed: %v", cout.OpID, cerr)
	}
	return out, ErrHelperRetireWithdrawn
}

// Withdraw is the user's withdrawal of the op's ordinary retirement: it
// refuses with client.ErrNotCancellable while a force-wanting request runs,
// and otherwise invalidates every request that arrived before it, clears the
// busy window, and calls cancel.
func (g *HelperRetireGate) Withdraw(ctx context.Context, cancel func(context.Context) (client.Outcome, error)) (client.Outcome, error) {
	return g.withdraw(ctx, cancel, false)
}

// WithdrawInternal is JuiceMount's own withdrawal (a Stop or the stale
// retirer giving up on the job it started). Like Withdraw it refuses while a
// force runs, clears the busy window and calls cancel under the ordering
// lock. It does NOT invalidate requests in flight: a user's eject-now still
// stands, and one whose job this ended returns ErrHelperRetireInterrupted.
func (g *HelperRetireGate) WithdrawInternal(ctx context.Context, cancel func(context.Context) (client.Outcome, error)) (client.Outcome, error) {
	return g.withdraw(ctx, cancel, true)
}

func (g *HelperRetireGate) withdraw(ctx context.Context, cancel func(context.Context) (client.Outcome, error), internal bool) (client.Outcome, error) {
	g.order.Lock()
	defer g.order.Unlock()
	g.mu.Lock()
	if g.forcing > 0 {
		g.mu.Unlock()
		return client.Outcome{}, client.ErrNotCancellable
	}
	if internal {
		g.internal++
		// The job is over: only a request still in flight can revive it,
		// and only such a request may still keep a crossed one from
		// withdrawing what it starts (review #5, finding 1).
		g.admitted = g.cur
	} else {
		g.gen++
		g.admitted = 0
		g.cur = 0
	}
	g.busySince, g.busyLast = time.Time{}, time.Time{}
	g.mu.Unlock()
	return cancel(ctx)
}

package farmqueue

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type h1ReaperGateHook struct {
	afterCommit bool
	armed       atomic.Bool
	entered     chan struct{}
	release     chan struct{}
}

func (h *h1ReaperGateHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *h1ReaperGateHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *h1ReaperGateHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		args := cmd.Args()
		if len(args) < 2 || args[0] != "eval" || args[1] != reclaimTrackedReservationScript ||
			!h.armed.CompareAndSwap(true, false) {
			return next(ctx, cmd)
		}
		if h.afterCommit {
			err := next(ctx, cmd)
			close(h.entered)
			select {
			case <-h.release:
			case <-ctx.Done():
			}
			return err
		}
		close(h.entered)
		select {
		case <-h.release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return next(ctx, cmd)
	}
}

func h1ReaperRunningChild(t *testing.T) (*Client, context.Context, Claim) {
	t.Helper()
	q, ctx := openWatchEpochTestQueue(t)
	worker := watchPlannerWorker()
	planner, child := h1ReviewTrackedChild(t, q, ctx, worker)
	if err := q.AckClaim(ctx, planner); err != nil {
		t.Fatalf("ack dispatched planner: %v", err)
	}
	claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
	if err != nil || !ok || claim.Job.ID != child.ID {
		t.Fatalf("claim child: %+v ok=%v err=%v", claim, ok, err)
	}
	if err := q.MarkClaimRunning(ctx, claim, worker); err != nil {
		t.Fatalf("start child: %v", err)
	}
	return q, ctx, claim
}

func h1AwaitReaperGate(t *testing.T, ctx context.Context, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("reaper never reached conditional Redis mutation")
	}
}

func TestH1ReaperReadThenRenewalKeepsRunningChild(t *testing.T) {
	q, ctx, claim := h1ReaperRunningChild(t)
	now, err := q.rdb.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	oldDeadline := now.Unix() + 2
	if err := q.rdb.HSet(ctx, JobHashPrefix+claim.Job.ID, "lease_expires_unix", oldDeadline).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.ZAdd(ctx, LeaseIndexKey, redis.Z{Score: float64(oldDeadline), Member: claim.Job.ID}).Err(); err != nil {
		t.Fatal(err)
	}
	gate := &h1ReaperGateHook{entered: make(chan struct{}), release: make(chan struct{})}
	gate.armed.Store(true)
	q.rdb.AddHook(gate)
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := q.RecoverAbandonedWorkers(ctx)
		done <- result{n, err}
	}()
	h1AwaitReaperGate(t, ctx, gate.entered)
	if _, err := q.RenewClaimWithDeadline(ctx, claim); err != nil {
		close(gate.release)
		t.Fatalf("renew while reaper is paused: %v", err)
	}
	for {
		redisNow, err := q.rdb.Time(ctx).Result()
		if err != nil {
			close(gate.release)
			t.Fatal(err)
		}
		if redisNow.Unix() > oldDeadline {
			break
		}
		select {
		case <-ctx.Done():
			close(gate.release)
			t.Fatal("old lease deadline did not pass")
		case <-time.After(50 * time.Millisecond):
		}
	}
	close(gate.release)
	select {
	case got := <-done:
		if got.err != nil || got.n != 0 {
			t.Fatalf("renewed child reclaimed: count=%d err=%v", got.n, got.err)
		}
	case <-ctx.Done():
		t.Fatal("reaper did not finish")
	}
	fields, err := q.rdb.HGetAll(ctx, JobHashPrefix+claim.Job.ID).Result()
	if err != nil || fields["claim_token"] != claim.Token || fields["status"] != StatusRunning {
		t.Fatalf("renewed claim changed: %+v err=%v", fields, err)
	}
	processing, err := q.rdb.LRange(ctx, claimProcessingKey(claim), 0, -1).Result()
	if err != nil || !slices.Contains(processing, claim.Raw) {
		t.Fatalf("renewed processing receipt: %q err=%v", processing, err)
	}
	ready, err := q.rdb.LRange(ctx, QueueKeyFor(&claim.Job), 0, -1).Result()
	if err != nil || len(ready) != 0 {
		t.Fatalf("renewed child also ready: %q err=%v", ready, err)
	}
}

func TestH1ReaperCommitThenRenewalLosesExpiredChild(t *testing.T) {
	q, ctx, claim := h1ReaperRunningChild(t)
	if err := q.rdb.HSet(ctx, JobHashPrefix+claim.Job.ID, "lease_expires_unix", time.Now().Add(-time.Minute).Unix()).Err(); err != nil {
		t.Fatal(err)
	}
	gate := &h1ReaperGateHook{afterCommit: true, entered: make(chan struct{}), release: make(chan struct{})}
	gate.armed.Store(true)
	q.rdb.AddHook(gate)
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := q.RecoverAbandonedWorkers(ctx)
		done <- result{n, err}
	}()
	h1AwaitReaperGate(t, ctx, gate.entered)
	if _, err := q.RenewClaimWithDeadline(ctx, claim); !errors.Is(err, ErrClaimLost) {
		close(gate.release)
		t.Fatalf("renew after reaper commit: got %v, want ErrClaimLost", err)
	}
	close(gate.release)
	select {
	case got := <-done:
		if got.err != nil || got.n != 1 {
			t.Fatalf("reaper result: count=%d err=%v", got.n, got.err)
		}
	case <-ctx.Done():
		t.Fatal("reaper did not finish")
	}
	fields, err := q.rdb.HGetAll(ctx, JobHashPrefix+claim.Job.ID).Result()
	if err != nil || fields["claim_token"] != "" || fields["status"] != StatusQueued {
		t.Fatalf("expired claim retains authority: %+v err=%v", fields, err)
	}
	processing, err := q.rdb.LRange(ctx, claimProcessingKey(claim), 0, -1).Result()
	if err != nil || slices.Contains(processing, claim.Raw) {
		t.Fatalf("expired claim still processing: %q err=%v", processing, err)
	}
	ready, err := q.rdb.LRange(ctx, QueueKeyFor(&claim.Job), 0, -1).Result()
	if err != nil || len(ready) != 1 || ready[0] != claim.Raw {
		t.Fatalf("recovered ready delivery: %q err=%v", ready, err)
	}
}

package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
	"github.com/redis/go-redis/v9"
)

// claimPublisher is the queue surface a worker uses to publish one claim's
// outcome and release its durable receipt.
type claimPublisher interface {
	CompleteClaim(ctx context.Context, claim farmqueue.Claim, status string, processed, failed int, errMsg string) error
	AckClaim(ctx context.Context, claim farmqueue.Claim) error
}

// claimPublishRetry bounds how long a worker keeps trying to publish an
// outcome through a transient Redis fault. Both writes are idempotent for the
// claim that made them, so a retry after a lost reply is safe.
type claimPublishRetry struct {
	attempts   int
	backoff    time.Duration
	maxBackoff time.Duration
}

// About eight seconds of backoff in all: long enough to ride out a dropped
// connection or a brief Redis restart, short enough that a lasting fault stops
// the worker promptly and leaves the job to the reaper.
var defaultClaimPublishRetry = claimPublishRetry{attempts: 6, backoff: 250 * time.Millisecond, maxBackoff: 4 * time.Second}

// transientRedisReplies are server error replies that clear on their own:
// a long script, a restarting or failing-over server. go-redis retries most
// of them internally but not BUSY.
var transientRedisReplies = []string{"BUSY", "LOADING", "READONLY", "MASTERDOWN", "TRYAGAIN", "CLUSTERDOWN"}

// retryableClaimWriteError reports whether retrying the same write could
// succeed. ErrClaimLost means another delivery owns the job. Any other Redis
// error reply (a script error, WRONGTYPE, NOPERM) is deterministic: the same
// call fails the same way. Network and timeout errors are transient.
func retryableClaimWriteError(err error) bool {
	if err == nil || errors.Is(err, farmqueue.ErrClaimLost) {
		return false
	}
	var reply redis.Error
	if !errors.As(err, &reply) {
		return true
	}
	for _, prefix := range transientRedisReplies {
		if redis.HasErrorPrefix(err, prefix) {
			return true
		}
	}
	return false
}

// retryClaimWrite runs op until it succeeds, fails in a way a retry cannot
// fix, or the retry budget is spent.
func retryClaimWrite(ctx context.Context, retry claimPublishRetry, op func(context.Context) error) error {
	delay := retry.backoff
	for attempt := 1; ; attempt++ {
		err := op(ctx)
		if !retryableClaimWriteError(err) || attempt >= retry.attempts {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
		if delay *= 2; delay > retry.maxBackoff {
			delay = retry.maxBackoff
		}
	}
}

// publishClaimOutcome commits the claim's terminal status and then releases its
// receipt. A non-nil error means the outcome or the release is not durable: the
// caller must stop the worker so its heartbeat lapses and the reaper can
// recover the receipt. Continuing would keep the heartbeat fresh, and the
// reaper never recovers a live worker's legacy receipt, so the job would show
// running for as long as the worker lived.
func publishClaimOutcome(ctx context.Context, q claimPublisher, claim farmqueue.Claim, retry claimPublishRetry,
	status string, processed, failed int, errMsg string) error {
	if err := retryClaimWrite(ctx, retry, func(ctx context.Context) error {
		return q.CompleteClaim(ctx, claim, status, processed, failed, errMsg)
	}); err != nil {
		return fmt.Errorf("mark-%s: %w", status, err)
	}
	if err := retryClaimWrite(ctx, retry, func(ctx context.Context) error {
		return q.AckClaim(ctx, claim)
	}); err != nil {
		return fmt.Errorf("ack %s: %w", status, err)
	}
	return nil
}

// dispatchPublisher is the queue surface a worker uses to release a parent
// whose bounded children now own its work.
type dispatchPublisher interface {
	MarkClaimDispatched(ctx context.Context, claim farmqueue.Claim) error
	AckClaim(ctx context.Context, claim farmqueue.Claim) error
}

// publishClaimDispatched marks the parent dispatched and releases its receipt
// with the same retry and stop rules as publishClaimOutcome. A tracked planner
// passes markDispatched=false: its coverage publication is the outcome.
func publishClaimDispatched(ctx context.Context, q dispatchPublisher, claim farmqueue.Claim, retry claimPublishRetry,
	markDispatched bool) error {
	if markDispatched {
		if err := retryClaimWrite(ctx, retry, func(ctx context.Context) error {
			return q.MarkClaimDispatched(ctx, claim)
		}); err != nil {
			return fmt.Errorf("mark-dispatched: %w", err)
		}
	}
	if err := retryClaimWrite(ctx, retry, func(ctx context.Context) error {
		return q.AckClaim(ctx, claim)
	}); err != nil {
		return fmt.Errorf("ack dispatched: %w", err)
	}
	return nil
}

// claimPublishStopReason names what the worker leaves behind when it stops
// after a failed publication.
func claimPublishStopReason(err error) string {
	if errors.Is(err, farmqueue.ErrClaimLost) {
		return "stopping; the claim belongs to another delivery"
	}
	return "stopping with durable claim intact"
}

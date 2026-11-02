package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

// claimPublisher is the queue surface a worker uses to publish one claim's
// outcome and release its durable receipt.
type claimPublisher interface {
	CompleteClaim(ctx context.Context, claim farmqueue.Claim, status string, processed, failed int, errMsg string) error
	AckClaim(ctx context.Context, claim farmqueue.Claim) error
}

// claimPublishRetry bounds how long a worker keeps trying to publish an
// outcome through a Redis fault. Both writes are idempotent for the claim that
// made them, so a retry after a lost reply is safe.
type claimPublishRetry struct {
	attempts   int
	backoff    time.Duration
	maxBackoff time.Duration
}

// About eight seconds of backoff in all: long enough to ride out a dropped
// connection or a brief Redis restart, short enough that a lasting fault stops
// the worker promptly and leaves the job to the reaper.
var defaultClaimPublishRetry = claimPublishRetry{attempts: 6, backoff: 250 * time.Millisecond, maxBackoff: 4 * time.Second}

// retryClaimWrite runs op until it succeeds, the claim is lost, or the retry
// budget is spent. ErrClaimLost means another delivery owns the job: retrying
// cannot succeed and must not be mistaken for a transient fault.
func retryClaimWrite(ctx context.Context, retry claimPublishRetry, op func(context.Context) error) error {
	delay := retry.backoff
	for attempt := 1; ; attempt++ {
		err := op(ctx)
		if err == nil || errors.Is(err, farmqueue.ErrClaimLost) || attempt >= retry.attempts {
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

// claimPublishStopReason names what the worker leaves behind when it stops
// after a failed publication.
func claimPublishStopReason(err error) string {
	if errors.Is(err, farmqueue.ErrClaimLost) {
		return "stopping; the claim belongs to another delivery"
	}
	return "stopping with durable claim intact"
}

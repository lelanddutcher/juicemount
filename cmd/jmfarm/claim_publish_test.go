package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

// scriptedPublisher returns the queued errors in order, then succeeds.
type scriptedPublisher struct {
	completeErrs, ackErrs []error
	completes, acks       int
}

func (p *scriptedPublisher) CompleteClaim(context.Context, farmqueue.Claim, string, int, int, string) error {
	p.completes++
	return popScriptedErr(&p.completeErrs)
}

func (p *scriptedPublisher) AckClaim(context.Context, farmqueue.Claim) error {
	p.acks++
	return popScriptedErr(&p.ackErrs)
}

func popScriptedErr(errs *[]error) error {
	if len(*errs) == 0 {
		return nil
	}
	err := (*errs)[0]
	*errs = (*errs)[1:]
	return err
}

var fastClaimPublishRetry = claimPublishRetry{attempts: 4, backoff: time.Millisecond, maxBackoff: 2 * time.Millisecond}

func TestPublishClaimOutcomeRetriesTransientFailures(t *testing.T) {
	transient := errors.New("i/o timeout")
	p := &scriptedPublisher{completeErrs: []error{transient, transient}, ackErrs: []error{transient}}
	if err := publishClaimOutcome(context.Background(), p, farmqueue.Claim{}, fastClaimPublishRetry,
		farmqueue.StatusDone, 1, 0, ""); err != nil {
		t.Fatalf("publish through transient faults: %v", err)
	}
	if p.completes != 3 || p.acks != 2 {
		t.Fatalf("complete calls = %d, ack calls = %d; want 3 and 2", p.completes, p.acks)
	}
}

func TestPublishClaimOutcomeGivesUpAfterBoundedRetries(t *testing.T) {
	down := errors.New("connection refused")
	p := &scriptedPublisher{completeErrs: []error{down, down, down, down, down, down}}
	err := publishClaimOutcome(context.Background(), p, farmqueue.Claim{}, fastClaimPublishRetry,
		farmqueue.StatusFailed, 0, 1, "boom")
	if !errors.Is(err, down) {
		t.Fatalf("persistent fault = %v; want the Redis error", err)
	}
	if p.completes != fastClaimPublishRetry.attempts || p.acks != 0 {
		t.Fatalf("complete calls = %d, ack calls = %d; want %d and no ACK",
			p.completes, p.acks, fastClaimPublishRetry.attempts)
	}
	if reason := claimPublishStopReason(err); reason != "stopping with durable claim intact" {
		t.Fatalf("stop reason = %q", reason)
	}
}

func TestPublishClaimOutcomeDoesNotRetryLostClaim(t *testing.T) {
	lost := fmt.Errorf("%w: legacy job x is no longer delivered to worker a", farmqueue.ErrClaimLost)
	for name, p := range map[string]*scriptedPublisher{
		"complete": {completeErrs: []error{lost}},
		"ack":      {ackErrs: []error{lost}},
	} {
		err := publishClaimOutcome(context.Background(), p, farmqueue.Claim{}, fastClaimPublishRetry,
			farmqueue.StatusDone, 1, 0, "")
		if !errors.Is(err, farmqueue.ErrClaimLost) {
			t.Fatalf("%s lost = %v; want ErrClaimLost", name, err)
		}
		if p.completes+p.acks != map[string]int{"complete": 1, "ack": 2}[name] {
			t.Fatalf("%s lost retried: complete=%d ack=%d", name, p.completes, p.acks)
		}
		if reason := claimPublishStopReason(err); reason == "stopping with durable claim intact" {
			t.Fatalf("%s lost claim reported as intact", name)
		}
	}
}

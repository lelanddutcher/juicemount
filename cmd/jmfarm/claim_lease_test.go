package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestClaimLeaseGuardCancelsOnConfirmedLoss(t *testing.T) {
	lostErr := errors.New("claim lost")
	g := startClaimLeaseGuard(context.Background(), time.Now().Add(time.Second), time.Millisecond,
		nil, func(context.Context) (time.Time, error) { return time.Time{}, lostErr },
		func(err error) bool { return errors.Is(err, lostErr) })
	select {
	case <-g.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("execution remained active after confirmed claim loss")
	}
	if !g.Stop() {
		t.Fatal("confirmed claim loss was not recorded")
	}
}

func TestClaimLeaseGuardCancelsAfterUncertainDeadline(t *testing.T) {
	uncertain := errors.New("redis unavailable")
	g := startClaimLeaseGuard(context.Background(), time.Now().Add(30*time.Millisecond), time.Millisecond,
		nil, func(context.Context) (time.Time, error) { return time.Time{}, uncertain },
		func(error) bool { return false })
	select {
	case <-g.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("execution outlived its last confirmed lease")
	}
	if !g.Stop() {
		t.Fatal("uncertain deadline loss was not recorded")
	}
}

func TestClaimLeaseGuardExtendsOnlyOnConfirmedRenewal(t *testing.T) {
	renewed := make(chan struct{}, 1)
	g := startClaimLeaseGuard(context.Background(), time.Now().Add(30*time.Millisecond), time.Millisecond,
		nil, func(context.Context) (time.Time, error) {
			select {
			case renewed <- struct{}{}:
			default:
			}
			return time.Now().Add(time.Second), nil
		}, func(error) bool { return false })
	select {
	case <-renewed:
	case <-time.After(time.Second):
		t.Fatal("renewal did not run")
	}
	select {
	case <-g.Context().Done():
		t.Fatal("execution canceled before confirmed extension expired")
	case <-time.After(60 * time.Millisecond):
	}
	if g.Stop() {
		t.Fatal("clean stop was reported as lost ownership")
	}
}

func TestClaimLeaseGuardRenewsBeforeBlockedHeartbeatPulseCompletes(t *testing.T) {
	deadline := time.Now().Add(300 * time.Millisecond)
	pulseStarted := make(chan struct{})
	releasePulse := make(chan struct{})
	renewed := make(chan time.Time, 1)
	g := startClaimLeaseGuard(context.Background(), deadline, time.Millisecond,
		func(context.Context) {
			select {
			case <-pulseStarted:
			default:
				close(pulseStarted)
			}
			<-releasePulse
		}, func(ctx context.Context) (time.Time, error) {
			select {
			case <-pulseStarted:
			case <-ctx.Done():
				return time.Time{}, ctx.Err()
			}
			confirmed := time.Now().Add(time.Second)
			select {
			case renewed <- time.Now():
			default:
			}
			return confirmed, nil
		}, func(error) bool { return false })
	defer func() {
		close(releasePulse)
		g.Stop()
	}()

	select {
	case <-pulseStarted:
	case <-time.After(time.Until(deadline)):
		t.Fatal("heartbeat pulse did not start before the lease deadline")
	}

	select {
	case renewedAt := <-renewed:
		if !renewedAt.Before(deadline) {
			t.Fatalf("lease renewal began at %s, after confirmed deadline %s", renewedAt, deadline)
		}
	case <-time.After(time.Until(deadline)):
		t.Fatal("blocked heartbeat pulse delayed lease renewal past the confirmed deadline")
	}
}

package health

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReachabilityOwnerProbeRequiresRemoteSuccessWithinDeadline(t *testing.T) {
	var observed time.Duration
	r := NewReachability("remote.invalid:6379", WithDialTimeout(20*time.Millisecond),
		WithRTTObserver(func(sample time.Duration) { observed = sample }),
		WithProbe(func(ctx context.Context) (time.Duration, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Error("owner probe had no deadline")
			}
			return 450 * time.Microsecond, nil
		}))
	if !r.probe() || observed != 450*time.Microsecond {
		t.Fatal("remote protocol witness not passed to adaptive observer")
	}
	r.remoteProbe = func(context.Context) (time.Duration, error) { return 0, errors.New("remote Redis refused") }
	if r.probe() {
		t.Fatal("failed owner probe claimed reachable")
	}
	r.remoteProbe = func(ctx context.Context) (time.Duration, error) {
		<-ctx.Done()
		return time.Millisecond, nil // a late positive result must not override the deadline
	}
	if r.probe() {
		t.Fatal("deadline-expired owner proof claimed reachable")
	}
}

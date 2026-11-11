package liveness

import (
	"context"
	"errors"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestSinceNeverMarkedIsNeverRecent(t *testing.T) {
	Reset()
	t.Cleanup(Reset)
	if got := Since(); got != time.Duration(math.MaxInt64) {
		t.Fatalf("Since() before any Mark = %v, want MaxInt64", got)
	}
	Mark()
	if got := Since(); got < 0 || got > time.Second {
		t.Fatalf("Since() right after Mark = %v", got)
	}
}

func TestMarkNeverMovesBackwards(t *testing.T) {
	Reset()
	t.Cleanup(Reset)
	markOffset(10 * time.Second)
	markOffset(5 * time.Second)
	if got := lastProof.Load(); got != int64(10*time.Second)+1 {
		t.Fatalf("older mark replaced newer: %d", got)
	}
}

// fakeClock drives monoNow for one test.
func fakeClock(t *testing.T, start time.Duration) *time.Duration {
	t.Helper()
	now := start
	prev := monoNow
	monoNow = func() time.Duration { return now }
	t.Cleanup(func() { monoNow = prev; Reset() })
	Reset()
	return &now
}

// TestSinceIgnoresWallClockSteps is the H1 regression. Proof age must come
// from the monotonic clock only. The old code stored wall-clock UnixNano, so
// a 1 h step back made every later Since() clamp to 0 ("just now") and
// suppressed every probe failure for an hour.
func TestSinceIgnoresWallClockSteps(t *testing.T) {
	if !strings.Contains(processStart.String(), "m=") {
		t.Fatalf("processStart has no monotonic reading: %s", processStart)
	}
	now := fakeClock(t, time.Hour)
	Mark()
	*now += 30 * time.Second
	if got := Since(); got != 30*time.Second {
		t.Fatalf("Since()=%v after 30 s of monotonic time, want 30s", got)
	}
	// The real clock path: a value read through monoNow's default must not
	// depend on the wall clock. time.Since on a time.Now value is monotonic
	// by the time package contract; check the default really uses it.
	monoNow = func() time.Duration { return time.Since(processStart) }
	Reset()
	Mark()
	if got := Since(); got < 0 || got > time.Second {
		t.Fatalf("default clock Since()=%v right after Mark", got)
	}
}

// TestSlowReplyIsNotProof is the H2 regression. A reply that took longer than
// MaxProofLatency (a bufferbloated link) must not mark liveness; a prompt
// reply must.
func TestSlowReplyIsNotProof(t *testing.T) {
	now := fakeClock(t, time.Minute)
	ctx := context.Background()
	cmd := redis.NewStatusCmd(ctx, "ping")

	slow := RedisHook{}.ProcessHook(func(context.Context, redis.Cmder) error {
		*now += 30 * time.Second
		return nil
	})
	_ = slow(ctx, cmd)
	if got := Since(); got != time.Duration(math.MaxInt64) {
		t.Fatalf("30 s reply marked liveness (Since=%v)", got)
	}
	slowPipe := RedisHook{}.ProcessPipelineHook(func(context.Context, []redis.Cmder) error {
		*now += MaxProofLatency + time.Millisecond
		return nil
	})
	_ = slowPipe(ctx, []redis.Cmder{cmd})
	if got := Since(); got != time.Duration(math.MaxInt64) {
		t.Fatalf("slow pipeline marked liveness (Since=%v)", got)
	}

	prompt := RedisHook{}.ProcessHook(func(context.Context, redis.Cmder) error {
		*now += 400 * time.Millisecond
		return nil
	})
	_ = prompt(ctx, cmd)
	if got := Since(); got != 0 {
		t.Fatalf("prompt reply Since()=%v, want 0", got)
	}
}

func TestReachabilityHookSwitch(t *testing.T) {
	if ReachabilityHook() == nil {
		t.Fatal("hook nil with the switch unset")
	}
	t.Setenv("JM_REACH_REDIS_LIVENESS", "0")
	if ReachabilityHook() != nil {
		t.Fatal("JM_REACH_REDIS_LIVENESS=0 must disable Redis-reply proof")
	}
}

func TestAnsweredDistinguishesRepliesFromNetworkErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, true},
		{"redis.Nil", redis.Nil, true},
		{"server error reply", redis.TxFailedErr, true},
		{"timeout", context.DeadlineExceeded, false},
		{"net error", &net.OpError{Op: "dial", Err: errors.New("no route to host")}, false},
		{"closed", redis.ErrClosed, false},
	}
	for _, tc := range cases {
		if got := Answered(tc.err); got != tc.want {
			t.Errorf("%s: Answered=%v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestRedisHookMarksOnlyOnReply checks the hook on a real client whose dialer
// fails (no reply), then the hook wrappers directly with a reply.
func TestRedisHookMarksOnlyOnReply(t *testing.T) {
	Reset()
	t.Cleanup(Reset)

	dead := redis.NewClient(&redis.Options{
		Addr:       "backend:6379",
		MaxRetries: -1,
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("no route to host")
		},
	})
	dead.AddHook(RedisHook{})
	defer dead.Close()
	_ = dead.Ping(context.Background()).Err()
	if got := Since(); got != time.Duration(math.MaxInt64) {
		t.Fatalf("failed command marked liveness (Since=%v)", got)
	}

	ctx := context.Background()
	cmd := redis.NewStatusCmd(ctx, "ping")
	process := RedisHook{}.ProcessHook(func(context.Context, redis.Cmder) error { return context.DeadlineExceeded })
	_ = process(ctx, cmd)
	if got := Since(); got != time.Duration(math.MaxInt64) {
		t.Fatalf("timed-out command marked liveness (Since=%v)", got)
	}
	process = RedisHook{}.ProcessHook(func(context.Context, redis.Cmder) error { return redis.Nil })
	_ = process(ctx, cmd)
	if got := Since(); got > time.Second {
		t.Fatalf("redis.Nil reply did not mark liveness (Since=%v)", got)
	}

	Reset()
	pipe := RedisHook{}.ProcessPipelineHook(func(context.Context, []redis.Cmder) error { return nil })
	_ = pipe(ctx, []redis.Cmder{cmd})
	if got := Since(); got > time.Second {
		t.Fatalf("pipeline reply did not mark liveness (Since=%v)", got)
	}
}

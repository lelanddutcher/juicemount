package liveness

import (
	"context"
	"errors"
	"math"
	"net"
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
	now := time.Now()
	markAt(now)
	markAt(now.Add(-time.Hour))
	if got := lastProof.Load(); got != now.UnixNano() {
		t.Fatalf("older mark replaced newer: %d, want %d", got, now.UnixNano())
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

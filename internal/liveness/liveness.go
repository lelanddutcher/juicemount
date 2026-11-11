// Package liveness records when the backend last answered real traffic.
//
// The reachability monitor (health.Reachability) sends a small probe every
// couple of seconds. On a lossy link a probe can fail while ordinary traffic
// still flows: one lost SYN or PING costs a full TCP retransmit. A Redis
// command that returns a reply is stronger evidence than any probe, because a
// real Redis server produced that reply over the same link. This package keeps
// one process-wide timestamp of the most recent such reply so the monitor can
// ignore a probe failure that live traffic contradicts.
//
// The package has no dependencies on the rest of the tree so both the metadata
// client and the cache reader can record into it without import cycles.
package liveness

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// lastProof holds time.Now().UnixNano() of the most recent proven reply.
// Zero means no reply has been seen.
var lastProof atomic.Int64

// Mark records that the backend answered just now.
func Mark() { markAt(time.Now()) }

func markAt(t time.Time) {
	n := t.UnixNano()
	for {
		old := lastProof.Load()
		if old >= n || lastProof.CompareAndSwap(old, n) {
			return
		}
	}
}

// Since returns the time since the backend last answered. It returns
// math.MaxInt64 when no answer has been recorded, so a caller's window check
// never treats "never" as recent.
func Since() time.Duration {
	n := lastProof.Load()
	if n == 0 {
		return time.Duration(math.MaxInt64)
	}
	d := time.Since(time.Unix(0, n))
	if d < 0 {
		return 0
	}
	return d
}

// Reset clears the recorded proof. Tests only.
func Reset() { lastProof.Store(0) }

// Answered reports whether err means the Redis server produced a reply. A nil
// error and redis.Nil ("no such key") are answers. A server error reply
// ("-ERR", "-NOAUTH", ...) is also an answer: it came from a live server. A
// network error, a timeout or a cancelled context is not.
func Answered(err error) bool {
	if err == nil || errors.Is(err, redis.Nil) {
		return true
	}
	var rerr redis.Error
	if errors.As(err, &rerr) {
		return true
	}
	return false
}

// RedisHook marks liveness each time a command or pipeline gets a reply.
// Attach it to every client whose address is the backend (or a proxy that can
// only answer by forwarding to the backend).
type RedisHook struct{}

func (RedisHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (RedisHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if Answered(err) {
			Mark()
		}
		return err
	}
}

func (RedisHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		if Answered(err) {
			Mark()
		}
		return err
	}
}

// Compile-time check that RedisHook satisfies redis.Hook.
var _ redis.Hook = RedisHook{}

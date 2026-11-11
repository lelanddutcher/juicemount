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
	"os"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// processStart anchors every timestamp in this package. time.Since on a value
// from time.Now uses the monotonic clock, so a wall-clock step (NTP, a manual
// change, a sleep/wake correction) cannot make an old reply look recent.
var processStart = time.Now()

// monoNow returns the monotonic time since processStart. A variable so tests
// can drive the clock.
var monoNow = func() time.Duration { return time.Since(processStart) }

// lastProof holds monoNow()+1 (in ns) at the most recent proven reply. The +1
// keeps zero free to mean "no reply has been seen".
var lastProof atomic.Int64

// MaxProofLatency is the longest a command may take and still count as proof.
// A reply that arrives after a long wait proves only that the backend was
// reachable when it answered, through a link too slow for interactive use. On
// a bufferbloated link where replies take 10-50 s, late replies every few
// seconds would otherwise suppress every probe failure and keep the volume
// online with an unusable backend. 2 s matches the probe budget on a 200 ms
// WAN (probe plus one retransmit).
const MaxProofLatency = 2 * time.Second

// Mark records that the backend answered just now.
func Mark() { markOffset(monoNow()) }

func markOffset(d time.Duration) {
	n := int64(d) + 1
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
	d := monoNow() - time.Duration(n-1)
	if d < 0 {
		return 0
	}
	return d
}

// ReachabilityHook returns Since for health.WithLivenessHook, or nil when the
// operator has turned Redis-reply proof off with JM_REACH_REDIS_LIVENESS=0
// (health.WithLivenessHook ignores nil). The drain-liveness override is
// separate and has its own switch, JM_REACH_DRAIN_LIVENESS.
func ReachabilityHook() func() time.Duration {
	if os.Getenv("JM_REACH_REDIS_LIVENESS") == "0" {
		return nil
	}
	return Since
}

// Reset clears the recorded proof. Tests only.
func Reset() { lastProof.Store(0) }

// markIfPrompt marks liveness when err is a reply and the command took no
// longer than MaxProofLatency.
func markIfPrompt(start time.Duration, err error) {
	if !Answered(err) {
		return
	}
	now := monoNow()
	if now-start > MaxProofLatency {
		return
	}
	markOffset(now)
}

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

// RedisHook marks liveness each time a command or pipeline gets a prompt reply
// (within MaxProofLatency of being sent).
// Attach it to every client whose address is the backend (or a proxy that can
// only answer by forwarding to the backend).
type RedisHook struct{}

func (RedisHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (RedisHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		start := monoNow()
		err := next(ctx, cmd)
		markIfPrompt(start, err)
		return err
	}
}

func (RedisHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		start := monoNow()
		err := next(ctx, cmds)
		markIfPrompt(start, err)
		return err
	}
}

// Compile-time check that RedisHook satisfies redis.Hook.
var _ redis.Hook = RedisHook{}

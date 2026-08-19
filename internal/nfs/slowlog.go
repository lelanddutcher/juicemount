package nfs

import (
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

// slowRPCLogInterval is the minimum gap between two "slow RPC" log lines.
//
// One per second keeps the signal -- a stall lasting long enough to matter
// still prints, repeatedly -- while capping the process-wide logger lock at one
// acquisition per second instead of one per slow RPC.
var slowRPCLogInterval = func() time.Duration {
	if v := os.Getenv("JM_SLOW_RPC_LOG_INTERVAL_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	return time.Second
}()

// slowRPCLogNext holds the unix-nano time at which the next slow-RPC line may
// be emitted.
var slowRPCLogNext atomic.Int64

// allowSlowRPCLog reports whether a slow-RPC line may be logged now, and claims
// the slot if so.
//
// CAS rather than a mutex on purpose: this is called from every handler
// goroutine on a contended path, and taking a lock to decide whether to take a
// lock would reproduce the problem it exists to solve. Losing the CAS means
// another goroutine just claimed the slot, which is exactly when this one
// should stay quiet -- so a lost race needs no retry.
func allowSlowRPCLog() bool {
	if slowRPCLogInterval <= 0 {
		return true // explicitly disabled rate limiting
	}
	now := time.Now().UnixNano()
	next := slowRPCLogNext.Load()
	if now < next {
		return false
	}
	return slowRPCLogNext.CompareAndSwap(next, now+int64(slowRPCLogInterval))
}

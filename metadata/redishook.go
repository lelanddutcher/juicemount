package metadata

import (
	"context"
	"net"
	"sync/atomic"

	"github.com/redis/go-redis/v9"

	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// redishook.go — count Redis ROUND TRIPS, the thing that actually costs on a
// high-RTT link.
//
// WHY ROUND TRIPS AND NOT BYTES. The gap identified while building the
// wire-byte sampler was "Redis/metadata traffic is unmeasured", and the
// instinctive fix is a byte counter. But bytes are the wrong unit here and a
// hook cannot honestly produce them anyway: go-redis hooks see COMMANDS, not
// the wire, so any byte figure would be an estimate dressed up as a
// measurement. Worse, it would be the wrong estimate to act on.
//
// The cellular cost model is round trips x RTT. The measured evidence is
// unambiguous: a 2026-07-29 session at ~500 ms RTT spent 67 minutes of
// cumulative metadata wait against only 77 object GETs — 9,322 Lookups and
// 1,727 StatFS calls, each one a round trip. The bytes were trivial; the
// round trips were the entire cost. Counting them exactly beats estimating
// bytes approximately.
//
// WHAT COUNTS AS ONE ROUND TRIP:
//   - a single command            (ProcessHook)
//   - a pipeline, whatever its length (ProcessPipelineHook) — this is the point
//     of pipelining, and conflating it with its command count would make
//     pipelining look expensive when it is the fix
//   - a dial                      (DialHook) — TCP setup is a round trip too,
//     and on a flapping cellular link reconnects are not noise
//
// Registered from the client constructors so a new client cannot silently
// escape accounting — the failure mode this codebase keeps hitting is an
// instrument that exists and is never wired up.

var (
	redisCommands  atomic.Int64 // single commands
	redisPipelines atomic.Int64 // pipeline batches (one round trip each)
	redisPipelined atomic.Int64 // commands carried inside those batches
	redisDials     atomic.Int64 // connection establishments
	redisCmdErrors atomic.Int64 // commands returning an error (excl. redis.Nil)
)

// RedisRoundTripCounters returns the raw counts.
func RedisRoundTripCounters() (commands, pipelines, pipelined, dials, errs int64) {
	return redisCommands.Load(), redisPipelines.Load(), redisPipelined.Load(),
		redisDials.Load(), redisCmdErrors.Load()
}

// countingHook implements redis.Hook.
type countingHook struct{}

func (countingHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		redisDials.Add(1)
		return next(ctx, network, addr)
	}
}

func (countingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		redisCommands.Add(1)
		err := next(ctx, cmd)
		// redis.Nil is "no such key", an ordinary answer rather than a failure.
		// Counting it as an error would make a healthy negative-lookup workload
		// look broken.
		if err != nil && err != redis.Nil {
			redisCmdErrors.Add(1)
		}
		return err
	}
}

func (countingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		redisPipelines.Add(1)
		redisPipelined.Add(int64(len(cmds)))
		return next(ctx, cmds)
	}
}

// instrumentRedis attaches the counter to a client. Call it at every place a
// client is constructed.
func instrumentRedis(c *redis.Client) *redis.Client {
	if c != nil {
		c.AddHook(countingHook{})
	}
	return c
}

// Publish to /metrics from an init() rather than a wiring site: an accessor
// nobody calls is the defect class this codebase has paid for repeatedly
// (InflightStats claimed in its own doc comment to be "exposed for the metrics
// endpoint" while having no caller but a watchdog).
func init() {
	metrics.Default().SetRedisProvider(func() *metrics.RedisSnapshot {
		cmds, pipes, pipelined, dials, errs := RedisRoundTripCounters()
		return &metrics.RedisSnapshot{
			Commands:          cmds,
			Pipelines:         pipes,
			PipelinedCommands: pipelined,
			Dials:             dials,
			Errors:            errs,
			// One network exchange each. Pipelined commands are deliberately
			// NOT added: the whole point of a pipeline is that N commands cost
			// one trip, and counting them individually would hide the win.
			RoundTrips: cmds + pipes + dials,
		}
	})
}

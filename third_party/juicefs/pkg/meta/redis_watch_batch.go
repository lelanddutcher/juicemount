//go:build !noredis

package meta

// JuiceMount-owned (patch 0019). Fewer synchronous Redis round trips per
// transaction, with the same WATCH coverage.
//
// Upstream sends every WATCH as its own round trip: one when go-redis opens
// the transaction, and one for each tx.Watch inside the callback. It also
// sends UNWATCH when the transaction closes, after EXEC already dropped every
// watch. On a distant link each of these costs a full round trip.
//
// watchBatch is a hook on one *redis.Tx. It holds each WATCH and sends it in
// the same pipeline as the next command of that transaction, in order, on
// the same connection. Redis runs a pipeline in order, so the WATCH takes
// effect before that command reads: the read is covered exactly as before.
// It drops the closing UNWATCH when no watch can remain on the connection.
//
// Invariant the callers keep (checked for every redisMeta.txn callback when
// this was written): after a tx.Watch, the callback issues a command on tx
// before it reads any watched key through another connection (m.rdb, for
// example resolveCase or Access without a full attribute). Such a read only
// happens after a read on tx, which sends the pending WATCH first.
//
// A held WATCH travels only with commands that read (readOnly). Before a
// command that writes (doSetXattr sends a bare HSETNX) or MULTI/EXEC, the
// WATCH is sent alone and its reply awaited, so a refused WATCH never lets
// a write run.
//
// Failure rules:
//   - A WATCH that fails fails every later command of the transaction
//     without sending it, so no EXEC runs without its watches. txn returns
//     the WATCH error, as upstream does. Failing includes an error reply and
//     any connection failure: a connection that broke, and one that could
//     not be had at all. go-redis sets no error on the commands in the last
//     case, so the pipeline's own error is put on every command that has
//     none. Otherwise the Tx would dial a fresh connection, with no WATCH,
//     for the next command. The read pipelines this patch adds to doMknod
//     and prepareBirthMutationAttr likewise return any pipeline error that
//     is not a Redis reply, with or without this hook: a read that never
//     reached Redis has an empty value and no error of its own.
//   - UNWATCH is dropped only when no WATCH reached Redis since the last
//     EXEC, or when the last EXEC certainly dropped the watches: it
//     committed, it was aborted by a watched change (nil reply), Redis
//     refused it (EXECABORT), or the JuiceMount Redis proxy refused it
//     (JMDENY, after it sent DISCARD upstream). Any other EXEC outcome keeps
//     the UNWATCH.
//
// Only standalone and Sentinel clients (*redis.Client) use it. Cluster
// clients keep upstream behavior. JM_REDIS_WATCH_BATCH=0 turns it off.

import (
	"context"
	"net"
	"os"
	"strings"

	"github.com/redis/go-redis/v9"
)

var watchBatchOff = os.Getenv("JM_REDIS_WATCH_BATCH") == "0"

// watchTxn runs txf in a WATCH transaction on keys, like rdb.Watch.
func (m *redisMeta) watchTxn(ctx context.Context, txf func(*redis.Tx) error, keys []string) error {
	client, ok := m.rdb.(*redis.Client)
	if !ok || watchBatchOff {
		return m.rdb.Watch(ctx, txf, keys...)
	}
	return client.Watch(ctx, func(tx *redis.Tx) error {
		w := &watchBatch{tx: tx}
		tx.AddHook(w)
		if len(keys) > 0 {
			_ = tx.Watch(ctx, keys...) // held until the first command
		}
		err := txf(tx)
		if w.err != nil {
			return w.err
		}
		return err
	}) // no keys: the first WATCH goes through the hook above
}

type watchBatch struct {
	tx      *redis.Tx
	pending []redis.Cmder // WATCH commands not sent yet
	live    bool          // a WATCH may be in force on the connection
	err     error         // the first WATCH failure; fails all later commands
}

func (w *watchBatch) DialHook(next redis.DialHook) redis.DialHook { return next }

func (w *watchBatch) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		switch cmd.Name() {
		case "watch":
			if w.err != nil {
				cmd.SetErr(w.err)
				return w.err
			}
			w.pending = append(w.pending, cmd)
			return nil
		case "unwatch":
			w.pending = nil
			if !w.live {
				return nil
			}
			w.live = false
			return next(ctx, cmd)
		}
		if w.err != nil {
			cmd.SetErr(w.err)
			return w.err
		}
		if len(w.pending) == 0 {
			return next(ctx, cmd)
		}
		if !readOnly(cmd) {
			// A write must not run unless its WATCH succeeded.
			if err := w.sendWatches(ctx); err != nil {
				cmd.SetErr(err)
				return err
			}
			return next(ctx, cmd)
		}
		_, err := w.tx.Pipelined(ctx, func(p redis.Pipeliner) error {
			return p.Process(ctx, cmd)
		})
		if err != nil && cmd.Err() == nil {
			cmd.SetErr(err) // go-redis leaves cmds unset when no connection was had
		}
		return cmd.Err()
	}
}

func (w *watchBatch) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if w.err != nil {
			setErrs(cmds, w.err)
			return w.err
		}
		if len(cmds) > 0 && cmds[0].Name() == "multi" {
			// go-redis reads a MULTI/EXEC pipeline by position, so a held
			// WATCH goes first in its own round trip (as upstream).
			if err := w.sendWatches(ctx); err != nil {
				setErrs(cmds, err)
				return err
			}
			err := next(ctx, cmds)
			if execDroppedWatches(cmds[len(cmds)-1].Err()) {
				w.live = false
			}
			return err
		}
		for _, c := range cmds {
			if c.Name() == "watch" {
				w.live = true
			}
		}
		if len(w.pending) == 0 {
			return next(ctx, cmds)
		}
		for _, c := range cmds {
			if !readOnly(c) {
				if err := w.sendWatches(ctx); err != nil {
					setErrs(cmds, err)
					return err
				}
				return next(ctx, cmds)
			}
		}
		watches := w.pending
		w.pending = nil
		return w.send(ctx, next, watches, cmds)
	}
}

// sendWatches sends the held WATCH commands alone, as a plain pipeline,
// and waits for their replies. Any failure, a connection that could not be
// had or broke as well as an error reply, is the transaction's WATCH error.
func (w *watchBatch) sendWatches(ctx context.Context) error {
	if len(w.pending) == 0 {
		return nil
	}
	watches := w.pending
	w.pending = nil
	w.live = true
	_, err := w.tx.Pipelined(ctx, func(p redis.Pipeliner) error {
		for _, c := range watches {
			_ = p.Process(ctx, c)
		}
		return nil
	})
	if err != nil && !isReplyError(err) {
		w.err = err
		return err
	}
	for _, c := range watches {
		if e := c.Err(); e != nil {
			w.err = e
			return e
		}
	}
	return nil
}

// readOnly reports whether a held WATCH may travel in the same pipeline as
// cmd: cmd only reads, so if Redis refuses the WATCH nothing was changed.
// Anything else (for example doSetXattr's bare HSETNX, or an unknown
// command) waits for the WATCH reply first, as upstream does.
func readOnly(cmd redis.Cmder) bool {
	switch cmd.Name() {
	case "get", "mget", "strlen", "getrange", "exists", "type", "ttl", "pttl",
		"hget", "hmget", "hgetall", "hkeys", "hvals", "hlen", "hexists",
		"lrange", "llen", "lindex", "smembers", "sismember", "scard",
		"zscore", "zrange", "zcard", "zrangebyscore":
		return true
	}
	return false
}

// send runs watches followed by cmds as one pipeline.
func (w *watchBatch) send(ctx context.Context, next redis.ProcessPipelineHook, watches, cmds []redis.Cmder) error {
	all := make([]redis.Cmder, 0, len(watches)+len(cmds))
	all = append(append(all, watches...), cmds...)
	w.live = true // sent, whatever the reply
	err := next(ctx, all)
	if err != nil && !isReplyError(err) {
		// No connection, or it broke: the watches may not be in force.
		w.err = err
		for _, c := range all {
			if c.Err() == nil {
				c.SetErr(err)
			}
		}
		return err
	}
	for _, c := range watches {
		if e := c.Err(); e != nil {
			w.err = e
			setErrs(cmds, e)
			return e
		}
	}
	if len(cmds) == 0 {
		return nil
	}
	if err != nil && !isReplyError(err) {
		return err
	}
	for _, c := range cmds {
		if e := c.Err(); e != nil {
			return e
		}
	}
	return nil
}

// execDroppedWatches reports whether an EXEC outcome certainly left no
// WATCH on the connection.
func execDroppedWatches(err error) bool {
	if err == nil || err == redis.TxFailedErr {
		return true
	}
	if !isReplyError(err) {
		return false
	}
	msg := err.Error()
	return strings.HasPrefix(msg, "EXECABORT") || strings.HasPrefix(msg, "JMDENY")
}

func isReplyError(err error) bool {
	if _, ok := err.(net.Error); ok {
		return false
	}
	_, ok := err.(redis.Error)
	return ok
}

func setErrs(cmds []redis.Cmder, err error) {
	for _, c := range cmds {
		c.SetErr(err)
	}
}

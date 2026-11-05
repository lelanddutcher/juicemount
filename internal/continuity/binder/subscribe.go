package binder

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Subscribe follows channel on rdb and offers every well-formed event until
// ctx ends. Its loop never blocks on the worker (Offer drops when full).
//
// Pub/sub is fire-and-forget: changes made before the first subscription
// (the Manager was down) or while go-redis re-subscribes after a dropped
// connection are never delivered. Stats reports when observation started
// and counts re-subscriptions, so a later rescan knows coverage is partial.
func (b *Binder) Subscribe(ctx context.Context, rdb *redis.Client, channel string) {
	backoff := time.Second
	for ctx.Err() == nil {
		ps := rdb.Subscribe(ctx, channel)
		if _, err := ps.Receive(ctx); err != nil {
			_ = ps.Close()
			if ctx.Err() == nil {
				b.logEvery("subscribe", "continuity: subscribe %s: %v (retrying in %s)", channel, err, backoff)
				select {
				case <-ctx.Done():
				case <-time.After(backoff):
				}
				backoff = min(backoff*2, time.Minute)
			}
			continue
		}
		backoff = time.Second
		first := false
		b.readyOnce.Do(func() {
			first = true
			b.observedSince.Store(b.cfg.Now().UnixNano())
			close(b.ready)
		})
		if !first {
			b.resubscribes.Add(1)
		}
		b.logEvery("subscribe", "")
		// go-redis re-subscribes on its own after a dropped connection and
		// then delivers a *redis.Subscription; the channel closes only
		// with ps.
		ch := ps.ChannelWithSubscriptions(redis.WithChannelSize(1024))
	read:
		for {
			select {
			case <-ctx.Done():
				break read
			case msg, ok := <-ch:
				if !ok {
					break read
				}
				switch m := msg.(type) {
				case *redis.Subscription:
					if m.Kind == "subscribe" {
						b.resubscribes.Add(1)
					}
				case *redis.Message:
					if ev, err := ParseEvent([]byte(m.Payload)); err == nil {
						b.Offer(ev)
					} else {
						b.received.Add(1)
						b.ignored.Add(1)
					}
				}
			}
		}
		_ = ps.Close()
	}
}

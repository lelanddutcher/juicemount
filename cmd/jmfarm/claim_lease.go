package main

import (
	"context"
	"sync"
	"time"
)

// claimLeaseGuard keeps execution authority tied to the last confirmed lease.
// Queue mutations still fence independently; cancellation limits in-flight work.
type claimLeaseGuard struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu       sync.Mutex
	deadline time.Time
	lost     bool
	update   chan struct{}
}

func startClaimLeaseGuard(parent context.Context, deadline time.Time, interval time.Duration,
	pulse func(context.Context), renew func(context.Context) (time.Time, error),
	isLost func(error) bool) *claimLeaseGuard {
	ctx, cancel := context.WithCancel(parent)
	g := &claimLeaseGuard{
		ctx: ctx, cancel: cancel, done: make(chan struct{}),
		deadline: deadline, update: make(chan struct{}, 1),
	}
	go func() {
		defer close(g.done)
		var workers sync.WaitGroup
		workers.Add(3)
		go func() {
			defer workers.Done()
			for {
				g.mu.Lock()
				remaining := time.Until(g.deadline)
				g.mu.Unlock()
				if remaining <= 0 {
					g.markLost()
					return
				}
				timer := time.NewTimer(remaining)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-g.update:
					timer.Stop()
				case <-timer.C:
				}
			}
		}()
		go func() {
			defer workers.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if pulse != nil {
						pulse(ctx)
					}
				}
			}
		}()
		go func() {
			defer workers.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					confirmed, err := renew(ctx)
					if err != nil {
						if isLost(err) {
							g.markLost()
							return
						}
						continue
					}
					g.mu.Lock()
					if confirmed.After(g.deadline) && ctx.Err() == nil {
						g.deadline = confirmed
						select {
						case g.update <- struct{}{}:
						default:
						}
					}
					g.mu.Unlock()
				}
			}
		}()
		workers.Wait()
	}()
	return g
}

func (g *claimLeaseGuard) markLost() {
	g.mu.Lock()
	g.lost = true
	g.mu.Unlock()
	g.cancel()
}

func (g *claimLeaseGuard) Context() context.Context { return g.ctx }

func (g *claimLeaseGuard) Lost() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.lost
}

func (g *claimLeaseGuard) Stop() bool {
	g.cancel()
	<-g.done
	return g.Lost()
}

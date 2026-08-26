package main

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

func runWatchBackstop(ctx context.Context, cfg queueConfig, q *farmqueue.Client, workerID string, kinds []string) {
	interval := time.Duration(farmEnvInt("JM_FARM_WATCH_BACKSTOP_SEC", 900)) * time.Second
	if interval < time.Minute {
		interval = time.Minute
	}
	runScan := func(now time.Time) {
		ctl, err := q.GetControl(ctx)
		if err != nil || ctl.Paused || !ctl.WatchEnabled {
			return
		}
		leader, err := q.AcquireWatchLeadership(ctx, workerID, 45*time.Second)
		if err != nil || !leader {
			return
		}
		cursor, err := q.GetWatchCursor(ctx)
		if err != nil {
			fmt.Printf("farm watch backstop: cursor: %v\n", err)
			return
		}

		// A brand-new deployment has no cursor. Queue one recursive root catch-up
		// immediately instead of declaring the existing library current and waiting
		// for future keyspace events. The Redis path dedupe makes leader restarts
		// idempotent while that root job is still queued/running.
		if cursor.IsZero() {
			if _, err := q.EnqueueDiscovered(ctx, cfg.mount, kinds, "farm-watch-catchup"); err != nil {
				fmt.Printf("farm watch backstop: initial catch-up enqueue: %v\n", err)
				return
			}
			if err := q.StoreWatchCursor(ctx, now); err != nil {
				fmt.Printf("farm watch backstop: initial cursor: %v\n", err)
				return
			}
			fmt.Printf("farm watch backstop: queued initial recursive catch-up for %q\n", cfg.mount)
			return
		}

		paths, saturated, err := farm.DiscoverModified(ctx, cfg.mount, cursor,
			farmEnvInt("JM_FARM_WATCH_BACKSTOP_MAX", 2000))
		if err != nil {
			fmt.Printf("farm watch backstop: scan: %v\n", err)
			return
		}
		// Hitting the bounded candidate cap means a per-directory batch cannot
		// advance a timestamp-only cursor without risking omissions or replaying
		// the same first page forever. One recursive root job is the lossless,
		// bounded queue representation of that overflow.
		if saturated {
			if _, err := q.EnqueueDiscovered(ctx, cfg.mount, kinds, "farm-watch-catchup"); err != nil {
				fmt.Printf("farm watch backstop: saturated catch-up enqueue: %v\n", err)
				return
			}
		} else {
			for _, rel := range paths {
				if _, err := q.EnqueueDiscovered(ctx, filepath.Join(cfg.mount, rel), kinds, "farm-watch-backstop"); err != nil {
					// Do not advance the durable cursor past work that was not queued.
					fmt.Printf("farm watch backstop: enqueue %q: %v\n", rel, err)
					return
				}
			}
		}
		// now was captured BEFORE the scan. A mutation arriving during/after the
		// walk therefore has mtime > cursor and is included on the next pass.
		if err := q.StoreWatchCursor(ctx, now); err != nil {
			fmt.Printf("farm watch backstop: store cursor: %v\n", err)
			return
		}
		if len(paths) > 0 || saturated {
			fmt.Printf("farm watch backstop: discovered %d modified target(s) (saturated=%v)\n", len(paths), saturated)
		}
	}

	// Do not wait fifteen minutes after startup/restart to catch missed work.
	runScan(time.Now())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			runScan(now)
		}
	}
}

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
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			ctl, err := q.GetControl(ctx)
			if err != nil || ctl.Paused || !ctl.WatchEnabled {
				continue
			}
			leader, err := q.AcquireWatchLeadership(ctx, workerID, 45*time.Second)
			if err != nil || !leader {
				continue
			}
			cursor, err := q.GetWatchCursor(ctx)
			if err != nil {
				fmt.Printf("farm watch backstop: cursor: %v\n", err)
				continue
			}
			if cursor.IsZero() {
				_ = q.StoreWatchCursor(ctx, now)
				continue
			}
			paths, saturated, err := farm.DiscoverModified(ctx, cfg.mount, cursor,
				farmEnvInt("JM_FARM_WATCH_BACKSTOP_MAX", 2000))
			if err != nil {
				fmt.Printf("farm watch backstop: scan: %v\n", err)
				continue
			}
			for _, rel := range paths {
				if _, err := q.EnqueueDiscovered(ctx, filepath.Join(cfg.mount, rel), kinds, "farm-watch-backstop"); err != nil {
					fmt.Printf("farm watch backstop: enqueue %q: %v\n", rel, err)
				}
			}
			if !saturated {
				_ = q.StoreWatchCursor(ctx, now)
			}
			if len(paths) > 0 {
				fmt.Printf("farm watch backstop: discovered %d modified target(s) (saturated=%v)\n", len(paths), saturated)
			}
		}
	}
}

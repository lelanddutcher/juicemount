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
	// Do not wait fifteen minutes after startup/restart to catch missed work.
	scanWatchBackstop(ctx, cfg, q, workerID, kinds, time.Now())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	retryTicker := time.NewTicker(time.Minute)
	defer retryTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-retryTicker.C:
			if _, err := q.PromoteWatchRetries(ctx, now, 128); err != nil && ctx.Err() == nil {
				fmt.Printf("farm watch backstop: scheduled retry: %v\n", err)
			}
			if _, err := q.RetireTrackedTerminals(ctx, now, 4096); err != nil && ctx.Err() == nil {
				fmt.Printf("farm watch backstop: terminal retirement: %v\n", err)
			}
		case now := <-ticker.C:
			scanWatchBackstop(ctx, cfg, q, workerID, kinds, now)
		}
	}
}

func scanWatchBackstop(ctx context.Context, cfg queueConfig, q *farmqueue.Client, workerID string, kinds []string, now time.Time) {
	ctl, err := q.GetControl(ctx)
	if err != nil || ctl.Paused || !ctl.WatchEnabled {
		return
	}
	leader, err := q.AcquireWatchLeadership(ctx, workerID, 45*time.Second)
	if err != nil || !leader {
		return
	}
	if _, err := q.PromoteWatchRetries(ctx, now, 128); err != nil {
		fmt.Printf("farm watch backstop: scheduled retry: %v\n", err)
		return
	}
	state, err := q.GetWatchEpoch(ctx, cfg.mount)
	if err != nil {
		fmt.Printf("farm watch backstop: epoch: %v\n", err)
		return
	}
	if state.Active != "" {
		if _, err := q.EnsureWatchEpochParents(ctx, cfg.mount, state); err != nil {
			fmt.Printf("farm watch backstop: repair epoch parents: %v\n", err)
			return
		}
		if state.Pending == 0 {
			if committed, err := q.CommitWatchEpoch(ctx, cfg.mount, workerID, state.Active); err != nil {
				fmt.Printf("farm watch backstop: commit epoch: %v\n", err)
			} else if committed {
				fmt.Printf("farm watch backstop: committed complete epoch %s for %q\n", state.Active, cfg.mount)
			}
		}
		return
	}
	if !state.Cursor.IsZero() {
		paths, saturated, err := farm.DiscoverModified(ctx, cfg.mount, state.Cursor,
			farmEnvInt("JM_FARM_WATCH_BACKSTOP_MAX", 2000))
		if err != nil {
			fmt.Printf("farm watch backstop: scan: %v\n", err)
			return
		}
		if len(paths) == 0 && !saturated {
			if _, err := q.AdvanceWatchIdleCursor(ctx, cfg.mount, workerID, state.Cursor, now); err != nil {
				fmt.Printf("farm watch backstop: idle cursor: %v\n", err)
			}
			return
		}
	}
	// An old v1 cursor is not coverage evidence. The first v2 epoch always
	// starts from a complete recursive root plan. Later changed/saturated
	// scans use the same root unit so no dedupe or candidate ordering can drop
	// a subtree before all configured kinds have durable execution owners.
	if _, err := q.AcquireWatchLeadership(ctx, workerID, 45*time.Second); err != nil {
		fmt.Printf("farm watch backstop: renew leadership: %v\n", err)
		return
	}
	if _, err := q.StartWatchEpoch(ctx, cfg.mount, workerID, kinds, now); err != nil {
		fmt.Printf("farm watch backstop: start epoch: %v\n", err)
	}
}

func scanWatchBackstopCandidates(ctx context.Context, cfg queueConfig, kinds []string, cursor, now time.Time,
	enqueue func(context.Context, string, []string, string) ([]string, error),
	storeCursor func(context.Context, time.Time) error,
) error {
	paths, saturated, err := farm.DiscoverModified(ctx, cfg.mount, cursor,
		farmEnvInt("JM_FARM_WATCH_BACKSTOP_MAX", 2000))
	if err != nil {
		return err
	}
	// This compatibility helper remains for the starvation oracle. The active
	// backstop uses tracked root epochs above.
	if saturated {
		ids, err := enqueue(ctx, cfg.mount, kinds, "farm-watch-catchup")
		if err != nil {
			return fmt.Errorf("saturated catch-up enqueue: %w", err)
		}
		_ = ids
	} else {
		deduped := false
		for _, rel := range paths {
			ids, err := enqueue(ctx, filepath.Join(cfg.mount, rel), kinds, "farm-watch-backstop")
			if err != nil {
				// Do not advance the durable cursor past work that was not queued.
				return fmt.Errorf("enqueue %q: %w", rel, err)
			}
			if len(ids) == 0 {
				deduped = true
			}
		}
		if deduped {
			if _, err := enqueue(ctx, cfg.mount, kinds, "farm-watch-catchup"); err != nil {
				return fmt.Errorf("deduped catch-up enqueue: %w", err)
			}
		}
	}
	// No candidate/root enqueue is proof of a completed recursive plan.
	if len(paths) == 0 && !saturated {
		if err := storeCursor(ctx, now); err != nil {
			return fmt.Errorf("store cursor: %w", err)
		}
	}
	if len(paths) > 0 || saturated {
		fmt.Printf("farm watch backstop: discovered %d modified target(s) (saturated=%v)\n", len(paths), saturated)
	}
	return nil
}

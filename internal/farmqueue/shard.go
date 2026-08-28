package farmqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// EnqueueTargetShards replaces one directory-sized ownership unit with
// deterministic, bounded children. It is idempotent across worker crashes:
// replaying the durable parent claim observes each existing child status and
// publishes only children that were not committed before the crash.
//
// Render children retain the verified backend but release the exact worker
// pin, allowing any online worker with that same proven encoder/transcript
// backend to claim the next shard. CPU fallback children remain on CPU.
func (c *Client) EnqueueTargetShards(ctx context.Context, parent Job, targets []string, maxTargets int) ([]Job, int, error) {
	children, err := newTargetShards(parent, targets, maxTargets)
	if err != nil {
		return nil, 0, err
	}
	created, err := c.enqueueTargetShardsAtomic(ctx, children)
	return children, created, err
}

func newTargetShards(parent Job, targets []string, maxTargets int) ([]Job, error) {
	if strings.TrimSpace(parent.ID) == "" {
		return nil, fmt.Errorf("target-shard parent has no job ID")
	}
	if parent.ShardCount > 0 {
		return nil, fmt.Errorf("job %s is already target shard %d/%d", parent.ID, parent.ShardIndex, parent.ShardCount)
	}
	if maxTargets < 1 {
		return nil, fmt.Errorf("target-shard size must be positive")
	}
	if len(targets) <= maxTargets {
		return nil, fmt.Errorf("target-shard split needs more than %d target(s), got %d", maxTargets, len(targets))
	}

	count := (len(targets) + maxTargets - 1) / maxTargets
	children := make([]Job, 0, count)
	enqueuedAt := nowISO()
	for i, start := 0, 0; start < len(targets); i, start = i+1, start+maxTargets {
		end := start + maxTargets
		if end > len(targets) {
			end = len(targets)
		}
		child := parent
		child.ID = fmt.Sprintf("%s-shard-%04d", parent.ID, i+1)
		child.ParentID = parent.ID
		child.EnqueuedAt = enqueuedAt
		// Preserve the parent's hardware-attempt budget. A recovered directory
		// claim must not manufacture fresh render retries merely by becoming
		// bounded children; an initial parent naturally carries zero attempts.
		child.Attempts = parent.Attempts
		child.ProcessedOffset = 0
		child.ShardIndex = i + 1
		child.ShardCount = count
		child.RetryTargets = append([]string(nil), targets[start:end]...)
		child.RequiredCapabilities = append([]string(nil), parent.RequiredCapabilities...)
		if child.QueueClass == QueueClassRender {
			child.SelectedWorker = ""
			caps := child.RequiredCapabilities[:0]
			for _, capability := range child.RequiredCapabilities {
				if !strings.HasPrefix(capability, "worker:") {
					caps = append(caps, capability)
				}
			}
			child.RequiredCapabilities = caps
		}
		children = append(children, child)
	}
	return children, nil
}

func (c *Client) enqueueTargetShardsAtomic(ctx context.Context, children []Job) (int, error) {
	raws := make([][]byte, len(children))
	statuses := make([]JobStatus, len(children))
	statusKeys := make([]string, len(children))
	for i, child := range children {
		raw, err := json.Marshal(child)
		if err != nil {
			return 0, err
		}
		raws[i] = raw
		statusKeys[i] = JobHashPrefix + child.ID
		statuses[i] = JobStatus{
			ID: child.ID, Status: StatusQueued, Path: child.Path, Kinds: strings.Join(child.Kinds, ","),
			Producer: child.Producer, EnqueuedAt: child.EnqueuedAt, Backend: child.SelectedBackend,
			TargetWorker: child.SelectedWorker, QueueClass: child.QueueClass, Attempts: child.Attempts,
			ParentID: child.ParentID,
		}
	}
	for attempt := 0; attempt < splitTransactionRetries; attempt++ {
		created := 0
		err := c.rdb.Watch(ctx, func(tx *redis.Tx) error {
			check := tx.Pipeline()
			exists := make([]*redis.IntCmd, len(statusKeys))
			for i, key := range statusKeys {
				exists[i] = check.Exists(ctx, key)
			}
			if _, checkErr := check.Exec(ctx); checkErr != nil {
				return checkErr
			}
			missing := make([]bool, len(children))
			for i := range children {
				missing[i] = exists[i].Val() == 0
				if missing[i] {
					created++
				}
			}
			if created == 0 {
				return nil
			}
			score := time.Now().UnixMicro()
			_, txErr := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				for i, child := range children {
					if !missing[i] {
						continue
					}
					pipe.LPush(ctx, QueueKeyFor(&child), raws[i])
					pipe.HSet(ctx, statusKeys[i], statuses[i].toMap())
					pipe.Expire(ctx, statusKeys[i], JobTTL)
					pipe.ZAdd(ctx, JobIndexKey, redis.Z{Score: float64(score + int64(i)), Member: child.ID})
				}
				return nil
			})
			return txErr
		}, statusKeys...)
		if err == redis.TxFailedErr {
			continue
		}
		return created, err
	}
	return 0, fmt.Errorf("target-shard transaction conflicted %d times for parent %s", splitTransactionRetries, children[0].ParentID)
}

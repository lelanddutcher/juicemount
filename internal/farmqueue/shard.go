package farmqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// EnqueuePlannedChildren atomically publishes a server planner's complete set
// of bounded execution children. Child IDs must be deterministic: if the
// planner's durable claim is replayed after a crash, existing status hashes
// suppress duplicate work even when the currently preferred device changed.
func (c *Client) EnqueuePlannedChildren(ctx context.Context, children []Job) (int, error) {
	if len(children) == 0 {
		return 0, fmt.Errorf("planned child set is empty")
	}
	seen := make(map[string]bool, len(children))
	for _, child := range children {
		if strings.TrimSpace(child.ID) == "" || strings.TrimSpace(child.ParentID) == "" {
			return 0, fmt.Errorf("planned child is missing id or parent_id")
		}
		if seen[child.ID] {
			return 0, fmt.Errorf("duplicate planned child id %s", child.ID)
		}
		seen[child.ID] = true
		if child.PlanOnly || child.ShardCount < 1 || child.ShardIndex < 1 || len(child.RetryTargets) == 0 {
			return 0, fmt.Errorf("planned child %s is not a bounded execution job", child.ID)
		}
		if child.QueueClass == "" || len(child.RequiredCapabilities) == 0 {
			return 0, fmt.Errorf("planned child %s has no execution route", child.ID)
		}
	}
	// Stable status-key order avoids needless WATCH conflicts if a replay built
	// the same children by iterating a map in a different order.
	sort.SliceStable(children, func(i, j int) bool { return children[i].ID < children[j].ID })
	return c.enqueueTargetShardsAtomic(ctx, children)
}

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
	if parent.PlanOnly {
		route, routeErr := c.SnapshotRouter(ctx)
		if routeErr != nil {
			return nil, 0, fmt.Errorf("snapshot live worker routes: %w", routeErr)
		}
		for i := range children {
			route(&children[i])
			// A bounded render child is portable across workers with the same
			// verified backend. Do not recreate the directory parent's exact-node
			// pin or one offline node can strand every already-planned child.
			if children[i].QueueClass == QueueClassRender {
				children[i].SelectedWorker = ""
				caps := children[i].RequiredCapabilities[:0]
				for _, capability := range children[i].RequiredCapabilities {
					if !strings.HasPrefix(capability, "worker:") {
						caps = append(caps, capability)
					}
				}
				children[i].RequiredCapabilities = caps
			}
		}
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
	if len(targets) == 0 {
		return nil, fmt.Errorf("target-shard split has no targets")
	}
	if len(targets) <= maxTargets && !parent.PlanOnly {
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
		// Preserve both delivery history and the parent's hardware-failure budget.
		// A recovered directory claim must not manufacture fresh render retries
		// merely by becoming bounded children; an initial parent carries zero.
		child.Attempts = parent.Attempts
		child.HardwareFailures = parent.HardwareFailures
		child.ProcessedOffset = 0
		child.ShardIndex = i + 1
		child.ShardCount = count
		child.RetryTargets = append([]string(nil), targets[start:end]...)
		child.RequiredCapabilities = append([]string(nil), parent.RequiredCapabilities...)
		if parent.PlanOnly {
			// Execution routing is selected after target expansion from current
			// measured worker profiles. Clear the server planner admission first.
			child.PlanOnly = false
			child.QueueClass = ""
			child.RequiredCapabilities = nil
			child.SelectedBackend = ""
			child.SelectedWorker = ""
		}
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
			HardwareFailures: child.HardwareFailures,
			ParentID:         child.ParentID, DerivativePass: child.DerivativePass, Error: child.RoutingReason,
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

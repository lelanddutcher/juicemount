package farmqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const splitTransactionRetries = 5

// EnqueueCPUFallbackSubset creates one idempotent CPU child for the exact
// sources a render worker cannot keep on its verified hardware path. The child
// is independent, so the server can claim those files while the parent keeps
// encoding the compatible majority on the GPU.
//
// The deterministic child ID and WATCH transaction make worker-crash recovery
// safe: replaying the original durable parent claim observes the existing child
// instead of publishing a duplicate fallback batch.
func (c *Client) EnqueueCPUFallbackSubset(ctx context.Context, parent Job, targets []string, reason string) (Job, bool, error) {
	child, err := newCPUFallbackSubset(parent, targets)
	if err != nil {
		return Job{}, false, err
	}
	// Persist the terminal source-admission/render-failure provenance in the
	// immutable queue payload as well as the mutable status hash. Maintenance
	// may rewrite status notes during recovery; the raw reason prevents a later
	// outage from making a deliberate compatibility fallback look temporary.
	child.RoutingReason = strings.TrimSpace(reason)

	raw, err := json.Marshal(child)
	if err != nil {
		return Job{}, false, err
	}
	st := JobStatus{
		ID: child.ID, Status: StatusQueued, Path: child.Path, Kinds: strings.Join(child.Kinds, ","),
		Producer: child.Producer, EnqueuedAt: child.EnqueuedAt, Backend: child.SelectedBackend,
		QueueClass: child.QueueClass, ParentID: child.ParentID, Error: reason,
	}
	statusKey := JobHashPrefix + child.ID
	created := false
	for attempt := 0; attempt < splitTransactionRetries; attempt++ {
		created = false
		err = c.rdb.Watch(ctx, func(tx *redis.Tx) error {
			exists, existsErr := tx.Exists(ctx, statusKey).Result()
			if existsErr != nil {
				return existsErr
			}
			if exists > 0 {
				return nil
			}
			_, txErr := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.LPush(ctx, QueueKeyFor(&child), raw)
				pipe.HSet(ctx, statusKey, st.toMap())
				pipe.Expire(ctx, statusKey, JobTTL)
				pipe.ZAdd(ctx, JobIndexKey, redis.Z{Score: float64(time.Now().UnixMicro()), Member: child.ID})
				return nil
			})
			if txErr == nil {
				created = true
			}
			return txErr
		}, statusKey)
		if err == redis.TxFailedErr {
			continue
		}
		return child, created, err
	}
	return Job{}, false, fmt.Errorf("CPU fallback split transaction conflicted %d times", splitTransactionRetries)
}

func newCPUFallbackSubset(parent Job, targets []string) (Job, error) {
	if strings.TrimSpace(parent.ID) == "" {
		return Job{}, fmt.Errorf("CPU fallback parent has no job ID")
	}
	if len(parent.Kinds) != 1 || parent.Kinds[0] != KindProxy {
		return Job{}, fmt.Errorf("CPU fallback split requires one proxy kind, got %v", parent.Kinds)
	}
	if len(targets) == 0 {
		return Job{}, fmt.Errorf("CPU fallback split has no targets")
	}

	child := parent
	child.ID = parent.ID + "-cpu"
	child.ParentID = parent.ID
	child.EnqueuedAt = nowISO()
	child.VCodec = "libx264"
	child.QueueClass = QueueClassCPU
	child.RequiredCapabilities = []string{"cpu"}
	child.SelectedBackend = "libx264"
	child.SelectedWorker = ""
	child.CPUFallbackLocked = true
	child.Attempts = 0
	child.ProcessedOffset = 0
	child.RetryTargets = append([]string(nil), targets...)
	return child, nil
}

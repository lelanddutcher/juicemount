package farmqueue

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// EnqueueDiscovered turns one settled directory into independent routed jobs.
// The Redis dedupe survives watch-leader changes, preventing a new leader from
// replaying the same event while still allowing a later filesystem mutation to
// trigger a fresh ingest pass.
func (c *Client) EnqueueDiscovered(ctx context.Context, path string, kinds []string, producer string) ([]string, error) {
	sum := sha256.Sum256([]byte(path))
	dedupeKey := fmt.Sprintf("juicefarm:watch:dedupe:%x", sum[:12])
	fresh, err := c.rdb.SetNX(ctx, dedupeKey, nowISO(), 10*time.Minute).Result()
	if err != nil {
		return nil, err
	}
	if !fresh {
		return nil, nil
	}

	var ids []string
	for _, kind := range DrainKinds(kinds) {
		job := NewJob(path, []string{kind}, producer)
		if err := c.Enqueue(ctx, job); err != nil {
			_ = c.rdb.Del(ctx, dedupeKey).Err()
			return ids, err
		}
		ids = append(ids, job.ID)
	}
	return ids, nil
}

// FarmControl is the operator-owned run state for the whole farm. Pausing is
// cooperative: workers finish their current atomic job and stop claiming new
// work; discovery keeps its dirty set in memory and resumes enqueueing when the
// control plane is played again.
type FarmControl struct {
	Revision     int64  `json:"revision"`
	Paused       bool   `json:"paused"`
	WatchEnabled bool   `json:"watch_enabled"`
	UpdatedAt    string `json:"updated_at,omitempty"`
	UpdatedBy    string `json:"updated_by,omitempty"`
}

// DefaultFarmControl is intentionally active. Existing deployments without a
// control key retain the standing-worker behavior they had before this surface
// shipped, while gaining an explicit pause switch as soon as Manager writes it.
func DefaultFarmControl() FarmControl {
	return FarmControl{WatchEnabled: true}
}

func (c *Client) GetControl(ctx context.Context) (FarmControl, error) {
	raw, err := c.rdb.Get(ctx, ControlKey).Result()
	if err == redis.Nil {
		return DefaultFarmControl(), nil
	}
	if err != nil {
		return FarmControl{}, err
	}
	var ctl FarmControl
	if err := json.Unmarshal([]byte(raw), &ctl); err != nil {
		return FarmControl{}, err
	}
	return ctl, nil
}

// StoreControl publishes a full control document with an optimistic revision.
// forceRevision is reserved for restore/tests; normal callers pass zero.
func (c *Client) StoreControl(ctx context.Context, ctl FarmControl, forceRevision int64) (int64, error) {
	for attempt := 0; attempt < 3; attempt++ {
		err := c.rdb.Watch(ctx, func(tx *redis.Tx) error {
			var cur int64
			raw, err := tx.Get(ctx, ControlKey).Result()
			if err != nil && err != redis.Nil {
				return err
			}
			if err == nil {
				var existing FarmControl
				if json.Unmarshal([]byte(raw), &existing) == nil {
					cur = existing.Revision
				}
			}
			if forceRevision > 0 {
				ctl.Revision = forceRevision
			} else {
				ctl.Revision = cur + 1
			}
			ctl.UpdatedAt = nowISO()
			blob, err := json.Marshal(ctl)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
				p.Set(ctx, ControlKey, blob, 0)
				return nil
			})
			return err
		}, ControlKey)
		if err == redis.TxFailedErr {
			continue
		}
		return ctl.Revision, err
	}
	return 0, fmt.Errorf("farm control store: contention after 3 attempts")
}

// AcquireWatchLeadership elects exactly one currently-running worker to turn
// JuiceFS keyspace events into jobs. The holder renews its short lease; another
// worker can take over shortly after it disappears.
func (c *Client) AcquireWatchLeadership(ctx context.Context, workerID string, ttl time.Duration) (bool, error) {
	if workerID == "" {
		return false, fmt.Errorf("watch leadership requires worker id")
	}
	if ttl <= 0 {
		ttl = 45 * time.Second
	}
	acquired, err := c.rdb.SetNX(ctx, WatchLeaderKey, workerID, ttl).Result()
	if err != nil || acquired {
		return acquired, err
	}
	owner, err := c.rdb.Get(ctx, WatchLeaderKey).Result()
	if err != nil {
		return false, err
	}
	if owner != workerID {
		return false, nil
	}
	return true, c.rdb.Expire(ctx, WatchLeaderKey, ttl).Err()
}

func (c *Client) ReleaseWatchLeadership(ctx context.Context, workerID string) error {
	const script = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0`
	return c.rdb.Eval(ctx, script, []string{WatchLeaderKey}, workerID).Err()
}

func (c *Client) GetWatchCursor(ctx context.Context) (time.Time, error) {
	raw, err := c.rdb.Get(ctx, WatchCursorKey).Result()
	if err == redis.Nil {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339Nano, raw)
}

func (c *Client) StoreWatchCursor(ctx context.Context, cursor time.Time) error {
	return c.rdb.Set(ctx, WatchCursorKey, cursor.UTC().Format(time.RFC3339Nano), 0).Err()
}

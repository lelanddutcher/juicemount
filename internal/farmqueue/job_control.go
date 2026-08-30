package farmqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrJobNotFound  = errors.New("farm job not found")
	ErrJobTerminal  = errors.New("farm job is already terminal")
	ErrJobNotFailed = errors.New("farm job is not failed")
)

// RequestJobCancel sets the cooperative cancellation flag. A queued job keeps
// its durable list entry; whichever worker claims it observes the flag and
// commits a canceled terminal state without running media stages.
func (c *Client) RequestJobCancel(ctx context.Context, id, requestedBy string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return ErrJobNotFound
	}
	at := nowISO()
	payload, _ := json.Marshal(map[string]string{"requested_at": at, "requested_by": strings.TrimSpace(requestedBy)})
	const requestCancelScript = `
local status = redis.call('HGET', KEYS[1], 'status')
if not status then return {-1, ''} end
if status == 'done' or status == 'failed' or status == 'canceled' or status == 'dispatched' then
  return {-2, status}
end
redis.call('SET', KEYS[2], ARGV[1], 'EX', ARGV[3])
redis.call('HSET', KEYS[1], 'cancel_requested_at', ARGV[2])
return {1, status}`
	result, err := c.rdb.Eval(ctx, requestCancelScript, []string{JobHashPrefix + id, CancelPrefix + id},
		payload, at, int64(JobTTL/time.Second)).Slice()
	if err != nil {
		return err
	}
	code, _ := result[0].(int64)
	status, _ := result[1].(string)
	switch code {
	case -1:
		return ErrJobNotFound
	case -2:
		return fmt.Errorf("%w: %s", ErrJobTerminal, status)
	case 1:
		return nil
	default:
		return fmt.Errorf("request cancel: unexpected Redis result %v", result)
	}
}

func (c *Client) JobCancellationRequested(ctx context.Context, id string) (bool, error) {
	n, err := c.rdb.Exists(ctx, CancelPrefix+strings.TrimSpace(id)).Result()
	return n > 0, err
}

func (c *Client) ClearJobCancellation(ctx context.Context, id string) error {
	return c.rdb.Del(ctx, CancelPrefix+strings.TrimSpace(id)).Err()
}

// RequeueFailed copies the exact last durable payload into a new job ID. The
// old terminal record remains immutable and links to the retry. A WATCH makes
// repeated clicks idempotent and rejects a status that changed under us.
func (c *Client) RequeueFailed(ctx context.Context, id, requestedBy string) (Job, bool, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return Job{}, false, ErrJobNotFound
	}
	oldKey := JobHashPrefix + id
	var retry Job
	created := false
	err := c.rdb.Watch(ctx, func(tx *redis.Tx) error {
		m, err := tx.HGetAll(ctx, oldKey).Result()
		if err != nil {
			return err
		}
		if len(m) == 0 {
			return ErrJobNotFound
		}
		if m["status"] != StatusFailed {
			return fmt.Errorf("%w: %s", ErrJobNotFailed, m["status"])
		}
		if existing := strings.TrimSpace(m["requeued_as"]); existing != "" {
			raw, err := tx.HGet(ctx, JobHashPrefix+existing, "payload").Result()
			if err != nil {
				return err
			}
			if err := json.Unmarshal([]byte(raw), &retry); err != nil {
				return err
			}
			return nil
		}
		if err := json.Unmarshal([]byte(m["payload"]), &retry); err != nil {
			return fmt.Errorf("failed job has no recoverable payload: %w", err)
		}
		retry.RequeueOf = id
		retry.ID = NewID()
		retry.EnqueuedAt = nowISO()
		retry.Attempts++
		raw, err := json.Marshal(retry)
		if err != nil {
			return err
		}
		st := JobStatus{
			ID: retry.ID, Status: StatusQueued, Path: retry.Path, Kinds: strings.Join(retry.Kinds, ","),
			Producer: retry.Producer, EnqueuedAt: retry.EnqueuedAt, Backend: retry.SelectedBackend,
			TargetWorker: retry.SelectedWorker, QueueClass: retry.QueueClass, Attempts: retry.Attempts,
			HardwareFailures: retry.HardwareFailures, ParentID: retry.ParentID, RequeueOf: id,
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.LPush(ctx, QueueKeyFor(&retry), raw)
			pipe.HSet(ctx, JobHashPrefix+retry.ID, st.toMap())
			pipe.HSet(ctx, JobHashPrefix+retry.ID, "payload", raw, "requeued_by", strings.TrimSpace(requestedBy))
			pipe.Expire(ctx, JobHashPrefix+retry.ID, JobTTL)
			pipe.ZAdd(ctx, JobIndexKey, redis.Z{Score: float64(time.Now().UnixMicro()), Member: retry.ID})
			pipe.HSet(ctx, oldKey, "requeued_as", retry.ID)
			return nil
		})
		if err == nil {
			created = true
		}
		return err
	}, oldKey)
	return retry, created, err
}

// WorkerLogLine is a bounded Redis-backed operational tail. It intentionally
// contains only timestamp + rendered line; Manager never executes its content.
type WorkerLogLine struct {
	At   string `json:"at"`
	Line string `json:"line"`
}

func workerLogKey(workerID string) string { return WorkerPrefix + workerID + ":log" }

func (c *Client) AppendWorkerLog(ctx context.Context, workerID, line string) error {
	if strings.TrimSpace(workerID) == "" || strings.TrimSpace(line) == "" {
		return nil
	}
	raw, err := json.Marshal(WorkerLogLine{At: nowISO(), Line: strings.TrimSpace(line)})
	if err != nil {
		return err
	}
	key := workerLogKey(workerID)
	pipe := c.rdb.TxPipeline()
	pipe.LPush(ctx, key, raw)
	pipe.LTrim(ctx, key, 0, 199)
	pipe.Expire(ctx, key, JobTTL)
	_, err = pipe.Exec(ctx)
	return err
}

func (c *Client) WorkerLog(ctx context.Context, workerID string, limit int64) ([]WorkerLogLine, error) {
	if limit <= 0 || limit > 200 {
		limit = 200
	}
	raws, err := c.rdb.LRange(ctx, workerLogKey(workerID), 0, limit-1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]WorkerLogLine, 0, len(raws))
	for _, raw := range raws {
		var entry WorkerLogLine
		if json.Unmarshal([]byte(raw), &entry) == nil {
			out = append(out, entry)
		}
	}
	return out, nil
}

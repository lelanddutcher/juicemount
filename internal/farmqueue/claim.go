package farmqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const claimLeaseTTL = 45 * time.Second

// Claim is the durable receipt for a job atomically moved from a ready lane to
// a worker processing list. Raw is retained so acknowledgement can remove the
// exact list entry without trusting mutable job fields.
type Claim struct {
	Job      Job
	Raw      string
	QueueKey string
	WorkerID string
}

// ClaimForWorker polls the worker's eligible lanes and atomically moves one raw
// job into its processing list. Unlike BRPOP, a worker crash cannot lose the
// entry: RecoverAbandonedWorkers can see and requeue the durable receipt.
func (c *Client) ClaimForWorker(ctx context.Context, timeout time.Duration, w Worker) (Claim, bool, error) {
	keys := WorkerQueueKeys(w)
	if len(keys) == 0 {
		return Claim{}, false, nil
	}
	deadline := time.Now().Add(timeout)
	processingKey := ProcessingPrefix + w.ID
	capabilities, _ := json.Marshal(workerCapabilitySet(w))
	const script = `
local controlRaw = redis.call('GET', KEYS[1])
if controlRaw then
  local controlOK, control = pcall(cjson.decode, controlRaw)
  if controlOK and control and control.paused then return {} end
end
local caps = cjson.decode(ARGV[2])
for i = 4, #KEYS do
  local count = redis.call('LLEN', KEYS[i])
  for n = 1, count do
    local raw = redis.call('RPOP', KEYS[i])
    if raw then
      local eligible = true
      local ok, job = pcall(cjson.decode, raw)
      if ok and job and job.required_capabilities then
        for _, required in ipairs(job.required_capabilities) do
          if not caps[required] then eligible = false; break end
        end
      end
		if eligible then
			redis.call('LPUSH', KEYS[3], raw)
			redis.call('SADD', KEYS[2], ARGV[1])
        return {raw, KEYS[i]}
      end
      redis.call('LPUSH', KEYS[i], raw)
    end
  end
end
return {}`
	scriptKeys := append([]string{ControlKey, ProcessingIndexKey, processingKey}, keys...)
	for {
		res, err := c.rdb.Eval(ctx, script, scriptKeys, w.ID, string(capabilities)).StringSlice()
		if err != nil && err != redis.Nil {
			return Claim{}, false, err
		}
		if len(res) == 2 {
			var job Job
			if err := json.Unmarshal([]byte(res[0]), &job); err != nil {
				_ = c.rdb.LRem(ctx, processingKey, 1, res[0]).Err()
				return Claim{}, false, fmt.Errorf("decode claimed job: %w", err)
			}
			return Claim{Job: job, Raw: res[0], QueueKey: res[1], WorkerID: w.ID}, true, nil
		}
		if timeout <= 0 || time.Now().After(deadline) {
			return Claim{}, false, nil
		}
		select {
		case <-ctx.Done():
			return Claim{}, false, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// RecoverUnserviceableReady demotes queued render jobs when no active render
// profile can satisfy their exact verified capability. This covers the window
// where a GPU disappears after scheduling but before it claims the job.
func (c *Client) RecoverUnserviceableReady(ctx context.Context) (int, error) {
	workers, err := c.ActiveWorkers(ctx)
	if err != nil {
		return 0, err
	}
	sort.SliceStable(workers, func(i, j int) bool {
		return workers[i].Benchmarks.EncodeFPS > workers[j].Benchmarks.EncodeFPS
	})
	recovered := 0
	for _, kind := range []string{KindProxy, KindTranscript} {
		key := classQueue(kind, QueueClassRender)
		raws, err := c.rdb.LRange(ctx, key, 0, -1).Result()
		if err != nil {
			return recovered, err
		}
		for _, raw := range raws {
			var job Job
			if json.Unmarshal([]byte(raw), &job) != nil {
				continue
			}
			serviceable := false
			for _, worker := range workers {
				if worker.Role == QueueClassRender && WorkerSupports(worker, job.RequiredCapabilities) {
					serviceable = true
					break
				}
			}
			if serviceable {
				continue
			}
			job.Attempts++
			targetClass := QueueClassCPU
			reason := "render capability went offline before claim; queued CPU fallback"
			if kind == KindProxy {
				if selected, encoder, ok := preferredHardwareWorker(workers); ok {
					targetClass = QueueClassRender
					job.QueueClass = QueueClassRender
					job.VCodec = encoder
					job.SelectedBackend = encoder
					job.SelectedWorker = workerDisplayName(selected)
					job.RequiredCapabilities = []string{"encoder:" + encoder, "worker:" + selected.ID}
					reason = "selected render capability went offline; re-routed to active hardware"
				} else {
					job.QueueClass = QueueClassCPU
					job.RequiredCapabilities = []string{"cpu"}
					job.VCodec = "libx264"
					job.SelectedBackend = "libx264"
					job.SelectedWorker = ""
				}
			} else if selected, backend, ok := preferredTranscriptWorker(workers); ok {
				targetClass = QueueClassRender
				job.QueueClass = QueueClassRender
				job.SelectedBackend = backend
				job.SelectedWorker = workerDisplayName(selected)
				job.RequiredCapabilities = []string{"transcript:" + backend, "worker:" + selected.ID}
				reason = "selected render capability went offline; re-routed to active hardware"
			} else {
				job.QueueClass = QueueClassCPU
				job.RequiredCapabilities = []string{"cpu"}
				job.SelectedBackend = "cpu"
				job.SelectedWorker = ""
			}
			nextRaw, err := json.Marshal(job)
			if err != nil {
				continue
			}
			const script = `
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 1 then
  redis.call('LPUSH', KEYS[2], ARGV[2])
  redis.call('HSET', KEYS[3], 'status', 'queued', 'backend', ARGV[3],
    'target_worker', ARGV[4], 'queue_class', ARGV[5], 'attempts', ARGV[6], 'error', ARGV[7])
  return 1
end
return 0`
			n, err := c.rdb.Eval(ctx, script, []string{
				key, classQueue(kind, targetClass), JobHashPrefix + job.ID,
			}, raw, string(nextRaw), job.SelectedBackend, job.SelectedWorker, targetClass, strconv.Itoa(job.Attempts), reason).Int()
			if err != nil {
				return recovered, err
			}
			recovered += n
		}
	}
	return recovered, nil
}

func (c *Client) MarkClaimRunning(ctx context.Context, claim Claim, worker Worker) error {
	expires := time.Now().Add(claimLeaseTTL)
	pipe := c.rdb.TxPipeline()
	pipe.HSet(ctx, JobHashPrefix+claim.Job.ID, map[string]any{
		"status": StatusRunning, "started_at": nowISO(), "worker": worker.Name,
		"backend": claim.Job.SelectedBackend, "queue_class": claim.Job.QueueClass,
		"attempts":    strconv.Itoa(claim.Job.Attempts),
		"lease_owner": worker.ID, "lease_expires_at": expires.UTC().Format(time.RFC3339),
	})
	pipe.ZAdd(ctx, LeaseIndexKey, redis.Z{Score: float64(expires.Unix()), Member: claim.Job.ID})
	_, err := pipe.Exec(ctx)
	return err
}

func (c *Client) RenewClaim(ctx context.Context, claim Claim) error {
	expires := time.Now().Add(claimLeaseTTL)
	pipe := c.rdb.TxPipeline()
	pipe.HSet(ctx, JobHashPrefix+claim.Job.ID, "lease_expires_at", expires.UTC().Format(time.RFC3339))
	pipe.ZAdd(ctx, LeaseIndexKey, redis.Z{Score: float64(expires.Unix()), Member: claim.Job.ID})
	_, err := pipe.Exec(ctx)
	return err
}

func (c *Client) AckClaim(ctx context.Context, claim Claim) error {
	processingKey := ProcessingPrefix + claim.WorkerID
	const script = `
redis.call('LREM', KEYS[1], 1, ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('HDEL', KEYS[3], 'lease_owner', 'lease_expires_at')
if redis.call('LLEN', KEYS[1]) == 0 then
  redis.call('SREM', KEYS[4], ARGV[3])
end
return 1`
	return c.rdb.Eval(ctx, script, []string{
		processingKey, LeaseIndexKey, JobHashPrefix + claim.Job.ID, ProcessingIndexKey,
	}, claim.Raw, claim.Job.ID, claim.WorkerID).Err()
}

// RequeueClaim atomically acknowledges a failed claim and publishes its next
// attempt. fallbackCPU is used after a render-node failure; the retry is routed
// to server H.264/CPU instead of silently changing backend inside ffmpeg.
func (c *Client) RequeueClaim(ctx context.Context, claim Claim, fallbackCPU bool, reason string) error {
	job := claim.Job
	job.Attempts++
	if fallbackCPU {
		job.QueueClass = QueueClassCPU
		job.RequiredCapabilities = []string{"cpu"}
		job.SelectedWorker = ""
		switch {
		case len(job.Kinds) == 1 && job.Kinds[0] == KindProxy:
			job.VCodec = "libx264"
			job.SelectedBackend = "libx264"
		case len(job.Kinds) == 1 && job.Kinds[0] == KindTranscript:
			job.SelectedBackend = "cpu"
		}
	} else {
		job.QueueClass = ""
		job.RequiredCapabilities = nil
		job.SelectedBackend = ""
		job.SelectedWorker = ""
		c.RouteInitialJob(ctx, &job)
	}
	return c.requeueClaimAs(ctx, claim, job, reason)
}

// RequeueClaimSameRoute releases a durable claim without consuming a hardware
// retry or changing its proven backend. It is reserved for queue-orchestration
// failures (for example, a transient Redis error while publishing target
// shards), which say nothing about the render node's codec capability.
func (c *Client) RequeueClaimSameRoute(ctx context.Context, claim Claim, reason string) error {
	return c.requeueClaimAs(ctx, claim, claim.Job, reason)
}

func (c *Client) requeueClaimAs(ctx context.Context, claim Claim, job Job, reason string) error {
	raw, err := json.Marshal(job)
	if err != nil {
		return err
	}
	target := QueueKeyFor(&job)
	processingKey := ProcessingPrefix + claim.WorkerID
	const script = `
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then
  return 0
end
redis.call('LPUSH', KEYS[2], ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[3])
redis.call('HSET', KEYS[4],
  'status', 'queued', 'worker', '', 'backend', ARGV[4],
  'target_worker', ARGV[5], 'queue_class', ARGV[6], 'attempts', ARGV[7], 'error', ARGV[8],
  'processed', ARGV[10], 'failed', '0')
redis.call('HDEL', KEYS[4], 'lease_owner', 'lease_expires_at', 'finished_at')
if redis.call('LLEN', KEYS[1]) == 0 then
  redis.call('SREM', KEYS[5], ARGV[9])
end
return 1`
	n, err := c.rdb.Eval(ctx, script, []string{
		processingKey, target, LeaseIndexKey, JobHashPrefix + job.ID, ProcessingIndexKey,
	}, claim.Raw, string(raw), job.ID, job.SelectedBackend, job.SelectedWorker, job.QueueClass,
		strconv.Itoa(job.Attempts), reason, claim.WorkerID, strconv.Itoa(job.ProcessedOffset)).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("claim %s is no longer owned by worker %s", job.ID, claim.WorkerID)
	}
	return nil
}

// RecoverAbandonedWorkers requeues every durable claim owned by a worker whose
// heartbeat expired. Routing is recomputed against current measured worker
// profiles: another verified render node wins, while CPU/H.264 is used only
// when no suitable accelerator remains online.
func (c *Client) RecoverAbandonedWorkers(ctx context.Context) (int, error) {
	workers, err := c.rdb.SMembers(ctx, ProcessingIndexKey).Result()
	if err != nil {
		return 0, err
	}
	total := 0
	for _, workerID := range workers {
		alive, err := c.rdb.Exists(ctx, WorkerPrefix+workerID).Result()
		if err != nil || alive > 0 {
			continue
		}
		lock := "juicefarm:reaper:" + workerID
		got, err := c.rdb.SetNX(ctx, lock, "1", 15*time.Second).Result()
		if err != nil || !got {
			continue
		}
		processingKey := ProcessingPrefix + workerID
		raws, listErr := c.rdb.LRange(ctx, processingKey, 0, -1).Result()
		if listErr != nil {
			_ = c.rdb.Del(ctx, lock).Err()
			return total, listErr
		}
		for _, raw := range raws {
			var job Job
			if err := json.Unmarshal([]byte(raw), &job); err != nil || job.ID == "" {
				if err := c.requeueMalformedClaim(ctx, processingKey, workerID, raw); err != nil {
					_ = c.rdb.Del(ctx, lock).Err()
					return total, err
				}
				total++
				continue
			}
			claim := Claim{Job: job, Raw: raw, WorkerID: workerID}
			reason := "recovered after worker " + workerID + " disappeared; compatible worker selection rerun"
			if err := c.RequeueClaim(ctx, claim, false, reason); err != nil {
				_ = c.rdb.Del(ctx, lock).Err()
				return total, err
			}
			total++
		}
		_ = c.rdb.Del(ctx, lock).Err()
	}
	return total, nil
}

func (c *Client) requeueMalformedClaim(ctx context.Context, processingKey, workerID, raw string) error {
	const script = `
redis.call('LREM', KEYS[1], 1, ARGV[1])
redis.call('LPUSH', KEYS[2], ARGV[1])
if redis.call('LLEN', KEYS[1]) == 0 then
  redis.call('SREM', KEYS[3], ARGV[2])
end
return 1`
	return c.rdb.Eval(ctx, script, []string{processingKey, QueueKey, ProcessingIndexKey}, raw, workerID).Err()
}

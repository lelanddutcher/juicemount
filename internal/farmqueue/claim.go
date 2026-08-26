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
		c.RouteJob(ctx, &job)
	}
	raw, err := json.Marshal(job)
	if err != nil {
		return err
	}
	target := QueueKeyFor(&job)
	processingKey := ProcessingPrefix + claim.WorkerID
	const script = `
redis.call('LREM', KEYS[1], 1, ARGV[1])
redis.call('LPUSH', KEYS[2], ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[3])
redis.call('HSET', KEYS[4],
  'status', 'queued', 'worker', '', 'backend', ARGV[4],
  'target_worker', ARGV[5], 'queue_class', ARGV[6], 'attempts', ARGV[7], 'error', ARGV[8])
redis.call('HDEL', KEYS[4], 'lease_owner', 'lease_expires_at', 'finished_at')
if redis.call('LLEN', KEYS[1]) == 0 then
  redis.call('SREM', KEYS[5], ARGV[9])
end
return 1`
	return c.rdb.Eval(ctx, script, []string{
		processingKey, target, LeaseIndexKey, JobHashPrefix + job.ID, ProcessingIndexKey,
	}, claim.Raw, string(raw), job.ID, job.SelectedBackend, job.SelectedWorker, job.QueueClass,
		strconv.Itoa(job.Attempts), reason, claim.WorkerID).Err()
}

// RecoverAbandonedWorkers requeues every durable claim owned by a worker whose
// heartbeat expired. Render work is deliberately demoted to the CPU fallback
// lane; this is the visible, auditable HEVC/H.264 failover boundary.
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
		const script = `
local raws = redis.call('LRANGE', KEYS[1], 0, -1)
for _, raw in ipairs(raws) do
  local ok, job = pcall(cjson.decode, raw)
  if ok and job and job.id then
    local kind = ''
    if job.kinds and #job.kinds == 1 then kind = job.kinds[1] end
    job.attempts = (job.attempts or 0) + 1
    local class = job.queue_class or ''
    local backend = job.selected_backend or ''
    local target = ARGV[1]
    if class == 'render' and kind == 'proxy' then
      class = 'cpu'; backend = 'libx264'; job.vcodec = 'libx264'
      job.required_capabilities = {'cpu'}; job.selected_worker = ''
      target = ARGV[1] .. ':proxy:cpu'
    elseif class == 'render' and kind == 'transcript' then
      class = 'cpu'; backend = 'cpu'; job.required_capabilities = {'cpu'}; job.selected_worker = ''
      target = ARGV[1] .. ':transcript:cpu'
    elseif kind == 'derivatives' then
      class = 'server'; backend = 'server-cpu'; target = ARGV[1] .. ':derivatives:server'
    end
    job.queue_class = class; job.selected_backend = backend
    local nextRaw = cjson.encode(job)
    redis.call('LPUSH', target, nextRaw)
    local h = ARGV[2] .. job.id
    redis.call('HSET', h, 'status', 'queued', 'worker', '', 'backend', backend,
      'target_worker', job.selected_worker or '', 'queue_class', class, 'attempts', tostring(job.attempts),
      'error', 'recovered after worker ' .. ARGV[3] .. ' disappeared')
    redis.call('HDEL', h, 'lease_owner', 'lease_expires_at', 'finished_at')
    redis.call('ZREM', KEYS[3], job.id)
  else
    redis.call('LPUSH', ARGV[1], raw)
  end
end
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[2], ARGV[3])
return #raws`
		n, evalErr := c.rdb.Eval(ctx, script, []string{
			ProcessingPrefix + workerID, ProcessingIndexKey, LeaseIndexKey,
		}, QueueKey, JobHashPrefix, workerID).Int()
		_ = c.rdb.Del(ctx, lock).Err()
		if evalErr != nil {
			return total, evalErr
		}
		total += n
	}
	return total, nil
}

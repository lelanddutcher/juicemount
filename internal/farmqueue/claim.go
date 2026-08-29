package farmqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const claimLeaseTTL = 45 * time.Second

const maxReadyPromotionsPerScan = 512

const (
	// A single missed 30s heartbeat must not dump an entire render backlog onto
	// the slow CPU lane. Preserve ready work for two worker TTLs, then perform
	// the visible fallback if no compatible hardware has returned.
	readyFallbackGrace      = 2 * WorkerTTL
	readyFallbackSinceField = "unserviceable_since"
)

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
	decodeLimits, _ := json.Marshal(w.DecodeLimits)
	if string(decodeLimits) == "null" {
		decodeLimits = []byte("{}")
	}
	const script = `
local controlRaw = redis.call('GET', KEYS[1])
if controlRaw then
  local controlOK, control = pcall(cjson.decode, controlRaw)
  if controlOK and control and control.paused then return {} end
end
local caps = cjson.decode(ARGV[2])
local decodeLimits = cjson.decode(ARGV[3])
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
      if eligible and job and job.required_capabilities and
          job.source_video_width and job.source_video_height then
        local decoder = nil
        for _, required in ipairs(job.required_capabilities) do
          decoder = string.match(required, '^decoder:([^:]+)$')
          if decoder then break end
        end
        if decoder then
          local limit = decodeLimits[decoder]
          local width = tonumber(job.source_video_width) or 0
          local height = tonumber(job.source_video_height) or 0
          if width > 0 and height > 0 and
              (not limit or (tonumber(limit.max_width) or 0) < width or
               (tonumber(limit.max_height) or 0) < height) then
            eligible = false
          end
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
		res, err := c.rdb.Eval(ctx, script, scriptKeys, w.ID, string(capabilities), string(decodeLimits)).StringSlice()
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
	for _, kind := range []string{KindDerivatives, KindProxy, KindTranscript} {
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
				if worker.Role == QueueClassRender && workerCanClaimJob(worker, job) {
					serviceable = true
					break
				}
			}
			if serviceable {
				// A prior miss recovered before the grace expired. Clear the durable
				// timer so a later independent outage receives its own full window.
				_ = c.rdb.HDel(ctx, JobHashPrefix+job.ID, readyFallbackSinceField).Err()
				continue
			}
			targetClass := QueueClassCPU
			reason := "render capability went offline before claim; queued CPU fallback"
			if kind == KindDerivatives && job.DerivativePass == DerivativePassPreviews {
				if _, decoder, ok := preferredHardwareDecoderWorkerForSourceWithLoad(
					workers, job.SourceVideoCodec, job.SourceVideoProfile, job.SourcePixelFormat, job.SourceVideoWidth, job.SourceVideoHeight, nil,
				); ok {
					targetClass = QueueClassRender
					job.QueueClass = QueueClassRender
					job.SelectedBackend = decoder
					job.SelectedWorker = ""
					job.RequiredCapabilities = DecoderSourceRequirements(
						decoder, job.SourceVideoCodec, job.SourceVideoProfile, job.SourcePixelFormat,
					)
					reason = "selected decoder went offline; re-routed to active verified hardware"
				} else {
					job.QueueClass = QueueClassCPU
					job.RequiredCapabilities = []string{"cpu"}
					job.SelectedBackend = "cpu-decode"
					job.SelectedWorker = ""
					reason = "verified video decoder went offline before claim; queued explicit CPU decode fallback"
				}
			} else if kind == KindProxy {
				candidate := job
				candidate.QueueClass = ""
				candidate.RequiredCapabilities = nil
				candidate.SelectedBackend = ""
				candidate.SelectedWorker = ""
				candidate.RoutingReason = ""
				candidate.VCodec = ""
				routeJobWithWorkers(&candidate, workers, nil)
				if candidate.QueueClass == QueueClassRender {
					targetClass = QueueClassRender
					job = candidate
					reason = "selected render capability went offline; re-routed to active hardware"
				} else {
					job = candidate
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
			if targetClass == QueueClassCPU {
				elapsed, err := c.readyFallbackGraceElapsed(ctx, JobHashPrefix+job.ID)
				if err != nil {
					return recovered, err
				}
				if !elapsed {
					continue
				}
			}
			// Losing the selected worker before claim is an availability event,
			// not an execution attempt and never proof that the source cannot use
			// hardware. Preserve both retry counters. Clear any stale
			// terminal bit carried by a previously promoted legacy child so the
			// queued CPU fallback can be promoted again when hardware returns.
			job.CPUFallbackLocked = false
			if targetClass == QueueClassCPU {
				job.CPUFallbackLocked = false
				if strings.TrimSpace(job.RoutingReason) == "" {
					job.RoutingReason = reason
				} else {
					reason = job.RoutingReason
				}
			}
			nextRaw, err := json.Marshal(job)
			if err != nil {
				continue
			}
			const script = `
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 1 then
  redis.call('LPUSH', KEYS[2], ARGV[2])
  redis.call('HSET', KEYS[3], 'status', 'queued', 'backend', ARGV[3],
	'target_worker', ARGV[4], 'queue_class', ARGV[5], 'attempts', ARGV[6],
	'hardware_failures', ARGV[7], 'error', ARGV[8])
	redis.call('HDEL', KEYS[3], 'unserviceable_since')
  return 1
end
return 0`
			n, err := c.rdb.Eval(ctx, script, []string{
				key, classQueue(kind, targetClass), JobHashPrefix + job.ID,
			}, raw, string(nextRaw), job.SelectedBackend, job.SelectedWorker, targetClass,
				strconv.Itoa(job.Attempts), strconv.Itoa(job.HardwareFailures), reason).Int()
			if err != nil {
				return recovered, err
			}
			recovered += n
		}
	}
	return recovered, nil
}

func (c *Client) readyFallbackGraceElapsed(ctx context.Context, statusKey string) (bool, error) {
	since, err := c.rdb.HGet(ctx, statusKey, readyFallbackSinceField).Result()
	if err == redis.Nil || strings.TrimSpace(since) == "" {
		_, setErr := c.rdb.HSetNX(ctx, statusKey, readyFallbackSinceField, time.Now().UTC().Format(time.RFC3339Nano)).Result()
		return false, setErr
	}
	if err != nil {
		return false, err
	}
	started, parseErr := time.Parse(time.RFC3339Nano, since)
	if parseErr != nil {
		// A malformed maintenance marker is not evidence of a long outage. Reset
		// it rather than immediately spilling work onto CPU.
		return false, c.rdb.HSet(ctx, statusKey, readyFallbackSinceField,
			time.Now().UTC().Format(time.RFC3339Nano)).Err()
	}
	return time.Since(started) >= readyFallbackGrace, nil
}

// PromoteServiceableReady moves only temporary CPU fallbacks back onto a
// verified render lane when compatible hardware returns. Jobs explicitly
// locked to CPU because their source failed hardware admission or exhausted
// its retry budget stay on CPU; active durable claims are never inspected.
//
// The Redis mutation is compare-and-move on the exact serialized list value,
// making concurrent maintenance scans idempotent. A bounded scan limit avoids
// letting a large historical CPU queue monopolize the worker heartbeat loop.
func (c *Client) PromoteServiceableReady(ctx context.Context) (int, error) {
	workers, err := c.ActiveWorkers(ctx)
	if err != nil {
		return 0, err
	}
	hasRender := false
	for _, worker := range workers {
		if worker.Role == QueueClassRender {
			hasRender = true
			break
		}
	}
	if !hasRender {
		return 0, nil
	}
	loads := c.queuedWorkerLoads(ctx, workers)
	promoted := 0
	for _, kind := range []string{KindDerivatives, KindProxy, KindTranscript} {
		sourceKey := classQueue(kind, QueueClassCPU)
		raws, err := c.rdb.LRange(ctx, sourceKey, 0, -1).Result()
		if err != nil {
			return promoted, err
		}
		for _, raw := range raws {
			if promoted >= maxReadyPromotionsPerScan {
				return promoted, nil
			}
			var original Job
			if json.Unmarshal([]byte(raw), &original) != nil || original.ID == "" {
				continue
			}
			statusKey := JobHashPrefix + original.ID
			previousReason, err := c.rdb.HGet(ctx, statusKey, "error").Result()
			if err != nil && err != redis.Nil {
				return promoted, err
			}
			// Compatibility and exhausted-render fallbacks are terminal. A stale
			// lock whose durable provenance says only that a worker disappeared is
			// repairable: the returning worker will perform live source admission
			// before touching user media and split only truly incompatible files.
			if legacyCPUFallbackLocked(original, previousReason) &&
				!temporaryAvailabilityFallback(original, previousReason) {
				continue
			}

			candidate := original
			candidate.QueueClass = ""
			candidate.RequiredCapabilities = nil
			candidate.SelectedBackend = ""
			candidate.SelectedWorker = ""
			candidate.RoutingReason = ""
			candidate.CPUFallbackLocked = false
			if kind == KindProxy {
				candidate.VCodec = ""
			}
			routeJobWithWorkers(&candidate, workers, loads)
			if candidate.QueueClass != QueueClassRender {
				continue
			}

			promotionReason := "verified render capability returned; promoted queued CPU fallback to " + candidate.SelectedBackend
			if strings.TrimSpace(previousReason) != "" {
				promotionReason = previousReason + "; " + promotionReason
			}
			candidate.RoutingReason = promotionReason
			nextRaw, err := json.Marshal(candidate)
			if err != nil {
				continue
			}
			const script = `
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 1 then
  redis.call('LPUSH', KEYS[2], ARGV[2])
  redis.call('HSET', KEYS[3], 'status', 'queued', 'worker', '',
    'backend', ARGV[3], 'target_worker', ARGV[4], 'queue_class', ARGV[5], 'error', ARGV[6])
  redis.call('HDEL', KEYS[3], 'lease_owner', 'lease_expires_at', 'finished_at')
  return 1
end
return 0`
			n, err := c.rdb.Eval(ctx, script, []string{
				sourceKey, classQueue(kind, QueueClassRender), statusKey,
			}, raw, string(nextRaw), candidate.SelectedBackend, candidate.SelectedWorker,
				candidate.QueueClass, promotionReason).Int()
			if err != nil {
				return promoted, err
			}
			if n == 1 {
				promoted++
				for _, capability := range candidate.RequiredCapabilities {
					if id, ok := strings.CutPrefix(capability, "worker:"); ok && id != "" {
						loads[id]++
						break
					}
				}
			}
		}
	}
	return promoted, nil
}

// Old queued records predate CPUFallbackLocked. Preserve the terminal intent
// encoded in their deterministic split ID and visible status reason so an RC
// upgrade cannot create a retry loop for known-incompatible media.
func legacyCPUFallbackLocked(job Job, statusReason string) bool {
	if job.CPUFallbackLocked || (strings.HasSuffix(job.ID, "-cpu") && job.ParentID != "") {
		return true
	}
	reason := strings.ToLower(strings.TrimSpace(statusReason))
	for _, marker := range []string{
		"queued explicit cpu", "explicit cpu/h.264 fallback", "render backend failed after",
		"live source admission rejected", "source(s) partitioned to explicit cpu",
	} {
		if strings.Contains(reason, marker) {
			return true
		}
	}
	if len(job.Kinds) == 1 && job.Kinds[0] == KindDerivatives {
		routing := strings.ToLower(strings.TrimSpace(job.RoutingReason))
		if routing != "" && !strings.HasPrefix(routing, "no verified hardware decoder is online for ") &&
			!strings.Contains(routing, "decoder went offline") &&
			!strings.Contains(routing, "worker discovery unavailable") {
			return true
		}
	}
	return false
}

// temporaryAvailabilityFallback recognizes CPU routing caused only by worker
// availability. Raw job provenance wins over the mutable status note: a new
// source-incompatible child carries its terminal reason in RoutingReason, while
// a legacy child whose old lock survived an outage can carry the exact outage
// reason shown in the deployed RC queue. Returning hardware will still run its
// live admission probe, so repairing the stale lock cannot silently execute an
// unsupported source on CPU inside a GPU worker.
func temporaryAvailabilityFallback(job Job, statusReason string) bool {
	routing := strings.ToLower(strings.TrimSpace(job.RoutingReason))
	if routing != "" {
		return containsAvailabilityFallbackMarker(routing)
	}
	return containsAvailabilityFallbackMarker(strings.ToLower(strings.TrimSpace(statusReason)))
}

func containsAvailabilityFallbackMarker(reason string) bool {
	for _, marker := range []string{
		"recovered after worker",
		"render capability went offline before claim",
		"video decoder went offline before claim",
		"selected render capability went offline",
		"no verified hardware decode+encode path is online",
		"no verified hardware decoder is online",
		"worker discovery unavailable",
	} {
		if strings.Contains(reason, marker) {
			return true
		}
	}
	return false
}

func (c *Client) MarkClaimRunning(ctx context.Context, claim Claim, worker Worker) error {
	expires := time.Now().Add(claimLeaseTTL)
	pipe := c.rdb.TxPipeline()
	pipe.HSet(ctx, JobHashPrefix+claim.Job.ID, map[string]any{
		"status": StatusRunning, "started_at": nowISO(), "worker": worker.Name,
		"backend": claim.Job.SelectedBackend, "queue_class": claim.Job.QueueClass,
		"attempts":          strconv.Itoa(claim.Job.Attempts),
		"hardware_failures": strconv.Itoa(claim.Job.HardwareFailures),
		"lease_owner":       worker.ID, "lease_expires_at": expires.UTC().Format(time.RFC3339),
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

// RequeueClaim atomically acknowledges a claim and publishes its next delivery.
// fallbackCPU is used after a render-node failure; the retry is routed to server
// H.264/CPU instead of silently changing backend inside ffmpeg. Callers must use
// RequeueClaimAfterHardwareFailure when a completed accelerator execution, rather
// than worker availability, caused the requeue.
func (c *Client) RequeueClaim(ctx context.Context, claim Claim, fallbackCPU bool, reason string) error {
	job := claim.Job
	job.Attempts++
	if fallbackCPU {
		job.QueueClass = QueueClassCPU
		job.RequiredCapabilities = []string{"cpu"}
		job.SelectedWorker = ""
		job.CPUFallbackLocked = true
		switch {
		case len(job.Kinds) == 1 && job.Kinds[0] == KindProxy:
			job.VCodec = "libx264"
			job.SelectedBackend = "libx264"
		case len(job.Kinds) == 1 && job.Kinds[0] == KindTranscript:
			job.SelectedBackend = "cpu"
		case len(job.Kinds) == 1 && job.Kinds[0] == KindDerivatives && job.DerivativePass == DerivativePassPreviews:
			job.SelectedBackend = "cpu-decode"
		}
	} else {
		job.QueueClass = ""
		job.RequiredCapabilities = nil
		job.SelectedBackend = ""
		job.SelectedWorker = ""
		c.RouteInitialJob(ctx, &job)
		if len(job.Kinds) == 1 && job.Kinds[0] == KindDerivatives &&
			job.DerivativePass == DerivativePassPreviews && job.QueueClass == QueueClassRender {
			job.SelectedWorker = ""
			caps := job.RequiredCapabilities[:0]
			for _, capability := range job.RequiredCapabilities {
				if !strings.HasPrefix(capability, "worker:") {
					caps = append(caps, capability)
				}
			}
			job.RequiredCapabilities = caps
		}
	}
	return c.requeueClaimAs(ctx, claim, job, reason)
}

// RequeueClaimAfterHardwareFailure records exactly one completed accelerator
// execution failure before releasing the durable claim. Keeping this counter
// separate from delivery Attempts prevents restarts and ready-lane rerouting
// from exhausting the hardware retry policy without any codec execution.
func (c *Client) RequeueClaimAfterHardwareFailure(ctx context.Context, claim Claim, fallbackCPU bool, reason string) error {
	claim.Job.HardwareFailures++
	return c.RequeueClaim(ctx, claim, fallbackCPU, reason)
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
  'target_worker', ARGV[5], 'queue_class', ARGV[6], 'attempts', ARGV[7],
  'hardware_failures', ARGV[8], 'error', ARGV[9], 'processed', ARGV[11], 'failed', '0')
redis.call('HDEL', KEYS[4], 'lease_owner', 'lease_expires_at', 'finished_at')
if redis.call('LLEN', KEYS[1]) == 0 then
  redis.call('SREM', KEYS[5], ARGV[10])
end
return 1`
	n, err := c.rdb.Eval(ctx, script, []string{
		processingKey, target, LeaseIndexKey, JobHashPrefix + job.ID, ProcessingIndexKey,
	}, claim.Raw, string(raw), job.ID, job.SelectedBackend, job.SelectedWorker, job.QueueClass,
		strconv.Itoa(job.Attempts), strconv.Itoa(job.HardwareFailures), reason,
		claim.WorkerID, strconv.Itoa(job.ProcessedOffset)).Int()
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

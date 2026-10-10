package farmqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const claimLeaseTTL = 45 * time.Second
const trackedCustodyIndexKey = "juicefarm:watch:v2:custody:jobs"

var ErrClaimLost = errors.New("farm claim ownership lost")

const maxReadyPromotionsPerScan = 512
const maxReadyRecoveryInspectionsPerLane = 128

func maintenanceQueueKey(kind, class string, tracked bool) string {
	if tracked {
		return TrackedQueuePrefix + class + ":" + kind
	}
	return classQueue(kind, class)
}

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
	Job            Job
	Raw            string
	QueueKey       string
	WorkerID       string
	Token          string
	Generation     int64
	LeaseExpiresAt time.Time
}

func claimProcessingKey(claim Claim) string {
	if claim.Job.TrackedEpoch != "" {
		return TrackedProcessingPrefix + claim.WorkerID
	}
	return ProcessingPrefix + claim.WorkerID
}

func claimProcessingIndex(claim Claim) string {
	if claim.Job.TrackedEpoch != "" {
		return TrackedProcessingIndexKey
	}
	return ProcessingIndexKey
}

const claimForWorkerScript = `
local controlRaw = redis.call('GET', KEYS[1])
if controlRaw then
  local controlOK, control = pcall(cjson.decode, controlRaw)
  if controlOK and control and control.paused then return {} end
end
if ARGV[6] == '1' and redis.call('EXISTS', KEYS[2]) == 0 then return {} end
local caps = cjson.decode(ARGV[2])
local decodeLimits = cjson.decode(ARGV[3])
local workerPin = ARGV[4]
local workerDisplay = ARGV[5]
local currentWorker = nil
local authoritativeRenderProfile = false
if ARGV[10] == 'render' then
  local heartbeatRaw = redis.call('GET', 'juicefarm:worker:' .. ARGV[1])
  if not heartbeatRaw then
    return {}
  end
  local heartbeatOK, heartbeat = pcall(cjson.decode, heartbeatRaw)
  if not heartbeatOK or type(heartbeat) ~= 'table' or heartbeat.id ~= ARGV[1] or
     heartbeat.role ~= 'render' or heartbeat.admission_blocked == true then
    return {}
  end
  local permitRaw = redis.call('GET', KEYS[2])
  local permitOK, permit = false, nil
  if permitRaw then permitOK, permit = pcall(cjson.decode, permitRaw) end
  local permitTTL = redis.call('PTTL', KEYS[2])
  local redisTime = redis.call('TIME')
  local permitNow = tonumber(redisTime[1])
  local permitChecked = nil
  -- Pre-H-2 publishers write only a canonical whole-second UTC checked_at.
  -- Same rule as legacyPermitCheckedUnix in storage_permit.go.
  local function legacyPermitChecked(text)
    if type(text) ~= 'string' then return nil end
    local y, mo, d, h, mi, se = string.match(text, '^(%d%d%d%d)%-(%d%d)%-(%d%d)T(%d%d):(%d%d):(%d%d)Z$')
    if not y then return nil end
    y, mo, d, h, mi, se = tonumber(y), tonumber(mo), tonumber(d), tonumber(h), tonumber(mi), tonumber(se)
    local monthDays = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
    if (y % 4 == 0 and y % 100 ~= 0) or y % 400 == 0 then monthDays[2] = 29 end
    if mo < 1 or mo > 12 or d < 1 or d > monthDays[mo] or h > 23 or mi > 59 or se > 59 then return nil end
    local cy = y
    if mo <= 2 then cy = cy - 1 end
    local era = math.floor(cy / 400)
    local yoe = cy - era * 400
    local doy = math.floor((153 * ((mo + 9) % 12) + 2) / 5) + d - 1
    local doe = yoe * 365 + math.floor(yoe / 4) - math.floor(yoe / 100) + doy
    return (era * 146097 + doe - 719468) * 86400 + h * 3600 + mi * 60 + se
  end
  if permitOK and type(permit) == 'table' then
    permitChecked = permit.checked_at_unix
    if permitChecked == nil then permitChecked = legacyPermitChecked(permit.checked_at) end
  end
  local function validPermitInteger(value)
    return type(value) == 'number' and value >= 0 and value <= 9007199254740991 and
      math.floor(value) == value
  end
  if not permitOK or type(permit) ~= 'table' or type(permit.source) ~= 'string' or permit.source == '' or
     not validPermitInteger(permitChecked) or permitChecked <= 0 or permitChecked > permitNow + 2 or
     permitNow - permitChecked >= 20 or
     not validPermitInteger(permit.available_bytes) or not validPermitInteger(permit.required_bytes) or
     (permit.total_bytes ~= nil and not validPermitInteger(permit.total_bytes)) or
     permit.available_bytes < permit.required_bytes or permitTTL <= 0 or permitTTL > 20000 then
    return {}
  end
  local benchmarks = heartbeat.benchmarks
  if type(benchmarks) ~= 'table' or type(benchmarks.probed_at) ~= 'string' or
     not string.match(benchmarks.probed_at, '^%d%d%d%d%-%d%d%-%d%dT%d%d:%d%d:%d%dZ$') or
     type(heartbeat.encoders or {}) ~= 'table' or type(heartbeat.decoders or {}) ~= 'table' or
     type(heartbeat.transcript_backends or {}) ~= 'table' or type(heartbeat.capabilities or {}) ~= 'table' or
     type(heartbeat.decode_limits or {}) ~= 'table' then
    return redis.error_reply('invalid render heartbeat profile')
  end
  currentWorker = heartbeat
  authoritativeRenderProfile = true
  caps = {cpu = true, ['worker:' .. ARGV[1]] = true}
  for _, value in ipairs(heartbeat.capabilities or {}) do
    if type(value) ~= 'string' then return redis.error_reply('invalid render heartbeat capabilities') end
    local encoder = string.match(value, '^encoder:(.+)$')
    local decoder = string.match(value, '^decoder:([^:]+)')
    local transcript = string.match(value, '^transcript:(.+)$')
    local measured = true
    if string.match(value, '^encoder:') then
      measured = false
      for _, candidate in ipairs(heartbeat.encoders or {}) do
        if encoder and candidate == encoder then measured = true break end
      end
    elseif string.match(value, '^decoder:') then
      measured = false
      for _, candidate in ipairs(heartbeat.decoders or {}) do
        if decoder and candidate == decoder then measured = true break end
      end
    elseif string.match(value, '^transcript:') then
      measured = false
      for _, candidate in ipairs(heartbeat.transcript_backends or {}) do
        if candidate == transcript and candidate ~= '' and string.lower(candidate) ~= 'cpu' then
          measured = true
          break
        end
      end
    end
    if measured then caps[value] = true end
  end
  for _, value in ipairs(heartbeat.encoders or {}) do
    if type(value) ~= 'string' then return redis.error_reply('invalid render heartbeat encoders') end
    caps['encoder:' .. value] = true
  end
  for _, value in ipairs(heartbeat.decoders or {}) do
    if type(value) ~= 'string' then return redis.error_reply('invalid render heartbeat decoders') end
    caps['decoder:' .. value] = true
  end
  for _, value in ipairs(heartbeat.transcript_backends or {}) do
    if type(value) ~= 'string' then return redis.error_reply('invalid render heartbeat transcript backends') end
    if value ~= '' and string.lower(value) ~= 'cpu' then caps['transcript:' .. value] = true end
  end
  local name = string.lower(string.gsub(heartbeat.name or '', '^%s*(.-)%s*$', '%1'))
  if #name > 0 and #name <= 64 and string.match(name, '^[a-z0-9_.-]+$') then
    workerPin = 'worker-name:' .. name
    workerDisplay = heartbeat.name
  else
    workerPin = 'worker:' .. ARGV[1]
    workerDisplay = ARGV[1]
  end
  caps[workerPin] = true
  decodeLimits = heartbeat.decode_limits or {}
elseif ARGV[6] == '1' and redis.call('EXISTS', KEYS[2]) == 0 then
  return {}
end
if authoritativeRenderProfile then
  if type(decodeLimits) ~= 'table' then
    return redis.error_reply('invalid render decode-limits profile')
  end
  for decoder, limit in pairs(decodeLimits) do
    if type(decoder) ~= 'string' or type(limit) ~= 'table' or
       type(limit.max_width) ~= 'number' or type(limit.max_height) ~= 'number' or
       limit.max_width < 0 or limit.max_height < 0 or
       math.floor(limit.max_width) ~= limit.max_width or
       math.floor(limit.max_height) ~= limit.max_height then
      return redis.error_reply('invalid render decode-limits profile')
    end
  end
end
for i = 8, #KEYS do
  local count = redis.call('LLEN', KEYS[i])
  for n = 1, count do
    -- Inspect the next candidate without removing it. Tracked execution claims
    -- must transfer directly from ready to processing after the worker index
    -- is durable, so a Lua runtime error cannot strand an unindexed raw.
    local raw = redis.call('LINDEX', KEYS[i], -1)
    if raw then
      local sourceRaw = raw
      local eligible = true
      local staleRuntimePin = false
      local ok, job = pcall(cjson.decode, raw)
      if authoritativeRenderProfile then
        local kind = nil
        if job and job.kinds and #job.kinds == 1 then kind = job.kinds[1] end
        local admittedLane = false
        if kind == 'proxy' then
          admittedLane = currentWorker.encoders and #currentWorker.encoders > 0 and
            type(currentWorker.benchmarks.encode_fps) == 'number' and currentWorker.benchmarks.encode_fps > 0
          if admittedLane then
            for _, required in ipairs(job.required_capabilities or {}) do
              if string.match(required, '^decoder:') then
                admittedLane = currentWorker.decoders and #currentWorker.decoders > 0 and
                  type(currentWorker.benchmarks.decode_fps) == 'number' and currentWorker.benchmarks.decode_fps > 0
                break
              end
            end
          end
        elseif kind == 'derivatives' then
          admittedLane = currentWorker.decoders and #currentWorker.decoders > 0 and
            type(currentWorker.benchmarks.decode_fps) == 'number' and currentWorker.benchmarks.decode_fps > 0
        elseif kind == 'transcript' then
          for _, backend in ipairs(currentWorker.transcript_backends or {}) do
            if type(backend) == 'string' and backend ~= '' and string.lower(backend) ~= 'cpu' then
              admittedLane = type(currentWorker.benchmarks.transcript_x_realtime) == 'number' and
                currentWorker.benchmarks.transcript_x_realtime > 0
              break
            end
          end
        end
        if not admittedLane then eligible = false end
      end
      if ok and job and job.required_capabilities then
        for _, required in ipairs(job.required_capabilities) do
          if not caps[required] then
            local pinned = string.match(required, '^worker:(.+)$')
              if pinned and redis.call('EXISTS', 'juicefarm:worker:' .. pinned) == 0 then
              staleRuntimePin = true
            else
              eligible = false
              break
            end
          end
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
      if eligible and job and job.id and redis.call('HEXISTS', 'juicefarm:job:' .. job.id, 'held_token') == 1 then
        eligible = false
      end
      if eligible then
        local reclaimedOwner = nil
        local processing = KEYS[4]
        local processingIndex = KEYS[3]
        if job and job.tracked_epoch then
          processing = KEYS[6]
          processingIndex = KEYS[5]
          local statusType = redis.call('TYPE', 'juicefarm:job:' .. job.id).ok
          local processingType = redis.call('TYPE', processing).ok
          local processingIndexType = redis.call('TYPE', processingIndex).ok
          local leaseIndexType = redis.call('TYPE', KEYS[7]).ok
          local custodyIndexType = redis.call('TYPE', 'juicefarm:watch:v2:custody:jobs').ok
          if (statusType ~= 'hash' and statusType ~= 'none') or
             (processingType ~= 'list' and processingType ~= 'none') or
             (processingIndexType ~= 'set' and processingIndexType ~= 'none') or
             (leaseIndexType ~= 'zset' and leaseIndexType ~= 'none') or
             (custodyIndexType ~= 'set' and custodyIndexType ~= 'none') then
            return redis.error_reply('invalid tracked claim key type')
          end
          local statusKey = 'juicefarm:job:' .. job.id
          local phase = redis.call('HGET', statusKey, 'claim_phase')
          local retiredPlannerLease = phase == 'running' and
            redis.call('HGET', statusKey, 'status') == 'queued' and
            not redis.call('HGET', statusKey, 'claim_token') and
            not redis.call('HGET', statusKey, 'lease_owner') and
            not redis.call('HGET', statusKey, 'lease_expires_unix')
          if phase == 'transfer' or phase == 'reserved' or
             (phase == 'running' and not retiredPlannerLease) then
            eligible = false
          elseif phase == 'reclaiming' then
            local owner = redis.call('HGET', statusKey, 'lease_owner')
            if not owner or owner == '' then
              eligible = false
            else
              local oldProcessing = 'juicefarm:watch:v2:processing:' .. owner
              local oldType = redis.call('TYPE', oldProcessing).ok
              if oldType ~= 'list' and oldType ~= 'none' then
                return redis.error_reply('invalid reclaiming tracked processing key type')
              end
              if redis.call('LPOS', oldProcessing, raw) then
                eligible = false
              else
                reclaimedOwner = owner
              end
            end
          end
        end
        if eligible and staleRuntimePin and job then
          local requirements = {}
          for _, required in ipairs(job.required_capabilities or {}) do
            local pinned = string.match(required, '^worker:(.+)$')
            if pinned and not caps[required] and
                redis.call('EXISTS', 'juicefarm:worker:' .. pinned) == 0 then
              if workerPin ~= '' then table.insert(requirements, workerPin) end
            else
              table.insert(requirements, required)
            end
          end
          job.required_capabilities = requirements
          job.selected_worker = ARGV[5]
          raw = cjson.encode(job)
          if job.id and not job.tracked_epoch then
            redis.call('HSET', 'juicefarm:job:' .. job.id,
              'target_worker', workerDisplay, 'payload', raw)
          end
        end
        if eligible then
          if job and job.tracked_epoch then
            -- SADD can fail before ownership moves; that leaves the raw ready.
            -- Record the new owner and generation before the nonrollbackable
            -- move. Recovery can resolve either side of an interrupted move.
            local statusKey = 'juicefarm:job:' .. job.id
            local generation = tonumber(redis.call('HGET', statusKey, 'claim_generation') or '0')
            if not generation or generation < 0 or generation >= 9007199254740991 or
               math.floor(generation) ~= generation then
              return redis.error_reply('invalid tracked claim generation')
            end
            generation = generation + 1
            local custodyRaw = raw
            if redis.call('TYPE', KEYS[i]).ok ~= 'list' then
              return redis.error_reply('invalid tracked ready key type')
            end
            redis.call('SADD', 'juicefarm:watch:v2:custody:jobs', job.id)
            redis.call('SADD', processingIndex, ARGV[1])
            redis.call('HSET', statusKey, 'claim_phase', 'transfer',
              'claim_generation', generation, 'claim_token', ARGV[7],
              'lease_owner', ARGV[1], 'custody_source', KEYS[i],
              'custody_raw', sourceRaw, 'custody_processing_raw', custodyRaw)
            local moved = redis.call('LMOVE', KEYS[i], processing, 'RIGHT', 'LEFT')
            if not moved then return redis.error_reply('tracked ready claim disappeared') end
            if moved ~= sourceRaw then return redis.error_reply('tracked ready claim changed') end
            if raw ~= sourceRaw then
              redis.call('LSET', processing, 0, raw)
              redis.call('HSET', statusKey, 'target_worker', workerDisplay,
                'payload', raw)
            end
          else
            redis.call('RPOP', KEYS[i])
            redis.call('LPUSH', processing, raw)
            redis.call('SADD', processingIndex, ARGV[1])
            if job and job.id then
              redis.call('HSET', 'juicefarm:job:' .. job.id, 'delivery_token', ARGV[7])
            end
          end
        end
        local leaseExpires = false
        if eligible and job and job.tracked_epoch then
          local leaseNow = redis.call('TIME')
          leaseExpires = tonumber(leaseNow[1]) + 45
          if not job.parent_id then
            redis.call('HSET', 'juicefarm:job:' .. job.id,
              'status', 'running', 'claim_token', ARGV[7], 'claim_phase', 'running',
              'lease_owner', ARGV[1], 'lease_expires_at', ARGV[8],
              'lease_expires_unix', leaseExpires)
          else
            redis.call('HSET', 'juicefarm:job:' .. job.id,
              'claim_token', ARGV[7], 'claim_phase', 'reserved',
              'lease_owner', ARGV[1], 'lease_expires_at', ARGV[8],
              'lease_expires_unix', leaseExpires)
          end
          redis.call('ZADD', KEYS[7], leaseExpires, job.id)
          redis.call('HDEL', 'juicefarm:job:' .. job.id,
            'custody_source', 'custody_raw', 'custody_processing_raw')
          redis.call('SREM', 'juicefarm:watch:v2:custody:jobs', job.id)
        end
        if eligible then
          if reclaimedOwner and reclaimedOwner ~= ARGV[1] then
            local oldProcessing = 'juicefarm:watch:v2:processing:' .. reclaimedOwner
            if redis.call('LLEN', oldProcessing) == 0 then
              redis.call('SREM', KEYS[5], reclaimedOwner)
            end
          end
          if job and job.tracked_epoch then
            return {raw, KEYS[i], processing, redis.call('HGET', 'juicefarm:job:' .. job.id, 'claim_generation'), tostring(leaseExpires)}
          end
          return {raw, KEYS[i], processing}
        end
      end
      -- Rotate skipped candidates without ever making a tracked raw absent
      -- from both its ready lane and a recoverable processing lane.
      redis.call('RPOPLPUSH', KEYS[i], KEYS[i])
    end
  end
end
return {}`

// ClaimForWorker polls the worker's eligible lanes and atomically moves one raw
// job into its processing list. Unlike BRPOP, a worker crash cannot lose the
// entry: RecoverAbandonedWorkers can see and requeue the durable receipt.
func (c *Client) ClaimForWorker(ctx context.Context, timeout time.Duration, w Worker) (Claim, bool, error) {
	if w.AdmissionBlocked {
		return Claim{}, false, nil
	}
	keys := WorkerQueueKeys(w)
	if len(keys) == 0 {
		return Claim{}, false, nil
	}
	deadline := time.Now().Add(timeout)
	processingKey := ProcessingPrefix + w.ID
	trackedProcessingKey := TrackedProcessingPrefix + w.ID
	capabilities, _ := json.Marshal(workerCapabilitySet(w))
	decodeLimits, _ := json.Marshal(w.DecodeLimits)
	if string(decodeLimits) == "null" {
		decodeLimits = []byte("{}")
	}
	scriptKeys := append([]string{ControlKey, StoragePermitKey, ProcessingIndexKey, processingKey,
		TrackedProcessingIndexKey, trackedProcessingKey, LeaseIndexKey}, keys...)
	requireStoragePermit := "0"
	if w.RequireStoragePermit {
		requireStoragePermit = "1"
	}
	for {
		token := NewID()
		requestStart := time.Now()
		expires := time.Now().Add(claimLeaseTTL)
		res, err := c.rdb.Eval(ctx, claimForWorkerScript, scriptKeys, w.ID, string(capabilities), string(decodeLimits),
			workerPinCapability(w), workerDisplayName(w), requireStoragePermit,
			token, expires.UTC().Format(time.RFC3339), expires.Unix(), w.Role).StringSlice()
		if err != nil && err != redis.Nil {
			return Claim{}, false, err
		}
		if len(res) >= 3 {
			var job Job
			if err := json.Unmarshal([]byte(res[0]), &job); err != nil {
				_ = c.rdb.LRem(ctx, res[2], 1, res[0]).Err()
				return Claim{}, false, fmt.Errorf("decode claimed job: %w", err)
			}
			var generation int64
			if len(res) >= 4 {
				generation, err = strconv.ParseInt(res[3], 10, 64)
				if err != nil {
					return Claim{}, false, fmt.Errorf("decode claim generation: %w", err)
				}
			}
			claim := Claim{Job: job, Raw: res[0], QueueKey: res[1], WorkerID: w.ID, Token: token, Generation: generation}
			if len(res) >= 5 {
				_, parseErr := strconv.ParseInt(res[4], 10, 64)
				if parseErr != nil {
					return Claim{}, false, fmt.Errorf("decode claim lease: %w", parseErr)
				}
				// The worker's clock may differ from Redis. The request start is a
				// conservative local bound on when Redis could have granted the lease.
				claim.LeaseExpiresAt = requestStart.Add(claimLeaseTTL - 2*time.Second)
			}
			return claim, true, nil
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
	activeWorkerIDs := make(map[string]bool, len(workers))
	for _, worker := range workers {
		activeWorkerIDs[worker.ID] = true
	}
	recovered := 0
	for _, tracked := range []bool{false, true} {
		for _, kind := range []string{KindDerivatives, KindProxy, KindTranscript} {
			key := maintenanceQueueKey(kind, QueueClassRender, tracked)
			// Claims pop from the right. Inspect only the jobs that can block the next
			// claim, rather than rewriting an entire multi-thousand-item backlog when
			// one container restarts with a new legacy runtime ID.
			raws, err := c.rdb.LRange(ctx, key, -maxReadyRecoveryInspectionsPerLane, -1).Result()
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
					if worker.Role == QueueClassRender &&
						workerCanClaimOrAdoptStaleRuntimePin(worker, job, activeWorkerIDs) {
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
					job.RequiredCapabilities = []string{"transcript:" + backend, workerPinCapability(selected)}
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
if redis.call('HEXISTS', KEYS[3], 'held_token') == 1 then return 0 end
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 1 then
  redis.call('LPUSH', KEYS[2], ARGV[2])
  redis.call('HSET', KEYS[3], 'status', 'queued', 'backend', ARGV[3],
	'target_worker', ARGV[4], 'queue_class', ARGV[5], 'attempts', ARGV[6],
	'hardware_failures', ARGV[7], 'error', ARGV[8], 'payload', ARGV[2])
	redis.call('HDEL', KEYS[3], 'unserviceable_since')
  return 1
end
return 0`
				n, err := c.rdb.Eval(ctx, script, []string{
					key, maintenanceQueueKey(kind, targetClass, tracked), JobHashPrefix + job.ID,
				}, raw, string(nextRaw), job.SelectedBackend, job.SelectedWorker, targetClass,
					strconv.Itoa(job.Attempts), strconv.Itoa(job.HardwareFailures), reason).Int()
				if err != nil {
					return recovered, err
				}
				recovered += n
			}
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
	for _, tracked := range []bool{false, true} {
		for _, kind := range []string{KindDerivatives, KindProxy, KindTranscript} {
			sourceKey := maintenanceQueueKey(kind, QueueClassCPU, tracked)
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
if redis.call('HEXISTS', KEYS[3], 'held_token') == 1 then return 0 end
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 1 then
  redis.call('LPUSH', KEYS[2], ARGV[2])
  redis.call('HSET', KEYS[3], 'status', 'queued', 'worker', '',
	'backend', ARGV[3], 'target_worker', ARGV[4], 'queue_class', ARGV[5],
	'error', ARGV[6], 'payload', ARGV[2])
  redis.call('HDEL', KEYS[3], 'lease_owner', 'lease_expires_at', 'finished_at')
  return 1
end
return 0`
				n, err := c.rdb.Eval(ctx, script, []string{
					sourceKey, maintenanceQueueKey(kind, QueueClassRender, tracked), statusKey,
				}, raw, string(nextRaw), candidate.SelectedBackend, candidate.SelectedWorker,
					candidate.QueueClass, promotionReason).Int()
				if err != nil {
					return promoted, err
				}
				if n == 1 {
					promoted++
					for _, capability := range candidate.RequiredCapabilities {
						if id := workerIDForPin(workers, capability); id != "" {
							loads[id]++
							break
						}
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
	_, err := c.MarkClaimRunningWithDeadline(ctx, claim, worker)
	return err
}

// MarkClaimRunningWithDeadline returns the last Redis-confirmed lease deadline.
func (c *Client) MarkClaimRunningWithDeadline(ctx context.Context, claim Claim, worker Worker) (time.Time, error) {
	if claim.Token == "" || claim.WorkerID != worker.ID {
		return time.Time{}, fmt.Errorf("%w: invalid delivery owner", ErrClaimLost)
	}
	requestStart := time.Now()
	expires := time.Now().Add(claimLeaseTTL)
	if claim.Job.TrackedEpoch != "" && claim.Job.ParentID != "" {
		const script = `
if redis.call('TYPE', KEYS[1]).ok ~= 'hash' or
   redis.call('TYPE', KEYS[2]).ok ~= 'list' or
   (redis.call('TYPE', KEYS[3]).ok ~= 'zset' and redis.call('TYPE', KEYS[3]).ok ~= 'none') then
  return redis.error_reply('invalid tracked claim key type') end
local now = tonumber(redis.call('TIME')[1])
if redis.call('HGET', KEYS[1], 'claim_token') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'lease_owner') ~= ARGV[2] or
   redis.call('HGET', KEYS[1], 'claim_generation') ~= ARGV[10] or
   redis.call('HGET', KEYS[1], 'claim_phase') ~= 'reserved' or
   redis.call('HGET', KEYS[1], 'status') ~= 'queued' or
   tonumber(redis.call('HGET', KEYS[1], 'lease_expires_unix') or '0') <= now then
  return 0
end
if not redis.call('LPOS', KEYS[2], ARGV[3]) then return 0 end
local deadline = now + 45
redis.call('HSET', KEYS[1], 'status', 'running', 'claim_phase', 'running',
  'started_at', ARGV[4], 'worker', ARGV[5], 'lease_expires_at', ARGV[6],
  'lease_expires_unix', deadline)
redis.call('ZADD', KEYS[3], deadline, ARGV[9])
return deadline`
		n, err := c.rdb.Eval(ctx, script, []string{JobHashPrefix + claim.Job.ID,
			TrackedProcessingPrefix + worker.ID, LeaseIndexKey}, claim.Token, worker.ID, claim.Raw,
			nowISO(), worker.Name, expires.UTC().Format(time.RFC3339),
			time.Now().Unix(), expires.Unix(), claim.Job.ID, claim.Generation).Int()
		if err != nil {
			return time.Time{}, err
		}
		if n == 0 {
			return time.Time{}, fmt.Errorf("%w: tracked child %s lost its reserved claim", ErrClaimLost, claim.Job.ID)
		}
		return requestStart.Add(claimLeaseTTL - 2*time.Second), nil
	}
	if claim.Job.TrackedEpoch != "" && claim.Job.ParentID == "" {
		const script = `
local statusType = redis.call('TYPE', KEYS[1]).ok
local leaseType = redis.call('TYPE', KEYS[2]).ok
local processingType = redis.call('TYPE', KEYS[3]).ok
if statusType ~= 'hash' or (leaseType ~= 'zset' and leaseType ~= 'none') or
   processingType ~= 'list' then return redis.error_reply('invalid tracked claim key type') end
local now = tonumber(redis.call('TIME')[1])
if redis.call('HGET', KEYS[1], 'claim_token') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'lease_owner') ~= ARGV[7] or
   redis.call('HGET', KEYS[1], 'claim_generation') ~= ARGV[8] or
   redis.call('HGET', KEYS[1], 'status') ~= 'running' or
   redis.call('HGET', KEYS[1], 'claim_phase') ~= 'running' or
   tonumber(redis.call('HGET', KEYS[1], 'lease_expires_unix') or '0') <= now or
   not redis.call('LPOS', KEYS[3], ARGV[10]) then return 0 end
local deadline = now + 45
redis.call('HSET', KEYS[1], 'started_at', ARGV[2], 'worker', ARGV[3],
  'lease_expires_at', ARGV[4], 'lease_expires_unix', deadline)
redis.call('ZADD', KEYS[2], deadline, ARGV[6])
return deadline`
		n, err := c.rdb.Eval(ctx, script, []string{JobHashPrefix + claim.Job.ID, LeaseIndexKey,
			TrackedProcessingPrefix + worker.ID},
			claim.Token, nowISO(), worker.Name, expires.UTC().Format(time.RFC3339),
			expires.Unix(), claim.Job.ID, worker.ID, claim.Generation, time.Now().Unix(), claim.Raw).Int()
		if err != nil {
			return time.Time{}, err
		}
		if n == 0 {
			return time.Time{}, fmt.Errorf("%w: tracked planner %s lost its claim", ErrClaimLost, claim.Job.ID)
		}
		return requestStart.Add(claimLeaseTTL - 2*time.Second), nil
	}
	const script = `
if redis.call('HEXISTS', KEYS[1], 'held_token') == 1 or
   redis.call('HGET', KEYS[1], 'delivery_token') ~= ARGV[1] or
   not redis.call('LPOS', KEYS[2], ARGV[2]) then return 0 end
local deadline = tonumber(redis.call('TIME')[1]) + 45
redis.call('HSET', KEYS[1], 'status', 'running', 'started_at', ARGV[3], 'worker', ARGV[4],
 'backend', ARGV[5], 'queue_class', ARGV[6], 'attempts', ARGV[7], 'hardware_failures', ARGV[8],
 'lease_owner', ARGV[9], 'lease_expires_at', ARGV[10], 'delivery_expires_unix', deadline)
redis.call('ZADD', KEYS[3], deadline, ARGV[11])
return deadline`
	n, err := c.rdb.Eval(ctx, script, []string{JobHashPrefix + claim.Job.ID, claimProcessingKey(claim), LeaseIndexKey},
		claim.Token, claim.Raw, nowISO(), worker.Name, claim.Job.SelectedBackend, claim.Job.QueueClass,
		claim.Job.Attempts, claim.Job.HardwareFailures, worker.ID, expires.UTC().Format(time.RFC3339), claim.Job.ID).Int()
	if err != nil {
		return time.Time{}, err
	}
	if n == 0 {
		return time.Time{}, fmt.Errorf("%w: legacy job %s cannot start", ErrClaimLost, claim.Job.ID)
	}
	return requestStart.Add(claimLeaseTTL - 2*time.Second), nil
}

func (c *Client) RenewClaim(ctx context.Context, claim Claim) error {
	_, err := c.RenewClaimWithDeadline(ctx, claim)
	return err
}

// RenewClaimWithDeadline returns the lease deadline confirmed by Redis.
func (c *Client) RenewClaimWithDeadline(ctx context.Context, claim Claim) (time.Time, error) {
	requestStart := time.Now()
	expires := time.Now().Add(claimLeaseTTL)
	if claim.Job.TrackedEpoch != "" && claim.Job.ParentID != "" {
		const script = `
local statusType = redis.call('TYPE', KEYS[1]).ok
local leaseType = redis.call('TYPE', KEYS[2]).ok
local processingType = redis.call('TYPE', KEYS[3]).ok
if statusType ~= 'hash' or (leaseType ~= 'zset' and leaseType ~= 'none') or
   (processingType ~= 'list' and processingType ~= 'none') then
  return redis.error_reply('invalid tracked claim key type') end
local now = tonumber(redis.call('TIME')[1])
if redis.call('HGET', KEYS[1], 'claim_token') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'lease_owner') ~= ARGV[2] or
   redis.call('HGET', KEYS[1], 'claim_generation') ~= ARGV[6] or
   redis.call('HGET', KEYS[1], 'claim_phase') ~= 'running' or
   redis.call('HGET', KEYS[1], 'status') ~= 'running' or
   tonumber(redis.call('HGET', KEYS[1], 'lease_expires_unix') or '0') <= now or
   not redis.call('LPOS', KEYS[3], ARGV[8]) then return 0 end
local deadline = now + 45
redis.call('HSET', KEYS[1], 'lease_expires_at', ARGV[3], 'lease_expires_unix', deadline)
redis.call('ZADD', KEYS[2], deadline, ARGV[5])
return deadline`
		n, err := c.rdb.Eval(ctx, script, []string{JobHashPrefix + claim.Job.ID, LeaseIndexKey,
			TrackedProcessingPrefix + claim.WorkerID},
			claim.Token, claim.WorkerID, expires.UTC().Format(time.RFC3339), expires.Unix(),
			claim.Job.ID, claim.Generation, time.Now().Unix(), claim.Raw).Int()
		if err != nil {
			return time.Time{}, err
		}
		if n == 0 {
			return time.Time{}, fmt.Errorf("%w: tracked child %s lost its running claim", ErrClaimLost, claim.Job.ID)
		}
		return requestStart.Add(claimLeaseTTL - 2*time.Second), nil
	}
	if claim.Job.TrackedEpoch != "" && claim.Job.ParentID == "" {
		const script = `
local statusType = redis.call('TYPE', KEYS[1]).ok
local leaseType = redis.call('TYPE', KEYS[2]).ok
local processingType = redis.call('TYPE', KEYS[3]).ok
if statusType ~= 'hash' or (leaseType ~= 'zset' and leaseType ~= 'none') or
   (processingType ~= 'list' and processingType ~= 'none') then
  return redis.error_reply('invalid tracked claim key type') end
local now = tonumber(redis.call('TIME')[1])
if redis.call('HGET', KEYS[1], 'claim_token') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'lease_owner') ~= ARGV[5] or
   redis.call('HGET', KEYS[1], 'claim_generation') ~= ARGV[6] or
   redis.call('HGET', KEYS[1], 'status') ~= 'running' or
   redis.call('HGET', KEYS[1], 'claim_phase') ~= 'running' or
   tonumber(redis.call('HGET', KEYS[1], 'lease_expires_unix') or '0') <= now or
   not redis.call('LPOS', KEYS[3], ARGV[8]) then return 0 end
local deadline = now + 45
redis.call('HSET', KEYS[1], 'lease_expires_at', ARGV[2], 'lease_expires_unix', deadline)
redis.call('ZADD', KEYS[2], deadline, ARGV[4])
return deadline`
		n, err := c.rdb.Eval(ctx, script, []string{JobHashPrefix + claim.Job.ID, LeaseIndexKey,
			TrackedProcessingPrefix + claim.WorkerID},
			claim.Token, expires.UTC().Format(time.RFC3339), expires.Unix(), claim.Job.ID,
			claim.WorkerID, claim.Generation, time.Now().Unix(), claim.Raw).Int()
		if err != nil {
			return time.Time{}, err
		}
		if n == 0 {
			return time.Time{}, fmt.Errorf("%w: tracked planner %s lost its claim", ErrClaimLost, claim.Job.ID)
		}
		return requestStart.Add(claimLeaseTTL - 2*time.Second), nil
	}
	const script = `
if redis.call('HEXISTS', KEYS[1], 'held_token') == 1 or
   redis.call('HGET', KEYS[1], 'delivery_token') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'lease_owner') ~= ARGV[2] or
   redis.call('HGET', KEYS[1], 'status') ~= 'running' or
   not redis.call('LPOS', KEYS[2], ARGV[3]) then return 0 end
local deadline = tonumber(redis.call('TIME')[1]) + 45
redis.call('HSET', KEYS[1], 'lease_expires_at', ARGV[4], 'delivery_expires_unix', deadline)
redis.call('ZADD', KEYS[3], deadline, ARGV[5])
return deadline`
	n, err := c.rdb.Eval(ctx, script, []string{JobHashPrefix + claim.Job.ID, claimProcessingKey(claim), LeaseIndexKey},
		claim.Token, claim.WorkerID, claim.Raw, expires.UTC().Format(time.RFC3339), claim.Job.ID).Int()
	if err != nil {
		return time.Time{}, err
	}
	if n == 0 {
		return time.Time{}, fmt.Errorf("%w: legacy job %s cannot renew", ErrClaimLost, claim.Job.ID)
	}
	return requestStart.Add(claimLeaseTTL - 2*time.Second), nil
}

// CompleteClaim durably records one tracked execution result without releasing
// its receipt. AckClaim may be retried after a lost response using that result.
func (c *Client) CompleteClaim(ctx context.Context, claim Claim, status string, processed, failed int, errMsg string) error {
	if status != StatusDone && status != StatusFailed && status != StatusCanceled {
		return fmt.Errorf("unsupported terminal claim status %q", status)
	}
	if status == StatusDone && failed > 0 && processed == 0 {
		status = StatusFailed
	}
	if claim.Job.TrackedEpoch == "" {
		switch status {
		case StatusDone:
			return c.MarkDone(ctx, claim.Job.ID, processed, failed)
		case StatusCanceled:
			return c.MarkCanceled(ctx, claim.Job.ID, processed, failed)
		default:
			return c.MarkFailedWithCounts(ctx, claim.Job.ID, processed, failed, errMsg)
		}
	}
	if claim.Job.ParentID == "" {
		return fmt.Errorf("tracked planner %s requires coverage publication", claim.Job.ID)
	}
	const script = `
local statusType = redis.call('TYPE', KEYS[1]).ok
local processingType = redis.call('TYPE', KEYS[2]).ok
if statusType ~= 'hash' or processingType ~= 'list' then
  return redis.error_reply('invalid tracked completion key type') end
local status = redis.call('HGET', KEYS[1], 'status')
if (status == 'done' or status == 'failed' or status == 'canceled') and
   redis.call('HGET', KEYS[1], 'terminal_token') == ARGV[1] and
   redis.call('HGET', KEYS[1], 'terminal_generation') == ARGV[3] and
   redis.call('HGET', KEYS[1], 'terminal_owner') == ARGV[2] then
  return 1
end
local now = tonumber(redis.call('TIME')[1])
if status ~= 'running' or redis.call('HGET', KEYS[1], 'claim_phase') ~= 'running' or
   redis.call('HGET', KEYS[1], 'claim_token') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'lease_owner') ~= ARGV[2] or
   redis.call('HGET', KEYS[1], 'claim_generation') ~= ARGV[3] or
   tonumber(redis.call('HGET', KEYS[1], 'lease_expires_unix') or '0') <= now or
   not redis.call('LPOS', KEYS[2], ARGV[4]) then return 0 end
redis.call('HSET', KEYS[1], 'status', ARGV[5], 'finished_at', ARGV[6],
  'processed', ARGV[7], 'failed', ARGV[8], 'error', ARGV[9],
  'terminal_token', ARGV[1], 'terminal_generation', ARGV[3], 'terminal_owner', ARGV[2])
redis.call('PERSIST', KEYS[1])
return 1`
	n, err := c.rdb.Eval(ctx, script, []string{JobHashPrefix + claim.Job.ID, claimProcessingKey(claim)},
		claim.Token, claim.WorkerID, claim.Generation, claim.Raw, status, nowISO(), processed, failed, errMsg).Int()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: tracked child %s cannot complete", ErrClaimLost, claim.Job.ID)
	}
	return c.retireTrackedTerminal(ctx, claim.Job.ID)
}

func (c *Client) AckClaim(ctx context.Context, claim Claim) error {
	processingKey := claimProcessingKey(claim)
	if claim.Job.TrackedEpoch != "" {
		const script = `
local statusType = redis.call('TYPE', KEYS[3]).ok
local processingType = redis.call('TYPE', KEYS[1]).ok
local leaseType = redis.call('TYPE', KEYS[2]).ok
local indexType = redis.call('TYPE', KEYS[4]).ok
local custodyType = redis.call('TYPE', KEYS[5]).ok
local epochType = redis.call('TYPE', KEYS[6]).ok
local retireType = redis.call('TYPE', KEYS[7]).ok
local ledgerType = redis.call('TYPE', KEYS[8]).ok
local membersType = redis.call('TYPE', KEYS[9]).ok
if statusType ~= 'hash' or (processingType ~= 'list' and processingType ~= 'none') or
   (leaseType ~= 'zset' and leaseType ~= 'none') or
   (indexType ~= 'set' and indexType ~= 'none') or
   (custodyType ~= 'set' and custodyType ~= 'none') or
   (ARGV[7] == '1' and epochType ~= 'hash') or
   (ARGV[7] == '0' and epochType ~= 'hash' and epochType ~= 'none') or
   (retireType ~= 'zset' and retireType ~= 'none') or
   (ledgerType ~= 'hash' and ledgerType ~= 'none') or
   (membersType ~= 'zset' and membersType ~= 'none') then
  return redis.error_reply('invalid tracked acknowledgement key type') end
local protocol = redis.call('HGET', KEYS[3], 'coverage_protocol')
local status = redis.call('HGET', KEYS[3], 'status')
local alreadyAcked = redis.call('HGET', KEYS[3], 'ack_token') == ARGV[4] and
   redis.call('HGET', KEYS[3], 'ack_owner') == ARGV[3] and
   redis.call('HGET', KEYS[3], 'ack_generation') == ARGV[5] and
   (status == 'dispatched' or status == 'done' or status == 'failed' or status == 'canceled')
if protocol == '1' then
  local openCount = tonumber(redis.call('HGET', KEYS[8], 'open_count'))
  local openMembers = redis.call('ZCOUNT', KEYS[9], 0, 0)
  if ledgerType ~= 'hash' or
     redis.call('HGET', KEYS[8], 'parent_id') ~= ARGV[8] or
     redis.call('HGET', KEYS[8], 'epoch') ~= ARGV[9] or
     redis.call('HGET', KEYS[8], 'root') ~= ARGV[10] or
     not openCount then
    return redis.error_reply('tracked acknowledgement lacks an open coverage ledger') end
  local state = redis.call('HGET', KEYS[8], 'state')
  if state ~= 'open' then
    if (state == 'draining' or state == 'drained') and alreadyAcked then return 1 end
    return redis.error_reply('tracked acknowledgement coverage is no longer open')
  end
  if ARGV[7] == '0' then
    local member = redis.call('ZSCORE', KEYS[9], ARGV[2])
    if member ~= '0' and member ~= '1' then
      return redis.error_reply('tracked acknowledgement lacks coverage membership') end
    local marker = redis.call('HGET', KEYS[8], 'settling:' .. ARGV[2])
    if openCount > openMembers and not (alreadyAcked and member == '1' and
       openCount == openMembers + 1 and marker == 'pending:' .. ARGV[4]) then
      return redis.error_reply('tracked acknowledgement has unexplained coverage overcount') end
    if member == '0' and marker and marker ~= 'pending:' .. ARGV[4] then
      return redis.error_reply('tracked acknowledgement settlement marker conflicts') end
  elseif openCount > openMembers then
    return redis.error_reply('tracked planner has unexplained coverage overcount')
  end
elseif ledgerType ~= 'none' or membersType ~= 'none' then
  return redis.error_reply('tracked acknowledgement has an unversioned ledger')
end
local function settleCoverage()
  redis.call('PERSIST', KEYS[3])
  if protocol ~= '1' then
    if ARGV[7] == '0' then
      redis.call('ZADD', KEYS[7], tonumber(redis.call('TIME')[1]) + 3600, ARGV[2])
    end
    return
  end
  if ARGV[7] == '1' then
    redis.call('HSET', KEYS[8], 'planner_ack', '1')
  elseif redis.call('ZSCORE', KEYS[9], ARGV[2]) == '0' then
    redis.call('ZADD', KEYS[7], tonumber(redis.call('TIME')[1]), ARGV[2])
    redis.call('HSET', KEYS[8], 'settling:' .. ARGV[2], 'pending:' .. ARGV[4])
    redis.call('ZADD', KEYS[9], 1, ARGV[2])
  end
  if ARGV[7] == '0' then
    redis.call('HSET', KEYS[8], 'open_count', redis.call('ZCOUNT', KEYS[9], 0, 0),
      'settling:' .. ARGV[2], 'done:' .. ARGV[4])
  else
    redis.call('HSET', KEYS[8], 'open_count', redis.call('ZCOUNT', KEYS[9], 0, 0))
  end
  redis.call('ZADD', KEYS[7], tonumber(redis.call('TIME')[1]), 'coverage:' .. ARGV[8])
end
if ARGV[7] == '1' and not redis.call('HGET', KEYS[6], 'receipt:' .. ARGV[2]) then return 0 end
if redis.call('HGET', KEYS[3], 'claim_generation') ~= ARGV[5] then return 0 end
if alreadyAcked then
  settleCoverage()
  return 1
end
if redis.call('HGET', KEYS[3], 'claim_token') ~= ARGV[4] or
   redis.call('HGET', KEYS[3], 'lease_owner') ~= ARGV[3] then return 0 end
if ARGV[7] == '1' then
  if status == 'running' and redis.call('HGET', KEYS[6], 'receipt:' .. ARGV[2]) then
    redis.call('HSET', KEYS[3], 'status', 'dispatched')
    status = 'dispatched'
  end
  if status ~= 'dispatched' then return 0 end
else
  if (status ~= 'done' and status ~= 'failed' and status ~= 'canceled') or
     redis.call('HGET', KEYS[3], 'terminal_token') ~= ARGV[4] or
     redis.call('HGET', KEYS[3], 'terminal_generation') ~= ARGV[5] or
     redis.call('HGET', KEYS[3], 'terminal_owner') ~= ARGV[3] then return 0 end
end
if redis.call('LREM', KEYS[1], 1, ARGV[1]) ~= 1 then return 0 end
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('HSET', KEYS[3], 'ack_token', ARGV[4], 'ack_owner', ARGV[3], 'ack_generation', ARGV[5])
redis.call('HDEL', KEYS[3], 'lease_owner', 'lease_expires_at', 'lease_expires_unix', 'claim_token', 'claim_phase', 'delivery_token', 'delivery_expires_unix')
redis.call('SREM', KEYS[5], ARGV[2])
if redis.call('LLEN', KEYS[1]) == 0 then redis.call('SREM', KEYS[4], ARGV[3]) end
settleCoverage()
return 1`
		plannerFlag := "0"
		if claim.Job.ParentID == "" {
			plannerFlag = "1"
		}
		coverageParent := claim.Job.CoverageParentID
		if coverageParent == "" {
			coverageParent = claim.Job.ParentID
		}
		if plannerFlag == "1" {
			coverageParent = claim.Job.ID
		}
		n, err := c.rdb.Eval(ctx, script, []string{
			processingKey, LeaseIndexKey, JobHashPrefix + claim.Job.ID, claimProcessingIndex(claim),
			trackedCustodyIndexKey, watchEpochKey(claim.Job.Path), watchRetireKey,
			coverageLedgerKey(coverageParent), coverageMembersKey(coverageParent),
		}, claim.Raw, claim.Job.ID, claim.WorkerID, claim.Token, claim.Generation,
			int64(JobTTL/time.Second), plannerFlag, coverageParent,
			claim.Job.TrackedEpoch, claim.Job.Path).Int()
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("%w: tracked job %s cannot be acknowledged without its completed claim", ErrClaimLost, claim.Job.ID)
		}
		return nil
	}
	const script = `
if redis.call('HEXISTS', KEYS[3], 'held_token') == 1 then return 1 end
redis.call('LREM', KEYS[1], 1, ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('HDEL', KEYS[3], 'lease_owner', 'lease_expires_at', 'lease_expires_unix',
  'claim_token', 'claim_phase', 'delivery_token', 'delivery_expires_unix')
if redis.call('LLEN', KEYS[1]) == 0 then
  redis.call('SREM', KEYS[4], ARGV[3])
end
return 1`
	return c.rdb.Eval(ctx, script, []string{
		processingKey, LeaseIndexKey, JobHashPrefix + claim.Job.ID, claimProcessingIndex(claim),
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
				if !IsWorkerPinCapability(capability) {
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
	processingKey := claimProcessingKey(claim)
	if claim.Job.TrackedEpoch != "" {
		const script = `
local sourceType = redis.call('TYPE', KEYS[1]).ok
local targetType = redis.call('TYPE', KEYS[2]).ok
local leaseType = redis.call('TYPE', KEYS[3]).ok
local statusType = redis.call('TYPE', KEYS[4]).ok
local indexType = redis.call('TYPE', KEYS[5]).ok
local custodyType = redis.call('TYPE', KEYS[6]).ok
if sourceType ~= 'list' or (targetType ~= 'list' and targetType ~= 'none') or
   (leaseType ~= 'zset' and leaseType ~= 'none') or statusType ~= 'hash' or
   (indexType ~= 'set' and indexType ~= 'none') or
   (custodyType ~= 'set' and custodyType ~= 'none') then
  return redis.error_reply('invalid tracked requeue key type') end
local now = tonumber(redis.call('TIME')[1])
local status = redis.call('HGET', KEYS[4], 'status')
local phase = redis.call('HGET', KEYS[4], 'claim_phase')
if (status ~= 'running' and status ~= 'queued') or
   (phase ~= 'running' and phase ~= 'reserved') or
   redis.call('HGET', KEYS[4], 'claim_token') ~= ARGV[13] or
   redis.call('HGET', KEYS[4], 'lease_owner') ~= ARGV[10] or
   redis.call('HGET', KEYS[4], 'claim_generation') ~= ARGV[14] or
   tonumber(redis.call('HGET', KEYS[4], 'lease_expires_unix') or '0') <= now or
   not redis.call('LPOS', KEYS[1], ARGV[1]) then return 0 end
redis.call('LREM', KEYS[1], 1, ARGV[1])
redis.call('LPUSH', KEYS[2], ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[3])
redis.call('HSET', KEYS[4],
  'status', 'queued', 'worker', '', 'backend', ARGV[4],
  'target_worker', ARGV[5], 'queue_class', ARGV[6], 'attempts', ARGV[7],
  'hardware_failures', ARGV[8], 'error', ARGV[9], 'processed', ARGV[11], 'failed', '0',
  'payload', ARGV[12])
redis.call('HDEL', KEYS[4], 'lease_owner', 'lease_expires_at', 'lease_expires_unix',
  'claim_token', 'claim_phase', 'finished_at', 'delivery_token', 'delivery_expires_unix')
redis.call('SREM', KEYS[6], ARGV[3])
if redis.call('LLEN', KEYS[1]) == 0 then redis.call('SREM', KEYS[5], ARGV[10]) end
return 1`
		n, err := c.rdb.Eval(ctx, script, []string{
			processingKey, target, LeaseIndexKey, JobHashPrefix + job.ID, claimProcessingIndex(claim), trackedCustodyIndexKey,
		}, claim.Raw, string(raw), job.ID, job.SelectedBackend, job.SelectedWorker, job.QueueClass,
			strconv.Itoa(job.Attempts), strconv.Itoa(job.HardwareFailures), reason, claim.WorkerID,
			strconv.Itoa(job.ProcessedOffset), string(raw), claim.Token, claim.Generation).Int()
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("%w: tracked job %s cannot requeue", ErrClaimLost, job.ID)
		}
		return nil
	}
	const script = `
if redis.call('HEXISTS', KEYS[4], 'held_token') == 1 then return 0 end
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then
  return 0
end
redis.call('LPUSH', KEYS[2], ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[3])
redis.call('HSET', KEYS[4],
  'status', 'queued', 'worker', '', 'backend', ARGV[4],
  'target_worker', ARGV[5], 'queue_class', ARGV[6], 'attempts', ARGV[7],
  'hardware_failures', ARGV[8], 'error', ARGV[9], 'processed', ARGV[11], 'failed', '0',
  'payload', ARGV[12])
redis.call('HDEL', KEYS[4], 'lease_owner', 'lease_expires_at', 'lease_expires_unix',
  'claim_token', 'claim_phase', 'finished_at', 'delivery_token', 'delivery_expires_unix')
if redis.call('LLEN', KEYS[1]) == 0 then
  redis.call('SREM', KEYS[5], ARGV[10])
end
return 1`
	n, err := c.rdb.Eval(ctx, script, []string{
		processingKey, target, LeaseIndexKey, JobHashPrefix + job.ID, claimProcessingIndex(claim),
	}, claim.Raw, string(raw), job.ID, job.SelectedBackend, job.SelectedWorker, job.QueueClass,
		strconv.Itoa(job.Attempts), strconv.Itoa(job.HardwareFailures), reason,
		claim.WorkerID, strconv.Itoa(job.ProcessedOffset), string(raw)).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: claim %s is no longer owned by worker %s", ErrClaimLost, job.ID, claim.WorkerID)
	}
	return nil
}

const recoverTrackedTransferScript = `
local statusType = redis.call('TYPE', KEYS[1]).ok
if statusType == 'none' then
  if redis.call('TYPE', KEYS[2]).ok ~= 'set' then
    return redis.error_reply('invalid tracked transfer index type') end
  redis.call('SREM', KEYS[2], ARGV[1])
  return 0
end
if statusType ~= 'hash' or
   (redis.call('TYPE', KEYS[2]).ok ~= 'set' and redis.call('TYPE', KEYS[2]).ok ~= 'none') or
   (redis.call('TYPE', KEYS[3]).ok ~= 'zset' and redis.call('TYPE', KEYS[3]).ok ~= 'none') or
   (redis.call('TYPE', KEYS[4]).ok ~= 'set' and redis.call('TYPE', KEYS[4]).ok ~= 'none') then
  return redis.error_reply('invalid tracked transfer index type')
end
if redis.call('HGET', KEYS[1], 'claim_phase') ~= 'transfer' then return 0 end
local owner = redis.call('HGET', KEYS[1], 'lease_owner')
local source = redis.call('HGET', KEYS[1], 'custody_source')
local sourceRaw = redis.call('HGET', KEYS[1], 'custody_raw')
local processingRaw = redis.call('HGET', KEYS[1], 'custody_processing_raw')
local generation = tonumber(redis.call('HGET', KEYS[1], 'claim_generation') or '')
if not owner or owner == '' or not source or not sourceRaw or not processingRaw or
   not generation or generation < 1 or math.floor(generation) ~= generation then
  return redis.error_reply('invalid tracked transfer metadata')
end
local processing = 'juicefarm:watch:v2:processing:' .. owner
if (redis.call('TYPE', source).ok ~= 'list' and redis.call('TYPE', source).ok ~= 'none') or
   (redis.call('TYPE', processing).ok ~= 'list' and redis.call('TYPE', processing).ok ~= 'none') then
  return redis.error_reply('invalid tracked transfer lane type')
end
local inReady = redis.call('LPOS', source, sourceRaw)
local actualProcessingRaw = processingRaw
local inProcessing = redis.call('LPOS', processing, actualProcessingRaw)
if not inProcessing and processingRaw ~= sourceRaw then
  actualProcessingRaw = sourceRaw
  inProcessing = redis.call('LPOS', processing, actualProcessingRaw)
end
if not inReady and not inProcessing then
  return redis.error_reply('tracked transfer has no exact custody raw')
end
if inProcessing then
  if not inReady then redis.call('LPUSH', source, sourceRaw) end
  if redis.call('LREM', processing, 1, actualProcessingRaw) ~= 1 then
    return redis.error_reply('tracked transfer processing receipt changed')
  end
end
redis.call('HSET', KEYS[1], 'status', 'queued')
redis.call('HDEL', KEYS[1], 'claim_phase', 'claim_token', 'lease_owner',
  'lease_expires_at', 'lease_expires_unix', 'custody_source', 'custody_raw',
  'custody_processing_raw')
redis.call('ZREM', KEYS[3], ARGV[1])
if redis.call('LLEN', processing) == 0 then redis.call('SREM', KEYS[4], owner) end
redis.call('SREM', KEYS[2], ARGV[1])
return 1`

func (c *Client) recoverTrackedTransfer(ctx context.Context, id string) (int, error) {
	return c.rdb.Eval(ctx, recoverTrackedTransferScript, []string{
		JobHashPrefix + id, trackedCustodyIndexKey, LeaseIndexKey, TrackedProcessingIndexKey,
	}, id).Int()
}

// RecoverAbandonedWorkers requeues every durable claim owned by a worker whose
// heartbeat expired. Routing is recomputed against current measured worker
// profiles: another verified render node wins, while CPU/H.264 is used only
// when no suitable accelerator remains online.
func (c *Client) RecoverAbandonedWorkers(ctx context.Context) (int, error) {
	total := 0
	// A transfer can fail before it has a processing receipt. Its job ID is
	// indexed before LMOVE, so resolve it independently of worker heartbeats.
	ids, err := c.rdb.SMembers(ctx, trackedCustodyIndexKey).Result()
	if err != nil {
		return 0, err
	}
	for _, id := range ids {
		n, err := c.recoverTrackedTransfer(ctx, id)
		if err != nil {
			return total, err
		}
		total += n
	}
	for _, group := range []struct {
		index, prefix string
		tracked       bool
	}{
		{ProcessingIndexKey, ProcessingPrefix, false},
		{TrackedProcessingIndexKey, TrackedProcessingPrefix, true},
	} {
		workers, err := c.rdb.SMembers(ctx, group.index).Result()
		if err != nil {
			return total, err
		}
		for _, workerID := range workers {
			alive, err := c.rdb.Exists(ctx, WorkerPrefix+workerID).Result()
			if err != nil {
				return total, err
			}
			if alive > 0 && !group.tracked {
				continue
			}
			lock := "juicefarm:reaper:" + group.prefix + workerID
			got, err := c.rdb.SetNX(ctx, lock, "1", 15*time.Second).Result()
			if err != nil || !got {
				continue
			}
			processingKey := group.prefix + workerID
			raws, listErr := c.rdb.LRange(ctx, processingKey, 0, -1).Result()
			if listErr != nil {
				_ = c.rdb.Del(ctx, lock).Err()
				return total, listErr
			}
			for _, raw := range raws {
				var job Job
				if err := json.Unmarshal([]byte(raw), &job); err != nil || job.ID == "" {
					if group.tracked {
						_ = c.rdb.Del(ctx, lock).Err()
						return total, fmt.Errorf("malformed tracked processing receipt for worker %s", workerID)
					}
					if err := c.requeueMalformedClaim(ctx, processingKey, workerID, raw); err != nil {
						_ = c.rdb.Del(ctx, lock).Err()
						return total, err
					}
					total++
					continue
				}
				if group.tracked && job.TrackedEpoch == "" {
					_ = c.rdb.Del(ctx, lock).Err()
					return total, fmt.Errorf("untracked job %s in isolated watch processing lane", job.ID)
				}
				held, heldErr := c.rdb.HGetAll(ctx, JobHashPrefix+job.ID).Result()
				if heldErr != nil {
					_ = c.rdb.Del(ctx, lock).Err()
					return total, heldErr
				}
				if held["held_token"] != "" {
					generation, parseErr := strconv.ParseInt(held["held_generation"], 10, 64)
					if parseErr != nil {
						_ = c.rdb.Del(ctx, lock).Err()
						return total, parseErr
					}
					heldClaim := Claim{Job: job, Raw: raw, WorkerID: workerID, Token: held["held_token"], Generation: generation}
					if holdErr := c.HoldClaim(ctx, heldClaim, held["error"]); holdErr != nil {
						_ = c.rdb.Del(ctx, lock).Err()
						return total, holdErr
					}
					total++
					continue
				}
				terminalTracked := false
				if group.tracked {
					fields, fieldsErr := c.rdb.HGetAll(ctx, JobHashPrefix+job.ID).Result()
					if fieldsErr != nil {
						_ = c.rdb.Del(ctx, lock).Err()
						return total, fieldsErr
					}
					phase := fields["claim_phase"]
					if phase == "transfer" || fields["lease_owner"] != "" && fields["lease_owner"] != workerID {
						continue
					}
					terminalTracked = fields["status"] == StatusDone || fields["status"] == StatusFailed ||
						fields["status"] == StatusCanceled || fields["status"] == StatusDispatched
					if !terminalTracked && (phase == "" || phase == "reclaiming") && fields["status"] == StatusQueued &&
						fields["claim_token"] == "" && fields["lease_expires_unix"] == "" &&
						fields["lease_expires_at"] == "" {
						requeued, reclaimErr := c.reclaimUnreservedTrackedChild(ctx, processingKey,
							QueueKeyFor(&job), job, raw, workerID)
						if reclaimErr != nil {
							_ = c.rdb.Del(ctx, lock).Err()
							return total, reclaimErr
						}
						total += requeued
						continue
					}
				}
				if group.tracked && job.ParentID != "" && !terminalTracked {
					if len(job.Kinds) != 1 || !isDedicatedKind(job.Kinds[0]) ||
						(job.QueueClass != QueueClassServer && job.QueueClass != QueueClassRender && job.QueueClass != QueueClassCPU) {
						_ = c.rdb.Del(ctx, lock).Err()
						return total, fmt.Errorf("invalid tracked execution child route for %s", job.ID)
					}
					phase, phaseErr := c.rdb.HGet(ctx, JobHashPrefix+job.ID, "claim_phase").Result()
					if phaseErr != nil && phaseErr != redis.Nil {
						_ = c.rdb.Del(ctx, lock).Err()
						return total, phaseErr
					}
					if phase != "reserved" && phase != "reclaiming" && phase != "running" {
						continue
					}
					token, tokenErr := c.rdb.HGet(ctx, JobHashPrefix+job.ID, "claim_token").Result()
					if tokenErr != nil {
						_ = c.rdb.Del(ctx, lock).Err()
						return total, tokenErr
					}
					requeued, reclaimErr := c.reclaimTrackedReservation(ctx, processingKey,
						QueueKeyFor(&job), job, raw, workerID, token)
					if reclaimErr != nil {
						_ = c.rdb.Del(ctx, lock).Err()
						return total, reclaimErr
					}
					total += requeued
					continue
				}
				if alive > 0 && !group.tracked {
					continue
				}
				if !group.tracked || !terminalTracked && job.ParentID == "" {
					expires, expiryErr := c.rdb.HGet(ctx, JobHashPrefix+job.ID, "lease_expires_unix").Int64()
					if expiryErr != nil && expiryErr != redis.Nil {
						_ = c.rdb.Del(ctx, lock).Err()
						return total, expiryErr
					}
					nowUnix := time.Now().Unix()
					if group.tracked {
						redisNow, timeErr := c.rdb.Time(ctx).Result()
						if timeErr != nil {
							_ = c.rdb.Del(ctx, lock).Err()
							return total, timeErr
						}
						nowUnix = redisNow.Unix()
					}
					if expires > nowUnix {
						continue
					}
				}
				claim := Claim{Job: job, Raw: raw, WorkerID: workerID}
				if job.TrackedEpoch != "" {
					claim.Token, err = c.rdb.HGet(ctx, JobHashPrefix+job.ID, "claim_token").Result()
					if err != nil {
						_ = c.rdb.Del(ctx, lock).Err()
						return total, err
					}
					claim.Generation, err = c.rdb.HGet(ctx, JobHashPrefix+job.ID, "claim_generation").Int64()
					if err != nil {
						_ = c.rdb.Del(ctx, lock).Err()
						return total, err
					}
				}
				status, statusErr := c.rdb.HGet(ctx, JobHashPrefix+job.ID, "status").Result()
				if statusErr != nil && statusErr != redis.Nil {
					_ = c.rdb.Del(ctx, lock).Err()
					return total, statusErr
				}
				if status == StatusDone || status == StatusFailed || status == StatusCanceled || status == StatusDispatched {
					// Terminal/dispatched status was committed before the original
					// worker lost its ACK response or heartbeat. The status is the
					// durable execution proof; requeueing this receipt would run or
					// recount already-completed work. Finish only the orphaned ACK.
					if err := c.AckClaim(ctx, claim); err != nil {
						_ = c.rdb.Del(ctx, lock).Err()
						return total, err
					}
					total++
					continue
				}
				if job.TrackedEpoch != "" && job.ParentID == "" {
					complete, err := c.TrackedPlanComplete(ctx, job)
					if err != nil {
						_ = c.rdb.Del(ctx, lock).Err()
						return total, err
					}
					if complete {
						if err := c.AckClaim(ctx, claim); err != nil {
							_ = c.rdb.Del(ctx, lock).Err()
							return total, err
						}
						total++
						continue
					}
					if err := c.deferTrackedClaim(ctx, claim,
						"planner lease expired before complete publication", time.Now(), true); err != nil {
						if errors.Is(err, ErrClaimLost) {
							continue
						}
						_ = c.rdb.Del(ctx, lock).Err()
						return total, err
					}
					total++
					continue
				}
				reason := "recovered after worker " + workerID + " disappeared; compatible worker selection rerun"
				statusReason, reasonErr := c.rdb.HGet(ctx, JobHashPrefix+job.ID, "error").Result()
				if reasonErr != nil && reasonErr != redis.Nil {
					_ = c.rdb.Del(ctx, lock).Err()
					return total, reasonErr
				}
				// An interrupted explicit CPU compatibility fallback must remain on
				// that lane. Re-running initial routing here previously promoted it to
				// the restarted GPU, whose live admission split the same sources into
				// a nested -cpu-cpu-cpu child. Worker loss is not new codec evidence.
				if job.QueueClass == QueueClassCPU && legacyCPUFallbackLocked(job, statusReason) &&
					!temporaryAvailabilityFallback(job, statusReason) {
					job.Attempts++
					job.CPUFallbackLocked = true
					provenance := strings.TrimSpace(job.RoutingReason)
					if provenance == "" {
						provenance = strings.TrimSpace(statusReason)
					}
					if provenance != "" {
						reason = provenance + "; recovered after worker " + workerID +
							" disappeared; preserved explicit CPU route"
					}
					if err := c.requeueClaimAs(ctx, claim, job, reason); err != nil {
						_ = c.rdb.Del(ctx, lock).Err()
						return total, err
					}
				} else if err := c.RequeueClaim(ctx, claim, false, reason); err != nil {
					_ = c.rdb.Del(ctx, lock).Err()
					return total, err
				}
				total++
			}
			_ = c.rdb.Del(ctx, lock).Err()
		}
	}
	return total, nil
}

func (c *Client) reclaimTrackedReservation(ctx context.Context, processingKey, destination string,
	job Job, raw, workerID, token string) (int, error) {
	generation, err := c.rdb.HGet(ctx, JobHashPrefix+job.ID, "claim_generation").Result()
	if err != nil {
		return 0, err
	}
	n, err := c.rdb.Eval(ctx, reclaimTrackedReservationScript, []string{
		processingKey, destination, JobHashPrefix + job.ID, LeaseIndexKey, TrackedProcessingIndexKey,
	}, token, workerID, raw, 0, job.ID, generation).Int()
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (c *Client) reclaimUnreservedTrackedChild(ctx context.Context, processingKey, destination string,
	job Job, raw, workerID string) (int, error) {
	n, err := c.rdb.Eval(ctx, reclaimUnreservedTrackedChildScript, []string{
		processingKey, destination, JobHashPrefix + job.ID, TrackedProcessingIndexKey,
	}, raw, workerID).Int()
	if err != nil {
		return 0, err
	}
	return n, nil
}

const reclaimUnreservedTrackedChildScript = `
local sourceType = redis.call('TYPE', KEYS[1]).ok
local destinationType = redis.call('TYPE', KEYS[2]).ok
local statusType = redis.call('TYPE', KEYS[3]).ok
local processingIndexType = redis.call('TYPE', KEYS[4]).ok
if sourceType ~= 'list' or
   (destinationType ~= 'list' and destinationType ~= 'none') or
   statusType ~= 'hash' or
   (processingIndexType ~= 'set' and processingIndexType ~= 'none') then
  return redis.error_reply('invalid unreserved tracked recovery key type')
end
local phase = redis.call('HGET', KEYS[3], 'claim_phase')
local owner = redis.call('HGET', KEYS[3], 'lease_owner')
if redis.call('HGET', KEYS[3], 'status') ~= 'queued' or
   (phase ~= false and phase ~= 'reclaiming') or
   (owner ~= false and owner ~= ARGV[2]) or
   redis.call('HGET', KEYS[3], 'claim_token') or
   redis.call('HGET', KEYS[3], 'lease_expires_unix') or
   redis.call('HGET', KEYS[3], 'lease_expires_at') then
  return 0
end
if not redis.call('LPOS', KEYS[1], ARGV[1]) then return 0 end
redis.call('HSET', KEYS[3], 'claim_phase', 'reclaiming', 'lease_owner', ARGV[2], 'error',
  'recovering tracked child before reservation')
if not redis.call('LPOS', KEYS[2], ARGV[1]) then redis.call('LPUSH', KEYS[2], ARGV[1]) end
if redis.call('LREM', KEYS[1], 1, ARGV[1]) ~= 1 then return 0 end
if redis.call('LLEN', KEYS[1]) == 0 then redis.call('SREM', KEYS[4], ARGV[2]) end
redis.call('HDEL', KEYS[3], 'claim_phase', 'lease_owner')
return 1`

const reclaimTrackedReservationScript = `
local sourceType = redis.call('TYPE', KEYS[1]).ok
local destinationType = redis.call('TYPE', KEYS[2]).ok
local statusType = redis.call('TYPE', KEYS[3]).ok
local leaseIndexType = redis.call('TYPE', KEYS[4]).ok
local processingIndexType = redis.call('TYPE', KEYS[5]).ok
if sourceType ~= 'list' or
   (destinationType ~= 'list' and destinationType ~= 'none') or
   statusType ~= 'hash' or
   (leaseIndexType ~= 'zset' and leaseIndexType ~= 'none') or
   (processingIndexType ~= 'set' and processingIndexType ~= 'none') then
  return redis.error_reply('invalid tracked reservation recovery key type')
end
local redisNow = tonumber(redis.call('TIME')[1])
if redis.call('HGET', KEYS[3], 'claim_token') ~= ARGV[1] or
   redis.call('HGET', KEYS[3], 'lease_owner') ~= ARGV[2] or
   redis.call('HGET', KEYS[3], 'claim_generation') ~= ARGV[6] or
   (redis.call('HGET', KEYS[3], 'claim_phase') ~= 'reserved' and
    redis.call('HGET', KEYS[3], 'claim_phase') ~= 'reclaiming' and
    redis.call('HGET', KEYS[3], 'claim_phase') ~= 'running') or
   (redis.call('HGET', KEYS[3], 'status') ~= 'queued' and
    redis.call('HGET', KEYS[3], 'status') ~= 'running') or
   tonumber(redis.call('HGET', KEYS[3], 'lease_expires_unix') or '0') > redisNow then
  return 0
end
if not redis.call('LPOS', KEYS[1], ARGV[3]) then return 0 end
redis.call('HSET', KEYS[3], 'claim_phase', 'reclaiming', 'error',
  'recovering expired reservation before execution')
redis.call('LREM', KEYS[2], 0, ARGV[3])
redis.call('LPUSH', KEYS[2], ARGV[3])
redis.call('ZREM', KEYS[4], ARGV[5])
if redis.call('LREM', KEYS[1], 1, ARGV[3]) ~= 1 then return 0 end
redis.call('HSET', KEYS[3], 'status', 'queued')
redis.call('HDEL', KEYS[3], 'claim_phase', 'claim_token', 'lease_owner',
  'lease_expires_at', 'lease_expires_unix')
if redis.call('LLEN', KEYS[1]) == 0 then redis.call('SREM', KEYS[5], ARGV[2]) end
return 1`

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

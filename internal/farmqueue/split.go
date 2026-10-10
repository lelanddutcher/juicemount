package farmqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const splitTransactionRetries = 5

// ErrCPUFallbackAmbiguous means an existing deterministic fallback child
// cannot be proven published from its immutable payload and receipt. The
// caller must retain the original parent rather than mint another child ID.
var ErrCPUFallbackAmbiguous = errors.New("CPU fallback publication ambiguous")

const enqueueCPUFallbackSubsetScript = `
local function keyType(key)
  return redis.call('TYPE', key).ok
end
local expected = {'hash', 'list', 'zset'}
for i = 1, 3 do
  local got = keyType(KEYS[i])
  if got ~= 'none' and got ~= expected[i] then
    return redis.error_reply('CPU fallback key has type ' .. got .. ', expected ' .. expected[i])
  end
end
local protocol = false
if ARGV[4] ~= '' then
  if keyType(KEYS[4]) ~= 'hash' or keyType(KEYS[5]) ~= 'list' then
    return redis.error_reply('invalid tracked fallback parent key type') end
  protocol = redis.call('HGET', KEYS[4], 'coverage_protocol')
  if protocol == '1' then
    if keyType(KEYS[6]) ~= 'hash' or
       (keyType(KEYS[7]) ~= 'zset' and keyType(KEYS[7]) ~= 'none') or
       keyType(KEYS[8]) ~= 'hash' or
       redis.call('HGET', KEYS[6], 'coverage_protocol') ~= '1' or
       redis.call('HGET', KEYS[6], 'parent_id') ~= ARGV[11] or
       redis.call('HGET', KEYS[6], 'epoch') ~= ARGV[4] or
       redis.call('HGET', KEYS[6], 'root') ~= ARGV[12] or
       redis.call('HGET', KEYS[6], 'state') ~= 'open' or
       not tonumber(redis.call('HGET', KEYS[6], 'open_count')) or
       tonumber(redis.call('HGET', KEYS[6], 'open_count')) >
         redis.call('ZCOUNT', KEYS[7], 0, 0) or
       redis.call('HGET', KEYS[4], 'coverage_parent_id') ~= ARGV[11] then
      return redis.error_reply('tracked fallback has no open coverage ledger') end
  elseif protocol or keyType(KEYS[6]) ~= 'none' or keyType(KEYS[7]) ~= 'none' then
    return redis.error_reply('legacy tracked fallback has unexpected coverage state')
  end
  local now = tonumber(redis.call('TIME')[1])
  if redis.call('HGET', KEYS[4], 'claim_token') ~= ARGV[6] or
     redis.call('HGET', KEYS[4], 'lease_owner') ~= ARGV[7] or
     redis.call('HGET', KEYS[4], 'claim_generation') ~= ARGV[8] or
     redis.call('HGET', KEYS[4], 'claim_phase') ~= 'running' or
     redis.call('HGET', KEYS[4], 'status') ~= 'running' or
     tonumber(redis.call('HGET', KEYS[4], 'lease_expires_unix') or '0') <= now or
     not redis.call('LPOS', KEYS[5], ARGV[9]) then
    return {-1, ''} end
end

local exists = redis.call('EXISTS', KEYS[1]) == 1
local membership = nil
if protocol == '1' then
  membership = redis.call('ZSCORE', KEYS[7], ARGV[10])
  if (exists and not membership) or
     (membership and membership ~= '0' and membership ~= '1') or
     (membership == '1' and not exists) then
    return redis.error_reply('tracked fallback lacks matching coverage membership') end
end

local function equal(a, b)
  if type(a) ~= type(b) then return false end
  if type(a) ~= 'table' then return a == b end
  for k, v in pairs(a) do
    if not equal(v, b[k]) then return false end
  end
  for k, _ in pairs(b) do
    if a[k] == nil then return false end
  end
  return true
end

local function decode(raw)
  local ok, value = pcall(cjson.decode, raw)
  if not ok or type(value) ~= 'table' then return nil end
  return value
end

local function matchesRequest(savedRaw, requestRaw)
  local saved = decode(savedRaw)
  local request = decode(requestRaw)
  if not saved or not request then return false end
  saved.enqueued_at = nil
  request.enqueued_at = nil
  saved.routing_reason = nil
  request.routing_reason = nil
  return equal(saved, request)
end

if exists then
  local savedRaw = redis.call('HGET', KEYS[1], 'payload')
  if not savedRaw then
    return {-2, 'missing immutable payload'}
  end
  if redis.call('HGET', KEYS[1], 'cpu_fallback_published') ~= '1' then
    return {-2, 'missing publication receipt'}
  end
  if not matchesRequest(savedRaw, ARGV[1]) then
    return {-2, 'immutable request mismatch'}
  end
  return {0, savedRaw}
end

local statusOK, statusFields = pcall(cjson.decode, ARGV[2])
if not statusOK or type(statusFields) ~= 'table' or statusFields.id ~= ARGV[10] then
  return redis.error_reply('invalid CPU fallback status payload') end
if protocol == '1' and not membership then
  redis.call('ZADD', KEYS[7], 0, ARGV[10])
end
if protocol == '1' then
  redis.call('HSET', KEYS[6], 'open_count', redis.call('ZCOUNT', KEYS[7], 0, 0))
end
for field, value in pairs(statusFields) do
  redis.call('HSET', KEYS[1], field, value)
end
redis.call('HSET', KEYS[1], 'payload', ARGV[1])
if ARGV[4] ~= '' then
  redis.call('HSET', KEYS[1], 'tracked_epoch', ARGV[4])
  if protocol == '1' then redis.call('HSET', KEYS[1], 'coverage_protocol', '1') end
else
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[5]))
end
redis.call('LPUSH', KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[3], tonumber(ARGV[3]), statusFields.id)
redis.call('HSET', KEYS[1], 'cpu_fallback_published', '1')
return {1, ARGV[1]}
`

// EnqueueCPUFallbackSubset creates one idempotent CPU child for the exact
// sources a render worker cannot keep on its verified hardware path. The child
// is independent, so the server can claim those files while the parent keeps
// encoding the compatible majority on the GPU.
//
// The deterministic child ID and atomic Lua write make worker-crash recovery
// safe: replaying the original durable parent claim returns the persisted child
// payload instead of publishing a duplicate fallback batch.
func (c *Client) EnqueueCPUFallbackSubset(ctx context.Context, parent Job, targets []string, reason string) (Job, bool, error) {
	if parent.TrackedEpoch != "" {
		return Job{}, false, fmt.Errorf("%w: tracked fallback publication requires a claim", ErrClaimLost)
	}
	return c.enqueueCPUFallbackSubset(ctx, Claim{}, parent, targets, reason)
}

func (c *Client) EnqueueCPUFallbackSubsetForClaim(ctx context.Context, claim Claim, targets []string, reason string) (Job, bool, error) {
	if claim.Job.TrackedEpoch == "" || claim.Job.ParentID == "" || claim.Token == "" || claim.Generation < 1 || claim.Raw == "" {
		return Job{}, false, fmt.Errorf("%w: tracked fallback requires an execution claim", ErrClaimLost)
	}
	return c.enqueueCPUFallbackSubset(ctx, claim, claim.Job, targets, reason)
}

func (c *Client) enqueueCPUFallbackSubset(ctx context.Context, claim Claim, parent Job, targets []string, reason string) (Job, bool, error) {
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
		QueueClass: child.QueueClass, ParentID: child.ParentID, CoverageParentID: child.CoverageParentID, Error: reason,
	}
	statusJSON, err := json.Marshal(st.toMap())
	if err != nil {
		return Job{}, false, err
	}
	keys := []string{JobHashPrefix + child.ID, QueueKeyFor(&child), JobIndexKey}
	if child.TrackedEpoch != "" {
		keys = append(keys, JobHashPrefix+parent.ID, claimProcessingKey(claim),
			coverageLedgerKey(child.CoverageParentID), coverageMembersKey(child.CoverageParentID),
			watchEpochKey(child.Path))
	}
	result, err := c.rdb.Eval(ctx, enqueueCPUFallbackSubsetScript, keys,
		raw, statusJSON, time.Now().UnixMicro(), child.TrackedEpoch, int64(JobTTL/time.Second),
		claim.Token, claim.WorkerID, claim.Generation, claim.Raw,
		child.ID, child.CoverageParentID, child.Path).Slice()
	if err != nil {
		return Job{}, false, err
	}
	if len(result) != 2 {
		return Job{}, false, fmt.Errorf("unexpected CPU fallback enqueue result: %v", result)
	}
	created, ok := result[0].(int64)
	if !ok {
		return Job{}, false, fmt.Errorf("unexpected CPU fallback enqueue status: %T", result[0])
	}
	if created == -2 {
		reason, ok := result[1].(string)
		if !ok {
			return Job{}, false, fmt.Errorf("unexpected CPU fallback ambiguity reason: %T", result[1])
		}
		return Job{}, false, fmt.Errorf("%w: child %s: %s", ErrCPUFallbackAmbiguous, child.ID, reason)
	}
	if created < 0 {
		return Job{}, false, fmt.Errorf("%w: tracked fallback parent %s cannot publish", ErrClaimLost, parent.ID)
	}
	payload, ok := result[1].(string)
	if !ok {
		return Job{}, false, fmt.Errorf("unexpected CPU fallback enqueue payload: %T", result[1])
	}
	if err := json.Unmarshal([]byte(payload), &child); err != nil {
		return Job{}, false, fmt.Errorf("decode persisted CPU fallback payload: %w", err)
	}
	return child, created == 1, nil
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
	child.HardwareFailures = 0
	child.ProcessedOffset = 0
	child.RetryTargets = append([]string(nil), targets...)
	return child, nil
}

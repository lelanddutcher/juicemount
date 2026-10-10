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
	if len(children) > 0 && children[0].TrackedEpoch != "" {
		return 0, fmt.Errorf("%w: tracked planner publication requires a claim", ErrClaimLost)
	}
	return c.enqueuePlannedChildren(ctx, Claim{}, children)
}

func (c *Client) EnqueuePlannedChildrenForClaim(ctx context.Context, planner Claim, children []Job) (int, error) {
	if planner.Job.TrackedEpoch == "" || planner.Job.ParentID != "" || planner.Token == "" || planner.Generation < 1 {
		return 0, fmt.Errorf("%w: tracked planner publication requires its current claim", ErrClaimLost)
	}
	return c.enqueuePlannedChildren(ctx, planner, children)
}

func (c *Client) enqueuePlannedChildren(ctx context.Context, planner Claim, children []Job) (int, error) {
	if len(children) == 0 {
		return 0, fmt.Errorf("planned child set is empty")
	}
	seen := make(map[string]bool, len(children))
	for i, child := range children {
		if planner.Token != "" {
			if child.TrackedClaim != "" && child.TrackedClaim != planner.Token {
				return 0, fmt.Errorf("%w: planned child has a different claim", ErrClaimLost)
			}
			children[i].TrackedClaim = planner.Token
			child.TrackedClaim = planner.Token
		}
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
	return c.enqueueTargetShardsAtomic(ctx, planner, children)
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
	if parent.TrackedEpoch != "" {
		return nil, 0, fmt.Errorf("%w: tracked planner publication requires a claim", ErrClaimLost)
	}
	return c.enqueueTargetShards(ctx, Claim{}, parent, targets, maxTargets)
}

func (c *Client) EnqueueTargetShardsForClaim(ctx context.Context, planner Claim, targets []string, maxTargets int) ([]Job, int, error) {
	if planner.Job.TrackedEpoch == "" || planner.Job.ParentID != "" || planner.Token == "" || planner.Generation < 1 {
		return nil, 0, fmt.Errorf("%w: tracked planner publication requires its current claim", ErrClaimLost)
	}
	parent := planner.Job
	parent.TrackedClaim = planner.Token
	return c.enqueueTargetShards(ctx, planner, parent, targets, maxTargets)
}

func (c *Client) enqueueTargetShards(ctx context.Context, planner Claim, parent Job, targets []string, maxTargets int) ([]Job, int, error) {
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
					if !IsWorkerPinCapability(capability) {
						caps = append(caps, capability)
					}
				}
				children[i].RequiredCapabilities = caps
			}
		}
	}
	created, err := c.enqueueTargetShardsAtomic(ctx, planner, children)
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
	if parent.TrackedEpoch != "" {
		enqueuedAt = parent.EnqueuedAt
	}
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
				if !IsWorkerPinCapability(capability) {
					caps = append(caps, capability)
				}
			}
			child.RequiredCapabilities = caps
		}
		children = append(children, child)
	}
	return children, nil
}

func (c *Client) enqueueTargetShardsAtomic(ctx context.Context, planner Claim, children []Job) (int, error) {
	if len(children) > 0 && children[0].TrackedEpoch != "" {
		return c.publishTrackedPlanAtomic(ctx, planner, children[0], children)
	}
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

// CompleteTrackedEmptyPlan records a successful full traversal with no
// execution children. A failed or partial walk must never call this method.
func (c *Client) CompleteTrackedEmptyPlan(ctx context.Context, parent Job) error {
	return fmt.Errorf("%w: tracked empty plan requires a claim", ErrClaimLost)
}

func (c *Client) CompleteTrackedEmptyPlanForClaim(ctx context.Context, planner Claim) error {
	if planner.Job.TrackedEpoch == "" || planner.Job.ParentID != "" || planner.Token == "" || planner.Generation < 1 {
		return fmt.Errorf("%w: tracked empty plan requires its current claim", ErrClaimLost)
	}
	parent := planner.Job
	parent.TrackedClaim = planner.Token
	_, err := c.publishTrackedPlanAtomic(ctx, planner, parent, nil)
	return err
}

func (c *Client) publishTrackedPlanAtomic(ctx context.Context, planner Claim, first Job, children []Job) (int, error) {
	parentID := first.ParentID
	if len(children) == 0 {
		parentID = first.ID
	}
	if parentID == "" || first.TrackedEpoch == "" || first.TrackedClaim == "" ||
		planner.Job.ID != parentID || planner.Job.Path != first.Path ||
		planner.Job.TrackedEpoch != first.TrackedEpoch || planner.Token != first.TrackedClaim ||
		planner.Generation < 1 || planner.Raw == "" || planner.WorkerID == "" {
		return 0, fmt.Errorf("tracked plan has no epoch, parent, or claim")
	}
	parentKey := JobHashPrefix + parentID
	epochKey := watchEpochKey(first.Path)
	statusKeys := make([]string, 0, len(children))
	raws := make([][]byte, len(children))
	statuses := make([]JobStatus, len(children))
	for i, child := range children {
		child.CoverageParentID = parentID
		children[i].CoverageParentID = parentID
		if child.ParentID != parentID || child.Path != first.Path ||
			child.TrackedEpoch != first.TrackedEpoch || child.TrackedClaim != first.TrackedClaim {
			return 0, fmt.Errorf("tracked plan mixes parent, path, epoch, or claim")
		}
		if err := validateTrackedKinds(child.Kinds); err != nil {
			return 0, err
		}
		if child.PlanOnly || child.QueueClass == "" || len(child.RequiredCapabilities) == 0 {
			return 0, fmt.Errorf("tracked child %s lacks a bounded execution route", child.ID)
		}
		var err error
		raws[i], err = json.Marshal(child)
		if err != nil {
			return 0, err
		}
		statusKeys = append(statusKeys, JobHashPrefix+child.ID)
		statuses[i] = JobStatus{
			ID: child.ID, Status: StatusQueued, Path: child.Path, Kinds: strings.Join(child.Kinds, ","),
			Producer: child.Producer, EnqueuedAt: child.EnqueuedAt, Backend: child.SelectedBackend,
			TargetWorker: child.SelectedWorker, QueueClass: child.QueueClass, Attempts: child.Attempts,
			HardwareFailures: child.HardwareFailures, ParentID: child.ParentID, CoverageParentID: child.CoverageParentID,
			DerivativePass: child.DerivativePass, Error: child.RoutingReason,
		}
	}
	keys := []string{parentKey, epochKey, JobIndexKey, claimProcessingKey(planner),
		coverageLedgerKey(parentID), coverageMembersKey(parentID), watchRetireKey}
	args := []any{first.TrackedClaim, first.TrackedEpoch, time.Now().Unix(), parentID,
		len(children), time.Now().UnixMicro(), planner.WorkerID, planner.Generation, planner.Raw}
	for i, child := range children {
		status, err := json.Marshal(statuses[i].toMap())
		if err != nil {
			return 0, err
		}
		keys = append(keys, statusKeys[i], QueueKeyFor(&child))
		args = append(args, string(raws[i]), child.ID, string(status))
	}
	const script = `
local parentType = redis.call('TYPE', KEYS[1]).ok
local epochType = redis.call('TYPE', KEYS[2]).ok
local indexType = redis.call('TYPE', KEYS[3]).ok
local processingType = redis.call('TYPE', KEYS[4]).ok
local ledgerType = redis.call('TYPE', KEYS[5]).ok
local membersType = redis.call('TYPE', KEYS[6]).ok
local retireType = redis.call('TYPE', KEYS[7]).ok
if parentType ~= 'hash' or epochType ~= 'hash' or
   (indexType ~= 'zset' and indexType ~= 'none') or processingType ~= 'list' or
   (ledgerType ~= 'hash' and ledgerType ~= 'none') or
   (membersType ~= 'zset' and membersType ~= 'none') or
   (retireType ~= 'zset' and retireType ~= 'none') then
  return redis.error_reply('invalid tracked plan key type') end
if redis.call('HGET', KEYS[2], 'active_epoch') ~= ARGV[2] or
   redis.call('HGET', KEYS[2], 'coverage_protocol') ~= '1' or
   redis.call('HGET', KEYS[1], 'coverage_protocol') ~= '1' then return -1 end
if ledgerType == 'hash' then
  if redis.call('HGET', KEYS[5], 'parent_id') ~= ARGV[4] or
     redis.call('HGET', KEYS[5], 'epoch') ~= ARGV[2] or
     redis.call('HGET', KEYS[5], 'coverage_protocol') ~= '1' or
     redis.call('HGET', KEYS[5], 'root') ~= redis.call('HGET', KEYS[1], 'path') or
     redis.call('HGET', KEYS[5], 'state') ~= 'open' or
     not tonumber(redis.call('HGET', KEYS[5], 'open_count')) or
     tonumber(redis.call('HGET', KEYS[5], 'open_count')) >
       redis.call('ZCOUNT', KEYS[6], 0, 0) then
    return redis.error_reply('tracked coverage ledger is not open or has wrong identity') end
elseif membersType ~= 'none' then
  return redis.error_reply('tracked coverage members have no ledger')
end
local receiptKey = 'receipt:' .. ARGV[4]
if redis.call('HGET', KEYS[1], 'claim_token') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'lease_owner') ~= ARGV[7] or
   redis.call('HGET', KEYS[1], 'claim_generation') ~= ARGV[8] or
   redis.call('HGET', KEYS[1], 'claim_phase') ~= 'running' or
   not redis.call('LPOS', KEYS[4], ARGV[9]) then
  return -1 end
if redis.call('HGET', KEYS[2], receiptKey) then
  if redis.call('HGET', KEYS[1], 'status') == 'dispatched' then return 0 end
  return redis.error_reply('tracked planner receipt conflicts with status')
end
local now = tonumber(redis.call('TIME')[1])
if redis.call('HGET', KEYS[1], 'claim_token') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'status') ~= 'running' or
   tonumber(redis.call('HGET', KEYS[1], 'lease_expires_unix') or '0') <= now then
  return -1
end
local pendingPayload = {}
local pendingRoute = {}
for i = 1, tonumber(ARGV[5]) do
  local key = 8 + (i-1)*2
  local arg = 10 + (i-1)*3
  local statusType = redis.call('TYPE', KEYS[key]).ok
  local queueType = redis.call('TYPE', KEYS[key+1]).ok
  if (statusType ~= 'hash' and statusType ~= 'none') or
     (queueType ~= 'list' and queueType ~= 'none') then
    return redis.error_reply('invalid tracked child key type')
  end
  if statusType == 'hash' and
     (redis.call('HGET', KEYS[key], 'tracked_epoch') ~= ARGV[2] or
      redis.call('HGET', KEYS[key], 'parent_id') ~= ARGV[4]) then
    return redis.error_reply('tracked child id collision')
  end
  local member = redis.call('ZSCORE', KEYS[6], ARGV[arg+1])
  if (statusType == 'hash' and not member) or
     (member and member ~= '0' and member ~= '1') or
     (ledgerType == 'none' and statusType == 'hash') then
    return redis.error_reply('tracked child lacks matching coverage membership') end
  local statusOK, statusFields = pcall(cjson.decode, ARGV[arg+2])
  if not statusOK or type(statusFields) ~= 'table' or statusFields.id ~= ARGV[arg+1] then
    return redis.error_reply('invalid tracked child status payload') end
  if statusType == 'hash' and redis.call('HGET', KEYS[key], 'published') ~= '1' then
    local raw = redis.call('HGET', KEYS[key], 'payload')
    local ok, stored = pcall(cjson.decode, raw or '')
    local expectedOK, expected = pcall(cjson.decode, ARGV[arg])
    if not ok or not expectedOK or stored.id ~= ARGV[arg+1] or
       stored.parent_id ~= ARGV[4] or stored.tracked_epoch ~= ARGV[2] or
       not stored.retry_targets or not expected.retry_targets or
       #stored.retry_targets ~= #expected.retry_targets then
      return redis.error_reply('corrupt unpublished tracked child payload')
    end
    for target = 1, #stored.retry_targets do
      if stored.retry_targets[target] ~= expected.retry_targets[target] then
        return redis.error_reply('unpublished tracked child targets changed')
      end
    end
    if not stored.kinds or #stored.kinds ~= 1 or
       not expected.kinds or #expected.kinds ~= 1 or stored.kinds[1] ~= expected.kinds[1] or
       (stored.queue_class ~= 'server' and stored.queue_class ~= 'render' and stored.queue_class ~= 'cpu') then
      return redis.error_reply('invalid unpublished tracked child route')
    end
    local route = 'juicefarm:queue:watch-v2:' .. stored.queue_class .. ':' .. stored.kinds[1]
    local routeType = redis.call('TYPE', route).ok
    if routeType ~= 'list' and routeType ~= 'none' then
      return redis.error_reply('invalid unpublished tracked child lane')
    end
    pendingPayload[i] = raw
    pendingRoute[i] = route
  end
end
if ledgerType == 'none' then
  redis.call('HSET', KEYS[5], 'parent_id', ARGV[4], 'epoch', ARGV[2],
    'root', redis.call('HGET', KEYS[1], 'path'), 'coverage_protocol', '1', 'state', 'open',
    'open_count', '0', 'committed', '0', 'planner_ack', '0')
end
for i = 1, tonumber(ARGV[5]) do
  local arg = 10 + (i-1)*3
  if not redis.call('ZSCORE', KEYS[6], ARGV[arg+1]) then
    redis.call('ZADD', KEYS[6], 0, ARGV[arg+1])
  end
end
redis.call('HSET', KEYS[5], 'open_count', redis.call('ZCOUNT', KEYS[6], 0, 0))
redis.call('ZADD', KEYS[7], tonumber(redis.call('TIME')[1]) + 3600,
  'coverage:' .. ARGV[4])
local created = 0
for i = 1, tonumber(ARGV[5]) do
  local key = 8 + (i-1)*2
  local arg = 10 + (i-1)*3
  local fresh = false
  if redis.call('EXISTS', KEYS[key]) == 0 then
    local status = cjson.decode(ARGV[arg+2])
    for field, value in pairs(status) do redis.call('HSET', KEYS[key], field, value) end
    redis.call('HSET', KEYS[key], 'payload', ARGV[arg], 'tracked_epoch', ARGV[2],
      'coverage_protocol', '1', 'published', '0')
    fresh = true
  end
  if redis.call('HGET', KEYS[key], 'published') ~= '1' then
    local status = redis.call('HGET', KEYS[key], 'status')
    local payload = pendingPayload[i] or ARGV[arg]
    local route = pendingRoute[i] or KEYS[key+1]
    if status == 'queued' then
      if fresh or not redis.call('LPOS', route, payload) then
        redis.call('LPUSH', route, payload)
      end
    end
    redis.call('HSET', KEYS[key], 'published', '1')
    redis.call('ZADD', KEYS[3], tonumber(ARGV[6])+i, ARGV[arg+1])
    created = created + 1
  end
end
redis.call('HSET', KEYS[2], receiptKey, ARGV[5])
redis.call('HSET', KEYS[1], 'status', 'dispatched', 'coverage_receipt', ARGV[5], 'error', '')
return created`
	created, err := c.rdb.Eval(ctx, script, keys, args...).Int()
	if err != nil {
		return 0, err
	}
	if created < 0 {
		return 0, fmt.Errorf("%w: tracked planner %s cannot publish", ErrClaimLost, parentID)
	}
	return created, nil
}

func (c *Client) TrackedPlanComplete(ctx context.Context, parent Job) (bool, error) {
	if parent.TrackedEpoch == "" || parent.ParentID != "" {
		return false, nil
	}
	receipt, err := c.rdb.HGet(ctx, watchEpochKey(parent.Path), "receipt:"+parent.ID).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if receipt == "" {
		return false, nil
	}
	return true, nil
}

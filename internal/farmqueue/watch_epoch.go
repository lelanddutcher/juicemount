package farmqueue

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const watchRetryKey = "juicefarm:watch:v2:retry"
const watchRetireKey = "juicefarm:watch:v2:retire"
const coverageRetirePrefix = "coverage:"

func coverageLedgerKey(parentID string) string {
	return "juicefarm:watch:v2:coverage:" + parentID
}

func coverageMembersKey(parentID string) string {
	return coverageLedgerKey(parentID) + ":members"
}

// WatchEpoch is the nonexpiring custody record for one logical mount root.
// The legacy cursor is deliberately ignored until this v2 record exists.
type WatchEpoch struct {
	Cursor     time.Time
	Active     string
	Cutoff     time.Time
	Pending    int
	Status     string
	Error      string
	NextRetry  time.Time
	RetryCount int
	Kinds      []string
}

func watchEpochKey(root string) string {
	sum := sha256.Sum256([]byte(root))
	return fmt.Sprintf("juicefarm:watch:v2:root:%x", sum[:16])
}

func parseWatchTime(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return time.Unix(seconds, 0).UTC(), nil
	}
	return time.Parse(time.RFC3339Nano, raw)
}

func (c *Client) GetWatchEpoch(ctx context.Context, root string) (WatchEpoch, error) {
	m, err := c.rdb.HGetAll(ctx, watchEpochKey(root)).Result()
	if err != nil {
		return WatchEpoch{}, err
	}
	var state WatchEpoch
	if len(m) == 0 {
		return state, nil
	}
	if state.Cursor, err = parseWatchTime(m["cursor"]); err != nil {
		return WatchEpoch{}, fmt.Errorf("watch epoch cursor: %w", err)
	}
	if state.Cutoff, err = parseWatchTime(m["cutoff"]); err != nil {
		return WatchEpoch{}, fmt.Errorf("watch epoch cutoff: %w", err)
	}
	if state.NextRetry, err = parseWatchTime(m["next_retry_at"]); err != nil {
		return WatchEpoch{}, fmt.Errorf("watch epoch retry: %w", err)
	}
	state.Active = m["active_epoch"]
	if m["kinds"] != "" {
		state.Kinds = strings.Split(m["kinds"], ",")
		if state.Active != "" {
			state.Pending = len(state.Kinds)
			for _, kind := range state.Kinds {
				if m["receipt:watch-v2-"+state.Active+"-"+kind] != "" {
					state.Pending--
				}
			}
		}
	}
	state.Status = m["status"]
	state.Error = m["last_error"]
	if m["kinds"] == "" {
		state.Pending, err = strconv.Atoi(m["pending"])
		if m["pending"] != "" && err != nil {
			return WatchEpoch{}, fmt.Errorf("watch epoch pending: %w", err)
		}
	}
	state.RetryCount, err = strconv.Atoi(m["retry_count"])
	if m["retry_count"] != "" && err != nil {
		return WatchEpoch{}, fmt.Errorf("watch epoch retry_count: %w", err)
	}
	return state, nil
}

// StartWatchEpoch publishes the custody record and every kind's isolated
// server planner in one Redis operation. A lost response is reconciled by
// reading the active epoch; it cannot create a second set of parents.
func (c *Client) StartWatchEpoch(ctx context.Context, root, leader string, kinds []string, cutoff time.Time) (WatchEpoch, error) {
	kinds = DrainKinds(kinds)
	if len(kinds) == 0 {
		return WatchEpoch{}, fmt.Errorf("watch epoch has no kinds")
	}
	for _, kind := range kinds {
		if !isDedicatedKind(kind) {
			return WatchEpoch{}, fmt.Errorf("watch epoch unsupported kind %q", kind)
		}
	}
	epochID := NewID()
	keys := []string{WatchLeaderKey, watchEpochKey(root), JobIndexKey}
	args := []any{leader, epochID, cutoff.UTC().Format(time.RFC3339Nano), strconv.Itoa(len(kinds)), strconv.FormatInt(time.Now().UnixMicro(), 10), strings.Join(kinds, ",")}
	for _, kind := range kinds {
		job := NewJob(root, []string{kind}, "farm-watch-catchup")
		job.ID = "watch-v2-" + epochID + "-" + kind
		job.EnqueuedAt = cutoff.UTC().Format(time.RFC3339Nano)
		job.PlanOnly = true
		job.TrackedEpoch = epochID
		job.QueueClass = QueueClassServer
		job.SelectedBackend = "server-dispatch"
		job.RequiredCapabilities = []string{"metadata"}
		raw, err := json.Marshal(job)
		if err != nil {
			return WatchEpoch{}, err
		}
		keys = append(keys, JobHashPrefix+job.ID, QueueKeyFor(&job))
		args = append(args, string(raw), job.ID, kind, job.EnqueuedAt)
	}
	const script = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
if redis.call('HGET', KEYS[2], 'active_epoch') ~= false and
   redis.call('HGET', KEYS[2], 'active_epoch') ~= '' then return 0 end
local epochType = redis.call('TYPE', KEYS[2]).ok
local indexType = redis.call('TYPE', KEYS[3]).ok
if (epochType ~= 'hash' and epochType ~= 'none') or
   (indexType ~= 'zset' and indexType ~= 'none') then
  return redis.error_reply('invalid watch epoch key type')
end
for i = 1, tonumber(ARGV[4]) do
  local key = 4 + (i-1)*2
  local arg = 8 + (i-1)*4
  local statusType = redis.call('TYPE', KEYS[key]).ok
  local queueType = redis.call('TYPE', KEYS[key+1]).ok
  if (statusType ~= 'hash' and statusType ~= 'none') or
     (queueType ~= 'list' and queueType ~= 'none') then
    return redis.error_reply('invalid watch parent key type')
  end
  local payloadOK, payload = pcall(cjson.decode, ARGV[arg-1])
  if not payloadOK or type(payload) ~= 'table' or payload.id ~= ARGV[arg] or
     payload.tracked_epoch ~= ARGV[2] or payload.path == nil then
    return redis.error_reply('invalid watch parent payload') end
end
redis.call('HSET', KEYS[2], 'version', '2', 'active_epoch', ARGV[2],
  'cutoff', ARGV[3], 'pending', ARGV[4], 'kinds', ARGV[6],
  'coverage_protocol', '1', 'status', 'planning', 'last_error', '',
  'next_retry_at', '', 'retry_count', '0')
for i = 1, tonumber(ARGV[4]) do
  local key = 4 + (i-1)*2
  local arg = 8 + (i-1)*4
  redis.call('LPUSH', KEYS[key+1], ARGV[arg-1])
  redis.call('HSET', KEYS[key],
    'id', ARGV[arg], 'status', 'queued', 'path', cjson.decode(ARGV[arg-1]).path,
    'kinds', ARGV[arg+1], 'producer', 'farm-watch-catchup',
    'enqueued_at', ARGV[arg+2], 'queue_class', 'server',
    'backend', 'server-dispatch', 'payload', ARGV[arg-1],
    'tracked_epoch', ARGV[2], 'coverage_protocol', '1', 'published', '1')
  redis.call('ZADD', KEYS[3], tonumber(ARGV[5]) + i, ARGV[arg])
end
return 1`
	code, err := c.rdb.Eval(ctx, script, keys, args...).Int()
	if err != nil {
		return WatchEpoch{}, err
	}
	if code == -1 {
		return WatchEpoch{}, fmt.Errorf("watch leadership expired before epoch publication")
	}
	return c.GetWatchEpoch(ctx, root)
}

// EnsureWatchEpochParents repairs a partial start operation. The epoch was
// reserved before any parent enqueue; a lost response or command error can
// therefore only leave missing parents, never an untracked cursor advance.
func (c *Client) EnsureWatchEpochParents(ctx context.Context, root string, state WatchEpoch) (int, error) {
	if state.Active == "" {
		return 0, nil
	}
	const script = `
if redis.call('HGET', KEYS[1], 'active_epoch') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'coverage_protocol') ~= '1' then return -1 end
if redis.call('HGET', KEYS[1], 'receipt:' .. ARGV[2]) then return 0 end
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
local queueType = redis.call('TYPE', KEYS[3]).ok
if queueType ~= 'list' and queueType ~= 'none' then
  return redis.error_reply('invalid tracked parent lane')
end
local indexType = redis.call('TYPE', KEYS[4]).ok
if indexType ~= 'zset' and indexType ~= 'none' then
  return redis.error_reply('invalid tracked index')
end
if not redis.call('LPOS', KEYS[3], ARGV[3]) then
  redis.call('LPUSH', KEYS[3], ARGV[3])
end
redis.call('HSET', KEYS[2], 'id', ARGV[2], 'status', 'queued',
  'path', ARGV[4], 'kinds', ARGV[5], 'producer', 'farm-watch-catchup',
  'enqueued_at', ARGV[6], 'queue_class', 'server',
  'backend', 'server-dispatch', 'payload', ARGV[3],
  'tracked_epoch', ARGV[1], 'coverage_protocol', '1', 'published', '1')
redis.call('ZADD', KEYS[4], ARGV[7], ARGV[2])
return 1`
	repaired := 0
	for i, kind := range state.Kinds {
		job := NewJob(root, []string{kind}, "farm-watch-catchup")
		job.ID = "watch-v2-" + state.Active + "-" + kind
		job.EnqueuedAt = state.Cutoff.UTC().Format(time.RFC3339Nano)
		job.PlanOnly = true
		job.TrackedEpoch = state.Active
		job.QueueClass = QueueClassServer
		job.SelectedBackend = "server-dispatch"
		job.RequiredCapabilities = []string{"metadata"}
		raw, err := json.Marshal(job)
		if err != nil {
			return repaired, err
		}
		n, err := c.rdb.Eval(ctx, script, []string{
			watchEpochKey(root), JobHashPrefix + job.ID, QueueKeyFor(&job), JobIndexKey,
		}, state.Active, job.ID, string(raw), root, kind, job.EnqueuedAt,
			time.Now().UnixMicro()+int64(i)).Int()
		if err != nil {
			return repaired, err
		}
		if n < 0 {
			return repaired, fmt.Errorf("watch epoch %s changed during parent repair", state.Active)
		}
		repaired += n
	}
	return repaired, nil
}

func (c *Client) CommitWatchEpoch(ctx context.Context, root, leader, epochID string) (bool, error) {
	state, err := c.GetWatchEpoch(ctx, root)
	if err != nil {
		return false, err
	}
	if state.Active != epochID || len(state.Kinds) == 0 {
		return false, nil
	}
	keys := []string{WatchLeaderKey, watchEpochKey(root), WatchCursorKey}
	for _, kind := range state.Kinds {
		keys = append(keys, coverageLedgerKey("watch-v2-"+epochID+"-"+kind))
	}
	const script = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
if redis.call('HGET', KEYS[2], 'active_epoch') ~= ARGV[2] then return 0 end
if redis.call('HGET', KEYS[2], 'coverage_protocol') ~= '1' then return 0 end
if redis.call('TYPE', KEYS[2]).ok ~= 'hash' or
   (redis.call('TYPE', KEYS[3]).ok ~= 'string' and redis.call('TYPE', KEYS[3]).ok ~= 'none') then
  return redis.error_reply('invalid watch commit key type') end
local kinds = redis.call('HGET', KEYS[2], 'kinds') or ''
if kinds == '' or kinds ~= ARGV[3] then return 0 end
local index = 4
for kind in string.gmatch(kinds, '([^,]+)') do
  local parent = 'watch-v2-' .. ARGV[2] .. '-' .. kind
  if not redis.call('HGET', KEYS[2], 'receipt:' .. parent) or
     redis.call('TYPE', KEYS[index]).ok ~= 'hash' or
     redis.call('HGET', KEYS[index], 'parent_id') ~= parent or
     redis.call('HGET', KEYS[index], 'epoch') ~= ARGV[2] or
     redis.call('HGET', KEYS[index], 'root') ~= ARGV[4] or
     redis.call('HGET', KEYS[index], 'state') ~= 'open' or
     not tonumber(redis.call('HGET', KEYS[index], 'open_count')) then return 0 end
  index = index + 1
end
local cutoff = redis.call('HGET', KEYS[2], 'cutoff')
if not cutoff or cutoff == '' then return redis.error_reply('watch commit has no cutoff') end
local rootUpdates = {'cursor', cutoff, 'active_epoch', '',
  'status', 'complete', 'last_error', '', 'next_retry_at', '', 'pending', '0'}
for kind in string.gmatch(kinds, '([^,]+)') do
  table.insert(rootUpdates, 'committed:watch-v2-' .. ARGV[2] .. '-' .. kind)
  table.insert(rootUpdates, '1')
end
redis.call('HSET', KEYS[2], unpack(rootUpdates))
for i = 4, #KEYS do redis.call('HSET', KEYS[i], 'committed', '1') end
redis.call('SET', KEYS[3], cutoff)
return 1`
	n, err := c.rdb.Eval(ctx, script, keys, leader, epochID,
		strings.Join(state.Kinds, ","), root).Int()
	return n == 1, err
}

// An empty, successful candidate traversal has no execution obligations.
func (c *Client) AdvanceWatchIdleCursor(ctx context.Context, root, leader string, previous, next time.Time) (bool, error) {
	const script = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
if redis.call('HGET', KEYS[2], 'active_epoch') ~= '' then return 0 end
if redis.call('HGET', KEYS[2], 'cursor') ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[2], 'cursor', ARGV[3], 'status', 'idle')
redis.call('SET', KEYS[3], ARGV[3])
return 1`
	n, err := c.rdb.Eval(ctx, script, []string{WatchLeaderKey, watchEpochKey(root), WatchCursorKey},
		leader, previous.UTC().Format(time.RFC3339Nano), next.UTC().Format(time.RFC3339Nano)).Int()
	return n == 1, err
}

var watchRetryDelays = []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour}

func WatchRetryDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return watchRetryDelays[0]
	}
	if attempt <= len(watchRetryDelays) {
		return watchRetryDelays[attempt-1]
	}
	return 6 * time.Hour
}

// DeferTrackedClaim moves a failed planner from processing to a scheduled
// retry, preserving both its epoch and its nonexpiring status/payload.
func (c *Client) DeferTrackedClaim(ctx context.Context, claim Claim, reason string, now time.Time) error {
	return c.deferTrackedClaim(ctx, claim, reason, now, false)
}

func (c *Client) deferTrackedClaim(ctx context.Context, claim Claim, reason string, now time.Time, requireExpired bool) error {
	if claim.Job.TrackedEpoch == "" || claim.Job.ParentID != "" {
		return fmt.Errorf("only a tracked planner can be deferred")
	}
	statusKey := JobHashPrefix + claim.Job.ID
	processingKey := claimProcessingKey(claim)
	const script = `
if redis.call('TYPE', KEYS[1]).ok ~= 'hash' or
   redis.call('TYPE', KEYS[2]).ok ~= 'list' or
   (redis.call('TYPE', KEYS[3]).ok ~= 'zset' and redis.call('TYPE', KEYS[3]).ok ~= 'none') or
   (redis.call('TYPE', KEYS[4]).ok ~= 'zset' and redis.call('TYPE', KEYS[4]).ok ~= 'none') or
   redis.call('TYPE', KEYS[5]).ok ~= 'hash' or
   (redis.call('TYPE', KEYS[6]).ok ~= 'set' and redis.call('TYPE', KEYS[6]).ok ~= 'none') or
   (redis.call('TYPE', KEYS[7]).ok ~= 'set' and redis.call('TYPE', KEYS[7]).ok ~= 'none') then
  return redis.error_reply('invalid tracked retry key type')
end
local redisNow = tonumber(redis.call('TIME')[1])
local expires = tonumber(redis.call('HGET', KEYS[1], 'lease_expires_unix') or '0')
if redis.call('HGET', KEYS[1], 'claim_token') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'lease_owner') ~= ARGV[7] or
   redis.call('HGET', KEYS[1], 'claim_generation') ~= ARGV[8] or
   redis.call('HGET', KEYS[1], 'claim_phase') ~= 'running' or
   redis.call('HGET', KEYS[1], 'status') ~= 'running' or
   not redis.call('LPOS', KEYS[2], ARGV[2]) or
   expires <= 0 or (ARGV[9] == '1' and expires > redisNow) or
   (ARGV[9] == '0' and expires <= redisNow) then return -1 end
local status = 'running'
local attempts = tonumber(redis.call('HGET', KEYS[1], 'retry_count') or '0')
if status == 'running' then attempts = attempts + 1 end
local delays = {60, 300, 900, 3600}
local delay = delays[attempts] or 21600
local due = tonumber(redis.call('HGET', KEYS[1], 'next_retry_at'))
if not due then due = redisNow + delay end
redis.call('HSET', KEYS[1], 'status', 'blocked', 'error', ARGV[4],
  'next_retry_at', tostring(due), 'retry_count', attempts)
redis.call('ZADD', KEYS[3], due, ARGV[6])
redis.call('LREM', KEYS[2], 1, ARGV[2])
redis.call('HDEL', KEYS[1], 'lease_owner', 'lease_expires_at', 'lease_expires_unix', 'claim_token')
redis.call('ZREM', KEYS[4], ARGV[6])
redis.call('HSET', KEYS[5], 'status', 'blocked', 'last_error', ARGV[4],
  'next_retry_at', tostring(due), 'retry_count', attempts)
if redis.call('LLEN', KEYS[2]) == 0 then redis.call('SREM', KEYS[6], ARGV[7]) end
redis.call('SREM', KEYS[7], ARGV[6])
return delay`
	expiredFlag := "0"
	if requireExpired {
		expiredFlag = "1"
	}
	n, err := c.rdb.Eval(ctx, script, []string{
		statusKey, processingKey, watchRetryKey, LeaseIndexKey,
		watchEpochKey(claim.Job.Path), claimProcessingIndex(claim), trackedCustodyIndexKey,
	}, claim.Token, claim.Raw, now.Unix(), reason, claim.Job.ID, claim.Job.ID,
		claim.WorkerID, claim.Generation, expiredFlag).Int()
	if err != nil {
		return err
	}
	if n < 0 {
		return fmt.Errorf("%w: tracked planner %s lost its claim before retry", ErrClaimLost, claim.Job.ID)
	}
	return nil
}

func (c *Client) PromoteWatchRetries(ctx context.Context, now time.Time, limit int64) (int, error) {
	if limit <= 0 || limit > 128 {
		limit = 128
	}
	ids, err := c.rdb.ZRangeByScore(ctx, watchRetryKey, &redis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(now.Unix(), 10), Count: limit,
	}).Result()
	if err != nil {
		return 0, err
	}
	const script = `
if redis.call('HGET', KEYS[1], 'status') ~= 'blocked' or
   redis.call('HGET', KEYS[4], 'active_epoch') ~= ARGV[2] then
  redis.call('ZREM', KEYS[2], ARGV[1]); return 0
end
local raw = redis.call('HGET', KEYS[1], 'payload')
if not raw then return -1 end
local queueType = redis.call('TYPE', KEYS[3]).ok
if queueType ~= 'list' and queueType ~= 'none' then
  return redis.error_reply('invalid tracked retry lane')
end
redis.call('LPUSH', KEYS[3], raw)
redis.call('HSET', KEYS[1], 'status', 'queued', 'next_retry_at', '')
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HSET', KEYS[4], 'status', 'planning', 'next_retry_at', '')
return 1`
	promoted := 0
	for _, id := range ids {
		m, err := c.rdb.HGetAll(ctx, JobHashPrefix+id).Result()
		if err != nil {
			return promoted, err
		}
		var job Job
		if err := json.Unmarshal([]byte(m["payload"]), &job); err != nil {
			return promoted, fmt.Errorf("watch retry %s has no valid payload: %w", id, err)
		}
		if err := validateTrackedKinds(job.Kinds); err != nil {
			return promoted, err
		}
		n, err := c.rdb.Eval(ctx, script, []string{
			JobHashPrefix + id, watchRetryKey, QueueKeyFor(&job), watchEpochKey(job.Path),
		}, id, job.TrackedEpoch).Int()
		if err != nil {
			return promoted, err
		}
		if n < 0 {
			return promoted, fmt.Errorf("watch retry %s lost its nonexpiring payload", id)
		}
		promoted += n
	}
	return promoted, nil
}

func validateTrackedKinds(kinds []string) error {
	if len(kinds) != 1 || !isDedicatedKind(kinds[0]) {
		return fmt.Errorf("tracked job needs exactly one supported kind")
	}
	return nil
}

// A tracked terminal child remains nonexpiring while its coverage family is
// open. The family ledger, not an individual receipt, controls retirement.
func (c *Client) retireTrackedTerminal(ctx context.Context, id string) error {
	_, err := c.retireTrackedTerminalOnce(ctx, id, time.Now())
	return err
}

func (c *Client) retireTrackedTerminalOnce(ctx context.Context, id string, now time.Time) (bool, error) {
	m, err := c.rdb.HGetAll(ctx, JobHashPrefix+id).Result()
	if err != nil {
		return false, err
	}
	if len(m) == 0 {
		return false, c.rdb.ZRem(ctx, watchRetireKey, id).Err()
	}
	parent := m["coverage_parent_id"]
	if parent == "" {
		parent = m["parent_id"]
	}
	const script = `
local statusType = redis.call('TYPE', KEYS[1]).ok
local ledgerType = redis.call('TYPE', KEYS[2]).ok
local membersType = redis.call('TYPE', KEYS[3]).ok
local retireType = redis.call('TYPE', KEYS[4]).ok
if (statusType ~= 'hash' and statusType ~= 'none') or
   (ledgerType ~= 'hash' and ledgerType ~= 'none') or
   (membersType ~= 'zset' and membersType ~= 'none') or
   (retireType ~= 'zset' and retireType ~= 'none') then
  return redis.error_reply('invalid tracked retirement key type')
end
local status = redis.call('HGET', KEYS[1], 'status')
if redis.call('HGET', KEYS[1], 'tracked_epoch') ~= ARGV[3] or
   not redis.call('HGET', KEYS[1], 'parent_id') or
   (status ~= 'done' and status ~= 'failed' and status ~= 'canceled') then
  redis.call('ZREM', KEYS[4], ARGV[1])
  return 0
end
local parent = redis.call('HGET', KEYS[1], 'coverage_parent_id')
if not parent or parent == '' then parent = redis.call('HGET', KEYS[1], 'parent_id') end
if parent ~= ARGV[4] then return redis.error_reply('tracked retirement parent changed') end
local acknowledged = redis.call('HGET', KEYS[1], 'ack_token') and
   redis.call('HGET', KEYS[1], 'ack_token') == redis.call('HGET', KEYS[1], 'terminal_token') and
   redis.call('HGET', KEYS[1], 'ack_owner') == redis.call('HGET', KEYS[1], 'terminal_owner') and
   redis.call('HGET', KEYS[1], 'ack_generation') == redis.call('HGET', KEYS[1], 'terminal_generation')
if ledgerType == 'hash' then
  if redis.call('HGET', KEYS[2], 'parent_id') ~= parent or
     redis.call('HGET', KEYS[2], 'epoch') ~= ARGV[3] or
     redis.call('HGET', KEYS[2], 'root') ~= ARGV[5] then
    return redis.error_reply('tracked retirement ledger identity changed') end
  local state = redis.call('HGET', KEYS[2], 'state')
  if state == 'open' and acknowledged and
     redis.call('HGET', KEYS[1], 'coverage_protocol') == '1' and
     redis.call('HGET', KEYS[2], 'coverage_protocol') == '1' then
    local member = redis.call('ZSCORE', KEYS[3], ARGV[1])
    local marker = redis.call('HGET', KEYS[2], 'settling:' .. ARGV[1])
    local token = redis.call('HGET', KEYS[1], 'ack_token')
    local openCount = tonumber(redis.call('HGET', KEYS[2], 'open_count'))
    local memberOpen = redis.call('ZCOUNT', KEYS[3], 0, 0)
    if not openCount then return redis.error_reply('invalid coverage count during ACK repair') end
    if member == '1' and marker == 'pending:' .. token and
       openCount == memberOpen + 1 then
      redis.call('HSET', KEYS[2], 'open_count', memberOpen,
        'settling:' .. ARGV[1], 'done:' .. token)
    elseif member == '0' and (not marker or marker == 'pending:' .. token) and
           openCount == memberOpen then
      redis.call('HSET', KEYS[2], 'settling:' .. ARGV[1], 'pending:' .. token)
      redis.call('ZADD', KEYS[3], 1, ARGV[1])
      redis.call('HSET', KEYS[2], 'open_count', redis.call('ZCOUNT', KEYS[3], 0, 0),
        'settling:' .. ARGV[1], 'done:' .. token)
    elseif openCount > memberOpen then
      redis.call('HSET', KEYS[2], 'last_error', 'unexplained coverage overcount')
      redis.call('ZADD', KEYS[4], ARGV[7], ARGV[1])
      return 0
    end
  end
  if state == 'drained' and acknowledged then
    if redis.call('TTL', KEYS[1]) == -1 then redis.call('EXPIRE', KEYS[1], ARGV[2]) end
    redis.call('ZREM', KEYS[4], ARGV[1])
    return 1
  end
  if state ~= 'open' and state ~= 'draining' and state ~= 'drained' then
    return redis.error_reply('invalid tracked retirement ledger state') end
  if state == 'draining' and acknowledged and
     not redis.call('ZSCORE', KEYS[3], ARGV[1]) then
    redis.call('ZREM', KEYS[4], ARGV[1])
    return 1
  end
  if (state == 'open' or state == 'draining') and acknowledged then
    redis.call('PERSIST', KEYS[1])
    redis.call('ZADD', KEYS[4], ARGV[6], 'coverage:' .. parent)
    redis.call('ZREM', KEYS[4], ARGV[1])
    return 0
  end
end
redis.call('PERSIST', KEYS[1])
redis.call('ZADD', KEYS[4], ARGV[7], ARGV[1])
return 0`
	n, err := c.rdb.Eval(ctx, script, []string{
		JobHashPrefix + id, coverageLedgerKey(parent), coverageMembersKey(parent), watchRetireKey,
	}, id, int64(JobTTL/time.Second), m["tracked_epoch"], parent, m["path"],
		now.Unix(), now.Add(time.Hour).Unix()).Int()
	return n == 1, err
}

func (c *Client) retireCoverageLedgerOnce(ctx context.Context, parent string, now time.Time, budget int) (int, error) {
	if budget < 1 {
		return 0, nil
	}
	if budget > 64 {
		budget = 64
	}
	metadata, err := c.rdb.HMGet(ctx, coverageLedgerKey(parent), "root", "epoch").Result()
	if err != nil {
		return 0, err
	}
	if len(metadata) != 2 || (metadata[0] == nil && metadata[1] == nil) {
		return 0, c.rdb.ZAdd(ctx, watchRetireKey, redis.Z{
			Score: float64(now.Add(time.Hour).Unix()), Member: coverageRetirePrefix + parent,
		}).Err()
	}
	root, rootOK := metadata[0].(string)
	epoch, epochOK := metadata[1].(string)
	if !rootOK || !epochOK || root == "" || epoch == "" {
		return 0, fmt.Errorf("coverage ledger %s has incomplete identity", parent)
	}
	const script = `
local ledgerType = redis.call('TYPE', KEYS[1]).ok
local membersType = redis.call('TYPE', KEYS[2]).ok
local rootType = redis.call('TYPE', KEYS[3]).ok
local retireType = redis.call('TYPE', KEYS[4]).ok
local plannerType = redis.call('TYPE', KEYS[5]).ok
if ledgerType ~= 'hash' or
   (membersType ~= 'zset' and membersType ~= 'none') or
   rootType ~= 'hash' or
   (retireType ~= 'zset' and retireType ~= 'none') or
   plannerType ~= 'hash' then
  return redis.error_reply('invalid coverage retirement key type') end
if redis.call('HGET', KEYS[1], 'parent_id') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'epoch') ~= ARGV[2] or
   redis.call('HGET', KEYS[1], 'root') ~= ARGV[3] or
   redis.call('HGET', KEYS[1], 'coverage_protocol') ~= '1' or
   redis.call('HGET', KEYS[5], 'id') ~= ARGV[1] or
   redis.call('HGET', KEYS[5], 'tracked_epoch') ~= ARGV[2] or
   redis.call('HGET', KEYS[5], 'path') ~= ARGV[3] or
   redis.call('HGET', KEYS[5], 'coverage_protocol') ~= '1' then
  return redis.error_reply('coverage retirement identity changed') end
local state = redis.call('HGET', KEYS[1], 'state')
if state == 'drained' then
  if redis.call('TTL', KEYS[5]) == -1 then redis.call('EXPIRE', KEYS[5], ARGV[6]) end
  redis.call('HDEL', KEYS[3], 'receipt:' .. ARGV[1], 'committed:' .. ARGV[1])
  redis.call('EXPIRE', KEYS[1], ARGV[6])
  if membersType == 'zset' then redis.call('EXPIRE', KEYS[2], ARGV[6]) end
  redis.call('ZREM', KEYS[4], ARGV[4])
  return 1
end
if state ~= 'open' and state ~= 'draining' then
  return redis.error_reply('invalid coverage retirement state') end
if redis.call('HGET', KEYS[3], 'committed:' .. ARGV[1]) ~= '1' then
  redis.call('ZADD', KEYS[4], ARGV[5], ARGV[4])
  return 0
end
if redis.call('HGET', KEYS[1], 'committed') ~= '1' then
  redis.call('HSET', KEYS[1], 'committed', '1')
end
local openCount = tonumber(redis.call('HGET', KEYS[1], 'open_count'))
if not openCount or openCount < 0 then
  return redis.error_reply('invalid coverage open count') end
local memberOpenCount = redis.call('ZCOUNT', KEYS[2], 0, 0)
if openCount > memberOpenCount then
  redis.call('HSET', KEYS[1], 'last_error', 'open_count exceeds durable open membership')
  redis.call('ZADD', KEYS[4], ARGV[5], ARGV[4])
  return 0
end
if openCount < memberOpenCount then
  redis.call('HSET', KEYS[1], 'open_count', memberOpenCount)
  openCount = memberOpenCount
end
local rootReceipt = redis.call('HGET', KEYS[3], 'receipt:' .. ARGV[1])
local ackToken = redis.call('HGET', KEYS[5], 'ack_token')
local ackOwner = redis.call('HGET', KEYS[5], 'ack_owner')
local ackGeneration = redis.call('HGET', KEYS[5], 'ack_generation')
local claimGeneration = redis.call('HGET', KEYS[5], 'claim_generation')
local plannerAckProof = redis.call('HGET', KEYS[5], 'status') == 'dispatched' and
  rootReceipt and rootReceipt == redis.call('HGET', KEYS[5], 'coverage_receipt') and
  ackToken and ackToken ~= '' and ackOwner and ackOwner ~= '' and
  ackGeneration and ackGeneration ~= '' and ackGeneration == claimGeneration
if redis.call('HGET', KEYS[1], 'planner_ack') ~= '1' then
  if not plannerAckProof then
    redis.call('ZADD', KEYS[4], ARGV[5], ARGV[4])
    return 0
  end
  redis.call('HSET', KEYS[1], 'planner_ack', '1')
end
if openCount ~= 0 then
  redis.call('ZADD', KEYS[4], ARGV[5], ARGV[4])
  return 0
end
if not plannerAckProof then
  return redis.error_reply('coverage planner has no durable acknowledgement') end
local batch = redis.call('ZRANGE', KEYS[2], 0, tonumber(ARGV[7]) - 1)
for _, id in ipairs(batch) do
  if redis.call('ZSCORE', KEYS[2], id) ~= '1' then
    return redis.error_reply('unsettled coverage member in retirement batch') end
  local statusKey = 'juicefarm:job:' .. id
  local statusType = redis.call('TYPE', statusKey).ok
  if statusType ~= 'hash' and statusType ~= 'none' then
    return redis.error_reply('invalid coverage member status key type') end
  if statusType == 'hash' then
    local memberParent = redis.call('HGET', statusKey, 'coverage_parent_id')
    if not memberParent or memberParent == '' then
      memberParent = redis.call('HGET', statusKey, 'parent_id') end
    if redis.call('HGET', statusKey, 'tracked_epoch') ~= ARGV[2] or
       memberParent ~= ARGV[1] then
      return redis.error_reply('coverage member status identity changed') end
  end
end
if state == 'open' then redis.call('HSET', KEYS[1], 'state', 'draining') end
for _, id in ipairs(batch) do
  local statusKey = 'juicefarm:job:' .. id
  if redis.call('TTL', statusKey) == -1 then redis.call('EXPIRE', statusKey, ARGV[6]) end
  redis.call('ZREM', KEYS[2], id)
  redis.call('HDEL', KEYS[1], 'settling:' .. id)
end
if redis.call('ZCARD', KEYS[2]) == 0 then
  if redis.call('TTL', KEYS[5]) == -1 then redis.call('EXPIRE', KEYS[5], ARGV[6]) end
  redis.call('HSET', KEYS[1], 'state', 'drained')
  redis.call('HDEL', KEYS[3], 'receipt:' .. ARGV[1], 'committed:' .. ARGV[1])
  redis.call('EXPIRE', KEYS[1], ARGV[6])
  if membersType == 'zset' then redis.call('EXPIRE', KEYS[2], ARGV[6]) end
  redis.call('ZREM', KEYS[4], ARGV[4])
else
  redis.call('ZADD', KEYS[4], ARGV[5], ARGV[4])
end
if #batch == 0 then return 1 end
return #batch`
	n, err := c.rdb.Eval(ctx, script, []string{
		coverageLedgerKey(parent), coverageMembersKey(parent), watchEpochKey(root),
		watchRetireKey, JobHashPrefix + parent,
	}, parent, epoch, root, coverageRetirePrefix+parent,
		now.Add(time.Minute).Unix(), int64(JobTTL/time.Second), budget).Int()
	return n, err
}

func (c *Client) RetireTrackedTerminals(ctx context.Context, now time.Time, limit int64) (int, error) {
	if limit <= 0 || limit > 4096 {
		limit = 4096
	}
	ids, err := c.rdb.ZRangeByScore(ctx, watchRetireKey, &redis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(now.Unix(), 10), Count: limit,
	}).Result()
	if err != nil {
		return 0, err
	}
	retired := 0
	for _, id := range ids {
		if retired >= int(limit) {
			break
		}
		if strings.HasPrefix(id, coverageRetirePrefix) {
			n, err := c.retireCoverageLedgerOnce(ctx, strings.TrimPrefix(id, coverageRetirePrefix), now, int(limit)-retired)
			if err != nil {
				return retired, err
			}
			retired += n
			continue
		}
		done, err := c.retireTrackedTerminalOnce(ctx, id, now)
		if err != nil {
			return retired, err
		}
		if done {
			retired++
		}
	}
	return retired, nil
}

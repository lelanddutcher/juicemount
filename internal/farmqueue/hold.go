package farmqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// StatusBlocked retains unresolved publication custody. It is neither a
// completed job nor an ordinary failed job that can be copied to a new ID.
const StatusBlocked = "blocked"

const holdClaimScript = `
local expected = {'hash', 'list', 'zset', 'set', 'zset', 'set'}
for i = 1, 6 do
  local got = redis.call('TYPE', KEYS[i]).ok
  if got ~= expected[i] and (got ~= 'none' or i == 1) then
    return redis.error_reply('invalid hold custody key type') end
end
local held = redis.call('HGET', KEYS[1], 'held_token')
if held then
  if held ~= ARGV[1] or redis.call('HGET', KEYS[1], 'held_owner') ~= ARGV[2] or
     redis.call('HGET', KEYS[1], 'held_raw') ~= ARGV[3] or
     redis.call('HGET', KEYS[1], 'held_generation') ~= ARGV[4] then return 0 end
else
  local tokenField, expiryField = 'delivery_token', 'delivery_expires_unix'
  if ARGV[7] ~= '' then tokenField, expiryField = 'claim_token', 'lease_expires_unix' end
  local now = tonumber(redis.call('TIME')[1])
  if redis.call('HGET', KEYS[1], tokenField) ~= ARGV[1] or
     redis.call('HGET', KEYS[1], 'lease_owner') ~= ARGV[2] or
     redis.call('HGET', KEYS[1], 'status') ~= 'running' or
     tonumber(redis.call('HGET', KEYS[1], expiryField) or '0') <= now or
     not redis.call('LPOS', KEYS[2], ARGV[3]) then return 0 end
  if ARGV[7] ~= '' and
     (redis.call('HGET', KEYS[1], 'claim_generation') ~= ARGV[4] or
      redis.call('HGET', KEYS[1], 'claim_phase') ~= 'running') then return 0 end
  local display = cjson.decode(ARGV[10])
  if display.id ~= ARGV[6] then return redis.error_reply('invalid hold display identity') end
  -- Persist display identity and complete custody in one write. Legacy hashes
  -- can be sparse after their original status expired during a long backlog.
  local fields = {'status', 'blocked', 'error', ARGV[5], 'payload', ARGV[3],
    'held_raw', ARGV[3], 'held_token', ARGV[1], 'held_owner', ARGV[2],
    'held_generation', ARGV[4], 'held_at', ARGV[8]}
  for k, v in pairs(display) do table.insert(fields, k); table.insert(fields, v) end
  -- A later script error must not leave a held receipt on its old expiry.
  redis.call('PERSIST', KEYS[1])
  redis.call('HSET', KEYS[1], unpack(fields))
end
redis.call('PERSIST', KEYS[1])
redis.call('ZADD', KEYS[5], ARGV[9], ARGV[6])
redis.call('LREM', KEYS[2], 1, ARGV[3])
redis.call('ZREM', KEYS[3], ARGV[6])
redis.call('HDEL', KEYS[1], 'lease_owner', 'lease_expires_at', 'lease_expires_unix',
  'claim_token', 'claim_phase', 'delivery_token', 'delivery_expires_unix')
if redis.call('LLEN', KEYS[2]) == 0 then redis.call('SREM', KEYS[4], ARGV[2]) end
if ARGV[7] ~= '' then redis.call('SREM', KEYS[6], ARGV[6]) end
return 1`

// HoldClaim preserves the same identity and raw payload without publishing any
// new work or retiring tracked coverage. Only an explicit future reconciliation
// with proof of fallback fulfillment may resolve this state.
func (c *Client) HoldClaim(ctx context.Context, claim Claim, reason string) error {
	if claim.Token == "" || claim.WorkerID == "" || claim.Raw == "" || strings.TrimSpace(reason) == "" ||
		(claim.Job.TrackedEpoch != "" && (claim.Job.ParentID == "" || claim.Generation < 1)) {
		return fmt.Errorf("%w: invalid hold claim", ErrClaimLost)
	}
	var saved Job
	if json.Unmarshal([]byte(claim.Raw), &saved) != nil || saved.ID == "" || saved.ID != claim.Job.ID ||
		saved.TrackedEpoch != claim.Job.TrackedEpoch || saved.ParentID != claim.Job.ParentID {
		return fmt.Errorf("%w: hold payload identity mismatch", ErrClaimLost)
	}
	display, err := json.Marshal(map[string]string{
		"id": saved.ID, "path": saved.Path, "kinds": strings.Join(saved.Kinds, ","),
		"producer": saved.Producer, "enqueued_at": saved.EnqueuedAt, "backend": saved.SelectedBackend,
		"target_worker": saved.SelectedWorker, "queue_class": saved.QueueClass,
		"parent_id": saved.ParentID, "coverage_parent_id": saved.CoverageParentID,
		"derivative_pass": saved.DerivativePass, "requeue_of": saved.RequeueOf,
	})
	if err != nil {
		return err
	}
	n, err := c.rdb.Eval(ctx, holdClaimScript, []string{JobHashPrefix + saved.ID,
		claimProcessingKey(claim), LeaseIndexKey, claimProcessingIndex(claim), JobIndexKey, trackedCustodyIndexKey},
		claim.Token, claim.WorkerID, claim.Raw, claim.Generation, reason, saved.ID,
		saved.TrackedEpoch, nowISO(), time.Now().UnixMicro(), display).Int()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: job %s cannot be held", ErrClaimLost, saved.ID)
	}
	return nil
}

// updateUnheldJob keeps legacy ID-only status writers from overwriting custody
// retained by HoldClaim. The check and update must share a Redis operation.
func (c *Client) updateUnheldJob(ctx context.Context, id string, fields map[string]any) error {
	raw, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	const script = `
if redis.call('HEXISTS', KEYS[1], 'held_token') == 1 or
   (redis.call('HGET', KEYS[1], 'tracked_epoch') or '') ~= '' then return 0 end
local fields = cjson.decode(ARGV[1])
for k, v in pairs(fields) do redis.call('HSET', KEYS[1], k, tostring(v)) end
return 1`
	n, err := c.rdb.Eval(ctx, script, []string{JobHashPrefix + id}, raw).Int()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: job %s is held", ErrClaimLost, id)
	}
	return nil
}

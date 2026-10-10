package respproxy

import (
	"math"
	"strconv"
)

// This snapshot is advisory for upload admission. ARM and rename still verify
// authority at publication. Decimal strings preserve all signed-int64 epochs.
const checkReplacementHistoryLua = `
local function decimal(s)
 return s and string.match(s, '^%d+$') and (#s == 1 or string.sub(s,1,1) ~= '0') and
 (#s < 19 or (#s == 19 and s <= '9223372036854775807'))
end
local function greater(a,b) return #a > #b or (#a == #b and a > b) end
if redis.call('GET',KEYS[1]) ~= ARGV[1] then return redis.error_reply('replacement singleton changed') end
if redis.call('GET',KEYS[2]) ~= ARGV[3] or
 redis.call('HGET',KEYS[3],'epoch') ~= ARGV[3] or
 redis.call('HGET',KEYS[3],'holder') ~= ARGV[2] then
 return redis.error_reply('replacement lease changed') end
local history=redis.call('HGETALL',KEYS[3])
local foreign='0'
local own=false
for i=1,#history,2 do
 local key,value=history[i],history[i+1]
 if string.sub(key,1,2) == 'e:' then
  if #key == 2 or not decimal(value) or value == '0' or greater(value,ARGV[3]) then
   return redis.error_reply('replacement malformed history') end
  if key == 'e:'..ARGV[2] then
   if value ~= ARGV[3] then return redis.error_reply('replacement caller history changed') end
   own=true
  elseif greater(value,foreign) then foreign=value end
 end
end
if not own then return redis.error_reply('replacement caller history missing') end
local status='CLEAN'
if greater(foreign,ARGV[4]) then status='FOREIGN' end
return {status,foreign}
`

// checkReplacementHistory is only routed by the authenticated enforcing
// singleton connection. It never grants, renews, clears or publishes anything.
func (e *LeaseEngine) checkReplacementHistory(p Principal, db int, proxy string, args [][]byte) []byte {
	if p.Kind != KindMac || db != e.cfg.VolumeDB || proxy == "" || len(args) != 3 {
		return errFrame("replacement history requires the volume's Mac writer and project/accepted/current epochs")
	}
	acceptedText, currentText := string(args[1]), string(args[2])
	accepted, err := strconv.ParseInt(acceptedText, 10, 64)
	current, ok := persistedLeaseEpoch(currentText)
	if err != nil || accepted < 0 || strconv.FormatInt(accepted, 10) != acceptedText || !ok || uint64(accepted) > current {
		return errFrame("invalid replacement history epochs")
	}
	e.topo.RLock()
	defer e.topo.RUnlock()
	project, err := e.resolveProj(string(args[0]))
	if err != nil || project <= 1 || project > math.MaxInt64 {
		return errFrame("replacement project unavailable")
	}
	fence := e.fence(project)
	fence.RLock()
	defer fence.RUnlock()
	e.mu.Lock()
	lease := e.leases[project]
	valid := lease != nil && !lease.unresolved && lease.held() && lease.holder == p.Name && lease.epoch == current
	e.mu.Unlock()
	if !valid {
		return errFrame("replacement current lease unavailable")
	}
	v, err := e.replacementRead(db, "EVAL_RO", checkReplacementHistoryLua, "3", SingletonKey, epochKey(project), leaseKey(project), proxy, p.Name, currentText, acceptedText)
	if err != nil || v.Type != '*' || len(v.Elems) != 2 || v.Elems[0].Type != '$' || v.Elems[0].Null || v.Elems[1].Type != '$' || v.Elems[1].Null {
		return errFrame("replacement history unavailable")
	}
	status, foreignText := string(v.Elems[0].Str), string(v.Elems[1].Str)
	foreign, err := strconv.ParseInt(foreignText, 10, 64)
	if err != nil || foreign < 0 || strconv.FormatInt(foreign, 10) != foreignText || uint64(foreign) > current ||
		(status != "CLEAN" && status != "FOREIGN") || (status == "FOREIGN") != (foreign > accepted) {
		return errFrame("replacement history unconfirmed")
	}
	var reply respBuf
	reply.arr(4)
	reply.simple(status)
	reply.integer(int64(project))
	reply.integer(int64(current))
	reply.integer(foreign)
	return reply.Bytes()
}

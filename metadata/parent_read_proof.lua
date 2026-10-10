-- One bounded, atomic validation of a caller-supplied JuiceFS 1.3.1 path.
-- ARGV: format UUID, depth, then (name, decimal full-u64 inode, packed edge).
-- The packed edge is type byte + big-endian u64; no Lua floating-point inode
-- conversion is permitted. This returns evidence, not a serving lease.
local depth = tonumber(ARGV[2])
if not depth or depth < 1 or depth > 32 or #ARGV ~= 2 + 3*depth then
  return redis.error_reply('read proof: invalid depth')
end
local settingType = redis.call('TYPE', 'setting')
if settingType.ok ~= 'string' then
  return redis.error_reply('read proof: invalid format type')
end
local settingLen = redis.call('STRLEN', 'setting')
if settingLen == 0 or settingLen > 65536 then
  return redis.error_reply('read proof: missing or oversized format')
end
local setting = redis.call('GET', 'setting')
local ok, format = pcall(cjson.decode, setting)
if not ok or type(format) ~= 'table' or type(format.UUID) ~= 'string' or
    format.UUID ~= ARGV[1] then
  return redis.error_reply('read proof: format UUID mismatch')
end
local function attr(inode)
  local length = redis.call('STRLEN', 'i'..inode)
  if length ~= 72 and length ~= 80 then
    return nil
  end
  local value = redis.call('GET', 'i'..inode)
  if not value or (#value ~= 72 and #value ~= 80) then
    return nil
  end
  return value
end
local root = attr('1')
if not root then return redis.error_reply('read proof: missing root attr') end
local out = {'v1', redis.sha1hex(setting), root}
local parent = '1'
for i=1,depth do
  local base = 3 + 3*(i-1)
  local name, inode, packed = ARGV[base], ARGV[base+1], ARGV[base+2]
  if not name or #name == 0 or #name > 255 or not inode or not packed or #packed ~= 9 then
    return redis.error_reply('read proof: malformed edge request')
  end
  if redis.call('HSTRLEN', 'd'..parent, name) ~= 9 then
    return redis.error_reply('read proof: missing or oversized edge')
  end
  local edge = redis.call('HGET', 'd'..parent, name)
  if not edge or edge ~= packed then
    return redis.error_reply('read proof: edge mismatch')
  end
  local value = attr(inode)
  if not value then return redis.error_reply('read proof: missing child attr') end
  out[#out+1] = edge
  out[#out+1] = value
  parent = inode
end
return out

package respproxy

// Replacement enrollment seals a staged inode under authenticated writer
// authority. Ordinary file xattrs are deliberately not part of this authority.
import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/juicedata/juicefs/pkg/replaceguard"
)

// All predicates precede the sole mutation. Decimal epoch comparison must not
// pass through Lua doubles (epochs can exceed 2^53).
const armReplacementLua = `
local function decimal(s)
 return s and string.match(s, '^%d+$') and (#s == 1 or string.sub(s,1,1) ~= '0') and
 (#s < 19 or (#s == 19 and s <= '9223372036854775807'))
end
local function greater(a,b) return #a > #b or (#a == #b and a > b) end
local birth=redis.call('GET',KEYS[8])
local operation=redis.call('GET',KEYS[9])
if ARGV[11] == '' then
 if birth or operation then return redis.error_reply('replacement birth appeared') end
elseif birth ~= ARGV[11] or operation ~= ARGV[11] then
 return redis.error_reply('replacement birth changed')
end
if redis.call('GET',KEYS[2]) ~= ARGV[2] then return redis.error_reply('replacement singleton changed') end
if redis.call('HGET',KEYS[3],'holder') ~= ARGV[3] or
 redis.call('HGET',KEYS[3],'epoch') ~= ARGV[4] or redis.call('GET',KEYS[4]) ~= ARGV[4] then
 return redis.error_reply('replacement lease changed') end
local h=redis.call('HGETALL',KEYS[3])
for i=1,#h,2 do
 if string.sub(h[i],1,2) == 'e:' then
  if not decimal(h[i+1]) or greater(h[i+1],ARGV[4]) then return redis.error_reply('replacement malformed history') end
  if h[i] ~= 'e:'..ARGV[3] and greater(h[i+1],ARGV[5]) then return redis.error_reply('replacement foreign writer') end
 end
end
if redis.call('GET',KEYS[5]) ~= ARGV[6] or redis.call('HGET',KEYS[6],ARGV[7]) ~= ARGV[8] or
 redis.call('GET',KEYS[7]) ~= ARGV[9] then return redis.error_reply('replacement stage changed') end
local old=redis.call('GET',KEYS[1])
if ARGV[10] ~= '' and old ~= ARGV[10] then
 return redis.error_reply('replacement expected enrollment changed')
end
if old then
 if old == ARGV[1] then return old end
 if ARGV[10] == '' or old ~= ARGV[10] then return redis.error_reply('replacement enrollment changed') end
elseif ARGV[10] ~= '' then
 return redis.error_reply('replacement enrollment disappeared')
end
redis.call('SET',KEYS[1],ARGV[1])
return ARGV[1]
`

func replacementDeny(detail string) *Denial {
	return &Denial{Reason: ReasonLeaseHeld, Detail: "protected replacement: " + detail}
}

func (e *LeaseEngine) replacementRead(db int, args ...string) (Value, error) {
	v, err := e.cfg.Side.Do(db, strArgs(args...)...)
	if err == nil {
		err = v.Err()
	}
	return v, err
}

// ARM_REPLACE takes topology WRITE so even an observe-mode denied unit (which
// keeps topology custody but releases project fences) has finished upstream.
// This rare enrollment can briefly delay other projects; normal writes retain
// the existing read fences and reply/confirmed-kill lifetime.
func (e *LeaseEngine) armReplacement(p Principal, db int, proxy string, raw, required []byte) []byte {
	if p.Kind != KindMac || db != e.cfg.VolumeDB || proxy == "" {
		return replacementDeny("enrollment requires the volume's Mac writer").Reply()
	}
	r, err := replaceguard.Decode(raw)
	if err != nil || r.Phase != "prepared" {
		return errFrame("invalid prepared replacement")
	}
	r.Owner, r.Proxy = p.Name, proxy // authenticated connection, never request identity
	raw, err = replaceguard.Encode(r)
	if err != nil {
		return errFrame("invalid replacement")
	}
	// A recovered armed operation must name its exact prior record. Absence
	// must never be interpreted as initial enrollment, even if it occurs
	// before our first backend read. Only current authority may advance.
	if required != nil {
		old, err := replaceguard.Decode(required)
		if err != nil || old.Phase != "prepared" || old.Owner != p.Name || old.Epoch > r.Epoch {
			return replacementDeny("invalid expected enrollment").Reply()
		}
		old.Proxy, old.Epoch = r.Proxy, r.Epoch
		wanted, err := replaceguard.Encode(old)
		if err != nil || !bytes.Equal(wanted, raw) {
			return replacementDeny("expected enrollment evidence differs").Reply()
		}
	}
	e.topo.Lock()
	defer e.topo.Unlock()
	f := e.fence(r.Project)
	f.Lock()
	defer f.Unlock()
	e.mu.Lock()
	l := e.leases[r.Project]
	valid := l != nil && !l.unresolved && l.held() && l.holder == p.Name && l.epoch == r.Epoch
	e.mu.Unlock()
	if !valid {
		return replacementDeny("current lease unavailable").Reply()
	}
	loc, err := e.anc.resolveDir(r.Stage.Parent)
	if err != nil || loc.kind != nsProject || loc.proj != r.Project || len(r.Parents) <= e.anc.depth || r.Parents[e.anc.depth].Inode != r.Project {
		return replacementDeny("stage project differs").Reply()
	}
	// A restart may change the proxy incarnation or the owner's current lease.
	// Only those two authority fields can be refreshed. The original accepted
	// epoch, namespace, image and graph remain immutable, even when the current
	// owner has since acquired a newer tenure. A committed/partial publication
	// is never inferred from a missing private name or re-armed as a new save.
	previous, err := e.replacementRead(db, "GET", replaceguard.Key(r.Stage.Inode))
	if err != nil || previous.Type != '$' {
		return replacementDeny("existing enrollment unavailable").Reply()
	}
	if required != nil && (previous.Null || !bytes.Equal(previous.Str, required)) {
		return replacementDeny("expected enrollment unavailable").Reply()
	}
	var expected []byte
	if !previous.Null {
		old, err := replaceguard.Decode(previous.Str)
		if err != nil || old.Phase != "prepared" || old.Owner != p.Name || old.Epoch > r.Epoch {
			return replacementDeny("existing enrollment cannot be refreshed").Reply()
		}
		old.Proxy, old.Epoch = r.Proxy, r.Epoch
		refreshed, err := replaceguard.Encode(old)
		if err != nil || !bytes.Equal(refreshed, raw) {
			return replacementDeny("original replacement evidence differs").Reply()
		}
		expected = previous.Str
	}
	birth, err := e.birthForArm(db, r)
	if err != nil {
		return replacementDeny(err.Error()).Reply()
	}
	stage, err := e.replacementRead(db, "GET", "i"+inoStr(r.Stage.Inode))
	if err != nil || stage.Null || !replacementAttr(stage.Str, r.Stage) {
		return replacementDeny("stage identity differs").Reply()
	}
	setting, err := e.replacementRead(db, "GET", "setting")
	var volume struct{ UUID string }
	if err != nil || setting.Null || len(setting.Str) > 1<<20 || json.Unmarshal(setting.Str, &volume) != nil || volume.UUID != r.BackendUUID {
		return replacementDeny("backend UUID differs").Reply()
	}
	entry := append([]byte{typeFile}, make([]byte, 8)...)
	binary.BigEndian.PutUint64(entry[1:], r.Stage.Inode)
	v, err := e.replacementRead(db, "EVAL", armReplacementLua, "9",
		replaceguard.Key(r.Stage.Inode), SingletonKey, leaseKey(r.Project), epochKey(r.Project),
		"i"+inoStr(r.Stage.Inode), "d"+inoStr(r.Stage.Parent), "setting",
		replaceguard.BirthInodeKey(r.Stage.Inode), replaceguard.BirthOperationKey(r.OperationID),
		string(raw), proxy, p.Name, inoStr(r.Epoch), inoStr(r.AcceptedEpoch), string(stage.Str),
		r.Stage.Name, string(entry), string(setting.Str), string(expected), string(birth))
	if err != nil || v.Type != '$' || v.Null || !bytes.Equal(v.Str, raw) {
		return errFrame("replacement enrollment unconfirmed; retain operation and stage")
	}
	var out respBuf
	out.bulk(string(raw))
	return out.Bytes()
}

// Decode only the stable wire fields needed here. Reject truncated attributes
// before indexing; full Attr.Unmarshal intentionally accepts older formats.
func replacementAttr(raw []byte, f replaceguard.File) bool {
	if len(raw) != 72 && len(raw) != 80 {
		return false
	}
	ns := func(at int) (int64, bool) {
		sec := int64(binary.BigEndian.Uint64(raw[at : at+8]))
		sub := binary.BigEndian.Uint32(raw[at+8 : at+12])
		if sec < 0 || sec > (int64(^uint64(0)>>1)-int64(sub))/1e9 || sub >= 1e9 {
			return 0, false
		}
		return sec*1e9 + int64(sub), true
	}
	mt, mok := ns(23)
	ct, cok := ns(35)
	return raw[1]>>4 == typeFile && binary.BigEndian.Uint32(raw[47:51]) == 1 &&
		binary.BigEndian.Uint64(raw[51:59]) == f.Length && binary.BigEndian.Uint64(raw[63:71]) == f.Parent &&
		mok && cok && mt == f.MtimeNS && ct == f.CtimeNS
}

// Called AFTER LeaseEngine.Decide acquires topology custody, before EXEC can
// escape. Enrollment's topology write lock closes the no-record/read race.
func (c *conn) checkReplacements(u *Unit) *Denial {
	if u.DB != c.p.cfg.VolumeDB {
		return nil
	}
	if u.Kind == UnitTxn && len(u.Watched) > 0 && (u.beforeSide == nil || u.beforeSide() != nil) {
		return replacementDeny("WATCH unavailable")
	}
	ids := map[uint64]bool{}
	for _, w := range u.Writes {
		if strings.HasPrefix(w.Raw, "jmlease:v1:replace:") || strings.HasPrefix(w.Raw, "jmlease:v1:birth:") {
			return replacementDeny("client cannot alter operation record")
		}
		if w.Key.Ino != 0 {
			ids[w.Key.Ino] = true
		}
		if w.Key.Shape != ShapeDentry {
			continue
		}
		cm := u.Cmds[w.Cmd]
		var fields [][]byte
		switch cm.Info.Name {
		case "HSET", "HMSET", "HSETNX":
			if len(cm.Args) < 4 || len(cm.Args)%2 != 0 {
				return replacementDeny("malformed dentry write")
			}
			for j := 2; j+1 < len(cm.Args); j += 2 {
				fields = append(fields, cm.Args[j])
				n, ok := entryInode(cm.Args[j+1])
				if !ok {
					return replacementDeny("malformed new entry")
				}
				ids[n] = true
			}
		case "HDEL":
			fields = cm.Args[2:]
		case "DEL", "UNLINK":
			v, err := c.p.side.Do(u.DB, strArgs("HLEN", w.Raw)...)
			if err != nil || v.Err() != nil || v.Type != ':' || v.Int != 0 {
				return replacementDeny("whole nonempty directory mutation")
			}
		default:
			return replacementDeny("unsupported directory mutation")
		}
		for _, field := range fields {
			v, err := c.p.side.Do(u.DB, []byte("HGET"), []byte(w.Raw), field)
			if err != nil || v.Err() != nil {
				return replacementDeny("old entry unavailable")
			}
			if !v.Null {
				n, ok := entryInode(v.Str)
				if !ok {
					return replacementDeny("malformed old entry")
				}
				ids[n] = true
			}
		}
	}
	for _, n := range u.RefInodes {
		ids[n] = true
	}
	if denied := c.checkStageBirth(u, ids); denied != nil {
		return denied
	}
	// Most units touch only a handful of inodes; pipeline-sized units already
	// have protocol bounds. Never enumerate the volume to find a stage.
	var found *replaceguard.Record
	for n := range ids {
		v, err := c.p.side.Do(u.DB, strArgs("GET", replaceguard.Key(n))...)
		if err != nil || v.Err() != nil {
			return replacementDeny("operation lookup unavailable")
		}
		if v.Null {
			continue
		}
		r, err := replaceguard.Decode(v.Str)
		if err != nil || r.Stage.Inode != n {
			return replacementDeny("malformed operation")
		}
		if r.Phase == "committed" {
			continue
		}
		if found != nil {
			return replacementDeny("multiple sealed stages in one unit")
		}
		found = &r
	}
	if found == nil {
		return nil
	}
	r := *found
	if u.Kind != UnitTxn || r.Owner != c.principal.Name || r.Proxy != c.p.Incarnation() || c.p.fenced() {
		return replacementDeny("sealed stage is not an ordinary write")
	}
	watched := map[string]bool{}
	for _, k := range u.Watched {
		watched[k] = true
	}
	for _, k := range replaceguard.RequiredKeys(r) {
		if !watched[k] {
			return replacementDeny("missing publication WATCH")
		}
	}
	if u.beforeSide == nil || u.beforeSide() != nil {
		return replacementDeny("WATCH unconfirmed")
	}
	if err := c.replacementShape(u, r); err != nil {
		return replacementDeny(err.Error())
	}
	// doRename validates the complete original graph and namespace under these
	// WATCHes. The proxy additionally ties publication to the authenticated owner
	// and exact authority, including tenures older than the owner's latest grant.
	if err := c.replacementAuthority(r); err != nil {
		return replacementDeny(err.Error())
	}
	r.Phase = "committed"
	raw, err := replaceguard.Encode(r)
	if err != nil {
		return replacementDeny("receipt encoding")
	}
	u.replacementReceipt, err = c.replacementCommit(u, *found, raw)
	if err != nil {
		return replacementDeny(err.Error())
	}
	return nil
}

func (c *conn) replacementAuthority(r replaceguard.Record) error {
	read := func(args ...string) (Value, error) {
		v, e := c.p.side.Do(c.db, strArgs(args...)...)
		if e == nil {
			e = v.Err()
		}
		return v, e
	}
	v, e := read("GET", SingletonKey)
	if e != nil || v.Null || string(v.Str) != r.Proxy {
		return errors.New("singleton changed")
	}
	v, e = read("GET", epochKey(r.Project))
	if e != nil || v.Null || string(v.Str) != inoStr(r.Epoch) {
		return errors.New("epoch changed")
	}
	v, e = read("HGETALL", leaseKey(r.Project))
	if e != nil || v.Type != '*' || len(v.Elems)%2 != 0 {
		return errors.New("lease unavailable")
	}
	h := map[string]string{}
	for i := 0; i < len(v.Elems); i += 2 {
		h[string(v.Elems[i].Str)] = string(v.Elems[i+1].Str)
	}
	if h["holder"] != r.Owner || h["epoch"] != inoStr(r.Epoch) {
		return errors.New("holder changed")
	}
	for k, val := range h {
		if strings.HasPrefix(k, "e:") {
			n, err := strconv.ParseUint(val, 10, 63)
			if err != nil || inoStr(n) != val || n > r.Epoch || k != "e:"+r.Owner && n > r.AcceptedEpoch {
				return errors.New("foreign or malformed writer history")
			}
		}
	}
	return nil
}

func (c *conn) replacementShape(u *Unit, r replaceguard.Record) error {
	parentKey, inodeKey := "d"+inoStr(r.Stage.Parent), "i"+inoStr(r.Stage.Inode)
	sourceDeleted, destSet, stageSet := 0, 0, 0
	for _, cm := range u.Cmds {
		for _, k := range cm.Info.Keys {
			if k.Ino != r.Stage.Inode {
				continue
			}
			if k.Shape != ShapeInode || cm.Info.Name != "SET" || len(cm.Args) != 3 || string(cm.Args[1]) != inodeKey {
				return errors.New("sealed stage mutation")
			}
			next := append([]byte(nil), cm.Args[2]...)
			if len(next) != 72 && len(next) != 80 {
				return errors.New("malformed stage attr")
			}
			// Rename changes ctime only; compare all other bytes against the sealed
			// stage read under its required WATCH, including its content length.
			old, err := c.p.side.Do(u.DB, strArgs("GET", inodeKey)...)
			if err != nil || old.Err() != nil || old.Null || !replacementAttr(old.Str, r.Stage) || len(old.Str) != len(next) {
				return errors.New("sealed stage differs")
			}
			copy(next[35:47], old.Str[35:47])
			if !bytes.Equal(next, old.Str) {
				return errors.New("rename changes sealed image")
			}
			stageSet++
		}
		if len(cm.Args) < 2 || string(cm.Args[1]) != parentKey {
			continue
		}
		switch cm.Info.Name {
		case "HDEL":
			for _, name := range cm.Args[2:] {
				if string(name) != r.Stage.Name {
					return errors.New("unexpected removed name")
				}
				sourceDeleted++
			}
		case "HSET":
			if len(cm.Args) != 4 || string(cm.Args[2]) != r.Destination.Name {
				return errors.New("unexpected installed name")
			}
			n, ok := entryInode(cm.Args[3])
			typ, tok := entryType(cm.Args[3])
			if !ok || !tok || n != r.Stage.Inode || typ != typeFile {
				return errors.New("installed inode differs")
			}
			destSet++
		default:
			return errors.New("unexpected parent mutation")
		}
	}
	if sourceDeleted != 1 || destSet != 1 || stageSet != 1 {
		return fmt.Errorf("incomplete rename %d/%d/%d", sourceDeleted, destSet, stageSet)
	}
	// Aliases anywhere else are forbidden, including a transaction that also
	// includes an otherwise valid replacement.
	for _, w := range u.Writes {
		if w.Key.Shape == ShapeDentry && w.Raw != parentKey {
			cm := u.Cmds[w.Cmd]
			if isHSet(cm.Info.Name) {
				for j := 3; j < len(cm.Args); j += 2 {
					n, ok := entryInode(cm.Args[j])
					if ok && n == r.Stage.Inode {
						return errors.New("stage alias")
					}
				}
			}
		}
	}
	return nil
}

// EXEC does not roll back earlier commands when a later command fails. Check
// the selected transaction's actual postconditions before recording success;
// a missing/malformed reply or a partial transaction keeps the stage sealed.
const replacementCommitLua = `
if redis.call('GET',KEYS[1]) ~= ARGV[1] then return redis.error_reply('replacement record changed') end
for i=3,#ARGV,4 do
 local op,key,field,want=ARGV[i],ARGV[i+1],ARGV[i+2],ARGV[i+3]
 local got
 if op == 'GET' then got=redis.call('GET',key)
 elseif op == 'ABSENT' then got=tostring(redis.call('EXISTS',key))
 elseif op == 'HABSENT' then got=tostring(redis.call('HEXISTS',key,field))
 elseif op == 'SISMEMBER' then got=tostring(redis.call(op,key,field))
 elseif op == 'ZSCORE' then
  got=redis.call(op,key,field)
  if got and tonumber(got) == tonumber(want) then got=want end
 else got=redis.call(op,key,field) end
 if got ~= want then return redis.error_reply('replacement postcondition failed') end
end
return redis.call('SET',KEYS[1],ARGV[2])
`

func (c *conn) replacementCommit(u *Unit, r replaceguard.Record, committed []byte) ([][]byte, error) {
	prepared, err := replaceguard.Encode(r)
	if err != nil {
		return nil, err
	}
	args := strArgs("EVAL", replacementCommitLua, "1", replaceguard.Key(r.Stage.Inode), string(prepared), string(committed))
	add := func(op, key, field, want string) { args = append(args, strArgs(op, key, field, want)...) }
	watched := map[string]bool{}
	for _, k := range u.Watched {
		watched[k] = true
	}
	for _, cm := range u.Cmds {
		a := cm.Args
		switch cm.Info.Name {
		case "SET":
			if len(a) != 3 {
				return nil, errors.New("unsupported replacement SET")
			}
			add("GET", string(a[1]), "", string(a[2]))
		case "DEL":
			for _, key := range a[1:] {
				add("ABSENT", string(key), "", "0")
			}
		case "HSET":
			if len(a) < 4 || len(a)%2 != 0 {
				return nil, errors.New("unsupported replacement HSET")
			}
			for j := 2; j+1 < len(a); j += 2 {
				add("HGET", string(a[1]), string(a[j]), string(a[j+1]))
			}
		case "HDEL":
			for _, field := range a[2:] {
				add("HABSENT", string(a[1]), string(field), "0")
			}
		case "SADD":
			for _, member := range a[2:] {
				add("SISMEMBER", string(a[1]), string(member), "1")
			}
		case "ZADD":
			if len(a) != 4 {
				return nil, errors.New("unsupported replacement ZADD")
			}
			add("ZSCORE", string(a[1]), string(a[3]), string(a[2]))
		case "INCRBY", "DECR":
			if len(a) < 2 || !watched[string(a[1])] {
				return nil, errors.New("replacement counter not watched")
			}
			delta := int64(-1)
			if cm.Info.Name == "INCRBY" {
				var ok bool
				if len(a) != 3 {
					return nil, errors.New("bad increment")
				}
				delta, ok = RedisInt(a[2])
				if !ok {
					return nil, errors.New("bad delta")
				}
			}
			v, e := c.p.side.Do(u.DB, strArgs("GET", string(a[1]))...)
			if e != nil || v.Err() != nil || v.Null {
				return nil, errors.New("counter unavailable")
			}
			value, ok := RedisInt(v.Str)
			if !ok || delta > 0 && value > math.MaxInt64-delta || delta < 0 && value < math.MinInt64-delta {
				return nil, errors.New("counter overflow")
			}
			add("GET", string(a[1]), "", strconv.FormatInt(value+delta, 10))
		default:
			return nil, errors.New("unsupported protected rename command")
		}
	}
	return args, nil
}

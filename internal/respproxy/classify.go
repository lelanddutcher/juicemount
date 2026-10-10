package respproxy

import (
	"crypto/sha1"
	"encoding/hex"
	"strconv"
	"strings"
)

// Class is the coarse category of one command.
type Class string

const (
	ClassRead      Class = "read"
	ClassWrite     Class = "write"
	ClassTxn       Class = "txn"     // MULTI/EXEC/DISCARD/WATCH/UNWATCH
	ClassConn      Class = "conn"    // HELLO/AUTH/SELECT/CLIENT/PING/QUIT/...
	ClassPubSub    Class = "pubsub"  // SUBSCRIBE family (reader side)
	ClassPublish   Class = "publish" // PUBLISH: writes a channel, not a key
	ClassScript    Class = "script"  // EVAL/EVALSHA/SCRIPT LOAD of a body NOT on the allow-list
	ClassAdmin     Class = "admin"   // read-only server introspection (INFO, DBSIZE, SCRIPT LOAD)
	ClassAdminW    Class = "admin-write"
	ClassForbidden Class = "forbidden" // breaks the proxy's reply accounting or auth: refused for everyone
	ClassLocal     Class = "local"     // JM.* — served by the proxy, never forwarded
	ClassUnknown   Class = "unknown"
)

// keySpec locates keys in argv. first/last are argv indexes; last<0 counts
// from the end (-1 = last arg). step>1 skips values (MSET).
type keySpec struct{ first, last, step int }

type cmdInfo struct {
	class Class
	keys  keySpec
}

var (
	k1     = keySpec{1, 1, 1}
	kAll   = keySpec{1, -1, 1}
	kPair  = keySpec{1, -1, 2}
	k12    = keySpec{1, 2, 1}
	kBlock = keySpec{1, -2, 1} // BLPOP k1 k2 ... timeout
	kNone  = keySpec{0, 0, 0}
)

var commands = map[string]cmdInfo{
	// reads
	"GET": {ClassRead, k1}, "MGET": {ClassRead, kAll}, "EXISTS": {ClassRead, kAll},
	"TYPE": {ClassRead, k1}, "TTL": {ClassRead, k1}, "PTTL": {ClassRead, k1},
	"EXPIRETIME": {ClassRead, k1}, "PEXPIRETIME": {ClassRead, k1},
	"STRLEN": {ClassRead, k1}, "GETRANGE": {ClassRead, k1}, "SUBSTR": {ClassRead, k1},
	"HGET": {ClassRead, k1}, "HMGET": {ClassRead, k1}, "HGETALL": {ClassRead, k1},
	"HEXISTS": {ClassRead, k1}, "HKEYS": {ClassRead, k1}, "HVALS": {ClassRead, k1},
	"HLEN": {ClassRead, k1}, "HSCAN": {ClassRead, k1}, "HSTRLEN": {ClassRead, k1},
	"HRANDFIELD": {ClassRead, k1}, "HTTL": {ClassRead, k1}, "HPTTL": {ClassRead, k1},
	"LRANGE": {ClassRead, k1}, "LLEN": {ClassRead, k1}, "LINDEX": {ClassRead, k1}, "LPOS": {ClassRead, k1},
	"SMEMBERS": {ClassRead, k1}, "SISMEMBER": {ClassRead, k1}, "SMISMEMBER": {ClassRead, k1},
	"SCARD": {ClassRead, k1}, "SSCAN": {ClassRead, k1}, "SRANDMEMBER": {ClassRead, k1},
	"SINTER": {ClassRead, kAll}, "SUNION": {ClassRead, kAll}, "SDIFF": {ClassRead, kAll},
	"ZRANGE": {ClassRead, k1}, "ZRANGEBYSCORE": {ClassRead, k1}, "ZREVRANGE": {ClassRead, k1},
	"ZREVRANGEBYSCORE": {ClassRead, k1}, "ZSCORE": {ClassRead, k1}, "ZMSCORE": {ClassRead, k1},
	"ZCARD": {ClassRead, k1}, "ZCOUNT": {ClassRead, k1}, "ZSCAN": {ClassRead, k1}, "ZRANK": {ClassRead, k1},
	"ZREVRANK": {ClassRead, k1}, "ZRANGEBYLEX": {ClassRead, k1}, "ZLEXCOUNT": {ClassRead, k1},
	"SCAN": {ClassRead, kNone}, "KEYS": {ClassRead, kNone}, "RANDOMKEY": {ClassRead, kNone},
	"DUMP": {ClassRead, k1}, "OBJECT": {ClassRead, kNone}, "MEMORY": {ClassRead, kNone},
	"DBSIZE": {ClassAdmin, kNone}, "LASTSAVE": {ClassAdmin, kNone}, "ROLE": {ClassAdmin, kNone},
	"WAIT": {ClassAdmin, kNone}, "WAITAOF": {ClassAdmin, kNone}, "LOLWUT": {ClassAdmin, kNone},

	// writes
	"SET": {ClassWrite, k1}, "SETNX": {ClassWrite, k1}, "SETEX": {ClassWrite, k1},
	"PSETEX": {ClassWrite, k1}, "MSET": {ClassWrite, kPair}, "MSETNX": {ClassWrite, kPair},
	"GETSET": {ClassWrite, k1}, "GETDEL": {ClassWrite, k1}, "GETEX": {ClassWrite, k1},
	"APPEND": {ClassWrite, k1}, "SETRANGE": {ClassWrite, k1},
	"INCR": {ClassWrite, k1}, "INCRBY": {ClassWrite, k1}, "DECR": {ClassWrite, k1},
	"DECRBY": {ClassWrite, k1}, "INCRBYFLOAT": {ClassWrite, k1},
	"DEL": {ClassWrite, kAll}, "UNLINK": {ClassWrite, kAll},
	"EXPIRE": {ClassWrite, k1}, "PEXPIRE": {ClassWrite, k1}, "EXPIREAT": {ClassWrite, k1},
	"PEXPIREAT": {ClassWrite, k1}, "PERSIST": {ClassWrite, k1},
	"RENAME": {ClassWrite, k12}, "RENAMENX": {ClassWrite, k12}, "COPY": {ClassWrite, k12},
	"HSET": {ClassWrite, k1}, "HSETNX": {ClassWrite, k1}, "HMSET": {ClassWrite, k1},
	"HDEL": {ClassWrite, k1}, "HINCRBY": {ClassWrite, k1}, "HINCRBYFLOAT": {ClassWrite, k1},
	"HEXPIRE": {ClassWrite, k1}, "HPEXPIRE": {ClassWrite, k1}, "HPERSIST": {ClassWrite, k1},
	"RPUSH": {ClassWrite, k1}, "LPUSH": {ClassWrite, k1}, "RPUSHX": {ClassWrite, k1},
	"LPUSHX": {ClassWrite, k1}, "LTRIM": {ClassWrite, k1}, "LPOP": {ClassWrite, k1},
	"RPOP": {ClassWrite, k1}, "LSET": {ClassWrite, k1}, "LREM": {ClassWrite, k1},
	"LINSERT": {ClassWrite, k1}, "LMOVE": {ClassWrite, k12}, "RPOPLPUSH": {ClassWrite, k12},
	"BLMOVE": {ClassWrite, k12}, "BRPOPLPUSH": {ClassWrite, k12},
	"BLPOP": {ClassWrite, kBlock}, "BRPOP": {ClassWrite, kBlock},
	"SADD": {ClassWrite, k1}, "SREM": {ClassWrite, k1}, "SPOP": {ClassWrite, k1}, "SMOVE": {ClassWrite, k12},
	"ZADD": {ClassWrite, k1}, "ZREM": {ClassWrite, k1}, "ZINCRBY": {ClassWrite, k1},
	"ZREMRANGEBYSCORE": {ClassWrite, k1}, "ZREMRANGEBYRANK": {ClassWrite, k1}, "ZREMRANGEBYLEX": {ClassWrite, k1},
	"ZPOPMIN": {ClassWrite, k1}, "ZPOPMAX": {ClassWrite, k1},
	"BZPOPMIN": {ClassWrite, kBlock}, "BZPOPMAX": {ClassWrite, kBlock},

	// admin writes: the command gate (plan §1.4 step 1)
	"FLUSHDB": {ClassAdminW, kNone}, "FLUSHALL": {ClassAdminW, kNone},
	"RESTORE": {ClassAdminW, kNone}, "MIGRATE": {ClassAdminW, kNone}, "MOVE": {ClassAdminW, kNone},
	"SWAPDB": {ClassAdminW, kNone}, "DEBUG": {ClassAdminW, kNone}, "SHUTDOWN": {ClassAdminW, kNone},
	"ACL": {ClassAdminW, kNone}, "SAVE": {ClassAdminW, kNone}, "BGSAVE": {ClassAdminW, kNone},
	"BGREWRITEAOF": {ClassAdminW, kNone}, "REPLICAOF": {ClassAdminW, kNone}, "SLAVEOF": {ClassAdminW, kNone},
	"MODULE": {ClassAdminW, kNone}, "FUNCTION": {ClassAdminW, kNone}, "FCALL": {ClassAdminW, kNone},
	"FCALL_RO": {ClassAdminW, kNone}, "SORT": {ClassAdminW, kNone}, "SORT_RO": {ClassRead, k1},
	"SLOWLOG": {ClassAdminW, kNone}, "LATENCY": {ClassAdminW, kNone}, "FAILOVER": {ClassAdminW, kNone},
	"SUNIONSTORE": {ClassAdminW, kNone}, "SINTERSTORE": {ClassAdminW, kNone}, "SDIFFSTORE": {ClassAdminW, kNone},
	"ZUNIONSTORE": {ClassAdminW, kNone}, "ZINTERSTORE": {ClassAdminW, kNone}, "ZDIFFSTORE": {ClassAdminW, kNone},
	"ZRANGESTORE": {ClassAdminW, kNone}, "GEOADD": {ClassAdminW, kNone}, "PFADD": {ClassAdminW, kNone},
	"XADD": {ClassAdminW, kNone}, "BITOP": {ClassAdminW, kNone}, "SETBIT": {ClassAdminW, kNone},

	// transactions
	"MULTI": {ClassTxn, kNone}, "EXEC": {ClassTxn, kNone}, "DISCARD": {ClassTxn, kNone},
	"WATCH": {ClassTxn, kAll}, "UNWATCH": {ClassTxn, kNone},

	// connection
	"HELLO": {ClassConn, kNone}, "AUTH": {ClassConn, kNone}, "SELECT": {ClassConn, kNone},
	"CLIENT": {ClassConn, kNone}, "PING": {ClassConn, kNone}, "ECHO": {ClassConn, kNone},
	"QUIT": {ClassConn, kNone}, "COMMAND": {ClassConn, kNone},
	"READONLY": {ClassConn, kNone}, "READWRITE": {ClassConn, kNone}, "TIME": {ClassConn, kNone},

	// these change the shape of the reply stream or the upstream identity, so
	// the proxy cannot keep refusals slot-aligned: refused for everyone.
	"RESET": {ClassForbidden, kNone}, "MONITOR": {ClassForbidden, kNone}, "SYNC": {ClassForbidden, kNone},
	"PSYNC": {ClassForbidden, kNone}, "REPLCONF": {ClassForbidden, kNone},

	// pub/sub
	"SUBSCRIBE": {ClassPubSub, kNone}, "PSUBSCRIBE": {ClassPubSub, kNone},
	"UNSUBSCRIBE": {ClassPubSub, kNone}, "PUNSUBSCRIBE": {ClassPubSub, kNone},
	"SSUBSCRIBE": {ClassPubSub, kNone}, "SUNSUBSCRIBE": {ClassPubSub, kNone},
	"PUBSUB":  {ClassRead, kNone},
	"PUBLISH": {ClassPublish, kNone}, "SPUBLISH": {ClassPublish, kNone},

	// scripting / admin
	"EVALSHA": {ClassScript, kNone}, "EVAL": {ClassScript, kNone},
	"EVALSHA_RO": {ClassScript, kNone}, "EVAL_RO": {ClassScript, kNone},
	"INFO": {ClassAdmin, kNone}, "CLUSTER": {ClassAdmin, kNone},
}

// Subcommand-sensitive commands.
func classifySub(name string, args [][]byte) (Class, bool) {
	switch name {
	case "CONFIG", "SCRIPT", "CLIENT":
	default:
		return "", false
	}
	if len(args) < 2 {
		return ClassUnknown, true
	}
	sub := strings.ToUpper(string(args[1]))
	switch name {
	case "CONFIG":
		if sub == "GET" {
			return ClassAdmin, true
		}
		return ClassAdminW, true // SET, REWRITE, RESETSTAT
	case "SCRIPT":
		switch sub {
		case "EXISTS":
			return ClassAdmin, true
		case "LOAD":
			if len(args) == 3 && AllowedScripts[ScriptSHA(args[2])] != "" {
				return ClassAdmin, true
			}
			return ClassScript, true
		}
		return ClassAdminW, true // FLUSH, KILL, DEBUG
	case "CLIENT":
		switch sub {
		case "SETINFO", "SETNAME", "GETNAME", "ID", "INFO", "LIST", "GETREDIR", "TRACKINGINFO",
			"MAINT_NOTIFICATIONS": // go-redis >= 9.15 RESP3 handshake (pushes only)
			return ClassConn, true
		case "REPLY", "TRACKING", "CACHING":
			// REPLY OFF/SKIP removes reply slots; TRACKING redirects
			// invalidations. Either would desynchronise refusals.
			return ClassForbidden, true
		}
		return ClassAdminW, true // KILL, PAUSE, UNPAUSE, NO-EVICT, NO-TOUCH, UNBLOCK
	}
	return "", false
}

// Key shapes. Inode-keyed shapes carry the inode in KeyRef.Ino.
const (
	ShapeInode   = "i<ino>"
	ShapeDentry  = "d<ino>"
	ShapeParent  = "p<ino>"
	ShapeXattr   = "x<ino>"
	ShapeSymlink = "s<ino>"
	ShapeChunk   = "c<ino>_<idx>"
	ShapeFlock   = "lockf<ino>"
	ShapePlock   = "lockp<ino>"
	ShapeLocked  = "locked<sid>"
	ShapeSession = "session<sid>"
	ShapeUnknown = "UNKNOWN"

	// JuiceMount / farm / proxy namespaces.
	ShapePresence  = "jum:presence:<host>"
	ShapeLinkBench = "jm:link-bench:<nonce>"
	ShapeFarm      = "juicefarm:*"
	ShapeJMFarm    = "jmfarm:*"
	ShapeJMCtl     = "jmctl:*"
	ShapeLease     = "jmlease:*"
	ShapeChannel   = "juicemount:metadata"
)

// TrashInode is meta.TrashInode in JuiceFS v1.3.1 (pkg/meta/interface.go).
const TrashInode = 0x7FFFFFFF10000000

// Global keys, exact names (prefix "" for a redis:// volume URL).
var globalKeys = map[string]string{
	"setting": "admin", "acl": "admin",
	"nextinode": "counter", "nextchunk": "counter", "nextsession": "counter",
	"nextTrash": "counter", "usedSpace": "counter", "totalInodes": "counter",
	"lastCleanupSessions": "counter",
	"lastCleanupFiles":    "bgmark", "lastCleanupTrash": "bgmark", "nextCleanupSlices": "bgmark",
	"dirDataLength": "dirstat", "dirUsedSpace": "dirstat", "dirUsedInodes": "dirstat",
	"dirQuotaUsedSpace": "dirstat", "dirQuotaUsedInodes": "dirstat",
	"dirQuota": "quota",
	// gc: inherit a txn's project anchors, else bg-only. delSlices is
	// written INSIDE the chunk txn of a foreground compaction when trash is
	// enabled (redis.go doCompactChunk); the coverage run proved a Mac hits
	// it, so it cannot be bg-only as the plan first said.
	"delfiles": "gc", "sliceRef": "gc", "delSlices": "gc", "detachedNodes": "gc",
	"allSessions": "session", "sessionInfos": "session", "sessions": "session",
}

// KeyRef is one key mapped to its JuiceFS shape.
type KeyRef struct {
	Shape  string // e.g. "i<ino>" or the literal global name
	Group  string // inode | dentry | counter | dirstat | quota | gc | bgmark | session | admin | presence | linkbench | farm | lease | channel | unknown
	Ino    uint64 // inode, when the shape is inode-keyed
	HasIno bool
	Trash  bool
	Sid    uint64 // session id, for session<sid> / locked<sid>
	Suffix string // the variable part of a namespaced key (presence host, ...)
}

// digits parses a canonical unsigned decimal: digits only, no sign, no
// leading zeros except "0" itself. Key names and fields juicefs builds with
// strconv.FormatUint are canonical; "i012" is a different Redis key from
// "i12" and must not be mistaken for inode 12.
func digits(b string) (uint64, bool) {
	if b == "" || len(b) > 20 || (len(b) > 1 && b[0] == '0') {
		return 0, false
	}
	for i := 0; i < len(b); i++ {
		if b[i] < '0' || b[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseUint(b, 10, 64)
	return n, err == nil
}

// RedisInt parses an integer exactly as Redis's string2ll does (util.c):
// an optional '-', then digits; no '+', no spaces, no leading zeros except
// the single "0", no "-0", and within int64. Anything else Redis rejects
// with "value is not an integer or out of range" (review H8B-L1d #1).
func RedisInt(b []byte) (int64, bool) {
	if len(b) == 0 || len(b) > 20 {
		return 0, false
	}
	if len(b) == 1 && b[0] == '0' {
		return 0, true
	}
	neg := b[0] == '-'
	d := b
	if neg {
		d = b[1:]
	}
	if len(d) == 0 || d[0] < '1' || d[0] > '9' {
		return 0, false
	}
	for _, c := range d {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseUint(string(d), 10, 64)
	if err != nil {
		return 0, false
	}
	if neg {
		if v > 1<<63 {
			return 0, false
		}
		return -int64(v), true // -(1<<63) wraps to MinInt64, as intended
	}
	if v > 1<<63-1 {
		return 0, false
	}
	return int64(v), true
}

// ParseKey maps a raw key to its shape.
func ParseKey(k string) KeyRef {
	if g, ok := globalKeys[k]; ok {
		return KeyRef{Shape: k, Group: g}
	}
	switch {
	case strings.HasPrefix(k, "jum:presence:") && len(k) > len("jum:presence:"):
		return KeyRef{Shape: ShapePresence, Group: "presence", Suffix: k[len("jum:presence:"):]}
	case strings.HasPrefix(k, "jm:link-bench:") && len(k) > len("jm:link-bench:"):
		return KeyRef{Shape: ShapeLinkBench, Group: "linkbench", Suffix: k[len("jm:link-bench:"):]}
	case strings.HasPrefix(k, "juicefarm:"):
		return KeyRef{Shape: ShapeFarm, Group: "farm", Suffix: k[len("juicefarm:"):]}
	case strings.HasPrefix(k, "jmfarm:"):
		return KeyRef{Shape: ShapeJMFarm, Group: "farm", Suffix: k[len("jmfarm:"):]}
	case strings.HasPrefix(k, "jmctl:"):
		return KeyRef{Shape: ShapeJMCtl, Group: "farm", Suffix: k[len("jmctl:"):]}
	case strings.HasPrefix(k, "jmlease:"):
		return KeyRef{Shape: ShapeLease, Group: "lease", Suffix: k[len("jmlease:"):]}
	case k == "juicemount:metadata":
		return KeyRef{Shape: ShapeChannel, Group: "channel"}
	}
	ino := func(shape, group, rest string) (KeyRef, bool) {
		n, ok := digits(rest)
		if !ok {
			return KeyRef{}, false
		}
		return KeyRef{Shape: shape, Group: group, Ino: n, HasIno: true, Trash: n >= TrashInode}, true
	}
	switch {
	case strings.HasPrefix(k, "lockf"):
		if r, ok := ino(ShapeFlock, "inode", k[5:]); ok {
			return r
		}
	case strings.HasPrefix(k, "lockp"):
		if r, ok := ino(ShapePlock, "inode", k[5:]); ok {
			return r
		}
	case strings.HasPrefix(k, "locked"):
		if n, ok := digits(k[6:]); ok {
			return KeyRef{Shape: ShapeLocked, Group: "session", Sid: n}
		}
	case strings.HasPrefix(k, "session"):
		if n, ok := digits(k[7:]); ok {
			return KeyRef{Shape: ShapeSession, Group: "session", Sid: n}
		}
	}
	if len(k) >= 2 {
		rest := k[1:]
		switch k[0] {
		case 'i':
			if r, ok := ino(ShapeInode, "inode", rest); ok {
				return r
			}
		case 'd':
			if r, ok := ino(ShapeDentry, "dentry", rest); ok {
				return r
			}
		case 'p':
			if r, ok := ino(ShapeParent, "inode", rest); ok {
				return r
			}
		case 'x':
			if r, ok := ino(ShapeXattr, "inode", rest); ok {
				return r
			}
		case 's':
			if r, ok := ino(ShapeSymlink, "inode", rest); ok {
				return r
			}
		case 'c':
			if u := strings.IndexByte(rest, '_'); u > 0 {
				if _, ok := digits(rest[u+1:]); ok {
					if r, ok := ino(ShapeChunk, "inode", rest[:u]); ok {
						return r
					}
				}
			}
		}
	}
	return KeyRef{Shape: ShapeUnknown, Group: "unknown"}
}

// LeaseNeed says what a per-project lease check needs to admit a write to
// this key shape (coverage report column).
func LeaseNeed(r KeyRef) string {
	switch {
	case r.Trash:
		return "trash namespace (ns:trash; inode >= TrashInode)"
	case r.Group == "inode":
		return "inode->namespace map (ancestry)"
	case r.Group == "dentry":
		return "dir inode->namespace map (ancestry of parent dir)"
	case r.Group == "counter":
		return "global counter (no namespace; allow-list)"
	case r.Group == "dirstat":
		return "dir-stat hash; field=dir inode -> namespace map"
	case r.Group == "quota":
		return "quota limit: HSET admin-only; HDEL with rmdir (project)"
	case r.Group == "gc":
		return "deferred delete / slice refs: txn anchors, else bg-only"
	case r.Group == "bgmark":
		return "background-job bookkeeping: bg-only"
	case r.Group == "session":
		return "session-scoped (per principal)"
	case r.Group == "admin":
		return "admin-only (format/config/acl)"
	case r.Group == "presence":
		return "own host only"
	case r.Group == "linkbench":
		return "allow-list with expiry <= 60s"
	case r.Group == "farm":
		return "farm/manager only"
	case r.Group == "lease":
		return "proxy-owned lease table: admin only"
	case r.Group == "channel":
		return "pub/sub channel: PUBLISH only"
	}
	return "UNKNOWN"
}

// Command is the classification of one argv.
type Command struct {
	Name  string
	Class Class
	Keys  []KeyRef
	Raw   []string // raw keys, parallel to Keys
	// Script names the allow-listed script an EVAL/EVALSHA runs ("" if none).
	Script string
}

// AllowedScripts pins, by SHA1, the only Lua bodies a non-server principal
// may run. The proxy never parses a body to decide what it does: text
// matching was bypassed by building names at run time (review H8B-L1 #1).
// Each entry is a read-only script from the tree; TestScriptAllowListPinned
// recomputes every SHA1 from source, so an edited script fails the build's
// tests rather than silently losing (or gaining) access. The proxy also
// forwards these as EVAL_RO / EVALSHA_RO so Redis itself refuses any write.
var AllowedScripts = map[string]string{
	"8526c64984678fc27f67f44755c8ba7dcdf7ea60": "juicefs scriptLookup (third_party/juicefs/pkg/meta/lua_scripts.go)",
	"0547fa97c6a6bebfe522a4137b43f2d50eb9216a": "juicefs scriptResolve (third_party/juicefs/pkg/meta/lua_scripts.go)",
	"92d41a73b32a68ac03c1f7ce2910ed9a4ea2813c": "metadata luaScanBatch (metadata/redis.go)",
	"1767fecff3ce885cc018ce3379c55a434f577aa1": "metadata parentReadProofLua (metadata/parent_read_proof.lua)",
	"79760dcca4c279536b7667347e86d589d2903917": "cache boundMappingLua (cache/volume.go)",
	"70871faf1a8e776fa9dd0d48552178f9aa5c5384": "cache boundMappingAttrLua (cache/volume.go)",
	"116ebec64bd055ba335345057adcf93bdfe298b9": "cache boundedPrepareMappingAttrLua (cache/volume.go)",
	"251df9c21a32cbd8e343158f9a65c863357fac06": "cache ReadVolumeBinding inline script (cache/volume.go)",
}

// ScriptSHA is the Redis script id of a body.
func ScriptSHA(body []byte) string {
	sum := sha1.Sum(body)
	return hex.EncodeToString(sum[:])
}

// Classify classifies one command.
func Classify(args [][]byte) Command {
	if len(args) == 0 {
		return Command{Class: ClassUnknown}
	}
	name := strings.ToUpper(string(args[0]))
	c := Command{Name: name}
	if strings.HasPrefix(name, "JM.") {
		c.Class = ClassLocal
		return c
	}
	if cl, ok := classifySub(name, args); ok {
		c.Class = cl
		return c
	}
	info, ok := commands[name]
	if !ok {
		c.Class = ClassUnknown
		return c
	}
	c.Class = info.class
	if info.class == ClassScript {
		c.Class, c.Keys, c.Raw, c.Script = classifyScript(name, args)
		return c
	}
	if info.keys.step == 0 {
		return c
	}
	last := info.keys.last
	if last < 0 {
		last = len(args) + last
	}
	for i := info.keys.first; i <= last && i < len(args); i += info.keys.step {
		c.Keys = append(c.Keys, ParseKey(string(args[i])))
		c.Raw = append(c.Raw, string(args[i]))
	}
	return c
}

// classifyScript: an allow-listed body is a read (its KEYS are recorded);
// anything else is ClassScript, which only server principals may run.
func classifyScript(name string, args [][]byte) (Class, []KeyRef, []string, string) {
	if len(args) < 3 {
		return ClassScript, nil, nil, ""
	}
	var sha string
	if strings.HasPrefix(name, "EVALSHA") {
		sha = strings.ToLower(string(args[1]))
	} else {
		sha = ScriptSHA(args[1])
	}
	if AllowedScripts[sha] == "" {
		return ClassScript, nil, nil, ""
	}
	nk, err := strconv.Atoi(string(args[2]))
	if err != nil || nk < 0 || 3+nk > len(args) {
		return ClassScript, nil, nil, ""
	}
	// JuiceFS passes non-key data in KEYS (scriptLookup: [d<parent>, name]),
	// so only a parseable key is reported as a key.
	var keys []KeyRef
	var raw []string
	for _, k := range args[3 : 3+nk] {
		ref := ParseKey(string(k))
		if ref.Shape == ShapeUnknown {
			ref = KeyRef{Shape: "script-arg", Group: "script-arg"}
		}
		keys = append(keys, ref)
		raw = append(raw, string(k))
	}
	return ClassRead, keys, raw, sha
}

// sessionIDs extracts the session ids a write on a session-group key
// touches: the sid in the key itself (session<sid>, locked<sid>), or the
// field/member of allSessions / sessionInfos / sessions.
func sessionIDs(name string, key KeyRef, args [][]byte) []uint64 {
	switch key.Shape {
	case ShapeSession, ShapeLocked:
		return []uint64{key.Sid}
	}
	var out []uint64
	add := func(b []byte) {
		if n, ok := digits(string(b)); ok {
			out = append(out, n)
		}
	}
	switch name {
	case "ZADD": // ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member ...
		i := 2
	opts:
		for i < len(args) {
			switch strings.ToUpper(string(args[i])) {
			case "NX", "XX", "GT", "LT", "CH", "INCR":
				i++
			default:
				break opts
			}
		}
		for ; i+1 < len(args); i += 2 {
			add(args[i+1])
		}
	case "ZREM", "HDEL", "SREM":
		for _, a := range args[2:] {
			add(a)
		}
	case "HSET", "HMSET":
		for i := 2; i < len(args); i += 2 {
			add(args[i])
		}
	case "HSETNX", "HINCRBY":
		if len(args) > 2 {
			add(args[2])
		}
	}
	return out
}

// attrParent decodes Attr.Parent from a JuiceFS v1.3.1 marshalled attr
// (pkg/meta/interface.go Attr.Marshal: 63 bytes then Parent, big-endian).
func attrParent(v []byte) (uint64, bool) {
	if len(v) != 72 && len(v) != 80 {
		return 0, false
	}
	var p uint64
	for _, b := range v[63:71] {
		p = p<<8 | uint64(b)
	}
	return p, true
}

// entryInode decodes a d<parent> field value (type byte + 8-byte inode).
func entryInode(v []byte) (uint64, bool) {
	if len(v) != 9 {
		return 0, false
	}
	var p uint64
	for _, b := range v[1:9] {
		p = p<<8 | uint64(b)
	}
	return p, true
}

// entryType returns the type byte of a d<parent> field value (1 = file,
// 2 = directory, ... meta.TypeFile/TypeDirectory).
func entryType(v []byte) (byte, bool) {
	if len(v) != 9 {
		return 0, false
	}
	return v[0], true
}

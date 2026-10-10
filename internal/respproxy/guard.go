package respproxy

import (
	"bytes"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// KeyClass is the guard's class for one written key (plan §1.4 step 2).
type KeyClass string

const (
	KCCounter     KeyClass = "counter"      // global counters: any principal
	KCSession     KeyClass = "session"      // own session keys; others' are bg-only
	KCPresence    KeyClass = "presence"     // own jum:presence:<host>
	KCLinkBench   KeyClass = "linkbench"    // jm:link-bench:* with expiry <= 60 s
	KCTrashCreate KeyClass = "trash-create" // creating an hourly trash directory
	KCOwnRecord   KeyClass = "own-record"   // dropping a cleanup record this principal queued
	KCBG          KeyClass = "bg"           // background-job keys: bg principal only
	KCFarm        KeyClass = "farm"         // juicefarm:/jmfarm:/jmctl: farm or manager only
	KCAdmin       KeyClass = "admin"        // setting, acl, quota limits, jmlease:*
	KCProject     KeyClass = "project"      // left for Policy.Decide
	KCChannel     KeyClass = "channel"      // a pub/sub channel name: PUBLISH only, never written as a key
	KCUnknown     KeyClass = "unknown"      // not a JuiceFS or JuiceMount shape
)

// KeyClassOf is the context-free class of a key: what a bare SET to it
// would be checked as before unit context (trash pairing, txn inheritance)
// refines it. KCUnknown means the proxy does not recognise the shape.
func KeyClassOf(key string) KeyClass {
	return baseClass(ParseKey(key), "SET")
}

func baseClass(k KeyRef, cmd string) KeyClass {
	switch k.Group {
	case "counter":
		return KCCounter
	case "session":
		return KCSession
	case "presence":
		return KCPresence
	case "linkbench":
		return KCLinkBench
	case "farm":
		return KCFarm
	case "lease", "admin":
		return KCAdmin
	case "bgmark":
		return KCBG
	case "gc":
		return KCBG // refined: inherits a txn's project anchors
	case "quota":
		if cmd == "HDEL" {
			return KCProject // rmdir drops the dir's quota row in its txn
		}
		return KCAdmin // setting a quota limit
	case "dirstat":
		switch cmd {
		case "HINCRBY", "HINCRBYFLOAT", "HSET", "HSETNX", "HMSET", "HDEL":
			return KCProject // field = directory inode
		}
		// DEL/RENAME/EXPIRE of a whole volume-wide stats hash (juicefs does
		// it only in format, redis.go:300) is an admin operation.
		return KCAdmin
	case "inode", "dentry":
		return KCProject // trash refined below
	case "channel":
		return KCChannel
	}
	return KCUnknown
}

// SessionStore persists which principal a juicefs session id was allocated
// to. Ownership is granted ONLY when the proxy relays the reply of a
// principal's own INCR/INCRBY nextsession (the sid juicefs then registers),
// never by "first writer", so it cannot be pre-claimed or taken over after a
// proxy restart (review H8B-L1 #5). The proxy's store lives in Redis
// (jmlease:v1:sess, which clients cannot write).
type SessionStore interface {
	Owner(sid uint64) (owner string, ok bool, err error)
	// Claim records sid -> owner if sid has no owner yet (HSETNX) and
	// returns the owner that holds it afterwards.
	Claim(sid uint64, owner string) (string, error)
	Release(sids []uint64) error
}

// SliceStore records which principal each slice id was allocated to. The
// proxy fills it from the reply of a principal's own bare INCRBY nextchunk
// (juicefs allocates sliceIdBatch=4096 ids per call, base.go:1658: ids
// (R-n, R] for reply R), so a client can only claim ids the proxy watched
// Redis hand to it.
//
// Each slice id also has a persisted "used" bit, set atomically with the
// first EXEC that links it into a chunk (the proxy appends SETBIT to that
// MULTI), or after the compaction rollback that discards it. Rule (a) of
// validatePushes admits a self-allocated slice only while its bit is clear
// (review H8B-L1d #3/#5).
type SliceStore interface {
	Owner(id uint64) (owner string, ok bool, err error)
	Record(lo, hi uint64, owner string) error // ids lo..hi inclusive
	Used(id uint64) (bool, error)
	// MarkUsed records ids as used; persist also writes Redis (when the
	// bit was not already set by the unit's own MULTI).
	MarkUsed(ids []uint64, persist bool) error
}

// UsedBitKey / UsedBitOffset locate slice id's used bit: one 128 KiB bitmap
// per 2^20 ids, so Redis memory follows the ids actually allocated.
func UsedBitKey(id uint64) string    { return "jmlease:v1:sused:" + strconv.FormatUint(id>>20, 10) }
func UsedBitOffset(id uint64) uint64 { return id & (1<<20 - 1) }

// MemSliceStore is an in-memory SliceStore (tests).
type MemSliceStore struct {
	mu     sync.Mutex
	ranges []sliceRange
	used   map[uint64]bool
}

func (s *MemSliceStore) Used(id uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used[id], nil
}

func (s *MemSliceStore) MarkUsed(ids []uint64, persist bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.used == nil {
		s.used = map[uint64]bool{}
	}
	for _, id := range ids {
		s.used[id] = true
	}
	return nil
}

type sliceRange struct {
	lo, hi uint64
	owner  string
}

func (s *MemSliceStore) Owner(id uint64) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.ranges {
		if id >= r.lo && id <= r.hi {
			return r.owner, true, nil
		}
	}
	return "", false, nil
}

func (s *MemSliceStore) Record(lo, hi uint64, owner string) error {
	s.mu.Lock()
	s.ranges = append(s.ranges, sliceRange{lo, hi, owner})
	s.mu.Unlock()
	return nil
}

// ChunkReader returns the elements of a chunk list (LRANGE key 0 -1) from a
// proxy-owned side connection.
type ChunkReader interface {
	ChunkElems(db int, key string) ([][]byte, error)
}

// MemSessionStore is an in-memory SessionStore (tests).
type MemSessionStore struct {
	mu sync.Mutex
	m  map[uint64]string
}

func NewMemSessionStore() *MemSessionStore { return &MemSessionStore{m: map[uint64]string{}} }

func (s *MemSessionStore) Owner(sid uint64) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.m[sid]
	return o, ok, nil
}

func (s *MemSessionStore) Claim(sid uint64, owner string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if o, ok := s.m[sid]; ok {
		return o, nil
	}
	s.m[sid] = owner
	return owner, nil
}

func (s *MemSessionStore) Release(sids []uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sid := range sids {
		delete(s.m, sid)
	}
	return nil
}

type zeroRef struct {
	owner string
	ino   uint64
}

// Guard runs the command gate (step 1) and the key-class gate (step 2).
type Guard struct {
	sessions SessionStore
	chunks   ChunkReader
	slices   SliceStore

	mu sync.Mutex
	// zeroRefs: sliceRef fields a principal set to "0" inside its own,
	// verified compaction txn (doCompactChunk), with the inode of the chunk
	// the new slice was pushed to. juicefs then drops that tracking entry in
	// a txn holding only HDEL sliceRef <field> (cleanupZeroRef,
	// redis.go:3030/:3325). In memory only: after a proxy restart the drop
	// is refused and the bg principal's slice cleanup removes the entry.
	zeroRefs map[string]zeroRef
	// pendingDel: delfiles members ("<ino>:<len>") a principal queued in an
	// admitted unlink / close-of-unlinked-file txn. juicefs then deletes
	// the data itself and drops the entry with a BARE ZREM delfiles
	// (redis.go doDeleteFileData_ :3201), reached from tryDeleteFileData
	// (base.go:2422), whose concurrency is the fixed maxDeleting channel
	// (base.go:290), NOT --max-deletes, so --no-bgjob --max-deletes 0 do not
	// skip it. Admitting exactly the entry the same principal queued is the
	// "own record" rule (lane L3 finding, 2026-10-06). In memory only:
	// after a proxy restart the ZREM is refused and the bg principal's
	// cleanupDeletedFiles removes the entry.
	pendingDel map[string]string
	counters   CounterReader
	ctrCache   map[string]*counterCache
	rate       map[string]*rateWindow // principal|counter -> net delta this window

	// dropped: sliceRef fields a principal's COMMITTED txn decremented below
	// zero (the EXEC reply showed the HINCRBY result < 0): juicefs then
	// deletes the slice's objects and drops the field with a bare HDEL
	// sliceRef (deleteSlice -> doDeleteSlice, redis.go:279), whenever
	// --max-deletes > 0 (the manager's juicefs sync --delete-dst with
	// trash-days 0, lane L5). In memory; after a proxy restart the drop
	// is still admitted for slices the principal allocated (persisted
	// owner), else refused and the field is left for bg gc.
	dropped map[string]string

	// afterSideRead is a test hook run right after a side read.
	afterSideRead func()
}

// NewGuard returns a guard. chunks may be nil (then compaction-style
// cleanup records that need a side read are refused).
func NewGuard(sessions SessionStore, chunks ChunkReader) *Guard {
	if sessions == nil {
		sessions = NewMemSessionStore()
	}
	return &Guard{sessions: sessions, chunks: chunks, slices: &MemSliceStore{}, zeroRefs: map[string]zeroRef{},
		pendingDel: map[string]string{}, ctrCache: map[string]*counterCache{}, rate: map[string]*rateWindow{},
		dropped: map[string]string{}}
}

// guardEffect is state learned from a unit, applied by Commit only when the
// unit is admitted.
type guardEffect struct {
	zeroKey string // sliceRef field
	zeroAdd bool
	zero    zeroRef
	release []uint64 // session ids ended (ZREM allSessions)
	// sliceUsed: self-allocated slices this unit links for the first time
	// (the proxy appends SETBIT for them to the MULTI); usedPersist: mark
	// via a side write instead (bare compaction rollback).
	sliceUsed   []uint64
	usedPersist bool
	dropDone    []string // sliceRef fields dropped (forget their records)
	delKey      string   // delfiles member
	delAdd      bool
	delOwn      string
}

// Commit applies the effects of an admitted unit.
func (g *Guard) Commit(effects []guardEffect) {
	if len(effects) == 0 {
		return
	}
	var rel []uint64
	g.mu.Lock()
	if len(g.zeroRefs) > 100000 {
		g.zeroRefs = map[string]zeroRef{}
	}
	for _, e := range effects {
		switch {
		case e.zeroKey != "" && e.zeroAdd:
			g.zeroRefs[e.zeroKey] = e.zero
		case e.zeroKey != "":
			delete(g.zeroRefs, e.zeroKey)
		}
		switch {
		case e.delKey != "" && e.delAdd:
			if len(g.pendingDel) > 1000000 {
				g.pendingDel = map[string]string{}
			}
			g.pendingDel[e.delKey] = e.delOwn
		case e.delKey != "":
			delete(g.pendingDel, e.delKey)
		}
		rel = append(rel, e.release...)
		for _, f := range e.dropDone {
			delete(g.dropped, f)
		}
	}
	g.mu.Unlock()
	// Store I/O happens outside the guard lock.
	for _, e := range effects {
		if len(e.sliceUsed) > 0 {
			g.slices.MarkUsed(e.sliceUsed, e.usedPersist)
		}
	}
	if len(rel) > 0 {
		g.sessions.Release(rel)
	}
}

// needsGate reports whether a bare command has to go through the guard.
// Reads, transaction control, pub/sub and connection commands do not
// (step 0); CONFIG GET does, because it can reveal secrets.
func needsGate(c Command) bool {
	switch c.Class {
	case ClassRead, ClassTxn, ClassPubSub, ClassConn:
		return false
	case ClassAdmin:
		return c.Name == "CONFIG"
	}
	return true
}

func deny(r Reason, detail string) *Denial { return &Denial{Reason: r, Detail: detail} }

// serverPrincipal: the farm and manager run farmqueue's Lua scripts, which
// write the juicefarm: keyspace with keys built inside the body. The proxy
// does not parse bodies, so these principals are TRUSTED for scripts: any
// script they send is forwarded unexamined (full access). Every other
// principal may run only AllowedScripts.
func serverPrincipal(p Principal) bool {
	return p.Kind == KindFarm || p.Kind == KindManager || p.Kind == KindAdmin
}

// Allocation reports the allocator a bare unit advances and by how much
// (INCR/INCRBY nextsession or nextchunk), so the proxy can record from the
// reply who received which ids. n == 0 means none.
func Allocation(u *Unit) (counter string, n int64) {
	if u.Kind != UnitBare || len(u.Cmds) != 1 {
		return "", 0
	}
	c := u.Cmds[0]
	if len(c.Info.Keys) != 1 {
		return "", 0
	}
	switch k := c.Info.Keys[0].Shape; k {
	case "nextsession", "nextchunk":
		if d, ok := counterDelta(c); ok && d > 0 && d <= maxAlloc[k] {
			return k, d
		}
	}
	return "", 0
}

// Allocator batch caps, from juicefs pkg/meta/base.go: inodeBatch = 1<<10
// (:47, used at :1169), sliceIdBatch = 4<<10 (:48, used at :1658); a session
// takes one id (:525). A larger jump could push nextinode past TrashInode
// or wrap an allocator (review H8B-L1b #2).
var maxAlloc = map[string]int64{"nextinode": 1 << 10, "nextchunk": 4 << 10, "nextsession": 16, "nextTrash": 1}

// Per-txn bounds on the volume counters (review H8B-L1b #2). A juicefs txn
// changes usedSpace by the 4K-aligned size change of the files it touches
// (truncate/fallocate redis.go:1229/:1160, unlink :1593, clone :2795), and
// totalInodes by +-1 per inode created or removed (INCR/DECR only, never
// INCRBY). 64 TiB exceeds any file the NAS can hold. Residual: a principal
// can still drift the counters by many in-bound txns; bg's periodic
// recount (juicefs fsck / sync of volume stats) is the backstop.
const (
	maxUsedSpaceDelta   = 1 << 40 // = the per-minute growth bound below
	maxTotalInodesDelta = 1
	// counterLimit: no admitted write may move usedSpace/totalInodes past
	// +-2^60 (1 EiB / 1e18 inodes, far beyond any volume). Many in-bound
	// txns could otherwise overflow int64 (review H8B-L1c #2: 131,072 x
	// 64 TiB in 0.33 s), after which every legitimate update errors.
	counterLimit = int64(1) << 60
)

// CounterReader reads a volume counter from a side connection.
type CounterReader interface {
	Counter(db int, key string) (int64, error)
}

type counterCache struct {
	at      time.Time
	val     int64
	pending int64 // deltas admitted since val was read
}

// Per-principal rate bounds on the volume counters (review H8B-L1d #6),
// per rateWindowLen window, net of the principal's own deltas:
//
//	usedSpace   net growth <= 1 TiB/min: 13x a saturated 10GbE link
//	            (~75 GB/min of written data) and room for a 1 TiB fallocate or
//	            truncate a minute; net shrink >= -64 TiB/min, more than the
//	            NAS holds, so deleting a whole volume's tree fits.
//	totalInodes net +1,000,000/min (juicefs on the NAS creates ~10^3-10^4
//	            files/s) and -10,000,000/min for large deletes.
//
// At these rates one principal needs >16 years to overflow usedSpace
// upward; the absolute counterLimit ceiling below still stops many
// principals together. Deltas count at decision time (an aborted txn's
// delta stays charged: conservative).
const rateWindowLen = time.Minute

var rateBounds = map[string][2]int64{
	"usedSpace":   {-64 << 40, 1 << 40},
	"totalInodes": {-10_000_000, 1_000_000},
}

type rateWindow struct {
	start time.Time
	net   int64
}

// counterCeiling admits delta n of counter key for principal p only within
// p's rate bound and the absolute counterLimit, and charges it. The cached
// counter value is read and replaced atomically: a refresh only replaces
// the entry it found stale, and the check and charge happen under one lock.
func (g *Guard) counterCeiling(db int, p Principal, key string, n int64) *Denial {
	now := time.Now()
	g.mu.Lock()
	cc := g.ctrCache[key]
	stale := g.counters != nil && (cc == nil || now.Sub(cc.at) > counterRefresh)
	g.mu.Unlock()
	if stale {
		v, err := g.counters.Counter(db, key)
		if err != nil {
			return deny(ReasonBGOnly, "counter read: "+err.Error())
		}
		g.mu.Lock()
		if g.ctrCache[key] == cc { // nobody refreshed meanwhile
			g.ctrCache[key] = &counterCache{at: now, val: v}
		}
		g.mu.Unlock()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	rk := p.Name + "|" + key
	rw := g.rate[rk]
	if rw == nil || now.Sub(rw.start) >= rateWindowLen {
		if len(g.rate) > 100000 {
			g.rate = map[string]*rateWindow{}
		}
		rw = &rateWindow{start: now}
		g.rate[rk] = rw
	}
	if b, ok := rateBounds[key]; ok && p.Kind != KindBG { // bg's trash/gc cleanup is exempt
		if next := rw.net + n; next < b[0] || next > b[1] {
			return deny(ReasonAdminOnly, key+": "+p.Name+" would change it by more than its per-minute bound")
		}
	}
	if cc := g.ctrCache[key]; cc != nil {
		proj := cc.val + cc.pending
		if (n > 0 && proj > counterLimit-n) || (n < 0 && proj < -counterLimit-n) {
			return deny(ReasonAdminOnly, key+" would pass the volume counter limit")
		}
		cc.pending += n
	}
	rw.net += n
	return nil
}

const counterRefresh = time.Second

func counterDelta(c Cmd) (int64, bool) {
	switch c.Info.Name {
	case "INCR":
		return 1, true
	case "DECR":
		return -1, true
	case "INCRBY", "DECRBY":
		if len(c.Args) != 3 {
			return 0, false
		}
		n, ok := RedisInt(c.Args[2])
		if !ok || (c.Info.Name == "DECRBY" && n == math.MinInt64) {
			return 0, false
		}
		if c.Info.Name == "DECRBY" {
			n = -n
		}
		return n, true
	}
	return 0, false
}

// counterGate: the allocators (next*) only move forward, so no principal can
// rewind one into ids another principal owns (sessions, inodes, chunks).
func (g *Guard) counterGate(p Principal, u *Unit, c Cmd, w KeyWrite) *Denial {
	name := w.Key.Shape
	switch {
	case strings.HasPrefix(name, "next"):
		n, ok := counterDelta(c)
		if !ok || n <= 0 {
			return deny(ReasonAdminOnly, c.Info.Name+" "+w.Raw+": allocators only increase")
		}
		if max, capped := maxAlloc[name]; capped && n > max {
			return deny(ReasonAdminOnly, c.Info.Name+" "+w.Raw+" "+strconv.FormatInt(n, 10)+": over the juicefs batch size")
		}
		if (name == "nextsession" || name == "nextchunk") && u.Kind != UnitBare {
			return deny(ReasonAdminOnly, name+" is allocated by a bare INCRBY so the proxy can record who received the ids")
		}
		// juicefs always takes exactly sliceIdBatch; smaller steps would
		// grow the slice-owner table by one entry each (review H8B-L1c #3).
		if name == "nextchunk" && n != maxAlloc["nextchunk"] {
			return deny(ReasonAdminOnly, "nextchunk is allocated in batches of exactly 4096")
		}
	case name == "lastCleanupSessions":
		switch c.Info.Name {
		case "SET", "SETNX", "GETSET":
		default:
			return deny(ReasonAdminOnly, c.Info.Name+" "+w.Raw)
		}
	default: // usedSpace, totalInodes
		// juicefs changes these only inside the txn of the file operation
		// that causes the change (redis.go:1160,1229,1467,1593,1712,1987,
		// 2607,2663,2795); a bare write is not juicefs and is bg-only.
		if u.Kind != UnitTxn && p.Kind != KindBG {
			return deny(ReasonBGOnly, c.Info.Name+" "+w.Raw+" outside a file operation's txn")
		}
		n, ok := counterDelta(c)
		if !ok {
			return deny(ReasonAdminOnly, c.Info.Name+" "+w.Raw+": counters are only incremented")
		}
		lim := int64(maxUsedSpaceDelta)
		if name == "totalInodes" {
			lim = maxTotalInodesDelta
		}
		if n > lim || n < -lim {
			return deny(ReasonAdminOnly, c.Info.Name+" "+w.Raw+" "+strconv.FormatInt(n, 10)+": delta out of bounds")
		}
		if d := g.counterCeiling(u.DB, p, name, n); d != nil {
			return d
		}
	}
	return nil
}

// Check rules on a unit. It fills u.Writes, u.Project and u.RefInodes. On
// admit it returns the effects to Commit once the whole unit is admitted.
func (g *Guard) Check(p Principal, u *Unit) (*Denial, []guardEffect) {
	admin := p.Kind == KindAdmin
	u.Writes = u.Writes[:0]
	u.Project = u.Project[:0]
	u.RefInodes = u.RefInodes[:0]

	// Step 1: the command gate.
	for i, c := range u.Cmds {
		name := c.Info.Name
		switch c.Info.Class {
		case ClassRead, ClassTxn, ClassPubSub, ClassConn:
			continue
		case ClassAdmin:
			if name == "CONFIG" && !admin && !safeConfigGet(c.Args) {
				return deny(ReasonAdminOnly, "CONFIG GET of a sensitive parameter"), nil
			}
			continue
		case ClassForbidden:
			return deny(ReasonAdminOnly, name+" is refused for every principal"), nil
		case ClassAdminW, ClassUnknown:
			if !admin {
				return deny(ReasonAdminOnly, "command "+name), nil
			}
			continue
		case ClassScript:
			if !serverPrincipal(p) {
				return deny(ReasonAdminOnly, name+": script not on the allow-list"), nil
			}
			continue
		case ClassLocal:
			return deny(ReasonUnknownKey, name+" inside a transaction"), nil
		case ClassPublish:
			if admin || (len(c.Args) >= 2 && string(c.Args[1]) == ShapeChannel) {
				continue
			}
			return deny(ReasonAdminOnly, "PUBLISH to a channel other than juicemount:metadata"), nil
		case ClassWrite:
			if name == "COPY" && !admin && hasDBOption(c.Args) {
				return deny(ReasonAdminOnly, "COPY into another DB"), nil
			}
			if !admin && !integerArgsOK(c) {
				// Redis would reject it alone inside MULTI while the rest
				// runs; the guard must never credit a command that fails.
				return deny(ReasonUnknownKey, name+": an integer argument is not in Redis's canonical form"), nil
			}
			for j, k := range c.Info.Keys {
				u.Writes = append(u.Writes, KeyWrite{Cmd: i, Key: k, Raw: c.Info.Raw[j], Class: baseClass(k, name)})
			}
		default:
			if !admin {
				return deny(ReasonAdminOnly, "command "+name), nil
			}
		}
	}

	// Step 2a: refine classes that depend on the rest of the unit.
	var nonTrashDentry, trashRootHSet bool
	for _, w := range u.Writes {
		if w.Key.Shape != ShapeDentry {
			continue
		}
		if !w.Key.Trash {
			nonTrashDentry = true
		} else if w.Key.Ino == TrashInode && isHSet(u.Cmds[w.Cmd].Info.Name) {
			trashRootHSet = true
		}
	}
	for i := range u.Writes {
		w := &u.Writes[i]
		if !w.Key.Trash || (w.Key.Group != "inode" && w.Key.Group != "dentry") {
			continue
		}
		name := u.Cmds[w.Cmd].Info.Name
		switch {
		case nonTrashDentry && u.Kind == UnitTxn:
			// unlink / rename-replace into trash, or restore out of it:
			// anchored by the non-trash dentry in the same txn (L2).
			w.Class = KCProject
		case u.Kind == UnitBare && name == "SETNX" && w.Key.Shape == ShapeInode:
			w.Class = KCTrashCreate // format / first trash use (redis.go:326)
		case u.Kind == UnitTxn && trashRootHSet &&
			((w.Key.Shape == ShapeDentry && w.Key.Ino == TrashInode && isHSet(name)) ||
				(w.Key.Shape == ShapeInode && (name == "SET" || name == "SETNX"))):
			w.Class = KCTrashCreate // checkTrash's hourly directory mknod (base.go:2488)
		default:
			w.Class = KCBG // trash cleanup
		}
	}
	hasProject := false
	for _, w := range u.Writes {
		if w.Class == KCProject {
			hasProject = true
			break
		}
	}

	var effects []guardEffect
	if u.Kind == UnitBare && !admin && p.Kind != KindBG && len(u.Writes) == 1 &&
		u.Writes[0].Key.Shape == "sliceRef" && u.Cmds[0].Info.Name == "HDEL" {
		e, d := g.ownSliceDrop(p, u)
		if d != nil {
			return d, nil
		}
		u.Writes[0].Class = KCOwnRecord
		effects = append(effects, e...)
	}
	if u.Kind == UnitBare && !admin && p.Kind != KindBG && len(u.Writes) == 1 &&
		u.Writes[0].Key.Shape == "sliceRef" && u.Cmds[0].Info.Name == "HINCRBY" {
		// compaction EINVAL rollback of the principal's own never-linked slice
		c := u.Cmds[0]
		fs, ok := sliceRefFields(c)
		if !ok || len(fs) != 1 || len(c.Args) != 4 || string(c.Args[3]) != "-1" {
			return deny(ReasonBGOnly, "bare sliceRef write is not a compaction rollback"), nil
		}
		owner, known, err := g.slices.Owner(fs[0].id)
		if err != nil {
			return deny(ReasonBGOnly, "slice store: "+err.Error()), nil
		}
		used, err := g.slices.Used(fs[0].id)
		if err != nil {
			return deny(ReasonBGOnly, "slice store: "+err.Error()), nil
		}
		if !known || owner != p.Name || used {
			return deny(ReasonBGOnly, "bare sliceRef write is not the rollback of an unlinked slice this principal allocated"), nil
		}
		u.Writes[0].Class = KCOwnRecord
		effects = append(effects, guardEffect{sliceUsed: []uint64{fs[0].id}, usedPersist: true})
	}
	if u.Kind == UnitBare && !admin && p.Kind != KindBG && len(u.Writes) == 1 &&
		u.Writes[0].Key.Shape == "delfiles" && u.Cmds[0].Info.Name == "ZREM" {
		e, d := g.ownDelfilesDrop(p, u.Cmds[0])
		if d != nil {
			return d, nil
		}
		u.Writes[0].Class = KCOwnRecord
		effects = append(effects, e...)
	}
	hasGC := false
	for _, w := range u.Writes {
		if w.Key.Group == "gc" {
			hasGC = true
			break
		}
	}
	var sc *sliceScope
	if !admin && p.Kind != KindBG {
		sc = g.newSliceScope(u, p)
		if d := sc.validatePushes(p); d != nil {
			return d, nil
		}
	}
	if hasGC && !admin && p.Kind != KindBG && u.Kind == UnitTxn {
		var d *Denial
		if hasProject {
			effects, d = g.anchorCleanup(u, sc)
		} else {
			effects, d = g.ownZeroRefDrop(p, u)
		}
		for i := range effects {
			effects[i].delOwn = p.Name
		}
		if d != nil {
			return d, nil
		}
		for i := range effects {
			if effects[i].zeroAdd {
				effects[i].zero.owner = p.Name
			}
		}
	}
	if sc != nil && len(sc.firstUse) > 0 {
		effects = append(effects, guardEffect{sliceUsed: sc.firstUse})
	}

	// Step 2b: the key-class gate.
	for _, w := range u.Writes {
		c := u.Cmds[w.Cmd]
		switch w.Class {
		case KCTrashCreate, KCOwnRecord:
		case KCCounter:
			if !admin {
				if d := g.counterGate(p, u, c, w); d != nil {
					return d, nil
				}
			}
		case KCSession:
			e, d := g.sessionGate(p, c, w)
			if d != nil {
				return d, nil
			}
			effects = append(effects, e...)
		case KCPresence:
			if !admin && w.Key.Suffix != p.host() {
				return deny(ReasonAdminOnly, "presence key of another host "+w.Raw), nil
			}
		case KCLinkBench:
			if !admin && !linkBenchOK(c) {
				return deny(ReasonUnknownKey, "jm:link-bench write without expiry <= 60s"), nil
			}
		case KCBG:
			if !admin && p.Kind != KindBG {
				return deny(ReasonBGOnly, c.Info.Name+" "+w.Raw), nil
			}
		case KCFarm:
			if !admin && p.Kind != KindFarm && p.Kind != KindManager {
				return deny(ReasonFarmOnly, w.Raw), nil
			}
		case KCAdmin:
			if !admin {
				return deny(ReasonAdminOnly, c.Info.Name+" "+w.Raw), nil
			}
		case KCProject:
			if p.Kind == KindWebDAV {
				return deny(ReasonPrincipalReadonly, c.Info.Name+" "+w.Raw), nil
			}
			u.Project = append(u.Project, w)
		default: // KCUnknown, KCChannel
			if !admin {
				return deny(ReasonUnknownKey, c.Info.Name+" "+w.Raw), nil
			}
		}
	}
	return nil, effects
}

func hasDBOption(args [][]byte) bool {
	for _, a := range args[3:] {
		if strings.EqualFold(string(a), "DB") {
			return true
		}
	}
	return false
}

// anchorCleanup ties every shared cleanup record in a project txn to an
// inode the same txn writes (review H8B-L1 #6). A record that names an
// inode or slice the txn does not otherwise touch is refused: it would let
// one project's write queue another project's data for deletion.
//
//   - delfiles / detachedNodes members ("<ino>[:len]"): the inode must be
//     written elsewhere in the txn (its i<ino>, c<ino>_n, ...).
//   - sliceRef fields ("k<id>_<size>") and delSlices (field "<id>_<ts>",
//     value = 12-byte (id,size) records): every slice must be in a chunk
//     value this txn pushes, or in a chunk list this txn both WATCHes and
//     rewrites (LTRIM/DEL), read through a side connection. WATCH makes the
//     side read sound: if the list changes before EXEC, EXEC aborts.
//
// Every inode so found is appended to u.RefInodes for Decide, and the
// record is reclassified project-scoped.
func (g *Guard) anchorCleanup(u *Unit, sc *sliceScope) ([]guardEffect, *Denial) {
	touched := sc.touched
	resolve := sc.resolve
	refs := map[uint64]bool{}
	var effects []guardEffect
	for i := range u.Writes {
		w := &u.Writes[i]
		if w.Key.Group != "gc" {
			continue
		}
		c := u.Cmds[w.Cmd]
		switch w.Key.Shape {
		case "delfiles", "detachedNodes":
			members, ok := cleanupMembers(c)
			if !ok || len(members) == 0 {
				return nil, deny(ReasonBGOnly, c.Info.Name+" "+w.Raw+": unparsable members")
			}
			for _, ino := range members {
				if !touched[ino] {
					return nil, deny(ReasonBGOnly, w.Raw+" names inode "+strconv.FormatUint(ino, 10)+" this txn does not write")
				}
				refs[ino] = true
			}
			if w.Key.Shape == "delfiles" && c.Info.Name == "ZADD" {
				for i := 3; i < len(c.Args); i += 2 {
					effects = append(effects, guardEffect{delKey: string(c.Args[i]), delAdd: true})
				}
			}
		case "sliceRef":
			fields, ok := sliceRefFields(c)
			if !ok || len(fields) == 0 {
				return nil, deny(ReasonBGOnly, c.Info.Name+" "+w.Raw+": unparsable fields")
			}
			for _, f := range fields {
				switch {
				case c.Info.Name == "HSET" && len(c.Args) == 4 && string(c.Args[3]) == "0":
					// compaction tracks its NEW slice: the exact field of a slice this unit pushed
					ino, ok := sc.pushed[f.raw]
					if !ok {
						return nil, deny(ReasonBGOnly, "sliceRef 0 for "+sanitizeToken(f.raw)+", which this txn does not push")
					}
					refs[ino] = true
					effects = append(effects, guardEffect{zeroKey: f.raw, zeroAdd: true, zero: zeroRef{owner: "", ino: ino}})
				case c.Info.Name == "HINCRBY":
					n, ok := RedisInt(c.Args[3])
					if !ok || n == 0 || n == math.MinInt64 {
						return nil, deny(ReasonBGOnly, "sliceRef HINCRBY "+sanitizeToken(string(c.Args[3])))
					}
					if n < 0 {
						sc.dec[f.raw] += -n
						continue
					}
					ino, d := resolve(f.raw) // +n is in sc.inc; it must name a slice this unit handles
					if d != nil {
						return nil, d
					}
					refs[ino] = true
				default:
					// HDEL or an arbitrary HSET would reset a reference count.
					return nil, deny(ReasonBGOnly, c.Info.Name+" "+w.Raw+" resets a reference count")
				}
			}
		case "delSlices":
			if c.Info.Name != "HSET" || len(c.Args) != 4 {
				return nil, deny(ReasonBGOnly, c.Info.Name+" "+w.Raw)
			}
			id, ok := delSliceField(c.Args[2])
			if !ok || len(c.Args[3]) == 0 || len(c.Args[3])%12 != 0 {
				return nil, deny(ReasonBGOnly, "delSlices: unparsable record")
			}
			ino, ok := sc.pushedID[id] // the field names the compaction's NEW slice
			if !ok {
				return nil, deny(ReasonBGOnly, "delSlices field names slice "+strconv.FormatUint(id, 10)+" this txn does not push")
			}
			refs[ino] = true
			// each (id,size) record is a deferred decrement of sliceRef
			// k<id>_<size> (doCleanupDelayedSlices, redis.go:3240)
			for o := 0; o < len(c.Args[3]); o += 12 {
				r := c.Args[3][o : o+12]
				sc.dec[sliceKeyOf(be64u(r[0:8]), be32u(r[8:12]))]++
			}
		default:
			return nil, deny(ReasonBGOnly, c.Info.Name+" "+w.Raw)
		}
		w.Class = KCProject
	}
	// Every decrement (now or deferred) must be paid for by a reference the
	// txn actually removes from a chunk, net of copies it pushes back, plus
	// increments in the same txn (review H8B-L1c #1: a no-op LTRIM let any
	// number of decrements through while the slice stayed referenced).
	for id, n := range sc.dec {
		ino, d := sc.payDecrement(id, n)
		if d != nil {
			return nil, d
		}
		refs[ino] = true
	}
	for ino := range refs {
		u.RefInodes = append(u.RefInodes, ino)
	}
	return effects, nil
}

// ownZeroRefDrop: a gc-only txn whose every write is HDEL sliceRef of a
// field this principal set to "0" in its own verified compaction. The drop
// is project-scoped on the inode recorded then (u.RefInodes), so L2 checks
// the principal may still write it.
func (g *Guard) ownZeroRefDrop(p Principal, u *Unit) ([]guardEffect, *Denial) {
	var effects []guardEffect
	g.mu.Lock()
	defer g.mu.Unlock()
	for i := range u.Writes {
		w := &u.Writes[i]
		c := u.Cmds[w.Cmd]
		if w.Key.Shape != "sliceRef" || c.Info.Name != "HDEL" || len(c.Args) < 3 {
			return nil, deny(ReasonBGOnly, c.Info.Name+" "+w.Raw)
		}
		for _, f := range c.Args[2:] {
			z, ok := g.zeroRefs[string(f)]
			if !ok || z.owner != p.Name {
				return nil, deny(ReasonBGOnly, "sliceRef "+sanitizeToken(string(f))+" is not a zero-ref this principal created")
			}
			u.RefInodes = append(u.RefInodes, z.ino)
			effects = append(effects, guardEffect{zeroKey: string(f)})
		}
		w.Class = KCProject
	}
	return effects, nil
}

// NoteDropped records that p's committed txn took sliceRef field f below
// zero.
func (g *Guard) NoteDropped(p Principal, f string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.dropped) > 1_000_000 {
		g.dropped = map[string]string{}
	}
	g.dropped[f] = p.Name
}

// SliceDropScript drops the named sliceRef fields only while their value
// is <= 0 (no reference beyond the implicit one, or none): a field holding
// extra references is never reset, whatever happened since the decision.
// The proxy runs it in place of a principal's bare HDEL sliceRef.
const SliceDropScript = `local n = 0
for _, f in ipairs(ARGV) do
  local v = tonumber(redis.call('HGET', KEYS[1], f))
  if v ~= nil and v <= 0 then n = n + redis.call('HDEL', KEYS[1], f) end
end
return n`

// ownSliceDrop admits a bare HDEL sliceRef whose every field this principal
// took below zero in a committed txn, or names a slice the principal
// allocated (persisted owner). The proxy executes it as SliceDropScript,
// so it can only remove fields at <= 0.
func (g *Guard) ownSliceDrop(p Principal, u *Unit) ([]guardEffect, *Denial) {
	c := u.Cmds[0]
	fs, ok := sliceRefFields(c)
	if !ok || len(fs) == 0 {
		return nil, deny(ReasonBGOnly, "HDEL sliceRef: unparsable fields")
	}
	var fields []string
	for _, f := range fs {
		g.mu.Lock()
		mine := g.dropped[f.raw] == p.Name
		g.mu.Unlock()
		if !mine {
			owner, known, err := g.slices.Owner(f.id)
			if err != nil {
				return nil, deny(ReasonBGOnly, "slice store: "+err.Error())
			}
			mine = known && owner == p.Name
		}
		if !mine {
			return nil, deny(ReasonBGOnly, "HDEL "+sanitizeToken(f.raw)+": not a slice this principal dropped or allocated")
		}
		fields = append(fields, f.raw)
	}
	rw := strArgs("EVAL", SliceDropScript, "1", "sliceRef")
	rw = append(rw, strArgs(fields...)...)
	u.rewrite = rw
	return []guardEffect{{dropDone: fields}}, nil
}

// ownDelfilesDrop admits a bare ZREM delfiles whose every member this
// principal queued itself (pendingDel).
func (g *Guard) ownDelfilesDrop(p Principal, c Cmd) ([]guardEffect, *Denial) {
	if len(c.Args) < 3 {
		return nil, deny(ReasonBGOnly, "ZREM delfiles")
	}
	var effects []guardEffect
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, m := range c.Args[2:] {
		if g.pendingDel[string(m)] != p.Name {
			return nil, deny(ReasonBGOnly, "delfiles entry "+sanitizeToken(string(m))+" was not queued by this principal")
		}
		effects = append(effects, guardEffect{delKey: string(m)})
	}
	return effects, nil
}

// sliceScope is what one unit says about slices: which slices it pushes
// into which chunk, how many references it adds and removes, and (read
// lazily from a side connection) the current content of chunks it rewrites
// under WATCH.
//
// Everything is keyed by the EXACT sliceRef field "k<id>_<size>" juicefs
// uses (redis.go sliceKey), because Redis counts each field separately: a
// +1 on k<id>_<sizeA> cannot pay for a -1 on k<id>_<sizeB> (review
// H8B-L1d #2). A chunk element names its field by its id and size bytes
// (pkg/meta/slice.go marshalSlice: pos u32, id u64, size u32, off, len).
//
// Reference accounting (juicefs keeps sliceRef = references - 1; a chunk
// element is one reference). For each field F in a unit:
//
//	removed(F)  copies of F the unit's LTRIM/DEL actually drops from a WATCHed
//	            chunk (side read + trim range); only the FIRST write to that
//	            chunk in the unit may be the LTRIM/DEL, so indices are exact
//	inc(F)      sum of positive HINCRBY sliceRef F
//	pushes      each pushed copy must be (a) the FIRST link of a slice
//	            allocated to the principal, (b) paid by one inc, or (c) paid
//	            by one removed copy from the same chunk
//	dec(F)      sum of negative HINCRBY F plus delSlices records naming F;
//	            must be <= the removed + inc left after (b)/(c) pushes
type sliceScope struct {
	g         *Guard
	u         *Unit
	p         Principal
	touched   map[uint64]bool   // inodes the unit writes (non-gc keys)
	pushed    map[string]uint64 // field -> chunk inode, validated pushes only
	pushedID  map[uint64]uint64 // slice id -> chunk inode, validated pushes only
	inc       map[string]int64  // remaining positive sliceRef increments
	dec       map[string]int64  // decrements requested
	rewritten map[string]trimOp // WATCHed chunk keys whose first write is LTRIM/DEL
	removed   map[string]int64  // remaining removed copies, filled by sideRead
	removedIn map[string]string // field -> chunk key it was removed from
	firstUse  []uint64          // self-allocated slices linked for the first time
	read      bool
	readErr   *Denial
	badNumber *Denial
}

type trimOp struct {
	del         bool
	start, stop int64
}

// sliceKeyOf is juicefs's sliceRef field for a slice.
func sliceKeyOf(id uint64, size uint32) string {
	return "k" + strconv.FormatUint(id, 10) + "_" + strconv.FormatUint(uint64(size), 10)
}

type sliceRefEntry struct {
	id   uint64
	size uint32
}

// chunkElem parses one chunk list element: exactly one marshalled slice
// (juicefs readSlices treats any other length as a corrupt chunk).
func chunkElem(v []byte) (sliceRefEntry, bool) {
	if len(v) != 24 {
		return sliceRefEntry{}, false
	}
	return sliceRefEntry{id: be64u(v[4:12]), size: be32u(v[12:16])}, true
}

func (g *Guard) newSliceScope(u *Unit, p Principal) *sliceScope {
	sc := &sliceScope{g: g, u: u, p: p, touched: map[uint64]bool{}, pushed: map[string]uint64{}, pushedID: map[uint64]uint64{},
		inc: map[string]int64{}, dec: map[string]int64{}, rewritten: map[string]trimOp{}}
	watched := map[string]bool{}
	for _, k := range u.Watched {
		watched[k] = true
	}
	firstWrite := map[string]bool{}
	for _, w := range u.Writes {
		c := u.Cmds[w.Cmd]
		if w.Key.Shape == "sliceRef" && c.Info.Name == "HINCRBY" && len(c.Args) == 4 {
			n, ok := RedisInt(c.Args[3])
			if !ok {
				sc.badNumber = deny(ReasonUnknownKey, "HINCRBY increment "+sanitizeToken(string(c.Args[3]))+" is not a Redis integer")
			} else if n > 0 {
				if fs, ok := sliceRefFields(c); ok {
					for _, f := range fs {
						sc.inc[f.raw] += n
					}
				}
			}
		}
		if w.Key.Group == "gc" || !w.Key.HasIno {
			continue
		}
		sc.touched[w.Key.Ino] = true
		if w.Key.Shape != ShapeChunk {
			continue
		}
		seen := firstWrite[w.Raw]
		firstWrite[w.Raw] = true
		if seen {
			if _, ok := sc.rewritten[w.Raw]; ok && c.Info.Name != "RPUSH" && c.Info.Name != "LPUSH" {
				delete(sc.rewritten, w.Raw) // a second trim/del: indices no longer exact
			}
			continue
		}
		if c.Info.Name == "LTRIM" {
			if len(c.Args) != 4 {
				sc.badNumber = deny(ReasonUnknownKey, "LTRIM arity")
				continue
			}
			st, ok1 := RedisInt(c.Args[2])
			sp, ok2 := RedisInt(c.Args[3])
			if !ok1 || !ok2 {
				sc.badNumber = deny(ReasonUnknownKey, "LTRIM index is not a Redis integer")
				continue
			}
			if watched[w.Raw] {
				sc.rewritten[w.Raw] = trimOp{start: st, stop: sp}
			}
			continue
		}
		if !watched[w.Raw] {
			continue
		}
		switch c.Info.Name {
		case "DEL", "UNLINK":
			sc.rewritten[w.Raw] = trimOp{del: true}
		}
	}
	return sc
}

// sideRead loads every rewritten chunk once and counts the copies of each
// slice field its LTRIM/DEL removes. Before the first side read the
// connection's WATCH must have reached Redis (Unit.beforeSide), or a change
// made after this read would not abort the EXEC (review H8B-L1d #4).
func (sc *sliceScope) sideRead() *Denial {
	if sc.read || sc.readErr != nil {
		return sc.readErr
	}
	sc.read = true
	sc.removed = map[string]int64{}
	sc.removedIn = map[string]string{}
	if len(sc.rewritten) > 0 && sc.u.beforeSide != nil {
		if err := sc.u.beforeSide(); err != nil {
			sc.readErr = deny(ReasonBGOnly, "WATCH not confirmed before side read: "+err.Error())
			return sc.readErr
		}
	}
	for key, op := range sc.rewritten {
		if sc.g.chunks == nil {
			break
		}
		elems, err := sc.g.chunks.ChunkElems(sc.u.DB, key)
		if err != nil {
			sc.readErr = deny(ReasonBGOnly, "side read of "+key+": "+err.Error())
			return sc.readErr
		}
		keep := func(int64) bool { return false }
		if !op.del {
			n := int64(len(elems))
			s, e := op.start, op.stop
			if s < 0 {
				s += n
			}
			if e < 0 {
				e += n
			}
			if s < 0 {
				s = 0
			}
			keep = func(i int64) bool { return i >= s && i <= e }
		}
		for i, el := range elems {
			if keep(int64(i)) {
				continue
			}
			if se, ok := chunkElem(el); ok && se.id != 0 {
				f := sliceKeyOf(se.id, se.size)
				sc.removed[f]++
				sc.removedIn[f] = key
			}
		}
	}
	if sc.g.afterSideRead != nil {
		sc.g.afterSideRead()
	}
	return nil
}

// validatePushes: every slice a unit pushes into a chunk must be one the
// principal may reference (reviews H8B-L1b #1, L1c #1, L1d #2/#3):
//
//	(a) the FIRST link of a slice id the proxy relayed to THIS principal
//	    from INCRBY nextchunk (SliceStore owner, used bit clear, and not
//	    already pushed earlier in this unit); only inside MULTI, so the used
//	    bit is set atomically with the link;
//	(b) paid by one sliceRef increment of the same field in this unit
//	    (juicefs copy_file_range / clone);
//	(c) paid by one copy of the same field that this unit's LTRIM removes
//	    from the SAME chunk (compaction re-pushes its skipped head slices).
//
// Slice id 0 (a hole) is always fine.
func (sc *sliceScope) validatePushes(p Principal) *Denial {
	if sc.badNumber != nil {
		return sc.badNumber
	}
	thisUnit := map[uint64]bool{}
	for _, w := range sc.u.Writes {
		if w.Key.Shape != ShapeChunk {
			continue
		}
		c := sc.u.Cmds[w.Cmd]
		switch c.Info.Name {
		case "RPUSH", "LPUSH", "RPUSHX", "LPUSHX":
		case "LTRIM", "DEL", "UNLINK":
			continue
		default:
			return deny(ReasonAdminOnly, c.Info.Name+" on a chunk list")
		}
		for _, v := range c.Args[2:] {
			se, ok := chunkElem(v)
			if !ok {
				return deny(ReasonUnknownKey, "chunk element is not one marshalled slice")
			}
			if se.id == 0 {
				continue
			}
			f := sliceKeyOf(se.id, se.size)
			paid := false
			if sc.u.Kind == UnitTxn && !thisUnit[se.id] {
				owner, known, err := sc.g.slices.Owner(se.id)
				if err != nil {
					return deny(ReasonBGOnly, "slice store: "+err.Error())
				}
				if known && owner == p.Name {
					used, err := sc.g.slices.Used(se.id)
					if err != nil {
						return deny(ReasonBGOnly, "slice store: "+err.Error())
					}
					if !used {
						paid = true
						sc.firstUse = append(sc.firstUse, se.id)
					}
				}
			}
			if !paid && sc.inc[f] > 0 {
				sc.inc[f]--
				paid = true
			}
			if !paid {
				if d := sc.sideRead(); d != nil {
					return d
				}
				if sc.removed[f] > 0 && sc.removedIn[f] == w.Raw {
					sc.removed[f]--
					paid = true
				}
			}
			if !paid {
				return deny(ReasonAdminOnly, "slice "+sanitizeToken(f)+" is neither a first link of this principal's allocation nor paid by an increment or a removed copy")
			}
			thisUnit[se.id] = true
			sc.pushed[f] = w.Key.Ino
			sc.pushedID[se.id] = w.Key.Ino
		}
	}
	return nil
}

// payDecrement charges n decrements of field f against the references the
// unit removes and the increments it makes; it returns the chunk inode the
// slice belongs to.
func (sc *sliceScope) payDecrement(f string, n int64) (uint64, *Denial) {
	if d := sc.sideRead(); d != nil {
		return 0, d
	}
	avail := sc.removed[f] + sc.inc[f]
	if n > avail {
		return 0, deny(ReasonBGOnly, sanitizeToken(f)+": "+strconv.FormatInt(n, 10)+
			" decrements but the txn removes only "+strconv.FormatInt(avail, 10)+" references")
	}
	if key, ok := sc.removedIn[f]; ok {
		return ParseKey(key).Ino, nil
	}
	if ino, ok := sc.pushed[f]; ok {
		return ino, nil
	}
	return 0, deny(ReasonBGOnly, "decrement of "+sanitizeToken(f)+", a slice of no chunk this txn writes")
}

// resolve maps a sliceRef field named by an increment to the chunk inode
// that holds it: a validated push in this unit, or a chunk the unit rewrites.
func (sc *sliceScope) resolve(f string) (uint64, *Denial) {
	if ino, ok := sc.pushed[f]; ok {
		return ino, nil
	}
	if d := sc.sideRead(); d != nil {
		return 0, d
	}
	if key, ok := sc.removedIn[f]; ok {
		return ParseKey(key).Ino, nil
	}
	return 0, deny(ReasonBGOnly, "increment of "+sanitizeToken(f)+", a slice of no chunk this txn writes")
}

type sliceField struct {
	raw string
	id  uint64
}

// sliceRefFields parses the k<id>_<size> fields of a sliceRef write.
func sliceRefFields(c Cmd) ([]sliceField, bool) {
	var raws [][]byte
	switch c.Info.Name {
	case "HSET", "HMSET":
		if len(c.Args) < 4 || len(c.Args)%2 != 0 {
			return nil, false
		}
		for i := 2; i < len(c.Args); i += 2 {
			raws = append(raws, c.Args[i])
		}
	case "HINCRBY", "HSETNX":
		if len(c.Args) != 4 {
			return nil, false
		}
		raws = append(raws, c.Args[2])
	case "HDEL":
		raws = c.Args[2:]
	default:
		return nil, false
	}
	var out []sliceField
	for _, r := range raws {
		s := string(r)
		if len(s) < 4 || s[0] != 'k' {
			return nil, false
		}
		u := strings.IndexByte(s, '_')
		if u < 2 {
			return nil, false
		}
		id, ok := digits(s[1:u])
		if _, ok2 := digits(s[u+1:]); !ok || !ok2 {
			return nil, false
		}
		out = append(out, sliceField{raw: s, id: id})
	}
	return out, true
}

func delSliceField(b []byte) (uint64, bool) {
	s := string(b)
	u := strings.IndexByte(s, '_')
	if u < 1 {
		return 0, false
	}
	id, ok := digits(s[:u])
	if _, ok2 := digits(s[u+1:]); !ok || !ok2 {
		return 0, false
	}
	return id, true
}

// cleanupMembers parses delfiles/detachedNodes ZADD/ZREM members to inodes.
// Every member must parse; ZADD options are not used by juicefs.
func cleanupMembers(c Cmd) ([]uint64, bool) {
	var raws [][]byte
	switch c.Info.Name {
	case "ZADD":
		if len(c.Args) < 4 || len(c.Args)%2 != 0 {
			return nil, false
		}
		for i := 2; i < len(c.Args); i += 2 {
			if _, ok := RedisInt(c.Args[i]); !ok { // juicefs scores are unix seconds
				return nil, false
			}
			raws = append(raws, c.Args[i+1])
		}
	case "ZREM":
		raws = c.Args[2:]
	default:
		return nil, false
	}
	var out []uint64
	for _, r := range raws {
		if i := bytes.IndexByte(r, ':'); i > 0 {
			if _, ok := digits(string(r[i+1:])); !ok {
				return nil, false
			}
			r = r[:i]
		}
		n, ok := digits(string(r))
		if !ok {
			return nil, false
		}
		out = append(out, n)
	}
	return out, true
}

func be32u(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func be64u(b []byte) uint64 {
	var n uint64
	for _, c := range b[:8] {
		n = n<<8 | uint64(c)
	}
	return n
}

// integerArgsOK checks every argument Redis parses as an integer with
// string2ll for the commands the guard reasons about (review H8B-L1d #1).
func integerArgsOK(c Cmd) bool {
	var idx []int
	switch c.Info.Name {
	case "INCRBY", "DECRBY", "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT":
		idx = []int{2}
	case "HINCRBY", "LTRIM", "SETEX", "PSETEX":
		if c.Info.Name == "SETEX" || c.Info.Name == "PSETEX" {
			idx = []int{2}
		} else {
			idx = []int{2, 3}
			if c.Info.Name == "HINCRBY" {
				idx = []int{3}
			}
		}
	}
	for _, i := range idx {
		if i >= len(c.Args) {
			return false
		}
		if _, ok := RedisInt(c.Args[i]); !ok {
			return false
		}
	}
	return true
}

func isHSet(name string) bool { return name == "HSET" || name == "HSETNX" || name == "HMSET" }

// sessionGate: a principal may write only the juicefs sessions allocated to
// it (SessionStore).
//
// CUTOVER (runbook, lane L5): a session a mount registered BEFORE the proxy
// existed has no owner record, so every heartbeat or session write from it
// is refused (BG_ONLY), as is any slice from a pre-proxy nextchunk batch.
// Every mount must remount through the proxy at cutover: restart the
// JuiceMount app on every Mac (and the server's juicefs mounts) in the same
// step that moves port 30179 to the proxy. Removing or rewriting another principal's session is
// the bg principal's CleanStaleSessions job; a sid with no recorded owner is
// bg-only too (fail closed, also after a proxy restart). Whole-key writes to
// the session indexes (DEL allSessions, ...) are bg-only.
func (g *Guard) sessionGate(p Principal, c Cmd, w KeyWrite) ([]guardEffect, *Denial) {
	name := c.Info.Name
	sids := sessionIDs(name, w.Key, c.Args)
	var effects []guardEffect
	if w.Key.Shape == "allSessions" && name == "ZREM" && len(sids) > 0 {
		effects = append(effects, guardEffect{release: sids})
	}
	if p.Kind == KindAdmin || p.Kind == KindBG {
		return effects, nil
	}
	global := w.Key.Shape != ShapeSession && w.Key.Shape != ShapeLocked
	if global {
		switch name {
		case "ZADD", "ZREM", "HSET", "HMSET", "HSETNX", "HDEL", "HINCRBY":
		default:
			return nil, deny(ReasonBGOnly, name+" on session index "+w.Raw)
		}
	}
	if len(sids) == 0 {
		return nil, deny(ReasonBGOnly, name+" "+w.Raw+" names no session")
	}
	for _, sid := range sids {
		owner, ok, err := g.sessions.Owner(sid)
		switch {
		case err != nil:
			return nil, deny(ReasonBGOnly, "session store: "+err.Error())
		case !ok:
			return nil, deny(ReasonBGOnly, "session "+strconv.FormatUint(sid, 10)+" was not allocated through this proxy")
		case owner != p.Name:
			return nil, deny(ReasonBGOnly, "session "+strconv.FormatUint(sid, 10)+" belongs to "+owner)
		}
	}
	return effects, nil
}

// linkBenchOK admits bridge/cbridge.go's link benchmark: SET with EX<=60 or
// PX<=60000, and its DEL cleanup.
func linkBenchOK(c Cmd) bool {
	switch c.Info.Name {
	case "DEL", "UNLINK":
		return true
	case "SET":
	default:
		return false
	}
	for i := 3; i+1 < len(c.Args); i++ {
		opt := strings.ToUpper(string(c.Args[i]))
		n, ok := RedisInt(c.Args[i+1])
		if !ok || n <= 0 {
			continue
		}
		switch opt {
		case "EX":
			return n <= 60
		case "PX":
			return n <= 60000
		}
	}
	return false
}

// safeConfigGet admits CONFIG GET of named, non-secret parameters (juicefs
// reads maxmemory-policy; JuiceMount reads notify-keyspace-events).
func safeConfigGet(args [][]byte) bool {
	if len(args) < 3 || !strings.EqualFold(string(args[1]), "GET") {
		return false
	}
	for _, a := range args[2:] {
		s := strings.ToLower(string(a))
		if strings.ContainsAny(s, "*?[") {
			return false
		}
		for _, bad := range []string{"pass", "user", "acl", "tls", "auth", "dir", "dbfilename", "logfile"} {
			if strings.Contains(s, bad) {
				return false
			}
		}
	}
	return true
}

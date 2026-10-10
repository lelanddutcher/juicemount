package respproxy

// Engine tests without FUSE: hand-built JuiceFS trees in a disposable Redis
// and hand-built juicefs-shaped units through a real proxy. The mount-level
// acceptance tests are in acceptance_h8b_test.go (tag h8b_acceptance).

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *testClock) Add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func mkAttr(typ byte, parent uint64) string {
	b := make([]byte, 72)
	b[1] = typ << 4
	copy(b[63:71], be64(parent))
	return string(b)
}

func mkEntry(typ byte, ino uint64) string { return string(append([]byte{typ}, be64(ino)...)) }

// Tree: / (1) -> Projects (2) -> A (3), B (4); A/f (10), B/g (11),
// A/sub (5) -> A/sub/h (12).
func buildTree(t *testing.T, addr string) {
	rdb := direct(t, addr)
	ctx := ctx5(t)
	pipe := rdb.Pipeline()
	pipe.Set(ctx, "i1", mkAttr(typeDir, 1), 0)
	pipe.Set(ctx, "i2", mkAttr(typeDir, 1), 0)
	pipe.Set(ctx, "i3", mkAttr(typeDir, 2), 0)
	pipe.Set(ctx, "i4", mkAttr(typeDir, 2), 0)
	pipe.Set(ctx, "i5", mkAttr(typeDir, 3), 0)
	pipe.Set(ctx, "i10", mkAttr(typeFile, 3), 0)
	pipe.Set(ctx, "i11", mkAttr(typeFile, 4), 0)
	pipe.Set(ctx, "i12", mkAttr(typeFile, 5), 0)
	pipe.Set(ctx, "i13", mkAttr(typeFile, 0), 0) // hard link: A/hl and B/hl
	pipe.HSet(ctx, "d1", "Projects", mkEntry(typeDir, 2))
	pipe.HSet(ctx, "d2", "A", mkEntry(typeDir, 3), "B", mkEntry(typeDir, 4))
	pipe.HSet(ctx, "d3", "f", mkEntry(typeFile, 10), "sub", mkEntry(typeDir, 5), "hl", mkEntry(typeFile, 13))
	pipe.HSet(ctx, "d4", "g", mkEntry(typeFile, 11), "hl", mkEntry(typeFile, 13))
	pipe.HSet(ctx, "d5", "h", mkEntry(typeFile, 12))
	pipe.HSet(ctx, "p13", "3", "1", "4", "1")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
}

type leaseRig struct {
	tp  *testProxy
	eng *LeaseEngine
	clk *testClock
}

func startLeaseRig(t *testing.T, mode Mode, mut ...func(*LeaseConfig)) *leaseRig {
	return startLeaseRigP(t, mode, nil, mut...)
}

func startLeaseRigP(t *testing.T, mode Mode, pmut func(*Config), mut ...func(*LeaseConfig)) *leaseRig {
	addr := redisAddr(t, false)
	buildTree(t, addr)
	clk := &testClock{t: time.Now()}
	sink := &logSink{t: t}
	pc := Config{Upstream: addr, Principals: principalsForTest(t), Mode: mode, VolumeDB: testDB,
		Logf: sink.Logf, ModePoll: 50 * time.Millisecond}
	if pmut != nil {
		pmut(&pc)
	}
	p, err := New(pc)
	if err != nil {
		t.Fatal(err)
	}
	lc := LeaseConfig{Side: p.Side(), VolumeDB: testDB, Now: clk.Now, Logf: sink.Logf,
		SweepEvery: time.Hour, FlushEvery: 50 * time.Millisecond}
	for _, m := range mut {
		m(&lc)
	}
	eng := NewLeaseEngine(lc)
	if err := eng.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(eng.Close)
	p.SetPolicy(eng)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go p.Serve(ln)
	t.Cleanup(func() { p.Close() })
	return &leaseRig{tp: &testProxy{Proxy: p, addr: ln.Addr().String(), logs: sink}, eng: eng, clk: clk}
}

func (r *leaseRig) conn(t *testing.T, name string) *rawConn {
	rc := r.tp.raw(t)
	rc.login(name)
	return rc
}

// txn sends MULTI <cmds> EXEC and returns the EXEC reply.
func txn(rc *rawConn, cmds ...[]string) Value {
	rc.send("MULTI")
	for _, c := range cmds {
		rc.send(c...)
	}
	rc.send("EXEC")
	rc.flush()
	rc.read()
	for range cmds {
		rc.read()
	}
	return rc.read()
}

// treeParent is buildTree's directory -> parent map.
var treeParent = map[uint64]uint64{1: 1, 2: 1, 3: 2, 4: 2, 5: 3}

// create mimics juicefs doMknod of a file.
func create(rc *rawConn, parent, ino uint64, name string) Value {
	return txn(rc,
		[]string{"SET", "i" + inoStr(ino), mkAttr(typeFile, parent)},
		[]string{"SET", "i" + inoStr(parent), mkAttr(typeDir, treeParent[parent])}, // parent mtime
		[]string{"HSET", "d" + inoStr(parent), name, mkEntry(typeFile, ino)},
		[]string{"INCRBY", "usedSpace", "0"},
		[]string{"INCR", "totalInodes"})
}

func TestLeaseAnalyzeUnit(t *testing.T) {
	cmd := func(a ...string) Cmd {
		args := strArgs(a...)
		return Cmd{Args: args, Info: Classify(args)}
	}
	unit := func(cmds ...Cmd) *Unit {
		u := &Unit{Kind: UnitTxn, Cmds: cmds}
		NewGuard(nil, nil).Check(Principal{Name: "m", Kind: KindMac}, u)
		return u
	}
	trashDir := uint64(TrashInode + 1)
	// unlink into trash: one HDEL, one trash placement, inode 10 written.
	f := analyzeUnit(unit(
		cmd("HDEL", "d3", "f"),
		cmd("SET", "i10", mkAttr(typeFile, trashDir)),
		cmd("HSET", "d"+inoStr(trashDir), "3-10-f", mkEntry(typeFile, 10))))
	if f.topology || len(f.dentries) != 2 || len(f.inodes) != 1 || f.inodes[0] != 10 || f.setAttr[10].parent != trashDir {
		t.Fatalf("unlink: %+v", f)
	}
	// directory moved between parents: topology; the edge comes from the
	// dentry, never from the Attr value.
	f = analyzeUnit(unit(
		cmd("HDEL", "d3", "sub"),
		cmd("SET", "i5", mkAttr(typeDir, 4)),
		cmd("HSET", "d4", "sub", mkEntry(typeDir, 5))))
	if !f.topology || len(f.edges) != 1 || f.edges[0].parent != 4 || f.edges[0].child != 5 {
		t.Fatalf("dir move: %+v", f)
	}
	// rename within one directory is not a topology change.
	f = analyzeUnit(unit(cmd("HDEL", "d3", "sub"), cmd("HSET", "d3", "sub2", mkEntry(typeDir, 5))))
	if f.topology {
		t.Fatal("same-parent rename marked as topology")
	}
	// a bare dir-stat flush.
	u := &Unit{Kind: UnitBare, Cmds: []Cmd{cmd("HINCRBY", "dirUsedSpace", "3", "4096")}}
	NewGuard(nil, nil).Check(Principal{Name: "m", Kind: KindMac}, u)
	if f = analyzeUnit(u); !f.dirStatOnly() || len(f.dirStat) != 1 || f.dirStat[0] != 3 {
		t.Fatalf("dir-stat: %+v", f)
	}
}

func TestLeaseWireFormat(t *testing.T) {
	r := startLeaseRig(t, ModeEnforce)
	mac1, mac2, admin := r.conn(t, "mac1"), r.conn(t, "mac2"), r.conn(t, "admin1")
	acq := func(rc *rawConn, args ...string) Value {
		t.Helper()
		v := rc.do(append([]string{"JM.LEASE", "ACQUIRE"}, args...)...)
		if v.Type != '*' || len(v.Elems) != 5 {
			t.Fatalf("ACQUIRE %v: %c %q", args, v.Type, v.Str)
		}
		return v
	}
	v := acq(mac1, "path:/Projects/A/sub/deeper/ignored", "auto")
	if string(v.Elems[0].Str) != "GRANTED" || v.Elems[1].Int != 1 || string(v.Elems[2].Str) != "mac1@mac1-host" || v.Elems[3].Int <= 0 || v.Elems[4].Int != 0 {
		t.Fatalf("first ACQUIRE: %+v", v.Elems)
	}
	if v = acq(mac1, "ino:12", "auto"); string(v.Elems[0].Str) != "RENEWED" || v.Elems[1].Int != 1 {
		t.Fatalf("renew via a file inode in the project: %+v", v.Elems)
	}
	if v = acq(mac2, "ino:3", "checkout", "60"); string(v.Elems[0].Str) != "HELD" || string(v.Elems[2].Str) != "mac1@mac1-host" {
		t.Fatalf("ACQUIRE of a held project: %+v", v.Elems)
	}
	// STATUS
	st := mac2.do("JM.LEASE", "STATUS", "ino:3")
	if st.Type != '*' || len(st.Elems) != 30 || string(st.Elems[0].Str) != "proj" || st.Elems[1].Int != 3 {
		t.Fatalf("STATUS: %c %v", st.Type, st.Elems)
	}
	all := mac2.do("JM.LEASE", "STATUS", "*")
	if all.Type != '*' || len(all.Elems) != 1 {
		t.Fatalf("STATUS *: %v", all.Elems)
	}
	// HEARTBEAT: one held entry, one lost entry for a pending project not held.
	hb := mac1.do("JM.LEASE", "HEARTBEAT", "ino:3", "ino:4")
	if hb.Type != '*' || len(hb.Elems) != 2 || hb.Elems[0].Elems[0].Int != 3 || string(hb.Elems[0].Elems[2].Str) != "held" ||
		hb.Elems[1].Elems[0].Int != 4 || string(hb.Elems[1].Elems[2].Str) != "lost" {
		t.Fatalf("HEARTBEAT: %+v", hb.Elems)
	}
	// RELEASE: wrong holder, stale epoch, then the real one.
	if v = mac2.do("JM.LEASE", "RELEASE", "ino:3", "1"); string(v.Elems[0].Str) != "NOT_HOLDER" {
		t.Fatalf("RELEASE by non-holder: %+v", v.Elems)
	}
	if v = mac1.do("JM.LEASE", "RELEASE", "ino:3", "7"); string(v.Elems[0].Str) != "STALE_EPOCH" {
		t.Fatalf("RELEASE stale epoch: %+v", v.Elems)
	}
	if v = mac1.do("JM.LEASE", "RELEASE", "ino:3", "1"); string(v.Elems[0].Str) != "RELEASED" || v.Elems[1].Int != 2 {
		t.Fatalf("RELEASE: %+v", v.Elems)
	}
	// mac2 now; mac1 had held it more recently than mac2 ever did.
	if v = acq(mac2, "ino:3", "auto"); string(v.Elems[0].Str) != "GRANTED" || v.Elems[1].Int != 3 || v.Elems[4].Int != 1 {
		t.Fatalf("ACQUIRE after release: %+v", v.Elems)
	}
	// BREAK: admin only.
	mustDeny(t, mac1.do("JM.LEASE", "BREAK", "ino:3"), ReasonAdminOnly, "BREAK by a Mac")
	if v = admin.do("JM.LEASE", "BREAK", "ino:3"); string(v.Elems[0].Str) != "BROKEN" || v.Elems[1].Int != 4 || string(v.Elems[2].Str) != "mac2@mac2-host" {
		t.Fatalf("BREAK: %+v", v.Elems)
	}
	if v = admin.do("JM.LEASE", "BREAK", "ino:3"); string(v.Elems[0].Str) != "FREE" {
		t.Fatalf("BREAK of a free project: %+v", v.Elems)
	}
	if v = acq(mac1, "ino:3", "auto"); v.Elems[4].Int != 1 {
		t.Fatalf("foreign_since after mac2's tenure: %+v", v.Elems)
	}
	// Errors.
	for _, bad := range [][]string{
		{"JM.LEASE", "ACQUIRE", "path:/Projects", "auto"},
		{"JM.LEASE", "ACQUIRE", "ino:2", "auto"},
		{"JM.LEASE", "ACQUIRE", "3", "auto"},
		{"JM.LEASE", "ACQUIRE", "ino:3", "forever"},
		{"JM.LEASE", "NOPE"},
	} {
		if v := mac1.do(bad...); v.Type != '-' || string(v.Str[:4]) != "ERR " {
			t.Fatalf("%v: %c %q", bad, v.Type, v.Str)
		}
	}
	mustDeny(t, r.conn(t, "dav1").do("JM.LEASE", "ACQUIRE", "ino:3", "auto"), ReasonPrincipalReadonly, "webdav ACQUIRE")
	// The lease survives in Redis.
	h, err := r.tp.client(t, "admin1").HGetAll(ctx5(t), LeaseKeyPrefix+"3").Result()
	if err != nil || h["holder"] != "mac1" || h["epoch"] != "5" || h["e:mac2"] != "3" || h["label"] != "/Projects/A" {
		t.Fatalf("persisted lease: %v %v", h, err)
	}
}

type failSecondProjectGrant struct {
	inner sideDoer
	mu    sync.Mutex
	fired int
}

func (s *failSecondProjectGrant) Do(db int, args ...[]byte) (Value, error) {
	if len(args) > 5 && string(args[0]) == "EVAL" && string(args[1]) == leaseTransitionLua &&
		string(args[2]) == "2" && string(args[3]) == epochKey(4) && string(args[4]) == leaseKey(4) && string(args[5]) == "grant" {
		s.mu.Lock()
		s.fired++
		s.mu.Unlock()
		return Value{}, errors.New("injected second-project grant failure")
	}
	return s.inner.Do(db, args...)
}

func TestLeaseCrossProjectAllOrNone(t *testing.T) {
	move := [][]string{
		{"HDEL", "d3", "f"},
		{"SET", "i10", mkAttr(typeFile, 4)},
		{"HSET", "d4", "f", mkEntry(typeFile, 10)},
	}
	t.Run("failed_second_grant_rolls_back", func(t *testing.T) {
		ss := &failSecondProjectGrant{}
		r := startLeaseRig(t, ModeEnforce, func(c *LeaseConfig) { ss.inner = c.Side; c.Side = ss })
		mac1 := r.conn(t, "mac1")
		if v := txn(mac1, move...); v.Type != '-' {
			t.Fatal("move with a failing B grant was admitted")
		}
		ss.mu.Lock()
		fired := ss.fired
		ss.mu.Unlock()
		if fired != 1 {
			t.Fatalf("second-project grant failure fired %d times, want 1", fired)
		}
		if h := r.eng.holderOf(3); h != "" {
			t.Fatalf("all-or-none: A stayed granted to %q after B's grant failed", h)
		}
		r.eng.mu.Lock()
		unresolved := r.eng.leases[4] != nil && r.eng.leases[4].unresolved
		r.eng.mu.Unlock()
		if !unresolved {
			t.Fatal("failed B grant did not quarantine B")
		}
		mustDeny(t, mac1.do("HSET", "x11", "k", "v"), ReasonLeaseHeld, "write to unresolved B")
		if v := mac1.do("JM.LEASE", "ACQUIRE", "ino:4", "auto"); v.Type != '-' {
			t.Fatalf("ACQUIRE of unresolved B: %+v", v)
		}
		db := directLeaseView(t, r)
		ctx := ctx5(t)
		if v, err := db.HGet(ctx, "d3", "f").Result(); err != nil || v != mkEntry(typeFile, 10) {
			t.Fatalf("refused move changed source: %q %v", v, err)
		}
		if ok, err := db.HExists(ctx, "d4", "f").Result(); err != nil || ok {
			t.Fatalf("refused move created destination: %v %v", ok, err)
		}
		if v, err := db.Get(ctx, "i10").Result(); err != nil || v != mkAttr(typeFile, 3) {
			t.Fatalf("refused move changed inode: %q %v", v, err)
		}
	})
	// Uncertain authority stays quarantined until coherent recovery; use a
	// fresh fixture for the independent healthy cross-project cases.
	t.Run("healthy_cross_project_policy", func(t *testing.T) {
		r := startLeaseRig(t, ModeEnforce)
		mac1, mac2 := r.conn(t, "mac1"), r.conn(t, "mac2")
		// B held by mac2: the move needs both, so it is refused and grants nothing.
		mustOK(t, create(mac2, 4, 20, "mine"), "mac2 create in B")
		mustDeny(t, txn(mac1, move...), ReasonXProject, "cross-project move")
		if h := r.eng.holderOf(3); h != "" {
			t.Fatalf("refused unit left A granted to %s", h)
		}
		ep := mac2.do("JM.LEASE", "STATUS", "ino:4").Elems[3].Int
		mustOK(t, mac2.do("JM.LEASE", "RELEASE", "ino:4", strconv.FormatInt(ep, 10)), "mac2 releases B")

		// mac1 holds both A and B; the hard link with parents A and B is still
		// refused (plan: hard-link parents must be in one project).
		mustOK(t, mac1.do("HSET", "x10", "k", "v"), "mac1 takes A")
		mustOK(t, mac1.do("HSET", "x11", "k", "v"), "mac1 takes B")
		mustDeny(t, mac1.do("HSET", "x13", "user.k", "v"), ReasonXProject, "setxattr on a cross-project hard link")
		// An inode nobody can place.
		mustDeny(t, mac1.do("HSET", "x999", "user.k", "v"), ReasonUnknownKey, "setxattr on an unknown inode")
		// A deep file resolves through side reads (no cached edge).
		mustOK(t, mac1.do("HSET", "x12", "user.k", "v"), "mac1 setxattr deep in A")
	})
}

func TestLeaseRootEntryGate(t *testing.T) {
	r := startLeaseRig(t, ModeEnforce)
	mac1 := r.conn(t, "mac1")
	// Rename of a project root: refused for a Mac.
	v := txn(mac1, []string{"HDEL", "d2", "A"}, []string{"HSET", "d2", "A2", mkEntry(typeDir, 3)})
	mustDeny(t, v, ReasonAdminOnly, "rename of a project root")
	// Creating a new project folder is open, and does not grant it.
	v = txn(mac1,
		[]string{"SET", "i30", mkAttr(typeDir, 2)},
		[]string{"SET", "i2", mkAttr(typeDir, 1)},
		[]string{"HSET", "d2", "C", mkEntry(typeDir, 30)},
		[]string{"HSET", "dirUsedInodes", "30", "0"})
	mustOK(t, v, "create project folder")
	if r.eng.holderOf(30) != "" {
		t.Fatal("creating a project folder granted it")
	}
	// The admin may rename one.
	admin := r.conn(t, "admin1")
	v = txn(admin, []string{"HDEL", "d2", "C"}, []string{"HSET", "d2", "C2", mkEntry(typeDir, 30)})
	mustOK(t, v, "admin rename of a project root")
}

func TestLeaseDenyLogSampled(t *testing.T) {
	r := startLeaseRig(t, ModeEnforce)
	mac1, mac2 := r.conn(t, "mac1"), r.conn(t, "mac2")
	mustOK(t, mac1.do("HSET", "x10", "k", "v"), "mac1")
	for i := 0; i < 20; i++ {
		mustDeny(t, mac2.do("HSET", "x10", "k", strconv.Itoa(i)), ReasonLeaseHeld, "mac2")
	}
	adm := r.tp.client(t, "admin1")
	deadline := time.Now().Add(2 * time.Second)
	for {
		ents, _ := adm.XRange(ctx5(t), LeaseLogKey, "-", "+").Result()
		deny, grant := 0, 0
		for _, e := range ents {
			switch e.Values["event"] {
			case "deny":
				deny++
			case "grant":
				grant++
			}
		}
		if grant == 1 && deny == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("log: %d grant, %d deny entries; want 1 and 1 (sampled)", grant, deny)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *LeaseEngine) holderOf(proj uint64) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if l := e.leases[proj]; l != nil {
		return l.holder
	}
	return ""
}

// mkSlice is one marshalled chunk-list slice (pos, id, size, off, len).
func mkSlice(id uint64, size uint32) string {
	b := make([]byte, 24)
	copy(b[4:12], be64(id))
	b[12], b[13], b[14], b[15] = byte(size>>24), byte(size>>16), byte(size>>8), byte(size)
	b[20], b[21], b[22], b[23] = byte(size>>24), byte(size>>16), byte(size>>8), byte(size)
	return string(b)
}

// Shared cleanup records (delfiles, sliceRef) are project-scoped on the
// inode they name (L1's Unit.RefInodes): a Mac cannot use them on another
// project's data, and a zero-ref drop it recorded while holding a project
// is refused once the project has moved to another holder.
func TestLeaseRefInodesRequireLease(t *testing.T) {
	r := startLeaseRig(t, ModeEnforce)
	mac1, mac2, admin := r.conn(t, "mac1"), r.conn(t, "mac2"), r.conn(t, "admin1")
	mustOK(t, mac2.do("HSET", "x11", "k", "v"), "mac2 takes B")
	// Slice ids must come from mac1's own bare INCRBY nextchunk (L1 binds
	// each id to its allocator): ids (R-3, R].
	al := mac1.do("INCRBY", "nextchunk", "4096")
	if al.Type != ':' {
		t.Fatalf("INCRBY nextchunk: %c %q", al.Type, al.Str)
	}
	ids := []string{strconv.FormatInt(al.Int-2, 10), strconv.FormatInt(al.Int-1, 10), strconv.FormatInt(al.Int, 10)}

	// A txn of mac1's in A whose cleanup records name B's file 11.
	mustDeny(t, txn(mac1,
		[]string{"HDEL", "d3", "f"},
		[]string{"RPUSH", "c11_0", mkSlice(mustU64(ids[0]), 4096)},
		[]string{"HINCRBY", "sliceRef", "k" + ids[0] + "_4096", "1"}), ReasonXProject, "sliceRef naming B's chunk")
	mustDeny(t, txn(mac1,
		[]string{"HDEL", "d3", "f"},
		[]string{"DEL", "i11"},
		[]string{"ZADD", "delfiles", "1", "11:4096"}), ReasonXProject, "delfiles naming B's inode")
	if r.eng.holderOf(3) != "" {
		t.Fatal("refused units left A granted")
	}

	// Zero-ref entries recorded by mac1's own compaction of A/f (10).
	for _, id := range ids[1:] {
		mustOK(t, txn(mac1,
			[]string{"RPUSH", "c10_0", mkSlice(mustU64(id), 4096)},
			[]string{"HSET", "sliceRef", "k" + id + "_4096", "0"}), "mac1 compaction in A")
	}
	if r.eng.holderOf(3) != "mac1" {
		t.Fatal("compaction did not grant A")
	}
	mustOK(t, txn(mac1, []string{"HDEL", "sliceRef", "k" + ids[2] + "_4096"}), "mac1 drops its own zero ref while holding A")
	if v := admin.do("JM.LEASE", "BREAK", "ino:3"); string(v.Elems[0].Str) != "BROKEN" {
		t.Fatalf("BREAK: %v", v.Elems)
	}
	if v := mac2.do("JM.LEASE", "ACQUIRE", "ino:3", "auto"); string(v.Elems[0].Str) != "GRANTED" {
		t.Fatalf("mac2 ACQUIRE A: %v", v.Elems)
	}
	v := txn(mac1, []string{"HDEL", "sliceRef", "k" + ids[1] + "_4096"})
	mustDeny(t, v, ReasonLeaseHeld, "mac1 zero-ref drop after A moved to mac2")
	if !strings.Contains(string(v.Str), "proj=3 holder=mac2@") {
		t.Fatalf("refusal names the wrong project: %q", v.Str)
	}
}

func mustU64(s string) uint64 { n, _ := strconv.ParseUint(s, 10, 64); return n }

// A client that stops reading its replies keeps its unit's reply stuck in
// the proxy, so Done never runs. The fence hold is bounded: after
// FenceHoldMax the engine kills that unit's own upstream connection inside
// Redis (after which nothing it sent can still commit) and only
// then releases the fence, so a BREAK completes and the stuck EXEC's
// outcome is final before the transfer.
func TestLeaseFenceHoldBoundedForNonReadingClient(t *testing.T) {
	const hold = 600 * time.Millisecond
	r := startLeaseRig(t, ModeEnforce, func(c *LeaseConfig) { c.FenceHoldMax = hold })
	addr := redisAddr(t, false)
	plain := redisClientNoFlush(t, addr)
	ctx := ctx5(t)
	if err := plain.Set(ctx, "bigval", strings.Repeat("z", 4<<20), 0).Err(); err != nil {
		t.Fatal(err)
	}
	healthy := r.conn(t, "mac1")
	before := upstreamNames(t, plain, "p:mac1:")
	stuck := r.conn(t, "mac1")
	stuckName := newName(t, before, upstreamNames(t, plain, "p:mac1:"))
	for i := 0; i < 40; i++ { // ~160 MB of replies the client never reads
		stuck.send("GET", "bigval")
	}
	stuck.send("MULTI")
	stuck.send("HSET", "x10", "k", "stuck")
	stuck.send("EXEC")
	stuck.flush()
	deadline := time.Now().Add(5 * time.Second)
	for r.eng.holderOf(3) != "mac1" {
		if time.Now().After(deadline) {
			t.Fatal("the stuck client's EXEC was never decided")
		}
		time.Sleep(10 * time.Millisecond)
	}
	admin := r.conn(t, "admin1")
	t0 := time.Now()
	v := admin.doLong(10*time.Second, "JM.LEASE", "BREAK", "ino:3")
	took := time.Since(t0)
	if len(v.Elems) != 3 || string(v.Elems[0].Str) != "BROKEN" {
		t.Fatalf("BREAK: %c %q %v", v.Type, v.Str, v.Elems)
	}
	if took < hold/2 || took > 5*time.Second {
		t.Fatalf("BREAK took %s; want about FenceHoldMax (%s)", took, hold)
	}
	if n := r.eng.Stats().FenceTimeouts; n != 1 {
		t.Fatalf("fence timeouts = %d, want 1", n)
	}
	// Mechanism: the stuck connection was freed inside Redis before the
	// fence was released (so nothing it sent can commit later).
	checkTargetedKill(t, plain, stuckName, before, healthy)
	// The outcome was final before the break: the value is settled and no
	// late commit changes it.
	got, _ := plain.HGet(ctx, "x10", "k").Result()
	time.Sleep(300 * time.Millisecond)
	if again, _ := plain.HGet(ctx, "x10", "k").Result(); again != got {
		t.Fatalf("x10 changed after the transfer: %q -> %q", got, again)
	}
	if got != "stuck" {
		t.Fatalf("x10 k = %q: the EXEC Redis had already executed should be committed", got)
	}
	mustOK(t, r.conn(t, "mac2").do("HSET", "x10", "k", "mac2"), "mac2 after the break")
}

func redisClientNoFlush(t *testing.T, addr string) *redis.Client {
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: testDB})
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

func (rc *rawConn) doLong(d time.Duration, args ...string) Value {
	rc.t.Helper()
	rc.send(args...)
	rc.flush()
	rc.c.SetReadDeadline(time.Now().Add(d))
	v, err := ReadValue(rc.r)
	if err != nil {
		rc.t.Fatalf("read reply: %v", err)
	}
	return v
}

// L1's pending-reply cap disconnects a client that stops reading. A unit
// still in flight then ends as Lost; the engine kills the principal's
// upstream connections inside Redis before releasing the unit's fence, so a
// transfer goes through at once (not after FenceHoldMax) and nothing the
// dead connection sent can commit after it.
func TestLeaseLostUnitKillsUpstreamBeforeRelease(t *testing.T) {
	r := startLeaseRigP(t, ModeEnforce, func(c *Config) { c.MaxPendingReplies = 100 },
		func(c *LeaseConfig) { c.FenceHoldMax = time.Minute })
	addr := redisAddr(t, false)
	plain := redisClientNoFlush(t, addr)
	ctx := ctx5(t)
	if err := plain.Set(ctx, "bigval", strings.Repeat("z", 4<<20), 0).Err(); err != nil {
		t.Fatal(err)
	}
	healthy := r.conn(t, "mac1")
	before := upstreamNames(t, plain, "p:mac1:")
	stuck := r.conn(t, "mac1")
	stuckName := newName(t, before, upstreamNames(t, plain, "p:mac1:"))
	for i := 0; i < 40; i++ {
		stuck.send("GET", "bigval")
	}
	stuck.send("MULTI")
	stuck.send("HSET", "x10", "k", "lost")
	stuck.send("EXEC")
	stuck.flush()
	deadline := time.Now().Add(5 * time.Second)
	for r.eng.holderOf(3) != "mac1" {
		if time.Now().After(deadline) {
			t.Fatal("the EXEC was never decided")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i := 0; i < 200; i++ { // trips the 100-reply cap: the proxy drops the client
		stuck.send("GET", "bigval")
	}
	stuck.flush()
	deadline = time.Now().Add(10 * time.Second)
	for r.eng.Stats().LostKills == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("no Lost-path kill (stats %+v)", r.eng.Stats())
		}
		time.Sleep(10 * time.Millisecond)
	}
	t0 := time.Now()
	v := r.conn(t, "admin1").doLong(10*time.Second, "JM.LEASE", "BREAK", "ino:3")
	if len(v.Elems) != 3 || string(v.Elems[0].Str) != "BROKEN" || time.Since(t0) > 2*time.Second {
		t.Fatalf("BREAK after the lost unit: %c %q %v in %s", v.Type, v.Str, v.Elems, time.Since(t0))
	}
	if n := r.eng.Stats().FenceTimeouts; n != 0 {
		t.Fatalf("released by the hold timeout (%d), not the Lost path", n)
	}
	checkTargetedKill(t, plain, stuckName, before, healthy)
	got, _ := plain.HGet(ctx, "x10", "k").Result()
	time.Sleep(300 * time.Millisecond)
	if again, _ := plain.HGet(ctx, "x10", "k").Result(); again != got {
		t.Fatalf("x10 changed after the transfer: %q -> %q", got, again)
	}
}

// A checkout is capped at checkout_max_s from its first grant: re-ACQUIRE
// renews it, but never past that point.
func TestLeaseCheckoutLifetimeCapped(t *testing.T) {
	r := startLeaseRig(t, ModeEnforce)
	mac1 := r.conn(t, "mac1")
	acq := func() Value {
		v := mac1.do("JM.LEASE", "ACQUIRE", "ino:3", "checkout", "259200")
		if len(v.Elems) != 5 {
			t.Fatalf("ACQUIRE: %c %q", v.Type, v.Str)
		}
		return v
	}
	first := acq()
	limit := first.Elems[3].Int
	for i := 0; i < 4; i++ {
		r.clk.Add(20 * time.Hour)
		if v := acq(); string(v.Elems[0].Str) != "RENEWED" || v.Elems[3].Int > limit {
			t.Fatalf("re-ACQUIRE %d: %v (limit %d)", i, v.Elems, limit)
		}
		r.eng.sweep()
		if h := r.eng.holderOf(3); h != "mac1" && i < 3 {
			t.Fatalf("checkout ended early at +%dh", 20*(i+1))
		}
	}
	// +80 h: past 72 h from the first grant.
	r.eng.sweep()
	if h := r.eng.holderOf(3); h != "" {
		t.Fatalf("checkout outlived 72 h by re-ACQUIRE: holder %q", h)
	}
}

// L1 appends SETBIT <used bit> to the MULTI of a txn that first links a
// self-allocated slice and strips its result from the EXEC reply. The lease
// engine must see neither the extra command (no anchor, no project write)
// nor a changed outcome: Done gets Committed for the real commit and not
// for a WATCH abort, and the client sees only its own results.
func TestLeaseUsedBitSetbitInvisibleToDecide(t *testing.T) {
	r := startLeaseRig(t, ModeEnforce)
	mac1 := r.conn(t, "mac1")
	al := mac1.do("INCRBY", "nextchunk", "4096")
	if al.Type != ':' {
		t.Fatalf("INCRBY nextchunk: %c %q", al.Type, al.Str)
	}
	var mu sync.Mutex
	var outcomes []Outcome
	var sawUsedBit bool
	r.eng.hooks.Store(&engineHooks{
		admitted: func(p Principal, u *Unit) {
			mu.Lock()
			defer mu.Unlock()
			for _, c := range u.Cmds {
				if c.Info.Name == "SETBIT" {
					sawUsedBit = true
				}
			}
			for _, w := range u.Project {
				if strings.HasPrefix(w.Raw, "jmlease:v1:sused:") {
					sawUsedBit = true
				}
			}
		},
		done: func(p Principal, u *Unit, o Outcome, _ map[uint64]uint64) {
			mu.Lock()
			outcomes = append(outcomes, o)
			mu.Unlock()
		},
	})
	defer r.eng.hooks.Store(nil)

	id := uint64(al.Int)
	v := txn(mac1,
		[]string{"RPUSH", "c10_0", mkSlice(id, 4096)},
		[]string{"SET", "i10", mkAttr(typeFile, 3)})
	if v.Type != '*' || len(v.Elems) != 2 {
		t.Fatalf("EXEC reply: %c %d elems (want the client's 2 results only)", v.Type, len(v.Elems))
	}
	// A WATCH-aborted txn with a fresh slice: not committed.
	other := r.conn(t, "mac1")
	mac1.do("WATCH", "i10")
	mustOK(t, other.do("SET", "i10", mkAttr(typeFile, 3)), "break the WATCH")
	v = txn(mac1,
		[]string{"RPUSH", "c10_0", mkSlice(id-1, 4096)},
		[]string{"SET", "i10", mkAttr(typeFile, 3)})
	if !v.Null {
		t.Fatalf("aborted EXEC reply: %c null=%v", v.Type, v.Null)
	}
	// L1 rewrites a principal's own bare HDEL sliceRef into its
	// conditional drop script; that is an own-record write, not a project
	// write, so it never reaches Decide.
	mu.Lock()
	before := len(outcomes)
	mu.Unlock()
	mustOK(t, mac1.do("HDEL", "sliceRef", "k"+strconv.FormatUint(id, 10)+"_4096"), "own sliceRef drop (rewritten)")
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(outcomes) != before {
		t.Fatalf("the rewritten own sliceRef drop reached Decide (%d new outcomes)", len(outcomes)-before)
	}
	if sawUsedBit {
		t.Fatal("the proxy-appended SETBIT reached Decide")
	}
	var commits, aborts int
	for _, o := range outcomes {
		switch {
		case o.Committed:
			commits++
		case o.Forwarded && !o.Lost:
			aborts++
		}
	}
	if commits < 1 || aborts < 1 {
		t.Fatalf("outcomes %+v: want the commit and the WATCH abort told apart", outcomes)
	}
}

// upstreamNames lists Redis client names with the given prefix.
func upstreamNames(t *testing.T, rdb *redis.Client, prefix string) map[string]bool {
	list, err := rdb.ClientList(ctx5(t)).Result()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, line := range strings.Split(list, "\n") {
		for _, f := range strings.Fields(line) {
			if strings.HasPrefix(f, "name="+prefix) {
				out[f[5:]] = true
			}
		}
	}
	return out
}

func newName(t *testing.T, before, after map[string]bool) string {
	var n []string
	for k := range after {
		if !before[k] {
			n = append(n, k)
		}
	}
	if len(n) != 1 {
		t.Fatalf("want exactly one new upstream name, got %v", n)
	}
	return n[0]
}

// checkTargetedKill: the stuck unit's own upstream is gone from Redis and
// every other connection of the same principal survives and still works.
func checkTargetedKill(t *testing.T, rdb *redis.Client, stuckName string, before map[string]bool, healthy *rawConn) {
	t.Helper()
	now := upstreamNames(t, rdb, "")
	if now[stuckName] {
		t.Fatalf("the stuck unit's upstream %s is still open in Redis", stuckName)
	}
	for n := range before {
		if !now[n] {
			t.Fatalf("a healthy connection of the same principal (%s) was killed", n)
		}
	}
	if v := healthy.do("GET", "i10"); v.Type == '-' {
		t.Fatalf("healthy connection broken: %q", v.Str)
	}
}

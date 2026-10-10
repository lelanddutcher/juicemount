package respproxy

// Regression tests from the H-8B L2 adversarial review (probes in
// review-h8b-l2/zz_review_*_test.go), turned into assertions.

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func reviewRig(t *testing.T, mut ...func(*LeaseConfig)) (*leaseRig, *redis.Client, *rawConn, *rawConn) {
	r := startLeaseRig(t, ModeEnforce, mut...)
	rdb := redisClientNoFlush(t, redisAddr(t, true))
	return r, rdb, r.conn(t, "mac1"), r.conn(t, "mac2")
}

func get(t *testing.T, rdb *redis.Client, k string) string {
	v, err := rdb.Get(ctx5(t), k).Result()
	if err != nil && err != redis.Nil {
		t.Fatal(err)
	}
	return v
}

// R1: an Attr.Parent value is never trusted. A SET i<n> is anchored to the
// inode's stored parent, a parent change needs matching entries, and no
// edge is learned from the value.
func TestReviewAttrParentLie(t *testing.T) {
	r, rdb, mac1, mac2 := reviewRig(t)
	mustOK(t, mac2.do("HSET", "x10", "k", "v"), "mac2 takes A")
	mustOK(t, mac1.do("HSET", "x11", "k", "v"), "mac1 takes B")
	before := get(t, rdb, "i10")
	forged := mkAttr(typeFile, 4)
	v := mac1.do("SET", "i10", forged)
	if v.Type != '-' {
		t.Fatalf("R1a: B's holder rewrote A's file claiming Parent=B: %q", v.Str)
	}
	if get(t, rdb, "i10") != before {
		t.Fatal("R1a: i10 changed")
	}
	mustDeny(t, mac1.do("HSET", "x10", "user.evil", "1"), ReasonLeaseHeld, "R1b bare setxattr on A's file")
	tr := mkAttr(typeFile, TrashInode+1)
	before12 := get(t, rdb, "i12")
	if v := txn(mac1, []string{"SET", "i12", tr}, []string{"HSET", "d4", "zz", mkEntry(typeFile, 11)}); v.Type != '-' {
		t.Fatal("R1c: a trash parent paired with an unrelated dentry was admitted")
	}
	if get(t, rdb, "i12") != before12 {
		t.Fatal("R1c: i12 overwritten")
	}
	if r.eng.holderOf(3) != "mac2" || r.eng.holderOf(4) != "mac1" {
		t.Fatalf("holders changed: A=%q B=%q", r.eng.holderOf(3), r.eng.holderOf(4))
	}
}

// R1d: even a principal holding both projects cannot re-parent an inode by
// writing its Attr alone (it would move A's file into B for every later
// decision while its entry stays in A).
func TestReviewAttrParentChangeNeedsEntries(t *testing.T) {
	r, rdb, mac1, _ := reviewRig(t)
	mustOK(t, mac1.do("HSET", "x10", "k", "v"), "mac1 takes A")
	mustOK(t, mac1.do("HSET", "x11", "k", "v"), "mac1 takes B")
	mustDeny(t, mac1.do("SET", "i10", mkAttr(typeFile, 4)), ReasonUnknownKey, "Attr.Parent rewrite without a move")
	if p, _ := attrParent([]byte(get(t, rdb, "i10"))); p != 3 {
		t.Fatalf("i10 parent = %d", p)
	}
	// The real move (entries + attr) is fine.
	mustOK(t, txn(mac1,
		[]string{"HDEL", "d3", "f"},
		[]string{"SET", "i10", mkAttr(typeFile, 4)},
		[]string{"HSET", "d4", "f", mkEntry(typeFile, 10)}), "real move A/f -> B/f")
	_ = r
}

// R2: a link of A's file into B needs A too, and is refused outright when
// it would span projects (plan: hard-link parents in one project).
func TestReviewCrossProjectHardlinkCreate(t *testing.T) {
	_, rdb, mac1, mac2 := reviewRig(t)
	mustOK(t, mac2.do("HSET", "x10", "k", "v"), "mac2 takes A")
	mustOK(t, mac1.do("HSET", "x11", "k", "v"), "mac1 takes B")
	link := func(rc *rawConn) Value {
		return txn(rc,
			[]string{"HSET", "d4", "lnk", mkEntry(typeFile, 10)},
			[]string{"SET", "i10", mkAttr(typeFile, 0)},
			[]string{"HINCRBY", "p10", "3", "1"},
			[]string{"HINCRBY", "p10", "4", "1"})
	}
	if v := link(mac1); v.Type != '-' {
		t.Fatal("R2a: B's holder linked A's file into B")
	}
	if ok, _ := rdb.HExists(ctx5(t), "d4", "lnk").Result(); ok {
		t.Fatal("R2a: B/lnk exists")
	}
	mustDeny(t, mac1.do("HSET", "x10", "user.evil", "1"), ReasonLeaseHeld, "R2b setxattr on A's file")
	mustOK(t, mac2.do("SET", "i10", mkAttr(typeFile, 3)), "R2d A's holder writes its own file")
	// A holder of both still cannot create a cross-project link.
	mac2.do("JM.LEASE", "ACQUIRE", "ino:4", "auto")
	mustOK(t, mac1.do("JM.LEASE", "RELEASE", "ino:4", strconv.FormatInt(mac1.do("JM.LEASE", "STATUS", "ino:4").Elems[3].Int, 10)), "release B")
	mustOK(t, mac2.do("HSET", "x11", "k", "v2"), "mac2 takes B")
	mustDeny(t, link(mac2), ReasonXProject, "cross-project link by the holder of both")
}

type slowSide struct {
	mu    sync.Mutex
	inner sideDoer
	match string
	delay time.Duration
	armed chan struct{}
	fired chan struct{}
	fail  string // a command whose first argument pair matches fails
}

func (s *slowSide) set(match, fail string) {
	s.mu.Lock()
	s.match, s.fail = match, fail
	s.mu.Unlock()
}

func (s *slowSide) Do(db int, args ...[]byte) (Value, error) {
	s.mu.Lock()
	match, fail := s.match, s.fail
	s.mu.Unlock()
	if fail != "" && len(args) >= 2 && string(args[0])+" "+string(args[1]) == fail {
		return Value{}, errors.New("injected side failure")
	}
	v, err := s.inner.Do(db, args...)
	if match != "" && len(args) == 2 && string(args[0]) == "GET" && string(args[1]) == match {
		select {
		case <-s.armed:
			select {
			case s.fired <- struct{}{}:
			default:
			}
			time.Sleep(s.delay)
		default:
		}
	}
	return v, err
}

// R4: a side read that races a committed rename must not re-insert the old
// parent into the file cache.
func TestReviewFileCacheRace(t *testing.T) {
	ss := &slowSide{match: "i10", delay: 300 * time.Millisecond, armed: make(chan struct{}), fired: make(chan struct{}, 1)}
	r, _, mac1, mac2 := reviewRig(t, func(lc *LeaseConfig) { ss.inner = lc.Side; lc.Side = ss })
	mustOK(t, mac1.do("HSET", "x11", "k", "v"), "mac1 takes B")
	mustOK(t, mac1.do("HSET", "x12", "k", "v"), "mac1 takes A")
	r.eng.anc.files.del(10)
	close(ss.armed)
	res := make(chan Value, 1)
	go func() { res <- mac2.do("HSET", "x10", "k", "v") }() // side GET i10 -> Parent=A, then stalls
	<-ss.fired
	// The move reads i10 again (placement) — let it through quickly.
	ss.set("", "")
	mustOK(t, txn(mac1,
		[]string{"HDEL", "d3", "f"},
		[]string{"SET", "i10", mkAttr(typeFile, 4)},
		[]string{"HSET", "d4", "f", mkEntry(typeFile, 10)}), "mac1 moves A/f -> B/f")
	<-res
	if p, ok := r.eng.anc.files.get(10); ok && p != 4 {
		t.Fatalf("file cache says parent(10)=%d after the race (real parent 4)", p)
	}
	ep := mac1.do("JM.LEASE", "STATUS", "ino:3").Elems[3].Int
	mustOK(t, mac1.do("JM.LEASE", "RELEASE", "ino:3", strconv.FormatInt(ep, 10)), "release A")
	mustOK(t, mac2.do("HSET", "x12", "k", "mac2"), "mac2 takes A")
	mustDeny(t, mac2.do("HSET", "x10", "user.evil", "1"), ReasonLeaseHeld, "mac2 (A only) setxattr on B/f")
}

// R5: a checkout survives an engine restart without heartbeats and still
// ends on time.
func TestReviewCheckoutSurvivesRestart(t *testing.T) {
	r := startLeaseRig(t, ModeEnforce)
	mac1 := r.conn(t, "mac1")
	if v := mac1.do("JM.LEASE", "ACQUIRE", "ino:3", "checkout", "7200"); string(v.Elems[0].Str) != "GRANTED" {
		t.Fatalf("ACQUIRE: %v", v.Elems)
	}
	clk := &testClock{t: r.clk.Now().Add(30 * time.Minute)}
	e2 := NewLeaseEngine(LeaseConfig{Side: r.tp.Side(), VolumeDB: testDB, Now: clk.Now, SweepEvery: time.Hour, NoScan: true, Logf: t.Logf})
	if err := e2.Start(); err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	e2.sweep()
	if h := e2.holderOf(3); h != "mac1" {
		t.Fatalf("checkout lost across a restart past orphan_s: holder %q", h)
	}
	clk.Add(2 * time.Hour)
	e2.sweep()
	if h := e2.holderOf(3); h != "" {
		t.Fatalf("checkout outlived its ttl: holder %q", h)
	}
}

// R6: a WATCH-aborted move whose client disconnects before the reply is
// Lost; Lost must never be learned as a commit.
func TestReviewLostAbortedMoveLearned(t *testing.T) {
	r, rdb, mac1, _ := reviewRig(t)
	mustOK(t, mac1.do("HSET", "x11", "k", "v"), "mac1 takes B")
	mustOK(t, mac1.do("HSET", "x12", "k", "v"), "mac1 takes A (A/sub/h)")
	lost := 0
	for i := 0; i < 20; i++ {
		// Fresh connections each round.
		mac1 = r.conn(t, "mac1")
		x := r.conn(t, "mac1")
		x.do("WATCH", "d3")
		mustOK(t, create(mac1, 3, uint64(100+i), "bump"+strconv.Itoa(i)), "invalidate the WATCH")
		x.send("MULTI")
		x.send("HDEL", "d3", "sub")
		x.send("SET", "i5", mkAttr(typeDir, 4))
		x.send("HSET", "d4", "sub", mkEntry(typeDir, 5))
		x.send("EXEC")
		x.flush()
		x.c.Close()
		time.Sleep(30 * time.Millisecond)
		r.eng.anc.mu.RLock()
		bad := r.eng.anc.dirParent[5] == 4
		r.eng.anc.mu.RUnlock()
		if bad {
			t.Fatalf("try %d: the aborted move was learned (parent(5)=B) while Redis keeps A/sub", i)
		}
		lost++
	}
	if ok, _ := rdb.HExists(ctx5(t), "d3", "sub").Result(); !ok {
		t.Fatal("setup: A/sub moved")
	}
	time.Sleep(50 * time.Millisecond)
	mac1 = r.conn(t, "mac1")
	mac2 := r.conn(t, "mac2")
	ep := mac1.do("JM.LEASE", "STATUS", "ino:3").Elems[3].Int
	mustOK(t, mac1.do("JM.LEASE", "RELEASE", "ino:3", strconv.FormatInt(ep, 10)), "release A")
	mustOK(t, mac2.do("HSET", "x10", "k", "mac2"), "mac2 takes A")
	mustDeny(t, mac1.do("HSET", "x12", "user.evil", "1"), ReasonLeaseHeld, "mac1 (B only) setxattr on A/sub/h")
	if lost == 0 {
		t.Fatal("no attempt ran")
	}
}

// Trash pairing names the same inode: a holder of B cannot plant a trash
// entry for A's file next to an unlink of its own file (bg trash cleanup
// would then delete A's data).
func TestReviewTrashPairingSameInode(t *testing.T) {
	r, rdb, mac1, _ := reviewRig(t)
	// A stays free, so only the pairing rule (not A's lease) can refuse.
	mustOK(t, mac1.do("HSET", "x11", "k", "v"), "mac1 takes B")
	trashDir := uint64(TrashInode + 1)
	rdb.HSet(ctx5(t), "d"+inoStr(TrashInode), "2026100616", mkEntry(typeDir, trashDir))
	rdb.Set(ctx5(t), "i"+inoStr(trashDir), mkAttr(typeDir, TrashInode), 0)
	v := txn(mac1,
		[]string{"HDEL", "d4", "g"},
		[]string{"SET", "i11", mkAttr(typeFile, trashDir)},
		[]string{"HSET", "d" + inoStr(trashDir), "4-11-g", mkEntry(typeFile, 11)},
		[]string{"HSET", "d" + inoStr(trashDir), "3-10-f", mkEntry(typeFile, 10)})
	mustDeny(t, v, ReasonBGOnly, "trash entry naming A's file next to B's unlink")
	if ok, _ := rdb.HExists(ctx5(t), "d"+inoStr(trashDir), "3-10-f").Result(); ok {
		t.Fatal("forged trash entry landed")
	}
	if h := r.eng.holderOf(3); h != "" {
		t.Fatalf("the forged entry granted A to %q", h)
	}
	// The honest unlink alone is fine.
	mustOK(t, txn(mac1,
		[]string{"HDEL", "d4", "g"},
		[]string{"SET", "i11", mkAttr(typeFile, trashDir)},
		[]string{"HSET", "d" + inoStr(trashDir), "4-11-g", mkEntry(typeFile, 11)}), "B's own unlink into trash")
}

// R3 (opt-in): decision latency against the L1-only proxy.
func TestReviewDecideLatency(t *testing.T) {
	if os.Getenv("JM_H8B_LATENCY") == "" {
		t.Skip("set JM_H8B_LATENCY=1")
	}
	r := startLeaseRig(t, ModeEnforce)
	mac1 := r.conn(t, "mac1")
	mustOK(t, mac1.do("HSET", "x12", "k", "v"), "warm")
	base := startProxy(t, redisAddr(t, true), ModeEnforce)
	b1 := base.raw(t)
	b1.login("mac1")
	for run := 0; run < 6; run++ {
		const n = 5000
		t0 := time.Now()
		for i := 0; i < n; i++ {
			mac1.do("HSET", "x12", "k", strconv.Itoa(i))
		}
		viaLease := time.Since(t0) / n
		t0 = time.Now()
		for i := 0; i < n; i++ {
			b1.do("HSET", "x12", "k", strconv.Itoa(i))
		}
		viaBase := time.Since(t0) / n
		t.Logf("run %d: per write lease=%s L1-only=%s delta=%s", run, viaLease, viaBase, viaLease-viaBase)
	}
	_ = strings.Contains
}

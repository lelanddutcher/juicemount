package respproxy

// Regression tests for the fourth H8B-L1 review's code-reading leads
// (review H8B-L1d). Each closes the gap defensively.

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// #1: RedisInt accepts exactly what Redis's string2ll accepts. Every form is
// also checked against a real Redis INCRBY when one is available.
func TestRedisIntMatchesString2ll(t *testing.T) {
	accept := map[string]int64{
		"0": 0, "1": 1, "-1": -1, "10": 10, "4096": 4096,
		"9223372036854775807": 9223372036854775807, "-9223372036854775808": -9223372036854775808,
	}
	reject := []string{
		"", "+1", "+0", "01", "00", "-0", "-01", "-", "--1", "+-1", " 1", "1 ", "1\t", "1.0", "1e3",
		"0x10", "1_000", "9223372036854775808", "-9223372036854775809", "99999999999999999999",
		"123456789012345678901", "١", "1\x00", "\x001",
	}
	for s, want := range accept {
		if got, ok := RedisInt([]byte(s)); !ok || got != want {
			t.Errorf("RedisInt(%q) = %d,%v want %d", s, got, ok, want)
		}
	}
	for _, s := range reject {
		if v, ok := RedisInt([]byte(s)); ok {
			t.Errorf("RedisInt(%q) accepted (%d)", s, v)
		}
	}
	for _, s := range []string{"012", "00", "+5", "-5", " 5"} {
		if _, ok := digits(s); ok {
			t.Errorf("digits(%q) accepted", s)
		}
	}
	if k := ParseKey("i012"); k.Shape != ShapeUnknown {
		t.Errorf("i012 parsed as %+v (a different Redis key from i12)", k)
	}
	// Ground truth: real Redis agrees with every row.
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	for s := range accept {
		rdb.Set(ctx, "n", "0", 0)
		if s == "9223372036854775807" || s == "-9223372036854775808" {
			continue // would overflow the counter, not a parse question
		}
		if err := rdb.Do(ctx, "INCRBY", "n", s).Err(); err != nil {
			t.Errorf("Redis rejected %q, RedisInt accepted it: %v", s, err)
		}
	}
	for _, s := range reject {
		rdb.Set(ctx, "n", "0", 0)
		if err := rdb.Do(ctx, "INCRBY", "n", s).Err(); err == nil {
			t.Errorf("Redis accepted %q, RedisInt rejects it", s)
		}
	}
}

// #1: a unit with a non-canonical integer the guard would credit is refused.
func TestGuardRefusesNonCanonicalIntegers(t *testing.T) {
	mac := Principal{Name: "mac2", Kind: KindMac}
	g := NewGuard(NewMemSessionStore(), fakeChunks{"c100_0": {mslice(777, 4096)}})
	for _, c := range []struct {
		name  string
		watch []string
		cmds  [][]string
	}{
		{"+1 increment pays a push", nil, [][]string{{"RPUSH", "c100_0", mslice(777, 4096)}, {"HINCRBY", "sliceRef", "k777_4096", "+1"}}},
		{"01 increment", nil, [][]string{{"RPUSH", "c100_0", mslice(777, 4096)}, {"HINCRBY", "sliceRef", "k777_4096", "01"}}},
		{"LTRIM 00", []string{"c100_0"}, [][]string{{"LTRIM", "c100_0", "1", "00"}, {"HINCRBY", "sliceRef", "k777_4096", "-1"}}},
		{"LTRIM +1", []string{"c100_0"}, [][]string{{"LTRIM", "c100_0", "+1", "-1"}, {"HINCRBY", "sliceRef", "k777_4096", "-1"}}},
		{"usedSpace +4096", nil, [][]string{{"SET", "i9", "a"}, {"INCRBY", "usedSpace", "+4096"}}},
		{"nextinode 01024", nil, [][]string{{"INCRBY", "nextinode", "01024"}}},
		{"dirstat -0", nil, [][]string{{"HINCRBY", "dirUsedSpace", "5", "-0"}}},
		{"delfiles score +1", nil, [][]string{{"SET", "i9", "a"}, {"ZADD", "delfiles", "+1", "9:0"}}},
	} {
		kind := UnitTxn
		if len(c.cmds) == 1 {
			kind = UnitBare
		}
		if d, _, _ := guardRunW(g, mac, kind, c.watch, c.cmds...); d == nil {
			t.Errorf("%s: admitted", c.name)
		}
	}
}

// #2: references are counted per exact sliceRef field.
func TestGuardSliceRefFieldIdentity(t *testing.T) {
	mac := Principal{Name: "mac2", Kind: KindMac}
	g := NewGuard(NewMemSessionStore(), fakeChunks{"c100_0": {mslice(777, 4096)}})
	for _, c := range []struct {
		name  string
		watch []string
		cmds  [][]string
		ok    bool
	}{
		{"+1 on another size pays a push", nil, [][]string{{"RPUSH", "c200_0", mslice(777, 4096)}, {"HINCRBY", "sliceRef", "k777_8192", "1"}}, false},
		{"+1 on the pushed field pays it", nil, [][]string{{"RPUSH", "c200_0", mslice(777, 4096)}, {"HINCRBY", "sliceRef", "k777_4096", "1"}}, true},
		{"removed k777_4096 pays -1 on k777_8192", []string{"c100_0"}, [][]string{{"DEL", "c100_0"}, {"HINCRBY", "sliceRef", "k777_8192", "-1"}}, false},
		{"removed k777_4096 pays -1 on k777_4096", []string{"c100_0"}, [][]string{{"DEL", "c100_0"}, {"HINCRBY", "sliceRef", "k777_4096", "-1"}}, true},
		{"+1 sizeA pays -1 sizeB", []string{"c100_0"}, [][]string{{"LTRIM", "c100_0", "0", "-1"}, {"HINCRBY", "sliceRef", "k777_4096", "1"}, {"HINCRBY", "sliceRef", "k777_8192", "-1"}}, false},
		{"delSlices record of another size", []string{"c100_0"}, [][]string{{"DEL", "c100_0"}, {"HSET", "delSlices", "777_17", string(append(be64(777), 0, 0, 32, 0))}}, false},
		{"two slices in one element", nil, [][]string{{"RPUSH", "c200_0", mslice(777, 4096) + mslice(778, 4096)}, {"HINCRBY", "sliceRef", "k777_4096", "1"}, {"HINCRBY", "sliceRef", "k778_4096", "1"}}, false},
	} {
		d, _, _ := guardRunW(g, mac, UnitTxn, c.watch, c.cmds...)
		if (d == nil) != c.ok {
			t.Errorf("%s: denial=%v, want admitted=%v", c.name, d, c.ok)
		}
	}
}

// #3: a self-allocated slice is linked under rule (a) only once.
func TestGuardAllocatedSliceFirstUseOnly(t *testing.T) {
	mac1 := Principal{Name: "mac1", Kind: KindMac}
	g := NewGuard(NewMemSessionStore(), fakeChunks{})
	g.slices.Record(5000, 5000+4095, "mac1")
	push := func(cmds ...[]string) (*Denial, []guardEffect) {
		d, _, eff := guardRun(g, mac1, UnitTxn, append(cmds, []string{"SET", "i9", "a"})...)
		return d, eff
	}
	if d, _ := push([]string{"RPUSH", "c9_0", mslice(5001, 4096)}, []string{"RPUSH", "c9_1", mslice(5001, 4096)}); d == nil {
		t.Fatal("one slice pushed twice in one txn without an increment")
	}
	if d, _, _ := guardRun(g, mac1, UnitBare, []string{"RPUSH", "c9_0", mslice(5001, 4096)}); d == nil {
		t.Fatal("bare push of an allocated slice admitted (its used bit could not be set atomically)")
	}
	d, eff := push([]string{"RPUSH", "c9_0", mslice(5001, 4096)})
	if d != nil {
		t.Fatalf("first link refused: %+v", d)
	}
	g.Commit(eff)
	if d, _ := push([]string{"RPUSH", "c10_0", mslice(5001, 4096)}); d == nil {
		t.Fatal("second link of the same allocated slice admitted without an increment")
	}
	if d, _ := push([]string{"RPUSH", "c10_0", mslice(5001, 4096)}, []string{"HINCRBY", "sliceRef", "k5001_4096", "1"}); d != nil {
		t.Fatalf("second link paid by +1 refused: %+v", d)
	}
}

// #3 / #5 live: the first link's used bit is set atomically inside the same
// MULTI (the proxy strips the appended result), persists across a proxy
// restart, and an aborted link leaves it clear.
func TestReviewDUsedBitInTxn(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	rdb.Del(ctx, SliceOwnersKey)
	tp := startProxy(t, addr, ModeEnforce)
	b := tp.raw(t)
	b.login("mac2")
	r := b.do("INCRBY", "nextchunk", "4096")
	id := uint64(r.Int)
	rdb.Del(ctx, UsedBitKey(id))
	link := func(rc *rawConn, watch string, extra ...[]string) Value {
		if watch != "" {
			rc.do("WATCH", watch)
		}
		cmds := append([][]string{{"RPUSH", "c101_0", mslice(id, 4096)}, {"SET", "i101", "a"}}, extra...)
		if watch != "" {
			rdb.Set(ctx, watch, "changed", 0) // abort it
		}
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
	if v := link(b, "i555"); !v.Null {
		t.Fatalf("aborted link: %s", show(v))
	}
	if rdb.GetBit(ctx, UsedBitKey(id), int64(UsedBitOffset(id))).Val() != 0 {
		t.Fatal("aborted link set the used bit")
	}
	v := link(b, "")
	if v.Type != '*' || len(v.Elems) != 2 {
		t.Fatalf("committed link: %s (want exactly the client's 2 results)", show(v))
	}
	if rdb.GetBit(ctx, UsedBitKey(id), int64(UsedBitOffset(id))).Val() != 1 {
		t.Fatal("committed link did not set the used bit")
	}
	tp.Close()
	tp2 := startProxy(t, addr, ModeEnforce)
	b2 := tp2.raw(t)
	b2.login("mac2")
	mustDeny(t, link(b2, ""), ReasonAdminOnly, "second link after restart")
	mustDeny(t, b2.do("HINCRBY", "sliceRef", fmt.Sprintf("k%d_4096", id), "-1"), ReasonBGOnly, "rollback of a linked slice after restart")
}

// #4: the WATCH reaches Redis before the guard's side read, so a change
// between the side read and EXEC aborts the EXEC.
func TestReviewDWatchBeforeSideRead(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	rdb.RPush(ctx, "c100_0", mslice(777, 4096))
	rdb.HSet(ctx, "sliceRef", "k777_4096", "1")
	tp := startProxy(t, addr, ModeEnforce)
	var once sync.Once
	tp.Guard.afterSideRead = func() {
		once.Do(func() {
			c := direct2(addr)
			defer c.Close()
			c.RPush(context.Background(), "c100_0", mslice(888, 4096)) // a concurrent writer
		})
	}
	b := tp.raw(t)
	b.login("mac2")
	// WATCH..EXEC pipelined in ONE write: nothing flushes the WATCH except
	// the proxy's confirmWatch before its side read.
	b.send("WATCH", "c100_0")
	b.send("MULTI")
	b.send("DEL", "c100_0")
	b.send("HINCRBY", "sliceRef", "k777_4096", "-1")
	b.send("EXEC")
	b.flush()
	for i := 0; i < 4; i++ {
		b.read()
	}
	if v := b.read(); !v.Null {
		t.Fatalf("EXEC after a concurrent change to the side-read chunk = %s, want WATCH abort", show(v))
	}
	if rdb.LLen(ctx, "c100_0").Val() != 2 || rdb.HGet(ctx, "sliceRef", "k777_4096").Val() != "1" {
		t.Fatal("the txn applied")
	}
}

// #7: unauthenticated connections are capped across all IPs.
func TestReviewDGlobalPreAuthCap(t *testing.T) {
	addr := redisAddr(t, false)
	direct(t, addr)
	tp := startProxy(t, addr, ModeEnforce, func(c *Config) { c.MaxPreAuthTotal = 3; c.PreAuthTimeout = 3 * time.Second })
	var idle []net.Conn
	for i := 0; i < 3; i++ {
		c, err := net.Dial("tcp", tp.addr)
		if err != nil {
			t.Fatal(err)
		}
		idle = append(idle, c)
	}
	time.Sleep(100 * time.Millisecond)
	extra, _ := net.Dial("tcp", tp.addr)
	defer extra.Close()
	extra.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, err := extra.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("fourth unauthenticated connection admitted (read err %v)", err)
	}
	for _, c := range idle {
		c.Close()
	}
	time.Sleep(200 * time.Millisecond)
	ok := tp.raw(t)
	ok.login("mac1") // capacity freed
	if v := ok.do("PING"); string(v.Str) != "PONG" {
		t.Fatalf("after the idle ones left: %s", show(v))
	}
}

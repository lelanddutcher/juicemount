package respproxy

// Regression tests for the H8B-L1 re-review (2026-10-06, review-h8b-l1b
// zz{b,c,d}_probe_test.go). Each asserts the fixed behaviour.

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// #1: a forged chunk element naming a victim's slice is refused end to end;
// a slice the proxy relayed to this principal from INCRBY nextchunk is
// admitted, also after a proxy restart (persisted ownership).
func TestReviewBForgedSliceRefused(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	rdb.Del(ctx, SliceOwnersKey)
	victim := uint64(555555)
	rdb.HSet(ctx, "sliceRef", fmt.Sprintf("k%d_4096", victim), "0")
	tp := startProxy(t, addr, ModeEnforce)
	b := tp.raw(t)
	b.login("mac2")
	for _, rec := range [][]string{
		{"HINCRBY", "sliceRef", fmt.Sprintf("k%d_4096", victim), "-1"},
		{"HSET", "delSlices", fmt.Sprintf("%d_17", victim), delayed(victim)},
		{"SET", "i100", "attr"},
	} {
		b.send("MULTI")
		b.send("RPUSH", "c100_0", mslice(victim, 4096))
		b.send(rec...)
		b.send("EXEC")
		b.flush()
		b.read()
		b.read()
		b.read()
		mustDeny(t, b.read(), ReasonAdminOnly, "forged push with "+rec[0]+" "+rec[1])
	}
	if v := rdb.HGet(ctx, "sliceRef", fmt.Sprintf("k%d_4096", victim)).Val(); v != "0" {
		t.Fatalf("victim sliceRef = %q", v)
	}
	if rdb.Exists(ctx, "c100_0", "delSlices").Val() != 0 {
		t.Fatal("forged chunk element landed")
	}
	// Legit: mac2 allocates a batch, then writes a slice from it.
	v := b.do("INCRBY", "nextchunk", "4096")
	if v.Type != ':' {
		t.Fatalf("INCRBY nextchunk: %s", show(v))
	}
	own := uint64(v.Int)
	write := func(rc *rawConn, id uint64) Value {
		rc.send("MULTI")
		rc.send("RPUSH", "c100_0", mslice(id, 4096))
		rc.send("SET", "i100", "attr")
		rc.send("EXEC")
		rc.flush()
		rc.read()
		rc.read()
		rc.read()
		return rc.read()
	}
	if r := write(b, own); r.Type != '*' {
		t.Fatalf("write of an allocated slice: %s", show(r))
	}
	mustDeny(t, write(b, own+1), ReasonAdminOnly, "slice past the allocated batch")
	tp.Close()
	tp2 := startProxy(t, addr, ModeEnforce)
	b2 := tp2.raw(t)
	b2.login("mac2")
	if r := write(b2, own-1); r.Type != '*' {
		t.Fatalf("allocated slice after proxy restart: %s", show(r))
	}
	m1 := tp2.raw(t)
	m1.login("mac1")
	mustDeny(t, write(m1, own-2), ReasonAdminOnly, "mac1 pushes mac2's slice")
}

// #2: allocator jumps and volume-counter poisoning are refused.
func TestReviewBCounterAbuse(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	rdb.Set(ctx, "nextinode", "1000", 0)
	rdb.Set(ctx, "nextchunk", "1000", 0)
	rdb.Set(ctx, "usedSpace", "1000", 0)
	rdb.Set(ctx, "totalInodes", "10", 0)
	tp := startProxy(t, addr, ModeEnforce)
	b := tp.raw(t)
	b.login("mac2")
	for _, cmd := range [][]string{
		{"INCRBY", "nextinode", strconv.FormatUint(TrashInode, 10)},
		{"INCRBY", "nextchunk", "9223372036854775000"},
	} {
		mustDeny(t, b.do(cmd...), ReasonAdminOnly, strings.Join(cmd, " "))
	}
	for _, cmd := range [][]string{
		{"INCRBY", "usedSpace", "1000000000000000000"},
		{"DECRBY", "totalInodes", "1000000000000000"},
	} {
		mustDeny(t, b.do(cmd...), ReasonBGOnly, "bare "+strings.Join(cmd, " "))
		b.send("MULTI")
		b.send("SET", "i100", "a")
		b.send(cmd...)
		b.send("EXEC")
		b.flush()
		b.read()
		b.read()
		b.read()
		mustDeny(t, b.read(), ReasonAdminOnly, "txn "+strings.Join(cmd, " "))
	}
	if v := b.do("INCRBY", "nextinode", "1024"); v.Type != ':' || v.Int != 2024 {
		t.Fatalf("juicefs-sized nextinode batch: %s", show(v))
	}
	if rdb.Get(ctx, "usedSpace").Val() != "1000" || rdb.Get(ctx, "totalInodes").Val() != "10" || rdb.Get(ctx, "nextchunk").Val() != "1000" {
		t.Fatal("a refused counter write landed")
	}
}

// #3: client-chosen command names never become stats keys, before or after
// AUTH.
func TestReviewBStatsNamesBounded(t *testing.T) {
	addr := redisAddr(t, false)
	direct(t, addr)
	tp := startProxy(t, addr, ModeEnforce)
	pad := strings.Repeat("Q", 15<<10)
	for conn := 0; conn < 2; conn++ {
		c, err := net.Dial("tcp", tp.addr)
		if err != nil {
			t.Fatal(err)
		}
		r, w := bufio.NewReader(c), bufio.NewWriter(c)
		for i := 0; i < 500; i++ {
			name := fmt.Sprintf("U%d_%d%s", conn, i, pad)
			fmt.Fprintf(w, "*1\r\n$%d\r\n%s\r\n", len(name), name)
		}
		w.Flush()
		for i := 0; i < 500; i++ {
			if _, err := ReadValue(r); err != nil {
				t.Fatalf("pre-auth reply %d: %v", i, err)
			}
		}
		c.Close()
	}
	b := tp.raw(t)
	b.login("mac2")
	for i := 0; i < 500; i++ {
		b.send(fmt.Sprintf("X%d%s", i, pad))
	}
	b.send("JM.X" + pad)
	b.flush()
	for i := 0; i < 501; i++ {
		b.read()
	}
	time.Sleep(100 * time.Millisecond)
	snap := tp.Stats.Snapshot()
	names := map[string]bool{}
	big := 0
	for _, r := range snap.Rows {
		names[r.Cmd] = true
		if len(r.Cmd) > 64 {
			big++
		}
	}
	if big > 0 || len(names) > 20 {
		t.Fatalf("stats rows keyed by client names: %d distinct, %d over 64 bytes", len(names), big)
	}
	for k := range snap.UnknownCommands {
		if len(k) > 128 {
			t.Fatalf("unknown-command name kept at %d bytes", len(k))
		}
	}
	for _, d := range snap.Recent {
		if len(d.Unit) > 2048 || len(d.Detail) > 512 {
			t.Fatalf("refusal log kept %d/%d bytes of client text", len(d.Unit), len(d.Detail))
		}
	}
}

// #4: a client that floods refused commands without reading is
// disconnected once its pending replies exceed the cap; heap stays bounded.
func TestReviewBNonReaderCapped(t *testing.T) {
	addr := redisAddr(t, false)
	direct(t, addr)
	tp := startProxy(t, addr, ModeEnforce, func(c *Config) { c.MaxPendingBytes = 4 << 20 })
	c, err := net.Dial("tcp", tp.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprintf(c, "*3\r\n$4\r\nAUTH\r\n$4\r\nmac2\r\n$9\r\nmac2-pass\r\n")
	time.Sleep(100 * time.Millisecond)
	one := "*3\r\n$3\r\nSET\r\n$7\r\nsetting\r\n$1\r\nx\r\n"
	chunk := []byte(strings.Repeat(one, 10000))
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	c.SetWriteDeadline(time.Now().Add(20 * time.Second))
	closed := false
	for i := 0; i < 300; i++ {
		if _, err := c.Write(chunk); err != nil {
			closed = true
			break
		}
	}
	time.Sleep(300 * time.Millisecond)
	runtime.GC()
	runtime.ReadMemStats(&m1)
	if !closed || tp.Stats.overflowClosed.Load() != 1 {
		t.Fatalf("non-reading client not disconnected (write error=%v, overflow closes=%d)", closed, tp.Stats.overflowClosed.Load())
	}
	if grew := int64(m1.HeapInuse) - int64(m0.HeapInuse); grew > 64<<20 {
		t.Fatalf("heap grew %d MB", grew>>20)
	}
}

// #4 / H8B-L1c #6: global cap refuses; the per-IP pre-auth limit QUEUES
// (a waiting connection is served once an authenticating one finishes) and
// refuses only when the per-IP queue is full.
func TestReviewBConnectionCaps(t *testing.T) {
	addr := redisAddr(t, false)
	direct(t, addr)
	tp := startProxy(t, addr, ModeEnforce, func(c *Config) {
		c.MaxConns = 5
		c.MaxPreAuthPerIP = 1
		c.MaxPreAuthQueuePerIP = 2
		c.PreAuthTimeout = 2 * time.Second
	})
	closedSoon := func(c net.Conn) bool {
		c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, err := c.Read(make([]byte, 1))
		return err == io.EOF
	}
	idle, _ := net.Dial("tcp", tp.addr) // holds the one auth slot
	defer idle.Close()
	time.Sleep(50 * time.Millisecond)
	waiter := tp.raw(t) // queued behind idle
	full, _ := net.Dial("tcp", tp.addr)
	defer full.Close()
	if !closedSoon(full) {
		t.Fatal("connection beyond the per-IP queue admitted")
	}
	done := make(chan Value, 1)
	go func() { done <- waiter.do("AUTH", "mac1", "mac1-pass") }()
	select {
	case <-done:
		t.Fatal("queued connection authenticated while the slot was held")
	case <-time.After(300 * time.Millisecond):
	}
	idle.Close() // frees the slot
	select {
	case v := <-done:
		if string(v.Str) != "OK" {
			t.Fatalf("queued AUTH: %s", show(v))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("queued connection never served")
	}
	var others []*rawConn
	for _, who := range []string{"mac2", "farm1", "mgr1", "bg1"} {
		r := tp.raw(t)
		r.login(who)
		others = append(others, r)
	}
	e, _ := net.Dial("tcp", tp.addr)
	defer e.Close()
	if !closedSoon(e) {
		t.Fatal("connection over MaxConns admitted")
	}
	if v := waiter.do("PING"); string(v.Str) != "PONG" {
		t.Fatalf("existing connection broken: %s", show(v))
	}
}

// H8B-L1c #6: a burst of concurrent connects from one IP (a juicefs pool
// opening, containers behind NAT) all succeed with the default limits.
func TestReviewCPreAuthBurstQueued(t *testing.T) {
	addr := redisAddr(t, false)
	direct(t, addr)
	tp := startProxy(t, addr, ModeEnforce)
	ctx := ctx5(t)
	if err := tp.client(t, "mgr1").Ping(ctx).Err(); err != nil { // warm the credential cache
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var fails atomic.Int64
	start := make(chan struct{})
	for i := 0; i < 256; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := redis.NewClient(&redis.Options{Addr: tp.addr, DB: testDB, Username: "mgr1", Password: passOf("mgr1"), MaxRetries: -1, PoolSize: 1})
			defer c.Close()
			<-start
			if err := c.Ping(context.Background()).Err(); err != nil {
				fails.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if fails.Load() != 0 || tp.Stats.connRefused.Load() != 0 {
		t.Fatalf("burst of 256 from one IP: %d failed, %d refused", fails.Load(), tp.Stats.connRefused.Load())
	}
}

// The own deleted-file record is kept only when the EXEC that queued it
// committed: after a WATCH abort, the later bare ZREM is refused.
func TestReviewBOwnRecordOnlyAfterCommit(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	rdb.Set(ctx, "i9", "attr", 0)
	tp := startProxy(t, addr, ModeEnforce)
	rc := tp.raw(t)
	rc.login("mac1")
	rc.do("WATCH", "i9")
	rdb.Set(ctx, "i9", "changed", 0) // a concurrent writer aborts the txn
	rc.send("MULTI")
	rc.send("ZADD", "delfiles", "1", "9:4096")
	rc.send("DEL", "i9")
	rc.send("EXEC")
	rc.flush()
	rc.read()
	rc.read()
	rc.read()
	if v := rc.read(); !v.Null {
		t.Fatalf("EXEC was not aborted: %s", show(v))
	}
	mustDeny(t, rc.do("ZREM", "delfiles", "9:4096"), ReasonBGOnly, "drop after an aborted unlink")
	// Committed: the drop is then admitted.
	rc.send("MULTI")
	rc.send("ZADD", "delfiles", "1", "9:4096")
	rc.send("DEL", "i9")
	rc.send("EXEC")
	rc.flush()
	rc.read()
	rc.read()
	rc.read()
	if v := rc.read(); v.Type != '*' {
		t.Fatalf("unlink EXEC: %s", show(v))
	}
	mustOK(t, rc.do("ZREM", "delfiles", "9:4096"), "drop after the committed unlink")
}

// Review probe B4: a blocking command after AUTH outlives the pre-auth
// deadline.
func TestReviewBBlockingAfterAuth(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	tp := startProxy(t, addr, ModeEnforce, func(c *Config) { c.PreAuthTimeout = 500 * time.Millisecond })
	b := tp.raw(t)
	b.login("farm1")
	b.send("BRPOP", "juicefarm:q", "0")
	b.flush()
	go func() { time.Sleep(1500 * time.Millisecond); rdb.LPush(ctx, "juicefarm:q", "job1") }()
	b.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	v, err := ReadValue(b.r)
	if err != nil || len(v.Elems) != 2 || string(v.Elems[1].Str) != "job1" {
		t.Fatalf("BRPOP across the pre-auth deadline: %s %v", show(v), err)
	}
}

// Review probe B5: RESP3, a refused CLIENT TRACKING and interleaved
// refusals and PINGs stay slot-aligned, with no stray frame; idle refusals
// (woken by the proxy's own PING) too.
func TestReviewBResp3RefusalsAndPings(t *testing.T) {
	addr := redisAddr(t, false)
	direct(t, addr)
	tp := startProxy(t, addr, ModeEnforce)
	b := tp.raw(t)
	if v := b.do("HELLO", "3", "AUTH", "mac2", "mac2-pass"); v.Type != '%' {
		t.Fatalf("HELLO 3: %s", show(v))
	}
	b.send("CLIENT", "TRACKING", "ON")
	b.send("PING", "p1")
	b.send("SET", "setting", "x")
	b.send("PING", "p2")
	b.send("SET", "setting", "y")
	b.send("SET", "setting", "z")
	b.send("PING", "p3")
	b.send("ECHO", "end")
	b.flush()
	for i, want := range []string{"deny", "p1", "deny", "p2", "deny", "deny", "p3", "end"} {
		v := b.read()
		if want == "deny" {
			if !strings.HasPrefix(string(v.Str), "JMDENY ") {
				t.Fatalf("#%d = %s, want a refusal", i, show(v))
			}
		} else if string(v.Str) != want {
			t.Fatalf("#%d = %s, want %s", i, show(v), want)
		}
	}
	b.c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if v, err := ReadValue(b.r); err == nil {
		t.Fatalf("stray frame: %s", show(v))
	}
	b.c.SetReadDeadline(time.Time{})
	for i := 0; i < 3; i++ {
		mustDeny(t, b.do("SET", "setting", "q"), ReasonAdminOnly, "idle refusal")
		if v := b.do("PING", fmt.Sprint("x", i)); string(v.Str) != fmt.Sprint("x", i) {
			t.Fatalf("PING after idle refusal %d: %s", i, show(v))
		}
	}
}

// L5 review BLOCK: the command and key-class gates refuse in EVERY mode;
// observe/passthrough relax only the project-lease decision.
func TestReviewGuardRefusesInEveryMode(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	for _, mode := range []Mode{ModePassthrough, ModeObserve, ModeEnforce} {
		rdb.Set(ctx, "i42", "sentinel", 0)
		rdb.Set(ctx, "setting", "FORMAT", 0)
		rdb.ConfigSet(ctx, "notify-keyspace-events", "KEA")
		tp := startProxy(t, addr, mode)
		for _, who := range []string{"dav1", "mac1"} {
			rc := tp.raw(t)
			rc.login(who)
			mustDeny(t, rc.do("FLUSHDB"), ReasonAdminOnly, string(mode)+" "+who+" FLUSHDB")
			mustDeny(t, rc.do("CONFIG", "SET", "notify-keyspace-events", ""), ReasonAdminOnly, string(mode)+" "+who+" CONFIG SET")
			mustDeny(t, rc.do("HSET", ConfigKey, "mode", "passthrough"), ReasonAdminOnly, string(mode)+" "+who+" jmlease cfg")
			mustDeny(t, rc.do("SET", "setting", "x"), ReasonAdminOnly, string(mode)+" "+who+" setting")
			mustDeny(t, rc.do("EVAL", "return server.call('FLUSHDB')", "0"), ReasonAdminOnly, string(mode)+" "+who+" unpinned EVAL")
			mustDeny(t, rc.do("COPY", "i42", "i43", "DB", "0"), ReasonAdminOnly, string(mode)+" "+who+" COPY DB")
			mustDeny(t, rc.do("SELECT", "0"), ReasonAdminOnly, string(mode)+" "+who+" SELECT 0")
			mustDeny(t, rc.do("SET", "weird", "x"), ReasonUnknownKey, string(mode)+" "+who+" unknown shape")
			mustDeny(t, rc.do("HSET", "jmlease:v1:p:5", "holder", who), ReasonAdminOnly, string(mode)+" "+who+" lease table")
		}
		dav := tp.raw(t)
		dav.login("dav1")
		mustDeny(t, dav.do("SET", "i42", "x"), ReasonPrincipalReadonly, string(mode)+" webdav project write")
		mustDeny(t, tp.raw(t).do("SET", "i42", "x"), ReasonAuthRequired, string(mode)+" unauthenticated")
		if rdb.Get(ctx, "i42").Val() != "sentinel" || rdb.Get(ctx, "setting").Val() != "FORMAT" ||
			rdb.Exists(ctx, ConfigKey, "weird").Val() != 0 || tp.Mode() != mode {
			t.Fatalf("%s: a refused write landed", mode)
		}
		if v := rdb.ConfigGet(ctx, "notify-keyspace-events").Val()["notify-keyspace-events"]; v == "" {
			t.Fatalf("%s: CONFIG SET notify-keyspace-events landed", mode)
		}
		tp.Close()
	}
}

package respproxy

// Regression tests for the H8B-L1 review (2026-10-06). Each was a probe in
// review-h8b-l1/src/internal/respproxy/zz_*_test.go that reproduced a
// finding; here they assert the fixed behaviour.

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func show(v Value) string {
	if v.Type == '*' || v.Type == '>' || v.Type == '~' {
		parts := []string{}
		for _, e := range v.Elems {
			parts = append(parts, show(e))
		}
		return fmt.Sprintf("%c[%s]", v.Type, strings.Join(parts, ","))
	}
	if v.Type == ':' {
		return fmt.Sprintf(":%d", v.Int)
	}
	if v.Null {
		return "nil"
	}
	return fmt.Sprintf("%c%q", v.Type, v.Str)
}

func connectedClients(t *testing.T, rdb *redis.Client) int {
	for _, l := range strings.Split(rdb.Info(context.Background(), "clients").Val(), "\n") {
		if strings.HasPrefix(l, "connected_clients:") {
			n, _ := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(l, "connected_clients:")))
			return n
		}
	}
	t.Fatal("no connected_clients in INFO")
	return 0
}

// #2: before AUTH the proxy allocates at most PreAuthLimits per command,
// never dials Redis, and drops a connection that does not log in.
func TestReviewPreAuthResourceBounds(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	tp := startProxy(t, addr, ModeEnforce, func(c *Config) { c.PreAuthTimeout = 500 * time.Millisecond })
	base := connectedClients(t, rdb)
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	var cs []net.Conn
	for i := 0; i < 20; i++ {
		c, err := net.Dial("tcp", tp.addr)
		if err != nil {
			t.Fatal(err)
		}
		cs = append(cs, c)
		if i%2 == 0 {
			fmt.Fprintf(c, "*1\r\n$400000000\r\n") // over every limit
			c.Write(make([]byte, 1<<20))
		} else {
			// 1 MB: fine for a principal, far over the 16 KB pre-auth cap.
			fmt.Fprintf(c, "*1\r\n$1048576\r\n")
			c.Write(make([]byte, 1<<20))
			c.Write([]byte("\r\n"))
		}
	}
	idle, err := net.Dial("tcp", tp.addr) // never sends anything
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if n := connectedClients(t, rdb); n > base {
		t.Fatalf("unauthenticated connections opened %d upstream connections", n-base)
	}
	runtime.ReadMemStats(&m1)
	if grew := int64(m1.HeapInuse) - int64(m0.HeapInuse); grew > 64<<20 {
		t.Fatalf("20 unauthenticated 400 MB headers grew the heap by %d MB", grew>>20)
	}
	for _, c := range cs {
		c.SetReadDeadline(time.Now().Add(2 * time.Second))
		line, _ := bufio.NewReader(c).ReadString('\n')
		if !strings.HasPrefix(line, "-ERR Protocol error") {
			t.Fatalf("oversized pre-auth command: %q", line)
		}
		c.Close()
	}
	idle.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := idle.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("idle unauthenticated connection not closed after the pre-auth deadline: %v", err)
	}
	idle.Close()
}

// #3: a client that writes a huge pipeline before reading must not
// deadlock the proxy (Redis buffers the replies, as it does direct).
func TestReviewLargePipelineNoDeadlock(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	rdb.Set(ctx5(t), "i99", strings.Repeat("v", 512), 0)
	tp := startProxy(t, addr, ModeEnforce)
	c, err := net.Dial("tcp", tp.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	const n = 300000
	var b strings.Builder
	b.WriteString("*3\r\n$4\r\nAUTH\r\n$4\r\nmac1\r\n$9\r\nmac1-pass\r\n*2\r\n$6\r\nSELECT\r\n$2\r\n15\r\n")
	for i := 0; i < n; i++ {
		b.WriteString("*2\r\n$3\r\nGET\r\n$3\r\ni99\r\n")
	}
	done := make(chan error, 1)
	go func() { _, err := c.Write([]byte(b.String())); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("pipeline write blocked: proxy deadlocked on a client that writes before reading")
	}
	r := bufio.NewReaderSize(c, 1<<20)
	c.SetReadDeadline(time.Now().Add(60 * time.Second))
	for i := 0; i < n+2; i++ {
		v, err := ReadValue(r)
		if err != nil {
			t.Fatalf("reply %d: %v", i, err)
		}
		if i >= 2 && len(v.Str) != 512 {
			t.Fatalf("reply %d: %s", i, show(v))
		}
	}
}

// #4: in RESP3 an error to SUBSCRIBE is that command's reply, not the next
// command's; UNSUBSCRIBE-all consumes all its confirmations.
func TestReviewRESP3SubscribeSlots(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	rdb.Set(ctx, "i7", "SEVEN", 0)
	rdb.Set(ctx, "i8", "EIGHT", 0)
	tp := startProxy(t, addr, ModeEnforce)
	rc := tp.raw(t)
	rc.do("HELLO", "3", "AUTH", "mac1", "mac1-pass")
	rc.do("SELECT", "15")
	rc.send("SUBSCRIBE") // wrong number of arguments: an error reply
	rc.send("GET", "i7")
	rc.flush()
	if v := rc.read(); v.Type != '-' {
		t.Fatalf("SUBSCRIBE (no channel) slot = %s, want its error", show(v))
	}
	if v := rc.read(); string(v.Str) != "SEVEN" {
		t.Fatalf("GET i7 = %s (slots shifted)", show(v))
	}
	rc.send("SUBSCRIBE", "a", "b")
	rc.send("PSUBSCRIBE", "p*")
	rc.send("UNSUBSCRIBE")
	rc.send("GET", "i8")
	rc.flush()
	var kinds []string
	for {
		v := rc.read()
		if v.Type == '>' {
			kinds = append(kinds, string(v.Elems[0].Str))
			continue
		}
		if string(v.Str) != "EIGHT" {
			t.Fatalf("GET i8 = %s after pushes %v", show(v), kinds)
		}
		break
	}
	if strings.Join(kinds, ",") != "subscribe,subscribe,psubscribe,unsubscribe,unsubscribe" {
		t.Fatalf("pushes %v", kinds)
	}
	if v := rc.do("ECHO", "tail"); string(v.Str) != "tail" {
		t.Fatalf("ECHO after pub/sub = %s", show(v))
	}
}

// #4 (ACL): the plan's `+@all ~*` user lacks `&*`; SUBSCRIBE then gets
// -NOPERM. That error must land in the SUBSCRIBE slot.
func TestReviewRESP3NoPermLandsInItsSlot(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	if err := rdb.Do(ctx, "ACL", "SETUSER", "jm-proxy-test", "on", ">pw", "~*", "resetchannels", "+@all").Err(); err != nil {
		t.Fatal(err)
	}
	defer rdb.Do(context.Background(), "ACL", "DELUSER", "jm-proxy-test")
	tp := startProxy(t, addr, ModeEnforce, func(c *Config) { c.UpstreamUser, c.UpstreamPassword = "jm-proxy-test", "pw" })
	rc := tp.raw(t)
	rc.do("HELLO", "3", "AUTH", "mac1", "mac1-pass")
	rc.do("SELECT", "15")
	rc.send("SUBSCRIBE", "juicemount:metadata")
	rc.send("ECHO", "next")
	rc.flush()
	if v := rc.read(); v.Type != '-' || !strings.HasPrefix(string(v.Str), "NOPERM") {
		t.Fatalf("SUBSCRIBE slot = %s, want -NOPERM", show(v))
	}
	if v := rc.read(); string(v.Str) != "next" {
		t.Fatalf("ECHO slot = %s", show(v))
	}
}

// #5: session ownership comes from the proxy-relayed INCR nextsession reply
// and survives a proxy restart; another principal cannot take it over.
func TestReviewSessionOwnershipSurvivesRestart(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	tp := startProxy(t, addr, ModeEnforce)
	a := tp.raw(t)
	a.login("mac1")
	v := a.do("INCRBY", "nextsession", "1")
	if v.Type != ':' {
		t.Fatalf("INCRBY nextsession: %s", show(v))
	}
	sid := strconv.FormatInt(v.Int, 10)
	mustOK(t, a.do("ZADD", "allSessions", "1999999999", sid), "mac1 heartbeat")
	mustOK(t, a.do("HSET", "sessionInfos", sid, "{}"), "mac1 session info")
	// Not allocated through the proxy: refused even to the first writer.
	mustDeny(t, a.do("ZADD", "allSessions", "1", "99999"), ReasonBGOnly, "unallocated sid")
	tp.Close()

	tp2 := startProxy(t, addr, ModeEnforce)
	b := tp2.raw(t)
	b.login("mac2")
	mustDeny(t, b.do("ZADD", "allSessions", "0", sid), ReasonBGOnly, "mac2 hijack after restart")
	mustDeny(t, b.do("HDEL", "sessionInfos", sid), ReasonBGOnly, "mac2 drops mac1 session info")
	if s := rdb.ZScore(ctx, "allSessions", sid).Val(); s != 1999999999 {
		t.Fatalf("mac1 heartbeat score changed to %v", s)
	}
	// A counter rewind cannot hand mac2 mac1's sid.
	mustDeny(t, b.do("SET", "nextsession", strconv.FormatInt(v.Int-1, 10)), ReasonAdminOnly, "rewind nextsession")
	a2 := tp2.raw(t)
	a2.login("mac1")
	mustOK(t, a2.do("ZADD", "allSessions", "1999999999", sid), "mac1 heartbeat after restart")
}

// #6: a cleanup record naming another project's inode or slice is refused.
func TestReviewCrossProjectCleanupRecordRefused(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	tp := startProxy(t, addr, ModeEnforce)
	b := tp.raw(t)
	b.login("mac2")
	for _, rec := range [][]string{
		{"ZADD", "delfiles", "1", "777777:999999999"},
		{"HSET", "sliceRef", "k123_4096", "0"},
		{"HSET", "delSlices", "123_17", strings.Repeat("\x00", 12)},
		{"HINCRBY", "sliceRef", "k123_4096", "-1"},
	} {
		b.send("MULTI")
		b.send("HSET", "d100", "x", "y")
		b.send(rec...)
		b.send("EXEC")
		b.flush()
		b.read()
		b.read()
		b.read()
		mustDeny(t, b.read(), ReasonBGOnly, "EXEC with "+strings.Join(rec[:2], " "))
	}
	if rdb.Exists(ctx, "d100", "delfiles", "sliceRef", "delSlices").Val() != 0 {
		t.Fatal("refused cleanup txn applied")
	}
	b.send("MULTI")
	b.send("HDEL", "sliceRef", "k123_4096")
	b.send("EXEC")
	b.flush()
	b.read()
	b.read()
	mustDeny(t, b.read(), ReasonBGOnly, "zero-ref drop of a slice mac2 never created")
}

// #7: no write reaches a DB other than the configured one, and the unit's
// DB changes only after Redis confirmed the SELECT.
func TestReviewWrongDBRefused(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	rdb.Set(ctx, "i5", "ATTR", 0)
	pol := &recordingPolicy{}
	tp := startProxy(t, addr, ModeEnforce, func(c *Config) { c.Policy = pol })
	b := tp.raw(t)
	b.login("mac1")
	db0 := redis.NewClient(&redis.Options{Addr: addr, DB: 0})
	defer db0.Close()
	db0.Del(ctx, "i5", "i6")
	mustDeny(t, b.do("COPY", "i5", "i6", "DB", "0", "REPLACE"), ReasonAdminOnly, "COPY DB 0")
	mustDeny(t, b.do("SELECT", "0"), ReasonAdminOnly, "SELECT 0")
	mustDeny(t, b.do("MOVE", "i5", "0"), ReasonAdminOnly, "MOVE")
	if db0.Exists(ctx, "i6", "i5").Val() != 0 {
		t.Fatal("a write reached DB 0")
	}
	mustOK(t, b.do("SET", "i7", "x"), "SET after refused SELECT")
	units, _ := pol.snapshot()
	if len(units) == 0 || units[len(units)-1].DB != testDB {
		t.Fatalf("unit DB after refused SELECT: %+v", units)
	}
	// An admin may select another DB only on +OK; a failed SELECT leaves it.
	ad := tp.raw(t)
	ad.login("admin1")
	if v := ad.do("SELECT", "99999"); v.Type != '-' {
		t.Fatalf("SELECT 99999: %s", show(v))
	}
	mustOK(t, ad.do("SET", "i8", "x"), "admin SET after failed SELECT")
	units, _ = pol.snapshot()
	if u := units[len(units)-1]; u.DB != testDB {
		t.Fatalf("failed SELECT moved the unit DB to %d", u.DB)
	}
	if rdb.Get(ctx, "i8").Val() != "x" {
		t.Fatal("admin SET did not land in the test DB")
	}
}

// #8: another principal's typos behind the same IP do not lock you out.
func TestReviewLockoutIsPerPrincipal(t *testing.T) {
	addr := redisAddr(t, false)
	direct(t, addr)
	tp := startProxy(t, addr, ModeEnforce)
	for i := 0; i < throttleFree+1; i++ {
		tp.raw(t).do("AUTH", "mac2", "wrong")
	}
	if v := tp.raw(t).do("AUTH", "farm1", "farm1-pass"); string(v.Str) != "OK" {
		t.Fatalf("farm1 from the same IP after mac2's typos: %s", show(v))
	}
	if v := tp.raw(t).do("AUTH", "mac2", "mac2-pass"); !strings.HasPrefix(string(v.Str), "WRONGPASS") {
		t.Fatalf("mac2 not locked out: %s", show(v))
	}
}

// #10: commands that change proxy-tracked state inside MULTI poison the
// transaction; nothing queued is applied.
func TestReviewTxnPoison(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	tp := startProxy(t, addr, ModeEnforce)
	rc := tp.raw(t)
	rc.login("mac1")
	for _, bad := range [][]string{{"SELECT", "15"}, {"CLIENT", "SETNAME", "x"}, {"AUTH", "mac1", "mac1-pass"}, {"SUBSCRIBE", "c"}} {
		rc.send("MULTI")
		rc.send("SET", "i1", "x")
		rc.send(bad...)
		rc.send("EXEC")
		rc.flush()
		rc.read()
		rc.read()
		if v := rc.read(); v.Type != '-' {
			t.Fatalf("%v inside MULTI: %s", bad, show(v))
		}
		mustDeny(t, rc.read(), ReasonUnknownKey, "EXEC after "+bad[0])
		if rdb.Exists(ctx, "i1").Val() != 0 {
			t.Fatalf("poisoned txn (%v) applied", bad)
		}
	}
}

// #10: the upstream name always identifies the principal, whatever name the
// client asks for.
func TestReviewClientSetnameReplaced(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	tp := startProxy(t, addr, ModeEnforce)
	rc := tp.raw(t)
	rc.login("mac1")
	mustOK(t, rc.do("CLIENT", "SETNAME", "p:admin1"), "SETNAME")
	list := rdb.ClientList(ctx5(t)).Val()
	if strings.Contains(list, "name=p:admin1") || !strings.Contains(list, "name=p:mac1:") {
		t.Fatalf("client chose its upstream name:\n%s", list)
	}
}

// Singleton fencing: with no renewal for the TTL, writes stop (reads go on).
func TestReviewSingletonFencesWrites(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	tp := startProxy(t, addr, ModeEnforce, func(c *Config) {
		c.Singleton = true
		c.SingletonTTL = 1500 * time.Millisecond
		c.SingletonRenew = time.Hour // the renewal never runs: as if Redis were unreachable
	})
	rc := tp.raw(t)
	rc.login("mac1")
	mustOK(t, rc.do("SET", "i1", "a"), "write while held")
	time.Sleep(1300 * time.Millisecond)
	mustDeny(t, rc.do("SET", "i1", "b"), ReasonProxyFenced, "write after the TTL lapsed")
	if v := rc.do("GET", "i1"); string(v.Str) != "a" {
		t.Fatalf("read while fenced: %s", show(v))
	}
	if rdb.Get(ctx, "i1").Val() != "a" {
		t.Fatal("fenced write landed")
	}
}

// Review probe (zz_order_test.go): long mixed pipelines with refusals,
// blocking commands and transactions stay slot-aligned.
func TestReviewMixedPipelineOrder(t *testing.T) {
	addr := redisAddr(t, false)
	direct(t, addr)
	tp := startProxy(t, addr, ModeEnforce)
	rc := tp.raw(t)
	rc.login("mac1")
	rng := rand.New(rand.NewSource(7))
	type exp struct{ kind, tag string }
	for round := 0; round < 5; round++ {
		var want []exp
		for i := 0; i < 300; i++ {
			tag := fmt.Sprintf("t%d_%d", round, i)
			switch rng.Intn(6) {
			case 0, 1:
				rc.send("ECHO", tag)
				want = append(want, exp{"echo", tag})
			case 2:
				rc.send("SET", "setting", "x")
				want = append(want, exp{"deny", ""})
			case 3:
				rc.send("BLPOP", "i424242", "0.01")
				want = append(want, exp{"nil", ""})
			case 4:
				rc.send("MULTI")
				rc.send("ECHO", tag)
				want = append(want, exp{"ok", ""}, exp{"queued", ""})
				if rng.Intn(2) == 0 {
					rc.send("FLUSHDB")
					want = append(want, exp{"queued", ""})
					rc.send("EXEC")
					want = append(want, exp{"deny", ""})
				} else {
					rc.send("EXEC")
					want = append(want, exp{"execarr", tag})
				}
			case 5:
				rc.send("NOSUCHCMD", "a")
				want = append(want, exp{"deny", ""})
			}
		}
		rc.flush()
		for i, w := range want {
			rc.c.SetReadDeadline(time.Now().Add(10 * time.Second))
			v, err := ReadValue(rc.r)
			if err != nil {
				t.Fatalf("round %d #%d: %v", round, i, err)
			}
			ok := false
			switch w.kind {
			case "echo":
				ok = string(v.Str) == w.tag
			case "deny":
				ok = v.Type == '-' && strings.HasPrefix(string(v.Str), "JMDENY")
			case "nil":
				ok = v.Null
			case "ok":
				ok = string(v.Str) == "OK"
			case "queued":
				ok = string(v.Str) == "QUEUED"
			case "execarr":
				ok = len(v.Elems) == 1 && string(v.Elems[0].Str) == w.tag
			}
			if !ok {
				t.Fatalf("round %d #%d want %v got %s", round, i, w, show(v))
			}
		}
	}
}

// Review probe (zz_watch_test.go): after a refused EXEC the WATCH is gone,
// a fresh WATCH works, and EXECABORT passes through.
func TestReviewRewatchAfterRefusal(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	tp := startProxy(t, addr, ModeEnforce)
	rc := tp.raw(t)
	rc.login("mac1")
	rdb.Set(ctx, "i1", "A", 0)
	rc.do("WATCH", "i1")
	rc.do("MULTI")
	rc.do("SET", "i1", "B")
	rc.do("FLUSHDB")
	mustDeny(t, rc.do("EXEC"), ReasonAdminOnly, "refused EXEC")
	rc.do("WATCH", "i1")
	rdb.Set(ctx, "i1", "C", 0)
	rc.do("MULTI")
	rc.do("SET", "i1", "D")
	if v := rc.do("EXEC"); !v.Null {
		t.Fatalf("re-WATCH EXEC after a foreign write = %s, want nil", show(v))
	}
	if rdb.Get(ctx, "i1").Val() != "C" {
		t.Fatal("aborted txn applied")
	}
	rc.do("MULTI")
	rc.do("SET", "i1")
	if v := rc.do("EXEC"); v.Type != '-' || !strings.HasPrefix(string(v.Str), "EXECABORT") {
		t.Fatalf("EXECABORT path: %s", show(v))
	}
}

func FuzzReadCommand(f *testing.F) {
	f.Add([]byte("*2\r\n$3\r\nGET\r\n$1\r\na\r\n"))
	f.Add([]byte("PING x\r\n"))
	f.Fuzz(func(t *testing.T, b []byte) {
		for _, lim := range []Limits{PreAuthLimits, AuthLimits} {
			r := bufio.NewReader(strings.NewReader(string(b)))
			for i := 0; i < 8; i++ {
				if _, err := ReadCommandLimits(r, lim); err != nil {
					break
				}
			}
		}
	})
}

func FuzzCopyFrame(f *testing.F) {
	f.Add([]byte("*2\r\n$3\r\nGET\r\n:1\r\n"))
	f.Add([]byte("|1\r\n+a\r\n+b\r\n%1\r\n+k\r\n_\r\n"))
	f.Fuzz(func(t *testing.T, b []byte) {
		w := bufio.NewWriter(io.Discard)
		CopyFrame(bufio.NewReader(strings.NewReader(string(b))), w)
		ReadValue(bufio.NewReader(strings.NewReader(string(b))))
	})
}

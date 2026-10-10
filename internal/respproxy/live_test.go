package respproxy

import (
	"context"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestLivePassThrough drives a real Redis through the enforcing proxy with
// go-redis (the client library JuiceFS uses): HELLO 3 AUTH, SELECT,
// pipelining, WATCH/MULTI/EXEC, SCRIPT LOAD + EVALSHA, and keyspace
// notifications as RESP3 push frames.
func TestLivePassThrough(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	if err := rdb.ConfigSet(ctx, "notify-keyspace-events", "KEA").Err(); err != nil {
		t.Fatal(err)
	}
	tp := startProxy(t, addr, ModeEnforce)
	tp.Stats.TrackAnchors = true
	mac := tp.client(t, "mac1")

	sub := mac.PSubscribe(ctx, fmt.Sprintf("__keyspace@%d__:*", testDB))
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}

	cmds, err := mac.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i := 0; i < 100; i++ {
			pipe.HSet(ctx, "d1", "f", i)
		}
		pipe.HGet(ctx, "d1", "f")
		return nil
	})
	if err != nil || cmds[100].(*redis.StringCmd).Val() != "99" {
		t.Fatalf("pipeline: %v", err)
	}

	// WATCH / MULTI / EXEC, and a conflicting writer that must abort it.
	err = mac.Watch(ctx, func(tx *redis.Tx) error {
		if _, err := tx.Get(ctx, "i2").Result(); err != redis.Nil {
			return err
		}
		if err := rdb.Set(ctx, "i2", "other", 0).Err(); err != nil {
			return err
		}
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "i2", "mine", 0)
			return nil
		})
		return err
	}, "i2")
	if err != redis.TxFailedErr {
		t.Fatalf("WATCH conflict: got %v, want TxFailedErr", err)
	}
	if v, _ := rdb.Get(ctx, "i2").Result(); v != "other" {
		t.Fatalf("aborted txn applied: %q", v)
	}
	if _, err := mac.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, "i3", "a", 0)
		pipe.HIncrBy(ctx, "dirUsedSpace", "1", 4096)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// A pinned juicefs script loads and runs (forwarded as EVALSHA_RO).
	lookup := juicefsScript(t, "scriptLookup")
	sha, err := mac.ScriptLoad(ctx, lookup).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := mac.EvalSha(ctx, sha, []string{"d1", "missing"}).Err(); err == nil || !strings.Contains(err.Error(), "ENOENT") {
		t.Fatalf("evalsha scriptLookup: %v", err)
	}

	got := false
	deadline := time.After(5 * time.Second)
	for !got {
		select {
		case m := <-sub.Channel():
			if m.Channel == fmt.Sprintf("__keyspace@%d__:i3", testDB) {
				got = true
			}
		case <-deadline:
			t.Fatal("no keyspace notification through the proxy")
		}
	}
	// The upstream connection is named after the principal.
	list, err := rdb.ClientList(ctx).Result()
	if err != nil || !strings.Contains(list, "name=p:mac1:") {
		t.Fatalf("CLIENT LIST has no p:mac1:<connID>: %v\n%s", err, list)
	}

	snap := tp.Stats.Snapshot()
	if snap.Commands == 0 || len(snap.UnknownWriteKeys) != 0 || snap.WriteUnits["txn"] < 1 {
		t.Fatalf("stats: commands=%d unknown=%v units=%v", snap.Commands, snap.UnknownWriteKeys, snap.WriteUnits)
	}
	for _, d := range snap.Decisions {
		if d.Outcome != DecAllow {
			t.Fatalf("unexpected refusal in a legitimate session: %+v %+v", d, snap.Recent)
		}
	}
}

// RESP2 pub/sub: the connection turns into a message stream and still
// carries messages; a non-pub/sub command on it closes it.
func TestLiveRESP2PubSub(t *testing.T) {
	addr := redisAddr(t, false)
	direct(t, addr)
	ctx := ctx5(t)
	tp := startProxy(t, addr, ModeEnforce)
	mac := tp.client(t, "mac1", func(o *redis.Options) { o.Protocol = 2 })
	sub := mac.Subscribe(ctx, "juicemount:metadata")
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tp.client(t, "mac2").Publish(ctx, "juicemount:metadata", "hello").Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-sub.Channel():
		if m.Payload != "hello" {
			t.Fatalf("payload %q", m.Payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no RESP2 message through the proxy")
	}
	if err := sub.Ping(ctx); err != nil {
		t.Fatalf("PING on a RESP2 pub/sub connection: %v", err)
	}
}

func juicefsScript(t testing.TB, name string) string {
	b, err := os.ReadFile("../../third_party/juicefs/pkg/meta/lua_scripts.go")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile("(?s)const " + name + " = `(.*?)`").FindSubmatch(b)
	if m == nil {
		t.Fatalf("%s not found", name)
	}
	return string(m[1])
}

// Review H8B-L1 #1: Lua is gated by pinned SHA1, never by reading the body.
// Every bypass that text matching let through is refused for the Mac and
// WebDAV principals; pinned scripts run read-only; the farm (a trusted
// server principal) may run its own scripts.
func TestLiveScriptsPinned(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	tp := startProxy(t, addr, ModeEnforce)
	bypasses := [][]string{
		{"EVAL", "return redis.call('GET'..'DEL', KEYS[1])", "1", "setting"},
		{"EVAL", "return server.call('HSET', 'jmlease:v1:cfg', 'mode', 'passthrough')", "0"},
		{"EVAL", "return server.call('FLUSHDB')", "0"},
		{"EVAL", "return _G['re'..'dis'].call('FLUSHDB')", "0"},
		{"EVAL", "return _G['re'..'dis'].call('DEL', 'i5')", "0"},
		{"EVAL_RO", "return redis.call('GET', 'i5')", "0"},
		{"EVALSHA", "0123456789012345678901234567890123456789", "0"},
		{"SCRIPT", "LOAD", "return server.call('FLUSHDB')"},
		{"FCALL", "f", "0"},
	}
	for _, who := range []string{"mac1", "dav1"} {
		rdb.Set(ctx, "setting", "VOLUME-FORMAT", 0)
		rdb.Set(ctx, "i5", "x", 0)
		rc := tp.raw(t)
		rc.login(who)
		for _, b := range bypasses {
			mustDeny(t, rc.do(b...), ReasonAdminOnly, who+" "+strings.Join(b, " "))
		}
		if rdb.Get(ctx, "setting").Val() != "VOLUME-FORMAT" || rdb.Exists(ctx, "i5").Val() != 1 || rdb.Exists(ctx, ConfigKey).Val() != 0 {
			t.Fatalf("%s: a refused script ran", who)
		}
	}
	mac := tp.client(t, "mac1")
	before := evalRO(t, rdb)
	lookup := juicefsScript(t, "scriptLookup")
	// go-redis Script.Run: EVALSHA (NOSCRIPT after FLUSH) then EVAL fallback.
	rdb.ScriptFlush(ctx)
	if err := redis.NewScript(lookup).Run(ctx, mac, []string{"d1", "missing"}).Err(); err == nil || !strings.Contains(err.Error(), "ENOENT") {
		t.Fatalf("pinned script via Script.Run: %v", err)
	}
	if evalRO(t, rdb) <= before {
		t.Fatal("pinned script was not forwarded as EVAL_RO/EVALSHA_RO")
	}
	farm := tp.client(t, "farm1")
	if err := farm.Eval(ctx, "return redis.call('SET', KEYS[1], 'job')", []string{"juicefarm:test"}).Err(); err != nil {
		t.Fatalf("farm script: %v", err)
	}
}

func evalRO(t *testing.T, rdb *redis.Client) int64 {
	info := rdb.Info(context.Background(), "commandstats").Val()
	var n int64
	for _, l := range strings.Split(info, "\n") {
		if strings.HasPrefix(l, "cmdstat_eval_ro:") || strings.HasPrefix(l, "cmdstat_evalsha_ro:") {
			var c int64
			fmt.Sscanf(l[strings.Index(l, "calls=")+6:], "%d", &c)
			n += c
		}
	}
	return n
}

func TestLiveSingletonRefusesSecondProxy(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	single := func(c *Config) { c.Singleton = true }
	tp := startProxy(t, addr, ModeEnforce, single)
	if got := rdb.Get(ctx, SingletonKey).Val(); got != tp.Incarnation() || got == "" {
		t.Fatalf("singleton key = %q, want %q", got, tp.Incarnation())
	}
	if ttl := rdb.PTTL(ctx, SingletonKey).Val(); ttl <= 0 || ttl > tp.cfg.SingletonTTL {
		t.Fatalf("singleton TTL %v", ttl)
	}
	p2, err := New(Config{Upstream: addr, Principals: principalsForTest(t), VolumeDB: testDB, Singleton: true, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if err := p2.Start(); err == nil || !strings.Contains(err.Error(), "refusing to start") {
		t.Fatalf("second proxy Start: %v", err)
	}
	// A clean shutdown releases the key so a successor can start.
	tp.Close()
	if rdb.Exists(ctx, SingletonKey).Val() != 0 {
		t.Fatal("singleton not released on Close")
	}
	p3, _ := New(Config{Upstream: addr, Principals: principalsForTest(t), VolumeDB: testDB, Singleton: true, Logf: t.Logf})
	if err := p3.Start(); err != nil {
		t.Fatalf("successor: %v", err)
	}
	p3.Close()
}

// The mode follows jmlease:v1:cfg "mode" without a restart; observe admits
// a guard refusal and records WOULD_DENY.
func TestLiveModeFromConfigKeyAndObserve(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	pol := &recordingPolicy{} // refuses i666 (a stand-in lease refusal)
	tp := startProxy(t, addr, ModeEnforce, func(c *Config) { c.Policy = pol })
	rc := tp.raw(t)
	rc.login("mac1")
	mustDeny(t, rc.do("SET", "i666", "x"), ReasonLeaseHeld, "enforce lease")

	rdb.HSet(ctx, ConfigKey, "mode", "observe")
	waitMode(t, tp.Proxy, ModeObserve)
	mustOK(t, rc.do("SET", "i666", "x"), "observe admits the lease refusal")
	if rdb.Get(ctx, "i666").Val() != "x" {
		t.Fatal("observe did not admit")
	}
	would := false
	for _, d := range tp.Stats.Snapshot().Decisions {
		if d.Outcome == DecWouldDeny && d.Reason == ReasonLeaseHeld && d.Principal == "mac1" {
			would = true
		}
	}
	if !would || !tp.logs.contains("WOULD_DENY LEASE_HELD") {
		t.Fatal("observe did not record/log WOULD_DENY")
	}
	units, _ := pol.snapshot()
	n := len(units)

	rdb.HSet(ctx, ConfigKey, "mode", "passthrough")
	waitMode(t, tp.Proxy, ModePassthrough)
	mustOK(t, rc.do("SET", "i666", "y"), "passthrough")
	if units, _ := pol.snapshot(); len(units) != n {
		t.Fatal("passthrough called Decide")
	}
	rdb.HDel(ctx, ConfigKey, "mode")
}

func waitMode(t *testing.T, p *Proxy, m Mode) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if p.Mode() == m {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("mode stayed %s, want %s", p.Mode(), m)
}

// recordingPolicy is a stand-in for L2: it refuses writes to i666 and
// records every unit and outcome.
type recordingPolicy struct {
	mu       sync.Mutex
	units    []*Unit
	outcomes []Outcome
}

func (r *recordingPolicy) Decide(p Principal, u *Unit) Verdict {
	r.mu.Lock()
	r.units = append(r.units, u)
	r.mu.Unlock()
	v := Verdict{Done: func(o Outcome) {
		r.mu.Lock()
		r.outcomes = append(r.outcomes, o)
		r.mu.Unlock()
	}}
	for _, w := range u.Project {
		if w.Raw == "i666" {
			v.Deny = &Denial{Reason: ReasonLeaseHeld, Proj: 42, Holder: "mac2", Host: "mac2-host", Epoch: 7}
		}
	}
	return v
}

func (r *recordingPolicy) HandleLocal(p Principal, db int, args [][]byte) []byte {
	return []byte("+local:" + p.Name + "\r\n")
}

func (r *recordingPolicy) snapshot() ([]*Unit, []Outcome) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*Unit(nil), r.units...), append([]Outcome(nil), r.outcomes...)
}

// The Decide seam: called once per unit with project writes only, the
// refusal is rendered per §1.5 in the right slot, Done fires exactly once
// with the right outcome, and JM.* goes to the policy.
func TestLivePolicySeam(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	pol := &recordingPolicy{}
	tp := startProxy(t, addr, ModeEnforce, func(c *Config) { c.Policy = pol })
	rc := tp.raw(t)
	rc.login("mac1")

	mustOK(t, rc.do("GET", "i1"), "read")        // no Decide
	mustOK(t, rc.do("INCR", "nextinode"), "ctr") // no Decide
	mustOK(t, rc.do("SET", "i5", "a"), "bare project write")
	v := rc.do("SET", "i666", "a")
	if string(v.Str) != "JMDENY LEASE_HELD proj=42 holder=mac2@mac2-host epoch=7" {
		t.Fatalf("lease refusal = %q", v.Str)
	}
	// Committed EXEC; WATCH-aborted EXEC.
	rc.send("MULTI")
	rc.send("SET", "i7", "a")
	rc.send("EXEC")
	rc.flush()
	rc.read()
	rc.read()
	rc.read()
	rc.do("WATCH", "i9")
	rdb.Set(ctx, "i9", "z", 0)
	rc.send("MULTI")
	rc.send("SET", "i9", "a")
	rc.send("EXEC")
	rc.flush()
	rc.read()
	rc.read()
	if v := rc.read(); !v.Null {
		t.Fatalf("WATCH-aborted EXEC = %+v", v)
	}
	if v := rc.do("JM.LEASE", "STATUS"); string(v.Str) != "local:mac1" {
		t.Fatalf("JM.LEASE = %q", v.Str)
	}
	if rdb.Exists(ctx, "i666").Val() != 0 {
		t.Fatal("policy refusal not enforced")
	}
	units, outs := pol.snapshot()
	if len(units) != 4 {
		t.Fatalf("Decide called %d times, want 4", len(units))
	}
	if units[2].Kind != UnitTxn || units[3].Watched[0] != "i9" || units[0].DB != testDB {
		t.Fatalf("unit fields: %+v %+v", units[2], units[3])
	}
	id := units[0].ConnID
	want := []Outcome{
		{Forwarded: true, Committed: true, ConnID: id}, // SET i5
		{ConnID: id}, // refused i666
		{Forwarded: true, Committed: true, ConnID: id},  // EXEC
		{Forwarded: true, Committed: false, ConnID: id}, // WATCH abort
	}
	if fmt.Sprint(outs) != fmt.Sprint(want) {
		t.Fatalf("outcomes %+v, want %+v", outs, want)
	}
}

// Concurrency smoke under -race: many connections, pipelines and refusals
// interleaved; every reply stays in its slot.
func TestLiveConcurrentSlotAlignment(t *testing.T) {
	addr := redisAddr(t, false)
	direct(t, addr)
	tp := startProxy(t, addr, ModeEnforce)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			c, err := net.Dial("tcp", tp.addr)
			if err != nil {
				errs <- err
				return
			}
			defer c.Close()
			rc := &rawConn{t: t, c: c}
			rc.r, rc.w = newRW(c)
			rc.login("mac1")
			for round := 0; round < 50; round++ {
				for k := 0; k < 10; k++ {
					if k == 3 {
						rc.send("SET", "nope", "x")
					} else {
						rc.send("SET", fmt.Sprintf("i%d", g*1000+k), fmt.Sprint(round))
						rc.send("GET", fmt.Sprintf("i%d", g*1000+k))
					}
				}
				rc.flush()
				for k := 0; k < 10; k++ {
					if k == 3 {
						if v := rc.read(); !isDeny(v, ReasonUnknownKey) {
							errs <- fmt.Errorf("g%d r%d: slot 3 = %q", g, round, v.Str)
							return
						}
						continue
					}
					rc.read()
					if v := rc.read(); string(v.Str) != fmt.Sprint(round) {
						errs <- fmt.Errorf("g%d r%d k%d: GET = %q", g, round, k, v.Str)
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

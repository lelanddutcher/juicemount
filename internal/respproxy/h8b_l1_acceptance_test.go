//go:build h8b_acceptance

package respproxy

// H-8 option B lane L1 acceptance REDs (H8_OPTION_B_PLAN.md §2 L1).
// Run against a disposable Redis:
//
//	JM_RESPPROXY_TEST_REDIS=127.0.0.1:<port> go test -tags h8b_acceptance -run TestH8B ./internal/respproxy/
//
// Without a Redis these tests FAIL rather than skip.

import (
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

// A Mac principal cannot flush, reconfigure or wipe the volume.
func TestH8BMacPrincipalFlushDBRefused(t *testing.T) {
	addr := redisAddr(t, true)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	for i := 0; i < 3; i++ {
		rdb.Set(ctx, fmt.Sprintf("i%d", 100+i), "attr", 0)
	}
	tp := startProxy(t, addr, ModeEnforce)
	mac := tp.client(t, "mac1")

	err := mac.FlushDB(ctx).Err()
	if err == nil || !strings.HasPrefix(err.Error(), "JMDENY ADMIN_ONLY ") {
		t.Fatalf("FLUSHDB as mac: err=%v, want JMDENY ADMIN_ONLY", err)
	}
	for _, cmd := range [][]any{{"FLUSHALL"}, {"CONFIG", "SET", "appendonly", "no"}, {"SWAPDB", "0", "15"}, {"DEBUG", "SLEEP", "0"}} {
		if err := mac.Do(ctx, cmd...).Err(); err == nil || !strings.HasPrefix(err.Error(), "JMDENY ADMIN_ONLY ") {
			t.Fatalf("%v as mac: err=%v, want JMDENY ADMIN_ONLY", cmd, err)
		}
	}
	// Inside a transaction too.
	_, err = mac.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, "i200", "x", 0)
		p.FlushDB(ctx)
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "JMDENY ADMIN_ONLY") {
		t.Fatalf("MULTI{SET; FLUSHDB} as mac: err=%v", err)
	}
	if n := rdb.DBSize(ctx).Val(); n != 3 {
		t.Fatalf("DBSIZE after refused flushes = %d, want 3 (unchanged)", n)
	}
	if rdb.Exists(ctx, "i200").Val() != 0 {
		t.Fatal("refused transaction applied its SET")
	}
	// Positive control: the admin principal can flush through the same
	// proxy, so the refusal above is the gate, not a broken path.
	if err := tp.client(t, "admin1").FlushDB(ctx).Err(); err != nil {
		t.Fatalf("admin FLUSHDB: %v", err)
	}
	if n := rdb.DBSize(ctx).Val(); n != 0 {
		t.Fatalf("admin FLUSHDB left %d keys", n)
	}
}

// A refusal occupies exactly the refused command's reply slot, and a refused
// MULTI is DISCARDed upstream so nothing it queued (or WATCHed) changes.
func TestH8BDenyIsSlotAligned(t *testing.T) {
	addr := redisAddr(t, true)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	rdb.Set(ctx, "i9999", "sentinel", 0)
	tp := startProxy(t, addr, ModeEnforce)

	// 20-command pipeline, one write; command #7 (index 6) is refused.
	rc := tp.raw(t)
	rc.login("mac1")
	type exp struct {
		kind string // "ok", "deny", "get"
		val  string
	}
	model := map[string]string{"i9999": "sentinel"}
	var want []exp
	for k := 0; k < 20; k++ {
		key := fmt.Sprintf("i%d", 1000+k/2)
		switch {
		case k == 6:
			rc.send("FLUSHDB")
			want = append(want, exp{kind: "deny"})
		case k%2 == 0:
			v := fmt.Sprintf("v%d", k)
			rc.send("SET", key, v)
			model[key] = v
			want = append(want, exp{kind: "ok"})
		default:
			rc.send("GET", key)
			want = append(want, exp{kind: "get", val: model[key]})
		}
	}
	rc.flush()
	for k, w := range want {
		v := rc.read()
		switch w.kind {
		case "deny":
			mustDeny(t, v, ReasonAdminOnly, fmt.Sprintf("pipeline #%d", k+1))
		case "ok":
			if string(v.Str) != "OK" {
				t.Fatalf("pipeline #%d SET: %c %q", k+1, v.Type, v.Str)
			}
		case "get":
			if w.val == "" {
				if !v.Null {
					t.Fatalf("pipeline #%d GET: %q, want nil", k+1, v.Str)
				}
			} else if string(v.Str) != w.val {
				t.Fatalf("pipeline #%d GET = %q, want %q (reply slots shifted)", k+1, v.Str, w.val)
			}
		}
	}
	if rdb.Get(ctx, "i9999").Val() != "sentinel" {
		t.Fatal("refused FLUSHDB ran")
	}

	// WATCH ... MULTI ... (refused) EXEC, all in one pipelined write.
	rdb.Set(ctx, "i500", "orig", 0)
	before := tp.Stats.discards.Load()
	rc.send("WATCH", "i500")
	rc.send("MULTI")
	rc.send("SET", "i500", "changed")
	rc.send("FLUSHDB")
	rc.send("EXEC")
	rc.send("PING")
	rc.flush()
	for i, w := range []string{"OK", "OK", "QUEUED", "QUEUED"} {
		if v := rc.read(); string(v.Str) != w {
			t.Fatalf("txn reply %d = %c %q, want %s", i, v.Type, v.Str, w)
		}
	}
	mustDeny(t, rc.read(), ReasonAdminOnly, "EXEC slot")
	if v := rc.read(); string(v.Str) != "PONG" {
		t.Fatalf("after refused EXEC, PING = %c %q (connection still in MULTI?)", v.Type, v.Str)
	}
	if got := rdb.Get(ctx, "i500").Val(); got != "orig" {
		t.Fatalf("WATCHed key = %q after refused EXEC, want orig", got)
	}
	if rdb.Get(ctx, "i9999").Val() != "sentinel" {
		t.Fatal("refused FLUSHDB inside MULTI ran")
	}
	if d := tp.Stats.discards.Load() - before; d != 1 {
		t.Fatalf("DISCARD issued %d times, want 1", d)
	}
	// DISCARD also dropped the WATCH: a later transaction on this
	// connection commits even though i500 changed meanwhile.
	rdb.Set(ctx, "i500", "other", 0)
	rc.send("MULTI")
	rc.send("SET", "i501", "x")
	rc.send("EXEC")
	rc.flush()
	rc.read()
	rc.read()
	if v := rc.read(); v.Type != '*' || len(v.Elems) != 1 {
		t.Fatalf("follow-up EXEC = %c %q null=%v, want 1-element array", v.Type, v.Str, v.Null)
	}
}

// juicefs (pkg/meta/redis.go shouldRetry) and go-redis retry some error
// prefixes. A refusal retried would loop forever; no proxy-made reply may
// start with one.
func TestH8BNeverRetryablePrefix(t *testing.T) {
	// The mirror of shouldRetry must match the vendored source, so drift
	// in a juicefs upgrade fails here.
	src, err := os.ReadFile("../../third_party/juicefs/pkg/meta/redis.go")
	if err != nil {
		t.Fatalf("vendored juicefs: %v", err)
	}
	s := string(src)
	start := strings.Index(s, "func (m *redisMeta) shouldRetry(")
	if start < 0 {
		t.Fatal("shouldRetry not found in vendored juicefs")
	}
	body := s[start:]
	body = body[:strings.Index(body, "\n}\n")]
	var srcTokens []string
	for _, m := range regexp.MustCompile(`case "([A-Z]+)":`).FindAllStringSubmatch(body, -1) {
		srcTokens = append(srcTokens, m[1])
	}
	sort.Strings(srcTokens)
	mirror := []string{"ASK", "CLUSTERDOWN", "DISABLE", "ERR", "LOADING", "MOVED", "NOREAD", "NOWRITE", "READONLY", "TRYAGAIN"}
	if strings.Join(srcTokens, ",") != strings.Join(mirror, ",") {
		t.Fatalf("juicefs shouldRetry tokens changed: source %v, mirror %v — update the proxy's refusal checks", srcTokens, mirror)
	}
	for _, sub := range []string{`"EXECABORT"`, `"Conn is in a bad state"`, `"ERR max number of clients reached"`} {
		if !strings.Contains(body, sub) {
			t.Fatalf("juicefs shouldRetry no longer mentions %s — re-audit", sub)
		}
	}
	retried := func(msg string) bool {
		if msg == "ERR max number of clients reached" || strings.Contains(msg, "Conn is in a bad state") ||
			strings.Contains(msg, "EXECABORT") {
			return true
		}
		ps := strings.SplitN(msg, " ", 3)
		switch ps[0] {
		case "LOADING", "READONLY", "CLUSTERDOWN", "TRYAGAIN", "MOVED", "ASK",
			"MASTERDOWN", "NOREPLICAS": // the last two: go-redis's own retry set
			return true
		case "ERR":
			return len(ps) > 1 && (ps[1] == "DISABLE" || ps[1] == "NOWRITE" || ps[1] == "NOREAD" ||
				strings.HasPrefix(msg, "ERR max number of clients reached"))
		}
		return false
	}

	rng := rand.New(rand.NewSource(1))
	hostile := []string{"", "READONLY", "EXECABORT", "a b", "x\r\n-READONLY y", "ERR DISABLE", "ünïcode", strings.Repeat("h", 300), "\x00\x7f"}
	for _, reason := range AllReasons {
		for i := 0; i < 500; i++ {
			d := &Denial{
				Reason: reason, Proj: rng.Uint64(), Epoch: rng.Uint64(),
				Holder: hostile[rng.Intn(len(hostile))], Host: hostile[rng.Intn(len(hostile))],
				Detail: hostile[rng.Intn(len(hostile))],
			}
			msg := d.Error()
			if retried(msg) {
				t.Fatalf("refusal %q would be retried", msg)
			}
			f := strings.Fields(msg)
			if len(f) != 5 || f[0] != "JMDENY" || f[1] != string(reason) ||
				!strings.HasPrefix(f[2], "proj=") || !strings.HasPrefix(f[3], "holder=") || !strings.HasPrefix(f[4], "epoch=") {
				t.Fatalf("refusal %q is not `JMDENY <REASON> proj= holder= epoch=`", msg)
			}
			reply := d.Reply()
			if strings.Count(string(reply), "\r\n") != 1 || !strings.HasSuffix(string(reply), "\r\n") {
				t.Fatalf("refusal frame %q is not one line", reply)
			}
		}
	}
	for _, msg := range []string{wrongPass, "-ERR Protocol error: unauthenticated request too large", "ERR proxy: metadata server unavailable", "NOPROTO unsupported protocol version",
		"ERR proxy: AUTH is not allowed inside MULTI", "ERR unknown command 'JM.LEASE' (no lease service in this proxy)"} {
		if retried(msg) {
			t.Fatalf("proxy reply %q would be retried", msg)
		}
	}

	// End to end: go-redis with retries enabled sends a refused command
	// exactly once.
	addr := redisAddr(t, true)
	direct(t, addr)
	tp := startProxy(t, addr, ModeEnforce)
	mac := tp.client(t, "mac1", func(o *redis.Options) { o.MaxRetries = 5 })
	ctx := ctx5(t)
	if err := mac.FlushDB(ctx).Err(); err == nil {
		t.Fatal("FLUSHDB admitted")
	}
	n := int64(0)
	for _, d := range tp.Stats.Snapshot().Decisions {
		if d.Outcome == DecDeny && d.Principal == "mac1" {
			n += d.Count
		}
	}
	if n != 1 {
		t.Fatalf("refused FLUSHDB reached the proxy %d times, want 1 (client retried): %+v", n, tp.Stats.Snapshot().Recent)
	}
}

func TestH8BWrongPassRefused(t *testing.T) {
	addr := redisAddr(t, true)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	tp := startProxy(t, addr, ModeEnforce)

	rc := tp.raw(t)
	if v := rc.do("AUTH", "mac1", "not-the-password"); v.Type != '-' || !strings.HasPrefix(string(v.Str), "WRONGPASS") {
		t.Fatalf("AUTH with wrong password: %c %q", v.Type, v.Str)
	}
	mustDeny(t, rc.do("SET", "i1", "x"), ReasonAuthRequired, "SET after failed AUTH")
	if v := rc.do("HELLO", "3", "AUTH", "mac1", "nope"); !strings.HasPrefix(string(v.Str), "WRONGPASS") {
		t.Fatalf("HELLO 3 AUTH wrong: %c %q", v.Type, v.Str)
	}
	if v := rc.do("AUTH", "nosuchuser", "mac1-pass"); !strings.HasPrefix(string(v.Str), "WRONGPASS") {
		t.Fatalf("AUTH unknown user: %c %q", v.Type, v.Str)
	}
	if v := rc.do("AUTH", "mac1-pass"); !strings.HasPrefix(string(v.Str), "WRONGPASS") {
		t.Fatalf("AUTH <pass> (default user): %c %q", v.Type, v.Str)
	}
	// Another principal's password does not open this one.
	if v := rc.do("AUTH", "admin1", "mac1-pass"); !strings.HasPrefix(string(v.Str), "WRONGPASS") {
		t.Fatalf("AUTH admin1 with mac1's password: %c %q", v.Type, v.Str)
	}
	if rdb.Exists(ctx, "i1").Val() != 0 {
		t.Fatal("write landed from an unauthenticated connection")
	}
	bad := tp.client(t, "mac1", func(o *redis.Options) { o.Password = "wrong" })
	if err := bad.Set(ctx, "i2", "x", 0).Err(); err == nil || !strings.Contains(err.Error(), "WRONGPASS") && !strings.Contains(err.Error(), "AUTH_REQUIRED") {
		t.Fatalf("go-redis with wrong password: %v", err)
	}
	if rdb.Exists(ctx, "i2").Val() != 0 {
		t.Fatal("go-redis write with wrong password landed")
	}
	if tp.Stats.authBad.Load() < 5 {
		t.Fatalf("auth failures counted %d", tp.Stats.authBad.Load())
	}
	// A fresh IP-less view: the right password works on a new connection
	// from the throttle's point of view once failures are under the limit;
	// use a fresh proxy so the lockout above does not apply.
	tp2 := startProxy(t, addr, ModeEnforce)
	ok := tp2.raw(t)
	ok.login("mac1")
	mustOK(t, ok.do("SET", "i3", "x"), "SET after correct AUTH")
	if rdb.Get(ctx, "i3").Val() != "x" {
		t.Fatal("authenticated write did not land")
	}
}

func TestH8BUnauthenticatedRefused(t *testing.T) {
	addr := redisAddr(t, true)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	rdb.Set(ctx, "i42", "sentinel", 0)
	tp := startProxy(t, addr, ModePassthrough) // even passthrough authenticates

	rc := tp.raw(t)
	for _, cmd := range [][]string{
		{"PING"}, {"SELECT", "15"}, {"GET", "i42"}, {"SET", "i1", "x"}, {"DEL", "i42"}, {"FLUSHDB"},
		{"MULTI"}, {"HELLO", "3"}, {"CLIENT", "LIST"}, {"KEYS", "*"}, {"EVAL", "return 1", "0"},
		{"SUBSCRIBE", "juicemount:metadata"}, {"PUBLISH", "juicemount:metadata", "x"}, {"JM.LEASE", "STATUS"},
	} {
		mustDeny(t, rc.do(cmd...), ReasonAuthRequired, strings.Join(cmd, " "))
	}
	// Pipelined: every slot refused, in order.
	rc.send("SET", "i2", "x")
	rc.send("GET", "i42")
	rc.send("INCR", "nextinode")
	rc.flush()
	for i := 0; i < 3; i++ {
		mustDeny(t, rc.read(), ReasonAuthRequired, fmt.Sprintf("pipelined #%d", i+1))
	}
	anon := tp.client(t, "")
	if err := anon.Set(ctx, "i3", "x", 0).Err(); err == nil || !strings.Contains(err.Error(), "AUTH_REQUIRED") {
		t.Fatalf("go-redis without credentials: %v", err)
	}
	if rdb.Exists(ctx, "i1", "i2", "i3").Val() != 0 || rdb.Get(ctx, "i42").Val() != "sentinel" || rdb.Exists(ctx, "nextinode").Val() != 0 {
		t.Fatal("an unauthenticated command reached Redis")
	}
	if tp.Stats.unauthRefused.Load() < 17 {
		t.Fatalf("unauthenticated refusals counted %d", tp.Stats.unauthRefused.Load())
	}
	// The same connection works after AUTH.
	rc.login("mac1")
	if v := rc.do("GET", "i42"); string(v.Str) != "sentinel" {
		t.Fatalf("GET after AUTH: %q", v.Str)
	}
}

func TestH8BUnknownWriteShapeRefused(t *testing.T) {
	addr := redisAddr(t, true)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	tp := startProxy(t, addr, ModeEnforce)
	rc := tp.raw(t)
	rc.login("mac1")

	cases := []struct {
		cmd    []string
		reason Reason
	}{
		{[]string{"SET", "foo:bar", "x"}, ReasonUnknownKey},
		{[]string{"HSET", "random", "f", "v"}, ReasonUnknownKey},
		{[]string{"SET", "i12abc", "x"}, ReasonUnknownKey},
		{[]string{"RPUSH", "c5", "x"}, ReasonUnknownKey}, // chunk key without _<idx>
		{[]string{"SET", "juicemount:metadata", "x"}, ReasonUnknownKey},
		{[]string{"DEL", "i5", "notakey"}, ReasonUnknownKey}, // one bad key refuses the command
		{[]string{"HSET", "jmlease:v1:p:5", "holder", "mac1"}, ReasonAdminOnly},
		{[]string{"SET", "setting", "{}"}, ReasonAdminOnly},
		{[]string{"HSET", "dirQuota", "5", "x"}, ReasonAdminOnly},
		{[]string{"FOO.BAR", "k"}, ReasonAdminOnly},
		{[]string{"LPUSH", "juicefarm:queue", "job"}, ReasonFarmOnly},
		{[]string{"ZREM", "delfiles", "5:0"}, ReasonBGOnly},
		{[]string{"HSET", "jum:presence:other-host", "p", "1"}, ReasonAdminOnly},
		{[]string{"SET", "jm:link-bench:00", "x"}, ReasonUnknownKey}, // no expiry
	}
	for _, c := range cases {
		mustDeny(t, rc.do(c.cmd...), c.reason, strings.Join(c.cmd, " "))
	}
	// An unknown shape inside a transaction refuses the whole unit.
	rc.send("MULTI")
	rc.send("SET", "i7", "attr")
	rc.send("SET", "weird", "x")
	rc.send("EXEC")
	rc.flush()
	rc.read()
	rc.read()
	rc.read()
	mustDeny(t, rc.read(), ReasonUnknownKey, "EXEC with unknown shape")

	keys := rdb.Keys(ctx, "*").Val()
	if len(keys) != 0 {
		t.Fatalf("refused writes landed: %v", keys)
	}
	// Positive controls: known shapes go through.
	for _, cmd := range [][]string{
		{"SET", "i7", "attr"}, {"INCR", "nextinode"}, {"HSET", "jum:presence:mac1-host", "p", "1"},
		{"SET", "jm:link-bench:00", "x", "EX", "60"}, {"HINCRBY", "dirUsedSpace", "7", "4096"},
	} {
		mustOK(t, rc.do(cmd...), strings.Join(cmd, " "))
	}
}

// Every JuiceMount-owned key prefix in the tree classifies to a known class.
// The inventory is grep-built so a new key family added without a proxy rule
// fails here.
func TestH8BJuiceMountKeysClassified(t *testing.T) {
	root := "../.."
	lit := regexp.MustCompile(`["'` + "`" + `]((?:jum|jm|juicefarm|jmfarm|jmctl|jmlease|juicemount):[^"'` + "`" + `\s]*)["'` + "`" + `]`)
	inv := map[string][]string{} // literal -> files
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case "third_party", ".build", ".git", "node_modules", "build", "respproxy", "prototype", ".claude":
				return filepath.SkipDir
			}
			return nil
		}
		ext := filepath.Ext(path)
		if ext != ".go" && ext != ".lua" && ext != ".sh" && ext != ".py" && ext != ".swift" {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, m := range lit.FindAllStringSubmatch(string(b), -1) {
			inv[m[1]] = append(inv[m[1]], path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Validity gate: the walk must have found the families we know exist.
	for _, must := range []string{"juicefarm:queue", "jum:presence:", "jm:link-bench:%x", "juicemount:metadata", "juicefarm:job:"} {
		if _, ok := inv[must]; !ok {
			t.Fatalf("inventory missed %q: the walk is broken (found %d literals)", must, len(inv))
		}
	}
	if len(inv) < 20 {
		t.Fatalf("inventory has only %d literals; refusing to report", len(inv))
	}
	var unknown []string
	var names []string
	for k := range inv {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		key := strings.NewReplacer("%x", "0", "%s", "x", "%d", "0").Replace(k)
		if strings.HasSuffix(key, ":") {
			key += "x" // a prefix: classify a member of the family
		}
		cls := KeyClassOf(key)
		t.Logf("%-45s %-9s %v", k, cls, inv[k][0])
		if cls == KCUnknown {
			unknown = append(unknown, fmt.Sprintf("%s (%v)", k, inv[k]))
		}
	}
	if len(unknown) != 0 {
		t.Fatalf("%d JuiceMount key literals classify as unknown:\n%s", len(unknown), strings.Join(unknown, "\n"))
	}
	t.Logf("inventory: %d literals, 0 unknown", len(inv))
}

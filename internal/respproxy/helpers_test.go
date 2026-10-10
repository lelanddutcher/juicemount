package respproxy

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// testDB is the disposable Redis DB every live test uses.
const testDB = 15

// Test principals. Cheap argon2 parameters keep the suite fast; the
// verification path is the production one.
var testCreds = []struct {
	name, pass string
	kind       Kind
	host       string
}{
	{"mac1", "mac1-pass", KindMac, "mac1-host"},
	{"mac2", "mac2-pass", KindMac, "mac2-host"},
	{"admin1", "admin1-pass", KindAdmin, ""},
	{"bg1", "bg1-pass", KindBG, ""},
	{"farm1", "farm1-pass", KindFarm, ""},
	{"mgr1", "mgr1-pass", KindManager, ""},
	{"dav1", "dav1-pass", KindWebDAV, ""},
}

var (
	testPrincipalsOnce sync.Once
	testPrincipals     *Principals
)

func principalsForTest(t testing.TB) *Principals {
	testPrincipalsOnce.Do(func() {
		var b strings.Builder
		b.WriteString(`{"principals":[`)
		for i, c := range testCreds {
			h, err := HashPassword(c.pass, 1, 64, 1)
			if err != nil {
				panic(err)
			}
			if i > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, `{"name":%q,"kind":%q,"host":%q,"argon2id":%q}`, c.name, c.kind, c.host, h)
		}
		b.WriteString(`]}`)
		ps, err := ParsePrincipals([]byte(b.String()))
		if err != nil {
			panic(err)
		}
		testPrincipals = ps
	})
	return testPrincipals
}

func passOf(name string) string {
	for _, c := range testCreds {
		if c.name == name {
			return c.pass
		}
	}
	return ""
}

// redisAddr returns the disposable Redis address. Live tests skip without
// one; acceptance tests (required=true) FAIL, so a missing server can never
// read as a pass.
func redisAddr(t testing.TB, required bool) string {
	if a := os.Getenv("JM_RESPPROXY_TEST_REDIS"); a != "" {
		return a
	}
	if u := os.Getenv("JM_FARM_TEST_REDIS_URL"); u != "" {
		if pu, err := url.Parse(u); err == nil && pu.Host != "" {
			return pu.Host
		}
	}
	if required {
		t.Fatal("acceptance test needs a disposable Redis: set JM_RESPPROXY_TEST_REDIS=127.0.0.1:<port>")
	}
	t.Skip("JM_RESPPROXY_TEST_REDIS not set")
	return ""
}

// direct is a client straight to Redis (not through the proxy) for setup
// and for checking what really happened.
func direct(t testing.TB, addr string) *redis.Client {
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: testDB, Protocol: 3})
	t.Cleanup(func() { rdb.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush test DB: %v", err)
	}
	if err := rdb.Del(ctx, SingletonKey, ConfigKey).Err(); err != nil {
		t.Fatal(err)
	}
	return rdb
}

type testProxy struct {
	*Proxy
	addr string
	logs *logSink
}

type logSink struct {
	mu    sync.Mutex
	lines []string
	t     testing.TB
}

func (l *logSink) Logf(format string, args ...any) {
	s := fmt.Sprintf(format, args...)
	l.mu.Lock()
	l.lines = append(l.lines, s)
	l.mu.Unlock()
}

func (l *logSink) contains(sub string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range l.lines {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func startProxy(t testing.TB, upstream string, mode Mode, mut ...func(*Config)) *testProxy {
	sink := &logSink{t: t}
	cfg := Config{
		Upstream:   upstream,
		Principals: principalsForTest(t),
		Mode:       mode,
		VolumeDB:   testDB,
		Logf:       sink.Logf,
		ModePoll:   50 * time.Millisecond,
	}
	for _, m := range mut {
		m(&cfg)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go p.Serve(ln)
	t.Cleanup(func() { p.Close() })
	return &testProxy{Proxy: p, addr: ln.Addr().String(), logs: sink}
}

// client returns a go-redis client through the proxy as principal name
// ("" = no credentials).
func (tp *testProxy) client(t testing.TB, name string, mut ...func(*redis.Options)) *redis.Client {
	o := &redis.Options{Addr: tp.addr, DB: testDB, Protocol: 3, MaxRetries: -1}
	if name != "" {
		o.Username, o.Password = name, passOf(name)
	}
	for _, m := range mut {
		m(o)
	}
	rdb := redis.NewClient(o)
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

// rawConn is a hand-driven RESP client, so tests can assert exact reply
// slots and pipelines.
type rawConn struct {
	t testing.TB
	c net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

func (tp *testProxy) raw(t testing.TB) *rawConn {
	c, err := net.Dial("tcp", tp.addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &rawConn{t: t, c: c, r: bufio.NewReader(c), w: bufio.NewWriter(c)}
}

// send buffers commands; flush sends them in one write.
func (rc *rawConn) send(args ...string) {
	if err := WriteCommand(rc.w, strArgs(args...)); err != nil {
		rc.t.Fatal(err)
	}
}

func (rc *rawConn) flush() {
	if err := rc.w.Flush(); err != nil {
		rc.t.Fatal(err)
	}
}

func (rc *rawConn) read() Value {
	rc.t.Helper()
	rc.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	v, err := ReadValue(rc.r)
	if err != nil {
		rc.t.Fatalf("read reply: %v", err)
	}
	return v
}

func (rc *rawConn) do(args ...string) Value {
	rc.t.Helper()
	rc.send(args...)
	rc.flush()
	return rc.read()
}

// login authenticates and selects the test DB.
func (rc *rawConn) login(name string) {
	rc.t.Helper()
	if v := rc.do("AUTH", name, passOf(name)); string(v.Str) != "OK" {
		rc.t.Fatalf("AUTH %s: %q", name, v.Str)
	}
	if v := rc.do("SELECT", fmt.Sprint(testDB)); string(v.Str) != "OK" {
		rc.t.Fatalf("SELECT: %q", v.Str)
	}
}

func isDeny(v Value, reason Reason) bool {
	return v.Type == '-' && strings.HasPrefix(string(v.Str), "JMDENY "+string(reason)+" ")
}

func mustDeny(t testing.TB, v Value, reason Reason, what string) {
	t.Helper()
	if !isDeny(v, reason) {
		t.Fatalf("%s: got %c %q, want -JMDENY %s", what, v.Type, v.Str, reason)
	}
}

func mustOK(t testing.TB, v Value, what string) {
	t.Helper()
	if v.Type == '-' || v.Type == '!' {
		t.Fatalf("%s: unexpected error %q", what, v.Str)
	}
}

func ctx5(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func be64(v uint64) []byte {
	b := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		b[i] = byte(v)
		v >>= 8
	}
	return b
}

func bytesArgs(s ...string) [][]byte { return strArgs(s...) }

func newRW(c net.Conn) (*bufio.Reader, *bufio.Writer) { return bufio.NewReader(c), bufio.NewWriter(c) }

// direct2 is a plain client on the test DB without the flush direct() does.
func direct2(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr, DB: testDB, Protocol: 3})
}

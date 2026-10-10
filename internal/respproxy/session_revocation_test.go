package respproxy

import (
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

func sessionRevocationEnv(t *testing.T) (string, *testProxy) {
	t.Helper()
	if os.Getenv("JM_RESPPROXY_TEST_REDIS") == "" {
		t.Fatal("fresh root-owned JM_RESPPROXY_TEST_REDIS is required")
	}
	addr := redisAddr(t, true)
	host, _, err := net.SplitHostPort(addr)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		t.Fatal("test Redis must use a numeric loopback address")
	}
	return addr, startProxy(t, addr, ModePassthrough)
}

func sessionRevocationConnections(a *Authenticator, name string) []*conn {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []*conn
	for c, rec := range a.sessions {
		if rec.p.Name == name {
			out = append(out, c)
		}
	}
	return out
}

func sessionRevocationRemove(a *Authenticator) {
	next := copySessionPrincipals(a.table.Load())
	delete(next.byName, "mac1")
	a.Swap(next)
}

func sessionRevocationClosed(t *testing.T, conns []*conn) {
	t.Helper()
	if len(conns) == 0 {
		t.Fatal("no authenticated target connection was registered")
	}
	for _, c := range conns {
		if !c.revoked.Load() {
			t.Fatal("target connection has no proxy revocation mark")
		}
		select {
		case <-c.dead:
		default:
			t.Fatal("target connection transport was not closed by Swap")
		}
	}
}

// A closed socket is acceptable only after proving its matching proxy-owned
// revocation mark, a healthy witness, and unchanged backing data.
func sessionRevocationTry(rc *rawConn, args ...string) (Value, error) {
	if err := rc.c.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return Value{}, err
	}
	if err := WriteCommand(rc.w, strArgs(args...)); err != nil {
		return Value{}, err
	}
	if err := rc.w.Flush(); err != nil {
		return Value{}, err
	}
	if err := rc.c.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return Value{}, err
	}
	return ReadValue(rc.r)
}

func sessionRevocationNoSuccess(t *testing.T, rc *rawConn, args ...string) {
	t.Helper()
	v, err := sessionRevocationTry(rc, args...)
	if err == nil {
		t.Fatalf("revoked socket answered %s instead of closing: %c %q", args[0], v.Type, v.Str)
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("revoked %s timed out; closure not proven: %v", args[0], err)
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) &&
		!errors.Is(err, syscall.ECONNRESET) && !errors.Is(err, syscall.EPIPE) {
		t.Fatalf("revoked %s returned unclassified transport error: %v", args[0], err)
	}
}

func TestSessionRevocationRemovalAndWitness(t *testing.T) {
	addr := os.Getenv("JM_RESPPROXY_TEST_REDIS")
	if addr == "" {
		t.Fatal("fresh root-owned JM_RESPPROXY_TEST_REDIS is required")
	}
	// direct flushes only DB15 on the root-admitted private Redis process.
	// Validate the endpoint before that destructive setup.
	host, _, err := net.SplitHostPort(addr)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		t.Fatal("test Redis must use a numeric loopback address")
	}
	rdb := direct(t, addr)
	_, tp := sessionRevocationEnv(t)
	target := tp.raw(t)
	target.login("mac1")
	witness := tp.raw(t)
	witness.login("mac2") // this exact socket must survive the target's Swap
	second := startProxy(t, addr, ModePassthrough)
	second.Auth = tp.Auth // share before the second proxy accepts any AUTH
	secondTarget := second.raw(t)
	secondTarget.login("mac1")
	if v := target.do("SET", "i3", "before"); v.Type != '+' || string(v.Str) != "OK" {
		t.Fatalf("pre-Swap target write: %c %q", v.Type, v.Str)
	}
	if got, err := rdb.Get(ctx5(t), "i3").Result(); err != nil || got != "before" {
		t.Fatalf("pre-Swap backing value=%q error=%v", got, err)
	}
	owned := sessionRevocationConnections(tp.Auth, "mac1")
	sessionRevocationRemove(tp.Auth)
	sessionRevocationClosed(t, owned)

	freshOld := tp.raw(t)
	if v := freshOld.do("AUTH", "mac1", passOf("mac1")); v.Type != '-' || !strings.HasPrefix(string(v.Str), "WRONGPASS") {
		t.Fatalf("fresh old AUTH after removal: %c %q", v.Type, v.Str)
	}
	if v := witness.do("PING"); v.Type != '+' || string(v.Str) != "PONG" {
		t.Fatalf("witness PING: %c %q", v.Type, v.Str)
	}
	if v := witness.do("SET", "i4", "healthy"); v.Type != '+' || string(v.Str) != "OK" {
		t.Fatalf("witness SET: %c %q", v.Type, v.Str)
	}
	if got, err := rdb.Get(ctx5(t), "i4").Result(); err != nil || got != "healthy" {
		t.Fatalf("witness backing value=%q error=%v", got, err)
	}
	sessionRevocationNoSuccess(t, target, "PING")
	sessionRevocationNoSuccess(t, target, "SET", "i3", "after")
	sessionRevocationNoSuccess(t, secondTarget, "PING")
	if got, err := rdb.Get(ctx5(t), "i3").Result(); err != nil || got != "before" {
		t.Fatalf("revoked target backing value=%q error=%v", got, err)
	}
	// Re-adding the name permits a new socket, never resurrects the old one.
	readded := copySessionPrincipals(tp.Auth.table.Load())
	readded.byName["mac1"] = principalsForTest(t).byName["mac1"]
	tp.Auth.Swap(readded)
	if !owned[0].revoked.Load() {
		t.Fatal("old target socket resurrected after re-add")
	}
	sessionRevocationNoSuccess(t, target, "PING")
	newTarget := tp.raw(t)
	newTarget.login("mac1")
	if v := newTarget.do("PING"); v.Type != '+' || string(v.Str) != "PONG" {
		t.Fatalf("fresh re-added target PING: %c %q", v.Type, v.Str)
	}
}

func TestSessionRevocationPipelineAndTxn(t *testing.T) {
	addr := os.Getenv("JM_RESPPROXY_TEST_REDIS")
	if addr == "" {
		t.Fatal("fresh root-owned JM_RESPPROXY_TEST_REDIS is required")
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		t.Fatal("test Redis must use a numeric loopback address")
	}
	rdb := direct(t, addr)
	_, tp := sessionRevocationEnv(t)
	if err := rdb.Set(ctx5(t), "i3", "before", 0).Err(); err != nil {
		t.Fatal(err)
	}
	txn := tp.raw(t)
	txn.login("mac1")
	if v := txn.do("MULTI"); v.Type != '+' || string(v.Str) != "OK" {
		t.Fatalf("MULTI: %c %q", v.Type, v.Str)
	}
	if v := txn.do("SET", "i3", "txn"); v.Type != '+' || string(v.Str) != "QUEUED" {
		t.Fatalf("queued SET: %c %q", v.Type, v.Str)
	}
	pipeline := tp.raw(t)
	pipeline.login("mac1")
	pipeline.send("PING")
	pipeline.send("SET", "i3", "late") // client-side buffered until after Swap
	owned := sessionRevocationConnections(tp.Auth, "mac1")
	sessionRevocationRemove(tp.Auth)
	sessionRevocationClosed(t, owned)
	sessionRevocationNoSuccess(t, txn, "EXEC")
	_ = pipeline.w.Flush() // may fail because the proxy closed the exact socket
	if got, err := rdb.Get(ctx5(t), "i3").Result(); err != nil || got != "before" {
		t.Fatalf("revoked pipeline or queued EXEC backing value=%q error=%v", got, err)
	}
}

type sessionRevocationBlockingPolicy struct {
	entered chan struct{}
	release chan struct{}
}

func (p *sessionRevocationBlockingPolicy) Decide(Principal, *Unit) Verdict {
	select {
	case p.entered <- struct{}{}:
	default:
	}
	<-p.release
	return Verdict{}
}

func TestSessionRevocationBlockedPolicyDoesNotDelaySwap(t *testing.T) {
	addr := os.Getenv("JM_RESPPROXY_TEST_REDIS")
	if addr == "" {
		t.Fatal("fresh root-owned JM_RESPPROXY_TEST_REDIS is required")
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		t.Fatal("test Redis must use a numeric loopback address")
	}
	rdb := direct(t, addr)
	if err := rdb.Set(ctx5(t), "i3", "before", 0).Err(); err != nil {
		t.Fatal(err)
	}
	blocker := &sessionRevocationBlockingPolicy{entered: make(chan struct{}, 1), release: make(chan struct{})}
	defer func() {
		select {
		case <-blocker.release:
		default:
			close(blocker.release)
		}
	}()
	tp := startProxy(t, addr, ModeEnforce, func(c *Config) { c.Policy = blocker })
	target := tp.raw(t)
	target.login("mac1")
	owned := sessionRevocationConnections(tp.Auth, "mac1")
	attempted := make(chan error, 1)
	go func() {
		v, err := sessionRevocationTry(target, "SET", "i3", "after")
		if err == nil && v.Type == '+' && string(v.Str) == "OK" {
			attempted <- errors.New("held write succeeded after revocation")
			return
		}
		attempted <- err
	}()
	select {
	case <-blocker.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("write never entered held policy")
	}
	swapped := make(chan struct{})
	go func() { sessionRevocationRemove(tp.Auth); close(swapped) }()
	select {
	case <-swapped:
	case <-time.After(2 * time.Second):
		t.Fatal("Swap waited on a held policy decision")
	}
	sessionRevocationClosed(t, owned)
	close(blocker.release)
	select {
	case err := <-attempted:
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) &&
			!errors.Is(err, syscall.ECONNRESET) && !errors.Is(err, syscall.EPIPE) {
			t.Fatalf("held request ended with unclassified error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("held request did not unwind after revocation")
	}
	if got, err := rdb.Get(ctx5(t), "i3").Result(); err != nil || got != "before" {
		t.Fatalf("held revoked write reached Redis: value=%q error=%v", got, err)
	}
	witness := tp.raw(t)
	witness.login("mac2")
	if v := witness.do("SET", "i4", "healthy"); v.Type != '+' || string(v.Str) != "OK" {
		t.Fatalf("witness SET after held policy: %c %q", v.Type, v.Str)
	}
}

func TestSessionRevocationPubSubBlocked(t *testing.T) {
	addr := os.Getenv("JM_RESPPROXY_TEST_REDIS")
	if addr == "" {
		t.Fatal("fresh root-owned JM_RESPPROXY_TEST_REDIS is required")
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		t.Fatal("test Redis must use a numeric loopback address")
	}
	direct(t, addr)
	_, tp := sessionRevocationEnv(t)
	idle := tp.raw(t)
	idle.login("mac1")
	resp2 := tp.raw(t)
	resp2.login("mac1")
	if v := resp2.do("SUBSCRIBE", "juicemount:metadata"); v.Type == '-' || v.Type == '!' {
		t.Fatalf("RESP2 subscribe: %c %q", v.Type, v.Str)
	}
	resp3 := tp.raw(t)
	resp3.login("mac1")
	if v := resp3.do("HELLO", "3"); v.Type == '-' || v.Type == '!' {
		t.Fatalf("RESP3 hello: %c %q", v.Type, v.Str)
	}
	if v := resp3.do("SUBSCRIBE", "juicemount:metadata"); v.Type == '-' || v.Type == '!' {
		t.Fatalf("RESP3 subscribe: %c %q", v.Type, v.Str)
	}
	owned := sessionRevocationConnections(tp.Auth, "mac1")
	sessionRevocationRemove(tp.Auth)
	sessionRevocationClosed(t, owned)
	sessionRevocationNoSuccess(t, idle, "PING")
	sessionRevocationNoSuccess(t, resp2, "PING")
	sessionRevocationNoSuccess(t, resp3, "PING")
	witness := tp.raw(t)
	witness.login("mac2")
	if v := witness.do("PING"); v.Type != '+' || string(v.Str) != "PONG" {
		t.Fatalf("witness PING after pubsub retirement: %c %q", v.Type, v.Str)
	}
}

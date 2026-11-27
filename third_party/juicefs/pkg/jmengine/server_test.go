package jmengine

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/jmengine/wire"
)

type fakeEngine struct{ block chan struct{} }

func (fakeEngine) VolumeID() string   { return "vol-uuid" }
func (fakeEngine) VolumeName() string { return "vol" }
func (fakeEngine) SessionID() uint64  { return 0 }
func (fakeEngine) ReadOnly() bool     { return true }
func (fakeEngine) Metrics() string    { return "m 1\n" }
func (f fakeEngine) GetAttr(ctx context.Context, ino uint64) (wire.Attr, syscall.Errno) {
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return wire.Attr{}, syscall.ETIMEDOUT
		}
	}
	if ino == 404 {
		return wire.Attr{}, syscall.ENOENT
	}
	return wire.Attr{Inode: ino, Size: 7}, 0
}

var testToken = bytes.Repeat([]byte{7}, 32)

func serve(t *testing.T, cfg Config) (*Server, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "s")
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Listener = l
	if cfg.Token == nil {
		cfg.Token = testToken
	}
	if cfg.Engine == nil {
		cfg.Engine = fakeEngine{}
	}
	if cfg.AllowPeer == nil {
		cfg.AllowPeer = func(int, int) bool { return true }
	}
	cfg.Version = "test-jm0"
	cfg.Epoch = 42
	s := New(cfg)
	go s.Serve()
	t.Cleanup(func() { s.Close(time.Second) })
	return s, path
}

func call(t *testing.T, c net.Conn, op wire.Op, body interface{}) wire.Reply {
	t.Helper()
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	if err := wire.WriteRequest(c, wire.Request{Op: op, Body: b}); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	r, err := wire.ReadReply(c)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	return r
}

func dialHello(t *testing.T, path string, token []byte, major int) (net.Conn, wire.Reply) {
	t.Helper()
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, call(t, c, wire.OpHello, wire.HelloRequest{ProtoMajor: major, Token: hex.EncodeToString(token)})
}

func TestHelloPingGetAttrShutdown(t *testing.T) {
	s, path := serve(t, Config{})
	c, r := dialHello(t, path, testToken, wire.ProtoMajor)
	if r.Status != 0 {
		t.Fatalf("hello status %d", r.Status)
	}
	var h wire.HelloReply
	if err := json.Unmarshal(r.Body, &h); err != nil || h.Epoch != 42 || h.VolumeID != "vol-uuid" || h.JuiceFSVersion != "test-jm0" {
		t.Fatalf("hello %+v %v", h, err)
	}
	var p wire.PingReply
	if err := json.Unmarshal(call(t, c, wire.OpPing, nil).Body, &p); err != nil || p.Epoch != 42 || p.ObjectOKAgeMs != -1 {
		t.Fatalf("ping %+v %v", p, err)
	}
	var a wire.Attr
	if r := call(t, c, wire.OpGetAttr, wire.GetAttrRequest{Inode: 1}); r.Status != 0 || json.Unmarshal(r.Body, &a) != nil || a.Size != 7 {
		t.Fatalf("getattr %+v", r)
	}
	if r := call(t, c, wire.OpGetAttr, wire.GetAttrRequest{Inode: 404}); r.Status != uint32(syscall.ENOENT) {
		t.Fatalf("getattr missing: %d", r.Status)
	}
	var st wire.StatsReply
	if err := json.Unmarshal(call(t, c, wire.OpStats, nil).Body, &st); err != nil || st.Requests["getattr"] != 2 || st.Metrics == "" {
		t.Fatalf("stats %+v %v", st, err)
	}
	if r := call(t, c, wire.OpShutdown, nil); r.Status != 0 {
		t.Fatalf("shutdown status %d", r.Status)
	}
	select {
	case <-s.ShutdownRequested():
	case <-time.After(time.Second):
		t.Fatal("shutdown not signalled")
	}
}

func TestRefusals(t *testing.T) {
	_, path := serve(t, Config{})
	if _, r := dialHello(t, path, bytes.Repeat([]byte{8}, 32), wire.ProtoMajor); r.Status != uint32(syscall.EACCES) {
		t.Fatalf("wrong token: status %d", r.Status)
	}
	if _, r := dialHello(t, path, nil, wire.ProtoMajor); r.Status != uint32(syscall.EACCES) {
		t.Fatalf("no token: status %d", r.Status)
	}
	_, r := dialHello(t, path, testToken, wire.ProtoMajor+1)
	var h wire.HelloReply
	if r.Status != uint32(syscall.EPROTONOSUPPORT) || json.Unmarshal(r.Body, &h) != nil || h.ProtoMajor != wire.ProtoMajor {
		t.Fatalf("other major: status %d body %s", r.Status, r.Body)
	}
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if r := call(t, c, wire.OpPing, nil); r.Status != uint32(syscall.EACCES) {
		t.Fatalf("ping before hello: status %d", r.Status)
	}
}

func TestPeerRefused(t *testing.T) {
	_, path := serve(t, Config{AllowPeer: func(uid, pid int) bool { return false }})
	if _, r := dialHello(t, path, testToken, wire.ProtoMajor); r.Status != uint32(syscall.EACCES) {
		t.Fatalf("peer: status %d", r.Status)
	}
}

func TestDefaultPeerIsParentOnly(t *testing.T) {
	def := New(Config{}).cfg.AllowPeer
	if def(syscall.Getuid(), syscall.Getpid()) {
		t.Fatal("default peer check accepted a process that is not the parent")
	}
	if !def(syscall.Getuid(), syscall.Getppid()) {
		t.Fatal("default peer check refused the parent")
	}
	if def(syscall.Getuid()+1, syscall.Getppid()) {
		t.Fatal("default peer check accepted another user")
	}
}

func TestPeerCredIsTheDialer(t *testing.T) {
	var got [2]int
	done := make(chan struct{})
	_, path := serve(t, Config{AllowPeer: func(uid, pid int) bool { got = [2]int{uid, pid}; close(done); return true }})
	dialHello(t, path, testToken, wire.ProtoMajor)
	<-done
	if got != [2]int{syscall.Getuid(), syscall.Getpid()} {
		t.Fatalf("peer credentials %v, want this process", got)
	}
}

func TestDeadlineAndBusy(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	_, path := serve(t, Config{Engine: fakeEngine{block: block}, MaxInFlight: 1})
	c, _ := dialHello(t, path, testToken, wire.ProtoMajor)
	b, _ := json.Marshal(wire.GetAttrRequest{Inode: 1})
	start := time.Now()
	if err := wire.WriteRequest(c, wire.Request{Op: wire.OpGetAttr, DeadlineMs: 100, Body: b}); err != nil {
		t.Fatal(err)
	}
	// While that call blocks, a second connection is over the cap.
	c2, _ := dialHello(t, path, testToken, wire.ProtoMajor)
	if r := call(t, c2, wire.OpPing, nil); r.Status != uint32(syscall.EBUSY) {
		t.Logf("ping during a blocked call: status %d (raced the deadline)", r.Status)
	}
	r, err := wire.ReadReply(c)
	if err != nil || r.Status != uint32(syscall.ETIMEDOUT) || time.Since(start) > 2*time.Second {
		t.Fatalf("deadline: %+v %v after %s", r, err, time.Since(start))
	}
}

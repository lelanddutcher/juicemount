// Package jmengine serves the JuiceFS engine to the JuiceMount app over a
// private Unix socket (JuiceMount patch 0037). The `juicefs jm-engine`
// command builds the engine as `juicefs mount` does, without a mount, and
// hands it to this server through the Engine interface.
//
// Who may connect (all three must pass):
//  1. the socket lives in a folder only the app's user can enter (the app
//     makes it, mode 0700);
//  2. the kernel's peer credentials: same user ID as the helper, and the
//     helper's own parent process (the app that started it);
//  3. the first request is OpHello with the launch token from the start
//     message.
package jmengine

import (
	"context"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/jmengine/wire"
)

// Engine is what the server needs from the engine. The jm-engine command
// adapts the JuiceFS VFS to it; tests use a fake.
type Engine interface {
	// Volume identity for the hello.
	VolumeID() string
	VolumeName() string
	SessionID() uint64
	ReadOnly() bool
	// GetAttr reads one inode's attributes. ctx ends at the request deadline.
	GetAttr(ctx context.Context, ino uint64) (wire.Attr, syscall.Errno)
	// Metrics is the engine registry as Prometheus text ("" if none).
	Metrics() string
}

// Config configures a Server.
type Config struct {
	Listener *net.UnixListener
	Token    []byte // the launch token
	Engine   Engine
	Version  string // juicefs version string for the hello
	Epoch    uint64 // random per helper start
	// AllowPeer decides on the kernel's peer credentials. nil = same user ID
	// as this process and the parent process of this process.
	AllowPeer func(uid, pid int) bool
	// MaxInFlight caps requests being served at once; over it the server
	// answers EBUSY at once. 0 = 256.
	MaxInFlight int
	// DefaultDeadline bounds a request that names none. 0 = 60 s.
	DefaultDeadline time.Duration
	// ProtoMajor/ProtoMinor override the protocol version (tests only).
	ProtoMajor, ProtoMinor int
	Logf                   func(format string, args ...interface{})
}

// Server serves one engine on one listener.
type Server struct {
	cfg      Config
	start    time.Time
	shutdown chan struct{}
	shutOnce sync.Once
	closing  atomic.Bool
	inFlight atomic.Int64
	conns    atomic.Int64
	metaOK   atomic.Int64 // unix nanos of the last good engine metadata call
	objOK    atomic.Int64 // unix nanos of the last good object request (none in step 1)
	mu       sync.Mutex
	live     map[net.Conn]struct{}
	requests sync.Map // op name -> *atomic.Uint64
	refused  sync.Map // reason -> *atomic.Uint64
	wg       sync.WaitGroup
}

// New returns a server; call Serve.
func New(cfg Config) *Server {
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 256
	}
	if cfg.DefaultDeadline <= 0 {
		cfg.DefaultDeadline = 60 * time.Second
	}
	if cfg.ProtoMajor == 0 {
		cfg.ProtoMajor, cfg.ProtoMinor = wire.ProtoMajor, wire.ProtoMinor
	}
	if cfg.AllowPeer == nil {
		uid, ppid := os.Getuid(), os.Getppid()
		cfg.AllowPeer = func(u, p int) bool { return u == uid && p == ppid }
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...interface{}) {}
	}
	return &Server{cfg: cfg, start: time.Now(), shutdown: make(chan struct{}), live: map[net.Conn]struct{}{}}
}

// Ops is the list of operations this server answers.
var Ops = []wire.Op{wire.OpHello, wire.OpPing, wire.OpStats, wire.OpGetAttr, wire.OpShutdown}

// ShutdownRequested is closed when a client sent OpShutdown.
func (s *Server) ShutdownRequested() <-chan struct{} { return s.shutdown }

// NoteMetaOK records a good metadata request made outside the server (the
// engine's start).
func (s *Server) NoteMetaOK() { s.metaOK.Store(time.Now().UnixNano()) }

// Serve accepts until Close. It returns nil after Close.
func (s *Server) Serve() error {
	for {
		c, err := s.cfg.Listener.AcceptUnix()
		if err != nil {
			if s.closing.Load() {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		s.mu.Lock()
		if s.closing.Load() {
			s.mu.Unlock()
			c.Close()
			return nil
		}
		s.live[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.serveConn(c)
	}
}

// Close stops accepting, closes every connection and waits up to bound for
// requests being served.
func (s *Server) Close(bound time.Duration) {
	s.closing.Store(true)
	_ = s.cfg.Listener.Close()
	s.mu.Lock()
	for c := range s.live {
		_ = c.Close()
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(bound):
		s.cfg.Logf("jm-engine: %d requests still running at close", s.inFlight.Load())
	}
}

func count(m *sync.Map, key string) {
	v, _ := m.LoadOrStore(key, new(atomic.Uint64))
	v.(*atomic.Uint64).Add(1)
}

func snapshot(m *sync.Map) map[string]uint64 {
	out := map[string]uint64{}
	m.Range(func(k, v interface{}) bool {
		out[k.(string)] = v.(*atomic.Uint64).Load()
		return true
	})
	return out
}

func (s *Server) refuse(c net.Conn, reason string, status syscall.Errno, body []byte) {
	count(&s.refused, reason)
	s.cfg.Logf("jm-engine: refused a connection: %s", reason)
	_ = c.SetWriteDeadline(time.Now().Add(time.Second))
	_ = wire.WriteReply(c, wire.Reply{Status: uint32(status), Body: body})
}

func (s *Server) serveConn(c *net.UnixConn) {
	s.conns.Add(1)
	defer func() {
		s.conns.Add(-1)
		s.mu.Lock()
		delete(s.live, c)
		s.mu.Unlock()
		c.Close()
		s.wg.Done()
	}()
	uid, pid, err := peerCred(c)
	if err != nil {
		s.refuse(c, "peer credentials unavailable", syscall.EACCES, nil)
		return
	}
	if !s.cfg.AllowPeer(uid, pid) {
		s.refuse(c, "peer is not the app", syscall.EACCES, nil)
		return
	}
	// The hello must arrive promptly; later requests may come at any time.
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	req, err := wire.ReadRequest(c)
	if err != nil {
		count(&s.refused, "no hello")
		return
	}
	_ = c.SetReadDeadline(time.Time{})
	if req.Op != wire.OpHello {
		s.refuse(c, "first request is not hello", syscall.EACCES, nil)
		return
	}
	var hello wire.HelloRequest
	if json.Unmarshal(req.Body, &hello) != nil {
		s.refuse(c, "hello does not parse", syscall.EINVAL, nil)
		return
	}
	token, err := hex.DecodeString(hello.Token)
	if err != nil || len(s.cfg.Token) == 0 || subtle.ConstantTimeCompare(token, s.cfg.Token) != 1 {
		s.refuse(c, "wrong token", syscall.EACCES, nil)
		return
	}
	reply := s.helloReply()
	if hello.ProtoMajor != s.cfg.ProtoMajor {
		s.refuse(c, "protocol major differs", syscall.EPROTONOSUPPORT, reply)
		return
	}
	count(&s.requests, wire.OpHello.String())
	if wire.WriteReply(c, wire.Reply{Body: reply}) != nil {
		return
	}
	for {
		req, err := wire.ReadRequest(c)
		if err != nil {
			return
		}
		rep, closeAfter := s.handle(req)
		if wire.WriteReply(c, rep) != nil || closeAfter {
			return
		}
	}
}

func (s *Server) helloReply() []byte {
	ops := make([]string, len(Ops))
	for i, o := range Ops {
		ops[i] = o.String()
	}
	b, _ := json.Marshal(wire.HelloReply{
		ProtoMajor: s.cfg.ProtoMajor, ProtoMinor: s.cfg.ProtoMinor,
		JuiceFSVersion: s.cfg.Version, Ops: ops, Control: []int{},
		VolumeID: s.cfg.Engine.VolumeID(), VolumeName: s.cfg.Engine.VolumeName(),
		SessionID: s.cfg.Engine.SessionID(), Epoch: s.cfg.Epoch, PID: os.Getpid(),
		ReadOnly: s.cfg.Engine.ReadOnly(),
	})
	return b
}

func ageMs(nanos int64) int64 {
	if nanos == 0 {
		return -1
	}
	return time.Since(time.Unix(0, nanos)).Milliseconds()
}

func errReply(e syscall.Errno) wire.Reply { return wire.Reply{Status: uint32(e)} }

func jsonReply(v interface{}) wire.Reply {
	b, err := json.Marshal(v)
	if err != nil {
		return errReply(syscall.EIO)
	}
	return wire.Reply{Body: b}
}

func (s *Server) handle(req wire.Request) (wire.Reply, bool) {
	if s.inFlight.Add(1) > int64(s.cfg.MaxInFlight) {
		s.inFlight.Add(-1)
		count(&s.refused, "busy")
		return errReply(syscall.EBUSY), false
	}
	defer s.inFlight.Add(-1)
	count(&s.requests, req.Op.String())
	switch req.Op {
	case wire.OpPing:
		// No engine call, no lock, no network: answers while the NAS is gone.
		return jsonReply(wire.PingReply{
			Epoch: s.cfg.Epoch, UptimeMs: time.Since(s.start).Milliseconds(),
			MetaOKAgeMs: ageMs(s.metaOK.Load()), ObjectOKAgeMs: ageMs(s.objOK.Load()),
			InFlight: s.inFlight.Load() - 1,
		}), false
	case wire.OpStats:
		return jsonReply(wire.StatsReply{
			Epoch: s.cfg.Epoch, UptimeMs: time.Since(s.start).Milliseconds(),
			Requests: snapshot(&s.requests), Refused: snapshot(&s.refused),
			Connections: s.conns.Load(), Goroutines: runtime.NumGoroutine(),
			Metrics: s.cfg.Engine.Metrics(),
		}), false
	case wire.OpGetAttr:
		var r wire.GetAttrRequest
		if json.Unmarshal(req.Body, &r) != nil || r.Inode == 0 {
			return errReply(syscall.EINVAL), false
		}
		d := s.cfg.DefaultDeadline
		if req.DeadlineMs > 0 {
			d = time.Duration(req.DeadlineMs) * time.Millisecond
		}
		ctx, cancel := context.WithTimeout(context.Background(), d)
		defer cancel()
		a, st := s.cfg.Engine.GetAttr(ctx, r.Inode)
		if st != 0 {
			if st != syscall.ETIMEDOUT && st != syscall.EINTR && st != syscall.EIO {
				s.NoteMetaOK() // an answer such as ENOENT is a good round trip
			}
			return errReply(st), false
		}
		s.NoteMetaOK()
		return jsonReply(a), false
	case wire.OpShutdown:
		s.shutOnce.Do(func() { close(s.shutdown) })
		return wire.Reply{}, true
	case wire.OpHello:
		return errReply(syscall.EINVAL), false
	}
	return errReply(syscall.ENOSYS), false
}

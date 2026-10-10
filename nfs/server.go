package nfs

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"

	"github.com/lelanddutcher/juicemount/metadata"
)

// Config holds NFS server configuration.
type Config struct {
	ListenAddr string // e.g. "127.0.0.1:11049"
	FUSEPath   string // path to hidden JuiceFS FUSE mount

	// LB-4 (Phase 3b): user-tunable memory-buffer limits, in MB to match
	// the app preferences UI. 0 (or absent in the config JSON) keeps the
	// package defaults (DefaultMemBufBudget / DefaultMemBufThreshold), so
	// configs written by older app builds behave identically.
	MemBufBudgetMB    int // total heap budget for buffered small files
	MemBufFileLimitMB int // only files smaller than this are buffered
	// Nil keeps the legacy environment switch for CLI/direct constructors.
	NativePreviewWarm *bool
}

// Server wraps the go-nfs server with JuiceMount-specific configuration.
type Server struct {
	mu            sync.Mutex
	config        Config
	store         *metadata.Store
	handler       *JuiceMountHandler
	listener      net.Listener
	protocol      *nfslib.Server
	running       atomic.Bool
	startDisabled bool
}

// NewServer creates a new NFS server.
func NewServer(config Config, store *metadata.Store) *Server {
	// Route the go-nfs library's logger into jmlog before anything can log.
	// Without this the library writes to a stderr the app does not have.
	// See nfslog.go.
	installNFSLogBridge()
	s := &Server{
		config: config,
		store:  store,
	}
	// Construct the handler before the listener exists. The bridge must wire
	// Redis, cache, presence, and spool dependencies before a single client can
	// enter it; it also needs to be able to prepare a server while a deferred
	// JuiceFS/FUSE mount is still recovering. NewHandler only initializes local
	// state — it does not touch FUSE until an operation is served.
	s.handler = NewHandler(s.store, s.config.FUSEPath,
		WithNativePreviewWarm(s.config.NativePreviewWarm),
		WithMemBufLimits(
			int64(s.config.MemBufFileLimitMB)<<20,
			int64(s.config.MemBufBudgetMB)<<20,
		))
	return s
}

// Start begins listening and serving NFS requests.
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.startDisabled {
		return errors.New("nfs server: start disabled after shutdown began")
	}
	if err := s.handler.export.validate(); err != nil {
		return fmt.Errorf("nfs server: %w", err)
	}
	if err := s.store.CheckOnlineMutationRecovery(); err != nil {
		return fmt.Errorf("nfs server: %w", err)
	}
	if s.running.Load() {
		return nil
	}
	if err := s.store.CheckSpoolRecovery(); err != nil {
		return fmt.Errorf("nfs server: %w", err)
	}

	// Create a TCP listener with TCP_NODELAY
	l, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.config.ListenAddr, err)
	}
	listener := &noDelayListener{Listener: l}
	protocol := nfslib.NewManagedServer(s.handler)
	s.listener = listener
	s.protocol = protocol
	s.running.Store(true)

	jmlog.Info("nfs server listening",
		"addr", s.config.ListenAddr,
		"tcp_nodelay", true,
	)

	// [JM5] Serve the handler directly — JuiceMountHandler implements
	// deterministic inode-based ToHandle/FromHandle plus VerifierFor/
	// DataForVerifier for READDIRPLUS. The CachingHandler wrapper was
	// adding redundant UUID-based LRU caching with unnecessary mutex
	// contention and memory overhead.

	// Start serving in background
	go func() {
		err := protocol.Serve(listener)
		s.mu.Lock()
		// A stopped server may already have been started again. Only the
		// goroutine that still owns the published listener may clear it.
		if s.listener == listener {
			s.listener = nil
			s.running.Store(false)
		}
		s.mu.Unlock()
		if unexpectedServeError(err) {
			jmlog.Error("nfs server stopped with error", "error", err.Error())
		}
	}()

	return nil
}

func unexpectedServeError(err error) bool {
	return err != nil && !errors.Is(err, net.ErrClosed)
}

// Stop closes the listener and stops the server.
func (s *Server) Stop() error {
	s.mu.Lock()
	listener := s.listener
	s.listener = nil
	s.running.Store(false)
	s.mu.Unlock()
	if listener == nil {
		return nil
	}
	return listener.Close()
}

// DisableRestart prevents FUSE recovery callbacks from reopening the listener
// without closing the current listener needed during the NFS unmount handshake.
func (s *Server) DisableRestart() {
	s.mu.Lock()
	s.startDisabled = true
	s.mu.Unlock()
}

// DisableStart retires this server instance and closes its listener.
func (s *Server) DisableStart() error {
	s.DisableRestart()
	return s.Stop()
}

// QuiesceClients follows a confirmed NFS unmount and listener retirement.
// A timeout leaves the handler's backing stores live for a later retry.
func (s *Server) QuiesceClients(deadline time.Duration) bool {
	s.mu.Lock()
	protocol := s.protocol
	s.mu.Unlock()
	return protocol == nil || protocol.Quiesce(deadline)
}

// IsRunning reports whether the NFS accept loop is active. Server-only
// deployments use this for readiness instead of checking for a client-side
// mount that intentionally does not exist inside the container.
func (s *Server) IsRunning() bool {
	return s.running.Load()
}

// Handler returns the NFS handler for configuration (cache reader, redis client, etc.)
func (s *Server) Handler() *JuiceMountHandler {
	return s.handler
}

// Addr returns the listener address (for use in mount commands).
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return ""
}

// noDelayListener wraps a net.Listener to set TCP_NODELAY on accepted connections.
// This is CRITICAL for NFS performance: without it, Nagle's algorithm coalesces
// small NFS response headers with data, adding up to 40ms delay per batch.
type noDelayListener struct {
	net.Listener
}

func (l *noDelayListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)

		// Set larger socket buffers for throughput
		if raw, err := tcpConn.SyscallConn(); err == nil {
			raw.Control(func(fd uintptr) {
				syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, 4<<20)
				syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4<<20)
			})
		}
	}

	return conn, nil
}

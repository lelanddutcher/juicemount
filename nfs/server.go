package nfs

import (
	"fmt"
	"net"
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
}

// Server wraps the go-nfs server with JuiceMount-specific configuration.
type Server struct {
	config   Config
	store    *metadata.Store
	handler  *JuiceMountHandler
	listener net.Listener
}

// NewServer creates a new NFS server.
func NewServer(config Config, store *metadata.Store) *Server {
	return &Server{
		config: config,
		store:  store,
	}
}

// Start begins listening and serving NFS requests.
func (s *Server) Start() error {
	// MB → bytes; 0 stays 0, which NewMemoryBuffer maps to its defaults.
	s.handler = NewHandler(s.store, s.config.FUSEPath,
		WithMemBufLimits(
			int64(s.config.MemBufFileLimitMB)<<20,
			int64(s.config.MemBufBudgetMB)<<20,
		))

	// Create a TCP listener with TCP_NODELAY
	l, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.config.ListenAddr, err)
	}
	s.listener = &noDelayListener{Listener: l}

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
		if err := nfslib.Serve(s.listener, s.handler); err != nil {
			jmlog.Error("nfs server stopped with error", "error", err.Error())
		}
	}()

	return nil
}

// Stop closes the listener and stops the server.
func (s *Server) Stop() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// Handler returns the NFS handler for configuration (cache reader, redis client, etc.)
func (s *Server) Handler() *JuiceMountHandler {
	return s.handler
}

// Addr returns the listener address (for use in mount commands).
func (s *Server) Addr() string {
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

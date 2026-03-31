package nfs

import (
	"fmt"
	"log"
	"net"
	"syscall"
	"time"

	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"

	"github.com/lelanddutcher/juicemount/metadata"
)

// Config holds NFS server configuration.
type Config struct {
	ListenAddr string // e.g. "127.0.0.1:11049"
	FUSEPath   string // path to hidden JuiceFS FUSE mount
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
	s.handler = NewHandler(s.store, s.config.FUSEPath)

	// Create a TCP listener with TCP_NODELAY
	l, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.config.ListenAddr, err)
	}
	s.listener = &noDelayListener{Listener: l}

	log.Printf("nfs: listening on %s (TCP_NODELAY enabled)", s.config.ListenAddr)

	// [JM5] Serve the handler directly — JuiceMountHandler implements
	// deterministic inode-based ToHandle/FromHandle plus VerifierFor/
	// DataForVerifier for READDIRPLUS. The CachingHandler wrapper was
	// adding redundant UUID-based LRU caching with unnecessary mutex
	// contention and memory overhead.

	// Start serving in background
	go func() {
		if err := nfslib.Serve(s.listener, s.handler); err != nil {
			log.Printf("nfs: server error: %v", err)
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

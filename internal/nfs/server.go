package nfs

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"net"
	"sync/atomic"
	"time"
)

const (
	// DefaultRPCSemaphoreSize limits total in-flight RPCs across all connections.
	DefaultRPCSemaphoreSize = 128
)

// Server is a handle to the listening NFS server.
type Server struct {
	Handler
	ID [8]byte
	context.Context

	// [JM5] Cross-connection RPC semaphore. Limits total in-flight RPCs
	// across all connections to prevent goroutine/fd explosion under heavy
	// concurrent load (e.g., Finder + Premiere + DaVinci simultaneously).
	rpcSem chan struct{}

	// [JM5] Active connection tracking for health monitoring.
	activeConns atomic.Int64
}

// RegisterMessageHandler registers a handler for a specific
// XDR procedure.
func RegisterMessageHandler(protocol uint32, proc uint32, handler HandleFunc) error {
	if registeredHandlers == nil {
		registeredHandlers = make(map[registeredHandlerID]HandleFunc)
	}
	for k := range registeredHandlers {
		if k.protocol == protocol && k.proc == proc {
			return errors.New("already registered")
		}
	}
	id := registeredHandlerID{protocol, proc}
	registeredHandlers[id] = handler
	return nil
}

// HandleFunc represents a handler for a specific protocol message.
type HandleFunc func(ctx context.Context, w *response, userHandler Handler) error

// TODO: store directly as a uint64 for more efficient lookups
type registeredHandlerID struct {
	protocol uint32
	proc     uint32
}

var registeredHandlers map[registeredHandlerID]HandleFunc

// ActiveConnections returns the number of active connection goroutines.
func (s *Server) ActiveConnections() int64 {
	return s.activeConns.Load()
}

// RPCSemaphoreSize returns the configured semaphore capacity.
func (s *Server) RPCSemaphoreSize() int {
	if s.rpcSem == nil {
		return 0
	}
	return cap(s.rpcSem)
}

// Serve listens on the provided listener port for incoming client requests.
func (s *Server) Serve(l net.Listener) error {
	defer l.Close()
	baseCtx := context.Background()
	if s.Context != nil {
		baseCtx = s.Context
	}
	if bytes.Equal(s.ID[:], []byte{0, 0, 0, 0, 0, 0, 0, 0}) {
		if _, err := rand.Reader.Read(s.ID[:]); err != nil {
			return err
		}
	}

	// [JM5] Initialize RPC semaphore if not already set.
	if s.rpcSem == nil {
		s.rpcSem = make(chan struct{}, DefaultRPCSemaphoreSize)
	}

	// [JM6] Hold a macOS power assertion while the NFS mount is in active use
	// so a closed-lid / idle Mac can't idle-sleep and SUSPEND this process
	// mid-copy (which surfaces to Finder as "device disappeared" and aborts an
	// in-flight transfer). Activity-driven; releases after ~2 min of quiet so
	// the Mac can still sleep when the share is unused. See keepawake.go.
	startKeepAwake()

	var tempDelay time.Duration

	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				time.Sleep(tempDelay)
				continue
			}
			return err
		}
		tempDelay = 0
		c := s.newConn(conn)
		go c.serve(baseCtx)
	}
}

func (s *Server) newConn(nc net.Conn) *conn {
	c := &conn{
		Server: s,
		Conn:   nc,
	}
	return c
}

// [JM5] Direct map lookup instead of iterating all registered handlers.
func (s *Server) handlerFor(prog uint32, proc uint32) HandleFunc {
	id := registeredHandlerID{prog, proc}
	if handler, ok := registeredHandlers[id]; ok {
		return handler
	}
	return nil
}

// Serve is a singleton listener paralleling http.Serve
func Serve(l net.Listener, handler Handler) error {
	srv := &Server{Handler: handler}
	return srv.Serve(l)
}

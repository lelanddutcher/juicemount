package nfs

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultRPCSemaphoreSize limits navigation RPCs across all connections.
	DefaultRPCSemaphoreSize = 128

	// DataReadRPCSemaphoreSize preserves the previous data-read concurrency
	// ceiling while keeping slow backend bytes out of navigation admission.
	DataReadRPCSemaphoreSize = 128

	// WriteRPCReserve sizes the SEPARATE WRITE admission pool (writeSem) relative
	// to rpcSem: writeSem cap = DefaultRPCSemaphoreSize - WriteRPCReserve. WRITE
	// RPCs use writeSem ONLY (acquired inside the dispatch goroutine, off the
	// reader path); read/navigation RPCs use rpcSem (acquired in the serve loop),
	// while metadata mutations use their own mutationSem below.
	// Because the two pools are independent, a write parked indefinitely in the
	// spool capacity stall (offline buffer full / slow drain) can never consume a
	// read's rpcSem slot nor block the single per-connection reader — so the mount
	// stays navigable even when every write is stalled. The value bounds
	// concurrent RUNNING writes (96) to keep spool fds/handles in check; parked
	// (not-yet-admitted) writes are cheap goroutines bounded by the client's own
	// outstanding-write window.
	WriteRPCReserve = 32

	// MutationRPCSemaphoreSize bounds metadata-mutating RPCs independently of
	// reads and writes. Finder can issue a large burst of REMOVE/SETATTR/COMMIT
	// calls while it is ingesting AppleDouble metadata. Some of those calls must
	// wait on JuiceFS/FUSE, but they must not occupy every rpcSem slot and
	// head-of-line-block a following READDIR/GETATTR on the mount's single TCP
	// connection. The separate pool keeps that work bounded without coupling its
	// latency to navigation.
	MutationRPCSemaphoreSize = 32
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

	// Data READ acquires nonblocking in the connection reader. A full pool
	// returns JUKEBOX without a waiting goroutine or a metadata admission slot.
	dataReadSem chan struct{}

	// [JM6] WRITE-only admission semaphore (cap DefaultRPCSemaphoreSize -
	// WriteRPCReserve). A WRITE acquires this INSIDE its dispatch goroutine
	// (off the per-connection reader path) and NEVER touches rpcSem — the two
	// pools are independent. So a write parked in the spool capacity stall
	// (offline buffer full / slow drain) holds only a writeSem slot, the reader
	// never blocks on it, and reads/metadata keep their full rpcSem pool: the
	// mount stays navigable even when every write is stalled. See conn.go.
	writeSem chan struct{}

	// [JM6] Metadata-mutation admission semaphore. Mutating RPCs acquire this
	// inside their dispatch goroutine, so a slow burst of REMOVE/SETATTR/COMMIT
	// cannot fill rpcSem or park the per-connection reader ahead of READDIR.
	// This is intentionally independent of writeSem: a capacity-stalled WRITE
	// must not prevent REMOVE from cancelling/freeing that path.
	mutationSem chan struct{}

	// [JM5] Active connection tracking for health monitoring.
	activeConns   atomic.Int64
	lifecycleMu   sync.Mutex
	connections   map[net.Conn]struct{}
	activeReaders int
	activeRPCs    int
	quiescing     bool
	quiesced      chan struct{}
	serveDone     chan struct{}
}

// NewManagedServer enables bounded quiescence for owners that retire the
// listener before closing the handler's backing stores.
func NewManagedServer(handler Handler) *Server {
	return &Server{Handler: handler, serveDone: make(chan struct{}), quiesced: make(chan struct{})}
}

func (s *Server) readerStarted(conn net.Conn) {
	s.lifecycleMu.Lock()
	if s.connections == nil {
		s.connections = make(map[net.Conn]struct{})
	}
	s.connections[conn] = struct{}{}
	s.activeReaders++
	s.lifecycleMu.Unlock()
}

func (s *Server) readerDone(conn net.Conn) {
	s.lifecycleMu.Lock()
	delete(s.connections, conn)
	s.activeReaders--
	s.signalQuiescedLocked()
	s.lifecycleMu.Unlock()
}

func (s *Server) rpcStarted() {
	s.lifecycleMu.Lock()
	s.activeRPCs++
	s.lifecycleMu.Unlock()
}

func (s *Server) rpcDone() {
	s.lifecycleMu.Lock()
	s.activeRPCs--
	s.signalQuiescedLocked()
	s.lifecycleMu.Unlock()
}

func (s *Server) signalQuiescedLocked() {
	if s.quiescing && s.activeReaders == 0 && s.activeRPCs == 0 && s.quiesced != nil {
		close(s.quiesced)
		s.quiesced = nil
	}
}

// Quiesce must follow listener.Close. Waiting for Serve to exit ensures no
// accepted reader can be registered after the connection snapshot. A reader
// registers each RPC before it exits, so zero readers and RPCs is final.
func (s *Server) Quiesce(deadline time.Duration) bool {
	if s.serveDone == nil {
		return false
	}
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	select {
	case <-s.serveDone:
	case <-timer.C:
		return false
	}
	s.lifecycleMu.Lock()
	conns := make([]net.Conn, 0, len(s.connections))
	for conn := range s.connections {
		conns = append(conns, conn)
	}
	s.quiescing = true
	s.signalQuiescedLocked()
	done := s.quiesced
	s.lifecycleMu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	if done == nil {
		return true
	}
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
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
	if s.serveDone != nil {
		defer close(s.serveDone)
	}
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
	if s.dataReadSem == nil {
		s.dataReadSem = make(chan struct{}, DataReadRPCSemaphoreSize)
	}
	// [JM6] WRITE admission semaphore — independent from navigation RPCs.
	if s.writeSem == nil {
		n := DefaultRPCSemaphoreSize - WriteRPCReserve
		if n < 1 {
			n = 1
		}
		s.writeSem = make(chan struct{}, n)
	}
	if s.mutationSem == nil {
		s.mutationSem = make(chan struct{}, MutationRPCSemaphoreSize)
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
		s.readerStarted(conn)
		go func() {
			defer s.readerDone(conn)
			c.serve(baseCtx)
		}()
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

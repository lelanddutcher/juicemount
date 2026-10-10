//go:build latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// LatencyTransport is bounded diagnostic instrumentation for one dedicated
// fixture. It records no credentials or file contents. ReadNS is the return of
// the socket Read that completed a frame, not packet arrival; WriteNS is a
// successful socket Write, not client consumption. Times are monotonic offsets.
type LatencyTransport struct {
	mu      sync.Mutex
	start   time.Time
	limit   int
	serial  uint64
	err     string
	rows    []LatencyTransportRecord
	pending map[latencyTransportKey]int
}

type LatencyTransportRecord struct {
	Connection uint64 `json:"connection"`
	XID        uint32 `json:"xid"`
	Program    uint32 `json:"program"`
	Procedure  uint32 `json:"procedure"`
	ReadNS     int64  `json:"read_ns"`
	HandlerNS  int64  `json:"handler_ns"`
	HandledNS  int64  `json:"handled_ns"`
	WriteNS    int64  `json:"write_ns"`
	Handle     string `json:"handle,omitempty"`
	NFSOK      bool   `json:"nfs_ok"`
}

type LatencyTransportStatus struct {
	StartWallNS int64  `json:"start_wall_ns"`
	NowNS       int64  `json:"now_ns"`
	WallNS      int64  `json:"wall_ns"`
	Requests    int    `json:"requests"`
	Pending     int    `json:"pending"`
	Error       string `json:"error,omitempty"`
}

type latencyTransportKey struct {
	connection uint64
	xid        uint32
}

func NewLatencyTransport(limit int) (*LatencyTransport, error) {
	if limit < 1 || limit > 65536 {
		return nil, errors.New("invalid latency trace record bound")
	}
	return &LatencyTransport{start: time.Now(), limit: limit,
		rows: make([]LatencyTransportRecord, 0, limit), pending: make(map[latencyTransportKey]int)}, nil
}

func (t *LatencyTransport) statusLocked() LatencyTransportStatus {
	now := time.Now()
	return LatencyTransportStatus{t.start.UnixNano(), now.Sub(t.start).Nanoseconds(), now.UnixNano(), len(t.rows), len(t.pending), t.err}
}

func (t *LatencyTransport) Status() LatencyTransportStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.statusLocked()
}

// WriteJSON snapshots outside measured probes, normally after owned cleanup.
// Error and incomplete records must be retained and invalidate attribution.
func (t *LatencyTransport) WriteJSON(w io.Writer) error {
	t.mu.Lock()
	snapshot := struct {
		LatencyTransportStatus
		Records []LatencyTransportRecord `json:"records"`
	}{t.statusLocked(), append([]LatencyTransportRecord(nil), t.rows...)}
	t.mu.Unlock()
	return json.NewEncoder(w).Encode(snapshot)
}

func (t *LatencyTransport) fail(message string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err == "" {
		t.err = message
	}
}

// WrapListener preserves the raw TCP socket's NODELAY setting before hiding
// its concrete type. Install handler observation before starting the server.
func (t *LatencyTransport) WrapListener(listener net.Listener) net.Listener {
	return &latencyTransportListener{Listener: listener, owner: t}
}

type latencyTransportListener struct {
	net.Listener
	owner *LatencyTransport
}

func (l *latencyTransportListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		_ = conn.Close()
		return nil, errors.New("latency trace requires TCP connection")
	}
	if err := tcp.SetNoDelay(true); err != nil {
		_ = conn.Close()
		return nil, err
	}
	l.owner.mu.Lock()
	l.owner.serial++
	id := l.owner.serial
	l.owner.mu.Unlock()
	return &latencyTransportConn{Conn: conn, owner: l.owner, id: id}, nil
}

type latencyTransportConn struct {
	net.Conn
	owner           *LatencyTransport
	id              uint64
	readMu, writeMu sync.Mutex
	reads, writes   latencyRecordStream
}

func (c *latencyTransportConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	n, err := c.Conn.Read(p)
	at := time.Since(c.owner.start).Nanoseconds()
	if n > 0 {
		c.reads.feed(p[:n], func(frame []byte) { c.owner.request(c.id, frame, at) }, c.owner.fail)
	}
	if err != nil && len(c.reads.buffer) != 0 {
		c.owner.fail("socket read ended inside a traced frame")
	}
	return n, err
}

func (c *latencyTransportConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	n, err := c.Conn.Write(p)
	at := time.Since(c.owner.start).Nanoseconds()
	if err != nil || n != len(p) {
		c.owner.fail("socket response write failed or was short")
	}
	if n > 0 {
		c.writes.feed(p[:n], func(frame []byte) { c.owner.reply(c.id, frame, at, err == nil) }, c.owner.fail)
	}
	return n, err
}

// The server already refuses fragmented and >2MiB RPC records. Match that
// boundary while accepting arbitrary socket chunking and coalesced records.
type latencyRecordStream struct {
	buffer []byte
	failed bool
}

func (s *latencyRecordStream) feed(data []byte, record func([]byte), fail func(string)) {
	if s.failed {
		return
	}
	if len(s.buffer)+len(data) > 4<<20 {
		s.failed = true
		fail("trace stream buffer bound exceeded")
		return
	}
	s.buffer = append(s.buffer, data...)
	for len(s.buffer) >= 4 {
		marker := binary.BigEndian.Uint32(s.buffer)
		size := int(marker & 0x7fffffff)
		if marker&0x80000000 == 0 || size < 4 || size > 2<<20 {
			s.failed = true
			fail("trace encountered unsupported RPC record")
			return
		}
		if len(s.buffer) < size+4 {
			return
		}
		record(s.buffer[4 : size+4])
		s.buffer = s.buffer[size+4:]
	}
	if len(s.buffer) == 0 {
		s.buffer = s.buffer[:0]
	}
}

func (t *LatencyTransport) request(connection uint64, frame []byte, at int64) {
	if len(frame) < 40 || binary.BigEndian.Uint32(frame[4:]) != 0 {
		t.fail("trace request header mismatch")
		return
	}
	key := latencyTransportKey{connection, binary.BigEndian.Uint32(frame)}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.pending[key]; exists || len(t.rows) == t.limit {
		if t.err == "" {
			t.err = "duplicate pending XID or trace record bound exceeded"
		}
		return
	}
	t.pending[key] = len(t.rows)
	t.rows = append(t.rows, LatencyTransportRecord{Connection: connection, XID: key.xid,
		Program: binary.BigEndian.Uint32(frame[12:]), Procedure: binary.BigEndian.Uint32(frame[20:]), ReadNS: at})
}

func (t *LatencyTransport) reply(connection uint64, frame []byte, at int64, successfulWrite bool) {
	if len(frame) < 12 || binary.BigEndian.Uint32(frame[4:]) != 1 {
		t.fail("trace response header mismatch")
		return
	}
	key := latencyTransportKey{connection, binary.BigEndian.Uint32(frame)}
	t.mu.Lock()
	defer t.mu.Unlock()
	index, found := t.pending[key]
	if !found {
		if t.err == "" {
			t.err = "trace response has no pending connection/XID"
		}
		return
	}
	if !successfulWrite {
		return
	}
	t.rows[index].WriteNS = at
	t.rows[index].NFSOK = latencyReplyOK(frame)
	delete(t.pending, key)
}

// ObserveHandlers instruments only this trace's accepted connections. Registry
// changes follow the same before-Serve / after-Quiesce rule as reply observation.
func (t *LatencyTransport) ObserveHandlers() func() {
	originals := make(map[registeredHandlerID]HandleFunc)
	for _, proc := range []uint32{1, 3, 6, 16, 17} {
		id := registeredHandlerID{protocol: 100003, proc: proc}
		if handler := registeredHandlers[id]; handler != nil {
			originals[id] = handler
			registeredHandlers[id] = t.observeHandler(handler)
		}
	}
	return func() {
		for id, handler := range originals {
			registeredHandlers[id] = handler
		}
	}
}

func (t *LatencyTransport) observeHandler(handler HandleFunc) HandleFunc {
	return func(ctx context.Context, w *response, user Handler) error {
		conn, ok := w.conn.Conn.(*latencyTransportConn)
		if !ok || conn.owner != t {
			return handler(ctx, w, user)
		}
		key := latencyTransportKey{conn.id, w.req.xid}
		t.mu.Lock()
		index, found := t.pending[key]
		if found {
			t.rows[index].HandlerNS = time.Since(t.start).Nanoseconds()
			if body, ok := w.req.Body.(*bytes.Reader); ok {
				copy := *body
				if size, err := xdr.ReadUint32(&copy); err == nil && size <= 128 {
					handle := make([]byte, size)
					if _, err := io.ReadFull(&copy, handle); err == nil {
						t.rows[index].Handle = hex.EncodeToString(handle)
					}
				}
			}
		} else if t.err == "" {
			t.err = "handler has no traced request"
		}
		t.mu.Unlock()
		defer func() {
			t.mu.Lock()
			if found {
				t.rows[index].HandledNS = time.Since(t.start).Nanoseconds()
			}
			t.mu.Unlock()
		}()
		return handler(ctx, w, user)
	}
}

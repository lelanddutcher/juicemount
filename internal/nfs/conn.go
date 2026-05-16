package nfs

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	xdr2 "github.com/rasky/go-xdr/xdr2"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// [JM5] Buffer pool to avoid allocations on the RPC hot path.
// Each NFS RPC allocates a bytes.Buffer for the response; pooling
// eliminates ~1 alloc + GC pressure per RPC.
var responseBufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

func getResponseBuffer() *bytes.Buffer {
	buf := responseBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func putResponseBuffer(buf *bytes.Buffer) {
	if buf.Cap() > 65536 {
		// Don't pool oversized buffers (e.g. from large READ responses)
		return
	}
	responseBufferPool.Put(buf)
}

var (
	// ErrInputInvalid is returned when input cannot be parsed
	ErrInputInvalid = errors.New("invalid input")
	// ErrAlreadySent is returned when writing a header/status multiple times
	ErrAlreadySent = errors.New("response already started")

	// [JM5] RPC performance counters
	rpcCount     atomic.Int64
	slowRPCCount atomic.Int64

	// [JM5] Write coalescing metrics
	tcpFlushCount   atomic.Int64 // number of TCP flush syscalls
	tcpBatchedCount atomic.Int64 // number of responses batched (>1 per flush)

	// [JM5] Optional observer hook for the metrics package. Kept as an
	// atomic.Value of an ObserverFunc so the package can be imported by
	// internal/metrics without a circular dependency.
	observer atomic.Value // ObserverFunc
)

// ObserverFunc is invoked once per RPC after the handler returns.
// proc is the NFS procedure number (0..21 for NFSv3); program is the
// RPC program ID (100003 for nfs, 100005 for mount). Implementations
// must be non-blocking and goroutine-safe.
type ObserverFunc func(program uint32, proc uint32, elapsed time.Duration, err error)

// SetObserver registers a callback for per-RPC timing. Pass nil to
// disable. The previous observer (if any) is replaced.
func SetObserver(fn ObserverFunc) {
	if fn == nil {
		observer.Store(ObserverFunc(nil))
		return
	}
	observer.Store(fn)
}

func currentObserver() ObserverFunc {
	v := observer.Load()
	if v == nil {
		return nil
	}
	if fn, ok := v.(ObserverFunc); ok {
		return fn
	}
	return nil
}

// RPCStats returns the current RPC performance counters.
func RPCStats() (total, slow, flushes, batched int64) {
	return rpcCount.Load(), slowRPCCount.Load(), tcpFlushCount.Load(), tcpBatchedCount.Load()
}

// ResponseCode is a combination of accept_stat and reject_stat.
type ResponseCode uint32

// ResponseCode Codes
const (
	ResponseCodeSuccess ResponseCode = iota
	ResponseCodeProgUnavailable
	ResponseCodeProcUnavailable
	ResponseCodeGarbageArgs
	ResponseCodeSystemErr
	ResponseCodeRPCMismatch
	ResponseCodeAuthError
)

type conn struct {
	*Server
	writeSerializer chan []byte
	net.Conn
}

func (c *conn) serve(ctx context.Context) {
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// [JM5] Track active connections for cancel isolation hardening.
	// When this goroutine exits (client disconnect, error, EOF), the
	// deferred cleanup logs the event and decrements the counter so
	// ActiveConnections() accurately reflects live connections.
	remoteAddr := c.Conn.RemoteAddr().String()
	c.Server.activeConns.Add(1)
	defer func() {
		c.Server.activeConns.Add(-1)
		Log.Infof("[conn] connection closed: %s (active: %d)", remoteAddr, c.Server.activeConns.Load())
	}()

	// [JM5] Enable TCP_NODELAY to reduce per-RPC latency. Our buffered
	// writer (serializeWrites) handles coalescing, so Nagle's algorithm
	// just adds ~40ms delay on small responses like GETATTR.
	if tc, ok := c.Conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}

	// [JM5] Increased buffer from 1 to 64 for response batching.
	c.writeSerializer = make(chan []byte, 64)
	go c.serializeWrites(connCtx)

	bio := bufio.NewReader(c.Conn)
	for {
		w, err := c.readRequestHeader(connCtx, bio)
		if err != nil {
			if err == io.EOF {
				c.Close()
				return
			}
			return
		}
		Log.Tracef("request: %v", w.req)

		// [JM6-concurrent] Acquire the cross-connection RPC semaphore
		// in the serve loop (before dispatching). This back-pressures
		// the read loop: if the global slot pool is exhausted we wait
		// here rather than spawning unbounded goroutines. The select
		// against connCtx.Done() lets us exit cleanly if the client
		// disconnects while we're blocked on a full pool.
		if c.Server.rpcSem != nil {
			select {
			case c.Server.rpcSem <- struct{}{}:
			case <-connCtx.Done():
				return
			}
		}

		// [JM6-concurrent] Dispatch the handler in its own goroutine.
		// Each goroutine reads from the request's bytes.Reader (set up
		// by readRequestHeader), writes through the shared
		// writeSerializer channel (safe for concurrent senders — Go
		// channels are goroutine-safe), and releases its semaphore
		// slot on completion. macOS NFS uses a single TCP connection
		// per mount, so without this dispatch every slow Read would
		// freeze every subsequent Lookup/Getattr on that connection —
		// exactly the Finder-freeze symptom the overnight audit
		// identified as the dominant architectural issue.
		//
		// Response ordering: NFS RPCs over TCP carry their own XID;
		// clients demux by XID, not by arrival order. Concurrent
		// dispatch is safe at the protocol level.
		//
		// On respErr (write failure) we close the connection from
		// here. The serve loop's next readRequestHeader will then
		// return an error and we exit cleanly. net.Conn.Close is
		// idempotent so multiple goroutines closing is safe.
		go func(w *response) {
			defer func() {
				// [JM6-concurrent] Recover from handler panics so a
				// nil-deref or unanticipated XDR state in a single
				// RPC doesn't crash the whole mount daemon. With
				// sequential dispatch this was less of a concern —
				// one bad handler call took down a long-frozen loop.
				// With concurrent dispatch, any in-flight goroutine
				// panicking would terminate the process AND every
				// other in-flight RPC with it. Close the connection
				// on panic so the client retries on a fresh socket.
				if r := recover(); r != nil {
					Log.Errorf("handler panic: %v", r)
					c.Close()
				}
				if c.Server.rpcSem != nil {
					<-c.Server.rpcSem
				}
			}()

			start := time.Now()
			err := c.handle(connCtx, w)
			elapsed := time.Since(start)
			respErr := w.finish(connCtx)

			rpcCount.Add(1)
			if elapsed > 5*time.Millisecond {
				slowRPCCount.Add(1)
				if elapsed > 50*time.Millisecond {
					Log.Warnf("slow RPC: %v took %v", w.req, elapsed)
				}
			}

			if obs := currentObserver(); obs != nil {
				obs(w.req.Header.Prog, w.req.Header.Proc, elapsed, err)
			}

			if err != nil {
				Log.Errorf("error handling req: %v", err)
			}
			if respErr != nil {
				Log.Errorf("error sending response: %v", respErr)
				c.Close()
			}
		}(w)
	}
}

func (c *conn) serializeWrites(ctx context.Context) {
	// [JM5] 1MB write buffer matches NFS rsize for optimal TCP coalescing.
	writer := bufio.NewWriterSize(c.Conn, 1<<20)
	var fragmentBuf [4]byte

	writeMsg := func(msg []byte) error {
		fragmentInt := uint32(len(msg)) | (1 << 31)
		binary.BigEndian.PutUint32(fragmentBuf[:], fragmentInt)
		if _, err := writer.Write(fragmentBuf[:]); err != nil {
			return err
		}
		if _, err := writer.Write(msg); err != nil {
			return err
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-c.writeSerializer:
			if !ok {
				return
			}
			if err := writeMsg(msg); err != nil {
				return
			}
			// [JM5] Batch-drain: collect all queued responses before flushing.
			// This prevents TCP send buffer deadlock under concurrent load
			// (e.g., Finder copying 20 files simultaneously) and reduces
			// the number of TCP write syscalls.
			batchCount := int64(1)
		drain:
			for {
				select {
				case msg, ok = <-c.writeSerializer:
					if !ok {
						writer.Flush()
						return
					}
					if err := writeMsg(msg); err != nil {
						return
					}
					batchCount++
				default:
					break drain
				}
			}
			// [JM5] Track coalescing metrics
			tcpFlushCount.Add(1)
			if batchCount > 1 {
				tcpBatchedCount.Add(batchCount - 1)
			}
			if err := writer.Flush(); err != nil {
				return
			}
		}
	}
}

// Handle a request. errors from this method indicate a failure to read or
// write on the network stream, and trigger a disconnection of the connection.
func (c *conn) handle(ctx context.Context, w *response) error {
	handler := c.Server.handlerFor(w.req.Header.Prog, w.req.Header.Proc)
	if handler == nil {
		Log.Errorf("No handler for %d.%d", w.req.Header.Prog, w.req.Header.Proc)
		if err := w.drain(ctx); err != nil {
			return err
		}
		return c.err(ctx, w, &ResponseCodeProcUnavailableError{})
	}
	appError := handler(ctx, w, c.Server.Handler)
	if drainErr := w.drain(ctx); drainErr != nil {
		return drainErr
	}
	if appError != nil && !w.responded {
		if err := c.err(ctx, w, appError); err != nil {
			return err
		}
	}
	if !w.responded {
		Log.Errorf("Handler did not indicate response status via writing or erroring")
		if err := c.err(ctx, w, &ResponseCodeSystemError{}); err != nil {
			return err
		}
	}
	return nil
}

func (c *conn) err(ctx context.Context, w *response, err error) error {
	select {
	case <-ctx.Done():
		return nil
	default:
	}

	if w.err == nil {
		w.err = err
	}

	if w.responded {
		return nil
	}

	rpcErr := w.errorFmt(err)
	if writeErr := w.writeHeader(rpcErr.Code()); writeErr != nil {
		return writeErr
	}

	body, _ := rpcErr.MarshalBinary()
	return w.Write(body)
}

type request struct {
	xid uint32
	rpc.Header
	Body io.Reader
}

func (r *request) String() string {
	if r.Header.Prog == nfsServiceID {
		return fmt.Sprintf("RPC #%d (nfs.%s)", r.xid, NFSProcedure(r.Header.Proc))
	} else if r.Header.Prog == mountServiceID {
		return fmt.Sprintf("RPC #%d (mount.%s)", r.xid, MountProcedure(r.Header.Proc))
	}
	return fmt.Sprintf("RPC #%d (%d.%d)", r.xid, r.Header.Prog, r.Header.Proc)
}

type response struct {
	*conn
	writer    *bytes.Buffer
	responded bool
	err       error
	errorFmt  func(error) RPCError
	req       *request
}

func (w *response) writeXdrHeader() error {
	err := xdr.Write(w.writer, &w.req.xid)
	if err != nil {
		return err
	}
	respType := uint32(1)
	err = xdr.Write(w.writer, &respType)
	if err != nil {
		return err
	}
	return nil
}

func (w *response) writeHeader(code ResponseCode) error {
	if w.responded {
		return ErrAlreadySent
	}
	w.responded = true
	if err := w.writeXdrHeader(); err != nil {
		return err
	}

	status := rpc.MsgAccepted
	if code == ResponseCodeAuthError || code == ResponseCodeRPCMismatch {
		status = rpc.MsgDenied
	}

	err := xdr.Write(w.writer, &status)
	if err != nil {
		return err
	}

	if status == rpc.MsgAccepted {
		// Write opaque_auth header.
		err = xdr.Write(w.writer, &rpc.AuthNull)
		if err != nil {
			return err
		}
	}

	return xdr.Write(w.writer, &code)
}

// Write a response to an xdr message
func (w *response) Write(dat []byte) error {
	if !w.responded {
		if err := w.writeHeader(ResponseCodeSuccess); err != nil {
			return err
		}
	}

	acc := 0
	for acc < len(dat) {
		n, err := w.writer.Write(dat[acc:])
		if err != nil {
			return err
		}
		acc += n
	}
	return nil
}

// drain reads the rest of the request frame if not consumed by the handler.
func (w *response) drain(ctx context.Context) error {
	// [JM6-concurrent] The request body is now buffered in a
	// *bytes.Reader by readRequestHeader, so there's nothing to drain
	// from the network — the handler either consumed the in-memory
	// bytes or didn't, but the bufio.Reader on the connection is
	// already positioned at the start of the next request frame.
	if _, ok := w.req.Body.(*bytes.Reader); ok {
		return nil
	}
	// Legacy path: some callers (older tests, non-conn paths) may
	// still construct requests with a LimitedReader. Keep the drain
	// logic working for them.
	if reader, ok := w.req.Body.(*io.LimitedReader); ok {
		if reader.N == 0 {
			return nil
		}
		_, err := io.CopyN(io.Discard, w.req.Body, reader.N)
		if err == nil || err == io.EOF {
			return nil
		}
		return err
	}
	return io.ErrUnexpectedEOF
}

func (w *response) finish(ctx context.Context) error {
	// Copy bytes before returning buffer to pool, since the
	// writeSerializer consumer reads asynchronously.
	data := make([]byte, w.writer.Len())
	copy(data, w.writer.Bytes())
	// [JM6-concurrent] Use the capacity-guarded helper so a large READ
	// response (up to ~16 MiB) doesn't pollute the pool with an
	// oversized buffer that future small RPCs would inherit.
	putResponseBuffer(w.writer)

	select {
	case w.conn.writeSerializer <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *conn) readRequestHeader(ctx context.Context, reader *bufio.Reader) (w *response, err error) {
	fragment, err := xdr.ReadUint32(reader)
	if err != nil {
		if xdrErr, ok := err.(*xdr2.UnmarshalError); ok {
			if xdrErr.Err == io.EOF {
				return nil, io.EOF
			}
		}
		return nil, err
	}
	if fragment&(1<<31) == 0 {
		Log.Warnf("Warning: haven't implemented fragment reconstruction.\n")
		return nil, ErrInputInvalid
	}
	reqLen := fragment - uint32(1<<31)
	if reqLen < 40 {
		return nil, ErrInputInvalid
	}
	// [JM6-concurrent] Upper bound on frame size. NFSv3 max payload is
	// 1 MiB (RFC 1813); 2 MiB ceiling leaves generous slack for RPC
	// framing while preventing a malformed-fragment-header DoS from
	// allocating multi-GB buffers on the LAN. A real macOS client
	// will never send a frame this large; if we see one, the wire is
	// corrupt or hostile.
	const maxRPCFrameSize = 2 << 20
	if reqLen > maxRPCFrameSize {
		return nil, ErrInputInvalid
	}

	// [JM6-concurrent] Buffer the entire RPC frame so the bufio.Reader
	// advances past this request before the serve loop dispatches it.
	// This is the precondition for concurrent per-connection dispatch:
	// the read loop owns the wire, each handler goroutine reads from
	// its own in-memory bytes.Reader. Without this, handlers would
	// race the next iteration's read on the shared bufio.Reader.
	//
	// Frame size is bounded by the NFS protocol: NFSv3 max write is
	// 1 MiB plus RPC framing overhead, so reqLen here is at most ~1 MB
	// in practice. The allocation cost is acceptable for the
	// throughput win.
	frameBuf := make([]byte, reqLen)
	if _, err := io.ReadFull(reader, frameBuf); err != nil {
		return nil, err
	}
	frameReader := bytes.NewReader(frameBuf)

	xid, err := xdr.ReadUint32(frameReader)
	if err != nil {
		return nil, err
	}
	reqType, err := xdr.ReadUint32(frameReader)
	if err != nil {
		return nil, err
	}
	if reqType != 0 { // 0 = request, 1 = response
		return nil, ErrInputInvalid
	}

	req := request{
		xid,
		rpc.Header{},
		frameReader,
	}
	if err = xdr.Read(frameReader, &req.Header); err != nil {
		return nil, err
	}

	// [JM5] Use pooled buffer to avoid allocation per RPC.
	buf := responseBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	w = &response{
		conn:     c,
		req:      &req,
		errorFmt: basicErrorFormatter,
		writer:   buf,
	}
	return w, nil
}

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
)

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

		// [JM5] Acquire cross-connection RPC semaphore.
		// Limits total in-flight RPCs to prevent resource exhaustion
		// under heavy concurrent load (Finder + Premiere + DaVinci).
		if c.Server.rpcSem != nil {
			c.Server.rpcSem <- struct{}{}
		}

		// [JM5] Sequential dispatch with cancel isolation.
		start := time.Now()
		err = c.handle(connCtx, w)
		elapsed := time.Since(start)
		respErr := w.finish(connCtx)

		// Release semaphore after RPC completes.
		if c.Server.rpcSem != nil {
			<-c.Server.rpcSem
		}

		rpcCount.Add(1)
		if elapsed > 5*time.Millisecond {
			slowRPCCount.Add(1)
			if elapsed > 50*time.Millisecond {
				Log.Warnf("slow RPC: %v took %v", w.req, elapsed)
			}
		}

		if err != nil {
			Log.Errorf("error handling req: %v", err)
		}
		if respErr != nil {
			Log.Errorf("error sending response: %v", respErr)
			c.Close()
			return
		}
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
	if reader, ok := w.req.Body.(*io.LimitedReader); ok {
		if reader.N == 0 {
			return nil
		}
		// todo: wrap body in a context reader.
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
	responseBufferPool.Put(w.writer)

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

	r := io.LimitedReader{R: reader, N: int64(reqLen)}

	xid, err := xdr.ReadUint32(&r)
	if err != nil {
		return nil, err
	}
	reqType, err := xdr.ReadUint32(&r)
	if err != nil {
		return nil, err
	}
	if reqType != 0 { // 0 = request, 1 = response
		return nil, ErrInputInvalid
	}

	req := request{
		xid,
		rpc.Header{},
		&r,
	}
	if err = xdr.Read(&r, &req.Header); err != nil {
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

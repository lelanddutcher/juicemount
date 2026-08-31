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

	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// [JM5] Buffer pool to avoid allocations on the RPC hot path.
// Each NFS RPC allocates a bytes.Buffer for the response; pooling
// eliminates ~1 alloc + GC pressure per RPC.
var responseBufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

// largeResponseBufferPool serves READ replies, which are up to the client's
// rsize (1 MiB with our mount options).
//
// WHY A SECOND POOL (2026-08-19, measured). The small pool hands out 4 KiB
// buffers and putResponseBuffer refused to pool anything above 64 KiB — so the
// ONE RPC that is reliably large, READ, could never benefit from either. Every
// read reply grew 4K -> 8K -> ... -> 1M (about eight realloc-and-copy steps),
// then finish() copied the whole thing once more, and the grown buffer was
// discarded so the next read repeated all of it.
//
// A CPU profile under sustained reads was ~50% garbage collection, and an
// allocation profile over 15 s of reads showed 50 GB allocated:
// bytes.growSlice 24.3 GB (48.5%), response.finish 12.1 GB (24.3%),
// xdr encodeFixedArray 12.1 GB (24.2%). Serving 706 MB/s cost ~3.3 GB/s of
// garbage.
//
// The 64 KiB cap was right in spirit — a 1 MiB buffer inherited by a small
// GETATTR reply is waste — so the fix is a SECOND pool rather than a bigger
// cap: small RPCs keep small buffers, READs get one already big enough.
const largeResponseBufferCap = 1 << 20

var largeResponseBufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, largeResponseBufferCap+4096))
	},
}

// getResponseBufferFor picks a pool by RPC. Only NFS READ is reliably large;
// everything else stays on the small pool.
func getResponseBufferFor(prog, proc uint32) *bytes.Buffer {
	var buf *bytes.Buffer
	if prog == nfsServiceID && proc == uint32(NFSProcedureRead) {
		buf = largeResponseBufferPool.Get().(*bytes.Buffer)
	} else {
		buf = responseBufferPool.Get().(*bytes.Buffer)
	}
	buf.Reset()
	return buf
}

func getResponseBuffer() *bytes.Buffer {
	buf := responseBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// putResponseBuffer returns a buffer to the pool that matches its CAPACITY, not
// the RPC it served — a small RPC that happened to take a large buffer must not
// demote it back to the small pool, or the next READ pays the growth again.
func putResponseBuffer(buf *bytes.Buffer) {
	c := buf.Cap()
	switch {
	case c <= 65536:
		responseBufferPool.Put(buf)
	case c <= 4*largeResponseBufferCap:
		largeResponseBufferPool.Put(buf)
	default:
		// Genuinely oversized (a jumbo reply): let it go rather than pin
		// multiple megabytes per pool slot indefinitely.
	}
}

var (
	// ErrInputInvalid is returned when input cannot be parsed
	ErrInputInvalid = errors.New("invalid input")
	// ErrAlreadySent is returned when writing a header/status multiple times
	ErrAlreadySent = errors.New("response already started")

	// [JM5] RPC performance counters
	rpcCount     atomic.Int64
	slowRPCCount atomic.Int64

	// [JM6] Data-transfer activity counter — bumped only by READ/WRITE
	// handlers that actually move file bytes, NOT by metadata/liveness
	// RPCs (GETATTR/FSSTAT/LOOKUP/READDIR). The macOS NFS client emits a
	// steady low-rate trickle of those liveness RPCs even on a totally
	// idle mount, so rpcCount never goes flat — keying the keep-awake
	// power assertion off rpcCount would pin the Mac awake forever while
	// the mount is up (battery drain). This counter only advances during
	// an actual copy/read-back, which is exactly when we must not sleep.
	dataXferCount atomic.Int64

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

// DataXferActivity returns a monotonic counter of keep-awake-worthy activity:
// it advances when the READ/WRITE handlers move file bytes AND when the spool
// drains a file to the backend (via NoteDrainProgress). Used by the keep-awake
// loop to hold a macOS power assertion during an active copy/read-back OR a
// post-copy drain tail, while still letting a truly idle mount (metadata/
// liveness chatter only) release it and sleep. See the dataXferCount
// declaration for why rpcCount is unsuitable here.
func DataXferActivity() int64 {
	return dataXferCount.Load()
}

// NoteDrainProgress records that the spool freed bytes (a file drained to the
// backend), counting as keep-awake-worthy activity so the Mac stays awake to
// FINISH a post-copy / post-reconnect drain tail before idle-sleeping — the
// "reconnect, close the lid, walk away and it still uploads" case. The drainer
// is paused while offline, so this only fires online; if the drain wedges
// (stops making progress) the counter goes flat and the assertion releases on
// the normal idle timer, bounding the battery cost. Called from the spool's
// releaseCapacity (package nfs) across the internal/nfs boundary.
func NoteDrainProgress() {
	dataXferCount.Add(1)
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
			// Malformed/oversized frame: without Close the socket and the
			// serializeWrites goroutine leak per bad frame (a buggy client
			// could hold fds hostage). Mirror the EOF path.
			c.Close()
			return
		}
		Log.Tracef("request: %v", w.req)

		// [JM6] Admission control. A WRITE RPC can park INDEFINITELY in the spool
		// capacity stall (offline buffer full / slow drain). Two hard rules keep
		// that stall from wedging the single per-connection reader:
		//   1. The reader (this loop) must NOT block on a write-only gate, or one
		//      over-cap write head-of-line-blocks every following read/LOOKUP/
		//      GETATTR on the same TCP mount — the whole mount goes unnavigable.
		//   2. A parked write must NOT hold an rpcSem slot that reads need.
		// So: read/navigation RPCs acquire the shared rpcSem HERE (reader
		// back-pressure, bounds read goroutines). WRITE and metadata-mutation RPCs
		// are dispatched immediately and acquire their independent semaphores
		// INSIDE their goroutine. Slow JuiceFS REMOVE/SETATTR/COMMIT bursts therefore
		// cannot consume every reader slot or park this loop ahead of READDIR.
		admission := classifyRPCAdmission(w.req.Header.Prog, w.req.Header.Proc)
		if admission == rpcAdmissionRead && c.Server.rpcSem != nil {
			// [S6 / H1 grader] Admission is the read head-of-line point: when the
			// shared rpcSem is saturated the reader parks HERE, blocking every
			// following LOOKUP/GETATTR/READ on this single TCP mount. Grade the
			// wait WITHOUT adding cost to the uncontended path: attempt a
			// NON-BLOCKING acquire first — on the common warm case (a free slot)
			// we take it immediately with NO time.Now(), NO atomic, NO record.
			// Only when the slot is FULL (exactly the HOL case we want to measure)
			// do we sample the clock once and record the blocked duration via an
			// atomic CAS-max gauge + threshold buckets (ObserveAdmitWait — no
			// lock, no syscall). Warm expectation: rpc_admit_wait_us stays 0. A
			// fat tail here PROVES admission HOL before any readSem surgery.
			select {
			case c.Server.rpcSem <- struct{}{}:
				// fast path: slot free, zero admission wait — record nothing.
			default:
				admitStart := time.Now()
				select {
				case c.Server.rpcSem <- struct{}{}:
					metrics.Default().ObserveAdmitWait(time.Since(admitStart))
				case <-connCtx.Done():
					return
				}
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
				if admission == rpcAdmissionRead && c.Server.rpcSem != nil {
					<-c.Server.rpcSem
				}
			}()

			// [JM6] WRITE admission happens HERE, off the reader path. A write
			// parked on a full writeSem is a cheap goroutine holding NO rpcSem
			// slot, so reads stay live. connCtx.Done() lets an abandoned /
			// disconnected write unwind before it ever runs the handler.
			if admission == rpcAdmissionWrite && c.Server.writeSem != nil {
				select {
				case c.Server.writeSem <- struct{}{}:
				case <-connCtx.Done():
					return
				}
				defer func() { <-c.Server.writeSem }()
			}
			if admission == rpcAdmissionMutation && c.Server.mutationSem != nil {
				select {
				case c.Server.mutationSem <- struct{}{}:
				case <-connCtx.Done():
					return
				}
				defer func() { <-c.Server.mutationSem }()
			}

			start := time.Now()
			// Track this RPC from dispatch to completion. The completed-op
			// latency metrics can't see a HUNG RPC (it never completes); the
			// in-flight watchdog dumps goroutines when one crosses ~22s, well
			// before the ~40s soft-mount timeout aborts the client's copy with
			// "error 100060". defer (not an inline call after c.handle) so the
			// entry is always cleared, even if the handler panics.
			ifid := inflightRegister(inflightOpName(w.req))
			defer inflightDone(ifid)
			err := c.handle(connCtx, w)
			elapsed := time.Since(start)
			respErr := w.finish(connCtx)

			rpcCount.Add(1)
			if elapsed > 5*time.Millisecond {
				slowRPCCount.Add(1)
				// RATE-LIMITED, and it has to be. This line was the single
				// largest source of mutex contention in the whole process
				// under a concurrent read -- 45.6% of all contention, every
				// bit of it slog's global logger lock, reached from these
				// handler goroutines.
				//
				// It is a feedback loop, not just overhead. Load makes RPCs
				// queue; queueing pushes them past 50ms; every one that
				// crosses then takes a PROCESS-WIDE lock and formats the
				// whole request with %v; that serializes the handlers, which
				// lengthens the queue, which makes more RPCs cross 50ms. The
				// line meant to report slowness was causing it. A read at
				// 1,650 MB/s with 88 RPCs in flight puts per-RPC wall time at
				// ~53ms -- sitting exactly on the threshold, so nearly every
				// RPC logged.
				//
				// The COUNTER above stays unconditional, so nothing is lost
				// that anyone measures; only the log line is thinned.
				if elapsed > 50*time.Millisecond && allowSlowRPCLog() {
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
	if nfsTrace {
		if appError != nil {
			Log.Infof("TRACE rpc %s -> ERR %v", w.req.String(), appError)
		} else {
			Log.Infof("TRACE rpc %s -> OK", w.req.String())
		}
	}
	// A wedged JuiceFS surfaces as ErrFUSETimeout from the filesystem layer.
	// Map it (however the handler wrapped it) to NFS3ERR_JUKEBOX so the client
	// retries instead of aborting on a permanent error. The handler has already
	// returned — freeing its rpcSem slot — which is what keeps a backend wedge
	// from exhausting the slot budget and staling the whole mount.
	// Same treatment for a backend blip (#9): the op failed only because the
	// metadata backend was mid-restart; a client retry after the reconnect
	// succeeds. Both sentinels are BOUNDED at their source (wedge probe /
	// blipParkWindow), so neither can tarpit forever.
	if appError != nil && (errors.Is(appError, ErrFUSETimeout) || errors.Is(appError, ErrBackendBlip)) {
		appError = &NFSStatusError{NFSStatusJukebox, appError}
		// Count it: a JUKEBOX reply is a "success" to the latency metrics, so a
		// retry storm (the "error 100060" mechanism) is otherwise invisible.
		recordJukebox(inflightOpName(w.req))
	}
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

	// [JM5] Use pooled buffer to avoid allocation per RPC. The header is
	// already parsed here, so a READ can be given a buffer that is big enough
	// from the start instead of growing into one.
	buf := getResponseBufferFor(req.Header.Prog, req.Header.Proc)
	w = &response{
		conn:     c,
		req:      &req,
		errorFmt: basicErrorFormatter,
		writer:   buf,
	}
	return w, nil
}

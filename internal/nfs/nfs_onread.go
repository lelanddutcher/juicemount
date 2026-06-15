package nfs

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/willscott/go-nfs-client/nfs/xdr"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

type nfsReadArgs struct {
	Handle []byte
	Offset uint64
	Count  uint32
}

type nfsReadResponse struct {
	Count uint32
	EOF   uint32
	Data  []byte
}

// MaxRead is the advertised largest buffer the server is willing to read
const MaxRead = 1 << 24

// CheckRead is a size where - if a request to read is larger than this,
// the server will stat the file to learn it's actual size before allocating
// a buffer to read into.
const CheckRead = 1 << 15

// [JM6] subreadSize is the granularity at which a single NFS READ RPC is
// subdivided internally before checking the per-RPC deadline. The kernel
// NFS client gives the server a bounded window (with the macOS NFS mount
// flags we use, timeo=10 deciseconds × retrans=2 = ~3 s) before reporting
// EIO to the caller. If a single ReadAt for the full requested chunk
// blocks longer than that (e.g., JuiceFS fetching cold blocks from MinIO),
// the user's `cat`/`dd`/NLE-scrub hits "Operation timed out" — which is
// exactly what reproduced 2026-05-16 against a cold 5 GB file.
//
// Splitting into 256 KiB sub-reads lets us check the deadline between
// sub-reads and respond with whatever bytes we have so far when the
// budget is exhausted. NFS protocol allows short reads (RFC 1813 §3.3.6):
// the client uses the returned Count to know how much it got and reissues
// for the remainder. This effectively spreads cold-fetch latency across
// multiple kernel RPCs, none of which exceed the kernel's per-RPC budget.
//
// 256 KiB is small enough to fit comfortably in JuiceFS's chunk size
// (4 MiB) without crossing chunk boundaries — most subreads hit a single
// chunk's cached pages. Large enough that subdivision overhead is
// negligible (~4 subreads per 1 MiB request).
const subreadSize = 256 * 1024

// [JM6] subreadDeadline is the wall-clock budget for a single NFS READ
// RPC's handler-side work. Must be strictly less than the kernel's
// per-RPC timeout (~3 s on macOS with our mount opts) so we always
// surrender voluntarily rather than have the kernel surface EIO.
//
// 2 s gives 1 s of headroom over kernel timeo*retrans, and is long enough
// to absorb 1-2 MinIO round-trips for cold blocks while preventing the
// "stuck on one chunk for 4 s" failure mode we instrumented.
const subreadDeadline = 2 * time.Second

// [JM5] Pool for READ data buffers to avoid per-RPC allocation.
// The default capacity covers the common case (1MB rsize).
var readDataPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 1<<20) // 1MB
		return &buf
	},
}

func onRead(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = opAttrErrorFormatter
	var obj nfsReadArgs
	err := xdr.Read(w.req.Body, &obj)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	fs, path, err := userHandle.FromHandle(obj.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}

	fh, err := fs.Open(fs.Join(path...))
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		return &NFSStatusError{NFSStatusAccess, err}
	}
	defer fh.Close()

	resp := nfsReadResponse{}

	// QA-31 (2026-05-25): fast-path the size-clamp via the handle's cached
	// info when available. The legacy fs.Stat path went through JM's
	// Stat() which contains a 2-second-budgeted FUSE Lstat for the
	// phantom-purge gate — a fixed per-RPC cost that dominated read
	// throughput on cached files. Cached size is a snapshot from Open
	// time; a stale value can only cause a benign short read (NFS clients
	// reissue), not a correctness issue.
	if obj.Count > CheckRead {
		var size int64
		var haveSize bool
		if cp, ok := fh.(CachedInfoProvider); ok {
			if info := cp.CachedInfo(); info != nil {
				size = info.Size()
				haveSize = true
			}
		}
		if !haveSize {
			info, err := fs.Stat(fs.Join(path...))
			if err != nil {
				return &NFSStatusError{NFSStatusAccess, err}
			}
			size = info.Size()
		}
		// QA-31 HIGH-2 fix: explicit EOF-at-or-past-end handling. Without
		// this, uint64(size)-obj.Offset underflows when Offset > size,
		// producing a huge spurious Count that downstream clamps to
		// MaxRead and wastes a 16 MiB allocation per such RPC. RFC 1813
		// §3.3.6: server should return zero-length result when reading
		// at or past EOF.
		if int64(obj.Offset) >= size {
			obj.Count = 0
		} else if size-int64(obj.Offset) < int64(obj.Count) {
			obj.Count = uint32(size - int64(obj.Offset))
		}
	}
	if obj.Count > MaxRead {
		obj.Count = MaxRead
	}

	// [JM5] Use pooled buffer for read data when it fits the common 1MB case.
	var pooled bool
	if obj.Count <= 1<<20 {
		bufPtr := readDataPool.Get().(*[]byte)
		resp.Data = (*bufPtr)[:obj.Count]
		pooled = true
		defer func() {
			if pooled {
				readDataPool.Put(bufPtr)
			}
		}()
	} else {
		resp.Data = make([]byte, obj.Count)
	}

	// [JM6] Subdivided read with per-RPC wall-clock budget. See
	// subreadSize and subreadDeadline at file top for rationale.
	//
	// Loop invariants:
	//   cnt counts bytes actually copied into resp.Data so far.
	//   start is the wall-clock origin for the deadline check.
	//
	// Termination cases:
	//   - reached obj.Count: full request fulfilled, normal response.
	//   - real I/O error from fh.ReadAt: propagate as NFSStatusIO.
	//   - EOF from fh.ReadAt: mark resp.EOF and stop early.
	//   - short read from fh.ReadAt: pass the short count through;
	//     the client will reissue at offset+cnt for the rest.
	//   - deadline exceeded between subreads: return what we have;
	//     resp.EOF stays 0 so the client knows more bytes remain.
	cnt := 0
	want := int(obj.Count)
	start := time.Now()
	var ioErr error
	hitEOF := false
	for cnt < want {
		// Deadline check between subreads. We commit to completing
		// the current subread once started — Go file I/O is not
		// interruptible at this level — so the worst-case latency is
		// subreadDeadline + one subread's duration. With subreadSize
		// = 256 KiB and even a slow ~10 MB/s cold-fetch path, that's
		// ~25 ms of overshoot. Comfortable inside the kernel budget.
		if cnt > 0 && time.Since(start) >= subreadDeadline {
			break
		}
		remaining := want - cnt
		chunk := remaining
		if chunk > subreadSize {
			chunk = subreadSize
		}
		n, err := fh.ReadAt(resp.Data[cnt:cnt+chunk], int64(obj.Offset)+int64(cnt))
		if n > 0 {
			cnt += n
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				hitEOF = true
				break
			}
			ioErr = err
			break
		}
		if n < chunk {
			// Underlying ReadAt returned a short count without an
			// error. Pass-through (likely backed by a partial cache
			// hit followed by an EOF or short-cached-block).
			break
		}
	}
	if ioErr != nil {
		// [JM6 tier-1.7] Offline fail-fast: distinguish offline-refusal
		// from genuine I/O error. NFSStatusNXIO preserves the kernel's
		// file handle cache for post-recovery; NFSStatusIO would not.
		if pin.IsOfflineNotAvailable(ioErr) {
			return &NFSStatusError{NFSStatusNXIO, ioErr}
		}
		return &NFSStatusError{NFSStatusIO, ioErr}
	}
	resp.Count = uint32(cnt)
	resp.Data = resp.Data[:resp.Count]
	if hitEOF {
		resp.EOF = 1
	}
	if cnt > 0 {
		// [JM6] Mark data-transfer activity so the keep-awake assertion
		// holds during a read-back copy. Metadata RPCs never reach here.
		dataXferCount.Add(1)
	}

	// [JM5] Use pooled response buffer instead of allocating.
	writer := getResponseBuffer()
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		putResponseBuffer(writer)
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	// QA-31 (2026-05-25): prefer the cached attrs from the open handle
	// over a fresh tryStat. NFS post-op attrs are documented as advisory
	// (clients use a separate GETATTR for fresh state); skipping the
	// per-RPC Lstat here is a measured win on cached-read throughput.
	postAttrs := tryCachedStat(fh, fs.Join(path...))
	if postAttrs == nil {
		postAttrs = tryStat(fs, path)
	}
	if err := WritePostOpAttrs(writer, postAttrs); err != nil {
		putResponseBuffer(writer)
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := xdr.Write(writer, resp); err != nil {
		putResponseBuffer(writer)
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	bodyBytes := writer.Bytes()
	writeErr := w.Write(bodyBytes)
	putResponseBuffer(writer)
	if writeErr != nil {
		return &NFSStatusError{NFSStatusServerFault, writeErr}
	}
	return nil
}

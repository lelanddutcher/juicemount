package nfs

import "sync"

// offlinereadbuf.go — the private buffer the offline read path needs, without
// allocating it on every read.
//
// WHY A PRIVATE BUFFER AT ALL (do not remove this): the offline branch of
// cachedFile.ReadAt reads through readAtBounded, which returns as soon as the
// bound expires while its worker goroutine is STILL RUNNING and still holding
// the destination slice. If that slice were the caller's `p`, the orphan
// goroutine would scribble into a POOLED NFS buffer after the caller had
// already returned it and another request had taken it — silent cross-request
// data corruption. Reading into a private buffer and copying out is the guard.
//
// WHY POOL IT: the guard does not require a FRESH buffer, only a private one.
// As written it was `make([]byte, len(p))` per read — a zeroed allocation on
// exactly the path the product promises runs at disk speed, plus the GC
// pressure of throwing a megabyte away per read.
//
// THE RELEASE RULE, which is the whole subtlety: a buffer may go back in the
// pool ONLY when the bounded read completed. If the bound was exceeded, the
// orphan goroutine still owns the buffer and will write into it at some
// unknown later time. Recycling it then would reintroduce the exact corruption
// the private buffer exists to prevent, just with a longer fuse. So a
// timed-out buffer is ABANDONED to the GC, which collects it once the orphan
// finishes. Timeouts are the rare case (a bound-exceeded read is a refused
// read), so the pool still serves the overwhelming majority of reads.

// offlineReadBufCap is the size of a pooled buffer. NFS reads arrive at or
// below 1 MiB in practice; a request larger than this is allocated directly
// rather than growing every pooled buffer to fit the outlier.
const offlineReadBufCap = 1 << 20

var offlineReadBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, offlineReadBufCap)
		return &b
	},
}

// offlineReadBuf returns a private buffer of exactly n bytes for a bounded
// offline read. Pass the result to releaseOfflineReadBuf when the read is done.
func offlineReadBuf(n int) []byte {
	if n > offlineReadBufCap {
		return make([]byte, n)
	}
	bp := offlineReadBufPool.Get().(*[]byte)
	return (*bp)[:n]
}

// releaseOfflineReadBuf returns a buffer to the pool, but ONLY when the bounded
// read actually completed.
//
// done==false means readAtBounded gave up while its goroutine kept running and
// kept the buffer. Recycling it would let that goroutine write into a buffer
// another read is using. Dropping it costs one allocation on a path that is
// already failing (the read is about to be refused as ErrOfflineNotAvailable),
// which is the cheap side of an asymmetric trade: a wasted allocation versus
// silent wrong bytes.
func releaseOfflineReadBuf(buf []byte, done bool) {
	if !done || cap(buf) != offlineReadBufCap {
		return
	}
	full := buf[:offlineReadBufCap]
	offlineReadBufPool.Put(&full)
}

package nfs

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// A cold first read on a far link waits for round trips in series: the
// direct SSD reader's path proof, then its chunk mapping, then (on a miss)
// the FUSE open, then the FUSE read. Each costs one link round trip or more
// (remote lab, abroad: 0.2 + 0.2 + 0.4-0.6 s before the read starts). The
// mapping and the open do not depend on the proof's answer, so they start
// with it:
//
//   - the mapping fetch runs beside the proof (one round trip instead of two
//     when the blocks are local, as after a relaunch with a warm cache);
//   - when the reader has no mapping of the chunk in RAM (the file has no
//     known local blocks), the FUSE open starts too, and the read joins it
//     through the FD pool's single-flight.
//
// Neither grants anything: the proof still decides whether local blocks may
// be read, and the FUSE read path is unchanged. JM_OPEN_OVERLAP=0 turns both
// off.
var openOverlapOff = os.Getenv("JM_OPEN_OVERLAP") == "0"

// farLink is the link test readOpenTimeoutFor uses: a round trip is expensive.
func farLink() bool {
	p := netprofile.Default()
	c := p.Class()
	return p.HighLatency() || c == netprofile.ClassSlow || c == netprofile.ClassMetered
}

// overlapOpenStarted counts overlapped opens (tests and diagnostics).
var overlapOpenStarted atomic.Int64

// startOverlappedOpen begins the pooled FUSE open of a file whose read is
// about to consult the SSD reader. The open's descriptor goes to the FD pool
// and its reference is released at once; the read's own ensureFuseFD then
// finds the pooled entry or joins the open in flight. At most one per
// cachedFile.
func (f *cachedFile) startOverlappedOpen(off int64) {
	if openOverlapOff || f.fdPool == nil || f.handler == nil || f.handler.testRefuseFUSEContentReads ||
		pin.IsOffline() || off < 0 || f.cacheReader == nil || f.cacheReader.HasChunkMapping(f.inode, off/cache.ChunkSize) ||
		!farLink() || !f.overlapOpen.CompareAndSwap(false, true) {
		return
	}
	pool, path := f.fdPool, f.fusePath
	overlapOpenStarted.Add(1)
	go func() {
		fd, err, ok := pooledReadOpenWithTimeout(metrics.FUSESrcForeground, pool, path, readOpenTimeout())
		if ok && err == nil && fd != nil {
			pool.Release(path)
		}
	}()
}

// prepareMappingBesideProof starts the bounded mapping fetch of the chunk at
// off while the caller proves the path. The foreground lookup after the
// proof joins it or finds it in RAM. The job is binding work, so a stop
// waits for it.
func (b *cacheBinding) prepareMappingBesideProof(r *cache.Reader, inode uint64, off int64, timeout time.Duration) {
	if openOverlapOff || r == nil || off < 0 || timeout <= 0 || r.HasChunkMapping(inode, off/cache.ChunkSize) || !farLink() {
		return
	}
	b.workMu.Lock()
	if b.workClosed {
		b.workMu.Unlock()
		return
	}
	b.workActive++
	b.workMu.Unlock()
	go func() {
		defer func() {
			b.workMu.Lock()
			b.workActive--
			if b.workClosed && b.workActive == 0 {
				close(b.workDone)
			}
			b.workMu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(b.workCtx, timeout)
		defer cancel()
		_ = r.PrepareChunkMapping(ctx, inode, off/cache.ChunkSize)
	}()
}

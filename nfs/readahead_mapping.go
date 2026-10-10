package nfs

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/metadata"
)

const readaheadMappingDeadline = 3 * time.Second

// readaheadMappingJob owns only metadata preparation for ranges the existing
// worker actually read. It does not certify that the FUSE bytes populated the
// direct SSD cache; an ordinary read still verifies the requested CRC blocks.
type readaheadMappingJob struct {
	h           *JuiceMountHandler
	b           *cacheBinding
	reader      *cache.Reader
	identity    metadata.CachedFileIdentity
	observed    metadata.CachePathObservation
	binding     cache.VolumeBinding
	epoch       uint64
	writerGen   uint64
	ticket      *readAdmission
	chunks      []int64
	demand      bool
	releaseOnce sync.Once
}

// Called only by the already-owned prefetch goroutine, before any source I/O.
// The scheduled inode is part of the admission: a replaced name cannot borrow
// an old OnRead notification to publish its successor's map.
func (h *JuiceMountHandler) captureReadaheadMapping(inode uint64, name string) *readaheadMappingJob {
	if h == nil || h.store == nil || h.readahead == nil || !h.readahead.prefetchAllowed() {
		return nil
	}
	b := h.cacheBinding.Load()
	if b == nil || b.paths == nil || b.session == nil || b.workCtx.Err() != nil || h.cacheBackendError() != nil {
		return nil
	}
	binding, bound := b.session.SnapshotBinding()
	epoch, remote := b.session.RemoteObservation()
	r := h.cacheReader.Load()
	if !bound || !remote || r == nil {
		return nil
	}
	readerBinding, readerBound := r.VolumeBinding()
	if !readerBound || readerBinding != binding {
		return nil
	}
	b.workMu.Lock()
	if b.workClosed {
		b.workMu.Unlock()
		return nil
	}
	b.workActive++
	b.workMu.Unlock()
	j := &readaheadMappingJob{h: h, b: b, reader: r, binding: binding, epoch: epoch}
	fail := func() *readaheadMappingJob { j.release(); return nil }
	ticket, err := h.namespace.admitRead(name)
	if err != nil {
		return fail()
	}
	j.ticket = ticket
	id, ok := h.store.LookupCachedFileIdentity(name)
	if !ok || id.Inode != inode || id.Size <= 0 {
		return fail()
	}
	j.identity = id
	h.activeWritersMu.Lock()
	j.writerGen = h.activeWriterGen[name]
	clearWriter := h.activeWriters[name] == 0
	h.activeWritersMu.Unlock()
	if !clearWriter || !j.noPendingWrites() {
		return fail()
	}
	selected := metadata.CachePathBinding{Digest: binding.Backend, FormatUUID: binding.UUID}
	if !b.paths.CachePathEligible(selected, id) {
		return fail()
	}
	ctx, cancel := context.WithTimeout(b.workCtx, readaheadMappingDeadline)
	observed, err := b.paths.CaptureCachePathContext(ctx, name)
	cancel()
	if err != nil {
		return fail()
	}
	j.observed = observed
	if !j.current() {
		return fail()
	}
	return j
}

func (j *readaheadMappingJob) release() {
	j.releaseOnce.Do(func() {
		if j.ticket != nil {
			j.ticket.release()
		}
		j.b.workMu.Lock()
		j.b.workActive--
		if j.b.workClosed && j.b.workActive == 0 {
			close(j.b.workDone)
		}
		j.b.workMu.Unlock()
		if j.demand {
			j.h.releaseDemand()
		}
	})
}

func (j *readaheadMappingJob) noPendingWrites() bool {
	name := j.identity.Path
	if contiguous, written, writer, tracked := j.h.inPlaceHoles.routing(name); tracked && (writer || written > contiguous) {
		return false
	}
	j.h.writeSizeMu.Lock()
	high := j.h.writeSizes[name]
	j.h.writeSizeMu.Unlock()
	if high > j.identity.Size {
		return false
	}
	if spool := j.h.spool.Load(); spool != nil && spool.HasPending(name) {
		return false
	}
	return true
}

func (j *readaheadMappingJob) current() bool {
	if j == nil || j.h.cacheBinding.Load() != j.b || j.b.workCtx.Err() != nil || j.ticket.check() != nil || j.h.namespace.servingError(j.identity.Path) != nil || j.h.cacheBackendError() != nil {
		return false
	}
	if !j.noPendingWrites() || !j.demand && (j.h.readahead == nil || !j.h.readahead.prefetchAllowed()) {
		return false
	}
	j.h.activeWritersMu.Lock()
	gen, clearWriter := j.h.activeWriterGen[j.identity.Path], j.h.activeWriters[j.identity.Path] == 0
	j.h.activeWritersMu.Unlock()
	if !clearWriter || gen != j.writerGen {
		return false
	}
	live, ok := j.h.store.LookupCachedFileIdentity(j.identity.Path)
	if !ok || live != j.identity || !j.b.paths.CachePathObservationCurrent(j.observed) {
		return false
	}
	selected := metadata.CachePathBinding{Digest: j.binding.Backend, FormatUUID: j.binding.UUID}
	if !j.b.paths.CachePathEligible(selected, j.identity) {
		return false
	}
	binding, bound := j.b.session.SnapshotBinding()
	return bound && binding == j.binding && j.b.session.MatchesRemoteObservation(j.epoch) && j.h.cacheReader.Load() == j.reader
}

func (j *readaheadMappingJob) record(off int64, n int) {
	if j == nil || n <= 0 || off < 0 || off >= j.identity.Size {
		return
	}
	end := j.identity.Size
	if int64(n) <= end-off {
		end = off + int64(n)
	}
	for chunk := off / cache.ChunkSize; chunk <= (end-1)/cache.ChunkSize; chunk++ {
		if len(j.chunks) == 0 || j.chunks[len(j.chunks)-1] != chunk {
			j.chunks = append(j.chunks, chunk)
		}
	}
}

// Called after the source descriptor is closed and all FUSE data gates are
// released. One deadline bounds all distinct chunks the actual worker read.
func (j *readaheadMappingJob) prepare() {
	if j == nil || len(j.chunks) == 0 || !j.current() {
		return
	}
	ctx, cancel := context.WithTimeout(j.b.workCtx, readaheadMappingDeadline)
	defer cancel()
	for _, chunk := range j.chunks {
		if ctx.Err() != nil || !j.current() {
			return
		}
		err := j.reader.PrepareChunkMapping(ctx, j.identity.Inode, chunk)
		if errors.Is(err, cache.ErrVolumeBinding) {
			j.b.refuseIdentity(err)
			return
		}
		if err != nil || !j.current() {
			return
		}
	}
}

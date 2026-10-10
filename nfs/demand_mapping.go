package nfs

import (
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

// captureDemandMapping reserves the sole opportunistic metadata preparation
// before opening the source descriptor. Its ancestry comes exclusively from
// the already-qualified RAM record; a demand cannot create path authority.
func (h *JuiceMountHandler) captureDemandMapping(f *cachedFile) *readaheadMappingJob {
	if h == nil || f == nil || f.cacheReader == nil || f.cacheIdentityConflict || pin.IsOffline() || h.cacheBackendError() != nil {
		return nil
	}
	b := h.cacheBinding.Load()
	if b == nil || b.paths == nil || b.session == nil || b.workCtx.Err() != nil {
		return nil
	}
	binding, bound := b.session.SnapshotBinding()
	epoch, remote := b.session.RemoteObservation()
	reader := h.cacheReader.Load()
	if !bound || !remote || reader == nil || reader != f.cacheReader {
		return nil
	}
	readerBinding, readerBound := reader.VolumeBinding()
	if !readerBound || readerBinding != binding {
		return nil
	}
	selected := metadata.CachePathBinding{Digest: binding.Backend, FormatUUID: binding.UUID}
	observed, ok := b.paths.SnapshotEligibleCachePath(selected, f.cacheIdentity)
	if !ok || f.cacheIdentity.Path != f.name || f.cacheIdentity.Inode != f.inode || f.cacheIdentity.Size <= 0 {
		return nil
	}
	if !h.reserveDemand() {
		return nil
	}
	b.workMu.Lock()
	if b.workClosed {
		b.workMu.Unlock()
		h.releaseDemand()
		return nil
	}
	b.workActive++
	b.workMu.Unlock()
	j := &readaheadMappingJob{h: h, b: b, reader: reader, identity: f.cacheIdentity, observed: observed, binding: binding, epoch: epoch, demand: true}
	fail := func() *readaheadMappingJob { j.release(); return nil }
	ticket, err := h.namespace.admitRead(f.name)
	if err != nil {
		return fail()
	}
	j.ticket = ticket
	h.activeWritersMu.Lock()
	j.writerGen = h.activeWriterGen[f.name]
	clearWriter := h.activeWriters[f.name] == 0
	h.activeWritersMu.Unlock()
	if !clearWriter || !j.noPendingWrites() || !j.current() {
		return fail()
	}
	return j
}

func (h *JuiceMountHandler) reserveDemand() bool {
	h.demandMu.Lock()
	defer h.demandMu.Unlock()
	if h.demandStopping || h.demandActive {
		return false
	}
	h.demandActive = true
	return true
}

func (h *JuiceMountHandler) releaseDemand() {
	h.demandMu.Lock()
	h.demandActive = false
	if h.demandStopping && h.demandDone != nil {
		close(h.demandDone)
		h.demandDone = nil
	}
	h.demandMu.Unlock()
}

func (h *JuiceMountHandler) beginDemandStop() <-chan struct{} {
	h.demandMu.Lock()
	defer h.demandMu.Unlock()
	h.demandStopping = true
	if h.demandDone == nil {
		h.demandDone = make(chan struct{})
		if !h.demandActive {
			close(h.demandDone)
		}
	}
	return h.demandDone
}

func demandMappingMiss(tr *cachedReadTrace) bool {
	if tr == nil || !tr.CachePathInitiallyEligible {
		return false
	}
	switch tr.Reader.Outcome {
	case "mapping_deadline", "mapping_error", "mapping_absent_or_hole":
		return true
	default:
		return false
	}
}

// A successful FUSE response owns its bytes. This worker only supplies the
// missing chunk map for a later ordinary, CRC-verified Reader attempt.
func (j *readaheadMappingJob) finishDemand(off int64, n int) {
	if j == nil {
		return
	}
	j.record(off, n)
	if len(j.chunks) == 0 || !j.current() {
		j.release()
		return
	}
	go func() {
		defer j.release()
		j.prepare()
	}()
}

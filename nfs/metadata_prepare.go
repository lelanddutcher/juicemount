package nfs

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

const metadataPrepareDeadline = 5 * time.Second
const metadataPrepareMaxSize = 128 << 10

// StartMetadataPreparation consumes only bounded post-commit hints. The worker
// owns its original SQL, Reader and FUSE operations until they physically end.
// Its completion never labels a mirror row ready; ordinary reads still enforce
// current path/binding authority and CRC on every use.
func (h *JuiceMountHandler) StartMetadataPreparation(hints <-chan metadata.CacheCandidate) {
	h.metadataPrepareMu.Lock()
	defer h.metadataPrepareMu.Unlock()
	if h.metadataPrepareStopped || h.metadataPrepareDone != nil || hints == nil {
		return
	}
	h.metadataPrepareStop = make(chan struct{})
	h.metadataPrepareDone = make(chan struct{})
	go h.metadataPrepareLoop(hints, h.metadataPrepareStop, h.metadataPrepareDone)
}

func (h *JuiceMountHandler) beginMetadataPreparationStop() <-chan struct{} {
	h.metadataPrepareMu.Lock()
	defer h.metadataPrepareMu.Unlock()
	if !h.metadataPrepareStopped {
		h.metadataPrepareStopped = true
		if h.metadataPrepareStop != nil {
			close(h.metadataPrepareStop)
		}
	}
	if h.metadataPrepareDone != nil {
		return h.metadataPrepareDone
	}
	done := make(chan struct{})
	close(done)
	return done
}

func (h *JuiceMountHandler) metadataPrepareLoop(hints <-chan metadata.CacheCandidate, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	// One worker bounds physical work across cache-binding replacement. The
	// fixed channel bounds pending hints; this small seen set bounds dedupe RAM.
	seen := make(map[metadata.CacheCandidate]struct{}, 16)
	for {
		// Boot can buffer candidates before cache recovery clears automatic
		// offline. Do not consume and dedupe them while this gate is closed.
		for pin.IsOffline() {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
		select {
		case <-stop:
			return
		case hint, ok := <-hints:
			if !ok {
				return
			}
			select {
			case <-stop:
				return
			default:
			}
			// An offline transition between select and admission retains this
			// exact dequeued hint without putting it behind later candidates.
			for pin.IsOffline() {
				select {
				case <-stop:
					return
				case <-time.After(100 * time.Millisecond):
				}
			}
			if _, duplicate := seen[hint]; duplicate {
				continue
			}
			if len(seen) == 16 {
				clear(seen)
			}
			seen[hint] = struct{}{}
			h.prepareMetadataHint(hint)
		}
	}
}

type metadataPrepareJob struct {
	h        *JuiceMountHandler
	b        *cacheBinding
	r        *cache.Reader
	hint     metadata.CacheCandidate
	volume   cache.VolumeBinding
	epoch    uint64
	observed metadata.CachePathObservation
	writer   uint64
	ticket   *readAdmission
}

func (j *metadataPrepareJob) release() {
	if j.ticket != nil {
		j.ticket.release()
	}
	j.b.workMu.Lock()
	j.b.workActive--
	if j.b.workClosed && j.b.workActive == 0 {
		close(j.b.workDone)
	}
	j.b.workMu.Unlock()
}

func (j *metadataPrepareJob) current() bool {
	h, b, id := j.h, j.b, j.hint.Identity
	if h.cacheBinding.Load() != b || b.workCtx.Err() != nil || j.ticket == nil || j.ticket.check() != nil ||
		h.namespace.servingError(id.Path) != nil || pin.IsOffline() || h.cacheBackendError() != nil || !j.hint.MatchesStore(h.store) || !b.rc.CacheCandidateCurrent(j.hint) {
		return false
	}
	row := h.store.LookupByPath(id.Path)
	if row == nil || row.IsDir || row.LocalOnly || !row.Mode.IsRegular() || row.Size != id.Size {
		return false
	}
	if live, ok := h.store.LookupCachedFileIdentity(id.Path); !ok || live != id ||
		len(j.observed.Chain) == 0 || !b.paths.CachePathObservationCurrent(j.observed) {
		return false
	}
	h.activeWritersMu.Lock()
	changed := h.activeWriters[id.Path] != 0 || h.activeWriterGen[id.Path] != j.writer
	h.activeWritersMu.Unlock()
	if changed || !j.noPendingWrites() || h.cacheReader.Load() != j.r {
		return false
	}
	selected, bound := b.session.SnapshotBinding()
	readerVolume, readerBound := j.r.VolumeBinding()
	return bound && readerBound && selected == j.volume && readerVolume == j.volume && b.session.MatchesRemoteObservation(j.epoch)
}

func (j *metadataPrepareJob) noPendingWrites() bool {
	h, id := j.h, j.hint.Identity
	if contiguous, written, writer, tracked := h.inPlaceHoles.routing(id.Path); tracked && (writer || written > contiguous) {
		return false
	}
	h.writeSizeMu.Lock()
	high := h.writeSizes[id.Path]
	h.writeSizeMu.Unlock()
	if high > id.Size {
		return false
	}
	if spool := h.spool.Load(); spool != nil && spool.HasPending(id.Path) {
		return false
	}
	return true
}

func (h *JuiceMountHandler) prepareMetadataHint(hint metadata.CacheCandidate) {
	id := hint.Identity
	if h == nil || h.store == nil || id.Path == "" || id.Size <= 0 || id.Size > metadataPrepareMaxSize ||
		!cacheableMetaName(path.Base(id.Path)) || pin.IsOffline() || h.cacheBackendError() != nil {
		return
	}
	b := h.cacheBinding.Load()
	r := h.cacheReader.Load()
	if b == nil || b.rc == nil || b.paths == nil || b.session == nil || r == nil || b.workCtx.Err() != nil ||
		!hint.MatchesStore(h.store) || !b.rc.CacheCandidateCurrent(hint) {
		return
	}
	volume, bound := b.session.SnapshotBinding()
	readerVolume, readerBound := r.VolumeBinding()
	epoch, online := b.session.RemoteObservation()
	if !bound || !readerBound || !online || volume != readerVolume || b.session.Reader() != r {
		return
	}
	b.workMu.Lock()
	if b.workClosed {
		b.workMu.Unlock()
		return
	}
	b.workActive++
	b.workMu.Unlock()
	j := &metadataPrepareJob{h: h, b: b, r: r, hint: hint, volume: volume, epoch: epoch}
	defer j.release()
	ticket, err := h.namespace.admitRead(id.Path)
	if err != nil {
		return
	}
	j.ticket = ticket
	h.activeWritersMu.Lock()
	j.writer = h.activeWriterGen[id.Path]
	clearWriter := h.activeWriters[id.Path] == 0
	h.activeWritersMu.Unlock()
	if !clearWriter || !j.noPendingWrites() || !hint.MatchesStore(h.store) || !b.rc.CacheCandidateCurrent(hint) {
		return
	}
	ctx, cancel := context.WithTimeout(b.workCtx, metadataPrepareDeadline)
	defer cancel()
	observed, err := b.paths.CaptureCachePathContext(ctx, id.Path)
	if err != nil || observed.Epoch.StoreID == "" {
		return
	}
	j.observed = observed
	if !j.current() {
		return
	}
	selected := metadata.CachePathBinding{Digest: volume.Backend, FormatUUID: volume.UUID}
	if !b.paths.CachePathEligible(selected, id) {
		qualified, err := b.qualifyPathOwned(ctx, selected, id, epoch)
		if err != nil || qualified.Epoch != observed.Epoch || !reflect.DeepEqual(qualified.Chain, observed.Chain) {
			return
		}
	}
	if !j.current() || !b.paths.CachePathEligible(selected, id) {
		return
	}
	if err := r.PrepareChunkMapping(ctx, id.Inode, 0); err != nil || !j.current() {
		if errors.Is(err, cache.ErrVolumeBinding) {
			b.refuseIdentity(err)
		}
		return
	}
	if j.resident() {
		return
	}
	if ctx.Err() != nil || !j.current() || !j.readSource() || !j.current() {
		return
	}
	// Source bytes alone grant nothing. Re-pair the mapping and verify CRC+EOF.
	if err := r.PrepareChunkMapping(ctx, id.Inode, 0); err != nil || !j.current() {
		if errors.Is(err, cache.ErrVolumeBinding) {
			b.refuseIdentity(err)
		}
		return
	}
	_ = j.resident()
}

func (j *metadataPrepareJob) resident() bool {
	id := j.hint.Identity
	if !j.current() {
		return false
	}
	attr := cache.ResidentAttr{Size: id.Size, MtimeNS: id.MtimeNS, Valid: true}
	buf := make([]byte, int(id.Size))
	token, err := j.r.ReadResidentRange(id.Inode, 0, buf, attr)
	if err != nil || !token.Valid() || !j.current() {
		return false
	}
	eof, err := j.r.ResidentEOF(id.Inode, attr)
	if err != nil || !eof.Valid() || !j.current() || !token.Valid() {
		return false
	}
	// Snapshot persistence has its own debounced worker and Stop flush. A
	// persistence error cannot turn verified resident content into a source
	// miss, and this opportunistic preparation makes no durable-ready promise.
	return j.current() && token.Valid() && eof.Valid()
}

func metadataSourceMatches(info os.FileInfo, id metadata.CachedFileIdentity) bool {
	if info == nil || !info.Mode().IsRegular() || info.Size() != id.Size || info.ModTime().UnixNano() != id.MtimeNS {
		return false
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && st.Ino == id.Inode
}

// readSource performs at most one original, synchronous size+1 FUSE read. No
// timeout wrapper drops custody of a still-running syscall; Stop retains all
// dependencies until this worker and its descriptor actually settle.
func (j *metadataPrepareJob) readSource() bool {
	id := j.hint.Identity
	if j.h.testRefuseFUSEContentReads {
		return false
	}
	select {
	case warmGate <- struct{}{}:
		defer func() { <-warmGate }()
	default:
		return false
	}
	release, ok := tryAcquireFUSEDataBackground()
	if !ok {
		return false
	}
	defer release()
	if !j.current() {
		return false
	}
	source := filepath.Join(j.h.fusePath, filepath.FromSlash(id.Path))
	f, err := os.Open(source)
	if err != nil {
		return false
	}
	before, err := f.Stat()
	if err != nil || !metadataSourceMatches(before, id) || !j.current() {
		_ = f.Close()
		return false
	}
	buf := make([]byte, int(id.Size)+1)
	n, readErr := f.ReadAt(buf, 0)
	after, statErr := f.Stat()
	closeErr := f.Close()
	if n != int(id.Size) || !errors.Is(readErr, io.EOF) || statErr != nil || closeErr != nil ||
		!metadataSourceMatches(after, id) || !j.current() {
		return false
	}
	currentPath, err := os.Stat(source)
	return err == nil && metadataSourceMatches(currentPath, id) && j.current()
}

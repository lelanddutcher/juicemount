package nfs

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/metadata"
)

// Preview preparation is an experimental, best-effort logical read budget.
// FUSE and the object store may transfer more than these requested bytes.
const (
	previewBlockBytes       = 4 << 20
	previewMaxBytes         = 8 << 20
	previewQueueLimit       = 8
	previewChildrenLimit    = 128
	previewCandidatesLimit  = 8
	previewDedupLimit       = 128
	previewCursorLimit      = 128
	previewCooldown         = time.Minute
	previewMetadataDeadline = 3 * time.Second
)

type previewMode uint8

const (
	previewNone previewMode = iota
	previewMetadataOnly
	previewContent
)

type previewCursorKey struct {
	dir     string
	binding cache.VolumeBinding
}

type previewCursor struct {
	next int
	age  uint64
}

type previewKey struct {
	identity metadata.CachedFileIdentity
	binding  cache.VolumeBinding
	mode     previewMode
}
type previewJob struct {
	key     previewKey
	reader  *cache.Reader
	session *cache.Session
	epoch   uint64
}

func (rm *ReadaheadManager) previewOptIn() bool {
	if rm == nil {
		return false
	}
	if rm.previewOptInSet {
		return rm.previewOptInValue
	}
	return os.Getenv("JM_NATIVE_PREVIEW_WARM") == "1"
}

func (rm *ReadaheadManager) previewMode() previewMode {
	if rm == nil || rm.profile == nil || pin.IsOffline() || rm.refuseFUSEContentReads || !rm.previewOptIn() || os.Getenv("JM_JFS_MAX_READAHEAD") == "0" {
		return previewNone
	}
	snap := rm.profile.Snapshot()
	if !snap.HaveRTT && !snap.HaveBW {
		return previewNone
	}
	switch snap.Class {
	case netprofile.ClassSlow:
		return previewMetadataOnly
	case netprofile.ClassMedium, netprofile.ClassFast:
		return previewContent
	default:
		return previewNone
	}
}

func (rm *ReadaheadManager) previewHealthy() bool {
	return rm.previewMode() == previewContent
}

func (rm *ReadaheadManager) previewAllowed() bool {
	if !rm.previewHealthy() {
		return false
	}
	select {
	case <-rm.previewStop:
		return false
	case <-rm.stopCh:
		return false
	default:
		return true
	}
}

func (rm *ReadaheadManager) previewMetadataAllowed() bool {
	if rm.previewMode() < previewMetadataOnly {
		return false
	}
	select {
	case <-rm.previewStop:
		return false
	case <-rm.stopCh:
		return false
	default:
		return true
	}
}

// admitPreview performs no SQL, proof, open, or network work. Caller already
// owns a resident identity and selected binding captured from this listing.
func (rm *ReadaheadManager) admitPreview(job previewJob) bool {
	if job.key.mode == previewNone || !rm.previewMetadataAllowed() {
		return false
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.stopped || rm.previewClosed || !rm.previewMetadataAllowed() {
		return false
	}
	now := time.Now()
	if at, ok := rm.previewSeen[job.key]; ok && now.Sub(at) < previewCooldown {
		return false
	}
	select {
	case rm.previewQueue <- job:
		if len(rm.previewSeen) >= previewDedupLimit {
			var oldest previewKey
			var when time.Time
			for key, at := range rm.previewSeen {
				if when.IsZero() || at.Before(when) {
					oldest, when = key, at
				}
			}
			delete(rm.previewSeen, oldest)
		}
		rm.previewSeen[job.key] = now
		return true
	default:
		return false
	}
}

func (rm *ReadaheadManager) previewLoop() {
	defer rm.wg.Done()
	defer close(rm.previewDone)
	for {
		select {
		case <-rm.previewStop:
			return
		case <-rm.stopCh:
			return
		case job := <-rm.previewQueue:
			if rm.previewMetadataAllowed() && rm.previewRun != nil {
				rm.previewRun(job)
			}
		}
	}
}

// Close admission before handler dependencies retire. An in-flight syscall
// remains owned by the worker until it really returns; deadline retains them.
func (rm *ReadaheadManager) beginPreviewStop() <-chan struct{} {
	if rm == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	rm.previewOnce.Do(func() { rm.mu.Lock(); rm.previewClosed = true; close(rm.previewStop); rm.mu.Unlock() })
	return rm.previewDone
}

// Discard stale progress when a directory becomes empty. A concurrent
// speculative scan may hold the TryLock; that invocation owns its progress,
// and a later nonempty call still normalizes the cursor to the new length.
func (rm *ReadaheadManager) clearPreviewCursors(dir string) {
	if rm == nil || !rm.previewScanMu.TryLock() {
		return
	}
	defer rm.previewScanMu.Unlock()
	for key := range rm.previewCursors {
		if key.dir == dir {
			delete(rm.previewCursors, key)
		}
	}
}

func (h *JuiceMountHandler) scheduleNativePreviews(dir string, children []*metadata.Entry) {
	rm := h.readahead
	if rm == nil || !rm.previewOptIn() || !rm.previewMetadataAllowed() || h.store == nil || len(children) == 0 {
		return
	}
	mode := rm.previewMode()
	if mode == previewNone {
		return
	}
	b := h.cacheBinding.Load()
	r := h.cacheReader.Load()
	if b == nil || b.session == nil || r == nil || !b.session.Qualified() {
		return
	}
	selected, ok := b.session.SnapshotBinding()
	readerBinding, bound := r.VolumeBinding()
	epoch, ready := b.session.RemoteObservation()
	if !ok || !bound || !ready || selected != readerBinding || selected.BlockSize != previewBlockBytes {
		return
	}
	if !rm.previewScanMu.TryLock() {
		return
	}
	defer rm.previewScanMu.Unlock()
	key := previewCursorKey{dir: dir, binding: selected}
	cursor := rm.previewCursors[key]
	start := cursor.next % len(children)
	// The selected binding and normalized directory are the only retained
	// identity. No child slice, entry, job or content is kept in cursor state.
	admitted := 0
	examined := 0
	for examined < min(len(children), previewChildrenLimit) && admitted < previewCandidatesLimit {
		e := children[(start+examined)%len(children)]
		examined++
		if e == nil || e.IsDir || e.LocalOnly || !e.Mode.IsRegular() || e.Size <= 0 {
			continue
		}
		// AppleDouble sidecars share the original's extension but are not
		// native-preview sources. Keep them visible and available on demand.
		if strings.HasPrefix(e.Name, "._") {
			continue
		}
		switch strings.ToLower(filepath.Ext(e.Name)) {
		case ".mp4", ".mov", ".m4v":
		default:
			continue
		}
		identity, ok := h.store.LookupCachedFileIdentity(e.Path)
		if !ok || identity.Inode != e.Inode || identity.Size != e.Size || identity.Version != e.Version {
			continue
		}
		if h.hasActiveWriter(e.Path) {
			continue
		}
		if spool := h.spool.Load(); spool != nil && spool.HasPending(e.Path) {
			continue
		}
		job := previewJob{key: previewKey{identity: identity, binding: selected, mode: mode}, reader: r, session: b.session, epoch: epoch}
		if rm.admitPreview(job) {
			admitted++
		}
	}
	rm.previewScanAge++
	// Bound retained directory/binding state. Eviction can reset best-effort
	// progress for that key, but never changes read or publication authority.
	if _, exists := rm.previewCursors[key]; !exists && len(rm.previewCursors) >= previewCursorLimit {
		var oldest previewCursorKey
		var oldestAge uint64
		for k, state := range rm.previewCursors {
			if oldestAge == 0 || state.age < oldestAge {
				oldest, oldestAge = k, state.age
			}
		}
		delete(rm.previewCursors, oldest)
	}
	rm.previewCursors[key] = previewCursor{next: (start + examined) % len(children), age: rm.previewScanAge}
}

func (h *JuiceMountHandler) previewCurrent(job previewJob, ticket *readAdmission) bool {
	return h.previewCurrentMode(job, ticket, false)
}

func (h *JuiceMountHandler) previewMetadataCurrent(job previewJob, ticket *readAdmission) bool {
	return h.previewCurrentMode(job, ticket, true)
}

func (h *JuiceMountHandler) previewCurrentMode(job previewJob, ticket *readAdmission, metadataOnly bool) bool {
	allowed := h.readahead.previewAllowed()
	if metadataOnly {
		allowed = h.readahead.previewMetadataAllowed()
	}
	if !allowed || !job.session.MatchesRemoteObservation(job.epoch) || ticket.check() != nil {
		return false
	}
	if b, ok := job.session.SnapshotBinding(); !ok || b != job.key.binding {
		return false
	}
	if r := h.cacheReader.Load(); r != job.reader {
		return false
	}
	id, ok := h.store.LookupCachedFileIdentity(job.key.identity.Path)
	if !ok || id != job.key.identity || h.hasActiveWriter(id.Path) {
		return false
	}
	if contiguous, written, writer, tracked := h.inPlaceHoles.routing(id.Path); tracked && (writer || written > contiguous) {
		return false
	}
	h.writeSizeMu.Lock()
	highWater := h.writeSizes[id.Path]
	h.writeSizeMu.Unlock()
	if highWater > id.Size {
		return false
	}
	if spool := h.spool.Load(); spool != nil && spool.HasPending(id.Path) {
		return false
	}
	return true
}

func (h *JuiceMountHandler) previewPathEligible(job previewJob) bool {
	b := h.cacheBinding.Load()
	if b == nil || b.paths == nil || b.session != job.session {
		return false
	}
	selected := metadata.CachePathBinding{Digest: job.key.binding.Backend, FormatUUID: job.key.binding.UUID}
	return b.paths.CachePathEligible(selected, job.key.identity)
}

// runMetadataPreview prepares only the selected ancestry and at most two
// bounded chunk mappings. It never opens FUSE, reads SSD content, or invokes
// the source-read hooks. One deadline covers proof and both mappings.
func (h *JuiceMountHandler) runMetadataPreview(job previewJob, ticket *readAdmission) {
	if !h.previewMetadataCurrent(job, ticket) {
		return
	}
	b := h.cacheBinding.Load()
	if b == nil || b.session != job.session || b.paths == nil || b.readProof == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), previewMetadataDeadline)
	defer cancel()
	selected := metadata.CachePathBinding{Digest: job.key.binding.Backend, FormatUUID: job.key.binding.UUID}
	if !h.previewPathEligible(job) {
		if _, err := b.qualifyPath(ctx, selected, job.key.identity, job.epoch); err != nil {
			return
		}
	}
	if !h.previewMetadataCurrent(job, ticket) || !h.previewPathEligible(job) {
		return
	}
	last := (job.key.identity.Size - 1) / cache.ChunkSize
	for _, chunk := range []int64{0, last} {
		if !h.previewMetadataCurrent(job, ticket) || !h.previewPathEligible(job) || ctx.Err() != nil {
			return
		}
		if err := job.reader.PrepareChunkMapping(ctx, job.key.identity.Inode, chunk); err != nil {
			if errors.Is(err, cache.ErrVolumeBinding) {
				b.refuseIdentity(err)
			}
			return
		}
		if chunk == last {
			break
		}
	}
}

func (h *JuiceMountHandler) runNativePreview(job previewJob) {
	if hook := h.previewFinished; hook != nil {
		defer hook(job)
	}
	ticket, err := h.namespace.admitRead(job.key.identity.Path)
	if err != nil {
		return
	}
	defer ticket.release()
	if job.key.mode == previewNone {
		return
	}
	if !h.previewMetadataCurrent(job, ticket) {
		return
	}
	// A queued content job may be demoted after a link becomes slow. Its
	// immutable metadata-only ceiling is never promoted into source reads.
	if job.key.mode == previewMetadataOnly || !h.readahead.previewAllowed() {
		h.runMetadataPreview(job, ticket)
		return
	}
	release, ok := defaultReadQoS.tryAcquireBulk()
	if !ok {
		return
	}
	defer release()
	if !h.previewCurrent(job, ticket) {
		return
	}
	var fd *os.File
	defer func() {
		if fd != nil {
			fd.Close()
		}
	}()
	buf := make([]byte, previewBlockBytes)
	limit := min(job.key.identity.Size, int64(previewMaxBytes))
	for off := int64(0); off < limit; off += previewBlockBytes {
		if !h.previewCurrent(job, ticket) {
			return
		}
		length := int(min(int64(previewBlockBytes), limit-off))
		ctx := context.Background()
		n, err := h.readCacheBlock(ctx, job.reader, job.key.identity, job.key.identity.Inode, off, buf[:length], 2*time.Second)
		if err == nil && n == length {
			continue
		}
		if err != nil && !errors.Is(err, cache.ErrCacheMiss) {
			return
		}
		// ErrCacheMiss also covers an unproven path. Only a real selected
		// ancestry proof from readCacheBlock may authorize source traffic.
		if !h.previewCurrent(job, ticket) || !h.previewPathEligible(job) {
			return
		}
		if fd == nil {
			// Share the hardened no-symlink, nonblocking regular-file open.
			// Admission is opportunistic and never occupies foreground slots.
			select {
			case warmGate <- struct{}{}:
			default:
				return
			}
			fd, err = derivatives.OpenRegularUnder(h.fusePath, job.key.identity.Path)
			if err != nil {
				<-warmGate
				return
			}
			stat, err := fd.Stat()
			<-warmGate
			if err != nil || !stat.Mode().IsRegular() || stat.Size() != job.key.identity.Size || stat.ModTime().UnixNano() != job.key.identity.MtimeNS {
				return
			}
			inode := uint64(0)
			if st, ok := stat.Sys().(*syscall.Stat_t); ok {
				inode = st.Ino
			}
			if inode != job.key.identity.Inode {
				return
			}
			if !h.previewCurrent(job, ticket) {
				return
			}
		}
		dataRelease, admitted := tryAcquireFUSEDataBackground()
		if !admitted {
			return
		}
		if !h.previewCurrent(job, ticket) || !h.previewPathEligible(job) {
			dataRelease()
			return
		}
		var got int
		if hook := h.previewReadAt; hook != nil {
			got, err = hook(fd, buf[:length], off)
		} else {
			got, err = fd.ReadAt(buf[:length], off)
		}
		dataRelease()
		if err != nil && !errors.Is(err, io.EOF) {
			return
		}
		if got != length || !h.previewCurrent(job, ticket) || !h.previewPathEligible(job) {
			return
		}
		// Acceptance hook models JuiceFS's cache publication only after the
		// real source syscall. Production uses its own block-cache pipeline.
		if hook := h.previewAfterRead; hook != nil {
			hook(job, buf[:length], off)
		}
		if !h.previewCurrent(job, ticket) {
			return
		}
		n, err = h.readCacheBlock(ctx, job.reader, job.key.identity, job.key.identity.Inode, off, buf[:length], 2*time.Second)
		// The JuiceFS daemon can publish CRC bytes after a successful source
		// read returns. A still-missing block does not spend the next block's
		// separate source-read budget; the next iteration rechecks every guard.
		// Other errors and partial successful reads remain terminal.
		if (err == nil && n != length) || (err != nil && (!errors.Is(err, cache.ErrCacheMiss) || n != 0)) || !h.previewCurrent(job, ticket) {
			return
		}
	}
}

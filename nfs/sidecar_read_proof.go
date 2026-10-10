package nfs

import (
	"errors"
	"io"
	"os"
	"path"
	"syscall"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/metadata"
)

// sidecarReadProof is current-process source evidence and is never persisted.
// Its completed body may separately produce a read-only last-observed snapshot.
// The body is immutable and owned by the sidecar cache's intern pool.
type sidecarReadProof struct {
	identity            metadata.CachedFileIdentity
	generation          uint64
	data                []byte
	snapshotVolume      cache.VolumeBinding
	snapshotVolumeKnown bool
}

// Call with activeWritersMu held. No SQL, filesystem calls or wait on a writer.
func (h *JuiceMountHandler) strongSidecarIdentity(name string) (metadata.CachedFileIdentity, bool) {
	if h.store == nil || h.activeWriters[name] != 0 {
		return metadata.CachedFileIdentity{}, false
	}
	identity, ok := h.store.LookupCachedFileIdentity(name)
	if !ok || identity.Size <= 0 || identity.Size > sidecarMaxFile {
		return identity, false
	}
	h.writeSizeMu.Lock()
	size, exists := h.writeSizes[name]
	h.writeSizeMu.Unlock()
	if exists && size > identity.Size {
		return identity, false
	}
	return identity, true
}

func (h *JuiceMountHandler) beginStrongSidecarRead(name string) *sidecarReadProof {
	if h == nil || h.sidecar == nil || !h.sidecar.enabled || !cacheableMetaName(path.Base(name)) {
		return nil
	}
	h.activeWritersMu.Lock()
	defer h.activeWritersMu.Unlock()
	identity, ok := h.strongSidecarIdentity(name)
	if !ok {
		return nil
	}
	volume, bound := h.snapshotVolume()
	if h.cacheBinding.Load() != nil && !bound {
		return nil // configured scope cannot mint an unscoped strong proof
	}
	return &sidecarReadProof{identity: identity, generation: h.activeWriterGen[name], snapshotVolume: volume, snapshotVolumeKnown: bound}
}

func (h *JuiceMountHandler) validStrongSidecarLocked(proof *sidecarReadProof) bool {
	if proof == nil {
		return false
	}
	identity, ok := h.strongSidecarIdentity(proof.identity.Path)
	if !ok || identity != proof.identity || h.activeWriterGen[identity.Path] != proof.generation {
		return false
	}
	if h.cacheBinding.Load() != nil && !proof.snapshotVolumeKnown {
		return false // binding cannot promote a preexisting unscoped proof
	}
	if proof.snapshotVolumeKnown {
		volume, ok := h.snapshotVolume()
		return ok && volume == proof.snapshotVolume
	}
	return true
}

// The producer must capture proof before I/O and establish true EOF, not just
// len(body)==mirror.Size. Legacy reads/drain callbacks cannot call this method.
func (h *JuiceMountHandler) publishStrongSidecarRead(proof *sidecarReadProof, body []byte) {
	if proof == nil || int64(len(body)) != proof.identity.Size {
		return
	}
	h.activeWritersMu.Lock()
	defer h.activeWritersMu.Unlock()
	if !h.validStrongSidecarLocked(proof) {
		return
	}
	h.sidecar.putWithProof(proof.identity.Path, body, proof.identity.MtimeNS/1e9, proof.identity.Size, proof, h.newSidecarSnapshot(proof, body))
}

func (h *JuiceMountHandler) strongSidecarRead(name string) *sidecarReadProof {
	if h == nil || h.sidecar == nil || !h.sidecar.enabled || !cacheableMetaName(path.Base(name)) {
		return nil
	}
	h.activeWritersMu.Lock()
	defer h.activeWritersMu.Unlock()
	identity, ok := h.strongSidecarIdentity(name)
	if !ok {
		return nil
	}
	h.sidecar.mu.Lock()
	entry := h.sidecar.m[name]
	var proof *sidecarReadProof
	if entry != nil {
		proof = entry.proof
	}
	h.sidecar.mu.Unlock()
	if proof == nil || proof.identity != identity || !h.validStrongSidecarLocked(proof) {
		return nil
	}
	return proof
}

// A v1/v2 restored body is not source evidence. onRead clamps a wide READ
// to the authoritative size before ReadAt, so take one private extra byte
// from the already-checked descriptor to establish EOF explicitly. Failure
// leaves the old entry on its legacy fallback without minting a snapshot.
func (f *cachedFile) upgradeLegacySidecar(off int64, count int) *sidecarReadProof {
	if f == nil || f.handler == nil || off != 0 || pin.IsOffline() || f.handler.testRefuseFUSEContentReads {
		return nil
	}
	f.sourceProofMu.Lock()
	proof := f.sourceProof
	f.sourceProofMu.Unlock()
	if proof == nil || !proof.snapshotVolumeKnown || int64(count) < proof.identity.Size {
		return nil
	}
	f.handler.sidecar.mu.Lock()
	entry := f.handler.sidecar.m[f.name]
	weak := entry != nil && entry.snapshot == nil && entry.proof == nil &&
		entry.size == proof.identity.Size && entry.mtime == proof.identity.MtimeNS/1e9
	f.handler.sidecar.mu.Unlock()
	if !weak {
		return nil
	}
	return proof
}

func (f *cachedFile) readLegacySidecarUpgrade(proof *sidecarReadProof, p []byte) (int, error, bool) {
	if proof == nil {
		return 0, nil, false
	}
	if err := f.ensureFuseFD(); err != nil {
		return 0, nil, false
	}
	body := make([]byte, proof.identity.Size+1)
	n, err := boundedFUSEFileReadAt(f.fuseFD, body, 0, fuseReadTimeout())
	if int64(n) != proof.identity.Size || !errors.Is(err, io.EOF) {
		return 0, nil, false
	}
	f.publishSidecarSourceProof(proof, body[:n]) // includes post-read descriptor check
	if f.completedSidecarSourceProof() == nil {
		return 0, pin.ErrSpoolIncomplete, true
	}
	copied := copy(p, body[:n])
	metrics.Default().AddBytesRead(int64(copied))
	if copied < len(p) {
		return copied, io.EOF, true
	}
	return copied, nil, true
}

// readStrongSidecar validates and copies under the writer/cache lock order.
// Recheck the independently published RAM identity after copying. Any refusal
// is retryable; never fall through using a count clamped to this old body.
func (h *JuiceMountHandler) readStrongSidecar(proof *sidecarReadProof, p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, io.ErrUnexpectedEOF
	}
	h.activeWritersMu.Lock()
	defer h.activeWritersMu.Unlock()
	if !h.validStrongSidecarLocked(proof) {
		return 0, pin.ErrSpoolIncomplete
	}
	// Never acquire Store.mu or writeSizeMu while holding sidecar.mu.
	h.sidecar.mu.Lock()
	entry := h.sidecar.m[proof.identity.Path]
	if entry == nil || entry.proof != proof {
		h.sidecar.mu.Unlock()
		return 0, pin.ErrSpoolIncomplete
	}
	var n int
	if off < int64(len(proof.data)) {
		n = copy(p, proof.data[off:])
	}
	h.sidecar.tick++
	entry.lastUsed = h.sidecar.tick
	h.sidecar.mu.Unlock()
	if !h.validStrongSidecarLocked(proof) {
		clear(p[:n])
		return 0, pin.ErrSpoolIncomplete
	}
	metrics.Default().IncSidecarCacheHit()
	metrics.Default().AddBytesRead(int64(n))
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *cachedFile) checkSidecarProof() error {
	if f.snapshotProof != nil {
		if err := f.handler.checkSidecarSnapshot(f.snapshotProof); err != nil {
			return err
		}
	}
	if f.sidecarProof != nil && f.handler.strongSidecarRead(f.name) != f.sidecarProof {
		return pin.ErrSpoolIncomplete
	}
	if proof := f.completedSidecarSourceProof(); proof != nil {
		f.handler.activeWritersMu.Lock()
		valid := f.handler.validStrongSidecarLocked(proof)
		f.handler.activeWritersMu.Unlock()
		if !valid {
			return pin.ErrSpoolIncomplete
		}
	}
	return nil
}

func (f *cachedFile) CompleteCachedSize() (int64, bool, error) {
	if f.pinGeneration != "" {
		size, err := f.ownedPinSize()
		return size, err == nil, err
	}
	if f.snapshotProof != nil {
		if err := f.handler.checkSidecarSnapshot(f.snapshotProof); err != nil {
			return 0, false, err
		}
		return f.snapshotProof.snapshot.Size, true, nil
	}
	proof := f.sidecarProof
	if proof == nil {
		proof = f.completedSidecarSourceProof()
	}
	if proof == nil {
		return 0, false, nil
	}
	if err := f.checkSidecarProof(); err != nil {
		return 0, false, err
	}
	return int64(len(proof.data)), true, nil
}

func (f *cachedFile) completedSidecarSourceProof() *sidecarReadProof {
	f.sourceProofMu.Lock()
	defer f.sourceProofMu.Unlock()
	return f.completedSourceProof
}

// EOF alone could describe an old descriptor retained across replacement.
// Ineligible sources stay on the legacy lane; they never mint a strong proof.
func sidecarSourceMatches(proof *sidecarReadProof, fd *os.File, src metrics.FUSESource) bool {
	if proof == nil || fd == nil {
		return false
	}
	bound := fuseStatTimeout
	if src == metrics.FUSESrcSidecarWarm {
		bound = warmOpTimeout()
	}
	info, err := boundedDescriptorStatFromSource(src, fd, bound)
	if err != nil {
		return false
	}
	return sidecarInfoMatches(proof, info)
}

func sidecarInfoMatches(proof *sidecarReadProof, info os.FileInfo) bool {
	if proof == nil || info == nil || !info.Mode().IsRegular() || info.Size() != proof.identity.Size || info.ModTime().UnixNano() != proof.identity.MtimeNS {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Ino == proof.identity.Inode
}

// Capture an immutable candidate before this exact FUSE read. An oversized
// RPC already established descriptor identity while sizing; small READs need
// their own bounded pre-read stat. The optional shared candidate is only read
// under its mutex and is never reloaded after body I/O.
func (f *cachedFile) captureSidecarSourceProof(off int64, count int) *sidecarReadProof {
	if off != 0 || f.handler == nil || !cacheableMetaName(path.Base(f.name)) {
		return nil
	}
	f.sourceProofMu.Lock()
	proof := f.sourceProof
	f.sourceProofMu.Unlock()
	if proof != nil && int64(count) >= proof.identity.Size {
		return proof
	}
	proof = f.handler.beginStrongSidecarRead(f.name)
	if proof == nil || int64(count) < proof.identity.Size {
		return nil
	}
	if !sidecarSourceMatches(proof, f.fuseFD, metrics.FUSESrcForeground) {
		return nil
	}
	return proof
}

// A bound SSD hit may complete a proof already established by authoritative
// sizing. Never open or stat a descriptor here: ordinary small, partial, and
// unbound cache reads cannot create source authority. The caller snapshots
// this candidate before reading the CRC-checked cache block.
func (f *cachedFile) cachedSidecarSourceProof(off int64, count int) *sidecarReadProof {
	if f == nil || f.handler == nil || f.cacheReader == nil || off != 0 ||
		!cacheableMetaName(path.Base(f.name)) {
		return nil
	}
	f.sourceProofMu.Lock()
	proof := f.sourceProof
	f.sourceProofMu.Unlock()
	if proof == nil || !proof.snapshotVolumeKnown || proof.identity.Size <= 0 ||
		int64(count) != proof.identity.Size || proof.identity != f.cacheIdentity {
		return nil
	}
	readerVolume, bound := f.cacheReader.VolumeBinding()
	if !bound || readerVolume != proof.snapshotVolume {
		return nil
	}
	selectedVolume, selected := f.handler.snapshotVolume()
	if !selected || selectedVolume != proof.snapshotVolume {
		return nil
	}
	return proof
}

func (f *cachedFile) publishSidecarSourceProof(proof *sidecarReadProof, body []byte) {
	if proof == nil || int64(len(body)) != proof.identity.Size {
		return
	}
	if sidecarSourceMatches(proof, f.fuseFD, metrics.FUSESrcForeground) {
		f.handler.publishStrongSidecarRead(proof, body)
		// Keep the proof of these bytes separate from the immutable Open-time
		// cache handle. Concurrent reads may publish the same identity, but must
		// never rebind this handle's terminal boundary to a successor. Cache
		// eviction does not invalidate bytes already copied from the exact FD.
		f.handler.activeWritersMu.Lock()
		if f.handler.validStrongSidecarLocked(proof) {
			f.sourceProofMu.Lock()
			if f.completedSourceProof == nil {
				owned := *proof
				owned.data = append([]byte(nil), body...)
				f.completedSourceProof = &owned
			}
			f.sourceProofMu.Unlock()
		}
		f.handler.activeWritersMu.Unlock()
	}
}

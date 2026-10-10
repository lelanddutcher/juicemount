package nfs

import (
	"crypto/sha256"
	"io"
	"os"
	"path"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/metadata"
)

// sidecarSnapshot is read-only last-observed state. A MirrorToken identifies
// the local durable observation, not current remote freshness or write rights.
// The body is held separately by the cache's immutable intern pool.
type sidecarSnapshot struct {
	Token   metadata.MirrorToken
	Inode   uint64
	Size    int64
	MtimeNS int64
	Name    string
	Mode    os.FileMode
	SHA256  [sha256.Size]byte
	Volume  cache.VolumeBinding
}

type sidecarSnapshotHandle struct {
	entry      *sidecarEntry
	identity   metadata.CachedFileIdentity
	generation uint64
	snapshot   sidecarSnapshot
	data       []byte
}

func (s *sidecarSnapshot) matches(name string, row metadata.ResidentSnapshotFile, volume cache.VolumeBinding) bool {
	id := row.Identity
	return s != nil && cacheableMetaName(path.Base(name)) &&
		s.Token.Path == name && s.Token == id.Version && s.Inode == id.Inode &&
		s.Size == id.Size && s.MtimeNS == id.MtimeNS && s.Name == row.Name &&
		s.Mode == row.Mode && s.Volume == volume && s.Size > 0 && s.Size <= sidecarMaxFile
}

func (h *JuiceMountHandler) snapshotVolume() (cache.VolumeBinding, bool) {
	if h == nil {
		return cache.VolumeBinding{}, false
	}
	b := h.cacheBinding.Load()
	if b == nil || b.session == nil || b.identityRefused.Load() {
		return cache.VolumeBinding{}, false
	}
	return b.session.SnapshotBinding()
}

// beginSidecarSnapshotRead does bounded RAM work only. The selected session
// binding is valid while offline and does not depend on direct Reader support.
func (h *JuiceMountHandler) beginSidecarSnapshotRead(name string) *sidecarSnapshotHandle {
	if h == nil || h.store == nil || h.sidecar == nil || !h.sidecar.enabled || !cacheableMetaName(path.Base(name)) {
		return nil
	}
	h.activeWritersMu.Lock()
	defer h.activeWritersMu.Unlock()
	if h.activeWriters[name] != 0 {
		return nil
	}
	row, ok := h.store.LookupResidentSnapshotFile(name)
	if !ok {
		return nil
	}
	identity, ok := h.strongSidecarIdentity(name)
	if !ok || identity != row.Identity { // includes local write-size high water
		return nil
	}
	volume, ok := h.snapshotVolume()
	if !ok {
		return nil
	}
	h.sidecar.mu.Lock()
	defer h.sidecar.mu.Unlock()
	entry := h.sidecar.m[name]
	if entry == nil || !entry.snapshot.matches(name, row, volume) || int64(len(entry.data)) != entry.snapshot.Size {
		return nil
	}
	return &sidecarSnapshotHandle{entry: entry, identity: row.Identity, generation: h.activeWriterGen[name], snapshot: *entry.snapshot, data: entry.data}
}

// activeWritersMu is held. Every current-process mutation changes the RAM
// publication or write generation, even if its size and mtime are reused.
func (h *JuiceMountHandler) validSidecarSnapshotLocked(handle *sidecarSnapshotHandle) bool {
	if handle == nil || h.activeWriters[handle.identity.Path] != 0 || h.activeWriterGen[handle.identity.Path] != handle.generation {
		return false
	}
	row, ok := h.store.LookupResidentSnapshotFile(handle.identity.Path)
	if !ok || row.Identity != handle.identity || row.Name != handle.snapshot.Name || row.Mode != handle.snapshot.Mode {
		return false
	}
	identity, ok := h.strongSidecarIdentity(handle.identity.Path)
	if !ok || identity != handle.identity { // a drain-tail high water revokes old EOF
		return false
	}
	volume, ok := h.snapshotVolume()
	if !ok || volume != handle.snapshot.Volume {
		return false
	}
	h.sidecar.mu.Lock()
	live := h.sidecar.m[handle.identity.Path] == handle.entry && handle.entry.snapshot != nil && *handle.entry.snapshot == handle.snapshot
	h.sidecar.mu.Unlock()
	return live
}

func (h *JuiceMountHandler) checkSidecarSnapshot(handle *sidecarSnapshotHandle) error {
	h.activeWritersMu.Lock()
	defer h.activeWritersMu.Unlock()
	if !h.validSidecarSnapshotLocked(handle) {
		return pin.ErrSpoolIncomplete
	}
	return nil
}

func (h *JuiceMountHandler) readSidecarSnapshot(handle *sidecarSnapshotHandle, p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, syscall.EINVAL
	}
	h.activeWritersMu.Lock()
	defer h.activeWritersMu.Unlock()
	if !h.validSidecarSnapshotLocked(handle) {
		return 0, pin.ErrSpoolIncomplete
	}
	n := 0
	if off < int64(len(handle.data)) {
		n = copy(p, handle.data[off:])
	}
	h.sidecar.mu.Lock()
	if h.sidecar.m[handle.identity.Path] == handle.entry {
		h.sidecar.tick++
		handle.entry.lastUsed = h.sidecar.tick
	}
	h.sidecar.mu.Unlock()
	if !h.validSidecarSnapshotLocked(handle) {
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

func (handle *sidecarSnapshotHandle) fileInfo() *snapshotFileInfo {
	return &snapshotFileInfo{
		name: handle.snapshot.Name, size: handle.snapshot.Size, mode: handle.snapshot.Mode,
		mtime: time.Unix(0, handle.snapshot.MtimeNS), inode: handle.snapshot.Inode,
	}
}

func (h *JuiceMountHandler) newSidecarSnapshot(proof *sidecarReadProof, body []byte) *sidecarSnapshot {
	if proof == nil || int64(len(body)) != proof.identity.Size {
		return nil
	}
	row, ok := h.store.LookupResidentSnapshotFile(proof.identity.Path)
	if !ok || row.Identity != proof.identity {
		return nil
	}
	volume, ok := h.snapshotVolume()
	if !ok || !proof.snapshotVolumeKnown || volume != proof.snapshotVolume {
		return nil
	}
	return &sidecarSnapshot{
		Token: proof.identity.Version, Inode: proof.identity.Inode, Size: proof.identity.Size,
		MtimeNS: proof.identity.MtimeNS, Name: row.Name, Mode: row.Mode,
		SHA256: sha256.Sum256(body), Volume: volume,
	}
}

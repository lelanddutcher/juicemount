package nfs

import (
	"io"
	"math"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
)

func (f *cachedFile) ownedPinCheck(token *cache.ResidentToken) error {
	if f.handler == nil || f.cacheReader == nil || f.cacheReader != f.handler.cacheReader.Load() {
		return pin.ErrOfflineNotAvailable
	}
	ps := f.handler.pinStore.Load()
	if ps == nil {
		return pin.ErrOfflineNotAvailable
	}
	entry, ok := ps.ReadyAdmission(f.handler.canonicalize(f.name))
	if !ok || entry.Generation != f.pinGeneration || entry.Size != f.fileSize {
		return pin.ErrOfflineNotAvailable
	}
	if err := f.handler.namespace.servingError(f.name); err != nil {
		return err
	}
	if live, found := f.handler.store.LookupCachedFileIdentity(f.name); !found || live != f.cacheIdentity {
		return syscall.EAGAIN
	}
	if token != nil && !token.Valid() {
		return pin.ErrOfflineNotAvailable
	}
	return nil
}

func (f *cachedFile) ownedPinSize() (int64, error) {
	if err := f.ownedPinCheck(nil); err != nil {
		return 0, err
	}
	id := f.cacheIdentity
	token, err := f.cacheReader.ResidentEOF(id.Inode, cache.ResidentAttr{Size: id.Size, MtimeNS: id.MtimeNS, Valid: true})
	if err != nil {
		return 0, pin.ErrOfflineNotAvailable
	}
	if err := f.ownedPinCheck(&token); err != nil {
		return 0, err
	}
	return id.Size, nil
}

func (f *cachedFile) prepareOwnedPinResidentRead(offset uint64, count uint32) (*nfslib.ResidentRead, error) {
	if count == 0 {
		return nil, nil
	}
	if offset > math.MaxInt64 {
		return nil, syscall.EINVAL
	}
	size, err := f.ownedPinSize()
	if err != nil {
		return nil, err
	}
	request := uint64(count)
	if request > nfslib.MaxRead {
		request = nfslib.MaxRead
	}
	if offset < uint64(size) && request <= uint64(size)-offset {
		return nil, nil // ReadAt still uses the owned local-only route.
	}
	if offset >= uint64(size) {
		token, err := f.cacheReader.ResidentEOF(f.inode, cache.ResidentAttr{Size: size, MtimeNS: f.cacheIdentity.MtimeNS, Valid: true})
		if err != nil {
			return nil, pin.ErrOfflineNotAvailable
		}
		if err := f.ownedPinCheck(&token); err != nil {
			return nil, err
		}
		return &nfslib.ResidentRead{Size: size, Check: func() error { return f.ownedPinCheck(&token) }}, nil
	}
	length := int(uint64(size) - offset)
	data := make([]byte, length)
	token, err := f.cacheReader.ReadResidentRange(f.inode, int64(offset), data, cache.ResidentAttr{Size: size, MtimeNS: f.cacheIdentity.MtimeNS, Valid: true})
	if err != nil {
		return nil, pin.ErrOfflineNotAvailable
	}
	if err := f.ownedPinCheck(&token); err != nil {
		return nil, err
	}
	metrics.Default().ObserveDirectSSDCacheHit(int64(length))
	metrics.Default().AddBytesRead(int64(length))
	return &nfslib.ResidentRead{Data: data, Size: size, Check: func() error { return f.ownedPinCheck(&token) }}, nil
}

// readOwnedPinResident is the only content route for a handle admitted by an
// owned pin witness. CRC/authority loss returns unavailable; it never retries
// through FUSE or manufactures an EOF from an old size.
func (f *cachedFile) readOwnedPinResident(dst []byte, off int64, trace *cachedReadTrace) (int, error) {
	start := time.Now()
	if trace != nil {
		trace.FinalRoute = "owned_pin_unavailable"
		trace.Reader.Outcome = "local_resident_unavailable"
		defer func() { trace.CacheReadNS += time.Since(start).Nanoseconds() }()
	}
	if off < 0 {
		return 0, syscall.EINVAL
	}
	if len(dst) == 0 {
		return 0, nil
	}
	ps := f.handler.pinStore.Load()
	if ps == nil || f.cacheReader == nil || f.cacheReader != f.handler.cacheReader.Load() || !cacheReaderServeEnabled {
		return 0, pin.ErrOfflineNotAvailable
	}
	canonical := f.handler.canonicalize(f.name)
	entry, ok := ps.ReadyAdmission(canonical)
	if !ok || entry.Generation != f.pinGeneration || entry.Size != f.fileSize {
		return 0, pin.ErrOfflineNotAvailable
	}
	id := f.cacheIdentity
	if id.Inode != f.inode || id.Size != f.fileSize || id.Path != f.name {
		return 0, syscall.EAGAIN
	}
	if live, found := f.handler.store.LookupCachedFileIdentity(f.name); !found || live != id {
		return 0, syscall.EAGAIN
	}
	expected := cache.ResidentAttr{Size: id.Size, MtimeNS: id.MtimeNS, Valid: true}
	if off >= id.Size {
		token, err := f.cacheReader.ResidentEOF(id.Inode, expected)
		if err != nil || !token.Valid() {
			return 0, pin.ErrOfflineNotAvailable
		}
		if current, ready := ps.ReadyAdmission(canonical); !ready || current.Generation != entry.Generation || !token.Valid() {
			return 0, pin.ErrOfflineNotAvailable
		}
		if trace != nil {
			trace.FinalRoute, trace.Reader.Outcome = "owned_pin_resident_eof", "local_resident"
		}
		return 0, io.EOF
	}
	n := len(dst)
	if int64(n) > id.Size-off {
		n = int(id.Size - off)
	}
	token, err := f.cacheReader.ReadResidentRange(id.Inode, off, dst[:n], expected)
	if err != nil || !token.Valid() {
		clear(dst)
		return 0, pin.ErrOfflineNotAvailable
	}
	if current, ready := ps.ReadyAdmission(canonical); !ready || current.Generation != entry.Generation || !token.Valid() {
		clear(dst)
		return 0, pin.ErrOfflineNotAvailable
	}
	if live, found := f.handler.store.LookupCachedFileIdentity(f.name); !found || live != id {
		clear(dst)
		return 0, syscall.EAGAIN
	}
	metrics.Default().ObserveDirectSSDCacheHit(int64(n))
	metrics.Default().AddBytesRead(int64(n))
	if trace != nil {
		trace.FinalRoute, trace.Reader.Outcome = "owned_pin_resident", "local_resident"
	}
	if n < len(dst) {
		return n, io.EOF
	}
	return n, nil
}

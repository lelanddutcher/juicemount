package nfs

import (
	"errors"
	"math"
	"syscall"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
)

// PrepareResidentRead is deliberately narrow: only a bound file's positive-count
// request crossing or starting at/past the locally observed EOF can bypass
// backing reads. This includes Finder metadata only with the same complete
// CRC-backed range and authority checks; dedicated sidecar proofs take priority.
// The Redis inode attr and slice mapping were fetched in one operation and both
// must match the retained mirror identity. This is a local snapshot boundary,
// not a guarantee that an unseen remote writer has not changed the server.
//
// Every resident route answers the READ without reaching ReadAt, so the
// in-place hole guard runs here first, ahead of the owned-pin route too (B-9):
// a request whose resident answer would start in, or run into, a tracked
// in-place hole is refused (hold) or sent back to the ordinary ReadAt path,
// which clamps it at the hole. The returned Check repeats the hold test.
func (f *cachedFile) PrepareResidentRead(offset uint64, count uint32) (*nfslib.ResidentRead, error) {
	if f != nil && f.handler != nil && count > 0 && offset <= math.MaxInt64 {
		limit, hold := f.handler.inPlaceHoles.inPlaceReadBound(f.name, int64(offset), int(nfslib.MaxRead))
		if hold {
			f.handler.inPlaceHoles.logHold(f.name, int64(offset))
			return nil, pin.ErrSpoolIncomplete
		}
		if limit < int(nfslib.MaxRead) {
			return nil, nil
		}
	}
	r, err := f.prepareResidentRoute(offset, count)
	if r != nil && err == nil && r.Check != nil {
		inner := r.Check
		r.Check = func() error {
			if plan, tracked := f.handler.inPlaceHoles.inPlaceReadPlan(f.name, int64(offset)); tracked && plan == planHold {
				return pin.ErrSpoolIncomplete
			}
			return inner()
		}
	}
	return r, err
}

func (f *cachedFile) prepareResidentRoute(offset uint64, count uint32) (*nfslib.ResidentRead, error) {
	if f != nil && f.pinGeneration != "" {
		return f.prepareOwnedPinResidentRead(offset, count)
	}
	if f == nil || f.handler == nil || f.cacheReader == nil || !cacheReaderServeEnabled ||
		f.cacheIdentityConflict || f.snapshotProof != nil || f.sidecarProof != nil ||
		count == 0 ||
		offset > math.MaxInt64 || f.cacheIdentity.Path != f.name ||
		f.cacheIdentity.Inode == 0 || f.cacheIdentity.Inode != f.inode || f.cacheIdentity.Size < 0 {
		return nil, nil
	}
	if err := f.handler.cacheBackendError(); err != nil {
		return nil, err
	}
	if err := f.handler.namespace.servingError(f.name); err != nil {
		return nil, err
	}
	identity := f.cacheIdentity
	request := uint64(count)
	if request > nfslib.MaxRead {
		request = nfslib.MaxRead
	}
	if offset < uint64(identity.Size) && request <= uint64(identity.Size)-offset {
		return nil, nil // ordinary in-range reads retain their existing path
	}
	readerVolume, bound := f.cacheReader.VolumeBinding()
	selectedVolume, selected := f.handler.snapshotVolume()
	b := f.handler.cacheBinding.Load()
	if !bound || !selected || readerVolume != selectedVolume || b == nil || b.paths == nil {
		return nil, nil
	}
	pathBinding := metadata.CachePathBinding{Digest: readerVolume.Backend, FormatUUID: readerVolume.UUID}
	f.handler.activeWritersMu.Lock()
	writerGeneration := f.handler.activeWriterGen[f.name]
	active := f.handler.activeWriters[f.name] != 0
	f.handler.writeSizeMu.Lock()
	writeHigh, hasWriteHigh := f.handler.writeSizes[f.name]
	f.handler.writeSizeMu.Unlock()
	f.handler.activeWritersMu.Unlock()
	if active || hasWriteHigh && writeHigh > identity.Size ||
		!b.paths.CachePathEligible(pathBinding, identity) {
		return nil, nil
	}
	if live, ok := f.handler.store.LookupCachedFileIdentity(f.name); !ok || live != identity {
		return nil, syscall.EAGAIN
	}
	check := func(token *cache.ResidentToken) error {
		if err := f.handler.namespace.servingError(f.name); err != nil {
			return err
		}
		if err := f.handler.cacheBackendError(); err != nil {
			return err
		}
		volume, ok := f.handler.snapshotVolume()
		if !ok || volume != readerVolume {
			return pin.ErrSpoolIncomplete
		}
		f.handler.activeWritersMu.Lock()
		writerChanged := f.handler.activeWriters[f.name] != 0 || f.handler.activeWriterGen[f.name] != writerGeneration
		f.handler.writeSizeMu.Lock()
		writeHigh, hasWriteHigh := f.handler.writeSizes[f.name]
		f.handler.writeSizeMu.Unlock()
		f.handler.activeWritersMu.Unlock()
		if writerChanged || hasWriteHigh && writeHigh > identity.Size {
			return pin.ErrSpoolIncomplete
		}
		if live, ok := f.handler.store.LookupCachedFileIdentity(f.name); !ok || live != identity ||
			!b.paths.CachePathEligible(pathBinding, identity) || token != nil && !token.Valid() {
			return pin.ErrSpoolIncomplete
		}
		return nil
	}
	if err := check(nil); err != nil {
		return nil, err
	}
	witness := cache.ResidentAttr{Size: identity.Size, MtimeNS: identity.MtimeNS, Valid: true}
	if offset >= uint64(identity.Size) {
		token, err := f.cacheReader.ResidentEOF(f.inode, witness)
		if err != nil {
			if checkErr := check(nil); checkErr != nil {
				return nil, checkErr
			}
			return nil, nil
		}
		if err := check(&token); err != nil {
			return nil, err
		}
		return &nfslib.ResidentRead{Size: identity.Size, Check: func() error { return check(&token) }}, nil
	}
	length := int(uint64(identity.Size) - offset)
	data := make([]byte, length)
	token, err := f.cacheReader.ReadResidentRange(f.inode, int64(offset), data, witness)
	if err != nil {
		if checkErr := check(nil); checkErr != nil {
			return nil, checkErr
		}
		if errors.Is(err, cache.ErrVolumeBinding) {
			return nil, err
		}
		return nil, nil // a missing/corrupt range keeps the established fallback
	}
	if err := check(&token); err != nil {
		return nil, err
	}
	metrics.Default().ObserveDirectSSDCacheHit(int64(len(data)))
	metrics.Default().AddBytesRead(int64(len(data)))
	return &nfslib.ResidentRead{Data: data, Size: identity.Size, Check: func() error { return check(&token) }}, nil
}

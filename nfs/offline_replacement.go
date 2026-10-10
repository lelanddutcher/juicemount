package nfs

import (
	"errors"
	"fmt"
	"syscall"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

// acceptOfflineReplacement owns the complete local save boundary. It never
// cancels a destination or touches FUSE. Accepted images remain queued until
// conditional remote publication can compare the persisted original graph.
func (jfs *juiceFS) acceptOfflineReplacement(source, destination *metadata.Entry, op *namespaceOperation) (bool, error) {
	h := jfs.handler
	s := h.spool.Load()
	if s == nil || !pin.IsOffline() || source == nil || destination == nil || source.Path == destination.Path || !source.LocalOnly || source.IsDir || source.Mode.Type() != 0 || destination.IsDir || destination.Mode.Type() != 0 {
		return false, nil
	}
	refuse := func(err error) (bool, error) { return true, fmt.Errorf("offline replacement: %w", err) }
	b := h.cacheBinding.Load()
	if b == nil || b.session == nil || b.paths == nil || b.identityRefused.Load() {
		return refuse(syscall.EAGAIN)
	}
	binding, ok := b.session.SnapshotBinding()
	if !ok {
		return refuse(syscall.EAGAIN)
	}
	w := metadata.SpoolReplacementWitness{Version: 1, Source: source.Version, CurrentDestination: destination.Version, Binding: metadata.CachePathBinding{Digest: binding.Backend, FormatUUID: binding.UUID}}
	var graph cache.ContentGraphExport
	var prior *metadata.SpoolRow
	var err error
	if destination.LocalOnly {
		prior, err = s.meta.LookupByPath(destination.Path)
		if err != nil {
			return refuse(err)
		}
		witness, err := s.meta.ReplacementWitness(prior)
		if err != nil {
			return refuse(err)
		}
		if witness == nil || witness.Binding != w.Binding {
			return refuse(syscall.EAGAIN)
		}
		w.PredecessorRowID = prior.ID
		w.Destination, w.Graph, w.GraphDigest, w.AcceptedEpoch = witness.Destination, witness.Graph, witness.GraphDigest, witness.AcceptedEpoch
	} else {
		reader := h.cacheReader.Load()
		if reader == nil {
			return refuse(syscall.EAGAIN)
		}
		selected, ok := reader.VolumeBinding()
		if !ok || selected != binding {
			return refuse(syscall.EAGAIN)
		}
		identity, ok := h.store.LookupCachedFileIdentity(destination.Path)
		if !ok || identity.Version != destination.Version {
			return refuse(syscall.EAGAIN)
		}
		w.Destination, ok = b.paths.SnapshotEligibleCachePath(w.Binding, identity)
		if !ok {
			return refuse(syscall.EAGAIN)
		}
		graph, err = reader.ExportContentGraph(identity.Inode, cache.ResidentAttr{Size: identity.Size, MtimeNS: identity.MtimeNS, Valid: true})
		if err != nil {
			return refuse(err)
		}
		w.Graph, w.GraphDigest = graph.Canonical(), graph.Digest
	}
	w.SourceParents, err = h.store.CaptureSpoolRenameParents(source.Path)
	if err != nil {
		return refuse(err)
	}
	w.OperationID, err = metadata.NewSpoolCustodyID()
	if err != nil {
		return refuse(err)
	}
	s.lockAllShards()
	defer s.unlockAllShards()
	e, ok := s.index.Lookup(source.Path)
	if !ok {
		return refuse(syscall.EAGAIN)
	}
	if !e.streamAdmission.TryLock() {
		return refuse(syscall.EAGAIN)
	}
	defer e.streamAdmission.Unlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.retired.Load() || e.readBlocked.Load() || e.replacement != nil || e.refcount != 0 || e.punchedEnd != 0 || e.streamDest != "" || e.contiguousEnd != e.writtenEnd || s.hasPendingBoundContinuation(e.id) || e.nfsPath != source.Path {
		return refuse(syscall.EAGAIN)
	}
	var predecessor *SpoolEntry
	if prior != nil {
		predecessor, ok = s.index.Lookup(destination.Path)
		if !ok || predecessor == e {
			return refuse(syscall.EAGAIN)
		}
		predecessor.mu.Lock()
		defer predecessor.mu.Unlock()
		if predecessor.id != prior.ID || predecessor.replacement == nil || !predecessor.closed || predecessor.refcount != 0 || predecessor.retired.Load() {
			return refuse(syscall.EAGAIN)
		}
	}
	image, err := s.openReplacementImage(e.spoolFile, e.file)
	if err != nil {
		return refuse(err)
	}
	defer image.Close()
	w.Image, err = image.Seal()
	if err != nil {
		return refuse(err)
	}
	if w.Image.Size != e.writtenEnd {
		return refuse(syscall.EAGAIN)
	}
	w.SourceRowID, w.SourceFile, w.SourceIdentity = e.id, e.spoolFile, image.identity
	_, w.SourceEpoch, err = s.meta.DrainBase(e.id)
	if err != nil {
		return refuse(err)
	}
	if prior == nil && projectOf(source.Path) == projectOf(destination.Path) {
		w.AcceptedEpoch = w.SourceEpoch
	}
	verify := func() error {
		selected, ok := b.session.SnapshotBinding()
		if h.cacheBinding.Load() != b || b.identityRefused.Load() || !ok || selected != binding {
			return metadata.ErrMirrorConflict
		}
		if prior == nil && !graph.Token.Valid() {
			return metadata.ErrMirrorConflict
		}
		return image.VerifyImage(w.Image)
	}
	_, err = h.store.AcceptSpoolReplacement(w, verify)
	if err != nil {
		var pending *metadata.PendingSpoolReplacement
		if errors.As(err, &pending) {
			op.poison = true
			op.blockReads()
			e.closed = true
			e.writing.Store(false)
			e.readBlocked.Store(true)
			s.retainPendingBoundContinuation(e.id, source.Path, e.spoolFile, image.file)
			s.pendingContinuationMu.Lock()
			s.pendingContinuationPaths[destination.Path] = e.id
			s.pendingContinuationMu.Unlock()
			// The pending registry retains the original FD; do not close it here.
			image.ownedFile = false
			e.file = nil
		}
		return refuse(err)
	}
	op.mutated = true
	if e.file != nil {
		_ = e.file.Close()
		e.file = nil
	}
	e.closed = true
	e.writing.Store(false)
	e.sha256 = append([]byte(nil), w.Image.SHA256...)
	e.writtenEnd, e.contiguousEnd = w.Image.Size, w.Image.Size
	e.replacement = &w
	e.nfsPath = destination.Path
	if predecessor != nil {
		predecessor.retired.Store(true)
	}
	if !s.index.Move(source.Path, destination.Path, e) {
		op.poison = true
		op.blockReads()
		return refuse(metadata.ErrMirrorConflict)
	}
	h.invalidateReadCaches(source.Path)
	h.invalidateReadCaches(destination.Path)
	h.invalidatePooledFDs(source.Path)
	h.invalidatePooledFDs(destination.Path)
	h.clearWriteSize(source.Path)
	h.clearWriteSize(destination.Path)
	// Local acceptance is not a remote namespace change. Peers must not see
	// a rename event until a conditional remote publication has committed.
	return true, nil
}

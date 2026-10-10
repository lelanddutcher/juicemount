package nfs

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"syscall"

	"github.com/lelanddutcher/juicemount/metadata"
)

// boundDrainEffects keeps the same P41 registry lease across P40 postcommit
// effects and retirement. Stop cannot finish in a transfer gap.
type boundDrainEffects struct {
	lease                     *boundFDPhase
	spool                     *SpoolStore
	receipt                   metadata.BoundDrainCommitResult
	proof                     metadata.BoundEffectsProof
	applied                   bool
	retirement                *metadata.BoundRetirementCustody
	testBeforeCacheInvalidate func()
}

func (l *boundFDPhase) claimEffects(spool *SpoolStore) (*boundDrainEffects, error) {
	if spool == nil {
		return nil, ErrBoundFDPhase
	}
	l.registry.mu.Lock()
	defer l.registry.mu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || l.effectsClaimed || !l.batch || l.validated == nil || l.src == nil || l.registry.byPath[l.obs.Path] != l {
		return nil, ErrBoundFDPhase
	}
	l.effectsClaimed = true
	return &boundDrainEffects{lease: l, spool: spool, receipt: *l.validated}, nil
}

func (e *boundDrainEffects) sourceImage() ([32]byte, []byte, error) {
	var zero [32]byte
	r := e.receipt
	fi, err := e.lease.src.Stat()
	if err != nil || !fi.Mode().IsRegular() || fi.Size() != r.FullSize {
		return zero, nil, ErrBoundFDPhase
	}
	id, _, _, err := fdPhaseIdentity(e.lease.src)
	if err != nil || id != r.FileIdentity {
		return zero, nil, ErrBoundFDPhase
	}
	pathID, pathSize, err := metadataPrivateFileState(r.SpoolFile)
	if err != nil || pathID != r.FileIdentity || pathSize != r.FullSize {
		return zero, nil, ErrBoundFDPhase
	}
	var body []byte
	if cacheableMetaName(path.Base(r.Path)) && r.FullSize > 0 && r.FullSize <= sidecarMaxFile {
		body = make([]byte, r.FullSize)
		if n, err := e.lease.src.ReadAt(body, 0); int64(n) != r.FullSize || (err != nil && !errors.Is(err, io.EOF)) {
			return zero, nil, ErrBoundFDPhase
		}
	}
	h := sha256.New()
	if n, err := io.Copy(h, io.NewSectionReader(e.lease.src, 0, r.FullSize)); err != nil || n != r.FullSize {
		return zero, nil, ErrBoundFDPhase
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest, body, nil
}

func metadataPrivateFileState(name string) (string, int64, error) {
	fi, err := os.Lstat(name)
	if err != nil || !fi.Mode().IsRegular() {
		return "", 0, ErrBoundFDPhase
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return "", 0, ErrBoundFDPhase
	}
	return fmt.Sprintf("%d:%d", uint64(st.Dev), st.Ino), fi.Size(), nil
}

// apply persists the exact bounded body and reconcile hint before local cache
// invalidations. All source hashing happens outside Store/SQL locks.
func (e *boundDrainEffects) apply(h *JuiceMountHandler, publish func(metadata.MetadataEvent) error) error {
	if e == nil || e.applied {
		return ErrBoundFDPhase
	}
	r := e.receipt
	var oldMem *memBufEntry
	var oldSidecar *sidecarEntry
	if h != nil {
		if h.memBuf != nil {
			oldMem = h.memBuf.captureEntry(r.Path)
		}
		if h.sidecar != nil {
			oldSidecar = h.sidecar.captureEntry(r.Path)
		}
	}
	proof, err := e.lease.store.CaptureBoundEffects(r)
	if err != nil {
		return err
	}
	hash, body, err := e.sourceImage()
	if err != nil || hash != proof.SourceSHA256 {
		return ErrBoundFDPhase
	}
	if _, err = e.lease.store.CaptureBoundEffects(r); err != nil {
		return err
	}
	isSidecar := body != nil
	if err = e.lease.store.SettleBoundEffects(r, hash, body, isSidecar); err != nil {
		return err
	}
	// A drifted path cannot select the inode or a successor's buffer. Slice
	// invalidation always targets the captured predecessor inode.
	if h != nil {
		if cr := h.cacheReader.Load(); cr != nil && r.OriginalInode != 0 {
			cr.InvalidateSliceCache(r.OriginalInode)
		}
		if e.testBeforeCacheInvalidate != nil {
			e.testBeforeCacheInvalidate()
		}
		if h.memBuf != nil {
			h.memBuf.invalidateIfEntry(r.Path, oldMem)
		}
		if h.sidecar != nil {
			h.sidecar.invalidateIfEntry(r.Path, oldSidecar)
		}
	}
	// The SQL effects row retains reconcile_pending even when this publish
	// succeeds; delivery is not presumed. A later cohort can replay it.
	if publish != nil {
		_ = publish(metadata.MetadataEvent{Op: "reconcile", Path: r.Path})
	}
	e.proof = proof
	e.applied = true
	return nil
}

// retire records local settlement only. The retained image, intent, registry
// lease, source descriptor, FULL hold, binding and capacity remain fenced
// until a future qualified publication receipt can authorize retirement.
func (e *boundDrainEffects) retire() error {
	if e == nil || !e.applied {
		return ErrBoundFDPhase
	}
	r := e.receipt
	shard := e.spool.pathShard(r.Path)
	shard.Lock()
	defer shard.Unlock()
	if _, err := e.lease.store.CaptureBoundEffects(r); err != nil {
		return err
	}
	hash, _, err := e.sourceImage()
	if err != nil || hash != e.proof.SourceSHA256 {
		return &metadata.PendingBoundEffects{SpoolID: r.SpoolID, Cause: ErrBoundFDPhase}
	}
	if e.retirement == nil {
		w, err := metadata.PrepareBoundRetirement(r, e.lease.src, e.spool.root)
		if err != nil {
			return &metadata.PendingBoundEffects{SpoolID: r.SpoolID, Cause: err}
		}
		e.retirement = w
	}
	if err := e.lease.store.RetireBoundEffects(r, e.retirement); err != nil {
		return err
	}
	return &metadata.PendingBoundEffects{SpoolID: r.SpoolID, Cause: metadata.ErrBoundPublicationPending}
}

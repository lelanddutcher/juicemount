package nfs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/metadata"
)

// cacheBinding keeps backend identity separate from ordinary offline mode.
// Per-path provenance remains conditional on the current mirror ancestry;
// the cache volume record never labels every row in a legacy database.
type cacheBinding struct {
	session             *cache.Session
	rc                  *metadata.RedisClient
	paths               *metadata.CachePathEligibility
	identityRefused     atomic.Bool
	readProof           func(context.Context, string, string, []uint64) (metadata.RedisPathProof, error)
	beforeCacheRead     func()
	workMu              sync.Mutex
	workCtx             context.Context
	workCancel          context.CancelFunc
	workDone            chan struct{}
	workActive          int
	qualificationActive int // bounded foreground proof slots; owned pins do not consume them
	workClosed          bool
	proofMu             sync.Mutex
	proofFlights        []*cacheProofFlight // at most the four admitted qualifiers
	testProofFlightJoin func()              // test-owned rendezvous; nil in production
}

// SetBoundCacheSession must precede listener publication. Reader publication
// remains separate because an offline saved reader exists before qualification
// and a cold online reader can appear after startup.
func (h *JuiceMountHandler) SetBoundCacheSession(s *cache.Session, rc *metadata.RedisClient, paths *metadata.CachePathEligibility) {
	b := &cacheBinding{session: s, rc: rc, paths: paths, workDone: make(chan struct{})}
	b.workCtx, b.workCancel = context.WithCancel(context.Background())
	if rc != nil {
		b.readProof = rc.ReadParentPathProof
	}
	h.cacheBinding.Store(b)
}

// SetCacheBackendStatus is callback-safe: it does no I/O and never enters the
// Session lock. A transport failure cannot clear a prior identity refusal.
func (h *JuiceMountHandler) SetCacheBackendStatus(qualified bool, err error) {
	if b := h.cacheBinding.Load(); b != nil {
		if qualified {
			b.identityRefused.Store(false)
		} else if errors.Is(err, cache.ErrVolumeBinding) {
			b.identityRefused.Store(true)
		}
	}
}

func (h *JuiceMountHandler) cacheBackendError() error {
	if h != nil {
		if b := h.cacheBinding.Load(); b != nil && b.identityRefused.Load() {
			return fmt.Errorf("%w: %w", syscall.EIO, cache.ErrVolumeBinding)
		}
	}
	return nil
}

func (b *cacheBinding) refuseIdentity(err error) {
	if errors.Is(err, cache.ErrVolumeBinding) {
		b.identityRefused.Store(true)
		b.session.RefuseIdentity(err)
	}
}

func (h *JuiceMountHandler) readCacheBlock(ctx context.Context, r *cache.Reader, current metadata.CachedFileIdentity, inode uint64, off int64, dst []byte, timeout time.Duration) (n int, err error) {
	return h.readCacheBlockTraced(ctx, r, current, inode, off, dst, timeout, nil)
}

func (h *JuiceMountHandler) readCacheBlockTraced(ctx context.Context, r *cache.Reader, current metadata.CachedFileIdentity, inode uint64, off int64, dst []byte, timeout time.Duration, trace *cachedReadTrace) (n int, err error) {
	binding, bound := r.VolumeBinding()
	if !bound {
		if trace != nil {
			start := time.Now()
			n, err = r.ReadBlockWithMappingTimeoutTrace(ctx, inode, off, dst, timeout, &trace.Reader)
			trace.CacheReadNS += time.Since(start).Nanoseconds()
			return n, err
		}
		return r.ReadBlockWithMappingTimeout(ctx, inode, off, dst, timeout)
	}
	if h == nil {
		return 0, cache.ErrCacheMiss
	}
	b := h.cacheBinding.Load()
	if b == nil || b.paths == nil || b.session == nil || current.Inode == 0 {
		return 0, cache.ErrCacheMiss
	}
	if current.Inode != inode {
		return 0, syscall.EAGAIN
	}
	selected := metadata.CachePathBinding{Digest: binding.Backend, FormatUUID: binding.UUID}
	initiallyEligible := b.paths.CachePathEligible(selected, current)
	if trace != nil {
		trace.CachePathInitiallyEligible = initiallyEligible
	}
	var observed metadata.CachePathObservation
	// Every refusal after external work must preserve a local conflict; a miss
	// cannot turn a retained handle into permission to read its successor.
	defer func() {
		live, ok := h.store.LookupCachedFileIdentity(current.Path)
		if !ok || live != current || (len(observed.Chain) != 0 && !b.paths.CachePathObservationCurrent(observed)) || (initiallyEligible && !b.paths.CachePathEligible(selected, current)) {
			clear(dst)
			n = 0
			if !errors.Is(err, cache.ErrVolumeBinding) {
				err = syscall.EAGAIN
			}
		}
	}()
	if live, ok := h.store.LookupCachedFileIdentity(current.Path); !ok || live != current {
		return 0, syscall.EAGAIN
	}
	if !initiallyEligible {
		epoch, ready := b.session.RemoteObservation()
		if !ready || b.readProof == nil {
			return 0, cache.ErrCacheMiss
		}
		if timeout <= 0 {
			timeout = 2 * time.Second
		}
		proofCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		qualificationStart := time.Time{}
		if trace != nil {
			qualificationStart = time.Now()
		}
		observed, err = b.qualifyPath(proofCtx, selected, current, epoch)
		if trace != nil {
			trace.QualificationNS += time.Since(qualificationStart).Nanoseconds()
		}
		if err != nil {
			return 0, err
		}

	}
	// Do not compare the remote epoch for a qualified local hit. Suspension
	// preserves local maps and bytes; only changed local ancestry invalidates it.
	buf := offlineReadBuf(len(dst))
	defer releaseOfflineReadBuf(buf, true)
	if b.beforeCacheRead != nil {
		b.beforeCacheRead()
	}
	cacheStart := time.Time{}
	if trace != nil {
		cacheStart = time.Now()
	}
	if trace != nil {
		n, err = r.ReadBlockWithMappingTimeoutTrace(ctx, inode, off, buf, timeout, &trace.Reader)
		trace.CacheReadNS += time.Since(cacheStart).Nanoseconds()
	} else {
		n, err = r.ReadBlockWithMappingTimeout(ctx, inode, off, buf, timeout)
	}
	if errors.Is(err, cache.ErrVolumeBinding) {
		b.refuseIdentity(err)
		return 0, err
	}
	if err != nil {
		return 0, err
	}
	if !b.paths.CachePathEligible(selected, current) {
		return 0, syscall.EAGAIN
	}
	copy(dst, buf[:n])
	return n, nil
}

// beginBoundCacheStop closes admission before any dependency can be retired.
// A timeout in StopHandler retains the handler and all dependencies for retry.
func (h *JuiceMountHandler) beginBoundCacheStop() <-chan struct{} {
	b := h.cacheBinding.Load()
	if b == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	b.workMu.Lock()
	if !b.workClosed {
		b.workClosed = true
		b.workCancel()
		if b.workActive == 0 {
			close(b.workDone)
		}
	}
	b.workMu.Unlock()
	return b.workDone
}

type cacheQualificationResult struct {
	observed metadata.CachePathObservation
	err      error
}

// qualifyPath caps actual outstanding work, including jobs whose caller already
// timed out. Deadline means retry; it never grants backing access. The handler
// retains and joins SQL commit/rollback even if the driver outlasts cancellation.
func (b *cacheBinding) qualifyPath(ctx context.Context, selected metadata.CachePathBinding, current metadata.CachedFileIdentity, epoch uint64) (metadata.CachePathObservation, error) {
	b.workMu.Lock()
	if b.workClosed || b.qualificationActive >= 4 {
		b.workMu.Unlock()
		return metadata.CachePathObservation{}, syscall.EAGAIN
	}
	b.workActive++
	b.qualificationActive++
	b.workMu.Unlock()
	jobCtx, cancel := context.WithCancel(ctx)
	stopCancel := context.AfterFunc(b.workCtx, cancel)
	result := make(chan cacheQualificationResult, 1)
	go func() {
		defer func() {
			stopCancel()
			cancel()
			b.workMu.Lock()
			b.workActive--
			b.qualificationActive--
			if b.workClosed && b.workActive == 0 {
				close(b.workDone)
			}
			b.workMu.Unlock()
		}()
		observed, err := b.qualifyPathSharedOwned(jobCtx, selected, current, epoch)
		result <- cacheQualificationResult{observed, err}
	}()
	select {
	case r := <-result:
		return r.observed, r.err
	case <-ctx.Done():
		return metadata.CachePathObservation{}, syscall.EAGAIN
	case <-b.workCtx.Done():
		return metadata.CachePathObservation{}, syscall.EAGAIN
	}
}

func (b *cacheBinding) qualifyPathOwned(ctx context.Context, selected metadata.CachePathBinding, current metadata.CachedFileIdentity, epoch uint64) (observed metadata.CachePathObservation, err error) {
	return b.qualifyPathWithProof(ctx, selected, current, epoch, false)
}

// Only foreground/preview qualifiers share the remote read. Owned pin jobs
// retain their original direct, cancellation-bound proof acquisition.
func (b *cacheBinding) qualifyPathSharedOwned(ctx context.Context, selected metadata.CachePathBinding, current metadata.CachedFileIdentity, epoch uint64) (observed metadata.CachePathObservation, err error) {
	return b.qualifyPathWithProof(ctx, selected, current, epoch, true)
}

func (b *cacheBinding) qualifyPathWithProof(ctx context.Context, selected metadata.CachePathBinding, current metadata.CachedFileIdentity, epoch uint64, share bool) (observed metadata.CachePathObservation, err error) {
	defer func() {
		if len(observed.Chain) != 0 && !b.paths.CachePathObservationCurrent(observed) && !errors.Is(err, cache.ErrVolumeBinding) {
			err = syscall.EAGAIN
		}
	}()
	observed, err = b.paths.CaptureCachePathContext(ctx, current.Path)
	if err != nil {
		if ctx.Err() != nil {
			return observed, syscall.EAGAIN
		}
		return observed, cache.ErrCacheMiss
	}
	var proof metadata.RedisPathProof
	if share {
		proof, err = b.sharedPathProof(ctx, selected, observed, epoch)
	} else {
		proof, err = b.readProof(ctx, selected.FormatUUID, current.Path, observed.ExpectedInodes())
	}
	if err != nil {
		if errors.Is(err, metadata.ErrReadProofVolume) {
			err = fmt.Errorf("%w: %w", cache.ErrVolumeBinding, err)
			b.refuseIdentity(err)
			return observed, err
		}
		// A shared flight can expire while this caller still has time left.
		// That is a retryable refusal, not permission to read the backing FUSE.
		if share && (errors.Is(err, syscall.EAGAIN) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return observed, syscall.EAGAIN
		}
		if ctx.Err() != nil {
			return observed, syscall.EAGAIN
		}
		return observed, cache.ErrCacheMiss
	}
	if !b.session.MatchesRemoteObservation(epoch) {
		return observed, syscall.EAGAIN
	}
	if err = b.paths.ConfirmCachePathContext(ctx, selected, observed, proof); err != nil {
		if ctx.Err() != nil {
			return observed, syscall.EAGAIN
		}
		return observed, cache.ErrCacheMiss
	}
	if !b.session.MatchesRemoteObservation(epoch) || !b.paths.CachePathEligible(selected, current) {
		return observed, syscall.EAGAIN
	}
	return observed, nil
}

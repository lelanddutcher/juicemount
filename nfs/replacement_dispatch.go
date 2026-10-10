package nfs

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

const replacementDispatchCooldown = 30 * time.Second

// Independent serial workers keep local cleanup moving while remote recovery
// waits for capacity. Remote phases share the existing media/slow-link envelope.
// The configured recovery worker starts after the handler wires its callback.
type replacementDispatch struct {
	enabled        bool
	notify         chan struct{}
	localNotify    chan struct{}
	done           chan struct{}
	localDone      chan struct{}
	cancel         context.CancelFunc
	resume         func(context.Context, int64) error // registered before Start
	beforeCapacity func(int64)                        // test barrier; registered before Start
	availabilityMu sync.Mutex
	unavailable    map[int64]replacementUnavailable
}

func (d *Drainer) startReplacementDispatch() {
	q := d.replacementDispatch
	if q == nil || q.resume == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	q.cancel = cancel
	q.done = make(chan struct{})
	q.localDone = make(chan struct{})
	if q.enabled {
		go func() { defer close(q.done); d.replacementDispatchLoop(ctx, false) }()
	} else {
		close(q.done)
	}
	go func() { defer close(q.localDone); d.replacementDispatchLoop(ctx, true) }()
}

func (d *Drainer) replacementDispatchLoop(ctx context.Context, local bool) {
	notify := d.replacementDispatch.notify
	if local {
		notify = d.replacementDispatch.localNotify
	}
	next := time.Now()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-notify:
			if time.Now().Before(next) {
				continue
			}
		case <-timer.C:
		}
		if ctx.Err() != nil {
			return
		}
		if err := d.replacementDispatchPass(ctx, local); err != nil && ctx.Err() == nil {
			log.Printf("drainer: replacement pass retained pending saves: %v", err)
		}
		// Even a wake storm cannot retry an unchanged HOLD immediately. A frozen
		// pass eventually revisits rows accepted below its previous cursor.
		next = time.Now().Add(replacementDispatchCooldown)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(time.Until(next))
	}
}

func (d *Drainer) replacementDispatchPass(ctx context.Context, local bool) error {
	through, err := d.spool.meta.ReplacementDispatchHighWater(ctx)
	if err != nil {
		return err
	}
	var after int64
	for after < through {
		ids, err := d.spool.meta.ReplacementDispatchPage(ctx, after, through, 64)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			after = id // malformed/held rows must not starve later candidates
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := d.dispatchReplacement(ctx, id, local); err != nil && ctx.Err() == nil {
				log.Printf("drainer: replacement row %d retained: %v", id, err)
			}
		}
	}
	return nil
}

func replacementLocalCleanup(phase string) bool {
	return phase == metadata.ReplacementPublicationSettled || phase == metadata.ReplacementPublicationCleaning
}

// Each role owns a bounded pass; neither claims a row while waiting for capacity.
// The coordinator remains the only row-claim owner when both observe settlement.
func (d *Drainer) dispatchReplacement(ctx context.Context, id int64, local bool) error {
	if !local {
		// A new remote attempt replaces an earlier transient diagnosis even if
		// custody, capacity, or connectivity prevents reaching the provider.
		d.replacementDispatch.clearUnavailable(id)
	}
	p, err := d.spool.meta.ReplacementDispatchCandidate(ctx, id)
	if err != nil {
		return err
	}
	if replacementLocalCleanup(p.Phase) != local {
		return nil
	}
	resume := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Recheck eligibility and role after admission. A row that settled while
		// waiting belongs to the local loop; release the remote slots unchanged.
		p, err := d.spool.meta.ReplacementDispatchCandidate(ctx, id)
		if err != nil {
			return err
		}
		if replacementLocalCleanup(p.Phase) != local {
			return nil
		}
		if !local && pin.IsOffline() {
			return metadata.ErrMirrorConflict
		}
		q := d.replacementDispatch
		err = q.resume(ctx, id)
		if !local && errors.Is(err, errReplacementCapabilityUnavailable) {
			// The coordinator may have moved queued to intent before the probe.
			// Bind the transient reason to the post-attempt journal identity.
			if post, readErr := d.spool.meta.ReplacementDispatchCandidate(ctx, id); readErr == nil {
				q.noteUnavailable(*post)
			}
		}
		return err
	}
	if local {
		return resume()
	}
	if pin.IsOffline() {
		return metadata.ErrMirrorConflict
	}
	if before := d.replacementDispatch.beforeCapacity; before != nil {
		before(id)
	}
	return d.withReplacementCapacity(ctx, resume)
}

// Queue one cancellable sender behind ordinary work instead of repeatedly
// polling a full slot. Progress requires current holders to finish: this does
// not interrupt a held filesystem call or increase either concurrency limit.
func (d *Drainer) withReplacementCapacity(ctx context.Context, run func() error) error {
	select {
	case d.sem <- struct{}{}:
		defer func() { <-d.sem }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if drainClassGateEnabled() && drainLinkIsSlow() {
		select {
		case d.slowGate <- struct{}{}:
			defer func() { <-d.slowGate }()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return run()
}

// resumeQueuedReplacement binds publication to the currently attached export.
// The injected factory in SDK acceptance changes only the storage adapter; this
// same session/reader/lease verifier and the real coordinator remain in use.
func (h *JuiceMountHandler) resumeQueuedReplacement(ctx context.Context, expected *Drainer, id int64, factory func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error)) error {
	d, s := h.drainer.Load(), h.spool.Load()
	if d == nil || d != expected || s == nil || d.spool != s || s.meta.Owner() != h.store {
		return metadata.ErrMirrorConflict
	}
	p, err := s.meta.ReplacementDispatchCandidate(ctx, id)
	if err != nil {
		return err
	}
	if replacementLocalCleanup(p.Phase) {
		return h.resumeReplacementAttached(ctx, d, id, nil, nil)
	}
	if p.Phase == metadata.ReplacementPublicationCommitted {
		if pin.IsOffline() {
			return metadata.ErrMirrorConflict
		}
		return h.resumeReplacementAttached(ctx, d, id, nil, nil)
	}
	b, r := h.cacheBinding.Load(), h.cacheReader.Load()
	leases := d.ProjectLeases()
	if b == nil || b.session == nil || r == nil || leases == nil || factory == nil {
		return metadata.ErrMirrorConflict
	}
	binding, valid := b.session.SnapshotBinding()
	epoch, ready := b.session.RemoteObservation()
	if !valid || !ready || r != b.session.Reader() {
		return metadata.ErrMirrorConflict
	}
	verify := func(want metadata.CachePathBinding) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if h.drainer.Load() != d || h.spool.Load() != s || h.cacheBinding.Load() != b || h.cacheReader.Load() != r ||
			d.ProjectLeases() != leases || h.cacheBackendError() != nil || pin.IsOffline() || !b.session.TryMatchRemoteObservation(epoch, binding, r) ||
			want != (metadata.CachePathBinding{Digest: binding.Backend, FormatUUID: binding.UUID}) {
			return metadata.ErrMirrorConflict
		}
		return nil
	}
	guarded := func(w metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
		if err := verify(w.Binding); err != nil {
			return nil, nil, err
		}
		backend, closeBackend, err := factory(w)
		if err != nil {
			return nil, nil, err
		}
		if backend == nil || closeBackend == nil {
			return nil, nil, metadata.ErrMirrorConflict
		}
		if err := verify(w.Binding); err != nil {
			closeBackend()
			return nil, nil, err
		}
		return backend, closeBackend, nil
	}
	return h.resumeReplacementAttached(ctx, d, id, guarded, verify)
}

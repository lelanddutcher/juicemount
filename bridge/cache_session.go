package main

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
	"github.com/redis/go-redis/v9"
)

type bridgeCacheStatus struct {
	qualified bool
	err       error
}

// This owner has no callback path through globalMu. Session callbacks are
// limited to atomic publication; all FUSE identity checks run in its joined
// recovery worker, outside the Session publication lock.
type bridgeCacheOwner struct {
	session           *cache.Session
	candidateHints    chan metadata.CacheCandidate
	handler           atomic.Pointer[jmnfs.JuiceMountHandler]
	rc                atomic.Pointer[metadata.RedisClient]
	status            atomic.Pointer[bridgeCacheStatus]
	ready             atomic.Bool
	publicationMu     sync.Mutex
	publicationEpoch  uint64
	publicationClosed bool
	linkMu            sync.Mutex
	linkClosed        bool
	linkWG            sync.WaitGroup
	reachMu           sync.Mutex
	reachClosed       bool
	reachWG           sync.WaitGroup
	recoveryCancel    context.CancelFunc
	recoveryWG        sync.WaitGroup
}

func newBridgeCacheOwner(store *metadata.Store, logicalURL string, selection []string, options *redis.Options, stateDir, binary string) (*bridgeCacheOwner, error) {
	o := &bridgeCacheOwner{candidateHints: make(chan metadata.CacheCandidate, 16)}
	var err error
	o.session, err = cache.NewSession(cache.SessionConfig{
		Store: store, LogicalURL: logicalURL, Selection: selection,
		Options: options, StateDir: stateDir, JuiceFSBin: binary,
		VerifyReader: func(r *cache.Reader) error {
			if binary == "" {
				return cache.ErrReaderUnavailable
			}
			return r.VerifyWithBinary(context.Background(), binary)
		},
		OnReader: func(r *cache.Reader) {
			if h := o.handler.Load(); h != nil {
				h.SetCacheReader(r)
			}
		},
		OnQualified: o.qualificationChanged,
		OnStatus: func(qualified bool, err error) {
			o.status.Store(&bridgeCacheStatus{qualified, err})
			if h := o.handler.Load(); h != nil {
				h.SetCacheBackendStatus(qualified, err)
			}
		},
	})
	return o, err
}

// offerCandidate runs after a fenced mirror mutation. It only retains a hint;
// a full channel sheds work rather than delaying reconciliation.
func (o *bridgeCacheOwner) offerCandidate(c metadata.CacheCandidate) {
	select {
	case o.candidateHints <- c:
	default:
	}
}

// Start only after spool recovery and all handler dependencies are attached.
// The channel buffered boot hints before the handler existed.
func (o *bridgeCacheOwner) startCandidatePreparation() {
	if h := o.handler.Load(); h != nil {
		h.StartMetadataPreparation(o.candidateHints)
	}
}

// Session invokes this under its publication lock. No owner publication
// method calls back into Session while holding publicationMu.
func (o *bridgeCacheOwner) qualificationChanged(qualified bool) {
	o.publicationMu.Lock()
	defer o.publicationMu.Unlock()
	if o.publicationClosed {
		qualified = false
	}
	o.publicationEpoch++
	o.ready.Store(qualified)
	if !qualified {
		pin.SetAutoOffline(true, "cache backend unqualified")
	}
	if rc := o.rc.Load(); rc != nil {
		rc.SetBackendQualified(qualified)
	}
}

func (o *bridgeCacheOwner) recoveryCandidate() (uint64, bool) {
	o.publicationMu.Lock()
	defer o.publicationMu.Unlock()
	return o.publicationEpoch, !o.publicationClosed && o.ready.Load()
}

func (o *bridgeCacheOwner) clearAutoOfflineIfCurrent(epoch uint64) bool {
	o.publicationMu.Lock()
	defer o.publicationMu.Unlock()
	if o.publicationClosed || !o.ready.Load() || o.publicationEpoch != epoch {
		return false
	}
	pin.SetAutoOffline(false, "")
	return true
}

func (o *bridgeCacheOwner) closePublication() {
	o.publicationMu.Lock()
	o.publicationClosed = true
	o.publicationEpoch++
	o.ready.Store(false)
	pin.SetAutoOffline(true, "cache session stopped")
	if rc := o.rc.Load(); rc != nil {
		rc.SetBackendQualified(false)
	}
	o.publicationMu.Unlock()
}

// The checks can involve Session and FUSE I/O; only the final flag write is
// serialized with qualification loss, transport reset and Stop.
func (o *bridgeCacheOwner) recoverIfSafe(reachable func() bool, fusePath string) bool {
	epoch, ready := o.recoveryCandidate()
	if !ready || !o.session.Qualified() || (reachable != nil && !reachable()) {
		return false
	}
	if fusePath != "" {
		if ok, _ := pin.FUSEIdentityState(); !ok {
			return false
		}
	}
	return o.clearAutoOfflineIfCurrent(epoch)
}

func (o *bridgeCacheOwner) attach(rc *metadata.RedisClient, h *jmnfs.JuiceMountHandler, paths *metadata.CachePathEligibility) {
	rc.SetBackendQualified(o.ready.Load())
	o.rc.Store(rc)
	h.SetBoundCacheSession(o.session, rc, paths)
	o.handler.Store(h)
	if status := o.status.Load(); status != nil {
		h.SetCacheBackendStatus(status.qualified, status.err)
	}
	if saved := o.session.Reader(); saved != nil {
		h.SetCacheReader(saved)
	}
}

func (o *bridgeCacheOwner) startRecovery(reachable func() bool, fusePath string) {
	ctx, cancel := context.WithCancel(context.Background())
	o.recoveryCancel = cancel
	o.recoveryWG.Add(1)
	go func() {
		defer o.recoveryWG.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			o.recoverIfSafe(reachable, fusePath)
		}
	}()
}

// Finish cache startup before the remount callback may publish the NFS
// listener. A healthy, already-qualified boot can lift its automatic offline
// fence now; deferred or changed qualifications keep the fence and rely on the
// joined recovery worker below. The selected owner's offline state is a
// separately fenced mirror, not the local read-admission gate.
func (o *bridgeCacheOwner) startBeforeListener(reachable func() bool, fusePath string, listenerReady *atomic.Bool) bool {
	o.startCandidatePreparation()
	o.session.Start()
	recovered := o.recoverIfSafe(reachable, fusePath)
	listenerReady.Store(true)
	return recovered
}

// The Link hook may fire while the qualifier's own Redis dial is active.
// Fence synchronously, then reset pools in a joined worker without blocking
// the proxy callback. A late captured hook after Stop is harmless.
func (o *bridgeCacheOwner) transportChanged(mode string, reachable func() bool) {
	o.linkMu.Lock()
	if o.linkClosed {
		o.linkMu.Unlock()
		return
	}
	o.session.SetOnline(false)
	o.linkWG.Add(1)
	o.linkMu.Unlock()
	go func() {
		defer o.linkWG.Done()
		netprofile.Default().ResetForTransportChange()
		jmlog.Info("network profile reset after Link transport change", "mode", mode)
		o.session.TransportChanged()
		if reachable == nil || reachable() {
			o.session.SetOnline(true)
		}
	}()
}

func (o *bridgeCacheOwner) reachChanged(reachable bool, reason string) {
	o.reachMu.Lock()
	if o.reachClosed {
		o.reachMu.Unlock()
		return
	}
	o.reachWG.Add(1)
	o.reachMu.Unlock()
	defer o.reachWG.Done()
	if reachable {
		o.session.SetOnline(true)
		o.session.Kick()
		if rc := o.rc.Load(); rc != nil {
			rc.TriggerSync()
		}
	} else {
		o.session.SetOnline(false)
		pin.SetAutoOffline(true, reason)
	}
}

func (o *bridgeCacheOwner) stop() {
	if o == nil {
		return
	}
	o.closePublication()
	o.linkMu.Lock()
	o.linkClosed = true
	o.linkMu.Unlock()
	o.reachMu.Lock()
	o.reachClosed = true
	o.reachMu.Unlock()
	o.reachWG.Wait()
	o.linkWG.Wait()
	if o.recoveryCancel != nil {
		o.recoveryCancel()
	}
	o.recoveryWG.Wait()
	o.session.Stop()
	o.handler.Store(nil)
	o.rc.Store(nil)
}

func cacheSessionStateDir(dbPath string) (string, error) {
	return filepath.Abs(filepath.Dir(dbPath))
}

package main

import (
	"context"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

// cliCacheNetworkOwner joins callbacks that can outlive Reachability.Stop or
// NetWatcher.Stop. The Session and Redis client remain live until it returns.
type cliCacheNetworkOwner struct {
	mu               sync.Mutex
	closed           bool
	wg               sync.WaitGroup
	publicationEpoch uint64
	qualified        bool
	session          *cache.Session
	rc               *metadata.RedisClient
	reach            *health.Reachability
	fusePath         string
	cancel           context.CancelFunc
	recoveryDone     chan struct{}
}

func newCLICacheNetworkOwner(s *cache.Session, rc *metadata.RedisClient) *cliCacheNetworkOwner {
	return &cliCacheNetworkOwner{session: s, rc: rc}
}

// Session calls this under its own publication lock. Owner methods must
// release mu before calling Session, so loss and a late recovery flag write
// share one linearization point without a reverse lock edge.
func (o *cliCacheNetworkOwner) qualificationChanged(qualified bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		qualified = false
	}
	o.publicationEpoch++
	o.qualified = qualified
	if !qualified {
		pin.SetAutoOffline(true, "cache backend unqualified")
	}
	if o.rc != nil {
		o.rc.SetBackendQualified(qualified)
	}
}

func (o *cliCacheNetworkOwner) closePublication() {
	o.mu.Lock()
	o.closed = true
	o.publicationEpoch++
	o.qualified = false
	pin.SetAutoOffline(true, "cache session stopped")
	if o.rc != nil {
		o.rc.SetBackendQualified(false)
	}
	o.mu.Unlock()
}

func (o *cliCacheNetworkOwner) recoveryCandidate() (uint64, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.publicationEpoch, !o.closed && o.qualified
}

func (o *cliCacheNetworkOwner) clearAutoOfflineIfCurrent(epoch uint64) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || !o.qualified || o.publicationEpoch != epoch {
		return false
	}
	pin.SetAutoOffline(false, "")
	return true
}

func (o *cliCacheNetworkOwner) recoverIfSafe(fusePath string) bool {
	epoch, qualified := o.recoveryCandidate()
	if !qualified || !o.session.Qualified() || (o.reach != nil && !o.reach.Reachable()) {
		return false
	}
	if fusePath != "" {
		if verified, _ := pin.FUSEIdentityState(); !verified {
			return false
		}
	}
	return o.clearAutoOfflineIfCurrent(epoch)
}

func (o *cliCacheNetworkOwner) callback(fn func()) {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return
	}
	o.wg.Add(1)
	o.mu.Unlock()
	defer o.wg.Done()
	fn()
}

func (o *cliCacheNetworkOwner) start(redisURL, fusePath string) {
	o.fusePath = fusePath
	o.session.Start()
	addr, _, _ := metadata.ParseRedisURL(redisURL)
	if addr != "" {
		o.reach = health.NewReachability(addr)
		o.reach.OnChange(func(up bool, reason string) {
			o.callback(func() {
				o.session.SetOnline(up)
				if up {
					o.session.Kick()
					o.rc.TriggerSync()
				} else {
					pin.SetAutoOffline(true, reason)
				}
			})
		})
		o.reach.Start()
	}
	ctx, cancel := context.WithCancel(context.Background())
	o.cancel = cancel
	o.recoveryDone = make(chan struct{})
	go func() {
		defer close(o.recoveryDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			o.recoverIfSafe(fusePath)
		}
	}()
}

func (o *cliCacheNetworkOwner) interfaceChanged() {
	o.callback(func() {
		o.session.SetOnline(false)
		o.session.TransportChanged()
		if o.reach == nil || o.reach.Reachable() {
			o.session.SetOnline(true)
		}
		if err := o.rc.Reconnect(); err == nil {
			o.rc.TriggerSync()
		}
	})
}

func (o *cliCacheNetworkOwner) stop() {
	o.closePublication()
	if o.reach != nil {
		o.reach.Stop()
	}
	o.wg.Wait()
	if o.cancel != nil {
		o.cancel()
	}
	if o.recoveryDone != nil {
		<-o.recoveryDone
	}
}

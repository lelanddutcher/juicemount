package nfs

import (
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
)

// One managed server can run 96 WRITEs. Admit at most the other 95 as
// continuation waiters behind an opener; the gate remains handler-global and
// bounded across server generations and direct callers sharing the handler.
const continuationWaiterCap = nfslib.DefaultRPCSemaphoreSize - nfslib.WriteRPCReserve - 1

// continuationGate serializes only qualified per-RPC spool write opens.
// Namespace ownership still decides whether a mutation may run.
type continuationGate struct {
	mu       sync.Mutex
	lanes    map[string]*continuationLane
	waiters  int
	inFlight int
	stopping bool
	changed  chan struct{}
}

type continuationLane struct {
	held  bool
	queue []*continuationWaiter
}

type continuationWaiter struct {
	ready   chan bool
	granted bool
}

func (g *continuationGate) signalLocked() {
	if g.changed != nil {
		close(g.changed)
	}
	g.changed = make(chan struct{})
}

func (g *continuationGate) release(path string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	lane := g.lanes[path]
	if lane == nil || !lane.held {
		panic("continuation gate: release without holder")
	}
	g.inFlight--
	if len(lane.queue) != 0 && !g.stopping {
		w := lane.queue[0]
		lane.queue = lane.queue[1:]
		g.waiters--
		w.granted = true
		w.ready <- true
	} else {
		delete(g.lanes, path)
	}
	g.signalLocked()
}

func (g *continuationGate) acquire(path string, deadline time.Time, onQueued func()) (func(), error) {
	g.mu.Lock()
	if g.stopping || time.Until(deadline) <= 0 {
		// Keep the original short-circuit priority: stop wins if both hold.
		if g.stopping {
			metrics.Default().IncAdmissionRefusal(metrics.AdmissionContinuationStopped)
		} else {
			metrics.Default().IncAdmissionRefusal(metrics.AdmissionContinuationEntryExpired)
		}
		g.mu.Unlock()
		return nil, errFUSETimeout
	}
	if g.lanes == nil {
		g.lanes = make(map[string]*continuationLane)
	}
	lane := g.lanes[path]
	if lane == nil {
		lane = &continuationLane{held: true}
		g.lanes[path] = lane
		g.inFlight++
		g.mu.Unlock()
		return func() { g.release(path) }, nil
	}
	if g.waiters >= continuationWaiterCap {
		metrics.Default().IncAdmissionRefusal(metrics.AdmissionContinuationWaiterCap)
		g.mu.Unlock()
		return nil, errFUSETimeout
	}
	w := &continuationWaiter{ready: make(chan bool, 1)}
	lane.queue = append(lane.queue, w)
	g.waiters++
	g.inFlight++
	g.signalLocked()
	g.mu.Unlock()
	if onQueued != nil {
		onQueued()
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	stopped := false
	select {
	case ok := <-w.ready:
		if ok && time.Until(deadline) > 0 {
			return func() { g.release(path) }, nil
		}
		stopped = !ok // only beginStop sends false
	case <-timer.C:
	}
	g.mu.Lock()
	if w.granted {
		g.mu.Unlock()
		g.release(path)
		metrics.Default().IncAdmissionRefusal(metrics.AdmissionContinuationQueuedExpired)
		return nil, errFUSETimeout
	}
	for i, queued := range lane.queue {
		if queued == w {
			lane.queue = append(lane.queue[:i], lane.queue[i+1:]...)
			g.waiters--
			break
		}
	}
	g.inFlight--
	g.signalLocked()
	g.mu.Unlock()
	if stopped {
		metrics.Default().IncAdmissionRefusal(metrics.AdmissionContinuationStopped)
	} else {
		metrics.Default().IncAdmissionRefusal(metrics.AdmissionContinuationQueuedExpired)
	}
	return nil, errFUSETimeout
}

func (g *continuationGate) beginStop() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopping {
		return
	}
	g.stopping = true
	for _, lane := range g.lanes {
		for _, w := range lane.queue {
			w.ready <- false
		}
		lane.queue = nil
	}
	g.waiters = 0
	g.signalLocked()
}

func (g *continuationGate) stop(timeout time.Duration) bool {
	g.beginStop()
	deadline := time.Now().Add(timeout)
	for {
		g.mu.Lock()
		if g.inFlight == 0 {
			g.mu.Unlock()
			return true
		}
		changed := g.changed
		g.mu.Unlock()
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		timer := time.NewTimer(remaining)
		select {
		case <-changed:
		case <-timer.C:
		}
		timer.Stop()
	}
}

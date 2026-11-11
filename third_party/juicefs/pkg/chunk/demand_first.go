package chunk

// JuiceMount (patch 0020): with --demand-first, a read that an application
// is waiting for gets its bytes before background work.
//
//   - A small ranged read does not wait for a whole-block download that is
//     already in flight for the same block, unless that download is about to
//     finish. It sends its own ranged GET instead.
//   - The prefetcher waits, at most demandDeferMax, while ranged demand
//     reads are in flight, so it does not share the link with them.
//
// On a lossy 200 ms link one 4 MiB block takes 5-50 s; a 256 KiB range takes
// a fraction of that. Without this, a second read in a block that the
// prefetcher was filling waited for the whole block.

import (
	"sync"
	"sync/atomic"
	"time"
)

// demandDeferMax bounds how long one prefetch waits for demand reads to
// finish. A stream of demand reads therefore cannot hold the prefetcher off
// for ever.
const demandDeferMax = 10 * time.Second

type demandFirst struct {
	enabled  bool
	deferMax time.Duration

	mu       sync.Mutex
	inflight int           // ranged demand GETs in flight
	idle     chan struct{} // closed while inflight == 0
	fullAt   map[string]time.Time

	// Smoothed durations (ns) of whole-block downloads and of ranged GETs.
	fullNS   atomic.Int64
	rangedNS atomic.Int64
}

func newDemandFirst(enabled bool) *demandFirst {
	idle := make(chan struct{})
	close(idle)
	return &demandFirst{enabled: enabled, deferMax: demandDeferMax, idle: idle, fullAt: make(map[string]time.Time)}
}

// beginDemand marks one ranged demand GET in flight; call the result when it
// ends.
func (d *demandFirst) beginDemand() func() {
	if d == nil || !d.enabled {
		return func() {}
	}
	d.mu.Lock()
	if d.inflight == 0 {
		d.idle = make(chan struct{})
	}
	d.inflight++
	d.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			d.mu.Lock()
			d.inflight--
			if d.inflight == 0 {
				close(d.idle)
			}
			d.mu.Unlock()
		})
	}
}

// waitIdle blocks until no ranged demand GET is in flight, or deferMax. It
// reports how long it waited (0 when nothing was in flight).
func (d *demandFirst) waitIdle() time.Duration {
	if d == nil || !d.enabled {
		return 0
	}
	d.mu.Lock()
	idle := d.idle
	d.mu.Unlock()
	select {
	case <-idle:
		return 0
	default:
	}
	start := time.Now()
	timer := time.NewTimer(d.deferMax)
	defer timer.Stop()
	select {
	case <-idle:
	case <-timer.C:
	}
	return time.Since(start)
}

// beginFull records that a whole-block download of key started; call the
// result with its error when it ends.
func (d *demandFirst) beginFull(key string) func(error) {
	if d == nil || !d.enabled {
		return func(error) {}
	}
	start := time.Now()
	d.mu.Lock()
	d.fullAt[key] = start
	d.mu.Unlock()
	return func(err error) {
		d.mu.Lock()
		if d.fullAt[key] == start {
			delete(d.fullAt, key)
		}
		d.mu.Unlock()
		if err == nil {
			observeEWMA(&d.fullNS, time.Since(start))
		}
	}
}

func (d *demandFirst) observeRanged(used time.Duration) {
	if d != nil && d.enabled {
		observeEWMA(&d.rangedNS, used)
	}
}

// piggyback reports whether a ranged read of key should wait for the
// whole-block download in flight instead of sending its own GET. Without
// --demand-first it always does (upstream). With it, only when the download
// is expected to end within about two ranged GETs. With no measurements yet,
// or when the download started where it is not tracked, it does not.
func (d *demandFirst) piggyback(key string) bool {
	if d == nil || !d.enabled {
		return true
	}
	d.mu.Lock()
	start, ok := d.fullAt[key]
	d.mu.Unlock()
	full, ranged := d.fullNS.Load(), d.rangedNS.Load()
	if !ok || full == 0 || ranged == 0 {
		return false
	}
	remaining := time.Duration(full) - time.Since(start)
	return remaining <= 2*time.Duration(ranged)
}

// observeEWMA folds one sample into v with weight 1/4.
func observeEWMA(v *atomic.Int64, sample time.Duration) {
	s := int64(sample)
	if s <= 0 {
		return
	}
	for {
		old := v.Load()
		next := s
		if old != 0 {
			next = old + (s-old)/4
		}
		if v.CompareAndSwap(old, next) {
			return
		}
	}
}

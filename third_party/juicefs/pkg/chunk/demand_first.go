package chunk

// JuiceMount (patch 0020): with --demand-first, a read that an application
// is waiting for gets its bytes before background work.
//
//   - A small ranged read does not wait for a whole-block download that is
//     already in flight for the same block, unless that download is about to
//     finish. It sends its own ranged GET instead, up to a quarter block of
//     such reads per download (piggyback).
//   - The prefetcher of a block waits, at most demandDeferMax, while ranged
//     demand reads of other blocks are in flight, so it does not share the
//     link with a reader that has moved on.
//
// On a lossy 200 ms link one 4 MiB block takes 5-50 s; a 256 KiB range takes
// a fraction of that. Without this, a second read in a block that the
// prefetcher was filling waited for the whole block.

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// demandDeferMax bounds how long one prefetch waits for demand reads to
// finish. A stream of demand reads therefore cannot hold the prefetcher off
// for ever.
const demandDeferMax = 10 * time.Second

type demandFirst struct {
	// on is --demand-first; the JMLinkPolicy control message (patch 0027,
	// jm_link_policy.go) switches it while the volume is mounted.
	on       atomic.Bool
	deferMax time.Duration
	budget   int // bytes of reads that may bypass one block download

	mu       sync.Mutex
	inflight int            // ranged demand GETs in flight
	perKey   map[string]int // of those, per block key
	changed  chan struct{}  // closed and replaced whenever inflight drops
	fullAt   map[string]*fullDownload

	// Smoothed duration of whole-block downloads (ns), and of ranged GETs
	// per KiB requested (ns/KiB), so 64 KiB and 1 MiB reads compare.
	fullNS        atomic.Int64
	rangedNSPerKB atomic.Int64
}

// fullDownload is one whole-block download in flight.
type fullDownload struct {
	start    time.Time
	bypassed int // bytes of reads that sent their own GET meanwhile
}

func newDemandFirst(enabled bool, blockSize int) *demandFirst {
	d := &demandFirst{deferMax: demandDeferMax, budget: blockSize / 4,
		perKey: make(map[string]int), changed: make(chan struct{}), fullAt: make(map[string]*fullDownload)}
	d.on.Store(enabled)
	return d
}

// enabled reports whether demand-first is on now. Each call decides for one
// step: a GET or download that began while it was on still ends through the
// func its begin returned, so the counts stay balanced across a switch.
func (d *demandFirst) enabled() bool { return d != nil && d.on.Load() }

// beginDemand marks one ranged demand GET of key in flight; call the result
// when it ends.
func (d *demandFirst) beginDemand(key string) func() {
	if !d.enabled() {
		return func() {}
	}
	d.mu.Lock()
	d.inflight++
	d.perKey[key]++
	d.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			d.mu.Lock()
			d.inflight--
			if d.perKey[key]--; d.perKey[key] == 0 {
				delete(d.perKey, key)
			}
			close(d.changed)
			d.changed = make(chan struct{})
			d.mu.Unlock()
		})
	}
}

// waitIdle blocks until no ranged demand GET is in flight for a block other
// than key, or deferMax. Demand reads of key itself do not hold it: the
// download of key is what serves them (a sequential reader), while demand
// reads elsewhere are a reader that moved on (a scrub). It reports how long
// it waited (0 when nothing held it).
func (d *demandFirst) waitIdle(key string) time.Duration {
	return d.waitIdleCtx(context.Background(), key)
}

// waitIdleCtx is waitIdle that also returns when ctx is done (a canceled
// pin fill must not wait out deferMax).
func (d *demandFirst) waitIdleCtx(ctx context.Context, key string) time.Duration {
	if !d.enabled() {
		return 0
	}
	var start time.Time
	var timer *time.Timer
	for {
		d.mu.Lock()
		others := d.inflight - d.perKey[key]
		changed := d.changed
		d.mu.Unlock()
		if others == 0 {
			break
		}
		if timer == nil {
			start = time.Now()
			timer = time.NewTimer(d.deferMax)
			defer timer.Stop()
		}
		select {
		case <-changed:
		case <-timer.C:
			return time.Since(start)
		case <-ctx.Done():
			return time.Since(start)
		}
	}
	if timer == nil {
		return 0
	}
	return time.Since(start)
}

// beginFull records that a whole-block download of key started; call the
// result with its error when it ends.
func (d *demandFirst) beginFull(key string) func(error) {
	if !d.enabled() {
		return func(error) {}
	}
	dl := &fullDownload{start: time.Now()}
	d.mu.Lock()
	d.fullAt[key] = dl
	d.mu.Unlock()
	return func(err error) {
		d.mu.Lock()
		if d.fullAt[key] == dl {
			delete(d.fullAt, key)
		}
		d.mu.Unlock()
		if err == nil {
			observeEWMA(&d.fullNS, time.Since(dl.start))
		}
	}
}

// observeRanged records one ranged GET of n bytes that took used.
func (d *demandFirst) observeRanged(used time.Duration, n int) {
	if d.enabled() && n > 0 {
		kb := (n + 1023) / 1024
		observeEWMA(&d.rangedNSPerKB, used/time.Duration(kb))
	}
}

// piggyback reports whether a ranged read of n bytes of key should wait for
// the whole-block download in flight instead of sending its own GET.
// Without --demand-first it always does (upstream). With it:
//   - no download tracked, or no measurements yet: send our own GET;
//   - the download is expected to end within about two ranged GETs of this
//     size (0 <= remaining <= 2 x ranged): wait for it. A download that has
//     already overrun its smoothed time (remaining < 0) is late, not nearly
//     done, so reads do not wait for it;
//   - otherwise send our own GET, while the reads that bypassed this
//     download total at most a quarter block. Past that budget the block is
//     being read through anyway (a sequential reader), so later reads wait
//     for the download instead of fetching the block a second time; but
//     never for a late download, whatever the budget.
func (d *demandFirst) piggyback(key string, n int) bool {
	if !d.enabled() {
		return true
	}
	full, perKB := d.fullNS.Load(), d.rangedNSPerKB.Load()
	d.mu.Lock()
	defer d.mu.Unlock()
	dl, ok := d.fullAt[key]
	if !ok {
		return false
	}
	if full > 0 && perKB > 0 {
		remaining := time.Duration(full) - time.Since(dl.start)
		ranged := time.Duration(perKB) * time.Duration((n+1023)/1024)
		if remaining < 0 {
			// Late: its end is unknown, so no read waits for it, budget
			// or not (on a lossy link that wait was the 53 s first byte).
			dl.bypassed += n
			return false
		}
		if remaining <= 2*ranged {
			return true
		}
	}
	if dl.bypassed+n > d.budget {
		return true
	}
	dl.bypassed += n
	return false
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

// readTimelineOn: JM_READ_TIMELINE=1 (a lab switch the app passes through)
// logs each step of a block-cache miss at Info: piggyback waits, ranged and
// whole-block GETs (first byte and end), and prefetch work. The app's NFS
// layer writes the matching "rt fuse ..." lines.
var readTimelineOn = os.Getenv("JM_READ_TIMELINE") == "1"

func readTimeline(format string, args ...interface{}) {
	if readTimelineOn {
		logger.Infof(format, args...)
	}
}

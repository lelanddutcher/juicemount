package pin

// blockpin.go: pinning through whole blocks (bundled juicefs patch 0025).
//
// Before, a pin read the file through FUSE and relied on JuiceFS to cache what
// it read. 128 KiB FUSE reads take the ranged path, whose whole-block fetch
// queue drops requests when full, so large pins ended with missing blocks and
// failed (laptop lab 2026-11-12: 3 of 4 clips, 1.8x download). Pinned blocks
// were also ordinary LRU entries, evicted by unrelated reads.
//
// Now a worker first asks juicefs to fill the file: juicefs downloads each
// missing block whole, writes it to its disk cache before it reports done, and
// pins it (its eviction skips pinned blocks; pinned bytes are capped at 90% of
// the cache). The worker then reads the file through FUSE as before, now from
// local disk, to hash it for the resident authority. A juicefs without the
// message keeps the old read path.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"time"
)

// BlockPinner is the juicefs pin message (implemented by nfs over .control).
type BlockPinner interface {
	// Start begins filling and pinning the file's blocks. A fill already
	// running for inode is kept.
	Start(inode uint64) error
	Status(inode uint64) (BlockFillStatus, error)
	// Check is local: the pinned bytes of inode and how many are not on disk.
	// known is false when juicefs holds no pins for inode (it restarted).
	Check(inode uint64) (known bool, total, missing int64, err error)
	// Sweep unpins every inode not in keep. It returns juicefs's pinned bytes
	// and its cap.
	Sweep(keep []uint64) (dropped int, pinned, limit int64, err error)
}

// BlockFillStatus is one fill's progress in juicefs.
type BlockFillStatus struct {
	State   BlockFillState
	Errno   syscall.Errno
	Total   int64
	Done    int64
	Fetched int64
}

type BlockFillState uint8

const (
	BlockFillRunning BlockFillState = 1
	BlockFillDone    BlockFillState = 2
	BlockFillFailed  BlockFillState = 3
)

// ErrBlockPinUnsupported: the mounted juicefs cannot pin (no patch 0025, or a
// memory cache). Callers keep the old read path.
var ErrBlockPinUnsupported = errors.New("juicefs cannot pin blocks")

// ErrPinnedShareFull: juicefs refused the fill because the pinned blocks would
// pass its cap (90% of the cache). It wraps ErrInsufficientCacheCapacity.
var ErrPinnedShareFull = fmt.Errorf("%w: pinned files need more than 90%% of the cache size; unpin a folder or raise the cache size", ErrInsufficientCacheCapacity)

// inodeOfReady is an optional provider method: the inode recorded in a Ready
// entry's witness.
type inodeOfReady interface {
	ReadyInode(Entry) (uint64, bool)
}

// blockFillPoll is how often a worker asks juicefs for a fill's progress.
var blockFillPoll = 200 * time.Millisecond

// progressSaveEvery throttles the in-flight bytes_cached writes to pin.db.
var progressSaveEvery = time.Second

// SetBlockPinner installs the juicefs pin message. Call before workers take
// jobs; nil keeps the read-through path.
func (p *Prefetcher) SetBlockPinner(b BlockPinner) {
	p.pinMu.Lock()
	p.pinner = b
	p.pinMu.Unlock()
}

func (p *Prefetcher) blockPinner() BlockPinner {
	p.pinMu.Lock()
	defer p.pinMu.Unlock()
	return p.pinner
}

// fillBlocks runs one juicefs fill of inode to its end. progress gets the
// bytes on disk so far.
func (p *Prefetcher) fillBlocks(ctx context.Context, b BlockPinner, inode uint64, progress func(BlockFillStatus)) (BlockFillStatus, error) {
	if err := b.Start(inode); err != nil {
		return BlockFillStatus{}, err
	}
	for {
		st, err := b.Status(inode)
		if err != nil {
			return st, err
		}
		if progress != nil {
			progress(st)
		}
		switch st.State {
		case BlockFillDone:
			return st, nil
		case BlockFillFailed:
			if st.Errno == syscall.ENOSPC {
				return st, ErrPinnedShareFull
			}
			if st.Errno == syscall.ENOTSUP {
				return st, ErrBlockPinUnsupported
			}
			return st, fmt.Errorf("juicefs pin fill: %w", st.Errno)
		}
		select {
		case <-time.After(blockFillPoll):
		case <-ctx.Done():
			return st, ctx.Err()
		case <-p.stopCh:
			return st, ErrPrefetcherStopped
		}
	}
}

type inodeRec struct {
	ino uint64
	at  time.Time
}

func (p *Prefetcher) rememberInode(path string, inode uint64) {
	p.pinMu.Lock()
	if p.inodes == nil {
		p.inodes = make(map[string]inodeRec)
	}
	p.inodes[path] = inodeRec{inode, time.Now()}
	p.pinMu.Unlock()
}

// progressMeter turns cumulative resident bytes into a rate over the last
// rateWindow, for the ETA in /cache-status.
type progressMeter struct {
	mu      sync.Mutex
	samples []progressSample
}

type progressSample struct {
	at    time.Time
	bytes int64
}

const rateWindow = 20 * time.Second

func (m *progressMeter) add(now time.Time, total int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n := len(m.samples); n > 0 && now.Sub(m.samples[n-1].at) < 500*time.Millisecond {
		m.samples[n-1].bytes = total
		return
	}
	m.samples = append(m.samples, progressSample{now, total})
	cut := 0
	for cut < len(m.samples)-2 && now.Sub(m.samples[cut].at) > rateWindow {
		cut++
	}
	m.samples = m.samples[cut:]
}

// rate is bytes per second over the window, or 0 when nothing moved in the
// last 5 s (a stalled or finished pass has no ETA).
func (m *progressMeter) rate(now time.Time) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := len(m.samples)
	if n < 2 || now.Sub(m.samples[n-1].at) > 5*time.Second {
		return 0
	}
	first, last := m.samples[0], m.samples[n-1]
	dt := last.at.Sub(first.at).Seconds()
	if dt <= 0 || last.bytes <= first.bytes {
		return 0
	}
	return float64(last.bytes-first.bytes) / dt
}

// ETASeconds estimates the seconds left for the pinned set, or 0 when there is
// nothing left or no current rate.
func ETASeconds(a AggregateStats, rate float64) int64 {
	left := a.TotalBytes - a.CachedBytes
	if left <= 0 || rate <= 0 || a.PendingFiles+a.PrefetchingFiles == 0 {
		return 0
	}
	return int64(float64(left)/rate + 0.999)
}

// retryBackoff is the wait before the n-th retry of a failed pin (n >= 1):
// 30 s, doubling, at most 30 min. Retries never stop while the pin exists.
func retryBackoff(n int) time.Duration {
	d := 30 * time.Second
	for i := 1; i < n && d < 30*time.Minute; i++ {
		d *= 2
	}
	return min(d, 30*time.Minute)
}

// RetryFailedLoop requeues failed pins with backoff. Call via Prefetcher.Go.
func (p *Prefetcher) RetryFailedLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-tick.C:
			p.retryFailed(time.Now())
		}
	}
}

func (p *Prefetcher) retryFailed(now time.Time) int {
	failed, err := p.store.Failed(1000)
	if err != nil {
		return 0
	}
	requeued := 0
	for _, e := range failed {
		p.pinMu.Lock()
		if p.retries == nil {
			p.retries = make(map[string]int)
		}
		n := p.retries[e.Path]
		p.pinMu.Unlock()
		if now.Before(e.LastPrefetched.Add(retryBackoff(n + 1))) {
			continue
		}
		if changed, err := p.store.RequeueForRepair(e); err != nil || !changed {
			continue
		}
		p.pinMu.Lock()
		p.retries[e.Path] = n + 1
		p.pinMu.Unlock()
		requeued++
	}
	if requeued > 0 {
		p.Wake()
	}
	return requeued
}

func (p *Prefetcher) clearRetries(path string) {
	p.pinMu.Lock()
	delete(p.retries, path)
	p.pinMu.Unlock()
}

// GuardReport is one pass of the pin guard.
type GuardReport struct {
	Checked   int   // Ready pins checked against juicefs
	Repinned  int   // pins juicefs did not hold (it restarted) pinned again
	Requeued  int   // Ready pins with missing blocks sent back to the workers
	Dropped   int   // juicefs pins of files no longer pinned
	Pinned    int64 // juicefs pinned bytes after the pass
	Limit     int64 // juicefs pin cap
	Supported bool
}

// PinGuardLoop keeps juicefs's pins equal to the pin store and Ready honest.
// It runs one pass at once (after a relaunch juicefs holds no pins), then one
// every interval. Call via Prefetcher.Go, after RestoreReady.
func (p *Prefetcher) PinGuardLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	p.GuardPins(ctx)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-tick.C:
			p.GuardPins(ctx)
		}
	}
}

// GuardPins runs one guard pass:
//  1. each Ready pin whose blocks juicefs does not hold as pinned is pinned
//     again (a fill: present blocks cost a stat, missing ones are downloaded);
//  2. a Ready pin that had missing blocks goes back to the workers, which
//     fill it and re-establish its resident authority;
//  3. juicefs unpins every file the store no longer pins.
func (p *Prefetcher) GuardPins(ctx context.Context) GuardReport {
	var rep GuardReport
	b := p.blockPinner()
	if b == nil || p.refuseFUSEContentReads {
		return rep
	}
	entries, err := p.store.All()
	if err != nil {
		return rep
	}
	passStart := time.Now()
	ri, _ := p.provider.(inodeOfReady)
	keep := make([]uint64, 0, len(entries))
	pinned := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		pinned[e.Path] = struct{}{}
	}
	type job struct {
		e   Entry
		ino uint64
	}
	var ready []job
	for _, e := range entries {
		if e.Status != StatusReady || ri == nil {
			continue
		}
		if ino, ok := ri.ReadyInode(e); ok {
			keep = append(keep, ino)
			ready = append(ready, job{e, ino})
		}
	}
	jobs := make(chan job)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				known, _, missing, err := b.Check(j.ino)
				if errors.Is(err, ErrBlockPinUnsupported) {
					continue
				}
				if err != nil {
					continue
				}
				lost := known && missing > 0
				repinned := false
				if !known {
					st, err := p.fillBlocks(ctx, b, j.ino, nil)
					if err != nil {
						continue
					}
					repinned = true
					lost = st.Fetched > 0
				}
				requeued := false
				if lost {
					if changed, err := p.store.RequeueForRepair(j.e); err == nil && changed {
						requeued = true
					}
				}
				mu.Lock()
				rep.Checked++
				if repinned {
					rep.Repinned++
				}
				if requeued {
					rep.Requeued++
				}
				mu.Unlock()
			}
		}()
	}
	for _, j := range ready {
		if ctx.Err() != nil {
			break
		}
		select {
		case jobs <- j:
		case <-p.stopCh:
		}
	}
	close(jobs)
	wg.Wait()
	if rep.Requeued > 0 {
		p.Wake()
	}
	if ctx.Err() != nil {
		return rep
	}
	// Fills this process ran: those of files still pinned, and any that
	// started during this pass (a pin newer than the snapshot), so a fill
	// that ends during the pass is not swept.
	p.pinMu.Lock()
	for path, rec := range p.inodes {
		if _, ok := pinned[path]; ok || !rec.at.Before(passStart) {
			keep = append(keep, rec.ino)
		}
	}
	p.pinMu.Unlock()
	dropped, pinnedBytes, limit, err := b.Sweep(keep)
	if err != nil {
		return rep
	}
	rep.Supported, rep.Dropped, rep.Pinned, rep.Limit = true, dropped, pinnedBytes, limit
	p.pinnedBlockBytes.Store(pinnedBytes)
	p.pinLimitBytes.Store(limit)
	return rep
}

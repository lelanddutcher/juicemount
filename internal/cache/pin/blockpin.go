package pin

// blockpin.go: pinning through whole blocks (bundled juicefs patch 0026).
//
// Before, a pin read the file through FUSE and relied on JuiceFS to cache what
// it read. 128 KiB FUSE reads take the ranged path, whose whole-block fetch
// queue drops requests when full, so large pins ended with missing blocks and
// failed (laptop lab 2026-11-12: 3 of 4 clips, 1.8x download). Pinned blocks
// were also ordinary LRU entries, evicted by unrelated reads.
//
// Now a worker first asks juicefs to fill the file: juicefs downloads each
// missing block whole, writes it to its disk cache before it reports done, and
// pins it. Its eviction skips pinned blocks; it keeps the pin list on disk, so
// pins survive a restart (opening a pinned file still needs the metadata
// server); pinned bytes are capped at 90% of the cache and at what the disk
// can hold above its free-space floor. The worker
// then reads the file through FUSE as before, now from local disk, to hash it
// for the resident authority. A juicefs without the message keeps the old
// read path.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// BlockPinner is the juicefs pin message (implemented by nfs over .control).
type BlockPinner interface {
	// Start begins filling and pinning the file's blocks with at most
	// workers parallel downloads. workers 1 also limits juicefs to one pin
	// download at a time across all fills. A fill already running for inode
	// is kept when the file did not change.
	Start(inode uint64, workers int) error
	Status(inode uint64) (BlockFillStatus, error)
	Cancel(inode uint64) error
	// Verify returns the items that are not pinned, were pinned from
	// another length or mtime, or miss blocks. It is local to juicefs unless
	// slices is set: then juicefs also reads each file's slices from
	// metadata, for a few seconds at most, and reports a file whose slices
	// changed (compaction keeps the length and mtime).
	Verify(items []PinVerifyItem, slices bool) ([]PinVerifyResult, error)
	// Sweep unpins every inode not in keep and returns juicefs's pinned
	// bytes and its cap.
	Sweep(keep []uint64) (dropped int, pinned, limit int64, err error)
	// SetCapacity grows juicefs's cache capacity to at least bytes.
	SetCapacity(bytes int64) (capacity, pinned, limit int64, err error)
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

// PinVerifyItem is one pinned file as the mirror sees it now.
type PinVerifyItem struct {
	Inode   uint64
	Size    int64
	MtimeNS int64
}

// Reasons in PinVerifyResult.
const (
	PinUnknown    = 1 // juicefs holds no pins for the inode
	PinChanged    = 2 // pinned from another version of the file
	PinIncomplete = 3 // pinned blocks missing from disk
)

type PinVerifyResult struct {
	Inode   uint64
	Reason  uint8
	Missing int64
}

// ErrBlockPinUnsupported: the mounted juicefs cannot pin (a memory cache).
// Callers keep the old read path.
var ErrBlockPinUnsupported = errors.New("juicefs cannot pin blocks")

// ErrPinnedShareFull: juicefs refused the fill because the pinned blocks would
// pass its cap (90% of the cache, or what the disk holds above its free-space
// floor). It wraps ErrInsufficientCacheCapacity.
var ErrPinnedShareFull = fmt.Errorf("%w: not enough room on the cache disk for the pinned files; free disk space on that disk, or unpin a folder", ErrInsufficientCacheCapacity)

// pinIdentity is an optional provider method: the inode a Ready entry's
// witness recorded, and the file as the local mirror shows it now.
type pinIdentity interface {
	PinIdentity(Entry) (witnessInode uint64, current ExpectedFile, ok bool)
}

var (
	// blockFillPoll is how often a worker asks juicefs for a fill's progress.
	blockFillPoll = 200 * time.Millisecond
	// progressSaveEvery throttles the in-flight bytes_cached writes to pin.db.
	progressSaveEvery = time.Second
	// liveCheckEvery is how often a fill checks that its pin still exists.
	liveCheckEvery = time.Second
	// guardFillTimeout bounds one re-pin by the guard: with no server the
	// fill cannot read the file's slices and would wait forever.
	guardFillTimeout = 5 * time.Minute
)

// sliceCheckBatch is how many Ready pins one guard pass also checks against
// the file's slices in metadata (rotating: every pin in turn).
const sliceCheckBatch = 64

// pinVerifyBatch bounds one Verify or Sweep message (juicefs's limit).
const pinVerifyBatch = 4096

// pinnedBlockBytes is juicefs's pinned bytes from the last guard pass, for
// the spool's headroom (pinned blocks are not reclaimable).
var pinnedBlockBytes atomic.Int64

// PinnedBlockBytes returns juicefs's pinned bytes from the last guard pass.
func PinnedBlockBytes() int64 { return pinnedBlockBytes.Load() }

// CacheBytesForPins is the cache size that holds pinned bytes under juicefs's
// 90% pin share: pinned/0.9, plus 4 KiB per 4 MiB block of overhead, plus
// 1 GiB for ordinary reads.
func CacheBytesForPins(pinned int64) int64 {
	if pinned <= 0 {
		return 0
	}
	return pinned*10/9 + pinned/1024 + 1<<30
}

// SetBlockPinner installs the juicefs pin message. Call before workers take
// jobs; nil keeps the read-through path.
func (p *Prefetcher) SetBlockPinner(b BlockPinner) {
	p.pinMu.Lock()
	p.pinner = b
	p.pinMu.Unlock()
}

// SetFillWorkers installs the per-fill download width: 1 on a slow or
// metered link, so pins do not crowd out reads the editor waits for. Every
// fill passes it to juicefs (the workers' and the guard's), and juicefs
// treats 1 as one pin download at a time across all fills.
func (p *Prefetcher) SetFillWorkers(f func() int) {
	p.pinMu.Lock()
	p.fillWorkers = f
	p.pinMu.Unlock()
}

func (p *Prefetcher) blockPinner() BlockPinner {
	p.pinMu.Lock()
	defer p.pinMu.Unlock()
	return p.pinner
}

func (p *Prefetcher) workersForFill() int {
	p.pinMu.Lock()
	f := p.fillWorkers
	p.pinMu.Unlock()
	if f == nil {
		return 4
	}
	return min(max(f(), 1), 4)
}

// ClearBlockPins unpins every file in juicefs: with JM_PIN_WHOLE_BLOCKS=0
// the pins an earlier run persisted would otherwise hold up to 90% of the
// cache for ever. It tries up to tries times, every interval, until juicefs
// answers; a juicefs that cannot pin, or does not know the message, ends it.
func ClearBlockPins(ctx context.Context, b BlockPinner, every time.Duration, tries int) error {
	for i := 1; ; i++ {
		_, _, _, err := b.Sweep(nil)
		if err == nil || errors.Is(err, ErrBlockPinUnsupported) || errors.Is(err, syscall.EINVAL) || i >= tries {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(every):
		}
	}
}

// fillBlocks runs one juicefs fill of inode to its end. progress gets each
// status; alive, when set, is asked about once a second and a false answer
// cancels the fill (the pin was removed or superseded). Any early return
// cancels the fill in juicefs.
func (p *Prefetcher) fillBlocks(ctx context.Context, b BlockPinner, inode uint64, progress func(BlockFillStatus), alive func() bool) (st BlockFillStatus, err error) {
	if err := b.Start(inode, p.workersForFill()); err != nil {
		return BlockFillStatus{}, err
	}
	defer func() {
		if err != nil && st.State != BlockFillDone && st.State != BlockFillFailed {
			_ = b.Cancel(inode)
		}
	}()
	lastAlive := time.Now()
	for {
		st, err = b.Status(inode)
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
			switch st.Errno {
			case syscall.ENOSPC:
				return st, ErrPinnedShareFull
			case syscall.ENOTSUP:
				return st, ErrBlockPinUnsupported
			}
			return st, fmt.Errorf("juicefs pin fill: %w", st.Errno)
		}
		if alive != nil && time.Since(lastAlive) >= liveCheckEvery {
			lastAlive = time.Now()
			if !alive() {
				return st, ErrPinSuperseded
			}
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

// entryCurrent reports whether path still has a pin of this generation.
func (p *Prefetcher) entryCurrent(e Entry) bool {
	cur, ok := p.store.Get(e.Path)
	return ok && cur.Generation == e.Generation
}

// growCacheFor asks juicefs for a cache large enough for the whole pinned set
// (juicefs still refuses what the disk cannot hold). It reports whether the
// capacity grew.
func (p *Prefetcher) growCacheFor(b BlockPinner) bool {
	stats, err := p.store.AggregateStats()
	if err != nil {
		return false
	}
	want := CacheBytesForPins(stats.TotalBytes)
	before := CacheBudgetBytes()
	if want <= before {
		return false
	}
	capacity, _, limit, err := b.SetCapacity(want)
	if err != nil || capacity <= before {
		return false
	}
	SetCacheBudgetBytes(capacity)
	// A new budget invalidates the capacity verdict; recompute it now so
	// queued pins are not refused as "capacity unknown" until the next tick.
	p.pinMu.Lock()
	dir := p.capacityDir
	p.pinMu.Unlock()
	ComputeCapacity(p.store, dir)
	p.pinLimitBytes.Store(limit)
	return true
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

// progressMeter turns cumulative downloaded bytes into a rate over the last
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
// nothing left or no current download rate. It assumes the bytes not yet
// counted must all be downloaded, so it may overestimate.
func ETASeconds(a AggregateStats, rate float64) int64 {
	left := a.TotalBytes - a.CachedBytes
	if left <= 0 || rate <= 0 || a.PendingFiles+a.PrefetchingFiles == 0 {
		return 0
	}
	return int64(float64(left)/rate + 0.999)
}

// retryBackoff is the wait before the n-th retry of a failed pin (n >= 1):
// 30 s, doubling, at most 30 min.
func retryBackoff(n int) time.Duration {
	d := 30 * time.Second
	for i := 1; i < n && d < 30*time.Minute; i++ {
		d *= 2
	}
	return min(d, 30*time.Minute)
}

// maxMissingRetries bounds retries of a pin whose file is gone: a deleted
// file stays deleted, but a FUSE mount briefly absent after a restart also
// answers ENOENT, so a few retries remain.
const maxMissingRetries = 3

func missingFile(lastError string) bool {
	return strings.Contains(lastError, syscall.ENOENT.Error())
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
	// Oldest attempts first, so a long failed list rotates.
	failed, err := p.store.FailedOldest(1000)
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
		if missingFile(e.LastError) && n >= maxMissingRetries {
			continue
		}
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
	Checked      int   // Ready pins verified against juicefs
	SliceChecked int   // of those, also checked against metadata slices
	Repinned     int   // pins juicefs did not hold, pinned again
	Requeued     int   // Ready pins sent back to the workers
	Dropped      int   // juicefs pins of files no longer pinned
	Pinned       int64 // juicefs pinned bytes after the pass
	Limit        int64 // juicefs pin cap
	Supported    bool
}

// PinGuardLoop keeps juicefs's pins equal to the pin store and Ready honest.
// It runs one pass at once, then one every interval. Call via Prefetcher.Go,
// after RestoreReady.
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

// GuardPins runs one guard pass. Everything except a re-pin is local to
// juicefs and the app, so a pass works with no server:
//  1. one batched Verify of every Ready pin against the mirror's length and
//     mtime;
//  2. a pin juicefs does not hold is pinned again (a fill: present blocks
//     cost a stat); one that had to download, was pinned from another
//     version of the file, or misses blocks goes back to the workers, which
//     re-establish its resident authority;
//  3. juicefs unpins every file the store no longer pins;
//  4. the cache grows when the pinned set needs it.
func (p *Prefetcher) GuardPins(ctx context.Context) GuardReport {
	var rep GuardReport
	b := p.blockPinner()
	if b == nil || p.refuseFUSEContentReads {
		return rep
	}
	passStart := time.Now()
	entries, err := p.store.All()
	if err != nil {
		return rep
	}
	pinInodes, err := p.store.PinInodes()
	if err != nil {
		return rep
	}
	id, _ := p.provider.(pinIdentity)
	var items []PinVerifyItem
	byInode := map[uint64]Entry{}
	var requeue []Entry
	for _, e := range entries {
		if e.Status != StatusReady || id == nil {
			continue
		}
		witness, cur, ok := id.PinIdentity(e)
		if !ok {
			continue
		}
		// Record the pinned inode where a requeue does not clear it, before
		// anything below can requeue the entry (pins made before the column
		// existed have only the witness).
		if pinInodes[e.Path] != witness && p.store.SetPinInode(e.Path, witness) == nil {
			pinInodes[e.Path] = witness
		}
		if cur.Inode != witness { // replaced on the server since the pin
			requeue = append(requeue, e)
			continue
		}
		items = append(items, PinVerifyItem{Inode: cur.Inode, Size: cur.Size, MtimeNS: cur.MtimeNS})
		byInode[cur.Inode] = e
	}
	var unknown []Entry
	flagged := map[uint64]bool{}
	for start := 0; start < len(items); start += pinVerifyBatch {
		batch := items[start:min(start+pinVerifyBatch, len(items))]
		bad, err := b.Verify(batch, false)
		if err != nil {
			return rep
		}
		rep.Checked += len(batch)
		for _, r := range bad {
			e, ok := byInode[r.Inode]
			if !ok {
				continue
			}
			flagged[r.Inode] = true
			if r.Reason == PinUnknown {
				unknown = append(unknown, e)
			} else {
				requeue = append(requeue, e)
			}
		}
	}
	// A few of the pins that passed are also checked against their slices
	// in metadata (compaction keeps length and mtime). Offline, juicefs
	// skips that check and reports nothing.
	var passed []PinVerifyItem
	for _, it := range items {
		if !flagged[it.Inode] {
			passed = append(passed, it)
		}
	}
	if len(passed) > 0 {
		p.pinMu.Lock()
		from := p.sliceCursor % len(passed)
		p.sliceCursor = from + sliceCheckBatch
		p.pinMu.Unlock()
		window := passed[from:min(from+sliceCheckBatch, len(passed))]
		if bad, err := b.Verify(window, true); err == nil {
			rep.SliceChecked = len(window)
			for _, r := range bad {
				if e, ok := byInode[r.Inode]; ok && r.Reason != PinUnknown {
					requeue = append(requeue, e)
				}
			}
		}
	}
	// Re-pin what juicefs does not hold. With no server the fill cannot read
	// the file's slices: the timeout ends it, and the next pass tries again.
	jobs := make(chan Entry)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for e := range jobs {
				if !p.entryCurrent(e) { // unpinned or repinned meanwhile
					continue
				}
				witness, _, ok := id.PinIdentity(e)
				if !ok {
					continue
				}
				fctx, cancel := context.WithTimeout(ctx, guardFillTimeout)
				st, err := p.fillBlocks(fctx, b, witness, nil, func() bool { return p.entryCurrent(e) })
				cancel()
				if err != nil {
					continue
				}
				mu.Lock()
				rep.Repinned++
				if st.Fetched > 0 { // blocks were missing: re-check the bytes
					requeue = append(requeue, e)
				}
				mu.Unlock()
			}
		}()
	}
	for _, e := range unknown {
		if ctx.Err() != nil {
			break
		}
		select {
		case jobs <- e:
		case <-p.stopCh:
		}
	}
	close(jobs)
	wg.Wait()
	for _, e := range requeue {
		if !p.entryCurrent(e) {
			continue
		}
		if changed, err := p.store.RequeueForRepair(e); err == nil && changed {
			rep.Requeued++
		}
	}
	if rep.Requeued > 0 {
		p.Wake()
	}
	if ctx.Err() != nil {
		return rep
	}
	// Keep: every file pinned now (a fresh read, so a pin removed during the
	// pass is not kept), whatever its status: its recorded pin inode (which
	// a requeue keeps, so a Pending or Failed pin keeps its blocks while
	// offline), the inodes its witness and the mirror name, and fills that
	// started during the pass.
	entries, err = p.store.All()
	if err != nil {
		return rep
	}
	if pinInodes, err = p.store.PinInodes(); err != nil {
		return rep
	}
	pinned := make(map[string]struct{}, len(entries))
	var keep []uint64
	for _, e := range entries {
		pinned[e.Path] = struct{}{}
		if ino := pinInodes[e.Path]; ino != 0 {
			keep = append(keep, ino)
		}
		if e.Status == StatusReady && id != nil {
			if witness, cur, ok := id.PinIdentity(e); ok {
				keep = append(keep, witness)
				if cur.Inode != 0 && cur.Inode != witness {
					keep = append(keep, cur.Inode)
				}
			}
		}
	}
	p.pinMu.Lock()
	for path, rec := range p.inodes {
		_, still := pinned[path]
		if still || !rec.at.Before(passStart) {
			keep = append(keep, rec.ino)
		} else {
			delete(p.inodes, path)
		}
	}
	for path := range p.retries {
		if _, still := pinned[path]; !still {
			delete(p.retries, path)
		}
	}
	p.pinMu.Unlock()
	dropped, pinnedBytes, limit, err := b.Sweep(keep)
	if err != nil {
		return rep
	}
	rep.Supported, rep.Dropped, rep.Pinned, rep.Limit = true, dropped, pinnedBytes, limit
	p.pinnedBlockBytes.Store(pinnedBytes)
	pinnedBlockBytes.Store(pinnedBytes)
	p.pinLimitBytes.Store(limit)
	if p.growCacheFor(b) {
		rep.Limit = p.pinLimitBytes.Load()
	}
	return rep
}

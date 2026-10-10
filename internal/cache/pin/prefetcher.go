package pin

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/testguard"
)

// Prefetcher reads pinned files through the FUSE mount, which causes JuiceFS
// to populate its own LRU cache. It does NOT manage a separate cache directory
// — JuiceFS owns that — we just trigger the right reads.
//
// Concurrency model: a bounded pool of N workers pulls from a job queue.
// Each worker reads its assigned file in 1 MB chunks and discards the bytes;
// the side effect we care about is the read travelling through JuiceFS's
// download + cache pipeline.
type Prefetcher struct {
	store      *Store
	provider   OwnedAuthorityProvider
	fusePath   string // root of the FUSE mount, e.g. ~/.juicemount/fuse-internal
	mountPoint string // user-facing mount, e.g. /Volumes/zpool — used to strip
	//                   the prefix from canonical pin keys back to FUSE-relative
	workers int
	// Acceptance-only: pending pins remain pending; no worker may hydrate
	// their content through FUSE while the zero-byte remote budget is active.
	refuseFUSEContentReads bool

	jobs       chan jobReq
	stopCh     chan struct{}
	wakeCh     chan struct{} // R-2: nudge PullPending to drain now, not on the next tick
	wg         sync.WaitGroup
	mu         sync.Mutex    // serializes admission, Stop, and the final WaitGroup Add
	done       chan struct{} // one retained join, shared by every stop attempt
	stopping   bool
	queued     map[string]struct{} // captured path+generation, through worker completion
	queueSpace chan struct{}       // wakes EnqueueWait when a worker takes a job
	ctx        context.Context
	cancel     context.CancelFunc

	// Live progress counters (atomic for cheap concurrent reads)
	bytesPrefetched atomic.Int64
	filesPrefetched atomic.Int64
	currentFile     atomic.Pointer[string]
	// Test-only rendezvous after EOF and before the generation-fenced Ready write.
	testBeforeReady  func()
	testBeforeClaim  func(Entry)
	testAfterEnqueue func(Entry)
}

// OwnedAuthorityProvider is installed before workers start. It is implemented
// by NFS, so pin never imports metadata/cache and cannot infer provenance from
// a successful FUSE read alone.
type OwnedAuthorityProvider interface {
	Capture(context.Context, Entry) (OwnedAuthorityJob, error)
	ValidateReady(context.Context, Entry) bool // local-only boot restore, including CRC
	Current(Entry) bool                        // RAM-only per-use identity/writer/binding check
	DiscardReady(Entry)                        // exact-generation cleanup after rejected restore
}

type ExpectedFile struct {
	Inode   uint64
	Size    int64
	MtimeNS int64
}

type OwnedAuthorityJob interface {
	Release()
	Expected() ExpectedFile
	SourceCurrent() bool
	Complete(context.Context, [sha256.Size]byte) ([]byte, error)
	Finalize(context.Context, Entry) (bool, error) // short local guards + RAM admission
}

type jobReq struct {
	entry Entry
	done  chan error // optional; nil if fire-and-forget
}

// ErrPrefetcherStopped reports that shutdown closed job admission or dropped
// a queued synchronous job.
var ErrPrefetcherStopped = errors.New("prefetcher stopped")
var ErrPinSuperseded = errors.New("pin generation superseded")
var ErrAuthorityUnavailable = errors.New("pin resident authority unavailable")

// NewPrefetcher constructs a worker pool. workers <= 0 picks a sensible default.
//
// mountPoint is the user-facing path (e.g. "/Volumes/zpool") that pin store
// keys are anchored to. The prefetcher strips this prefix to translate keys
// back to FUSE-relative paths. Empty string falls back to the legacy default
// for backward compatibility.
func NewPrefetcher(store *Store, fusePath, mountPoint string, workers int, provider ...OwnedAuthorityProvider) *Prefetcher {
	if workers <= 0 {
		workers = 4 // good default: parallel enough to saturate WAN, not so many as to thrash
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Prefetcher{
		store:                  store,
		fusePath:               fusePath,
		mountPoint:             mountPoint,
		workers:                workers,
		jobs:                   make(chan jobReq, 256),
		queued:                 make(map[string]struct{}),
		stopCh:                 make(chan struct{}),
		done:                   make(chan struct{}),
		queueSpace:             make(chan struct{}, 1),
		ctx:                    ctx,
		cancel:                 cancel,
		wakeCh:                 make(chan struct{}, 1), // buffered+coalesced: one pending wake is enough
		refuseFUSEContentReads: testguard.RefuseFUSEContentReads(),
	}
	if len(provider) == 1 {
		p.provider = provider[0]
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.workerLoop()
	}
	return p
}

// Context is canceled when StopWithin closes admission. Long-running tracked
// loops must use this lifetime so their join can finish on ordinary stop.
func (p *Prefetcher) Context() context.Context { return p.ctx }

// RestoreReady rechecks durable witnesses using only local mirror, snapshot and
// CRC bytes. Call after spool recovery, before admitting cached pin reads.
func (p *Prefetcher) RestoreReady(ctx context.Context) (int, error) {
	if p.provider == nil {
		return 0, ErrAuthorityUnavailable
	}
	entries, err := p.store.All()
	if err != nil {
		return 0, err
	}
	count := 0
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		if e.Status != StatusReady || len(e.Authority) == 0 {
			continue
		}
		if !p.provider.ValidateReady(ctx, e) {
			continue
		}
		admitted, err := p.store.AdmitReady(e)
		if err != nil {
			p.provider.DiscardReady(e)
			return count, err
		}
		if admitted {
			count++
		} else {
			p.provider.DiscardReady(e)
		}
	}
	return count, nil
}

// Stop preserves the historical bounded call for clients that do not need a
// completion verdict. Lifecycle owners must use StopWithin before retiring
// dependencies the workers or tracked loops can still access.
//
// 2026-07-15 teardown-hang fix: this used to `close(p.jobs)` and let each
// worker's `for range p.jobs` DRAIN the whole buffered queue (up to 256 jobs,
// each a full file read through FUSE) before exiting — so on a large pinned set
// being warmed, Stop() blocked reading gigabytes through FUSE, hanging the
// "Stop everything"/quit teardown (stopServerLocked never got past
// prefetcher.Stop). Closing the queue also races a producer's non-blocking
// Enqueue → "send on closed channel" panic. Workers now exit on stopCh
// (see workerLoop), so we neither drain the queue nor close the channel.
func (p *Prefetcher) Stop() {
	_ = p.StopWithin(3 * time.Second)
}

// StopWithin closes admission once and reports whether every admitted worker
// and Go-launched task has actually returned. A timeout retains the same join
// for retry. Cancellation alone never counts as completion.
func (p *Prefetcher) StopWithin(timeout time.Duration) bool {
	p.mu.Lock()
	if !p.stopping {
		p.stopping = true
		if p.cancel != nil {
			p.cancel()
		}
		close(p.stopCh)
		// Go's final Add and this first Wait are serialized by mu.
		go func() {
			p.wg.Wait()
			close(p.done)
		}()
	}
	p.mu.Unlock()
	if timeout <= 0 {
		select {
		case <-p.done:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		return true
	case <-timer.C:
		// Prefer actual completion when both channels become ready together.
		select {
		case <-p.done:
			return true
		default:
			return false
		}
	}
}

// Wake nudges PullPending to drain the Pending queue immediately instead of
// waiting for its next tick. Non-blocking and coalescing — call it right after
// inserting new pins (R-2) so warming starts within milliseconds, not up to a
// full tick later. Safe to call from any goroutine, including after Stop (the
// send just no-ops against the buffer or is ignored once the loop exits).
func (p *Prefetcher) Wake() {
	select {
	case p.wakeCh <- struct{}{}:
	default: // a wake is already pending; one is enough
	}
}

// Enqueue queues a single file for prefetch. Non-blocking — returns false if
// the queue is full (caller can retry or backpressure).
func (p *Prefetcher) Enqueue(e Entry) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopping {
		return false
	}
	key := e.Path + "\x00" + e.Generation
	if e.Generation == "" || len(p.jobs) == cap(p.jobs) {
		return false
	}
	if _, exists := p.queued[key]; exists {
		return false
	}
	p.queued[key] = struct{}{}
	p.jobs <- jobReq{entry: e}
	if p.testAfterEnqueue != nil {
		p.testAfterEnqueue(e)
	}
	return true
}

// EnqueueWait queues a file and blocks until it's prefetched (or fails).
// Used by the CLI when the user wants synchronous "wait for it" behavior.
func (p *Prefetcher) EnqueueWait(ctx context.Context, e Entry) error {
	done := make(chan error, 1)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.mu.Lock()
		if p.stopping {
			p.mu.Unlock()
			return ErrPrefetcherStopped
		}
		key := e.Path + "\x00" + e.Generation
		if e.Generation == "" {
			p.mu.Unlock()
			return ErrPinSuperseded
		}
		if _, exists := p.queued[key]; exists {
			p.mu.Unlock()
			return ErrPinSuperseded
		}
		if len(p.jobs) < cap(p.jobs) {
			p.queued[key] = struct{}{}
			p.jobs <- jobReq{entry: e, done: done}
			if p.testAfterEnqueue != nil {
				p.testAfterEnqueue(e)
			}
			p.mu.Unlock()
			goto admitted
		}
		p.mu.Unlock()
		select {
		case <-p.queueSpace:
		case <-p.stopCh:
			return ErrPrefetcherStopped
		case <-ctx.Done():
			return ctx.Err()
		}
	}
admitted:
	select {
	case err := <-done:
		select {
		case <-p.stopCh:
			return ErrPrefetcherStopped
		default:
		}
		return err
	case <-p.stopCh:
		return ErrPrefetcherStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Go launches fn as a tracked background goroutine. StopWithin waits for
// every Go-launched goroutine to return before reporting completion. Use this
// for long-running loops (PullPending, ReWarmupLoop) that hold pin
// store references — calling `go x.PullPending(...)` directly leaks
// SQLite file descriptors on Stop because the goroutine may still
// have a connection checked out when Close runs (QA-7a, 2026-05-17).
//
// The Add MUST happen synchronously here, not inside fn, otherwise
// Stop().Wait() can return before fn even gets a chance to register
// itself — wg.Add after the counter has hit zero is undefined.
func (p *Prefetcher) Go(fn func()) { p.TryGo(fn) }

// TryGo reports whether fn was admitted before StopWithin closed admission.
// An admitted task stays in the stop join through its final side effect.
func (p *Prefetcher) TryGo(fn func()) bool {
	p.mu.Lock()
	if p.stopping {
		p.mu.Unlock()
		return false
	}
	p.wg.Add(1)
	p.mu.Unlock()
	go func() {
		defer p.wg.Done()
		fn()
	}()
	return true
}

// PullPending repeatedly drains the store's Pending queue and enqueues
// jobs. Runs until ctx is cancelled. Call via Prefetcher.Go so the
// goroutine is tracked for Stop's wg.Wait (QA-7a).
func (p *Prefetcher) PullPending(ctx context.Context, batchSize int) {
	if batchSize <= 0 {
		batchSize = 100
	}
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-tick.C:
			p.drainPending(batchSize)
		case <-p.wakeCh:
			// R-2: a fresh pin just landed — drain now instead of waiting up
			// to 2s for the next tick, so warming visibly starts right away.
			p.drainPending(batchSize)
		}
	}
}

// drainPending pulls one batch of Pending entries and enqueues them, marking
// each Prefetching. Shared by the PullPending ticker and the Wake signal.
func (p *Prefetcher) drainPending(batchSize int) {
	if p.refuseFUSEContentReads {
		return
	}
	pending, err := p.store.Pending(batchSize)
	if err != nil || len(pending) == 0 {
		return
	}
	stats, statsErr := p.store.AggregateStats()
	for _, e := range pending {
		admissionErr := error(nil)
		if statsErr != nil {
			admissionErr = ErrCacheCapacityUnknown
		} else {
			admissionErr = DownloadAdmissionFor(e.Size, stats.TotalBytes)
		}
		if admissionErr != nil {
			if e.LastError != admissionErr.Error() {
				_, _ = p.store.UpdateStatusForEntry(e, StatusPending, 0, admissionErr.Error())
			}
			continue
		}
		if !p.Enqueue(e) {
			continue // duplicate/stale or full; other pending paths may fit
		}
	}
}

// ReWarmupLoop runs periodically and re-reads pinned-and-ready files to
// keep them at the front of JuiceFS's LRU. Without this, eventually any
// pinned file falls off the LRU under cache pressure and gets evicted.
//
// ttl is how stale a file can get before re-warmup. Recommended: 6 hours.
// Call via Prefetcher.Go (QA-7a — see PullPending comment).
func (p *Prefetcher) ReWarmupLoop(ctx context.Context, ttl time.Duration, batchSize int) {
	if batchSize <= 0 {
		batchSize = 50
	}
	tick := time.NewTicker(15 * time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-tick.C:
			// R-1: never re-warm an over-capacity pinned set. Re-reading a
			// stale-but-Ready file evicts blocks another pinned file just
			// downloaded (JuiceFS holds the volume above its free-space
			// floor), so re-warm degenerates into perpetual evict-and-refetch
			// thrash that the set can never escape — and burns WAN bandwidth.
			// PullPending already warmed everything that fits; leave it. The
			// over-capacity verdict is surfaced to the user so they can free
			// disk or unpin a folder.
			if IsOverCapacity() {
				continue
			}
			stale, err := p.store.Stale(ttl, batchSize)
			if err != nil || len(stale) == 0 {
				continue
			}
			for _, e := range stale {
				p.Enqueue(e)
			}
		}
	}
}

// CapacityLoop periodically recomputes the pinned-set-vs-disk verdict (R-1)
// and publishes it via pin.Capacity()/IsOverCapacity(). The single cost is a
// walk of the JuiceFS cache tree, so it runs on a slow cadence well off the
// read hot path. cacheBaseDir empty falls back to the default (~/.juicefs/cache).
// Call via Prefetcher.Go (QA-7a — same pin.db lifetime concern as PullPending).
func (p *Prefetcher) CapacityLoop(ctx context.Context, interval time.Duration, cacheBaseDir string) {
	if interval <= 0 {
		interval = 60 * time.Second
	}
	// Compute once up front so the UI and the re-warm gate have a verdict
	// before the first tick — otherwise a freshly-launched over-capacity mount
	// would re-warm for a full interval before anyone noticed.
	ComputeCapacity(p.store, cacheBaseDir)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-tick.C:
			ComputeCapacity(p.store, cacheBaseDir)
		}
	}
}

// VerifyReport summarizes a pin-coverage verification pass.
type VerifyReport struct {
	TotalPinned     int    // files marked Ready in the pin store
	Reenqueued      int    // files re-queued for prefetch
	AlreadyComplete int    // files we believe are fully cached (not re-queued)
	QueueOverflow   int    // files we couldn't enqueue (worker pool saturated)
	Bytes           int64  // total bytes covered by re-queued files
	StartedAt       string // RFC3339
	Note            string // human-readable summary
}

// VerifyAndRepair walks every pinned-Ready entry and re-enqueues it for
// prefetch. The prefetcher reads each file through FUSE; JuiceFS serves
// from local LRU when the bytes are present (fast, no network) and
// fetches from the backend otherwise. Either way, post-prefetch the
// blocks are guaranteed on disk.
//
// This is the right operation for the "is my cache actually populated?"
// question — much simpler than walking Redis chunk-slice mappings and
// stat()-ing every block file ourselves. The cost is "re-read every
// pinned file once" which is bounded by sequential disk speed for
// already-cached files and by backend bandwidth for missing ones.
//
// Sets each entry's status to Pending before enqueueing so the existing
// workerLoop picks it up; the worker will write it back to Ready when
// the read completes.
func (p *Prefetcher) VerifyAndRepair(ctx context.Context) (*VerifyReport, error) {
	report := &VerifyReport{StartedAt: time.Now().Format(time.RFC3339)}

	// Pull every entry that's pinned and not actively in the queue:
	// Ready, Prefetching, AND Failed. Including Failed is critical for
	// the "FUSE was momentarily unmounted, every open() returned ENOENT,
	// all entries got marked Failed" recovery case — without it, the user
	// has to unpin and re-pin to retry, which is hostile.
	all, err := p.store.AllPinnedForRepair(100000)
	if err != nil {
		return nil, fmt.Errorf("list pinned: %w", err)
	}
	report.TotalPinned = len(all)
	if len(all) == 0 {
		report.Note = "no pinned files to verify"
		return report, nil
	}

	// Mark every entry as Pending up front so the existing PullPending
	// worker pool can drain them naturally — no need for us to manually
	// keep the in-memory queue topped up. PullPending wakes every 5s,
	// pulls up to 100 Pending rows, and enqueues them; with 4 workers and
	// typical sequential file-size variance this absorbs any pin set
	// without overflow.
	currentDemand := int64(-1)
	for _, e := range all {
		select {
		case <-ctx.Done():
			report.Note = "cancelled"
			return report, ctx.Err()
		default:
		}
		if e.Status == StatusReady && len(e.Authority) != 0 && p.provider != nil && p.provider.ValidateReady(ctx, e) {
			if admitted, err := p.store.AdmitReady(e); err != nil {
				return report, err
			} else if admitted {
				report.AlreadyComplete++
				continue
			}
		}
		if currentDemand < 0 {
			stats, err := p.store.AggregateStats()
			if err != nil {
				return report, fmt.Errorf("%w: %v", ErrCacheCapacityUnknown, err)
			}
			currentDemand = stats.TotalBytes
		}
		if err := DownloadAdmissionFor(e.Size, currentDemand); err != nil {
			report.Note = err.Error()
			return report, err
		}
		if changed, err := p.store.RequeueForRepair(e); err != nil {
			return report, err
		} else if changed {
			report.Reenqueued++
			report.Bytes += e.Size
		}
	}
	report.Note = fmt.Sprintf(
		"marked %d/%d files Pending (%.1f GiB); PullPending will pick them up",
		report.Reenqueued, report.TotalPinned, float64(report.Bytes)/(1<<30))
	return report, nil
}

// workerLoop pulls jobs and runs the prefetch. It exits on stopCh WITHOUT
// draining the queue — a shutdown must not wait to read every buffered job
// through FUSE (that was the teardown hang; see Stop). stopCh is checked with
// priority so a Stop() in flight wins over a ready job.
func (p *Prefetcher) workerLoop() {
	defer p.wg.Done()
	for {
		select {
		case <-p.stopCh:
			return
		default:
		}
		select {
		case <-p.stopCh:
			return
		case j := <-p.jobs:
			select {
			case p.queueSpace <- struct{}{}:
			default:
			}
			if p.testBeforeClaim != nil {
				p.testBeforeClaim(j.entry)
			}
			// Ordinary rewarm must not revoke a still-complete local witness.
			// Check CRC locally; only an actual loss enters a new FUSE attempt.
			if j.entry.Status == StatusReady && p.provider != nil && p.provider.ValidateReady(p.ctx, j.entry) {
				admitted, admissionErr := p.store.AdmitReady(j.entry)
				if admissionErr != nil || !admitted {
					p.provider.DiscardReady(j.entry)
				}
				if admissionErr != nil || admitted {
					if admissionErr == nil {
						var touched bool
						touched, admissionErr = p.store.TouchReady(j.entry)
						if !touched && admissionErr == nil {
							admissionErr = ErrPinSuperseded
						}
					}
					p.mu.Lock()
					delete(p.queued, j.entry.Path+"\x00"+j.entry.Generation)
					p.mu.Unlock()
					if j.done != nil {
						j.done <- admissionErr
					}
					continue
				}
			}
			claimed, ok, err := p.store.ClaimPrefetch(j.entry)
			if err == nil && !ok {
				err = ErrPinSuperseded
			}
			if err == nil {
				select {
				case <-p.stopCh:
					_, _ = p.store.UpdateStatusForEntry(claimed, StatusPending, 0, "")
					err = ErrPrefetcherStopped
				default:
					err = p.prefetch(claimed)
				}
			}
			p.mu.Lock()
			delete(p.queued, j.entry.Path+"\x00"+j.entry.Generation)
			p.mu.Unlock()
			if j.done != nil {
				j.done <- err
			}
		}
	}
}

// prefetch reads the file through FUSE, discarding the bytes. Updates the
// store with the result.
func (p *Prefetcher) prefetch(e Entry) (retErr error) {
	if p.refuseFUSEContentReads {
		// Keep durable intent intact for the next ordinary launch. A test guard
		// must not convert "not attempted" into a failed pin.
		_, _ = p.store.UpdateStatusForEntry(e, StatusPending, 0, "")
		return testguard.ErrFUSEContentReadRefused
	}
	stats, statsErr := p.store.AggregateStats()
	admissionErr := error(nil)
	if statsErr != nil {
		admissionErr = fmt.Errorf("%w: pinned demand unavailable: %v", ErrCacheCapacityUnknown, statsErr)
	} else {
		admissionErr = DownloadAdmissionFor(e.Size, stats.TotalBytes)
	}
	if err := admissionErr; err != nil {
		_, _ = p.store.UpdateStatusForEntry(e, StatusPending, 0, err.Error())
		return err
	}
	var totalRead int64
	completed := false
	defer func() {
		if completed {
			return
		}
		status := StatusFailed
		message := "resident authority unavailable"
		if retErr != nil {
			message = retErr.Error()
		}
		if p.ctx.Err() != nil {
			status, message = StatusPending, ""
		}
		if errors.Is(retErr, ErrCacheCapacityUnknown) || errors.Is(retErr, ErrNoUsableCacheCapacity) || errors.Is(retErr, ErrInsufficientCacheCapacity) {
			status, totalRead = StatusPending, 0
		}
		_, _ = p.store.UpdateStatusForEntry(e, status, totalRead, message)
	}()
	if p.provider == nil {
		return ErrAuthorityUnavailable
	}
	job, err := p.provider.Capture(p.ctx, e)
	if err != nil {
		return err
	}
	defer job.Release()
	want := job.Expected()
	if want.Inode == 0 || want.Size != e.Size || want.Size < 0 {
		return ErrAuthorityUnavailable
	}
	rel := p.stripMountPrefix(e.Path)
	full := filepath.Join(p.fusePath, rel)

	currentName := full
	p.currentFile.Store(&currentName)
	defer p.currentFile.Store(nil)

	f, err := derivatives.OpenRegularUnder(p.fusePath, rel)
	if err != nil {
		return err
	}
	before, err := f.Stat()
	if err != nil || !pinFileMatches(before, want) {
		_ = f.Close()
		return ErrAuthorityUnavailable
	}

	buf := make([]byte, 1024*1024) // 1 MB read chunks
	hash := sha256.New()
	nextCapacityCheck := int64(32 << 20)
	for totalRead < want.Size {
		if totalRead >= nextCapacityCheck {
			if err := DownloadAdmissionFor(e.Size, stats.TotalBytes); err != nil {
				_ = f.Close()
				return err
			}
			nextCapacityCheck = totalRead + (32 << 20)
		}
		// Abort a large in-flight prefetch promptly on shutdown so Stop()'s
		// wg.Wait() returns within one chunk instead of reading out a multi-GB
		// file through FUSE. The entry stays Prefetching and is re-warmed on the
		// next launch / verify pass.
		select {
		case <-p.stopCh:
			_ = f.Close()
			return ErrPrefetcherStopped
		default:
		}
		readLen := min(int64(len(buf)), want.Size-totalRead)
		n, rerr := f.Read(buf[:readLen])
		if n > 0 {
			totalRead += int64(n)
			p.bytesPrefetched.Add(int64(n))
			_, _ = hash.Write(buf[:n])
		}
		if rerr != nil || n == 0 {
			_ = f.Close()
			if rerr == nil {
				rerr = io.ErrUnexpectedEOF
			}
			return rerr
		}
	}
	var final [1]byte
	n, eofErr := f.Read(final[:])
	after, statErr := f.Stat()
	pathname, pathErr := os.Lstat(full)
	sourceCurrent := job.SourceCurrent()
	closeErr := f.Close()
	if n != 0 || eofErr != io.EOF || statErr != nil || pathErr != nil || !pinFileMatches(after, want) || !os.SameFile(before, after) || !os.SameFile(after, pathname) || !sourceCurrent || closeErr != nil {
		return fmt.Errorf("pin source changed or did not end at expected EOF: %w", ErrAuthorityUnavailable)
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	authority, err := job.Complete(p.ctx, digest)
	if err != nil {
		return fmt.Errorf("pin authority Complete rejected: %w", err)
	}
	if len(authority) == 0 {
		return fmt.Errorf("pin authority Complete returned empty witness: %w", ErrAuthorityUnavailable)
	}

	if p.testBeforeReady != nil {
		p.testBeforeReady()
	}
	changed, err := p.store.CompleteReady(e, authority)
	if err != nil {
		return err
	}
	if !changed {
		return ErrPinSuperseded
	}
	e.Authority = authority
	admitted, err := job.Finalize(p.ctx, e)
	if err != nil {
		return fmt.Errorf("pin authority Finalize rejected: %w", err)
	}
	if !admitted {
		return fmt.Errorf("pin authority Finalize refused admission: %w", ErrAuthorityUnavailable)
	}
	completed = true
	p.filesPrefetched.Add(1)
	return nil
}

func pinFileMatches(info os.FileInfo, want ExpectedFile) bool {
	if info == nil || !info.Mode().IsRegular() || info.Size() != want.Size || info.ModTime().UnixNano() != want.MtimeNS {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Ino == want.Inode
}

// LiveStats reports counters since process start. For UI progress bars.
type LiveStats struct {
	BytesPrefetched int64  `json:"bytes_prefetched"`
	FilesPrefetched int64  `json:"files_prefetched"`
	CurrentFile     string `json:"current_file"` // empty if idle
	Workers         int    `json:"workers"`
}

func (p *Prefetcher) LiveStats() LiveStats {
	cur := ""
	if cp := p.currentFile.Load(); cp != nil {
		cur = *cp
	}
	return LiveStats{
		BytesPrefetched: p.bytesPrefetched.Load(),
		FilesPrefetched: p.filesPrefetched.Load(),
		CurrentFile:     cur,
		Workers:         p.workers,
	}
}

// stripMountPrefix turns canonical pin-store paths (anchored to the user's
// mount point) into FUSE-relative paths. For the default "/Volumes/zpool":
//
//	"/Volumes/zpool/foo/bar" → "foo/bar"
//	"/Volumes/zpool"         → ""
//	"/other/path"            → "/other/path" (no-op fallback)
func (p *Prefetcher) stripMountPrefix(path string) string {
	mp := p.mountPoint
	if mp == "" {
		mp = "/Volumes/zpool"
	}
	for len(mp) > 1 && mp[len(mp)-1] == '/' {
		mp = mp[:len(mp)-1]
	}
	if path == mp {
		return ""
	}
	withSlash := mp + "/"
	if len(path) > len(withSlash) && path[:len(withSlash)] == withSlash {
		return path[len(withSlash):]
	}
	return path
}

// stripVolumePrefix is the legacy helper retained for the existing test.
// New code should use Prefetcher.stripMountPrefix which respects the
// configured mount point.
func stripVolumePrefix(path string) string {
	const vol = "/Volumes/zpool/"
	if len(path) > len(vol) && path[:len(vol)] == vol {
		return path[len(vol):]
	}
	const v2 = "/Volumes/zpool"
	if path == v2 {
		return ""
	}
	return path
}

// CountFilesUnder walks a directory in the JuiceMount volume and returns
// every regular file path + size. Used by Pin() when the user pins a
// whole subtree.
func CountFilesUnder(rootPath string) ([]Entry, error) {
	var out []Entry
	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // best effort — keep going past permission errors etc.
		}
		if info.IsDir() {
			return nil
		}
		// Skip Apple-Double and other macOS bookkeeping files
		base := filepath.Base(path)
		if len(base) >= 2 && base[:2] == "._" {
			return nil
		}
		switch base {
		case ".DS_Store", ".Spotlight-V100", ".Trashes", ".fseventsd",
			".TemporaryItems":
			return nil
		}
		out = append(out, Entry{
			Path:    path,
			Size:    info.Size(),
			PinRoot: rootPath,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk %q: %w", rootPath, err)
	}
	return out, nil
}

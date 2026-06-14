package nfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// Drainer copies ready spool entries into the JuiceFS FUSE mount.
//
// Design:
//   - A single dispatcher goroutine pulls batches of ready rows from
//     metadata.SpoolStore.ListReady. For each row it atomically claims
//     (metadata.MarkDraining) and hands off to a worker goroutine.
//   - Worker concurrency is bounded by a semaphore (default 4).
//   - On worker failure: bump drain_attempts via SpoolStore.MarkDrainRetry
//     and back off exponentially. After MaxAttempts the row is marked
//     `failed` and stays there until manual recovery.
//   - On SHA mismatch the row is quarantined immediately — bit flips
//     do not benefit from retry.
//   - On Stop: dispatcher exits, in-flight drains are waited on with a
//     30 s deadline. Claimed-but-unstarted rows are reset to ready so
//     they're not stranded.
//
// The drainer never touches read-path code paths (cachedFile,
// cache.Reader, readahead.Manager, memBuf, pin.Store). Per the slice
// plan guardrails, it interacts only with the spool and the FUSE mount.
type Drainer struct {
	spool        *SpoolStore
	fuseRoot     string
	workers      int
	maxAttempts  int
	backoffBase  time.Duration
	pollFallback time.Duration

	sem    chan struct{}
	notify chan struct{}
	stop   chan struct{}
	done   chan struct{}

	inFlight sync.WaitGroup

	metrics DrainerMetrics

	// onDrainComplete, if set, is invoked after a row successfully drains to
	// FUSE (post-MarkDrainComplete). Set once via SetOnDrainComplete BEFORE
	// Start; the handler uses it to sync the drained file's real size into
	// the metadata cache. Worker goroutines read it after Start, so
	// set-before-Start provides the happens-before (no lock needed).
	onDrainComplete func(nfsPath string, size int64)
}

// DrainerConfig controls drainer behavior. Zero values fall back to
// production-sensible defaults documented per field.
type DrainerConfig struct {
	// FuseRoot is the absolute path to the JuiceFS FUSE mount root.
	// Drained files land at FuseRoot/<nfs_path>.
	FuseRoot string

	// Workers caps concurrent in-flight drains. 0 → 4.
	Workers int

	// MaxAttempts is the per-row retry ceiling. 0 → 5.
	MaxAttempts int

	// BackoffBase is the base delay for exponential backoff between
	// retries (delay = BackoffBase * 2^(attempts-1)). 0 → 1 second.
	BackoffBase time.Duration

	// PollFallback is the maximum idle time before the dispatcher
	// re-scans even without a wake signal. Protects against missed
	// signals after process restart. 0 → 30 seconds.
	PollFallback time.Duration
}

// DrainerMetrics exposes counters useful for the manager UI + tests.
// All fields are atomic-safe to read concurrently with worker activity.
type DrainerMetrics struct {
	DrainsAttempted atomic.Int64
	DrainsSucceeded atomic.Int64
	DrainsFailed    atomic.Int64
	DrainsRetried   atomic.Int64
	Quarantined     atomic.Int64
	BytesDrained    atomic.Int64
	InFlight        atomic.Int64
}

// NewDrainer constructs but does not start the drainer. Call Start to
// begin processing.
func NewDrainer(spool *SpoolStore, cfg DrainerConfig) (*Drainer, error) {
	if spool == nil {
		return nil, errors.New("drainer: spool is required")
	}
	if cfg.FuseRoot == "" {
		return nil, errors.New("drainer: FuseRoot is required")
	}
	if cfg.Workers <= 0 {
		// 8 (up from 4): a large SD-card offload (tens of thousands of RAW
		// files) ingests far faster than 4 drain workers can copy+verify into
		// FUSE, so the spool backlog grows and capacity backpressure engages
		// sooner than necessary. 8 roughly doubles drain throughput to better
		// track ingest. Each worker is I/O-bound (FUSE copy + at-rest re-read
		// SHA), so this trades a bounded rise in concurrent JuiceFS writeback
		// pressure (--buffer-size 4096) for backlog headroom; tune if the
		// writeback buffer saturates on slower uplinks.
		cfg.Workers = 8
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 1 * time.Second
	}
	if cfg.PollFallback <= 0 {
		cfg.PollFallback = 30 * time.Second
	}
	return &Drainer{
		spool:        spool,
		fuseRoot:     cfg.FuseRoot,
		workers:      cfg.Workers,
		maxAttempts:  cfg.MaxAttempts,
		backoffBase:  cfg.BackoffBase,
		pollFallback: cfg.PollFallback,
		sem:          make(chan struct{}, cfg.Workers),
		notify:       make(chan struct{}, 1),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}, nil
}

// Start launches the dispatcher goroutine and registers the wake
// callback with SpoolStore. Safe to call only once.
//
// Sends an initial wake so any rows that landed in `ready` state
// before Start (e.g. recovered from a previous boot, or written
// during the brief window between SpoolStore construction and
// Drainer.Start) are picked up immediately rather than waiting for
// the first pollFallback tick.
func (d *Drainer) Start() {
	d.spool.SetDrainerWake(d.wakeNonBlocking)
	d.wakeNonBlocking()
	go d.dispatchLoop()
}

// Stop signals shutdown and waits for in-flight drains to complete
// within the deadline. Returns true if all drains drained cleanly,
// false if the deadline was hit (some drains will resume on next boot
// via the slice-F scrubber).
//
// Idempotent — second call returns immediately with true.
func (d *Drainer) Stop(deadline time.Duration) bool {
	select {
	case <-d.stop:
		return true
	default:
		close(d.stop)
	}
	// Wait for dispatcher to exit.
	<-d.done
	// Wait for in-flight workers with deadline.
	doneCh := make(chan struct{})
	go func() {
		d.inFlight.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
		return true
	case <-time.After(deadline):
		return false
	}
}

// Metrics returns the live counter struct. Caller may read fields
// concurrently with worker activity.
func (d *Drainer) Metrics() *DrainerMetrics { return &d.metrics }

// SetOnDrainComplete registers a callback invoked once per successful drain,
// after the row is marked done. Must be called BEFORE Start.
func (d *Drainer) SetOnDrainComplete(fn func(nfsPath string, size int64)) {
	d.onDrainComplete = fn
}

// wakeNonBlocking is the callback handed to SpoolStore.SetDrainerWake.
// Sends on the notify channel without blocking — if the dispatcher is
// already pending a wake, the signal is collapsed (we only need to know
// "there's something to do", not how many things).
func (d *Drainer) wakeNonBlocking() {
	select {
	case d.notify <- struct{}{}:
	default:
	}
}

// dispatchLoop is the single goroutine that walks ready rows and hands
// them to workers. Sleeps on notify; falls back to pollFallback for
// missed signals.
func (d *Drainer) dispatchLoop() {
	defer close(d.done)
	for {
		select {
		case <-d.stop:
			return
		case <-d.notify:
		case <-time.After(d.pollFallback):
		}

		// Drain all currently-ready rows before going back to sleep.
		for {
			select {
			case <-d.stop:
				return
			default:
			}
			rows, err := d.spool.Meta().ListReady(d.workers * 2)
			if err != nil {
				log.Printf("drainer: list ready: %v", err)
				break
			}
			if len(rows) == 0 {
				break
			}
			for _, row := range rows {
				if !d.dispatchRow(row) {
					return // stop signal received
				}
			}
		}
	}
}

// dispatchRow claims a single ready row and hands it to a worker.
// Returns false if stop was signaled (and the claimed row, if any, was
// returned to ready).
//
// Concurrency note (reviewer CRITICAL fix): inFlight.Add MUST happen
// BEFORE the sem channel send, not after. If Add ran inside the spawned
// goroutine, Stop's sequence (close stop → wait dispatcher done →
// inFlight.Wait()) could observe count=0 in the window between the sem
// send unblocking and the goroutine actually executing Add. Stop would
// then return prematurely with workers still mid-drain.
func (d *Drainer) dispatchRow(row *metadata.SpoolRow) bool {
	claimed, err := d.spool.Meta().MarkDraining(row.ID)
	if err != nil {
		log.Printf("drainer: claim %d: %v", row.ID, err)
		return true
	}
	if !claimed {
		// Another worker beat us (unlikely with one dispatcher, but
		// defensive — could happen after recovery).
		return true
	}

	// Re-read the row AFTER the claim and drain from the fresh copy, never
	// the ListReady snapshot (adversarial-review BUG A). The snapshot can be
	// minutes stale under a worker backlog; a rename in that window updates
	// a ready row's nfs_path IN PLACE (id and state unchanged), so a claim
	// keyed only on (id, state=ready) succeeds and a snapshot-path drain
	// would write to the OLD path — silently undoing the rename and leaving
	// a dead index entry shadowing the new path. Post-claim the row is in
	// `draining`, so any later migration takes the DELETE+requeue path and
	// this drain's MarkDone observes done=false and undoes itself (QA-37
	// contract) — making the re-read stable.
	fresh, err := d.spool.Meta().Get(row.ID)
	if errors.Is(err, sql.ErrNoRows) {
		// Row deleted between claim and re-read (CancelForDelete) — nothing
		// to drain; the cancel path owns file + capacity cleanup.
		return true
	}
	if err != nil {
		// Transient read failure: surrender the claim so the row is retried.
		log.Printf("drainer: post-claim re-read %d: %v — returning to ready", row.ID, err)
		_ = d.spool.Meta().ResetToReady(row.ID)
		return true
	}
	row = fresh

	// WaitGroup Add happens BEFORE sem to satisfy Stop's invariant
	// (see fn doc above). The InFlight metric, by contrast, tracks
	// *actively-running* drains and is bumped inside the goroutine
	// AFTER the sem slot is held — so it reflects worker concurrency
	// for the UI, not "claimed but slot-blocked" count.
	d.inFlight.Add(1)

	select {
	case <-d.stop:
		d.inFlight.Done()
		_ = d.spool.Meta().ResetToReady(row.ID)
		return false
	case d.sem <- struct{}{}:
	}

	go func(r *metadata.SpoolRow) {
		d.metrics.InFlight.Add(1)
		defer func() {
			d.metrics.InFlight.Add(-1)
			<-d.sem
			d.inFlight.Done()
		}()
		d.drainOne(r)
	}(row)
	return true
}

// drainOne copies a single spool file into the FUSE mount, SHA-verifies
// the copy, and dispositions the row.
func (d *Drainer) drainOne(row *metadata.SpoolRow) {
	d.metrics.DrainsAttempted.Add(1)

	if row.DrainAttempts >= d.maxAttempts {
		// Exhausted before we even started this attempt.
		d.failPermanent(row, fmt.Sprintf("retry budget exhausted (%d attempts)", row.DrainAttempts))
		return
	}

	dest := filepath.Join(d.fuseRoot, row.NFSPath)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		d.failTransient(row, fmt.Errorf("mkdir parent: %w", err))
		return
	}

	src, err := os.Open(row.SpoolFile)
	if err != nil {
		if os.IsNotExist(err) {
			// Spool file gone — nothing to do. Mark failed permanently
			// since there's no data to retry.
			d.failPermanent(row, "spool file missing on disk")
			return
		}
		d.failTransient(row, fmt.Errorf("open spool: %w", err))
		return
	}
	defer src.Close()

	dst, err := os.Create(dest)
	if err != nil {
		d.failTransient(row, fmt.Errorf("create dest: %w", err))
		return
	}

	h := sha256.New()
	mw := io.MultiWriter(dst, h)
	buf := make([]byte, 1<<20)
	n, copyErr := io.CopyBuffer(mw, src, buf)
	// fsync the FUSE destination before closing so JuiceFS --writeback stages
	// the bytes coherently. Without it, the at-rest re-read below is a
	// read-after-close against the writeback cache and can momentarily return
	// inconsistent bytes under a many-file burst — which the SHA check then
	// mis-diagnoses as a permanent bit flip and quarantines an intact file
	// (observed ~0.3% under a 2000-file storm). In writeback mode fsync
	// flushes to the local cache (it does NOT wait for the MinIO upload), so
	// it is cheap relative to the full readback that follows.
	var syncErr error
	if copyErr == nil {
		syncErr = dst.Sync()
	}
	closeErr := dst.Close()
	if copyErr != nil {
		_ = os.Remove(dest) // best-effort cleanup of partial write
		d.failTransient(row, fmt.Errorf("copy to fuse: %w", copyErr))
		return
	}
	if syncErr != nil {
		_ = os.Remove(dest)
		d.failTransient(row, fmt.Errorf("sync fuse dest: %w", syncErr))
		return
	}
	if closeErr != nil {
		_ = os.Remove(dest)
		d.failTransient(row, fmt.Errorf("close fuse fd: %w", closeErr))
		return
	}
	if n != row.Size {
		_ = os.Remove(dest)
		d.failTransient(row, fmt.Errorf("size mismatch: expected %d, got %d", row.Size, n))
		return
	}

	copyStreamSHA := h.Sum(nil)
	if len(row.SHA256) > 0 && !bytes.Equal(copyStreamSHA, row.SHA256) {
		// The bytes we read out of the spool file disagree with the
		// streaming SHA recorded at spool-write time → bit flip on the
		// spool SSD between write and re-read. Quarantine; retry would
		// just re-detect.
		d.quarantine(row, fmt.Sprintf("sha mismatch (spool): streamed=%x diskread=%x", row.SHA256, copyStreamSHA))
		_ = os.Remove(dest)
		return
	}

	// HIGH-1 fix (slice B reviewer): re-read the destination through
	// the FUSE mount and re-hash, then compare. The streaming hash
	// above proves "bytes we sent to dst.Write match row.SHA256", but
	// it does NOT prove what landed at-rest in JuiceFS — FUSE-side
	// page cache or JuiceFS writeback could have corrupted the bytes
	// in flight. The disk-readback is one sequential pass per file;
	// negligible on a video-editor machine vs the cost of silently
	// drained corruption.
	//
	// row.SHA256 is now ALWAYS populated: sequential writes carry the streaming
	// hash, and finalizeLocked derives a disk reference for out-of-order writes
	// (so this at-rest check, and the spool-SSD check above, run for EVERY
	// file). The len>0 guard remains only as defense for a finalize-time disk
	// hash that failed to compute.
	if len(row.SHA256) > 0 {
		atRestSHA, _, hashErr := hashSpoolFile(dest)
		if hashErr != nil {
			// Could not re-read what we just wrote. Treat as transient.
			d.failTransient(row, fmt.Errorf("post-copy reread for SHA verify: %w", hashErr))
			return
		}
		if !bytes.Equal(atRestSHA, row.SHA256) {
			// The FUSE at-rest copy differs from the verified reference, but the
			// spool-side check above PASSED — so the spool file holds GOOD data.
			// This is FUSE/writeback corruption (or a transient coherence blip),
			// NOT lost data: re-draining the preserved spool file fixes it.
			// failTransient retries (a blip clears), and on exhaustion
			// failPermanent KEEPS the spool file in place so RetryFailed can
			// recover the good bytes. We deliberately do NOT quarantine here —
			// quarantine moves the file aside and RetryFailed skips it, which
			// for recoverable, good-on-spool data would be needless loss of a
			// photo. (The spool-side mismatch above DOES quarantine — that one
			// is genuine spool-SSD corruption with no good copy.)
			reason := fmt.Sprintf("sha mismatch (fuse at-rest): fuse=%x want=%x — spool good, re-draining", atRestSHA, row.SHA256)
			_ = os.Remove(dest)
			d.failTransient(row, fmt.Errorf("%s", reason))
			return
		}
	}

	done, err := d.spool.MarkDrainComplete(row.ID, row.NFSPath, row.SpoolFile, row.Size)
	if err != nil {
		// Reviewer fix (slice B follow-on): MarkDrainComplete promises
		// the caller will retry on SQL failure. Doing so here means a
		// transient SQLite error (busy, WAL checkpoint) doesn't strand
		// the row in `draining` state with no path to recovery — the
		// dispatcher re-picks it after backoff.
		//
		// Note we ALSO have to remove the destination FUSE write we
		// just made: the row will be retried, which will copy to dest
		// again. Without removal, the retry's os.Create will truncate
		// the existing file (correct outcome), but a sibling
		// concurrent reader could briefly see the not-yet-retried
		// version. Best-effort removal here closes that window.
		_ = os.Remove(dest)
		d.failTransient(row, fmt.Errorf("mark drain complete: %w", err))
		return
	}
	if !done {
		// The NFS layer deleted this path while we were draining (QA-37):
		// the spool row was cancelled out from under us. Undo the FUSE write
		// so the delete sticks instead of resurrecting the file.
		_ = os.Remove(dest)
		log.Printf("drainer: row %d (%s) cancelled mid-drain (deleted) — undid FUSE write", row.ID, row.NFSPath)
		return
	}
	d.metrics.DrainsSucceeded.Add(1)
	d.metrics.BytesDrained.Add(n)
	if d.onDrainComplete != nil {
		d.onDrainComplete(row.NFSPath, row.Size)
	}
}

// failTransient: retryable failure path. Bumps attempts, schedules a
// delayed wake (so the dispatcher re-picks the row after backoff), and
// returns. If the bump would exceed maxAttempts, transitions to
// permanent failure instead.
//
// HIGH-3 fix (reviewer): the backoff sleep does NOT happen in this
// worker goroutine. Holding the semaphore slot during a 30 s backoff
// blocks healthy work for up to that long when all workers are in
// transient failure. Instead, we schedule a separate timer goroutine
// that fires wakeNonBlocking after the delay; the worker returns
// immediately, freeing its sem slot.
func (d *Drainer) failTransient(row *metadata.SpoolRow, err error) {
	// Infrastructure-unavailable (the destination filesystem is gone, not the
	// file): a JuiceFS FUSE mount that vanished mid-drain returns ENXIO
	// ("device not configured"), ENODEV, or ENOTCONN for every op against it.
	// This happens during a restart/unmount window. Spending the per-file
	// attempt budget on an OUTAGE would permanently `failed` every in-flight
	// drain within ~maxAttempts*backoff (≈2.5 min) — and a `failed` row is the
	// last copy of a photo. Treat it as a pause, NOT a per-file failure:
	// requeue to `ready` WITHOUT bumping drain_attempts, and re-drive after a
	// fixed backoff once the mount returns. "We can't lose photos."
	if isInfraUnavailable(err) {
		if rErr := d.spool.Meta().ResetToReady(row.ID); rErr != nil {
			log.Printf("drainer: infra-pause reset %d: %v", row.ID, rErr)
		}
		d.metrics.DrainsRetried.Add(1)
		d.scheduleDelayedWake(infraUnavailableBackoff)
		return
	}
	nextAttempts := row.DrainAttempts + 1
	if nextAttempts >= d.maxAttempts {
		d.failPermanent(row, err.Error())
		return
	}
	if rErr := d.spool.MarkDrainRetry(row.ID, err.Error()); rErr != nil {
		log.Printf("drainer: mark retry %d: %v", row.ID, rErr)
	}
	d.metrics.DrainsRetried.Add(1)
	d.scheduleDelayedWake(d.backoffDuration(nextAttempts))
}

// infraUnavailableBackoff is the fixed delay before re-driving a row that
// failed because the destination FUSE mount was unavailable. Kept short so
// recovery is prompt once the mount returns, but long enough that a sustained
// outage doesn't tight-spin the dispatcher. Under the soft-mount timeout.
const infraUnavailableBackoff = 5 * time.Second

// isInfraUnavailable reports whether err indicates the destination filesystem
// (the JuiceFS FUSE mount) is unavailable, as opposed to a problem with the
// individual file. A dead/unmounting FUSE mount surfaces as ENXIO ("device
// not configured" on Darwin), ENODEV, or ENOTCONN for every operation. These
// are transient outage conditions — the row must be re-driven without
// counting against its permanent-failure budget, never lost. errors.Is walks
// the *os.PathError → syscall.Errno chain through our fmt.Errorf("%w") wraps.
func isInfraUnavailable(err error) bool {
	return errors.Is(err, syscall.ENXIO) ||
		errors.Is(err, syscall.ENODEV) ||
		errors.Is(err, syscall.ENOTCONN)
}

// backoffDuration returns the exponential-backoff delay for the given
// attempt count, capped at 30 s. Pulled out of sleepBackoff so it can
// be reused by both the in-worker path (test compat) and the
// out-of-worker delayed wake.
func (d *Drainer) backoffDuration(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	delay := d.backoffBase << (attempts - 1)
	if delay > 30*time.Second {
		delay = 30 * time.Second
	}
	return delay
}

// scheduleDelayedWake fires wakeNonBlocking after delay. Does NOT hold
// the worker semaphore. Self-cancels on d.stop. Tracked via inFlight
// so Stop waits for in-flight backoff timers within its deadline.
func (d *Drainer) scheduleDelayedWake(delay time.Duration) {
	d.inFlight.Add(1)
	go func() {
		defer d.inFlight.Done()
		select {
		case <-d.stop:
		case <-time.After(delay):
			d.wakeNonBlocking()
		}
	}()
}

// failPermanent: terminal failure path. Marks the row failed (no retry)
// and releases its capacity reservation. Terminal rows are not part of
// the `used` budget — QuarantineDrain and the boot scrubber already
// follow that invariant (RecoverOnBoot rebuilds `used` from
// ready/draining rows only), and RetryFailed re-reserves on requeue.
// Before this release, failed rows pinned their bytes in the budget
// until restart. releaseCapacity floors at zero, so the
// file-already-missing case (reservation never held) cannot drive the
// counter negative.
func (d *Drainer) failPermanent(row *metadata.SpoolRow, reason string) {
	transitioned, err := d.spool.Meta().MarkFailed(row.ID, reason)
	if err != nil {
		log.Printf("drainer: mark failed %d: %v", row.ID, err)
	}
	// Release ONLY when this call actually moved the row to failed
	// (adversarial-review BUG 1). If the row is gone — deleted by
	// CancelForDelete mid-drain, or replaced by a rename-requeue's
	// DELETE+INSERT — its reservation was already released (or transferred
	// to the requeued row) by that path; releasing here again would
	// double-release and let the spool over-admit past its cap.
	if transitioned {
		d.spool.releaseCapacity(row.Size)
	}
	d.metrics.DrainsFailed.Add(1)
}

// quarantine: SHA-mismatch path. Moves the spool file aside, marks the
// row failed, drops capacity reservation, evicts the index entry.
func (d *Drainer) quarantine(row *metadata.SpoolRow, reason string) {
	if err := d.spool.QuarantineDrain(row.ID, row.NFSPath, row.SpoolFile, row.Size, reason); err != nil {
		log.Printf("drainer: quarantine %d: %v", row.ID, err)
	}
	d.metrics.Quarantined.Add(1)
}

// DrainOnceForTest runs a SINGLE ListReady scan, processes the rows
// returned, and waits for them to complete. Test-only entry point —
// not used in production.
//
// Unlike the production dispatchLoop, DrainOnceForTest does NOT re-scan
// after rows are reset to ready by failTransient — so a single call
// represents one drain attempt per ready row at the moment the call
// began, regardless of subsequent retry resets. This makes tests
// deterministic when asserting "after one transient failure the row is
// ready with attempts=1" rather than chasing N retries.
//
// HIGH-4 fix (slice B reviewer): if ctx fires before all dispatched
// rows complete, panic rather than silently returning. Silent return
// would race with workers and create flaky tests under heavy CI load.
// Tests should pass generous contexts (≥2 s); a hit on the panic path
// means a real bug or a stalled FUSE call.
//
// Returns the number of rows processed (claimed + handed to workers).
func (d *Drainer) DrainOnceForTest(ctx context.Context) int {
	rows, _ := d.spool.Meta().ListReady(d.workers * 2)
	count := 0
	for _, row := range rows {
		if !d.dispatchRow(row) {
			break
		}
		count++
	}
	doneCh := make(chan struct{})
	go func() { d.inFlight.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-ctx.Done():
		panic("DrainOnceForTest: ctx expired before in-flight drains completed; pass a longer context or check for a stalled FUSE call")
	}
	return count
}

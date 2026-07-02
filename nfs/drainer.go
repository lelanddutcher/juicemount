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
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
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

	// started gates Stop's wait on d.done: set true by Start (which launches
	// the dispatcher that closes d.done). Without it, Stop on a never-started
	// drainer would block forever on <-d.done.
	started atomic.Bool

	inFlight sync.WaitGroup

	metrics DrainerMetrics

	// onDrainComplete, if set, is invoked after a row successfully drains to
	// FUSE (post-MarkDrainComplete). Set once via SetOnDrainComplete BEFORE
	// Start; the handler uses it to sync the drained file's real size into
	// the metadata cache. Worker goroutines read it after Start, so
	// set-before-Start provides the happens-before (no lock needed).
	onDrainComplete func(nfsPath string, size int64)

	// lastDrainSuccessNanos is the wall-clock UnixNano of the most recent
	// COMPLETED drain — a real MinIO PUT that landed and was marked done (the
	// DrainsSucceeded.Add(1) site). It is positive proof the backend is
	// reachable over whatever link this Mac is on RIGHT NOW. The reachability
	// monitor's drain-liveness override (health.WithLivenessHook) reads it via
	// LastDrainSuccess() to suppress a probe-dial FALSE-failure when a cold SYN
	// merely queued behind the drainer's own bulk PUT traffic on a saturated
	// uplink (slow-link false-flap, NFSv3 sprint / task #66 salvage). Stamped
	// ONLY on a genuine success — never on an attempted/in-flight drain — so a
	// real outage (drains stop landing) lets the override lapse within seconds
	// and the normal 2-failure flip proceeds. Atomic: written by worker
	// goroutines, read by the probe-loop goroutine.
	lastDrainSuccessNanos atomic.Int64

	// onSizeReady, if set, publishes the drained file's authoritative size into
	// the metadata store BEFORE MarkDrainComplete evicts the spool shadow, so a
	// fresh read can never observe a post-eviction-but-pre-size (0/partial)
	// Entry.Size (task #65 read-during-active-drain). Set once via SetOnSizeReady
	// BEFORE Start (same set-before-Start happens-before as onDrainComplete).
	// row.Size is authoritative at the call site: the copy is rejected unless
	// n == row.Size.
	onSizeReady func(nfsPath string, size int64)

	// onSymlinkMaterialized, if set, is invoked after a deferred offline
	// symlink is os.Symlink'd onto FUSE at reconnect. Set once via
	// SetOnSymlinkMaterialized BEFORE Start; the handler uses it to
	// ClearLocalOnly on the link's cache entry (the link is now on the
	// backend, no longer local-only). The dispatcher goroutine reads it on
	// the reconnect edge, so set-before-Start provides the happens-before.
	onSymlinkMaterialized func(linkPath string)

	// atRestVerify gates the post-copy at-rest SHA re-read (re-opening the
	// just-written file through JuiceFS and re-hashing it). Default OFF: with
	// --writeback disabled (a86eaf6) dst.Sync() already blocks until the
	// durable MinIO PUT, and the spool-side SHA check already proved we sent
	// the right bytes, so the re-read is redundant — AND under a heavy offload
	// it re-reads not-yet-fully-served bytes back through JuiceFS, which
	// deadlocks the FUSE mount (read-after-write on a saturated writeback
	// buffer). Opt back in with JM_DRAIN_ATREST_VERIFY=1 only on a fast/idle
	// backend where the extra read pass is affordable.
	atRestVerify bool

	// batchInsert gates the drain metadata write-coalescer (Lever 1,
	// JM_DRAIN_BATCH_INSERT). Default OFF: behavior is byte-identical to the
	// per-file (onSizeReady → MarkDrainComplete) path. When ON, a
	// successfully-copied+verified drain is enqueued into the coalescer instead
	// of committing its (UpdateSize + MarkDone) pair inline; the coalescer
	// flushes N ops or T ms — whichever first — in ONE cross-table SQLite
	// transaction (metadata.Store.BatchDrainComplete), collapsing N fsync/
	// syscall cascades into 1 under a many-file write storm. Task #65's
	// size-publish-before-eviction ordering is preserved by construction: the
	// per-file UpdateSize and MarkDone land in the same committed tx, and the
	// batch's post-commit eviction cleanup runs only after that commit. See
	// drainer_batch.go.
	batchInsert bool

	// batch is the write-coalescer; non-nil only when batchInsert is true. Set
	// in NewDrainer. See drainBatcher.
	batch *drainBatcher

	// onBatchDrainComplete commits a batch of drained-file metadata writes in
	// one transaction and returns per-item done results. Wired (before Start)
	// to metadata.Store.BatchDrainComplete via SetOnBatchDrainComplete. Read
	// by the coalescer flusher goroutine; set-before-Start provides the
	// happens-before. nil ⇒ the batcher falls back to per-file completion
	// (defensive; production always wires it when the flag is on).
	onBatchDrainComplete func([]metadata.DrainCommitItem) ([]metadata.DrainCommitResult, error)
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
		// 4 concurrent drains. Each worker copies a whole file into JuiceFS and
		// (writeback now OFF, see fix a86eaf6) dst.Sync() BLOCKS until the real
		// MinIO PUT lands. Too many concurrent workers pile that many in-flight
		// 66 MB writes into JuiceFS's --buffer-size buffer; on a real
		// ~40 GB+ offload the buffer saturates, JuiceFS goes readdir-
		// unresponsive (wedge), the health watchdog then SIGKILLs+remounts it,
		// and the whole copy stalls (observed 2026-06-14 under a 16-way Finder
		// copy). 4 keeps JuiceFS healthy while still overlapping uploads; the
		// spool absorbs the burst and capacity backpressure paces ingest to
		// drain speed. Tune via JM_DRAIN_WORKERS for faster/slower uplinks.
		cfg.Workers = 4
		if v := os.Getenv("JM_DRAIN_WORKERS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				cfg.Workers = n
			}
		}
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
	d := &Drainer{
		spool:        spool,
		fuseRoot:     cfg.FuseRoot,
		workers:      cfg.Workers,
		maxAttempts:  cfg.MaxAttempts,
		backoffBase:  cfg.BackoffBase,
		pollFallback: cfg.PollFallback,
		atRestVerify: os.Getenv("JM_DRAIN_ATREST_VERIFY") == "1",
		batchInsert:  os.Getenv("JM_DRAIN_BATCH_INSERT") == "1",
		sem:          make(chan struct{}, cfg.Workers),
		notify:       make(chan struct{}, 1),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
	if d.batchInsert {
		d.batch = newDrainBatcher(d, drainBatchMaxOps, drainBatchMaxDelay)
	}
	return d, nil
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
	d.started.Store(true)
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
	// Never started: there's no dispatcher to wait on (d.done is never closed
	// without Start), so closing d.stop and blocking on <-d.done would deadlock.
	// A constructed-but-unstarted drainer has nothing in flight — return clean.
	// (SetSpool wires a drainer that the binary always Starts; this guards the
	// degenerate path and keeps Stop safe for tests/short-lived instances.)
	if !d.started.Load() {
		select {
		case <-d.stop:
		default:
			close(d.stop)
		}
		return true
	}
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
		// Lever 1: once every worker has finished enqueueing, flush the final
		// partial batch so no drained file's metadata commit is stranded at
		// shutdown. Runs inside the wait goroutine so it completes within the
		// same deadline the caller granted for in-flight work.
		d.flushBatch()
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

// LastDrainSuccess returns the wall-clock time of the most recent COMPLETED
// drain (a MinIO PUT that landed and was marked done). Returns the zero Time
// if no drain has succeeded yet. Used by the reachability monitor's
// drain-liveness override: a recent success is positive proof the backend is
// reachable over the current link, so a probe-dial timeout that merely queued
// behind the drainer's bulk PUT traffic is suppressed rather than counted as a
// failure. Concurrent-safe (atomic load).
func (d *Drainer) LastDrainSuccess() time.Time {
	ns := d.lastDrainSuccessNanos.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// SetOnDrainComplete registers a callback invoked once per successful drain,
// after the row is marked done. Must be called BEFORE Start.
func (d *Drainer) SetOnDrainComplete(fn func(nfsPath string, size int64)) {
	d.onDrainComplete = fn
}

// SetOnSizeReady registers the pre-eviction size-publish hook (task #65). Call
// once before Start.
func (d *Drainer) SetOnSizeReady(fn func(nfsPath string, size int64)) {
	d.onSizeReady = fn
}

// SetOnBatchDrainComplete registers the batched metadata-commit hook used when
// JM_DRAIN_BATCH_INSERT is on (Lever 1). It must commit every item's size
// publish + spool mark-done in ONE transaction, size-before-done per item, and
// return per-item done results aligned by index (see
// metadata.Store.BatchDrainComplete). Call once BEFORE Start; the coalescer
// flusher reads it after Start (set-before-Start happens-before). A no-op when
// the flag is off.
func (d *Drainer) SetOnBatchDrainComplete(fn func([]metadata.DrainCommitItem) ([]metadata.DrainCommitResult, error)) {
	d.onBatchDrainComplete = fn
}

// SetOnSymlinkMaterialized registers a callback invoked once per deferred
// offline symlink after it is materialized on FUSE at reconnect. Must be
// called BEFORE Start (read by the dispatcher on the reconnect edge).
func (d *Drainer) SetOnSymlinkMaterialized(fn func(linkPath string)) {
	d.onSymlinkMaterialized = fn
}

// flushBatch flushes any pending coalesced drains (Lever 1). No-op when the
// batcher is disabled. Called on the drain-idle edge (ListReady empty), before
// the dispatcher parks (offline / FUSE-identity loss), and on Stop — so a
// partial batch is never stranded and never spans the offline boundary.
func (d *Drainer) flushBatch() {
	if d.batch != nil {
		d.batch.flush()
	}
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
	wasOffline := false
	identityParked := false
	for {
		// When offline, poll at a tighter cadence than pollFallback so we
		// notice reconnection (manual toggle or auto-recovery) within a few
		// seconds and resume draining promptly. Online, the wake signal drives
		// us and pollFallback is just a missed-signal backstop.
		wait := d.pollFallback
		if pin.IsOffline() || !pin.FUSEIdentityOK() {
			wait = drainOfflineRecheck
		}
		select {
		case <-d.stop:
			return
		case <-d.notify:
		case <-time.After(wait):
		}

		// PAUSE while offline. Draining writes into the JuiceFS FUSE mount,
		// which is backed by the (now-unreachable) object store; attempting it
		// offline just hangs then fails — burning each row's drain_attempts
		// budget toward a spurious permanent `failed` (a `failed` row is the
		// last copy of a photo) and churning row state (ready→draining) that
		// the NFS spool-shadow read path keys on. Park the dispatcher until the
		// backend is reachable again; ingest keeps filling the spool meanwhile
		// (offline-ingest sprint) and we drain it on reconnect.
		if pin.IsOffline() {
			// Lever 1: flush any coalesced drains from the just-ended online
			// window before parking, so a partial batch is committed while the
			// backend is still reachable and never spans the offline boundary.
			// (In-flight stragglers are still backstopped by the batch timer.)
			d.flushBatch()
			wasOffline = true
			continue
		}
		// PAUSE while the FUSE identity gate fails (V2.3 G0). When the
		// mountpoint is a plain directory (macFUSE kext not loaded / mount
		// absent) or wedged, os.Create against it SUCCEEDS onto the local
		// boot disk and the "drained" bytes never reach the backend — the
		// 2026-07-01 174GB stranded-writes incident. Park exactly like
		// offline: the spool is the durable safe place; drains resume (with
		// the reconnect requeue below) the moment the mount is real again.
		if ok, reason := pin.FUSEIdentityState(); !ok {
			// Lever 1: flush before parking (see the offline branch).
			d.flushBatch()
			if !identityParked {
				log.Printf("drainer: PARKED — FUSE identity gate failed (%s); writes stay in the spool until the mount is real", reason)
				identityParked = true
			}
			wasOffline = true
			continue
		}
		if identityParked {
			log.Printf("drainer: FUSE identity restored — resuming drains")
			identityParked = false
		}
		// RECONNECT edge: any rows that exhausted their retry budget before or
		// during the offline window are stuck in `failed`. Requeue them now
		// that the backend is reachable so no spooled photo is left undrained.
		// RetryFailed is a no-op (returns 0) when nothing is failed.
		if wasOffline {
			wasOffline = false
			if n, err := d.spool.RetryFailed(); err != nil {
				log.Printf("drainer: reconnect RetryFailed: %v", err)
			} else if n > 0 {
				log.Printf("drainer: reconnect requeued %d failed spool row(s)", n)
			}
			// Symlinks created offline were deferred (juiceFS.Symlink does NOT
			// os.Symlink while offline); materialize them on FUSE now that the
			// backend is reachable. This is the symlink analogue of the file
			// drain — same reconnect path, same offline→online recovery.
			d.materializePendingSymlinks()
		}

		// Drain all currently-ready rows before going back to sleep.
		for {
			select {
			case <-d.stop:
				return
			default:
			}
			// Bail out of the batch if the network dropped mid-drain — or the
			// FUSE identity gate failed (mount unmounted/wedged mid-batch);
			// the outer loop re-evaluates and parks. In-flight workers finish
			// or fail-transient (treated as an offline pause, not a per-file
			// failure — see failTransient).
			if pin.IsOffline() || !pin.FUSEIdentityOK() {
				break
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
				if pin.IsOffline() || !pin.FUSEIdentityOK() {
					break
				}
				if !d.dispatchRow(row) {
					return // stop signal received
				}
			}
		}
		// Lever 1: the ready queue is drained (or we bailed to re-evaluate).
		// Flush whatever workers have enqueued so far rather than making it wait
		// for the batch timer before its size-publish + shadow eviction land.
		// Workers still finishing the last dispatch cycle enqueue after this and
		// are caught by the batch timer (or the next iteration's flush).
		d.flushBatch()
	}
}

// materializePendingSymlinks creates, on the FUSE mount, every symlink that was
// deferred while offline (juiceFS.Symlink persists (link, target) instead of
// os.Symlink'ing when the backend is unreachable). Called on the reconnect edge,
// alongside RetryFailed — the symlink half of offline→online recovery.
//
// For each persisted link: os.Symlink(target, fuseRoot/link), then clear the
// link's LocalOnly flag (via onSymlinkMaterialized → store.ClearLocalOnly) and
// drop the pending row. Idempotent and crash-safe:
//   - An os.IsExist link (already materialized, or a concurrent online create
//     beat us) is treated as success — we still clear LocalOnly and delete the
//     row so the persisted intent doesn't linger.
//   - A missing parent dir (ENOENT) means the link's directory hasn't been
//     materialized yet; MkdirAll the parent and retry once. The drainer's file
//     drain os.MkdirAlls parents too, but a symlink may have no sibling files to
//     trigger that, so we do it here.
//   - The row is deleted ONLY after a successful (or already-exists) os.Symlink,
//     so a crash mid-loop re-materializes the rest next reconnect; a genuine
//     error leaves the row for the next attempt and is logged, never fatal
//     (one bad link must not strand the others or the file drain).
func (d *Drainer) materializePendingSymlinks() {
	// V2.3 G0: os.Symlink into a plain-dir mountpoint would strand the links
	// on the local disk exactly like a misdirected file drain. Rows persist,
	// so skipping here just defers to the next reconnect edge.
	if ok, reason := pin.FUSEIdentityState(); !ok {
		log.Printf("drainer: skipping symlink materialization — FUSE identity gate failed (%s)", reason)
		return
	}
	pend, err := d.spool.Meta().ListPendingSymlinks()
	if err != nil {
		log.Printf("drainer: reconnect list pending symlinks: %v", err)
		return
	}
	if len(pend) == 0 {
		return
	}
	materialized := 0
	for _, p := range pend {
		// Review fix: re-check the gate PER LINK — an unmount mid-loop would
		// otherwise land the remaining links on the plain dir. Cached (2s
		// TTL) so the per-link cost is a mutex read. Bail without deleting
		// rows: they retry next reconnect (os.IsExist path is idempotent).
		if ok, reason := pin.FUSEIdentityState(); !ok {
			log.Printf("drainer: symlink materialization aborted — FUSE identity gate failed (%s); %d row(s) retry next reconnect",
				reason, len(pend)-materialized)
			return
		}
		fusePath := filepath.Join(d.fuseRoot, p.LinkPath)
		serr := os.Symlink(p.Target, fusePath)
		if serr != nil && os.IsNotExist(serr) {
			// Parent dir not present yet — create it and retry once.
			if mErr := os.MkdirAll(filepath.Dir(fusePath), 0o755); mErr == nil {
				serr = os.Symlink(p.Target, fusePath)
			}
		}
		if serr != nil && !os.IsExist(serr) {
			// Transient (e.g. backend flapped back offline mid-materialize):
			// leave the row for the next reconnect, log, keep going.
			log.Printf("drainer: materialize symlink %q -> %q: %v", p.LinkPath, p.Target, serr)
			continue
		}
		// Review fix: close the write-then-delete gap — if the mount vanished
		// between the Symlink and here, the link landed on the plain dir and
		// the row must NOT be deleted. Bail; the retry is idempotent.
		if ok, reason := pin.FUSEIdentityState(); !ok {
			log.Printf("drainer: symlink materialization aborted post-create — FUSE identity gate failed (%s); row %q retries next reconnect",
				reason, p.LinkPath)
			return
		}
		// On-FUSE now (created, or already existed). Clear LocalOnly so the
		// reconcile prune treats it as a normal backend entry, then drop the
		// persisted intent.
		if d.onSymlinkMaterialized != nil {
			d.onSymlinkMaterialized(p.LinkPath)
		}
		if dErr := d.spool.Meta().DeletePendingSymlink(p.LinkPath); dErr != nil {
			log.Printf("drainer: delete pending symlink %q: %v", p.LinkPath, dErr)
		}
		materialized++
	}
	if materialized > 0 {
		log.Printf("drainer: reconnect materialized %d deferred symlink(s)", materialized)
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

	// V2.3 G0: last-line identity guard for a worker that raced the
	// dispatcher's park (mount unmounted between dispatch and here). Writing
	// into a plain-dir mountpoint "succeeds" onto the local disk and strands
	// the bytes — refuse and pause instead (ErrFUSEIdentityGate routes to
	// failTransient's infra-pause branch: requeue, no budget burn).
	if ok, reason := pin.FUSEIdentityState(); !ok {
		d.failTransient(row, fmt.Errorf("%s: %w", reason, pin.ErrFUSEIdentityGate))
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
	// Review fix (phase-1 adversarial review, HIGH — the TOCTOU that would
	// have reopened the 174GB incident): the path-based identity gate above
	// reads a ≤2s-stale cache, so a mount torn down moments before Create
	// can still pass it and the file lands on the plain dir — where every
	// downstream safeguard (copy, fsync, SHA, at-rest re-read) verifies the
	// same local bytes and "succeeds". The fd's filesystem binding is
	// immutable, so checking the OPEN fd is race-free regardless of what
	// mounts or unmounts afterwards.
	if err := pin.FUSEIdentityCheckFD(dst.Fd()); err != nil {
		dst.Close()
		_ = os.Remove(dest)
		d.failTransient(row, err)
		return
	}
	// Capture where dest actually lives for the pre-completion device check.
	destDev := int32(-1)
	if fi, err := dst.Stat(); err == nil {
		if sys, ok := fi.Sys().(*syscall.Stat_t); ok {
			destDev = sys.Dev
		}
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
	if d.atRestVerify && len(row.SHA256) > 0 {
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

	// Review fix (phase-1 adversarial review, belt-and-suspenders half of the
	// TOCTOU close): before declaring the drain complete — which deletes the
	// spool copy, the only other copy of these bytes — confirm dest still
	// lives on the SAME device as the CURRENT mountpoint. If a remount landed
	// mid-copy, the bytes went to a dead mount instance (or the plain dir,
	// though the fd check at Create already catches that) and the current
	// mount does not have them: requeue instead of completing. os.Stat on a
	// healthy mount is cheap; on a wedged one it can block — same hazard
	// class as the at-rest re-read above (drainer worker, not the hot path).
	if destDev != -1 {
		if fi, statErr := os.Stat(d.fuseRoot); statErr == nil {
			if sys, ok := fi.Sys().(*syscall.Stat_t); ok && sys.Dev != destDev {
				_ = os.Remove(dest)
				d.failTransient(row, fmt.Errorf("dest device %d != current mount device %d (remount raced the copy): %w",
					destDev, sys.Dev, pin.ErrFUSEIdentityGate))
				return
			}
		}
	}

	// Lever 1 (JM_DRAIN_BATCH_INSERT): hand the fully-copied+verified drain to
	// the write-coalescer instead of committing its (UpdateSize + MarkDone) pair
	// inline. The coalescer batches many files' metadata writes into one SQLite
	// transaction, cutting fsync/syscall volume under a write storm. Correctness
	// is unchanged: BatchDrainComplete commits the size publish BEFORE the
	// mark-done per file (task #65), and the post-commit eviction cleanup runs
	// only after that tx commits — so this file's size is durable before its
	// spool shadow is evicted, exactly as the per-file path guarantees below.
	if d.batch != nil {
		d.batch.enqueue(pendingDrain{row: row, dest: dest, n: n})
		return
	}

	// task #65: publish the real size into the metadata store BEFORE eviction.
	// The backend file is whole + SHA-verified here and row.Size is authoritative
	// (n == row.Size enforced above), so this closes the eviction-before-publish
	// gap: once MarkDrainComplete evicts the spool shadow, fresh reads resolve
	// Entry.Size — now correct — instead of a stale 0/partial value. Safe during
	// the brief shadow overlap: for a sequential file ContiguousEnd == row.Size,
	// so the shadow and the store agree. It is on the drain's critical path (not
	// a queued callback), so it stays correct under a burst.
	if d.onSizeReady != nil {
		d.onSizeReady(row.NFSPath, row.Size)
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
	// Stamp drain liveness: a real MinIO PUT just landed, so the backend is
	// provably reachable over the current (possibly congested) link. The
	// reachability monitor consults this via LastDrainSuccess() to suppress a
	// probe-dial false-failure caused by the drainer's own uplink saturation.
	// Done ONLY here, on a COMPLETED success — never on an attempt/in-flight.
	d.lastDrainSuccessNanos.Store(time.Now().UnixNano())
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
	//
	// OFFLINE is the same class of outage from the row's perspective: a drain
	// that was in flight when the link dropped (or that raced the dispatcher's
	// pause) fails against the unreachable backend with an error that is NOT
	// necessarily ENXIO (the FUSE mount is still up; only MinIO/Redis is gone),
	// so isInfraUnavailable wouldn't catch it. Gate on pin.IsOffline() too:
	// don't spend this photo's per-file budget on a network outage. The
	// dispatcher is already parked offline, so this just resets the in-flight
	// row to `ready`; it drains on reconnect.
	if isInfraUnavailable(err) || pin.IsOffline() || errors.Is(err, pin.ErrFUSEIdentityGate) {
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

// drainOfflineRecheck is how often the parked dispatcher re-checks pin state
// while offline, so it resumes draining within a few seconds of reconnect
// rather than waiting a full pollFallback. Well under the NFS soft-mount
// window; reconnect itself is ~instant (manual toggle / auto recovery), so
// this bounds the resume latency without tight-spinning.
const drainOfflineRecheck = 3 * time.Second

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
	// Review FIX 3: evict the in-memory index entry, identity-checked on the
	// row id (mirroring QuarantineDrain). A permanently-failed drain is one of
	// the entry's eviction points — without this, HasPending(nfsPath) would
	// keep returning true for the process lifetime (the boot scrubber does not
	// repopulate the index for failed rows), so QA-30 Layer D would treat the
	// path as perpetually spool-pending and refuse to ever prune it — even
	// after the user deletes it. The identity check makes a no-op safe if a
	// newer writer (rename-requeue / re-open) already rotated in a fresh entry.
	d.spool.EvictIndex(row.NFSPath, row.ID)
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
	// Lever 1: workers enqueue into the coalescer instead of committing inline,
	// so a single test scan leaves a partial batch. Flush it synchronously here
	// so the same post-conditions the per-file path asserts (row done, spool
	// file gone, size published, capacity released) hold when this returns.
	d.flushBatch()
	return count
}

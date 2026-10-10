package nfs

import (
	"errors"
	"log"
	"os"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// errBatchResultMissing is the (should-never-happen) transient error used when
// BatchDrainComplete returns no result for a spool id in the batch. Routing it
// through failTransient re-drives the row rather than leaving it stranded in
// `draining`.
var errBatchResultMissing = errors.New("drainer: batch commit returned no result for row")

// Lever 1 — drain metadata write-coalescer (JM_DRAIN_BATCH_INSERT).
//
// PROBLEM. The drainer commits a SEPARATE SQLite write per drained file: the
// size publish (onSizeReady → Store.UpdateSize) and the mark-done
// (MarkDrainComplete → SpoolStore.MarkDone) are two transactions, each its own
// WAL append + eventual fsync. Under a many-file copy storm that is N × 2
// transactions competing with foreground NFS serving on the same DB — a
// reducible slice of the syscall/scheduler load the CPU profile flagged.
//
// FIX. When the flag is on, a copied+verified drain is enqueued here instead of
// committing inline. The coalescer flushes on the FIRST of two thresholds —
// drainBatchMaxOps ops accumulated, or drainBatchMaxDelay elapsed since the
// batch opened — committing every pending file's (size publish + mark-done) in
// ONE cross-table transaction (metadata.Store.BatchDrainComplete). N drains → 1
// transaction → 1 fsync cascade.
//
// TASK #65 ORDERING (size-publish-before-eviction). Preserved by construction:
//   - BatchDrainComplete qualifies each spool row and commits its size and
//     done state atomically in one transaction.
//   - The eviction side-effects for a file (index evict, spool-file removal,
//     capacity release — SpoolStore.BatchCompleteDrainCleanup) run ONLY after
//     that tx commits.
// So a file's authoritative size is durably published no later than its
// mark-done, and its spool shadow is not evicted until after the commit — a
// fresh read can never resolve a post-eviction-but-pre-size 0/partial size.
// This is the same guarantee the per-file path gives, with fewer fsyncs.
//
// NO STRANDED WRITES. A partial batch is never left open: the dispatcher flushes
// on drain-idle (ListReady empty), before parking offline / on FUSE-identity
// loss, and on Stop. The batch never spans the offline/online boundary — the
// dispatcher flushes before it parks.
//
// ON COMMIT FAILURE. The whole tx rolls back; each spool row is retained for
// retry. A public pathname may now belong to a replacement, so completion
// failure never authorizes deleting it.

const (
	// drainBatchMaxOps flushes once this many drained files are pending. Sized
	// so one flush amortizes the transaction/fsync cost across a healthy chunk
	// of a copy storm without letting the batch (and its held-open spool files
	// awaiting eviction) grow unbounded. Under the default 4 workers this is
	// ~64 dispatch cycles' worth; the time threshold bounds latency when the
	// arrival rate is lower.
	drainBatchMaxOps = 256

	// drainBatchMaxDelay caps how long the first file in a batch waits before
	// its size is published + its shadow evicted. Well under the NFS soft-mount
	// window and the spool sweeper idle, so a batched file's post-drain Stat
	// reflects the real size promptly. Keeps a trickle workload (a few files)
	// from sitting in the buffer until the op threshold that may never arrive.
	drainBatchMaxDelay = 50 * time.Millisecond
)

// pendingDrain is one copied+verified drain awaiting its coalesced metadata
// commit. Everything the post-commit disposition needs is captured at enqueue
// time so the flusher touches no per-worker state.
type pendingDrain struct {
	row             *metadata.SpoolRow
	dest            string    // observed FUSE destination; never deletion authority
	n               int64     // bytes copied (== row.Size; verified before enqueue)
	mtime           time.Time // authoritative post-Chtimes backend timestamp
	fd              *os.File  // retained original destination description; final close precedes cleanup
	sidecarBody     []byte    // bounded body read from the exact private source FD
	backingIdentity string
}

func (p *pendingDrain) closeFD() error {
	if p.fd == nil {
		return nil
	}
	f := p.fd
	p.fd = nil
	return f.Close()
}

// drainBatcher accumulates pendingDrains and flushes them in one transaction on
// an op-count or time threshold. Enqueue is called from many worker goroutines;
// flush from a timer goroutine and from the dispatcher (idle/park/Stop).
type drainBatcher struct {
	d        *Drainer
	maxOps   int
	maxDelay time.Duration

	mu      sync.Mutex
	pending []pendingDrain
	timer   *time.Timer // fires maxDelay after the batch opened; nil when empty

	// flushMu serializes flush bodies (the SQL commit + per-item cleanup) so a
	// timer flush and a threshold/idle/Stop flush never run cleanup
	// concurrently. Each pendingDrain is drained by exactly one flush (moved out
	// under mu), so this only orders the commits — it never holds across an
	// enqueue.
	flushMu sync.Mutex
}

func newDrainBatcher(d *Drainer, maxOps int, maxDelay time.Duration) *drainBatcher {
	return &drainBatcher{d: d, maxOps: maxOps, maxDelay: maxDelay}
}

// enqueue adds a copied+verified drain to the batch. Flushes inline if the op
// threshold is reached; otherwise arms the time-threshold timer on the first
// item of a batch. The inline threshold flush runs in the calling worker
// goroutine — acceptable because the worker already holds no sem-blocking lock
// here and the flush is bounded (one tx + N cheap cleanups).
func (b *drainBatcher) enqueue(p pendingDrain) {
	b.mu.Lock()
	b.pending = append(b.pending, p)
	// Arm the time threshold on the batch's first item.
	if b.timer == nil {
		b.timer = time.AfterFunc(b.maxDelay, b.flushOnTimer)
	}
	overOps := len(b.pending) >= b.maxOps
	b.mu.Unlock()

	if overOps {
		b.flush()
	}
}

// flushOnTimer is the time-threshold callback. A no-op if a threshold/idle flush
// already drained the batch (pending empty) in the interim.
func (b *drainBatcher) flushOnTimer() { b.flush() }

// flush commits all currently-pending drains in one transaction and disposes of
// each per its result. Safe to call with an empty batch (no-op). Callers:
// enqueue (op threshold), the time timer, and the dispatcher (idle / pre-park /
// Stop).
func (b *drainBatcher) flush() {
	// Serialize flush bodies. The swap below makes each item belong to exactly
	// one flush, but two concurrent flushes could still interleave their SQL
	// commits + cleanup; flushMu keeps them strictly ordered.
	b.flushMu.Lock()
	defer b.flushMu.Unlock()

	b.mu.Lock()
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	if len(b.pending) == 0 {
		b.mu.Unlock()
		return
	}
	batch := b.pending
	b.pending = nil
	b.mu.Unlock()

	b.commitAndDispose(batch)
}

// commitAndDispose commits the batch's metadata writes in one transaction, then
// dispositions each file. Mirrors the per-file drainOne tail:
//   - commit error → every file failTransient, retaining backing paths;
//   - result.Done == false → retain backing paths, no callbacks or success metrics;
//   - result.Done == true → BatchCompleteDrainCleanup (index evict, capacity
//     release, spool-file remove, manifest) + success metrics + liveness stamp +
//     onDrainComplete (the success tail).
func (b *drainBatcher) commitAndDispose(batch []pendingDrain) {
	if !b.d.hasTypedDrainHooks() {
		b.commitAndDisposeLegacy(batch)
		return
	}
	if !b.d.typedDrainReady(true) {
		for _, p := range batch {
			b.d.holdCompletion(p.row, errDrainEffectsUnwired)
			_ = p.closeFD()
		}
		return
	}
	// A batch may hold 256 rows; namespace admission permits 24 concurrent
	// paths. Commit bounded cohorts so ordinary large batches do not re-copy.
	for len(batch) > 0 {
		n := len(batch)
		if n > 16 {
			n = 16
		}
		b.commitExactCohort(batch[:n])
		batch = batch[n:]
	}
}

func (b *drainBatcher) commitExactCohort(batch []pendingDrain) {
	d := b.d
	type admitted struct {
		p  pendingDrain
		op *namespaceOperation
	}
	ready := make([]admitted, 0, len(batch))
	items := make([]metadata.DrainCommitItem, 0, len(batch))
	for _, p := range batch {
		op := d.onDrainAdmission(p.row.NFSPath)
		if op == nil {
			if ok, err := d.spool.Meta().RequeueUnadmittedDrain(itemsForPending(p)); err != nil || !ok {
				log.Printf("drainer: lease refusal could not requeue row %d: %v", p.row.ID, err)
			}
			if err := p.closeFD(); err != nil {
				log.Printf("drainer: close refused row %d: %v", p.row.ID, err)
			}
			continue
		}
		ready = append(ready, admitted{p: p, op: op})
		items = append(items, itemsForPending(p))
	}
	if len(ready) == 0 {
		return
	}
	results, err := d.onBatchDrainCompleteExact(items)
	if err != nil {
		for _, a := range ready {
			d.holdCompletion(a.p.row, err)
			if closeErr := a.p.closeFD(); closeErr != nil {
				log.Printf("drainer: close held row %d: %v", a.p.row.ID, closeErr)
			}
			a.op.finish(nil)
		}
		return
	}
	byID := make(map[int64]metadata.DrainCommitResult, len(results))
	duplicate := make(map[int64]bool)
	for _, r := range results {
		if _, ok := byID[r.SpoolID]; ok {
			duplicate[r.SpoolID] = true
		}
		byID[r.SpoolID] = r
	}
	for _, a := range ready {
		r, ok := byID[a.p.row.ID]
		if !ok || duplicate[a.p.row.ID] {
			d.holdCompletion(a.p.row, errBatchResultMissing)
			_ = a.p.closeFD()
		} else {
			d.finishAcknowledged(&a.p, r, a.op)
		}
		a.op.finish(nil)
	}
}

func (b *drainBatcher) commitAndDisposeLegacy(batch []pendingDrain) {
	d := b.d

	// Defensive fallback: if the batched-commit hook was never wired (should not
	// happen in production when the flag is on), complete each file the per-file
	// way, retaining custody if that transaction hook is also missing.
	if d.onBatchDrainComplete == nil {
		for _, p := range batch {
			b.disposePerFile(p)
		}
		return
	}

	items := make([]metadata.DrainCommitItem, len(batch))
	for i, p := range batch {
		items[i] = itemsForPending(p)
	}

	results, err := d.onBatchDrainComplete(items)
	if err != nil {
		// Retry retained spool rows. SQL uncertainty grants no ownership of
		// a public backing pathname, which may already contain a replacement.
		for _, p := range batch {
			d.holdCompletion(p.row, err)
			_ = p.closeFD()
		}
		return
	}

	// Index results by spool id so a (defensive) reordering can't misalign.
	doneByID := make(map[int64]metadata.DrainCommitResult, len(results))
	for _, r := range results {
		doneByID[r.SpoolID] = r
	}

	for _, p := range batch {
		defer p.closeFD()
		result, ok := doneByID[p.row.ID]
		if !ok {
			// A result went missing for this id — treat as transient so the row
			// isn't stranded in `draining`. Should never happen (results align
			// with items), but never leave a claimed row unresolved.
			d.holdCompletion(p.row, errBatchResultMissing)
			continue
		}
		if !result.Done {
			// The row no longer qualifies. A public pathname is not proof
			// of operation ownership; preserve it and suppress callbacks.
			log.Printf("drainer: row %d (%s) batch completion rejected — backing pathname retained", p.row.ID, p.row.NFSPath)
			continue
		}
		if result.CompletionID == "" {
			d.holdCompletion(p.row, errBatchResultMissing)
			continue
		}
		if err := d.spool.Meta().ValidateDrainCompletion(itemsForPending(p), result); err != nil {
			d.holdCompletion(p.row, err)
			continue
		}
		if d.onDrainMetadataReady != nil {
			d.onDrainMetadataReady(p.row.NFSPath, p.row.Size, p.mtime)
		}
		if err := p.closeFD(); err != nil {
			d.holdCompletion(p.row, err)
			continue
		}
		// Committed: run the identical post-commit eviction cleanup + success
		// tail the per-file path runs after MarkDrainComplete.
		if err := d.spool.CompleteAcknowledgedDrain(metadata.DrainCommitItem{SpoolID: p.row.ID, NFSPath: p.row.NFSPath, SpoolFile: p.row.SpoolFile, Size: p.row.Size}, result); err != nil {
			log.Printf("drainer: completion cleanup held row %d: %v", p.row.ID, err)
			continue
		}
		d.metrics.DrainsSucceeded.Add(1)
		d.metrics.BytesDrained.Add(p.n)
		d.lastDrainSuccessNanos.Store(time.Now().UnixNano())
		if d.onDrainComplete != nil {
			d.onDrainComplete(p.row.NFSPath, p.row.Size, p.mtime)
		}
	}
}

// disposePerFile uses the same qualified completion as the default inline
// path. If neither transaction hook was wired, retain custody and retry.
func (b *drainBatcher) disposePerFile(p pendingDrain) {
	b.d.completeInline(p)
}

var errDrainEffectsUnwired = errors.New("drainer: incomplete generation-bound completion wiring")

func (d *Drainer) hasTypedDrainHooks() bool {
	return d.onDrainAdmission != nil || d.onDrainEffects != nil ||
		d.onBatchDrainCompleteExact != nil || d.onDrainSucceeded != nil
}

func (d *Drainer) typedDrainReady(batch bool) bool {
	return d.onDrainAdmission != nil && d.onDrainEffects != nil && d.onDrainCommit != nil &&
		(!batch || d.onBatchDrainCompleteExact != nil)
}

// Complete generation-bound effects before releasing the private source. Only
// callers with every typed hook absent may use the legacy observer tail.
func (d *Drainer) completeInline(p pendingDrain) {
	defer p.closeFD()
	if d.hasTypedDrainHooks() && !d.typedDrainReady(false) {
		d.holdCompletion(p.row, errDrainEffectsUnwired)
		return
	}
	var op *namespaceOperation
	if d.onDrainAdmission != nil {
		op = d.onDrainAdmission(p.row.NFSPath)
		if op == nil {
			if ok, err := d.spool.Meta().RequeueUnadmittedDrain(itemsForPending(p)); err != nil || !ok {
				log.Printf("drainer: inline lease refusal could not requeue row %d: %v", p.row.ID, err)
			}
			return
		}
		defer op.finish(nil)
	}
	if d.onDrainCommit == nil {
		d.holdCompletion(p.row, errors.New("drainer: exact-size completion hook not wired"))
		return
	}
	result, err := d.onDrainCommit(itemsForPending(p))
	if err != nil {
		d.holdCompletion(p.row, err)
		return
	}
	if result.SpoolID != p.row.ID {
		d.holdCompletion(p.row, errBatchResultMissing)
		return
	}
	if !result.Done {
		log.Printf("drainer: row %d (%s) inline completion rejected — backing pathname retained", p.row.ID, p.row.NFSPath)
		return
	}
	if result.CompletionID == "" {
		d.holdCompletion(p.row, errBatchResultMissing)
		return
	}
	if err := d.spool.Meta().ValidateDrainCompletion(itemsForPending(p), result); err != nil {
		d.holdCompletion(p.row, err)
		return
	}
	if d.onDrainEffects != nil {
		d.finishAcknowledged(&p, result, op)
		return
	}
	if d.onSizeReady != nil {
		d.onSizeReady(p.row.NFSPath, p.row.Size, p.mtime)
	}
	if d.onDrainMetadataReady != nil {
		d.onDrainMetadataReady(p.row.NFSPath, p.row.Size, p.mtime)
	}
	if err := p.closeFD(); err != nil {
		d.holdCompletion(p.row, err)
		return
	}
	if err := d.spool.CompleteAcknowledgedDrain(metadata.DrainCommitItem{SpoolID: p.row.ID, NFSPath: p.row.NFSPath, SpoolFile: p.row.SpoolFile, Size: p.row.Size}, result); err != nil {
		log.Printf("drainer: completion cleanup held row %d: %v", p.row.ID, err)
		return
	}
	d.metrics.DrainsSucceeded.Add(1)
	d.metrics.BytesDrained.Add(p.n)
	d.lastDrainSuccessNanos.Store(time.Now().UnixNano())
	if d.onDrainComplete != nil {
		d.onDrainComplete(p.row.NFSPath, p.row.Size, p.mtime)
	}
}

// finishAcknowledged is the sole production effects/cleanup tail. The lease
// remains with its caller until this returns, even if FD work is delayed.
func (d *Drainer) finishAcknowledged(p *pendingDrain, result metadata.DrainCommitResult, op *namespaceOperation) {
	defer p.closeFD()
	if op == nil || d.onDrainEffects == nil {
		d.holdCompletion(p.row, errDrainEffectsUnwired)
		return
	}
	item := itemsForPending(*p)
	if !result.Done || result.SpoolID != p.row.ID || result.CompletionID == "" || result.Published.IsZero() {
		d.holdCompletion(p.row, errBatchResultMissing)
		return
	}
	if err := d.spool.Meta().ValidateDrainCompletion(item, result); err != nil {
		d.holdCompletion(p.row, err)
		return
	}
	if p.fd == nil {
		d.holdCompletion(p.row, errBatchResultMissing)
		return
	}
	if op != nil {
		op.mutated = true
	}
	if err := d.onDrainEffects(item, result, p.fd, p.sidecarBody); err != nil {
		if op != nil {
			op.poison = true
		}
		d.holdCompletion(p.row, err)
		return
	}
	// Even a verified original Close does not qualify an error from the
	// retained description. Retire this final FD before private cleanup.
	if err := p.closeFD(); err != nil {
		if op != nil {
			op.poison = true
		}
		d.holdCompletion(p.row, err)
		return
	}
	if err := d.spool.CompleteAcknowledgedDrain(item, result); err != nil {
		if op != nil {
			op.poison = true
		}
		log.Printf("drainer: completion cleanup held row %d: %v", p.row.ID, err)
		return
	}
	d.metrics.DrainsSucceeded.Add(1)
	d.metrics.BytesDrained.Add(p.n)
	d.lastDrainSuccessNanos.Store(time.Now().UnixNano())
	if d.onDrainSucceeded != nil {
		d.onDrainSucceeded(p.row.NFSPath)
	}
}

func itemsForPending(p pendingDrain) metadata.DrainCommitItem {
	return metadata.DrainCommitItem{SpoolID: p.row.ID, NFSPath: p.row.NFSPath, SpoolFile: p.row.SpoolFile, Size: p.row.Size, Mtime: p.mtime, BackingIdentity: p.backingIdentity}
}

// Completion errors are not file-copy failures. Never requeue, exhaust the
// file budget or evict custody on a response whose commit outcome is unknown.
func (d *Drainer) holdCompletion(row *metadata.SpoolRow, cause error) {
	item := metadata.DrainCommitItem{SpoolID: row.ID, NFSPath: row.NFSPath, SpoolFile: row.SpoolFile, Size: row.Size}
	if err := d.spool.Meta().HoldDrainCompletion(item); err != nil {
		log.Printf("drainer: completion hold row %d: %v (original: %v)", row.ID, err, cause)
		return
	}
	log.Printf("drainer: completion held row %d; local source retained: %v", row.ID, cause)
}

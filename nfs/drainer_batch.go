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
//   - BatchDrainComplete runs, per item, `UPDATE entries size` BEFORE `UPDATE
//     spool_entries done`, and both commit atomically in the one tx.
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
// ON COMMIT FAILURE. The whole tx rolls back (no partial commit); every file in
// the batch is treated as a transient failure (failTransient) and its FUSE
// write removed, mirroring the per-file MarkDrainComplete SQL-error path. The
// dispatcher re-picks the rows after backoff.

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
	row  *metadata.SpoolRow
	dest string // FUSE destination path (for undo-on-cancel / undo-on-commit-fail)
	n    int64  // bytes copied (== row.Size; verified before enqueue)
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
//   - commit error → every file failTransient + FUSE-undo (the MarkDrainComplete
//     SQL-error path);
//   - result.Done == false (cancelled mid-drain, QA-37) → FUSE-undo + log (the
//     !done path), no success metrics;
//   - result.Done == true → BatchCompleteDrainCleanup (index evict, capacity
//     release, spool-file remove, manifest) + success metrics + liveness stamp +
//     onDrainComplete (the success tail).
func (b *drainBatcher) commitAndDispose(batch []pendingDrain) {
	d := b.d

	// Defensive fallback: if the batched-commit hook was never wired (should not
	// happen in production when the flag is on), complete each file the per-file
	// way so nothing is stranded in `draining`.
	if d.onBatchDrainComplete == nil {
		for _, p := range batch {
			b.disposePerFile(p)
		}
		return
	}

	items := make([]metadata.DrainCommitItem, len(batch))
	now := time.Now()
	for i, p := range batch {
		items[i] = metadata.DrainCommitItem{
			SpoolID: p.row.ID,
			NFSPath: p.row.NFSPath,
			Size:    p.row.Size,
			Mtime:   now,
		}
	}

	results, err := d.onBatchDrainComplete(items)
	if err != nil {
		// Whole tx rolled back: nothing committed. Retry every file
		// transiently and undo its FUSE write (mirrors the per-file
		// MarkDrainComplete SQL-error branch).
		for _, p := range batch {
			_ = os.Remove(p.dest)
			d.failTransient(p.row, err)
		}
		return
	}

	// Index results by spool id so a (defensive) reordering can't misalign.
	doneByID := make(map[int64]bool, len(results))
	for _, r := range results {
		doneByID[r.SpoolID] = r.Done
	}

	for _, p := range batch {
		done, ok := doneByID[p.row.ID]
		if !ok {
			// A result went missing for this id — treat as transient so the row
			// isn't stranded in `draining`. Should never happen (results align
			// with items), but never leave a claimed row unresolved.
			_ = os.Remove(p.dest)
			d.failTransient(p.row, errBatchResultMissing)
			continue
		}
		if !done {
			// Cancelled mid-drain (QA-37): the NFS delete path removed the row.
			// Undo the FUSE write so the delete sticks. No success metrics.
			_ = os.Remove(p.dest)
			log.Printf("drainer: row %d (%s) cancelled mid-drain (deleted) — undid FUSE write (batched)", p.row.ID, p.row.NFSPath)
			continue
		}
		// Committed: run the identical post-commit eviction cleanup + success
		// tail the per-file path runs after MarkDrainComplete.
		d.spool.BatchCompleteDrainCleanup(p.row.ID, p.row.NFSPath, p.row.SpoolFile, p.row.Size)
		d.metrics.DrainsSucceeded.Add(1)
		d.metrics.BytesDrained.Add(p.n)
		d.lastDrainSuccessNanos.Store(time.Now().UnixNano())
		if d.onDrainComplete != nil {
			d.onDrainComplete(p.row.NFSPath, p.row.Size)
		}
	}
}

// disposePerFile completes a single pending drain via the per-file path. Used
// only as the defensive fallback when the batch hook is unwired; it publishes
// the size (task #65) then MarkDrainComplete, exactly as the inline path does.
func (b *drainBatcher) disposePerFile(p pendingDrain) {
	d := b.d
	if d.onSizeReady != nil {
		d.onSizeReady(p.row.NFSPath, p.row.Size)
	}
	done, err := d.spool.MarkDrainComplete(p.row.ID, p.row.NFSPath, p.row.SpoolFile, p.row.Size)
	if err != nil {
		_ = os.Remove(p.dest)
		d.failTransient(p.row, err)
		return
	}
	if !done {
		_ = os.Remove(p.dest)
		log.Printf("drainer: row %d (%s) cancelled mid-drain (deleted) — undid FUSE write (batch-fallback)", p.row.ID, p.row.NFSPath)
		return
	}
	d.metrics.DrainsSucceeded.Add(1)
	d.metrics.BytesDrained.Add(p.n)
	d.lastDrainSuccessNanos.Store(time.Now().UnixNano())
	if d.onDrainComplete != nil {
		d.onDrainComplete(p.row.NFSPath, p.row.Size)
	}
}

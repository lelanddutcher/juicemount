package metadata

import (
	"fmt"
	"time"
)

// DrainCommitItem is one drained file's coalesced metadata write: publish the
// authoritative size onto the entries row (task #65), then mark the spool row
// done (the eviction trigger). Both are the fsync-bearing writes drainOne does
// per file; BatchDrainComplete commits N of them in ONE SQLite transaction to
// collapse N fsync/syscall cascades into 1 under a write storm.
type DrainCommitItem struct {
	// SpoolID is the spool_entries row id to mark done.
	SpoolID int64
	// NFSPath is the entries-table path whose size is being published, and the
	// key for the in-memory pathCache size bump.
	NFSPath string
	// Size is the authoritative drained size (row.Size, already n==row.Size
	// verified by the drainer). Published MAX-only into entries.size.
	Size int64
	// Mtime is the publish timestamp for the entries row.
	Mtime time.Time
}

// DrainCommitResult reports, per input item, whether the spool row was actually
// marked done. Done==false means the row was DELETED out from under the drainer
// (CancelForDelete / rename-requeue — the QA-37 cancel contract): the caller
// must UNDO the FUSE write for that file instead of completing it, exactly as
// the per-file MarkDone==false path does today. Aligned by index with the input
// slice.
type DrainCommitResult struct {
	SpoolID int64
	Done    bool
}

// BatchDrainComplete commits a batch of drained-file metadata writes in a SINGLE
// cross-table SQLite transaction, then updates the in-memory entries cache.
// It is the coalesced form of the drainer's per-file (UpdateSize → MarkDone)
// pair, behind JM_DRAIN_BATCH_INSERT.
//
// TASK #65 ORDERING — preserved by construction. For every item the transaction
// runs `UPDATE entries SET size=MAX(...)` (the authoritative size publish)
// BEFORE `UPDATE spool_entries SET drain_state=done` (the eviction trigger), and
// both commit atomically. The caller runs the eviction side-effects (index
// evict, spool-file removal, capacity release) ONLY after this returns success —
// so a fresh read can never observe a post-eviction-but-pre-size (0/partial)
// Entry.Size. Atomicity subsumes the per-file "publish-before-evict" ordering:
// the size is durably committed no later than the mark-done in the same tx, and
// the shadow is not evicted until after the whole tx commits.
//
// entries.size stays MAX-only (idempotent, same as UpdateSize) so a concurrent
// reconcile or a re-drain can't shrink a size; spool_entries mark-done is by id
// with a rows-affected check so a row CANCELLED mid-drain (deleted by
// DeleteActiveByPath, whose DELETE filters drain_state IN writing/ready/draining
// and thus can't touch a just-done row) reports Done=false.
//
// QA-37 CANCEL CONTRACT — held by SHARING SpoolStore.writeMu, not by tx timing.
// The mark-done (SpoolStore.batchMarkDoneTx) runs UNDER SpoolStore.writeMu, the
// SAME mutex DeleteActiveByPath / MarkDone serialize on, so a concurrent cancel
// and this mark-done cannot interleave — one runs entirely before the other,
// exactly as MarkDone vs DeleteActiveByPath do in the per-file path. (The
// earlier "_txlock=immediate is enough" reasoning was WRONG: in WAL mode the
// cancel's SELECT is a snapshot reader holding NO write lock, so it could
// observe a still-'draining' row and collect it for deletion while this batch
// committed the mark-done first — resurrecting a user-deleted file. Sharing the
// mutex closes that window.)
//
// LOCK ORDER — global, uninverted: Store.writeMu → SpoolStore.writeMu →
// SQLite-write-lock (tx.Begin, _txlock=immediate). SpoolStore.writeMu is taken
// BEFORE tx.Begin so no SpoolStore writer can be mid-DB-write (holding
// SpoolStore.writeMu, blocked on the WAL write lock this tx holds) while this
// path waits on SpoolStore.writeMu — that reversed pairing would 30s-stall on
// busy_timeout. Because this is the ONLY site that holds both Go mutexes and no
// SpoolStore method ever calls back into a Store method, the order has no cycle.
//
// On ANY SQL error the whole tx rolls back and NO item is marked done, so the
// caller retries every file transiently (no partial commit, no half-evicted
// batch). The entries-size publish for the failed batch is rolled back too, so
// there is no orphaned size ahead of an un-drained shadow.
//
// results is aligned by index with items. A nil/empty items returns nil, nil.
func (s *Store) BatchDrainComplete(items []DrainCommitItem) ([]DrainCommitResult, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if s.spoolStore == nil {
		// Wiring invariant: the batch-insert lever must call SetSpoolStore before
		// the drainer can flush. Fail closed rather than run the mark-done
		// unserialized and reopen the QA-37 race.
		return nil, fmt.Errorf("batch drain complete: spool store not wired (SetSpoolStore)")
	}

	// LOCK ORDER: Store.writeMu, then SpoolStore.writeMu, then tx (see docstring).
	// SpoolStore.writeMu is held across the whole tx so the batched mark-done
	// serializes against DeleteActiveByPath / MarkDone (QA-37).
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("batch drain complete begin: %w", err)
	}
	// Rollback is a no-op after a successful Commit.
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// (1) size publish onto entries — MAX-only + mtime, identical to UpdateSize's
	//     SQL. Runs BEFORE every mark-done below (task #65).
	for _, it := range items {
		if it.Size > 0 {
			if _, err := tx.Exec(
				`UPDATE entries SET size = MAX(size, ?), mtime = ? WHERE path = ?`,
				it.Size, it.Mtime.Unix(), it.NFSPath,
			); err != nil {
				return nil, fmt.Errorf("batch drain complete update size %q: %w", it.NFSPath, err)
			}
		}
	}

	// (2) mark every spool row done — the eviction trigger — under
	//     SpoolStore.writeMu (already held), inside this same tx. rows-affected==0
	//     ⇒ row cancelled mid-drain ⇒ Done=false (caller undoes the FUSE write).
	//     Kept BELOW the size publishes so entries.size is durable no later than
	//     the mark-done in the single atomic commit (task #65).
	results, err := s.spoolStore.batchMarkDoneTx(tx, items)
	if err != nil {
		return nil, fmt.Errorf("batch drain complete: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("batch drain complete commit: %w", err)
	}
	committed = true

	// Post-commit: mirror UpdateSize's in-memory cache bump (MAX-only, mtime,
	// invalidate cached XDR bytes) for every item that carried a size. Done
	// under s.mu, AFTER the durable commit, so a reader either sees the old
	// cached size (still correct — the shadow hasn't been evicted yet by the
	// caller) or the freshly-published one.
	s.mu.Lock()
	for _, it := range items {
		if it.Size <= 0 {
			continue
		}
		if e, ok := s.pathCache[it.NFSPath]; ok {
			oldSize := e.Size
			if it.Size > e.Size {
				e.Size = it.Size
			}
			// INSTANT-NAV #2: mirror UpdateSize — an in-place size bump applies
			// the (new−old) delta to the ancestor subtree aggregates.
			s.subtreeResizeLocked(e, oldSize)
			e.Mtime = it.Mtime
			e.ResetGetAttrCache()
		}
	}
	s.mu.Unlock()

	return results, nil
}

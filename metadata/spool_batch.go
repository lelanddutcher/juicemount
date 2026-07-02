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
// and thus can't touch a just-done row) reports Done=false. The QA-37 cancel
// contract holds without sharing SpoolStore.writeMu: _txlock=immediate makes
// this tx take SQLite's write lock at Begin, so a concurrent cancel either
// committed its DELETE first (this UPDATE affects 0 rows → Done=false → caller
// undoes the FUSE write) or blocks until after this commit (and then its DELETE
// skips the now-done row). Either resolution matches the single-write path.
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

	s.writeMu.Lock()
	tx, err := s.db.Begin()
	if err != nil {
		s.writeMu.Unlock()
		return nil, fmt.Errorf("batch drain complete begin: %w", err)
	}
	// Rollback is a no-op after a successful Commit.
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
		s.writeMu.Unlock()
	}()

	results := make([]DrainCommitResult, len(items))
	for i, it := range items {
		// (1) size publish onto entries — MAX-only + mtime, identical to
		//     UpdateSize's SQL. Runs BEFORE the mark-done below (task #65).
		if it.Size > 0 {
			if _, err := tx.Exec(
				`UPDATE entries SET size = MAX(size, ?), mtime = ? WHERE path = ?`,
				it.Size, it.Mtime.Unix(), it.NFSPath,
			); err != nil {
				return nil, fmt.Errorf("batch drain complete update size %q: %w", it.NFSPath, err)
			}
		}
		// (2) mark the spool row done — the eviction trigger. WHERE id only,
		//     mirroring SpoolStore.MarkDone; rows-affected==0 ⇒ row cancelled
		//     mid-drain ⇒ Done=false (caller undoes the FUSE write).
		res, err := tx.Exec(
			`UPDATE spool_entries SET drain_state=?, updated_at=? WHERE id=?`,
			DrainDone, it.Mtime.Unix(), it.SpoolID,
		)
		if err != nil {
			return nil, fmt.Errorf("batch drain complete mark done %d: %w", it.SpoolID, err)
		}
		n, _ := res.RowsAffected()
		results[i] = DrainCommitResult{SpoolID: it.SpoolID, Done: n > 0}
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
			if it.Size > e.Size {
				e.Size = it.Size
			}
			e.Mtime = it.Mtime
			e.PreSerializedGetAttr = nil
		}
	}
	s.mu.Unlock()

	return results, nil
}

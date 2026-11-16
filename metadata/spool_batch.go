package metadata

import (
	"fmt"
	"io/fs"
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
	// SpoolFile identifies the private source selected by the drainer.
	SpoolFile string
	// NFSPath is the entries-table path whose size is being published, and the
	// key for the in-memory pathCache size bump.
	NFSPath string
	// Size is the authoritative drained size (row.Size, already n==row.Size
	// verified by the drainer). Published MAX-only into entries.size.
	Size int64
	// Mtime is the publish timestamp for the entries row.
	Mtime           time.Time
	BackingIdentity string
}

// DrainCommitResult is aligned with the input items. Done authorizes local
// spool cleanup only after the transaction commits. A rejected item must not
// change the mirror or trigger deletion of a public backing pathname.
type DrainCommitResult struct {
	SpoolID         int64
	Done            bool
	CompletionID    string
	Published       MirrorToken
	Inode           uint64
	Mode            fs.FileMode
	Size            int64
	Mtime           time.Time
	BackingIdentity string
}

// BatchDrainComplete commits eligible legacy spool rows and their metadata in
// one transaction, then publishes only those results to the RAM mirror. A
// canceled, requeued or mismatched row has no metadata effects. Bound rows
// must use CompleteSpool instead.
//
// Lock order is Store.writeMu → cachePublicationMu → SpoolStore.writeMu →
// SQLite transaction. The shared spool mutex serializes qualification against
// DeleteActiveByPath and
// other state transitions. Store.writeMu remains held through RAM publication.
// Both SQL updates commit before the caller may evict a spool shadow; their
// statement order does not expose a partially committed result. Any SQL error
// rolls back the entire batch, including eligible siblings.
//
// This legacy interface has no mirror incarnation or backend publication
// receipt. It does not authorize writes or deletion at a public pathname.
func (s *Store) BatchDrainComplete(items []DrainCommitItem) ([]DrainCommitResult, error) {
	return s.completeDrains(items, false)
}

// BatchDrainCompleteExact is the production full-image policy. Keep the
// historical MAX-only batch API for callers that explicitly need it.
func (s *Store) BatchDrainCompleteExact(items []DrainCommitItem) ([]DrainCommitResult, error) {
	return s.completeDrains(items, true)
}

// DrainComplete atomically qualifies one legacy row, publishes its exact size
// (including shrink and zero), and marks it done. Rejection has no mirror
// effects. Like BatchDrainComplete, this is local completion only: it carries
// no authority to mutate a public backing pathname or a later replacement.
func (s *Store) DrainComplete(item DrainCommitItem) (DrainCommitResult, error) {
	results, err := s.completeDrains([]DrainCommitItem{item}, true)
	if err != nil {
		return DrainCommitResult{}, err
	}
	return results[0], nil
}

func (s *Store) completeDrains(items []DrainCommitItem, exactSize bool) ([]DrainCommitResult, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if s.spoolStore == nil {
		// Wiring invariant: the batch-insert lever must call SetSpoolStore before
		// the drainer can flush. Fail closed rather than run the mark-done
		// unserialized and reopen the QA-37 race.
		return nil, fmt.Errorf("batch drain complete: spool store not wired (SetSpoolStore)")
	}

	seen := make(map[int64]struct{}, len(items))
	paths := make(map[string]struct{}, len(items))
	for _, it := range items {
		if it.SpoolID <= 0 || it.NFSPath == "" || it.Size < 0 {
			return nil, fmt.Errorf("batch drain complete: invalid item for spool %d", it.SpoolID)
		}
		if _, exists := seen[it.SpoolID]; exists {
			return nil, fmt.Errorf("batch drain complete: duplicate spool %d", it.SpoolID)
		}
		if _, exists := paths[it.NFSPath]; exists {
			return nil, fmt.Errorf("batch drain complete: duplicate path %q", it.NFSPath)
		}
		seen[it.SpoolID] = struct{}{}
		paths[it.NFSPath] = struct{}{}
	}

	// Keep both durable and cache-only publishers outside qualification and
	// publication; readers do not acquire cachePublicationMu.
	// SpoolStore.writeMu is held across the whole tx so the batched mark-done
	// serializes against DeleteActiveByPath / MarkDone (QA-37).
	s.replacementObservationMu.Lock()
	defer s.replacementObservationMu.Unlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()

	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	preimages := make(map[string]*Entry, len(items))
	if exactSize {
		for _, it := range items {
			var eligible int
			if err := s.db.QueryRow(`SELECT count(*) FROM spool_entries r WHERE r.id=? AND r.nfs_path=?
 AND r.spool_file=? AND r.size=? AND r.drain_state='draining' AND r.punched_end=0
 AND NOT EXISTS (SELECT 1 FROM spool_bindings b WHERE b.spool_id=r.id)`,
				it.SpoolID, it.NFSPath, it.SpoolFile, it.Size).Scan(&eligible); err != nil {
				return nil, err
			}
			if eligible == 0 {
				continue
			}
			e, err := scanEntry(s.db.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, it.NFSPath))
			if err != nil || e.Version.IsZero() {
				return nil, ErrMirrorConflict
			}
			s.mu.RLock()
			cached := s.pathCache[it.NFSPath]
			conflict := s.preparedCreateConflictLocked(it.NFSPath) || cached == nil ||
				cached.Version != e.Version || cached.Inode != e.Inode || cached.Mode != e.Mode ||
				cached.Size != e.Size || cached.Mtime.Unix() != e.Mtime.Unix()
			s.mu.RUnlock()
			if conflict {
				return nil, ErrMirrorConflict
			}
			preimages[it.NFSPath] = e
		}
	}

	completionIDs, err := s.prepareDrainCompletionsLocked(items)
	if err != nil {
		return nil, fmt.Errorf("prepare drain completion: %w", err)
	}
	tx, closeConn, err := s.beginSpoolDurableTx()
	if err != nil {
		return nil, fmt.Errorf("batch drain complete begin: %w", err)
	}
	defer closeConn()
	// Rollback is a no-op after a successful Commit.
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	for _, it := range items {
		if err := rejectBoundSpool(tx, it.SpoolID); err != nil {
			return nil, err
		}
	}
	// Qualify the current spool row before any path-only mirror update. These
	// provisional transitions roll back if a later metadata update fails.
	results, err := s.spoolStore.batchMarkDoneTx(tx, items)
	if err != nil {
		return nil, fmt.Errorf("batch drain complete: %w", err)
	}
	for i, it := range items {
		if !results[i].Done || (!exactSize && it.Size == 0) {
			continue
		}
		query := `UPDATE entries SET size = MAX(size, ?), mtime = ?, mtime_nsec = NULL WHERE path = ?`
		args := []any{it.Size, it.Mtime.Unix(), it.NFSPath}
		if exactSize {
			query = `UPDATE entries SET size = ?, mtime = ?, mtime_nsec = NULL WHERE path = ? AND incarnation=? AND revision=? AND inode=?`
			pre := preimages[it.NFSPath]
			args = append(args, pre.Version.Incarnation, pre.Version.Revision, int64(pre.Inode))
		}
		updated, err := tx.Exec(query, args...)
		if err != nil {
			return nil, fmt.Errorf("drain complete update size %q: %w", it.NFSPath, err)
		}
		if exactSize {
			if err := exactlyOneRow(updated); err != nil {
				return nil, fmt.Errorf("drain complete missing mirror %q: %w", it.NFSPath, err)
			}
		}
	}
	// Bind the post-update mirror generation and attributes to the exact FULL
	// legacy hold in this transaction. A caller cannot forge a current token
	// onto an old completion ID after the original generation has retired.
	for i, it := range items {
		if !exactSize || !results[i].Done {
			continue
		}
		if completionIDs[i] == "" {
			return nil, ErrMirrorConflict
		}
		e, err := scanEntry(tx.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, it.NFSPath))
		if err != nil || e.Version.IsZero() || e.Version.Path != it.NFSPath {
			return nil, fmt.Errorf("drain complete missing versioned mirror %q: %w", it.NFSPath, ErrMirrorConflict)
		}
		if exactSize && (e.Size != it.Size || e.Mtime.Unix() != it.Mtime.Unix()) {
			return nil, ErrMirrorConflict
		}
		res, err := tx.Exec(`UPDATE spool_completion_holds SET incarnation=?,published_revision=?,original_inode=?,original_mode=?,backing_identity=?
 WHERE completion_id=? AND spool_id=? AND nfs_path=? AND spool_file=? AND size=? AND kind='legacy' AND birth_id=''`,
			e.Version.Incarnation, e.Version.Revision, int64(e.Inode), uint32(e.Mode), it.BackingIdentity, completionIDs[i], it.SpoolID, it.NFSPath, it.SpoolFile, it.Size)
		if err != nil {
			return nil, err
		}
		if err := exactlyOneRow(res); err != nil {
			return nil, ErrMirrorConflict
		}
		results[i].Published, results[i].Inode, results[i].Mode = e.Version, e.Inode, e.Mode
		results[i].Size, results[i].Mtime = e.Size, e.Mtime
		results[i].BackingIdentity = it.BackingIdentity
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("batch drain complete commit: %w", err)
	}
	committed = true
	for i := range results {
		if results[i].Done {
			results[i].CompletionID = completionIDs[i]
		}
	}

	// Post-commit: publish the selected size policy and mtime to RAM,
	// invalidate cached XDR bytes) only for accepted items that carried a size. Done
	// under s.mu, AFTER the durable commit, so a reader either sees the old
	// cached size (still correct — the shadow hasn't been evicted yet by the
	// caller) or the freshly-published one.
	s.mu.Lock()
	for i, it := range items {
		if !results[i].Done || (!exactSize && it.Size == 0) {
			continue
		}
		if e, ok := s.pathCache[it.NFSPath]; ok {
			oldSize := e.Size
			if exactSize || it.Size > e.Size {
				e.Size = it.Size
			}
			// INSTANT-NAV #2: mirror UpdateSize — an in-place size bump applies
			// the (new−old) delta to the ancestor subtree aggregates.
			s.subtreeResizeLocked(e, oldSize)
			if exactSize {
				e.Mtime = results[i].Mtime
			} else {
				e.Mtime = it.Mtime
			}
			e.MtimeNsecKnown = false
			e.ResetGetAttrCache()
			if !e.Version.IsZero() {
				e.Version.Revision++
			}
		}
	}
	s.mu.Unlock()

	return results, nil
}

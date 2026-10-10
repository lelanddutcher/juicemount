package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"unicode/utf8"
)

// DrainCompletionHold records uncertainty about local completion only. It is
// not a backend publication receipt or a binding of a legacy writer to media.
type DrainCompletionHold struct {
	CompletionID       string
	SpoolID            int64
	NFSPath, SpoolFile string
	Size               int64
}

type PendingDrainCompletion struct{ Rows []DrainCompletionHold }

func (e *PendingDrainCompletion) Error() string {
	return fmt.Sprintf("drain completion requires reconciliation for %d source(s)", len(e.Rows))
}

// prepareDrainCompletionsLocked is called with replacementObservationMu, Store.writeMu,
// cachePublicationMu and SpoolStore.writeMu held. Its FULL commit precedes the
// separate size/done transaction, so any lost result leaves a durable hold.
func (s *Store) prepareDrainCompletionsLocked(items []DrainCommitItem) ([]string, error) {
	tx, closeConn, err := s.beginSpoolDurableTx()
	if err != nil {
		return nil, err
	}
	defer closeConn()
	defer tx.Rollback()
	ids := make([]string, len(items))
	for i, it := range items {
		if err := rejectBoundSpool(tx, it.SpoolID); err != nil {
			return nil, err
		}
		if err := rejectCompletionHold(tx, it.SpoolID); err != nil {
			return nil, err
		}
		id, err := NewSpoolCustodyID()
		if err != nil {
			return nil, err
		}
		res, err := tx.Exec(`INSERT INTO spool_completion_holds(completion_id,spool_id,nfs_path,spool_file,size)
 SELECT ?,id,nfs_path,spool_file,size FROM spool_entries
 WHERE id=? AND nfs_path=? AND spool_file=? AND size=? AND drain_state='draining' AND punched_end=0 AND retired_by=0`, id, it.SpoolID, it.NFSPath, it.SpoolFile, it.Size)
		if err != nil {
			return nil, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if n == 1 {
			ids[i] = id
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return ids, nil
}

func rejectCompletionHold(q versionQuerier, id int64) error {
	var h DrainCompletionHold
	err := q.QueryRow(`SELECT completion_id,spool_id,nfs_path,spool_file,size FROM spool_completion_holds WHERE spool_id=?`, id).Scan(&h.CompletionID, &h.SpoolID, &h.NFSPath, &h.SpoolFile, &h.Size)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return &PendingDrainCompletion{Rows: []DrainCompletionHold{h}}
}

func rejectCompletionPath(q versionQuerier, name string, subtree bool) error {
	prefix := name + "/"
	var h DrainCompletionHold
	err := q.QueryRow(`SELECT completion_id,spool_id,nfs_path,spool_file,size FROM spool_completion_holds
 WHERE nfs_path=? OR (? AND substr(nfs_path,1,?)=?) LIMIT 1`, name, subtree, utf8.RuneCountInString(prefix), prefix).Scan(&h.CompletionID, &h.SpoolID, &h.NFSPath, &h.SpoolFile, &h.Size)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return &PendingDrainCompletion{Rows: []DrainCompletionHold{h}}
}

// HoldDrainCompletion is used when no completion provider is available. The
// row remains in place; this never consumes attempts or transfers custody.
// A wired owner is required to serialize both subscription and SCAN admission.
func (s *SpoolStore) HoldDrainCompletion(item DrainCommitItem) error {
	owner := s.owner.Load()
	if owner == nil {
		return ErrMirrorConflict
	}
	owner.replacementObservationMu.Lock()
	defer owner.replacementObservationMu.Unlock()
	owner.writeMu.Lock()
	defer owner.writeMu.Unlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := rejectCompletionHold(s.db, item.SpoolID); err != nil {
		return err
	}
	conn, err := s.db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), `PRAGMA synchronous=FULL`); err != nil {
		return err
	}
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := rejectBoundSpool(tx, item.SpoolID); err != nil {
		return err
	}
	id, err := NewSpoolCustodyID()
	if err != nil {
		return err
	}
	res, err := tx.Exec(`INSERT INTO spool_completion_holds(completion_id,spool_id,nfs_path,spool_file,size)
 SELECT ?,id,nfs_path,spool_file,size FROM spool_entries WHERE id=? AND nfs_path=? AND spool_file=? AND size=? AND drain_state IN ('draining','done') AND retired_by=0`, id, item.SpoolID, item.NFSPath, item.SpoolFile, item.Size)
	if err != nil {
		return err
	}
	if err := exactlyOneRow(res); err != nil {
		return err
	}
	return tx.Commit()
}

// ValidateDrainCompletion admits callbacks only for an exact acknowledged
// result. Cleanup revalidates before disposal; this is not a namespace fence
// for path-based callbacks after a valid completion.
func (s *SpoolStore) ValidateDrainCompletion(item DrainCommitItem, result DrainCommitResult) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.validateDrainCompletionLocked(item, result)
}

// CleanupDrainCompletion runs local disposal at most once for the exact
// acknowledged result. The marker survives any failure or crash gap. fn must
// not reenter Store/SpoolStore writes or observation admission; it may only
// dispose this private source. The wired owner holds observation and writer
// gates until the exact marker deletion is durable.
func (s *SpoolStore) CleanupDrainCompletion(item DrainCommitItem, result DrainCommitResult, fn func() error) error {
	owner := s.owner.Load()
	if owner == nil {
		return ErrMirrorConflict
	}
	owner.replacementObservationMu.Lock()
	defer owner.replacementObservationMu.Unlock()
	owner.writeMu.Lock()
	defer owner.writeMu.Unlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.validateDrainCompletionLocked(item, result); err != nil {
		return err
	}
	if err := fn(); err != nil {
		return err
	}
	conn, err := s.db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), `PRAGMA synchronous=FULL`); err != nil {
		return err
	}
	_, err = conn.ExecContext(context.Background(), `DELETE FROM spool_completion_holds WHERE completion_id=? AND spool_id=?`, result.CompletionID, item.SpoolID)
	return err
}

func (s *SpoolStore) validateDrainCompletionLocked(item DrainCommitItem, result DrainCommitResult) error {
	if !result.Done || result.SpoolID != item.SpoolID || result.CompletionID == "" {
		return fmt.Errorf("drain cleanup requires exact acknowledged completion")
	}
	var count int
	err := s.db.QueryRow(`SELECT count(*) FROM spool_completion_holds h JOIN spool_entries r ON r.id=h.spool_id
 WHERE h.completion_id=? AND h.spool_id=? AND h.nfs_path=? AND h.spool_file=? AND h.size=?
 AND h.kind='legacy' AND h.birth_id='' AND NOT EXISTS (SELECT 1 FROM spool_bindings b WHERE b.spool_id=h.spool_id)
 AND r.nfs_path=h.nfs_path AND r.spool_file=h.spool_file AND r.size=h.size AND r.drain_state='done' AND r.punched_end=0`, result.CompletionID, item.SpoolID, item.NFSPath, item.SpoolFile, item.Size).Scan(&count)
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("drain cleanup completion no longer current")
	}
	return nil
}

// ApplyLegacyDrainEffects validates the committed generation and the still-live
// FULL hold while publishing only bounded in-memory effects. fn must not do
// filesystem/network I/O, reenter Store or acquire a path shard.
func (s *Store) ApplyLegacyDrainEffects(item DrainCommitItem, result DrainCommitResult, fn func(DrainCommitResult)) error {
	if s.spoolStore == nil || s.spoolStore.db != s.db || fn == nil ||
		!result.Done || result.SpoolID != item.SpoolID || result.CompletionID == "" ||
		result.Published.Path != item.NFSPath || result.Published.Incarnation == "" ||
		result.Published.Revision <= 0 || result.Size != item.Size ||
		result.Mtime.Unix() != item.Mtime.Unix() || result.BackingIdentity == "" ||
		result.BackingIdentity != item.BackingIdentity {
		return ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.spoolStore.writeMu.Lock()
	defer s.spoolStore.writeMu.Unlock()
	if err := s.spoolStore.validateDrainCompletionLocked(item, result); err != nil {
		return err
	}
	var incarnation, backingIdentity string
	var revision, inode, mode int64
	err := s.db.QueryRow(`SELECT incarnation,published_revision,original_inode,original_mode,backing_identity
 FROM spool_completion_holds WHERE completion_id=? AND spool_id=? AND nfs_path=? AND spool_file=? AND size=?
 AND kind='legacy' AND birth_id=''`, result.CompletionID, item.SpoolID, item.NFSPath, item.SpoolFile, item.Size).
		Scan(&incarnation, &revision, &inode, &mode, &backingIdentity)
	if err != nil || incarnation != result.Published.Incarnation || revision != result.Published.Revision ||
		uint64(inode) != result.Inode || uint32(mode) != uint32(result.Mode) || backingIdentity != result.BackingIdentity {
		return ErrMirrorConflict
	}
	e, err := scanEntry(s.db.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, item.NFSPath))
	if err != nil || e.Version != result.Published || e.Inode != result.Inode || e.Mode != result.Mode ||
		e.Size != result.Size || !e.Mtime.Equal(result.Mtime) {
		return ErrMirrorConflict
	}
	s.mu.RLock()
	cached := s.pathCache[item.NFSPath]
	conflict := s.preparedCreateConflictLocked(item.NFSPath) || cached == nil ||
		cached.Version != result.Published || cached.Inode != result.Inode || cached.Mode != result.Mode ||
		cached.Size != result.Size || !cached.Mtime.Equal(result.Mtime)
	s.mu.RUnlock()
	if conflict {
		return ErrMirrorConflict
	}
	fn(result)
	return nil
}

// CheckDrainCompletionRecovery runs before any scrub, counter reset or writer
// admission. Legacy in-flight rows and surviving done sources have no receipt
// proving that replay/deletion is safe; they also require reconciliation.
func (s *SpoolStore) CheckDrainCompletionRecovery() error {
	rows, err := s.db.Query(`SELECT completion_id,spool_id,nfs_path,spool_file,size FROM spool_completion_holds ORDER BY spool_id`)
	if err != nil {
		return err
	}
	var pending []DrainCompletionHold
	for rows.Next() {
		var h DrainCompletionHold
		if err := rows.Scan(&h.CompletionID, &h.SpoolID, &h.NFSPath, &h.SpoolFile, &h.Size); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, h)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(pending) > 0 {
		return &PendingDrainCompletion{Rows: pending}
	}
	rows, err = s.db.Query(`SELECT id,nfs_path,spool_file,size,drain_state FROM spool_entries WHERE drain_state IN ('draining','done')`)
	if err != nil {
		return err
	}
	type legacy struct {
		h     DrainCompletionHold
		state string
	}
	var old []legacy
	for rows.Next() {
		var x legacy
		if err := rows.Scan(&x.h.SpoolID, &x.h.NFSPath, &x.h.SpoolFile, &x.h.Size, &x.state); err != nil {
			rows.Close()
			return err
		}
		old = append(old, x)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, x := range old {
		if x.state == string(DrainDraining) {
			pending = append(pending, x.h)
			continue
		}
		// Replacement completion has its own canonical journal and recovery
		// phases. It deliberately retains a DONE source until exact cleanup;
		// the legacy missing-receipt rule must not reinterpret that custody.
		row, err := s.Get(x.h.SpoolID)
		if err != nil {
			return err
		}
		publication, err := s.ReplacementPublication(row)
		if err != nil {
			return err
		}
		if publication != nil {
			switch publication.Phase {
			case ReplacementPublicationSettled, ReplacementPublicationCleaning, ReplacementPublicationCleaned:
				continue
			}
		}
		_, err = os.Lstat(x.h.SpoolFile)
		if err == nil {
			pending = append(pending, x.h)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("inspect retained completion source: %w", err)
		}
	}
	if len(pending) > 0 {
		return &PendingDrainCompletion{Rows: pending}
	}
	return nil
}

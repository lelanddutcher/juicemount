package metadata

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"sort"
	"time"
)

// ============================================================================
// INSTANT-NAV #10 (task #73, second half): durable prune ladder.
//
// The pruneAbsent counter ladder (redis.go) was IN-MEMORY ONLY: every app
// restart reset all absence counters to zero, so under restart churn
// (deploys, crashes, user quits) a mass-deleted tree (120k+ paths) never
// accumulated PruneThreshold consecutive-absence cycles and lingered in the
// mirror as ghosts indefinitely. This table persists the ladder so a restart
// RESUMES convergence instead of resetting it.
//
// The legacy table is read before the first staged generation is activated.
// Thereafter the reconciler publishes immutable generation snapshots and
// construction loads the active generation. The ladder carries only the
// rate-limiter state — counts, never delete authority; see the safety
// analysis on loadDurablePruneLadder. Kill switch: JM_PRUNE_LADDER_DURABLE=0.
// ============================================================================

// pruneLadderSchema is additive (CREATE TABLE IF NOT EXISTS) so it lands on
// existing mirror DBs at open, exactly like fts_pending. It lives in the SAME
// SQLite DB as entries, so the app's "Reset local metadata cache" wipe also
// drops the ladder — a wiped mirror has no absence history by definition.
const pruneLadderSchema = `
CREATE TABLE IF NOT EXISTS prune_ladder (
    path          TEXT PRIMARY KEY,
    absent_cycles INTEGER NOT NULL,
    updated_ns    INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS prune_ladder_stage_state (
    generation  TEXT PRIMARY KEY,
    complete    INTEGER NOT NULL,
    row_count   INTEGER NOT NULL,
    created_ns  INTEGER NOT NULL,
    completed_ns INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS prune_ladder_stage_owner (
    generation TEXT PRIMARY KEY,
    owner_id   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS prune_ladder_stage (
    generation    TEXT NOT NULL,
    path          TEXT NOT NULL,
    absent_cycles INTEGER NOT NULL,
    updated_ns    INTEGER NOT NULL,
    PRIMARY KEY (generation, path)
);
CREATE INDEX IF NOT EXISTS idx_prune_ladder_stage_generation
    ON prune_ladder_stage(generation);
CREATE TRIGGER IF NOT EXISTS prune_ladder_stage_owner_gc
AFTER DELETE ON prune_ladder_stage_state
BEGIN
    DELETE FROM prune_ladder_stage_owner WHERE generation=OLD.generation;
END;
`

const pruneLadderActiveGenerationKey = "prune_ladder_active_generation"

// pruneLadderTxChunk bounds the rows written per transaction — and therefore
// per writeMu hold — by the legacy delta and staged snapshot paths. The initial persist after a
// 120k-path mass delete is the sizing case: 12 bounded transactions instead
// of one giant journal spike, with writeMu fully released between chunks so
// NFS-driven entries writes interleave (same pattern as bulkInsertBatch).
const pruneLadderTxChunk = 10000

const (
	pruneLadderStageChunk          = pruneLadderTxChunk
	pruneLadderStageGCDefaultLimit = 10000
	pruneLadderIncompleteGCGrace   = 24 * time.Hour
	pruneLadderGCRetryInterval     = time.Second
	pruneLadderGCMaxRetryInterval  = time.Minute
	pruneLadderGCIdleInterval      = 5 * time.Minute
)

const (
	pruneLadderStageIncomplete = 0
	pruneLadderStageCandidate  = 1
	pruneLadderStageActive     = 2
	pruneLadderStageRetired    = 3
	pruneLadderStageAbandoned  = 4
)

// LoadPruneLadder returns the full persisted ladder: path → consecutive
// absent-cycle count. Read path only (no writeMu), used once per process at
// RedisClient construction. An empty table returns an empty, non-nil map.
func (s *Store) LoadPruneLadder() (map[string]int, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("load prune ladder begin: %w", err)
	}
	defer tx.Rollback()
	var generation string
	err = tx.QueryRow(`SELECT value FROM store_meta WHERE key = ?`, pruneLadderActiveGenerationKey).Scan(&generation)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("load prune ladder active generation: %w", err)
	}
	query := `SELECT path, absent_cycles FROM prune_ladder`
	args := []any{}
	active := err == nil && generation != ""
	var expectedRows int
	if active {
		if err := tx.QueryRow(`SELECT row_count FROM prune_ladder_stage_state WHERE generation=? AND complete IN (1,2)`, generation).Scan(&expectedRows); err != nil {
			return nil, fmt.Errorf("load active prune ladder manifest %q: %w", generation, err)
		}
		query = `SELECT path, absent_cycles FROM prune_ladder_stage WHERE generation = ?`
		args = append(args, generation)
	}
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("load prune ladder: %w", err)
	}
	defer rows.Close()

	out := make(map[string]int)
	for rows.Next() {
		var p string
		var c int
		if err := rows.Scan(&p, &c); err != nil {
			return nil, fmt.Errorf("load prune ladder scan: %w", err)
		}
		out[p] = c
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load prune ladder rows: %w", err)
	}
	if active && len(out) != expectedRows {
		return nil, fmt.Errorf("active prune ladder %q has %d rows, want %d", generation, len(out), expectedRows)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("load prune ladder commit: %w", err)
	}
	return out, nil
}

// StagePruneLadderSnapshot writes an immutable full snapshot in bounded
// transactions. The returned generation is not visible to LoadPruneLadder
// until ActivatePruneLadderStage commits its active marker.
func (s *Store) StagePruneLadderSnapshot(snapshot map[string]int) (string, error) {
	var rawID [16]byte
	if _, err := rand.Read(rawID[:]); err != nil {
		return "", fmt.Errorf("create prune ladder generation: %w", err)
	}
	generation := hex.EncodeToString(rawID[:])
	type ladderRow struct {
		path  string
		count int
	}
	rows := make([]ladderRow, 0, len(snapshot))
	for path, count := range snapshot {
		if path == "" || count < 0 {
			return "", fmt.Errorf("invalid prune ladder snapshot row %q=%d", path, count)
		}
		rows = append(rows, ladderRow{path: path, count: count})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].path < rows[j].path })
	createdNS := time.Now().UnixNano()
	// Create an incomplete manifest first; any later failure leaves an
	// unpublishable generation that bounded GC can reclaim.
	s.writeMu.Lock()
	tx, err := s.db.Begin()
	if err == nil {
		_, err = tx.Exec(`INSERT INTO prune_ladder_stage_state(generation,complete,row_count,created_ns,completed_ns) VALUES(?,?,?, ?,0)`, generation, pruneLadderStageIncomplete, len(rows), createdNS)
		if err == nil && s.pruneLadderOwnerID != "" {
			_, err = tx.Exec(`INSERT INTO prune_ladder_stage_owner(generation,owner_id) VALUES(?,?)`, generation, s.pruneLadderOwnerID)
		}
	}
	if err != nil {
		if tx != nil {
			_ = tx.Rollback()
		}
		s.writeMu.Unlock()
		return "", fmt.Errorf("create prune ladder stage: %w", err)
	}
	if err := tx.Commit(); err != nil {
		s.writeMu.Unlock()
		return "", fmt.Errorf("commit prune ladder stage manifest: %w", err)
	}
	s.writeMu.Unlock()

	for start := 0; start < len(rows); start += pruneLadderStageChunk {
		end := min(start+pruneLadderStageChunk, len(rows))
		chunk := rows[start:end]
		s.writeMu.Lock()
		tx, err := s.db.Begin()
		if err != nil {
			s.writeMu.Unlock()
			return "", fmt.Errorf("prune ladder stage begin: %w", err)
		}
		stmt, err := tx.Prepare(`INSERT INTO prune_ladder_stage(generation,path,absent_cycles,updated_ns) VALUES(?,?,?,?)`)
		if err != nil {
			_ = tx.Rollback()
			s.writeMu.Unlock()
			return "", fmt.Errorf("prune ladder stage prepare: %w", err)
		}
		for _, row := range chunk {
			if _, err := stmt.Exec(generation, row.path, row.count, createdNS); err != nil {
				_ = stmt.Close()
				_ = tx.Rollback()
				s.writeMu.Unlock()
				return "", fmt.Errorf("prune ladder stage row %q: %w", row.path, err)
			}
		}
		_ = stmt.Close()
		if err := tx.Commit(); err != nil {
			s.writeMu.Unlock()
			return "", fmt.Errorf("prune ladder stage commit: %w", err)
		}
		s.writeMu.Unlock()
	}

	s.writeMu.Lock()
	tx, err = s.db.Begin()
	if err == nil {
		res, updateErr := tx.Exec(`UPDATE prune_ladder_stage_state SET complete=?,completed_ns=? WHERE generation=? AND complete=? AND row_count=? AND (SELECT COUNT(*) FROM prune_ladder_stage WHERE generation=?)=?`, pruneLadderStageCandidate, time.Now().UnixNano(), generation, pruneLadderStageIncomplete, len(rows), generation, len(rows))
		err = updateErr
		if err == nil {
			if n, rowsErr := res.RowsAffected(); rowsErr != nil {
				err = rowsErr
			} else if n != 1 {
				err = fmt.Errorf("stage row count mismatch")
			}
		}
	}
	if err != nil {
		if tx != nil {
			_ = tx.Rollback()
		}
		s.writeMu.Unlock()
		return "", fmt.Errorf("complete prune ladder stage: %w", err)
	}
	if err := tx.Commit(); err != nil {
		s.writeMu.Unlock()
		return "", fmt.Errorf("commit prune ladder stage completion: %w", err)
	}
	s.writeMu.Unlock()
	return generation, nil
}

// recoverOwnedPruneLadderCandidates runs only after an on-disk Store has
// acquired its lifetime lock. Legacy manifests have no owner row and remain
// untouched: old binaries do not participate in that lock protocol.
func (s *Store) recoverOwnedPruneLadderCandidates() error {
	if s.pruneLadderOwnerID == "" {
		return nil
	}
	var eligible int
	if err := s.db.QueryRow(`SELECT EXISTS(
		SELECT 1 FROM prune_ladder_stage_state st
		JOIN prune_ladder_stage_owner o ON o.generation=st.generation
		WHERE st.complete=? AND o.owner_id<>?
		AND NOT EXISTS (SELECT 1 FROM store_meta m WHERE m.key=? AND m.value=st.generation)
	)`, pruneLadderStageCandidate, s.pruneLadderOwnerID, pruneLadderActiveGenerationKey).Scan(&eligible); err != nil {
		return fmt.Errorf("inspect prune ladder candidate recovery: %w", err)
	}
	if eligible == 0 {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin prune ladder candidate recovery: %w", err)
	}
	defer tx.Rollback()
	_, err = tx.Exec(`UPDATE prune_ladder_stage_state SET complete=?
		WHERE complete=? AND EXISTS (
			SELECT 1 FROM prune_ladder_stage_owner o
			WHERE o.generation=prune_ladder_stage_state.generation AND o.owner_id<>?
		) AND NOT EXISTS (
			SELECT 1 FROM store_meta m WHERE m.key=? AND m.value=prune_ladder_stage_state.generation
		)`, pruneLadderStageAbandoned, pruneLadderStageCandidate, s.pruneLadderOwnerID, pruneLadderActiveGenerationKey)
	if err != nil {
		return fmt.Errorf("mark crash-left prune ladder candidates: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit prune ladder candidate recovery: %w", err)
	}
	return nil
}

// ActivatePruneLadderStage atomically publishes a complete generation with
// successful-SCAN metadata. An empty generation preserves the active ladder
// and commits metadata only (for durability-disabled callers).
func (s *Store) ActivatePruneLadderStage(generation string, syncedAt time.Time) error {
	return s.activatePruneLadderStage(generation, syncedAt, nil)
}

// activatePruneLadderStage checks the SCAN's Store observation in the same
// transaction that publishes the ladder marker and successful-sync metadata.
func (s *Store) activatePruneLadderStage(generation string, syncedAt time.Time, expected *StoreEpoch) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.RLock()
	defer s.mu.RUnlock()
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("activate prune ladder begin: %w", err)
	}
	defer tx.Rollback()
	if expected != nil {
		got, err := captureStoreEpoch(tx, s.cacheGeneration)
		if err != nil {
			return fmt.Errorf("verify SCAN store epoch: %w", err)
		}
		if got != *expected {
			return ErrMirrorConflict
		}
	}
	if generation != "" {
		var complete int
		if err := tx.QueryRow(`SELECT complete FROM prune_ladder_stage_state WHERE generation=?`, generation).Scan(&complete); err != nil {
			return fmt.Errorf("verify prune ladder stage: %w", err)
		}
		if complete != pruneLadderStageCandidate && complete != pruneLadderStageActive {
			return fmt.Errorf("prune ladder stage %q is incomplete", generation)
		}
		var oldGeneration string
		markerErr := tx.QueryRow(`SELECT value FROM store_meta WHERE key=?`, pruneLadderActiveGenerationKey).Scan(&oldGeneration)
		if markerErr != nil && markerErr != sql.ErrNoRows {
			return fmt.Errorf("read prior prune ladder marker: %w", markerErr)
		}
		if oldGeneration != "" && oldGeneration != generation {
			if _, err := tx.Exec(`UPDATE prune_ladder_stage_state SET complete=? WHERE generation=? AND complete IN (?,?)`, pruneLadderStageRetired, oldGeneration, pruneLadderStageCandidate, pruneLadderStageActive); err != nil {
				return fmt.Errorf("retire prior prune ladder generation: %w", err)
			}
		}
		if _, err := tx.Exec(`UPDATE prune_ladder_stage_state SET complete=? WHERE generation=? AND complete IN (?,?)`, pruneLadderStageActive, generation, pruneLadderStageCandidate, pruneLadderStageActive); err != nil {
			return fmt.Errorf("mark active prune ladder generation: %w", err)
		}
		if _, err := tx.Exec(`INSERT INTO store_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, pruneLadderActiveGenerationKey, generation); err != nil {
			return fmt.Errorf("activate prune ladder marker: %w", err)
		}
	}
	if _, err := tx.Exec(`INSERT INTO store_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, metaKeyLastSyncTime, fmt.Sprint(syncedAt.Unix())); err != nil {
		return fmt.Errorf("activate prune ladder sync time: %w", err)
	}
	if _, err := tx.Exec(`INSERT INTO store_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, metaKeyIncompleteBaseline, "0"); err != nil {
		return fmt.Errorf("activate prune ladder baseline: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit prune ladder activation: %w", err)
	}
	if generation != "" {
		s.wakePruneLadderGC()
	}
	return nil
}

// AbandonPruneLadderStage marks an unpublished generation eligible for GC.
// The active marker check and update share one write transaction, so uncertain
// publication outcomes cannot poison the currently visible generation.
func (s *Store) AbandonPruneLadderStage(generation string) error {
	if generation == "" {
		return nil
	}
	s.writeMu.Lock()
	tx, err := s.db.Begin()
	if err != nil {
		s.writeMu.Unlock()
		return fmt.Errorf("abandon prune ladder begin: %w", err)
	}
	var active string
	err = tx.QueryRow(`SELECT value FROM store_meta WHERE key=?`, pruneLadderActiveGenerationKey).Scan(&active)
	if err != nil && err != sql.ErrNoRows {
		_ = tx.Rollback()
		s.writeMu.Unlock()
		return fmt.Errorf("read active prune ladder generation: %w", err)
	}
	if active == generation {
		_ = tx.Rollback()
		s.writeMu.Unlock()
		return fmt.Errorf("cannot abandon active prune ladder generation %q", generation)
	}
	res, err := tx.Exec(`UPDATE prune_ladder_stage_state SET complete=? WHERE generation=? AND complete IN (?,?,?) AND NOT EXISTS (SELECT 1 FROM store_meta WHERE key=? AND value=?)`, pruneLadderStageAbandoned, generation, pruneLadderStageIncomplete, pruneLadderStageCandidate, pruneLadderStageRetired, pruneLadderActiveGenerationKey, generation)
	if err != nil {
		_ = tx.Rollback()
		s.writeMu.Unlock()
		return fmt.Errorf("mark prune ladder stage abandoned: %w", err)
	}
	updated, err := res.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		s.writeMu.Unlock()
		return fmt.Errorf("count abandoned prune ladder stage: %w", err)
	}
	if err := tx.Commit(); err != nil {
		s.writeMu.Unlock()
		return fmt.Errorf("commit prune ladder abandonment: %w", err)
	}
	s.writeMu.Unlock()
	if updated > 0 {
		s.wakePruneLadderGC()
	}
	return nil
}

// GCPruneLadderStages reclaims one bounded batch from retired or abandoned
// generations. Old incomplete stages also expire after the crash-recovery grace;
// complete unpublished candidates are intentionally preserved.
func (s *Store) GCPruneLadderStages(limit int) (int, error) {
	if limit <= 0 {
		limit = pruneLadderStageGCDefaultLimit
	}
	cutoff := time.Now().Add(-pruneLadderIncompleteGCGrace).UnixNano()
	var eligible int
	if err := s.db.QueryRow(`SELECT EXISTS(
 SELECT 1 FROM prune_ladder_stage_state st
 WHERE (st.complete IN (?,?) OR (st.complete=? AND st.created_ns < ?))
 AND NOT EXISTS (SELECT 1 FROM store_meta m WHERE m.key=? AND m.value=st.generation)
)`, pruneLadderStageRetired, pruneLadderStageAbandoned, pruneLadderStageIncomplete, cutoff, pruneLadderActiveGenerationKey).Scan(&eligible); err != nil {
		return 0, fmt.Errorf("check prune ladder stage gc eligibility: %w", err)
	}
	if eligible == 0 {
		return 0, nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("prune ladder stage gc begin: %w", err)
	}
	defer tx.Rollback()
	res, err := tx.Exec(`DELETE FROM prune_ladder_stage WHERE rowid IN (
 SELECT r.rowid FROM prune_ladder_stage r JOIN prune_ladder_stage_state st ON st.generation=r.generation
	WHERE (st.complete IN (?,?) OR (st.complete=? AND st.created_ns < ?))
	AND NOT EXISTS (SELECT 1 FROM store_meta m WHERE m.key=? AND m.value=st.generation)
	LIMIT ?)`, pruneLadderStageRetired, pruneLadderStageAbandoned, pruneLadderStageIncomplete, cutoff, pruneLadderActiveGenerationKey, limit)
	if err != nil {
		return 0, fmt.Errorf("prune ladder stage gc rows: %w", err)
	}
	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("prune ladder stage gc count: %w", err)
	}
	manifestLimit := limit - int(deleted)
	if manifestLimit <= 0 {
		manifestLimit = 0
	}
	var manifests int64
	if manifestLimit > 0 {
		manifestRes, manifestErr := tx.Exec(`DELETE FROM prune_ladder_stage_state WHERE generation IN (
 SELECT st.generation FROM prune_ladder_stage_state st
	WHERE (st.complete IN (?,?) OR (st.complete=? AND st.created_ns < ?))
	AND NOT EXISTS (SELECT 1 FROM store_meta m WHERE m.key=? AND m.value=st.generation)
	AND NOT EXISTS (SELECT 1 FROM prune_ladder_stage r WHERE r.generation=st.generation)
	 LIMIT ?)`, pruneLadderStageRetired, pruneLadderStageAbandoned, pruneLadderStageIncomplete, cutoff, pruneLadderActiveGenerationKey, manifestLimit)
		if manifestErr != nil {
			return int(deleted), fmt.Errorf("prune ladder stage gc manifest: %w", manifestErr)
		}
		manifests, err = manifestRes.RowsAffected()
		if err != nil {
			return int(deleted), fmt.Errorf("prune ladder stage gc manifest count: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit prune ladder stage gc: %w", err)
	}
	return int(deleted + manifests), nil
}

func (s *Store) wakePruneLadderGC() {
	if s.pruneLadderGCWake == nil {
		return
	}
	select {
	case s.pruneLadderGCWake <- struct{}{}:
	default:
	}
}

func (s *Store) setPruneLadderGCBeforeBatch(hook func()) {
	s.pruneLadderGCHookMu.Lock()
	s.pruneLadderGCBeforeBatch = hook
	s.pruneLadderGCHookMu.Unlock()
}

func (s *Store) pruneLadderGCWorker() {
	defer close(s.pruneLadderGCDone)
	ticker := time.NewTicker(pruneLadderGCIdleInterval)
	defer ticker.Stop()
	retryDelay := pruneLadderGCRetryInterval
	var retry <-chan time.Time
	errorLogged := false
	for {
		select {
		case <-s.pruneLadderGCStop:
			return
		case <-s.pruneLadderGCWake:
			retry = nil
		case <-ticker.C:
		case <-retry:
			retry = nil
		}
		for {
			select {
			case <-s.pruneLadderGCStop:
				return
			default:
			}
			s.pruneLadderGCHookMu.RLock()
			hook := s.pruneLadderGCBeforeBatch
			s.pruneLadderGCHookMu.RUnlock()
			if hook != nil {
				hook()
			}
			deleted, err := s.GCPruneLadderStages(pruneLadderStageGCDefaultLimit)
			if err != nil {
				if !errorLogged {
					log.Printf("[metadata] prune ladder GC batch failed (will retry): %v", err)
					errorLogged = true
				}
				retry = time.After(retryDelay)
				retryDelay *= 2
				if retryDelay > pruneLadderGCMaxRetryInterval {
					retryDelay = pruneLadderGCMaxRetryInterval
				}
				break
			}
			retryDelay = pruneLadderGCRetryInterval
			errorLogged = false
			if deleted == 0 || deleted < pruneLadderStageGCDefaultLimit {
				break
			}
			select {
			case <-s.pruneLadderGCStop:
				return
			case <-time.After(2 * time.Millisecond):
			}
		}
	}
}

// PersistPruneLadderDelta applies a legacy ladder diff to prune_ladder before
// the first staged activation. It does not update an active generation and
// must not be used by the reconciler after activation. Upserts and deletes
// (paths pruned or reappeared) are chunked at pruneLadderTxChunk rows
// per transaction, each chunk its own writeMu acquisition, so even the 120k
// initial diff never holds the store's write lock long.
//
// Chunk-failure semantics: a failed chunk leaves earlier chunks committed and
// returns the error; the caller (persistPruneLadderDiff) then does NOT update
// its in-memory mirror of the table, so the next cycle's diff simply re-emits
// whatever didn't land — upserts are idempotent and deletes of missing rows
// are no-ops, so the table converges to the ladder without ever needing a
// rollback of committed chunks.
func (s *Store) PersistPruneLadderDelta(upserts map[string]int, deletes []string) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}
	nowNS := time.Now().UnixNano()

	if len(upserts) > 0 {
		type ladderRow struct {
			path  string
			count int
		}
		rows := make([]ladderRow, 0, len(upserts))
		for p, c := range upserts {
			rows = append(rows, ladderRow{path: p, count: c})
		}
		for start := 0; start < len(rows); start += pruneLadderTxChunk {
			end := min(start+pruneLadderTxChunk, len(rows))
			chunk := rows[start:end]

			s.writeMu.Lock()
			tx, err := s.db.Begin()
			if err != nil {
				s.writeMu.Unlock()
				return fmt.Errorf("prune ladder upsert begin: %w", err)
			}
			stmt, err := tx.Prepare(
				`INSERT INTO prune_ladder (path, absent_cycles, updated_ns) VALUES (?, ?, ?)
				 ON CONFLICT(path) DO UPDATE SET
				   absent_cycles = excluded.absent_cycles,
				   updated_ns    = excluded.updated_ns`)
			if err != nil {
				tx.Rollback()
				s.writeMu.Unlock()
				return fmt.Errorf("prune ladder upsert prepare: %w", err)
			}
			for _, r := range chunk {
				if _, err := stmt.Exec(r.path, r.count, nowNS); err != nil {
					stmt.Close()
					tx.Rollback()
					s.writeMu.Unlock()
					return fmt.Errorf("prune ladder upsert %q: %w", r.path, err)
				}
			}
			stmt.Close()
			if err := tx.Commit(); err != nil {
				s.writeMu.Unlock()
				return fmt.Errorf("prune ladder upsert commit: %w", err)
			}
			s.writeMu.Unlock()
		}
	}

	for start := 0; start < len(deletes); start += pruneLadderTxChunk {
		end := min(start+pruneLadderTxChunk, len(deletes))
		chunk := deletes[start:end]

		s.writeMu.Lock()
		tx, err := s.db.Begin()
		if err != nil {
			s.writeMu.Unlock()
			return fmt.Errorf("prune ladder delete begin: %w", err)
		}
		stmt, err := tx.Prepare(`DELETE FROM prune_ladder WHERE path = ?`)
		if err != nil {
			tx.Rollback()
			s.writeMu.Unlock()
			return fmt.Errorf("prune ladder delete prepare: %w", err)
		}
		for _, p := range chunk {
			if _, err := stmt.Exec(p); err != nil {
				stmt.Close()
				tx.Rollback()
				s.writeMu.Unlock()
				return fmt.Errorf("prune ladder delete %q: %w", p, err)
			}
		}
		stmt.Close()
		if err := tx.Commit(); err != nil {
			s.writeMu.Unlock()
			return fmt.Errorf("prune ladder delete commit: %w", err)
		}
		s.writeMu.Unlock()
	}

	return nil
}

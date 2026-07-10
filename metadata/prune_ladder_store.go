package metadata

import (
	"fmt"
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
// The table is written by the Reconciler's cycle-end snapshot-diff
// (persistPruneLadderDiff, prune_ladder_durable.go) and read once at
// RedisClient construction (loadDurablePruneLadder). It carries only the
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
`

// pruneLadderTxChunk bounds the rows written per transaction — and therefore
// per writeMu hold — by PersistPruneLadderDelta. The initial persist after a
// 120k-path mass delete is the sizing case: 12 bounded transactions instead
// of one giant journal spike, with writeMu fully released between chunks so
// NFS-driven entries writes interleave (same pattern as bulkInsertBatch).
const pruneLadderTxChunk = 10000

// LoadPruneLadder returns the full persisted ladder: path → consecutive
// absent-cycle count. Read path only (no writeMu), used once per process at
// RedisClient construction. An empty table returns an empty, non-nil map.
func (s *Store) LoadPruneLadder() (map[string]int, error) {
	rows, err := s.db.Query(`SELECT path, absent_cycles FROM prune_ladder`)
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
	return out, nil
}

// PersistPruneLadderDelta applies one reconcile cycle's ladder diff to the
// durable prune_ladder table: upserts (new or changed counts) and deletes
// (paths pruned or reappeared). Work is chunked at pruneLadderTxChunk rows
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

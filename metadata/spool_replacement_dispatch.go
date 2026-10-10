package metadata

import (
	"context"
	"database/sql"
	"time"
)

// Candidate IDs are scheduling hints, never permission to publish. Local DONE
// cleanup is independent of lease waits and newer edits; its journal owns only
// the exact private backup. READY publication retains the ordinary queue holds.
const replacementDispatchPredicate = `s.replacement_witness IS NOT NULL
 AND s.replacement_publication_required=1 AND s.punched_end=0
 AND s.clone_hold='' AND s.qualification_hold='' AND s.retired_by=0
 AND (p.phase IN ('armed','committed','settled','cleaning') OR NOT EXISTS (SELECT 1 FROM spool_replacement_dispositions d
  WHERE d.spool_id=s.id AND d.state='writer_changed'))
 AND ((s.drain_state='ready' AND p.phase IN ('queued','intent','arming','armed','committed')
 AND (s.wait_project='' OR p.phase IN ('armed','committed')) AND s.retry_after_ns<=?
 AND NOT EXISTS (SELECT 1 FROM spool_entries newer WHERE newer.nfs_path=s.nfs_path AND newer.id>s.id AND newer.retired_by=0))
 OR (s.drain_state='done' AND p.phase IN ('settled','cleaning')))`

// ReplacementDispatchHighWater freezes the finite boundary of one queue pass.
func (s *SpoolStore) ReplacementDispatchHighWater(ctx context.Context) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(id),0) FROM spool_entries`).Scan(&id)
	return id, err
}

func (s *SpoolStore) ReplacementDispatchPage(ctx context.Context, after, through int64, limit int) ([]int64, error) {
	if after < 0 || through < after || limit < 1 || limit > 64 {
		return nil, ErrMirrorConflict
	}
	rows, err := s.db.QueryContext(ctx, `SELECT s.id FROM spool_entries s JOIN spool_replacement_publications p ON p.spool_id=s.id
 WHERE `+replacementDispatchPredicate+` AND s.id>? AND s.id<=? ORDER BY s.id LIMIT ?`, time.Now().UnixNano(), after, through, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ReplacementDispatchCandidate rechecks scheduling holds and the canonical
// row/witness/journal/mirror in one snapshot, after resource admission. The
// coordinator still revalidates custody and owns all publication CAS operations.
func (s *SpoolStore) ReplacementDispatchCandidate(ctx context.Context, id int64) (*SpoolReplacementPublication, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var found int64
	err = tx.QueryRow(`SELECT s.id FROM spool_entries s JOIN spool_replacement_publications p ON p.spool_id=s.id
 WHERE `+replacementDispatchPredicate+` AND s.id=?`, time.Now().UnixNano(), id).Scan(&found)
	if err == sql.ErrNoRows {
		return nil, ErrMirrorConflict
	}
	if err != nil {
		return nil, err
	}
	p, _, _, err := readReplacementPublication(tx, id)
	if err != nil || p == nil {
		return nil, ErrMirrorConflict
	}
	return p, nil
}

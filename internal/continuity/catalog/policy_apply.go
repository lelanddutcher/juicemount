package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"time"
)

func (s *Store) applyPolicyChanges(ctx context.Context, tx *sql.Tx, op PolicyOperation, sequence, rows, maxRows int64) (int64, error) {
	operator, err := readSubject(ctx, tx, s.library, op.OperatorSubjectID)
	if errors.Is(err, ErrNotFound) && len(op.Changes) == 1 {
		v, e := policyChangeValue(op.Changes[0])
		if e != nil {
			return 0, e
		}
		create, ok := v.(*CreateSubject)
		if !ok || create.SubjectID != op.OperatorSubjectID || create.Value.Kind != "person" || !create.Value.Enabled {
			return 0, ErrConflict
		}
	} else if err != nil {
		return 0, err
	} else if operator.Value.Kind != "person" || !operator.Value.Enabled {
		return 0, ErrConflict
	}
	touched := make(map[SubjectID]bool)
	for _, change := range op.Changes {
		value, err := policyChangeValue(change)
		if err != nil {
			return 0, err
		}
		switch v := value.(type) {
		case *CreateSubject:
			if rows >= maxRows {
				return 0, ErrLimit
			}
			r, err := tx.ExecContext(ctx, `INSERT INTO policy_subjects(subject_id,library_id,kind,label,enabled,authorization_generation,created_sequence,updated_sequence) VALUES(?,?,?,?,?,1,?,?) ON CONFLICT(subject_id) DO NOTHING`, v.SubjectID, s.library, v.Value.Kind, v.Value.Label, v.Value.Enabled, sequence, sequence)
			if err := changedOne(r, err); err != nil {
				return 0, err
			}
			rows++
			touched[v.SubjectID] = true
		case *UpdateSubject:
			prior, err := readSubject(ctx, tx, s.library, v.SubjectID)
			if err != nil {
				return 0, err
			}
			if *prior.Expected() != *v.Expected || prior.Value.Kind != v.Value.Kind || v.SubjectID == op.OperatorSubjectID && !v.Value.Enabled {
				return 0, ErrConflict
			}
			r, err := tx.ExecContext(ctx, `UPDATE policy_subjects SET label=?,enabled=?,updated_sequence=? WHERE subject_id=? AND updated_sequence=? AND authorization_generation=?`, v.Value.Label, v.Value.Enabled, sequence, v.SubjectID, v.Expected.UpdatedSequence, v.Expected.AuthorizationGeneration)
			if err := changedOne(r, err); err != nil {
				return 0, err
			}
			touched[v.SubjectID] = true
		case *PutGrant:
			sub, err := readSubject(ctx, tx, s.library, v.Value.SubjectID)
			if err != nil {
				return 0, err
			}
			if sub.Value.Kind == "service" && containsAction(v.Value.Actions, "activity.record") {
				return 0, ErrConflict
			}
			if v.Value.Scope.Kind == "asset" {
				if _, err := readAsset(ctx, tx, s.library, v.Value.Scope.AssetID); err != nil {
					return 0, err
				}
			}
			prior, err := readGrant(ctx, tx, s.library, v.ID)
			if errors.Is(err, ErrNotFound) {
				if v.Expected != nil {
					return 0, ErrConflict
				}
				if rows >= maxRows {
					return 0, ErrLimit
				}
				_, err = tx.ExecContext(ctx, `INSERT INTO policy_grants(grant_id,library_id,subject_id,scope_kind,asset_id,actions,created_sequence,updated_sequence,removed) VALUES(?,?,?,?,?,?,?, ?,0)`, v.ID, s.library, v.Value.SubjectID, v.Value.Scope.Kind, nullableAsset(v.Value.Scope), actionsBytes(v.Value.Actions), sequence, sequence)
				if err != nil {
					return 0, err
				}
				rows++
			} else if err != nil {
				return 0, err
			} else {
				if v.Expected == nil || !reflect.DeepEqual(prior.Expected(), v.Expected) || prior.Value.SubjectID != v.Value.SubjectID {
					return 0, ErrConflict
				}
				r, err := tx.ExecContext(ctx, `UPDATE policy_grants SET scope_kind=?,asset_id=?,actions=?,updated_sequence=?,removed=0 WHERE grant_id=? AND updated_sequence=? AND removed=?`, v.Value.Scope.Kind, nullableAsset(v.Value.Scope), actionsBytes(v.Value.Actions), sequence, v.ID, v.Expected.UpdatedSequence, v.Expected.Removed)
				if err := changedOne(r, err); err != nil {
					return 0, err
				}
			}
			touched[v.Value.SubjectID] = true
		case *RemoveGrant:
			prior, err := readGrant(ctx, tx, s.library, v.ID)
			if err != nil {
				return 0, err
			}
			if !reflect.DeepEqual(prior.Expected(), v.Expected) || prior.Removed {
				return 0, ErrConflict
			}
			r, err := tx.ExecContext(ctx, `UPDATE policy_grants SET removed=1,updated_sequence=? WHERE grant_id=? AND updated_sequence=? AND removed=0`, sequence, v.ID, v.Expected.UpdatedSequence)
			if err := changedOne(r, err); err != nil {
				return 0, err
			}
			touched[prior.Value.SubjectID] = true
		}
	}
	for id := range touched {
		sub, err := readSubject(ctx, tx, s.library, id)
		if err != nil {
			return 0, err
		}
		if sub.CreatedSequence == sequence {
			continue
		}
		if sub.AuthorizationGeneration == math.MaxInt64 {
			return 0, ErrConflict
		}
		r, err := tx.ExecContext(ctx, `UPDATE policy_subjects SET authorization_generation=authorization_generation+1,updated_sequence=? WHERE subject_id=? AND authorization_generation=?`, sequence, id, sub.AuthorizationGeneration)
		if err := changedOne(r, err); err != nil {
			return 0, err
		}
	}
	return rows, nil
}

func nullableAsset(scope Scope) any {
	if scope.Kind == "library" {
		return nil
	}
	return scope.AssetID
}

func nextSequence(ctx context.Context, q queryer, table string) (int64, error) {
	var last int64
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(max(sequence),0) FROM `+table).Scan(&last); err != nil {
		return 0, err
	}
	if last == math.MaxInt64 {
		return 0, ErrConflict
	}
	return last + 1, nil
}

func insertPolicyEvent(ctx context.Context, tx *sql.Tx, e eventRow) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO policy_events(sequence,library_id,operation_id,request,payload_sha256,recorded_unix_nano,receipt) VALUES(?,?,?,?,?,?,?)`, e.sequence, e.library, e.id, e.request, e.digest, e.timestamp, e.receipt)
	return err
}

func (s *Store) applyPolicy(ctx context.Context, tx *sql.Tx, op PolicyOperation) (Receipt, error) {
	if err := s.checkVersion(ctx, tx); err != nil {
		return Receipt{}, err
	}
	data, digest, err := CanonicalPolicyRequest(op)
	if err != nil {
		return Receipt{}, err
	}
	var prior []byte
	err = tx.QueryRowContext(ctx, `SELECT request FROM policy_events WHERE operation_id=?`, op.ID).Scan(&prior)
	if err == nil {
		return Receipt{}, ErrConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Receipt{}, err
	}
	sequence, err := nextSequence(ctx, tx, "policy_events")
	if err != nil {
		return Receipt{}, err
	}
	if _, err := s.applyPolicyChanges(ctx, tx, op, sequence, 0, math.MaxInt64); err != nil {
		return Receipt{}, err
	}
	receipt := Receipt{1, s.library, op.ID, sequence, digest, time.Now().UnixNano()}
	receiptBytes, _ := json.Marshal(receipt)
	err = insertPolicyEvent(ctx, tx, eventRow{sequence, s.library, op.ID, data, digest, receipt.RecordedUnixNano, receiptBytes})
	if err == nil && supportsCollections(s.version) {
		err = appendCollectionOrder(ctx, tx, "legacy_sequence", sequence)
	}
	return receipt, err
}

func (s *Store) replayPolicyEvent(ctx context.Context, e eventRow, op PolicyOperation, rows, maxRows int64) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if err := requireIncomplete(ctx, tx); err != nil {
		return 0, err
	}
	if err := s.checkPaths(); err != nil {
		return 0, err
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM policy_events WHERE operation_id=?)`, e.id).Scan(&exists); err != nil {
		return 0, err
	}
	if exists {
		return 0, corrupt("duplicate policy operation")
	}
	next, err := s.applyPolicyChanges(ctx, tx, op, e.sequence, rows, maxRows)
	if err != nil {
		return 0, err
	}
	if err := insertPolicyEvent(ctx, tx, e); err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return next, nil
}

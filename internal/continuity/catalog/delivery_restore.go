package catalog

import (
	"bytes"
	"context"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// Replay shares production structural CAS, generations, immutable result
// tokens, tombstones and terminal-removal reservations. There is no operational
// owner incarnation to recover here: the exact historical request is retained,
// with enabled-person attribution checked at this mixed chronological prefix.
func (s *Store) replayBytePolicy(ctx context.Context, record BytePolicyRecord, rows, maxRows int64) (int64, error) {
	if !supportsDelivery(s.version) {
		return 0, ErrUpgradeRequired
	}
	if _, _, err := CanonicalBytePolicyRecord(record); err != nil {
		return 0, err
	}
	if record.Request.LibraryID != s.library {
		return 0, corrupt("byte policy replay library")
	}
	budget, err := newCollectionProjectionBudget(rows, maxRows)
	if err != nil {
		return 0, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if err := s.prepareCollectionReplay(ctx, tx); err != nil {
		return 0, err
	}
	operator, err := readSubject(ctx, tx, s.library, record.OperatorSubjectID)
	if err != nil {
		return 0, err
	}
	if operator.Value.Kind != "person" || !operator.Value.Enabled {
		return 0, ErrConflict
	}
	sequence, err := nextSequence(ctx, tx, "byte_policy_events")
	if err != nil {
		return 0, err
	}
	if sequence != record.Receipt.Sequence {
		return 0, corrupt("byte policy replay sequence")
	}
	request, _, err := CanonicalBytePolicyRequest(record.Request)
	if err != nil {
		return 0, err
	}
	if _, found, err := bytePolicyPrior(ctx, tx, record.Request, request); err != nil {
		return 0, err
	} else if found {
		return 0, corrupt("duplicate byte policy operation")
	}
	versions := make([]string, len(record.Receipt.Results))
	for i, result := range record.Receipt.Results {
		versions[i] = result.Version
		if _, err := readByteGrant(ctx, tx, string(s.library), result.ID); err == ErrNotFound {
			if err := budget.take(); err != nil {
				return 0, err
			}
		} else if err != nil {
			return 0, err
		}
		if err := budget.take(); err != nil {
			return 0, err
		} // immutable result
	}
	if err := budget.take(); err != nil {
		return 0, err
	} // mixed-order row
	actual, err := s.applyBytePolicyStructural(ctx, tx, record.Request, record.OperatorSubjectID, record.Receipt.RecordedUnixNano, versions)
	if err != nil {
		return 0, err
	}
	want, err := CanonicalBytePolicyReceipt(record.Receipt)
	if err != nil {
		return 0, err
	}
	have, err := CanonicalBytePolicyReceipt(actual)
	if err != nil {
		return 0, err
	}
	if !bytes.Equal(want, have) {
		return 0, corrupt("replayed byte policy receipt")
	}
	if err := s.commitCollectionReplay(ctx, tx); err != nil {
		return 0, err
	}
	return budget.rows, nil
}

func validateInactiveDeliveryImport(ctx context.Context, q queryer) error {
	var operational int64
	if err := q.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM delivery_sources)+(SELECT count(*) FROM delivery_source_heads)+(SELECT count(*) FROM delivery_source_events)+(SELECT count(*) FROM delivery_selections)+(SELECT count(*) FROM delivery_materializations)`).Scan(&operational); err != nil {
		return err
	}
	if operational != 0 {
		return corrupt("operational delivery state in portable import")
	}
	var clockRows, quotaRows, clock, sources, selections, attempts, canonical, reserved int64
	var clockLibrary, quotaLibrary continuity.LibraryID
	if err := q.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM delivery_clock),(SELECT count(*) FROM delivery_quota)`).Scan(&clockRows, &quotaRows); err != nil {
		return err
	}
	if clockRows != 1 || quotaRows != 1 {
		return corrupt("delivery operational singleton count")
	}
	if err := q.QueryRowContext(ctx, `SELECT c.library_id,c.high_water_unix,q.library_id,q.source_versions,q.selections,q.attempts,q.canonical_bytes,q.reserved_logical_bytes FROM delivery_clock c CROSS JOIN delivery_quota q WHERE c.singleton=1 AND q.singleton=1`).Scan(&clockLibrary, &clock, &quotaLibrary, &sources, &selections, &attempts, &canonical, &reserved); err != nil {
		return err
	}
	var library continuity.LibraryID
	if err := q.QueryRowContext(ctx, `SELECT library_id FROM catalog WHERE singleton=1`).Scan(&library); err != nil {
		return err
	}
	if clockLibrary != library || quotaLibrary != library || clock != 0 || sources != 0 || selections != 0 || attempts != 0 || canonical != 0 || reserved != 0 {
		return corrupt("nonzero operational delivery import")
	}
	return nil
}

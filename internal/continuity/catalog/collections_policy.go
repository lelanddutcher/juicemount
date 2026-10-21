package catalog

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"reflect"
)

func freshCollectionToken(ctx context.Context, q queryer, prefix string, id any) (string, error) {
	for i := 0; i < 4; i++ {
		candidate, err := randomIdentity(prefix)
		if err != nil {
			return "", err
		}
		var exists bool
		if prefix == "cv_" {
			err = q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM collection_events WHERE collection_id=? AND result_version=?)`, id, candidate).Scan(&exists)
		} else {
			err = q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM collection_policy_results WHERE grant_id=? AND result_version=?)`, id, candidate).Scan(&exists)
		}
		if err != nil {
			return "", err
		}
		if !exists {
			return candidate, nil
		}
	}
	return "", ErrConflict
}
func collectionNullable(id CollectionID) any {
	if id == "" {
		return nil
	}
	return id
}
func sumCollectionDelta(a, b CollectionQuotaCounters) (CollectionQuotaCounters, error) {
	x, y := quotaFields(a), quotaFields(b)
	for i := range x {
		n, ok := checkedQuotaAdd(x[i], y[i])
		if !ok {
			return CollectionQuotaCounters{}, ErrConflict
		}
		x[i] = n
	}
	return quotaFromFields(x), nil
}
func preserveCollectionAdministrator(ctx context.Context, q queryer, id CollectionID) error {
	rows, err := q.QueryContext(ctx, `SELECT g.subject_id,g.actions FROM collection_grants g JOIN policy_subjects s USING(subject_id) WHERE g.collection_id=? AND g.removed=0 AND s.kind='person' AND s.enabled=1 ORDER BY g.subject_id`, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	rights := make(map[SubjectID]map[string]bool)
	count := 0
	for rows.Next() {
		count++
		if count > 16384 {
			return corrupt("collection roster bound")
		}
		var subject SubjectID
		var b []byte
		var actions []string
		if err := rows.Scan(&subject, &b); err != nil {
			return err
		}
		if len(b) > 128 || decodeCanonicalValue(b, &actions) != nil {
			return corrupt("collection roster")
		}
		if rights[subject] == nil {
			rights[subject] = make(map[string]bool)
		}
		for _, action := range actions {
			rights[subject][action] = true
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, v := range rights {
		if v["collections.read"] && v["collections.edit"] && v["collections.admin"] {
			return nil
		}
	}
	return ErrConflict
}

// Applies structural projection changes inside its caller's immediate transaction.
// Live authorization and the single combined quota plan remain caller-owned.
func (s *Store) applyCollectionPolicy(ctx context.Context, tx *sql.Tx, op CollectionPolicyRequest, actor SubjectID, administration string, timestamp int64) (int64, CollectionQuotaCounters, error) {
	_, digest, err := CanonicalCollectionPolicyRequest(op)
	if err != nil {
		return 0, CollectionQuotaCounters{}, err
	}
	receiptBytes, err := CanonicalCollectionPolicyReceipt(CollectionPolicyReceipt{2, s.library, op.ID, digest, timestamp})
	if err != nil {
		return 0, CollectionQuotaCounters{}, err
	}
	versions := func(_ int, id CollectionGrantID) (CollectionGrantVersion, error) {
		v, err := freshCollectionToken(ctx, tx, "cgv_", id)
		return CollectionGrantVersion(v), err
	}
	return s.applyCollectionPolicyStructural(ctx, tx, op, actor, administration, timestamp, receiptBytes, versions, nil)
}

// This core has no clock, random source, credential or operational-owner lookup.
// The live wrapper resolves fresh versions at the original per-change point;
// recorded replay supplies its already-decoded result versions in that slot.
func (s *Store) applyCollectionPolicyStructural(ctx context.Context, tx *sql.Tx, op CollectionPolicyRequest, actor SubjectID, administration string, timestamp int64, receiptBytes []byte, versions func(int, CollectionGrantID) (CollectionGrantVersion, error), budget *collectionProjectionBudget) (int64, CollectionQuotaCounters, error) {
	var delta CollectionQuotaCounters
	request, digest, err := CanonicalCollectionPolicyRequest(op)
	if err != nil {
		return 0, delta, err
	}
	if versions == nil {
		return 0, delta, invalid("collection policy versions")
	}
	if op.LibraryID != s.library {
		return 0, delta, invalid("collection policy library")
	}
	receipt, err := validateRecordedCollectionPolicyReceipt(op, receiptBytes, timestamp)
	if err != nil {
		return 0, delta, err
	}
	if timestamp <= 0 || actor.Validate() != nil {
		return 0, delta, invalid("collection policy attribution")
	}
	if administration != "local_owner" && administration != "collection_service" {
		return 0, delta, invalid("collection policy authority")
	}
	if (op.BindingOperationID != "") != (administration == "collection_service") {
		return 0, delta, invalid("collection policy administration")
	}
	if op.CollectionID != "" && administration == "local_owner" {
		if _, err := readAnyCollectionDefinition(ctx, tx, op.CollectionID); err != nil {
			return 0, delta, err
		}
	}
	sequence, err := nextSequence(ctx, tx, "collection_policy_events")
	if err != nil {
		return 0, delta, err
	}
	results := make([]CollectionGrantResult, 0, len(op.Changes))
	touched := make(map[SubjectID]bool)
	for index, change := range op.Changes {
		prior, readErr := readCollectionGrant(ctx, tx, change.ID)
		missing := errors.Is(readErr, ErrNotFound)
		if readErr != nil && !missing {
			return 0, delta, readErr
		}
		if missing && change.Expected != nil || !missing && (change.Expected == nil || !reflect.DeepEqual(prior.expected, *change.Expected)) {
			return 0, delta, ErrConflict
		}
		value := CollectionGrantValue{}
		removed := change.Kind == "remove"
		if removed {
			if missing || prior.expected.Removed {
				return 0, delta, ErrConflict
			}
			value = prior.expected.Value
			delta.ReservedPolicyEvents--
			delta.ReservedPolicyReceipts--
			delta.ReservedBytes -= CollectionRemovalReserveBytes
		} else {
			value = *change.Value
			sub, err := readSubject(ctx, tx, s.library, value.SubjectID)
			if err != nil {
				return 0, delta, err
			}
			if !sub.Value.Enabled || sub.Value.Kind != "person" {
				return 0, delta, ErrConflict
			}
			if missing {
				delta.LifetimeGrants++
			}
			if missing || prior.expected.Removed {
				delta.ReservedPolicyEvents++
				delta.ReservedPolicyReceipts++
				delta.ReservedBytes += CollectionRemovalReserveBytes
			}
		}
		version, err := versions(index, change.ID)
		if err != nil {
			return 0, delta, err
		}
		if err := requireFreshCollectionGrantVersion(ctx, tx, change.ID, version); err != nil {
			return 0, delta, err
		}
		if missing {
			if err := budget.take(); err != nil {
				return 0, delta, err
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO collection_grants(grant_id,library_id,subject_id,scope_kind,collection_id,actions,grant_version,removed,created_sequence,updated_sequence) VALUES(?,?,?,?,?,?,?,0,?,?)`, change.ID, s.library, value.SubjectID, value.Scope.Kind, collectionNullable(value.Scope.CollectionID), actionsBytes(value.Actions), version, sequence, sequence)
		} else {
			err = changedOne(tx.ExecContext(ctx, `UPDATE collection_grants SET actions=?,grant_version=?,removed=?,updated_sequence=? WHERE grant_id=? AND grant_version=? AND removed=?`, actionsBytes(value.Actions), version, removed, sequence, change.ID, prior.expected.Version, prior.expected.Removed))
		}
		if err != nil {
			return 0, delta, err
		}
		results = append(results, CollectionGrantResult{change.ID, CollectionGrantVersion(version), removed})
		touched[value.SubjectID] = true
	}
	if op.CollectionID != "" && op.Category != "security_remove" {
		if err := preserveCollectionAdministrator(ctx, tx, op.CollectionID); err != nil {
			return 0, delta, err
		}
	}
	for subject := range touched {
		prior, err := readSubject(ctx, tx, s.library, subject)
		if err != nil {
			return 0, delta, err
		}
		if prior.AuthorizationGeneration == math.MaxInt64 {
			return 0, delta, ErrConflict
		}
		if err := changedOne(tx.ExecContext(ctx, `UPDATE policy_subjects SET authorization_generation=authorization_generation+1 WHERE subject_id=? AND authorization_generation=?`, subject, prior.AuthorizationGeneration)); err != nil {
			return 0, delta, err
		}
	}
	if op.CollectionID != "" {
		if err := changedOne(tx.ExecContext(ctx, `UPDATE collection_identities SET policy_generation=policy_generation+1 WHERE collection_id=? AND policy_generation<9223372036854775807`, op.CollectionID)); err != nil {
			return 0, delta, err
		}
	}
	if op.Category == "security_remove" {
		if _, err := collectionSecurityRemovalCost(op, receipt); err != nil {
			return 0, delta, err
		}
	}
	record := CollectionPolicyRecord{2, op, administration, actor, results, timestamp}
	if _, _, err := CanonicalCollectionPolicyRecord(record); err != nil {
		return 0, delta, err
	}
	namespace := ""
	if administration == "collection_service" {
		namespace = string(actor)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO collection_policy_events(sequence,library_id,administration,namespace_subject_id,operator_subject_id,operation_id,category,collection_id,request,payload_sha256,receipt,recorded_unix_nano) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, sequence, s.library, administration, namespace, actor, op.ID, op.Category, collectionNullable(op.CollectionID), request, digest, receiptBytes, timestamp)
	if err != nil {
		return 0, delta, err
	}
	for _, r := range results {
		if err := budget.take(); err != nil {
			return 0, delta, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO collection_policy_results(policy_sequence,grant_id,result_version,removed) VALUES(?,?,?,?)`, sequence, r.ID, r.Version, r.Removed); err != nil {
			return 0, delta, err
		}
	}
	delta.PolicyEvents = 1
	delta.PolicyReceipts = 1
	delta.CanonicalBytes = int64(len(request) + len(receiptBytes))
	return sequence, delta, nil
}

func requireCollectionActor(ctx context.Context, q queryer, s *Store, c credential, subject Subject, collection CollectionID, actions ...string) error {
	if subject.Value.Kind != "person" || !subject.Value.Enabled {
		return ErrNotFound
	}
	return requireCollectionRights(ctx, q, c, collection, actions...)
}

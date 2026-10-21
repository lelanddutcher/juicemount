package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

type collectionMutationBinding struct {
	auth       discoveryBinding
	definition collectionDefinition
}

// MutateCollection owns one bounded authenticated operation. A successful
// receipt describes history; an error after commit never promises noncommit.
func (d *DiscoveryService) MutateCollection(parent context.Context, bearer []byte, input CollectionRequest, output io.Writer) (result DisclosureResult, resultErr error) {
	if d == nil || output == nil {
		return result, invalid("collection service or writer")
	}
	ctx, leave, err := d.enterPreferences(parent)
	if err != nil {
		return result, err
	}
	defer leave()
	if !supportsCollections(d.store.version) {
		return result, ErrUpgradeRequired
	}
	data, _, err := CanonicalCollectionRequest(input)
	if err != nil {
		return result, err
	}
	op, err := DecodeCollectionRequest(data)
	if err != nil {
		return result, err
	}
	if op.LibraryID != d.library {
		return result, ErrNotFound
	}
	tx, err := d.store.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	if err := d.store.checkVersion(ctx, tx); err != nil {
		return result, err
	}
	handle, c, subject, err := d.store.authenticateSnapshot(ctx, tx, d.library, bearer)
	if err != nil {
		return result, err
	}
	result.SubjectID, result.Selector = c.subject, c.selector
	if subject.Value.Kind != "person" || c.scope.Kind != "library" {
		return result, ErrNotFound
	}
	definition, defErr := readCollectionDefinition(ctx, tx, op.CollectionID)
	if defErr != nil && !errors.Is(defErr, ErrNotFound) {
		return result, defErr
	}
	if defErr == nil {
		if err := requireCollectionActor(ctx, tx, d.store, c, subject, op.CollectionID, "collections.read", "collections.edit"); err != nil {
			return result, err
		}
	} else {
		if op.Kind != "create" {
			return result, ErrNotFound
		}
		if err := requireCollectionActor(ctx, tx, d.store, c, subject, "", "collections.create"); err != nil {
			return result, err
		}
		for _, a := range []string{"collections.read", "collections.edit", "collections.admin"} {
			if !containsAction(c.actions, a) {
				return result, ErrNotFound
			}
		}
	}
	var receipt CollectionReceipt
	var old, receiptData []byte
	err = tx.QueryRowContext(ctx, `SELECT request,receipt FROM collection_events WHERE library_id=? AND actor_subject_id=? AND operation_id=?`, d.library, c.subject, op.ID).Scan(&old, &receiptData)
	if err == nil {
		if !bytes.Equal(old, data) {
			return result, ErrChangedRequest
		}
		receipt, err = DecodeCollectionReceipt(receiptData)
		if err != nil {
			return result, corrupt("collection retry receipt")
		}
		if receipt.OperationID != op.ID || receipt.CollectionID != op.CollectionID || defErr != nil {
			return result, corrupt("collection retry identity")
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return result, err
	} else {
		if op.Kind == "create" && defErr == nil {
			return result, ErrConflict
		}
		if op.Kind != "create" && definition.version != op.ExpectedVersion {
			return result, ErrConflict
		}
		if op.Kind == "archive" || op.Kind == "grants" {
			if err := requireCollectionRights(ctx, tx, c, op.CollectionID, "collections.admin"); err != nil {
				return result, err
			}
		}
		receipt, err = d.store.applyCollectionContent(ctx, tx, op, data, c)
		if err != nil {
			return result, err
		}
	}
	// The baseline is CURRENT/post-operation, not the historical receipt token.
	_, currentSubject, err := d.store.currentHandle(ctx, tx, handle)
	if err != nil {
		return result, err
	}
	definition, err = readCollectionDefinition(ctx, tx, op.CollectionID)
	if err != nil {
		return result, err
	}
	binding := collectionMutationBinding{discoveryBinding{handle, currentSubject.AuthorizationGeneration, c.issued, c.expires, nil}, definition}
	body, err := CanonicalCollectionReceipt(receipt)
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if now := d.store.now().Unix(); now < c.issued || now >= c.expires {
		return result, ErrUnauthenticated
	}
	if err := d.store.checkPaths(); err != nil {
		return result, err
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("collection commit outcome uncertain: %w", err)
	}
	if d.beforeFence != nil {
		if err := d.beforeFence(ctx); err != nil {
			return result, err
		}
	}
	if err := d.store.checkPaths(); err != nil {
		return result, err
	}
	if err := d.collectionMutationFence(ctx, binding); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if now := d.store.now().Unix(); now < c.issued || now >= c.expires {
		return result, ErrUnauthenticated
	}
	if err := d.store.checkPaths(); err != nil {
		return result, err
	}
	result.Started = true
	n, err := output.Write(body)
	if err != nil {
		return result, err
	}
	if n != len(body) {
		return result, io.ErrShortWrite
	}
	return result, nil
}
func (d *DiscoveryService) collectionMutationFence(ctx context.Context, b collectionMutationBinding) error {
	tx, done, err := d.store.readSnapshot(ctx)
	if err != nil {
		return err
	}
	c, subject, err := d.store.currentHandle(ctx, tx, b.auth.handle)
	if err == nil && subject.AuthorizationGeneration != b.auth.generation {
		err = errDiscoveryRestart
	}
	if err == nil {
		err = requireCollectionActor(ctx, tx, d.store, c, subject, b.definition.id, "collections.read", "collections.edit")
	}
	if err == nil {
		current, e := readCollectionDefinition(ctx, tx, b.definition.id)
		err = e
		if err == nil && (current.version != b.definition.version || current.generation != b.definition.generation) {
			err = errDiscoveryRestart
		}
	}
	return errors.Join(err, d.finishSnapshot(done))
}
func (s *Store) applyCollectionContent(ctx context.Context, tx *sql.Tx, op CollectionRequest, data []byte, c credential) (CollectionReceipt, error) {
	timestamp := s.now().UnixNano()
	if timestamp <= 0 {
		return CollectionReceipt{}, invalid("collection time")
	}
	version, err := freshCollectionToken(ctx, tx, "cv_", op.CollectionID)
	if err != nil {
		return CollectionReceipt{}, err
	}
	record := CollectionRecord{1, op, c.subject, CollectionVersion(version), "", timestamp}
	if op.Kind == "create" || op.Kind == "grants" {
		record.GeneratedPolicyOperationID = generatedCollectionPolicyID(op.ID)
	}
	receipt := CollectionReceipt{1, op.ID, op.CollectionID, record.ResultVersion}
	receiptBytes, err := CanonicalCollectionReceipt(receipt)
	if err != nil {
		return receipt, err
	}
	assetCheck := func(asset continuity.AssetID) error { return collectionAssetAllowed(ctx, tx, s.library, c, asset) }
	policyApply := func(p CollectionPolicyRequest) (int64, CollectionQuotaCounters, error) {
		return s.applyCollectionPolicy(ctx, tx, p, c.subject, "collection_service", timestamp)
	}
	return s.applyCollectionContentStructural(ctx, tx, record, data, receiptBytes, false, nil, assetCheck, policyApply)
}

// Structural history and live writes share this transaction core. All clocks,
// random tokens and current-credential checks belong to the live caller. Replay
// supplies recorded inputs and historical membership checks instead.
func (s *Store) applyCollectionContentStructural(ctx context.Context, tx *sql.Tx, record CollectionRecord, data, receiptBytes []byte, staged bool, budget *collectionProjectionBudget, assetCheck func(continuity.AssetID) error, policyApply func(CollectionPolicyRequest) (int64, CollectionQuotaCounters, error)) (CollectionReceipt, error) {
	if assetCheck == nil || policyApply == nil {
		return CollectionReceipt{}, invalid("collection structural callbacks")
	}
	op := record.Request
	receipt, err := validateRecordedCollectionContent(record, data, receiptBytes, s.library)
	if err != nil {
		return CollectionReceipt{}, err
	}
	sequence, err := nextSequence(ctx, tx, "collection_events")
	if err != nil {
		return receipt, err
	}
	timestamp, version, actor := record.RecordedUnixNano, string(record.ResultVersion), record.ActorSubjectID
	subject, err := readSubject(ctx, tx, s.library, actor)
	if err != nil {
		return receipt, err
	}
	if !subject.Value.Enabled || subject.Value.Kind != "person" {
		return receipt, ErrConflict
	}
	if err := requireFreshCollectionVersion(ctx, tx, op.CollectionID, record.ResultVersion); err != nil {
		return receipt, err
	}
	if op.Kind != "create" {
		current, err := readCollectionDefinition(ctx, tx, op.CollectionID)
		if err != nil {
			return receipt, err
		}
		if current.version != op.ExpectedVersion {
			return receipt, ErrConflict
		}
	}
	value, err := collectionValue(op)
	if err != nil {
		return receipt, err
	}
	delta := CollectionQuotaCounters{ContentEvents: 1, ContentReceipts: 1, CanonicalBytes: int64(len(data) + len(receiptBytes))}
	var policy *CollectionPolicyRequest
	switch v := value.(type) {
	case *CreateCollection:
		_, digest, _ := CanonicalCollectionRequest(op)
		if staged {
			if err := consumeCollectionDeclaration(ctx, tx, s.library, record, digest); err != nil {
				return receipt, err
			}
		} else {
			if err := budget.take(); err != nil {
				return receipt, err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO collection_identities(collection_id,library_id,creator_subject_id,creation_operation_id,creation_payload_sha256,policy_generation) VALUES(?,?,?,?,?,0)`, op.CollectionID, s.library, actor, op.ID, digest); err != nil {
				return receipt, ErrConflict
			}
		}
		if err := budget.take(); err != nil {
			return receipt, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO collection_quota(collection_id,`+collectionQuotaColumns+`) VALUES(?,0,0,0,0,0,0,0,0,0,0,0,0)`, op.CollectionID); err != nil {
			return receipt, err
		}
		for _, entry := range v.Entries {
			if err := assetCheck(entry.AssetID); err != nil {
				return receipt, err
			}
			if err := budget.take(); err != nil {
				return receipt, err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO collection_entries(entry_id,library_id,collection_id,asset_id,removed,created_sequence,updated_sequence) VALUES(?,?,?,?,0,?,?)`, entry.ID, s.library, op.CollectionID, entry.AssetID, sequence, sequence); err != nil {
				return receipt, ErrConflict
			}
		}
		delta.Collections = 1
		delta.ActiveEntries = int64(len(v.Entries))
		delta.LifetimeEntries = delta.ActiveEntries
		grant := CollectionGrantValue{actor, CollectionGrantScope{"collection", op.CollectionID}, []string{"collections.admin", "collections.edit", "collections.read"}}
		p := CollectionPolicyRequest{2, s.library, generatedCollectionPolicyID(op.ID), "collection", op.CollectionID, op.ID, digest, []CollectionGrantChange{{"put", v.CreatorGrantID, nil, &grant}}}
		policy = &p
		if err := budget.take(); err != nil {
			return receipt, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO collection_definitions(collection_id,kind,name,archived,definition_version,created_sequence,updated_sequence) VALUES(?,'manual',?,0,?,?,?)`, op.CollectionID, v.Name, version, sequence, sequence); err != nil {
			return receipt, err
		}
	case *RenameCollection:
		current, err := readCollectionDefinition(ctx, tx, op.CollectionID)
		if err != nil {
			return receipt, err
		}
		if current.name == v.Name {
			return receipt, ErrConflict
		}
		if err := changedOne(tx.ExecContext(ctx, `UPDATE collection_definitions SET name=? WHERE collection_id=? AND definition_version=?`, v.Name, op.CollectionID, op.ExpectedVersion)); err != nil {
			return receipt, err
		}
	case *SetCollectionArchived:
		current, err := readCollectionDefinition(ctx, tx, op.CollectionID)
		if err != nil {
			return receipt, err
		}
		if current.archived == v.Archived {
			return receipt, ErrConflict
		}
		if err := changedOne(tx.ExecContext(ctx, `UPDATE collection_definitions SET archived=? WHERE collection_id=? AND definition_version=?`, v.Archived, op.CollectionID, op.ExpectedVersion)); err != nil {
			return receipt, err
		}
	case *EditCollectionEntries:
		for _, change := range v.Changes {
			if err := assetCheck(change.AssetID); err != nil {
				return receipt, err
			}
			if change.Kind == "add" {
				if err := budget.take(); err != nil {
					return receipt, err
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO collection_entries(entry_id,library_id,collection_id,asset_id,removed,created_sequence,updated_sequence) VALUES(?,?,?,?,0,?,?)`, change.ID, s.library, op.CollectionID, change.AssetID, sequence, sequence); err != nil {
					return receipt, ErrConflict
				}
				delta.ActiveEntries++
				delta.LifetimeEntries++
			} else {
				removed := change.Kind == "remove"
				if err := changedOne(tx.ExecContext(ctx, `UPDATE collection_entries SET removed=?,updated_sequence=? WHERE entry_id=? AND collection_id=? AND asset_id=? AND removed=?`, removed, sequence, change.ID, op.CollectionID, change.AssetID, !removed)); err != nil {
					return receipt, err
				}
				if removed {
					delta.ActiveEntries--
				} else {
					delta.ActiveEntries++
				}
			}
		}
	case *EditCollectionGrants:
		_, digest, _ := CanonicalCollectionRequest(op)
		p := CollectionPolicyRequest{2, s.library, generatedCollectionPolicyID(op.ID), "collection", op.CollectionID, op.ID, digest, v.Changes}
		policy = &p
	}
	var child any
	if policy != nil {
		p, change, err := policyApply(*policy)
		if err != nil {
			return receipt, err
		}
		child = p
		delta, err = sumCollectionDelta(delta, change)
		if err != nil {
			return receipt, err
		}
	}
	if op.Kind != "create" {
		if err := changedOne(tx.ExecContext(ctx, `UPDATE collection_definitions SET definition_version=?,updated_sequence=? WHERE collection_id=? AND definition_version=?`, version, sequence, op.CollectionID, op.ExpectedVersion)); err != nil {
			return receipt, err
		}
	}
	usage, err := readCollectionQuota(ctx, tx, op.CollectionID)
	if err != nil {
		return receipt, err
	}
	local := delta
	local.Collections = 0
	after, err := planCollectionQuota(usage, CollectionQuotaDelta{delta, &local})
	if err != nil {
		return receipt, err
	}
	if err := saveCollectionQuota(ctx, tx, op.CollectionID, after); err != nil {
		return receipt, err
	}
	_, digest, _ := CanonicalCollectionRequest(op)
	_, err = tx.ExecContext(ctx, `INSERT INTO collection_events(sequence,library_id,actor_subject_id,operation_id,collection_id,request,payload_sha256,receipt,result_version,generated_policy_sequence,recorded_unix_nano) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, sequence, s.library, actor, op.ID, op.CollectionID, data, digest, receiptBytes, version, child, timestamp)
	if err != nil {
		return receipt, err
	}
	if err := budget.take(); err != nil {
		return receipt, err
	}
	if err := appendCollectionOrder(ctx, tx, "content_sequence", sequence); err != nil {
		return receipt, err
	}
	return receipt, nil
}

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

type savedQueryMutationBinding struct {
	auth       discoveryBinding
	definition savedQueryDefinition
}

// MutateSavedQuery owns one bounded authenticated operation. A successful
// receipt describes history; an error after commit never promises noncommit.
func (d *DiscoveryService) MutateSavedQuery(parent context.Context, bearer []byte, input SavedQueryRequest, output io.Writer) (result DisclosureResult, resultErr error) {
	if d == nil || output == nil {
		return result, invalid("collection service or writer")
	}
	ctx, leave, err := d.enterPreferences(parent)
	if err != nil {
		return result, err
	}
	defer leave()
	if !supportsSavedQueries(d.store.version) {
		return result, ErrUpgradeRequired
	}
	data, _, err := CanonicalSavedQueryRequest(input)
	if err != nil {
		return result, err
	}
	op, err := DecodeSavedQueryRequest(data)
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
	defer func() {
		err := tx.Rollback()
		if !errors.Is(err, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, err)
		}
	}()
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
	kind, kindErr := collectionStoredKind(ctx, tx, op.CollectionID)
	if kindErr != nil && !errors.Is(kindErr, ErrNotFound) {
		return result, kindErr
	}
	if kindErr == nil && kind != "saved_query" {
		return result, ErrNotFound
	}
	definition, defErr := readSavedQueryDefinition(ctx, tx, op.CollectionID)
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
	var receipt SavedQueryReceipt
	var old, receiptData []byte
	var retainedSequence int64
	err = tx.QueryRowContext(ctx, `SELECT sequence,CASE WHEN length(request) BETWEEN 1 AND 32768 THEN request END,CASE WHEN length(receipt) BETWEEN 1 AND 1024 THEN receipt END FROM collection_events WHERE library_id=? AND actor_subject_id=? AND operation_id=?`, d.library, c.subject, op.ID).Scan(&retainedSequence, &old, &receiptData)
	if err == nil {
		if !bytes.Equal(old, data) {
			return result, ErrChangedRequest
		}
		record, retainedReceipt, _, readErr := readSavedQueryContentRecord(ctx, tx, d.library, retainedSequence)
		if readErr != nil || !bytes.Equal(retainedReceipt, receiptData) || record.ActorSubjectID != c.subject {
			return result, corrupt("saved query retry record")
		}
		receipt, err = DecodeSavedQueryReceipt(receiptData)
		if err != nil {
			return result, corrupt("collection retry receipt")
		}
		if receipt.OperationID != op.ID || receipt.CollectionID != op.CollectionID || receipt.ResultVersion == op.ExpectedVersion || defErr != nil {
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
		receipt, err = d.store.applySavedQueryContent(ctx, tx, op, data, c)
		if err != nil {
			return result, err
		}
	}
	// Publication binds current state; the receipt remains historical.
	currentCredential, currentSubject, err := d.store.currentHandle(ctx, tx, handle)
	if err != nil {
		return result, err
	}
	if err := d.store.checkVersion(ctx, tx); err != nil {
		return result, err
	}
	if err := requireCollectionActor(ctx, tx, d.store, currentCredential, currentSubject, op.CollectionID, "collections.read", "collections.edit"); err != nil {
		return result, err
	}
	definition, err = readSavedQueryDefinition(ctx, tx, op.CollectionID)
	if err != nil {
		return result, err
	}
	binding := savedQueryMutationBinding{discoveryBinding{handle, currentSubject.AuthorizationGeneration, c.issued, c.expires, nil}, definition}
	body, err := CanonicalSavedQueryReceipt(receipt)
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
	if err := d.savedQueryMutationFence(ctx, binding); err != nil {
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
func (d *DiscoveryService) savedQueryMutationFence(ctx context.Context, b savedQueryMutationBinding) error {
	tx, done, err := d.store.readSnapshot(ctx)
	if err != nil {
		return err
	}
	c, subject, err := d.store.currentHandle(ctx, tx, b.auth.handle)
	// Current rights precede baseline drift: loss of either right is not_found.
	if err == nil {
		err = requireCollectionActor(ctx, tx, d.store, c, subject, b.definition.id, "collections.read", "collections.edit")
	}
	if err == nil && subject.AuthorizationGeneration != b.auth.generation {
		err = errDiscoveryRestart
	}
	if err == nil {
		current, e := readSavedQueryDefinition(ctx, tx, b.definition.id)
		err = e
		if err == nil && (current.version != b.definition.version || current.generation != b.definition.generation) {
			err = errDiscoveryRestart
		}
	}
	return errors.Join(err, d.finishSnapshot(done))
}
func (s *Store) applySavedQueryContent(ctx context.Context, tx *sql.Tx, op SavedQueryRequest, data []byte, c credential) (SavedQueryReceipt, error) {
	timestamp := s.now().UnixNano()
	if timestamp <= 0 {
		return SavedQueryReceipt{}, invalid("collection time")
	}
	version, err := freshCollectionToken(ctx, tx, "cv_", op.CollectionID)
	if err != nil {
		return SavedQueryReceipt{}, err
	}
	record := SavedQueryRecord{2, op, c.subject, CollectionVersion(version), "", timestamp}
	if op.Kind == "create" || op.Kind == "grants" {
		record.GeneratedPolicyOperationID = generatedCollectionPolicyID(op.ID)
	}
	receipt := SavedQueryReceipt{2, op.ID, op.CollectionID, record.ResultVersion}
	receiptBytes, err := CanonicalSavedQueryReceipt(receipt)
	if err != nil {
		return receipt, err
	}
	assetCheck := func(asset continuity.AssetID) error { return collectionAssetAllowed(ctx, tx, s.library, c, asset) }
	policyApply := func(p CollectionPolicyRequest) (int64, CollectionQuotaCounters, error) {
		return s.applyCollectionPolicy(ctx, tx, p, c.subject, "collection_service", timestamp)
	}
	return s.applySavedQueryContentStructural(ctx, tx, record, data, receiptBytes, nil, assetCheck, policyApply)
}

// The caller owns authorization and commit. The core consumes resolved tokens,
// time and child-policy results, and charges the existing shared counters once.
func (s *Store) applySavedQueryContentStructural(ctx context.Context, tx *sql.Tx, record SavedQueryRecord, data, receiptBytes []byte, budget *collectionProjectionBudget, assetCheck func(continuity.AssetID) error, policyApply func(CollectionPolicyRequest) (int64, CollectionQuotaCounters, error)) (SavedQueryReceipt, error) {
	return s.applySavedQueryContentStructuralMode(ctx, tx, record, data, receiptBytes, false, budget, assetCheck, policyApply)
}
func (s *Store) applySavedQueryContentStructuralMode(ctx context.Context, tx *sql.Tx, record SavedQueryRecord, data, receiptBytes []byte, staged bool, budget *collectionProjectionBudget, assetCheck func(continuity.AssetID) error, policyApply func(CollectionPolicyRequest) (int64, CollectionQuotaCounters, error)) (SavedQueryReceipt, error) {
	receipt, err := validateRecordedSavedQueryContent(record, data, receiptBytes, s.library)
	if err != nil {
		return SavedQueryReceipt{}, err
	}
	if assetCheck == nil || policyApply == nil {
		return receipt, invalid("saved query callbacks")
	}
	op := record.Request
	subject, err := readSubject(ctx, tx, s.library, record.ActorSubjectID)
	if err != nil {
		return receipt, err
	}
	if !subject.Value.Enabled || subject.Value.Kind != "person" {
		return receipt, ErrConflict
	}
	value, err := savedQueryValue(op)
	if err != nil {
		return receipt, err
	}
	var current savedQueryDefinition
	if op.Kind != "create" {
		current, err = readSavedQueryDefinition(ctx, tx, op.CollectionID)
		if err != nil {
			return receipt, err
		}
		if current.version != op.ExpectedVersion {
			return receipt, ErrConflict
		}
	}
	if err := requireFreshCollectionVersion(ctx, tx, op.CollectionID, record.ResultVersion); err != nil {
		return receipt, err
	}
	var proposed *SavedQueryDefinition
	switch v := value.(type) {
	case *CreateSavedQuery:
		proposed = &v.Definition
	case *ReplaceSavedQueryDefinition:
		proposed = &v.Definition
	}
	var definition []byte
	if proposed != nil {
		definition, _, err = CanonicalSavedQueryDefinition(*proposed)
		if err != nil {
			return receipt, err
		}
		for _, anchor := range proposed.ContextAssetIDs {
			if err := assetCheck(anchor); err != nil {
				return receipt, err
			}
		}
	}
	sequence, err := nextSequence(ctx, tx, "collection_events")
	if err != nil {
		return receipt, err
	}
	_, digest, _ := CanonicalSavedQueryRequest(op)
	delta := CollectionQuotaCounters{ContentEvents: 1, ContentReceipts: 1, CanonicalBytes: int64(len(data) + len(receiptBytes))}
	var policy *CollectionPolicyRequest
	switch v := value.(type) {
	case *CreateSavedQuery:
		if staged {
			if err := consumeTypedCollectionDeclaration(ctx, tx, s.library, op.CollectionID, record.ActorSubjectID, op.ID, digest); err != nil {
				return receipt, err
			}
		} else {
			if err := budget.take(); err != nil {
				return receipt, err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO collection_identities(collection_id,library_id,creator_subject_id,creation_operation_id,creation_payload_sha256,policy_generation) VALUES(?,?,?,?,?,0)`, op.CollectionID, s.library, record.ActorSubjectID, op.ID, digest); err != nil {
				return receipt, ErrConflict
			}
		}
		if err := budget.take(); err != nil {
			return receipt, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO collection_quota(collection_id,`+collectionQuotaColumns+`) VALUES(?,0,0,0,0,0,0,0,0,0,0,0,0)`, op.CollectionID); err != nil {
			return receipt, err
		}
		if err := budget.take(); err != nil {
			return receipt, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO saved_query_definitions(collection_id,kind,name,archived,definition_version,created_sequence,updated_sequence,definition) VALUES(?,'saved_query',?,0,?,?,?,?)`, op.CollectionID, v.Name, record.ResultVersion, sequence, sequence, definition); err != nil {
			return receipt, err
		}
		grant := CollectionGrantValue{record.ActorSubjectID, CollectionGrantScope{"collection", op.CollectionID}, []string{"collections.admin", "collections.edit", "collections.read"}}
		p := CollectionPolicyRequest{2, s.library, record.GeneratedPolicyOperationID, "collection", op.CollectionID, op.ID, digest, []CollectionGrantChange{{"put", v.CreatorGrantID, nil, &grant}}}
		policy = &p
		delta.Collections = 1
	case *ReplaceSavedQueryDefinition:
		if bytes.Equal(current.canonical, definition) {
			return receipt, ErrConflict
		}
		if err := changedOne(tx.ExecContext(ctx, `UPDATE saved_query_definitions SET definition=? WHERE collection_id=? AND definition_version=?`, definition, op.CollectionID, op.ExpectedVersion)); err != nil {
			return receipt, err
		}
	case *RenameCollection:
		if current.name == v.Name {
			return receipt, ErrConflict
		}
		if err := changedOne(tx.ExecContext(ctx, `UPDATE saved_query_definitions SET name=? WHERE collection_id=? AND definition_version=?`, v.Name, op.CollectionID, op.ExpectedVersion)); err != nil {
			return receipt, err
		}
	case *SetCollectionArchived:
		if current.archived == v.Archived {
			return receipt, ErrConflict
		}
		if err := changedOne(tx.ExecContext(ctx, `UPDATE saved_query_definitions SET archived=? WHERE collection_id=? AND definition_version=?`, v.Archived, op.CollectionID, op.ExpectedVersion)); err != nil {
			return receipt, err
		}
	case *EditCollectionGrants:
		p := CollectionPolicyRequest{2, s.library, record.GeneratedPolicyOperationID, "collection", op.CollectionID, op.ID, digest, v.Changes}
		policy = &p
	}
	var child any
	if policy != nil {
		sequence, change, err := policyApply(*policy)
		if err != nil {
			return receipt, err
		}
		child = sequence
		delta, err = sumCollectionDelta(delta, change)
		if err != nil {
			return receipt, err
		}
	}
	if op.Kind != "create" {
		if err := changedOne(tx.ExecContext(ctx, `UPDATE saved_query_definitions SET definition_version=?,updated_sequence=? WHERE collection_id=? AND definition_version=?`, record.ResultVersion, sequence, op.CollectionID, op.ExpectedVersion)); err != nil {
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
	if _, err := tx.ExecContext(ctx, `INSERT INTO collection_events(sequence,library_id,actor_subject_id,operation_id,collection_id,request,payload_sha256,receipt,result_version,generated_policy_sequence,recorded_unix_nano) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, sequence, s.library, record.ActorSubjectID, op.ID, op.CollectionID, data, digest, receiptBytes, record.ResultVersion, child, record.RecordedUnixNano); err != nil {
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

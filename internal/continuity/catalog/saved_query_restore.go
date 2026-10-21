package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// These bounded readers validate retained history, without live authorization.
func validateRecordedSavedQueryContent(record SavedQueryRecord, data, receiptBytes []byte, library continuity.LibraryID) (SavedQueryReceipt, error) {
	if _, _, err := CanonicalSavedQueryRecord(record); err != nil {
		return SavedQueryReceipt{}, err
	}
	expected, _, err := CanonicalSavedQueryRequest(record.Request)
	if err != nil || !bytes.Equal(expected, data) || record.Request.LibraryID != library {
		return SavedQueryReceipt{}, corrupt("saved query request binding")
	}
	receipt, err := DecodeSavedQueryReceipt(receiptBytes)
	if err != nil || receipt.OperationID != record.Request.ID || receipt.CollectionID != record.Request.CollectionID || receipt.ResultVersion != record.ResultVersion {
		return SavedQueryReceipt{}, corrupt("saved query receipt binding")
	}
	return receipt, nil
}

func bindSavedQueryChild(r SavedQueryRecord, p *CollectionPolicyRecord) error {
	generated := r.Request.Kind == "create" || r.Request.Kind == "grants"
	if !generated {
		if p != nil {
			return corrupt("unexpected generated policy")
		}
		return nil
	}
	if p == nil {
		return corrupt("missing generated policy")
	}
	_, digest, _ := CanonicalSavedQueryRequest(r.Request)
	if p.Request.LibraryID != r.Request.LibraryID || p.Request.CollectionID != r.Request.CollectionID || p.Administration != "collection_service" || p.OperatorSubjectID != r.ActorSubjectID || p.Request.ID != r.GeneratedPolicyOperationID || p.Request.BindingOperationID != r.Request.ID || p.Request.BindingPayloadSHA256 != digest || p.RecordedUnixNano != r.RecordedUnixNano {
		return corrupt("generated policy binding")
	}
	v, err := savedQueryValue(r.Request)
	if err != nil {
		return err
	}
	var changes []CollectionGrantChange
	switch x := v.(type) {
	case *CreateSavedQuery:
		grant := CollectionGrantValue{r.ActorSubjectID, CollectionGrantScope{"collection", r.Request.CollectionID}, []string{"collections.admin", "collections.edit", "collections.read"}}
		changes = []CollectionGrantChange{{"put", x.CreatorGrantID, nil, &grant}}
	case *EditCollectionGrants:
		changes = x.Changes
	default:
		return corrupt("generated policy kind")
	}
	a, err := json.Marshal(changes)
	if err != nil {
		return err
	}
	b, err := json.Marshal(p.Request.Changes)
	if err != nil {
		return err
	}
	if string(a) != string(b) {
		return corrupt("generated policy changes")
	}
	return nil
}

func readSavedQueryContentRecord(ctx context.Context, q queryer, library continuity.LibraryID, sequence int64) (SavedQueryRecord, []byte, int64, error) {
	var r SavedQueryRecord
	var lib continuity.LibraryID
	var id CollectionOperationID
	var collection CollectionID
	var request, receipt []byte
	var digest string
	var child int64
	err := q.QueryRowContext(ctx, `SELECT library_id,actor_subject_id,operation_id,collection_id,CASE WHEN length(request) BETWEEN 1 AND 32768 THEN request END,payload_sha256,CASE WHEN length(receipt) BETWEEN 1 AND 1024 THEN receipt END,result_version,COALESCE(generated_policy_sequence,0),recorded_unix_nano FROM collection_events WHERE sequence=?`, sequence).Scan(&lib, &r.ActorSubjectID, &id, &collection, &request, &digest, &receipt, &r.ResultVersion, &child, &r.RecordedUnixNano)
	if err != nil {
		return r, nil, 0, err
	}
	r.Request, err = DecodeSavedQueryRequest(request)
	if err != nil {
		return r, nil, 0, err
	}
	r.Version = 2
	if child != 0 {
		r.GeneratedPolicyOperationID = generatedCollectionPolicyID(r.Request.ID)
	}
	_, expected, _ := CanonicalSavedQueryRequest(r.Request)
	if lib != library || r.Request.LibraryID != library || r.Request.ID != id || r.Request.CollectionID != collection || digest != expected || child < 0 {
		return r, nil, 0, corrupt("stored collection content binding")
	}
	if _, _, err := CanonicalSavedQueryRecord(r); err != nil {
		return r, nil, 0, err
	}
	rc, err := DecodeSavedQueryReceipt(receipt)
	if err != nil || rc.OperationID != id || rc.CollectionID != collection || rc.ResultVersion != r.ResultVersion {
		return r, nil, 0, corrupt("stored collection content receipt")
	}
	return r, receipt, child, nil
}

func (s *Store) applyRecordedSavedQueryContent(ctx context.Context, tx *sql.Tx, record SavedQueryRecord, receiptBytes []byte, child *CollectionPolicyRecord, childReceipt []byte, budget *collectionProjectionBudget) (SavedQueryReceipt, error) {
	data, _, err := CanonicalSavedQueryRequest(record.Request)
	if err != nil {
		return SavedQueryReceipt{}, err
	}
	if _, err := validateRecordedSavedQueryContent(record, data, receiptBytes, s.library); err != nil {
		return SavedQueryReceipt{}, err
	}
	if err := bindSavedQueryChild(record, child); err != nil {
		return SavedQueryReceipt{}, err
	}
	if child == nil && len(childReceipt) != 0 {
		return SavedQueryReceipt{}, corrupt("receipt without generated policy")
	}
	if child != nil {
		if _, _, err := CanonicalCollectionPolicyRecord(*child); err != nil {
			return SavedQueryReceipt{}, err
		}
		if _, err := validateRecordedCollectionPolicyReceipt(child.Request, childReceipt, child.RecordedUnixNano); err != nil {
			return SavedQueryReceipt{}, err
		}
	}
	// Metadata was replayed first. Existence and immutable member bindings are
	// historical structural facts; current discovery grants are not replay proof.
	assetCheck := func(asset continuity.AssetID) error { _, err := readAsset(ctx, tx, s.library, asset); return err }
	policyApply := func(expected CollectionPolicyRequest) (int64, CollectionQuotaCounters, error) {
		if child == nil {
			return 0, CollectionQuotaCounters{}, corrupt("missing recorded child")
		}
		want, _, err := CanonicalCollectionPolicyRequest(expected)
		if err != nil {
			return 0, CollectionQuotaCounters{}, err
		}
		have, _, err := CanonicalCollectionPolicyRequest(child.Request)
		if err != nil || !bytes.Equal(want, have) {
			return 0, CollectionQuotaCounters{}, corrupt("recorded child request")
		}
		return s.applyRecordedCollectionPolicy(ctx, tx, *child, childReceipt, budget)
	}
	return s.applySavedQueryContentStructuralMode(ctx, tx, record, data, receiptBytes, true, budget, assetCheck, policyApply)
}

func (s *Store) replaySavedQueryContent(ctx context.Context, record SavedQueryRecord, receiptBytes []byte, child *CollectionPolicyRecord, childReceipt []byte, rows, maxRows int64) (int64, error) {
	if !supportsSavedQueries(s.version) {
		return 0, ErrUpgradeRequired
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
	if _, err := s.applyRecordedSavedQueryContent(ctx, tx, record, receiptBytes, child, childReceipt, &budget); err != nil {
		return 0, err
	}
	if err := s.commitCollectionReplay(ctx, tx); err != nil {
		return 0, err
	}
	return budget.rows, nil
}

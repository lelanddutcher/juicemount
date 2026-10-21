package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// A budget is transaction-local. Replay copies the committed row count into it
// and publishes the new count only after COMMIT. Failed transactions discard it.
// A nil budget belongs only to already-bounded live writes, not stream replay.
type collectionProjectionBudget struct{ rows, max int64 }

func newCollectionProjectionBudget(rows, max int64) (collectionProjectionBudget, error) {
	if rows < 2 || max < rows || max > MaxStreamProjectionRows {
		return collectionProjectionBudget{}, ErrLimit
	}
	return collectionProjectionBudget{rows, max}, nil
}
func (b *collectionProjectionBudget) take() error {
	if b == nil {
		return nil
	}
	if b.rows < 0 || b.rows >= b.max {
		return ErrLimit
	}
	b.rows++
	return nil
}
func requireFreshCollectionVersion(ctx context.Context, q queryer, id CollectionID, version CollectionVersion) error {
	if id.Validate() != nil || version.Validate() != nil {
		return invalid("collection historical version")
	}
	var exists bool
	if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM collection_events WHERE collection_id=? AND result_version=?)`, id, version).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return ErrConflict
	}
	return nil
}
func requireFreshCollectionGrantVersion(ctx context.Context, q queryer, id CollectionGrantID, version CollectionGrantVersion) error {
	if id.Validate() != nil || version.Validate() != nil {
		return invalid("collection historical grant version")
	}
	var exists bool
	if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM collection_policy_results WHERE grant_id=? AND result_version=?)`, id, version).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return ErrConflict
	}
	return nil
}
func validateRecordedCollectionContent(record CollectionRecord, data, receiptBytes []byte, library continuity.LibraryID) (CollectionReceipt, error) {
	if _, _, err := CanonicalCollectionRecord(record); err != nil {
		return CollectionReceipt{}, err
	}
	canonical, _, err := CanonicalCollectionRequest(record.Request)
	if err != nil {
		return CollectionReceipt{}, err
	}
	if record.Request.LibraryID != library || !bytes.Equal(data, canonical) {
		return CollectionReceipt{}, corrupt("recorded content request")
	}
	receipt, err := DecodeCollectionReceipt(receiptBytes)
	if err != nil {
		return receipt, err
	}
	if receipt.OperationID != record.Request.ID || receipt.CollectionID != record.Request.CollectionID || receipt.ResultVersion != record.ResultVersion {
		return receipt, corrupt("recorded content receipt")
	}
	return receipt, nil
}
func validateRecordedCollectionPolicyReceipt(op CollectionPolicyRequest, receiptBytes []byte, timestamp int64) (CollectionPolicyReceipt, error) {
	_, digest, err := CanonicalCollectionPolicyRequest(op)
	if err != nil {
		return CollectionPolicyReceipt{}, err
	}
	receipt, err := DecodeCollectionPolicyReceipt(receiptBytes)
	if err != nil {
		return receipt, err
	}
	if receipt.LibraryID != op.LibraryID || receipt.OperationID != op.ID || receipt.PayloadSHA256 != digest || receipt.RecordedUnixNano != timestamp {
		return receipt, corrupt("recorded policy receipt")
	}
	return receipt, nil
}
func consumeCollectionDeclaration(ctx context.Context, q queryer, library continuity.LibraryID, record CollectionRecord, digest string) error {
	if record.Request.Kind != "create" {
		return corrupt("declaration requires create")
	}
	return consumeTypedCollectionDeclaration(ctx, q, library, record.Request.CollectionID, record.ActorSubjectID, record.Request.ID, digest)
}
func consumeTypedCollectionDeclaration(ctx context.Context, q queryer, library continuity.LibraryID, id CollectionID, actor SubjectID, operationID CollectionOperationID, digest string) error {
	version, err := catalogVersion(ctx, q)
	if err != nil {
		return err
	}
	if supportsSavedQueries(version) {
		var exists bool
		if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM saved_query_definitions WHERE collection_id=?)`, id).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return corrupt("consumed query declaration")
		}
	}
	var lib continuity.LibraryID
	var creator SubjectID
	var operation CollectionOperationID
	var storedDigest string
	var generation int64
	var definition, quota, event bool
	err = q.QueryRowContext(ctx, `SELECT library_id,creator_subject_id,creation_operation_id,creation_payload_sha256,policy_generation,EXISTS(SELECT 1 FROM collection_definitions d WHERE d.collection_id=i.collection_id),EXISTS(SELECT 1 FROM collection_quota q WHERE q.collection_id=i.collection_id),EXISTS(SELECT 1 FROM collection_events e WHERE e.collection_id=i.collection_id) FROM collection_identities i WHERE collection_id=?`, id).Scan(&lib, &creator, &operation, &storedDigest, &generation, &definition, &quota, &event)
	if err != nil {
		return err
	}
	if lib != library || creator != actor || operation != operationID || storedDigest != digest || generation != 0 || definition || quota || event {
		return corrupt("unconsumed collection declaration binding")
	}
	// Consumption is the definition insertion in this same transaction. Identity
	// rows remain immutable attribution; no separate consumption flag is portable.
	return nil
}
func (s *Store) applyRecordedCollectionPolicy(ctx context.Context, tx *sql.Tx, record CollectionPolicyRecord, receiptBytes []byte, budget *collectionProjectionBudget) (int64, CollectionQuotaCounters, error) {
	if _, _, err := CanonicalCollectionPolicyRecord(record); err != nil {
		return 0, CollectionQuotaCounters{}, err
	}
	if record.Request.LibraryID != s.library {
		return 0, CollectionQuotaCounters{}, corrupt("recorded policy library")
	}
	if _, err := validateRecordedCollectionPolicyReceipt(record.Request, receiptBytes, record.RecordedUnixNano); err != nil {
		return 0, CollectionQuotaCounters{}, err
	}
	versions := func(index int, id CollectionGrantID) (CollectionGrantVersion, error) {
		if index < 0 || index >= len(record.Results) || record.Results[index].ID != id {
			return "", corrupt("recorded grant results")
		}
		return record.Results[index].Version, nil
	}
	return s.applyCollectionPolicyStructural(ctx, tx, record.Request, record.OperatorSubjectID, record.Administration, record.RecordedUnixNano, receiptBytes, versions, budget)
}
func (s *Store) applyRecordedCollectionContent(ctx context.Context, tx *sql.Tx, record CollectionRecord, receiptBytes []byte, child *CollectionPolicyRecord, childReceipt []byte, budget *collectionProjectionBudget) (CollectionReceipt, error) {
	data, _, err := CanonicalCollectionRequest(record.Request)
	if err != nil {
		return CollectionReceipt{}, err
	}
	if _, err := validateRecordedCollectionContent(record, data, receiptBytes, s.library); err != nil {
		return CollectionReceipt{}, err
	}
	if err := bindCollectionChild(record, child); err != nil {
		return CollectionReceipt{}, err
	}
	if child == nil && len(childReceipt) != 0 {
		return CollectionReceipt{}, corrupt("receipt without generated policy")
	}
	if child != nil {
		if _, _, err := CanonicalCollectionPolicyRecord(*child); err != nil {
			return CollectionReceipt{}, err
		}
		if _, err := validateRecordedCollectionPolicyReceipt(child.Request, childReceipt, child.RecordedUnixNano); err != nil {
			return CollectionReceipt{}, err
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
	return s.applyCollectionContentStructural(ctx, tx, record, data, receiptBytes, true, budget, assetCheck, policyApply)
}
func (s *Store) prepareCollectionReplay(ctx context.Context, tx *sql.Tx) error {
	if !supportsCollections(s.version) {
		return ErrUpgradeRequired
	}
	if err := s.checkVersion(ctx, tx); err != nil {
		return err
	}
	if err := requireIncomplete(ctx, tx); err != nil {
		return err
	}
	var library continuity.LibraryID
	if err := tx.QueryRowContext(ctx, `SELECT library_id FROM catalog WHERE singleton=1`).Scan(&library); err != nil {
		return err
	}
	if library != s.library {
		return corrupt("collection replay library")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.checkPaths()
}
func (s *Store) commitCollectionReplay(ctx context.Context, tx *sql.Tx) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.checkPaths(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("incomplete collection replay commit uncertain: %w", err)
	}
	return nil
}

// Declaration staging creates only the reserved identity: no grants, definition
// or quota, and no ready state. Its creator may be introduced at a later ordinal.
func (s *Store) replayCollectionDeclaration(ctx context.Context, d collectionDeclaration, rows, maxRows int64) (int64, error) {
	if err := d.validate(s.library); err != nil {
		return 0, err
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
	var count int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM collection_identities`).Scan(&count); err != nil {
		return 0, err
	}
	if count >= 1024 {
		return 0, ErrLimit
	}
	if err := budget.take(); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO collection_identities(collection_id,library_id,creator_subject_id,creation_operation_id,creation_payload_sha256,policy_generation) VALUES(?,?,?,?,?,0)`, d.CollectionID, d.LibraryID, d.CreatorSubjectID, d.CreationOperationID, d.CreationPayloadSHA256); err != nil {
		return 0, err
	}
	if err := s.commitCollectionReplay(ctx, tx); err != nil {
		return 0, err
	}
	return budget.rows, nil
}
func (s *Store) replayCollectionOwnerPolicy(ctx context.Context, record CollectionPolicyRecord, receiptBytes []byte, rows, maxRows int64) (int64, error) {
	if record.Administration != "local_owner" {
		return 0, corrupt("independent generated policy")
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
	// Both collection formats admit owner attribution at this historical prefix,
	// without operational authority or the subject's later final state.
	operator, err := readSubject(ctx, tx, s.library, record.OperatorSubjectID)
	if err != nil {
		return 0, err
	}
	if operator.Value.Kind != "person" || !operator.Value.Enabled {
		return 0, ErrConflict
	}
	sequence, delta, err := s.applyRecordedCollectionPolicy(ctx, tx, record, receiptBytes, &budget)
	if err != nil {
		return 0, err
	}
	usage, err := readCollectionQuota(ctx, tx, record.Request.CollectionID)
	if err != nil {
		return 0, err
	}
	change := CollectionQuotaDelta{Library: delta}
	if record.Request.CollectionID != "" {
		local := delta
		change.Collection = &local
	}
	after, err := planCollectionQuota(usage, change)
	if err != nil {
		return 0, err
	}
	if err := saveCollectionQuota(ctx, tx, record.Request.CollectionID, after); err != nil {
		return 0, err
	}
	if err := budget.take(); err != nil {
		return 0, err
	}
	if err := appendCollectionOrder(ctx, tx, "owner_policy_sequence", sequence); err != nil {
		return 0, err
	}
	if err := s.commitCollectionReplay(ctx, tx); err != nil {
		return 0, err
	}
	return budget.rows, nil
}
func (s *Store) replayCollectionContent(ctx context.Context, record CollectionRecord, receiptBytes []byte, child *CollectionPolicyRecord, childReceipt []byte, rows, maxRows int64) (int64, error) {
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
	if _, err := s.applyRecordedCollectionContent(ctx, tx, record, receiptBytes, child, childReceipt, &budget); err != nil {
		return 0, err
	}
	if err := s.commitCollectionReplay(ctx, tx); err != nil {
		return 0, err
	}
	return budget.rows, nil
}

func validateInactiveCollectionImport(ctx context.Context, q queryer) error {
	var operational, preferences int64
	if err := q.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM catalog_authority)+(SELECT count(*) FROM catalog_admin_events)+(SELECT count(*) FROM catalog_credentials)+(SELECT count(*) FROM credential_issuance_receipts)+(SELECT count(*) FROM credential_codecs)+(SELECT count(*) FROM catalog_admin_codecs)`).Scan(&operational); err != nil {
		return err
	}
	if operational != 0 {
		return corrupt("active operational state in portable import")
	}
	if err := q.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM preference_subjects)+(SELECT count(*) FROM preferences)+(SELECT count(*) FROM preference_receipts)+(SELECT clock_floor_unix+subject_count+entry_count+receipt_count FROM preference_control WHERE singleton=1)`).Scan(&preferences); err != nil {
		return err
	}
	if preferences != 0 {
		return corrupt("nonportable preferences in import")
	}
	return nil
}

// The magic has already been read into input's byte budget. Only a validated
// complete format3 header can reserve a new schema5 target. Every replay call
// commits one bounded incomplete transaction; failed targets are never adopted.
func restoreCollections(ctx context.Context, path string, library continuity.LibraryID, input *streamInput, limits Limits, directoryBarrier func(string) error) (resultErr error) {
	return restoreCollectionFormat(ctx, path, library, input, limits, directoryBarrier, 3)
}
func restoreCollectionFormat(ctx context.Context, path string, library continuity.LibraryID, input *streamInput, limits Limits, directoryBarrier func(string) error, format int) (resultErr error) {
	if format != 3 && format != 4 {
		return corrupt("collection restore format")
	}
	h, magic := collectionFormatHash(format)
	h.Write([]byte(magic))
	_, data, err := input.collectionFrame(false, 1, h)
	if err != nil {
		return err
	}
	header, err := decodeCollectionFormatHeader(data, library, limits, format)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s, err := openStoreVersionContext(ctx, path, library, true, false, header.SchemaVersion)
	if err != nil {
		return err
	}
	defer func() {
		if s != nil {
			resultErr = errors.Join(resultErr, s.Close())
		}
	}()
	rows := int64(2)
	for sequence := int64(1); sequence <= header.EventCount; sequence++ {
		_, payload, err := input.collectionFrame(false, 2, h)
		if err != nil {
			return protocolError("restore metadata frame", err)
		}
		event, op, err := decodeStreamEvent(payload, library, sequence)
		if err != nil {
			return err
		}
		next, err := s.replayEvent(ctx, event, op, rows, limits.MaxProjectionRows)
		if err != nil {
			return protocolError("restore metadata replay", err)
		}
		rows = next
	}
	var last CollectionID
	for n := int64(0); n < header.CollectionCount; n++ {
		_, payload, err := input.collectionFrame(false, 5, h)
		if err != nil {
			return protocolError("restore declaration frame", err)
		}
		var d collectionDeclaration
		if err := canonicalJSON(payload, &d); err != nil {
			return err
		}
		if err := d.validate(library); err != nil {
			return err
		}
		if d.CollectionID <= last {
			return corrupt("restore declaration order")
		}
		next, err := s.replayCollectionDeclaration(ctx, d, rows, limits.MaxProjectionRows)
		if err != nil {
			return protocolError("restore declaration replay", err)
		}
		rows = next
		last = d.CollectionID
	}
	var legacy, content, policy int64
	for ordinal := int64(1); ordinal <= header.TransactionCount; ordinal++ {
		kind, payload, err := input.collectionFrame(true, 0, h)
		if err != nil {
			return protocolError("restore transaction frame", err)
		}
		switch kind {
		case 4:
			legacy++
			if legacy > header.PolicyEventCount {
				return corrupt("excess legacy policy")
			}
			event, op, err := decodeStreamPolicyEvent(payload, library, legacy)
			if err != nil {
				return err
			}
			next, err := s.replayPolicyEvent(ctx, event, op, rows, limits.MaxProjectionRows)
			if err != nil {
				return protocolError("restore legacy policy replay", err)
			}
			rows = next
		case 6:
			policy++
			if policy > header.CollectionPolicyEventCount {
				return corrupt("excess owner policy")
			}
			record, receipt, err := decodeCollectionPolicyPair(payload, library)
			if err != nil {
				return err
			}
			next, err := s.replayCollectionOwnerPolicy(ctx, record, receipt, rows, limits.MaxProjectionRows)
			if err != nil {
				return protocolError("restore owner policy replay", err)
			}
			rows = next
		case 7:
			content++
			if content > header.CollectionEventCount {
				return corrupt("excess collection content")
			}
			replay, hasChild, err := decodePortableCollectionContent(payload, library, format)
			if err != nil {
				return err
			}
			if hasChild {
				policy++
				if policy > header.CollectionPolicyEventCount {
					return corrupt("excess generated policy")
				}
			}
			next, err := replay(ctx, s, rows, limits.MaxProjectionRows)
			if err != nil {
				return protocolError("restore content replay", err)
			}
			rows = next
		default:
			return corrupt("restore transaction kind")
		}
	}
	if legacy != header.PolicyEventCount || content != header.CollectionEventCount || policy != header.CollectionPolicyEventCount {
		return corrupt("restore encountered family counts")
	}
	_, data, err = input.collectionFrame(false, 3, nil)
	if err != nil {
		return err
	}
	footer, err := decodeCollectionFooter(data, header.collectionStreamCounts, limits)
	if err != nil {
		return err
	}
	digest, err := finishCollectionStreamHash(h, header.collectionStreamCounts, footer.ProjectionRows, footer.ProjectionSHA256)
	if err != nil {
		return err
	}
	if digest != footer.StreamSHA256 {
		return corrupt("collection stream digest mismatch")
	}
	if err := input.end(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.prepareCollectionReplay(ctx, tx); err != nil {
		return err
	}
	actual, err := validateSchemaState(ctx, tx, true)
	if err != nil {
		return err
	}
	if actual != library {
		return corrupt("restored collection library")
	}
	if err := checkSettings(ctx, tx); err != nil {
		return err
	}
	if err := checkIntegrity(ctx, tx); err != nil {
		return err
	}
	counts, err := collectionPortableTotals(ctx, tx)
	if err != nil {
		return err
	}
	if counts != header.collectionStreamCounts {
		return corrupt("restored family/order/declaration counts")
	}
	// initializing=true intentionally skips complete collection checks. Run them
	// explicitly while ready0, including reverse creates and all quota identities.
	if err := validateCollectionPortableState(ctx, tx, library); err != nil {
		return err
	}
	if err := walkEvents(ctx, tx, library, header.EventCount, nil); err != nil {
		return err
	}
	if err := walkPolicyEvents(ctx, tx, library, header.PolicyEventCount, nil); err != nil {
		return err
	}
	if err := walkCollectionDeclarations(ctx, tx, library, header.CollectionCount, nil); err != nil {
		return err
	}
	if err := walkCollectionTransactions(ctx, tx, library, counts, nil); err != nil {
		return err
	}
	if err := validateInactiveCollectionImport(ctx, tx); err != nil {
		return err
	}
	total, err := projectionTotal(ctx, tx)
	if err != nil {
		return err
	}
	projection, observed, err := projectionDigest(ctx, tx, library, limits.MaxProjectionRows)
	if err != nil {
		return err
	}
	if projection != footer.ProjectionSHA256 || observed != footer.ProjectionRows || observed != rows || total != observed {
		return corrupt("restored projection does not match source snapshot")
	}
	if err := s.checkPaths(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := changedOne(tx.ExecContext(ctx, `UPDATE catalog SET ready=1 WHERE singleton=1 AND ready=0`)); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog restore commit outcome uncertain: %w", err)
	}
	canonicalTarget := s.path
	closeErr := s.Close()
	s = nil
	if closeErr != nil {
		return fmt.Errorf("catalog restore close outcome uncertain: %w", closeErr)
	}
	if err := directoryBarrier(canonicalTarget); err != nil {
		return fmt.Errorf("catalog restore directory barrier uncertain: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("catalog restore acknowledgment uncertain: %w", err)
	}
	return nil
}

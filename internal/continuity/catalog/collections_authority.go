package catalog

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

type collectionAdminRow struct {
	request, receipt    []byte
	digest              string
	sequence, timestamp int64
}

func readCollectionAdmin(ctx context.Context, q queryer, id string) (collectionAdminRow, bool, error) {
	var r collectionAdminRow
	err := q.QueryRowContext(ctx, `SELECT CASE WHEN length(request)<=1048576 THEN request END,CASE WHEN length(receipt)<=2048 THEN receipt END,payload_sha256,sequence,recorded_unix_nano FROM catalog_admin_events WHERE operation_id=?`, id).Scan(&r.request, &r.receipt, &r.digest, &r.sequence, &r.timestamp)
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}
	return r, err == nil, err
}
func insertCollectionAdmin(ctx context.Context, tx *sql.Tx, library any, id string, data []byte, digest string, timestamp int64, receipt []byte, sequence int64, codec string) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO catalog_admin_events(sequence,library_id,operation_id,request,payload_sha256,recorded_unix_nano,receipt) VALUES(?,?,?,?,?,?,?)`, sequence, library, id, data, digest, timestamp, receipt); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO catalog_admin_codecs(operation_id,codec) VALUES(?,?)`, id, codec)
	return err
}
func (o *LocalOwnerService) lookupIssuanceV2(ctx context.Context, q queryer, op AdminRequestV2, data []byte) (IssuanceResultV2, bool, error) {
	row, found, err := readCollectionAdmin(ctx, q, op.ID)
	if err != nil || !found {
		return IssuanceResultV2{}, found, err
	}
	if err := requireAdminCodec(ctx, q, op.ID, "issue2"); err != nil {
		return IssuanceResultV2{}, false, err
	}
	if !bytes.Equal(row.request, data) {
		return IssuanceResultV2{}, false, ErrChangedRequest
	}
	receipt, err := decodeAdminReceiptV2(row.receipt, op, data, row.digest, row.sequence, row.timestamp)
	if err != nil {
		return IssuanceResultV2{}, false, err
	}
	var selector string
	if err := q.QueryRowContext(ctx, `SELECT selector FROM credential_issuance_receipts WHERE operation_id=?`, op.ID).Scan(&selector); err != nil || selector != receipt.Result.Selector {
		return IssuanceResultV2{}, false, corrupt("issuance selector link")
	}
	stored, err := readCredentialVersion(ctx, q, o.library, selector, o.store.version)
	var value IssueCredentialRequestV2
	if decodeCanonicalValue(op.Value, &value) != nil {
		return IssuanceResultV2{}, false, corrupt("issuance request")
	}
	if err != nil || stored.codec != 2 || stored.subject != receipt.Result.SubjectID || stored.incarnation != receipt.Result.Incarnation || stored.scope != value.Scope || !equalStringActions(stored.actions, value.Actions) || stored.expires != value.ExpiresUnix {
		return IssuanceResultV2{}, false, corrupt("issuance credential link")
	}
	status, err := credentialStatusVersion(ctx, q, o.library, receipt.Result.Selector, o.store.now().Unix(), o.store.version)
	return IssuanceResultV2{Receipt: receipt, Status: status}, true, err
}
func (o *LocalOwnerService) LookupIssuanceV2(ctx context.Context, op AdminRequestV2) (result IssuanceResultV2, found bool, resultErr error) {
	if err := o.enter(); err != nil {
		return result, false, err
	}
	defer o.store.leave()
	if !supportsCollections(o.store.version) {
		return result, false, ErrUpgradeRequired
	}
	data, _, err := CanonicalAdminRequestV2(op)
	if err != nil {
		return result, false, err
	}
	if op.LibraryID != o.library {
		return result, false, invalid("issuance library")
	}
	tx, done, err := o.store.readSnapshot(ctx)
	if err != nil {
		return result, false, err
	}
	defer func() { resultErr = errors.Join(resultErr, done()) }()
	authority, err := readAuthority(ctx, tx, o.library)
	if err != nil {
		return result, false, err
	}
	if authority.Incarnation == "" {
		return result, false, ErrUnauthenticated
	}
	return o.lookupIssuanceV2(ctx, tx, op, data)
}
func (o *LocalOwnerService) IssuanceStatusV2(ctx context.Context, id string) (result IssuanceResultV2, resultErr error) {
	if err := o.enter(); err != nil {
		return result, err
	}
	defer o.store.leave()
	if !supportsCollections(o.store.version) {
		return result, ErrUpgradeRequired
	}
	if !validOperationID(id) {
		return result, invalid("operation identity")
	}
	tx, done, err := o.store.readSnapshot(ctx)
	if err != nil {
		return result, err
	}
	defer func() { resultErr = errors.Join(resultErr, done()) }()
	authority, err := readAuthority(ctx, tx, o.library)
	if err != nil {
		return result, err
	}
	if authority.Incarnation == "" {
		return result, ErrUnauthenticated
	}
	row, found, err := readCollectionAdmin(ctx, tx, id)
	if err != nil {
		return result, err
	}
	if !found {
		return result, ErrNotFound
	}
	if err := requireAdminCodec(ctx, tx, id, "issue2"); err != nil {
		return result, err
	}
	op, err := DecodeAdminRequestV2(row.request)
	if err != nil {
		return result, corrupt("issuance request")
	}
	result, _, err = o.lookupIssuanceV2(ctx, tx, op, row.request)
	return result, err
}
func (o *LocalOwnerService) IssueCredentialV2(ctx context.Context, input AdminRequestV2) (IssuanceResultV2, error) {
	if err := o.enter(); err != nil {
		return IssuanceResultV2{}, err
	}
	defer o.store.leave()
	s := o.store
	if !supportsCollections(s.version) {
		return IssuanceResultV2{}, ErrUpgradeRequired
	}
	data, digest, err := CanonicalAdminRequestV2(input)
	if err != nil {
		return IssuanceResultV2{}, err
	}
	op, err := DecodeAdminRequestV2(data)
	if err != nil {
		return IssuanceResultV2{}, err
	}
	if op.LibraryID != o.library {
		return IssuanceResultV2{}, invalid("issuance library")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return IssuanceResultV2{}, err
	}
	defer tx.Rollback()
	if err := s.checkVersion(ctx, tx); err != nil {
		return IssuanceResultV2{}, err
	}
	a, err := readAuthority(ctx, tx, s.library)
	if err != nil {
		return IssuanceResultV2{}, err
	}
	if a.Incarnation == "" {
		return IssuanceResultV2{}, ErrUnauthenticated
	}
	prior, found, err := o.lookupIssuanceV2(ctx, tx, op, data)
	if err != nil || found {
		return prior, err
	}
	if a.Incarnation != op.ExpectedIncarnation {
		return IssuanceResultV2{}, ErrConflict
	}
	var value IssueCredentialRequestV2
	if decodeCanonicalValue(op.Value, &value) != nil {
		return IssuanceResultV2{}, invalid("issuance value")
	}
	subject, err := readSubject(ctx, tx, s.library, value.SubjectID)
	if err != nil {
		return IssuanceResultV2{}, err
	}
	if !subject.Value.Enabled || subject.Value.Kind == "service" && containsAction(value.Actions, "activity.record") {
		return IssuanceResultV2{}, ErrConflict
	}
	now := s.now().Unix()
	if now > math.MaxInt64-86400 || value.ExpiresUnix < now+60 || value.ExpiresUnix > now+86400 {
		return IssuanceResultV2{}, invalid("credential expiry")
	}
	selector := ""
	for i := 0; i < 8; i++ {
		candidate, err := randomIdentity("cred_")
		if err != nil {
			return IssuanceResultV2{}, err
		}
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM catalog_credentials WHERE selector=?)`, candidate).Scan(&exists); err != nil {
			return IssuanceResultV2{}, err
		}
		if !exists {
			selector = candidate
			break
		}
	}
	if selector == "" {
		return IssuanceResultV2{}, errors.New("catalog selector generation failed")
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return IssuanceResultV2{}, errors.New("catalog randomness unavailable")
	}
	verifier := credentialVerifier(s.library, a.Incarnation, selector, secret[:])
	if _, err := tx.ExecContext(ctx, `INSERT INTO catalog_credentials(selector,library_id,incarnation,subject_id,scope_kind,asset_id,actions,verifier,issued_unix,expires_unix,generation,revoked) VALUES(?,?,?,?,'library',NULL,?,?,?, ?,1,0)`, selector, s.library, a.Incarnation, value.SubjectID, actionsBytes(value.Actions), verifier[:], now, value.ExpiresUnix); err != nil {
		return IssuanceResultV2{}, errors.New("catalog credential insertion failed")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO credential_codecs(selector,codec) VALUES(?,2)`, selector); err != nil {
		return IssuanceResultV2{}, err
	}
	sequence, err := nextSequence(ctx, tx, "catalog_admin_events")
	if err != nil {
		return IssuanceResultV2{}, err
	}
	receipt := AdminReceiptV2(AdminReceipt{2, s.library, op.ID, sequence, digest, s.now().UnixNano(), AdminResult{Incarnation: a.Incarnation, OwnerSubjectID: a.OwnerSubjectID, SubjectID: value.SubjectID, Selector: selector}})
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return IssuanceResultV2{}, err
	}
	if err := insertCollectionAdmin(ctx, tx, s.library, op.ID, data, digest, receipt.RecordedUnixNano, encoded, sequence, "issue2"); err != nil {
		return IssuanceResultV2{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO credential_issuance_receipts(operation_id,selector) VALUES(?,?)`, op.ID, selector); err != nil {
		return IssuanceResultV2{}, err
	}
	if err := ctx.Err(); err != nil {
		return IssuanceResultV2{}, err
	}
	if err := s.checkPaths(); err != nil {
		return IssuanceResultV2{}, err
	}
	if err := tx.Commit(); err != nil {
		return IssuanceResultV2{}, fmt.Errorf("catalog issuance commit uncertain (operation %s selector %s): %w", op.ID, selector, err)
	}
	return IssuanceResultV2{receipt, "active", []byte("jmc1_" + selector[5:] + "." + hex.EncodeToString(secret[:]))}, nil
}
func (o *LocalOwnerService) ApplyCollectionPolicy(ctx context.Context, input CollectionOwnerRequest) (CollectionOwnerReceipt, error) {
	if err := o.enter(); err != nil {
		return CollectionOwnerReceipt{}, err
	}
	defer o.store.leave()
	s := o.store
	if !supportsCollections(s.version) {
		return CollectionOwnerReceipt{}, ErrUpgradeRequired
	}
	data, digest, err := CanonicalCollectionOwnerRequest(input)
	if err != nil {
		return CollectionOwnerReceipt{}, err
	}
	op, err := DecodeCollectionOwnerRequest(data)
	if err != nil {
		return CollectionOwnerReceipt{}, err
	}
	if op.LibraryID != o.library {
		return CollectionOwnerReceipt{}, invalid("collection owner library")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CollectionOwnerReceipt{}, err
	}
	defer tx.Rollback()
	if err := s.checkVersion(ctx, tx); err != nil {
		return CollectionOwnerReceipt{}, err
	}
	a, err := readAuthority(ctx, tx, s.library)
	if err != nil {
		return CollectionOwnerReceipt{}, err
	}
	if a.Incarnation == "" || a.Incarnation != op.ExpectedIncarnation {
		return CollectionOwnerReceipt{}, ErrConflict
	}
	row, found, err := readCollectionAdmin(ctx, tx, op.ID)
	if err != nil {
		return CollectionOwnerReceipt{}, err
	}
	if found {
		if err := requireAdminCodec(ctx, tx, op.ID, "collection_owner1"); err != nil {
			return CollectionOwnerReceipt{}, err
		}
		if !bytes.Equal(row.request, data) {
			return CollectionOwnerReceipt{}, ErrChangedRequest
		}
		var receipt CollectionOwnerReceipt
		if decodeCanonicalValue(row.receipt, &receipt) != nil || receipt.Version != 1 || receipt.LibraryID != o.library || receipt.OperationID != op.ID || receipt.Sequence != row.sequence || receipt.PayloadSHA256 != digest || row.digest != digest || receipt.RecordedUnixNano != row.timestamp || receipt.Result.Incarnation != a.Incarnation || receipt.Result.OwnerSubjectID != a.OwnerSubjectID || string(receipt.Result.PolicyOperationID) != op.ID {
			return CollectionOwnerReceipt{}, corrupt("collection owner receipt")
		}
		return receipt, nil
	}
	var value ApplyCollectionPolicyRequest
	if decodeCanonicalValue(op.Value, &value) != nil {
		return CollectionOwnerReceipt{}, invalid("collection owner value")
	}
	policySequence, delta, err := s.applyCollectionPolicy(ctx, tx, value.Operation, a.OwnerSubjectID, "local_owner", s.now().UnixNano())
	if err != nil {
		return CollectionOwnerReceipt{}, err
	}
	usage, err := readCollectionQuota(ctx, tx, value.Operation.CollectionID)
	if err != nil {
		return CollectionOwnerReceipt{}, err
	}
	change := CollectionQuotaDelta{Library: delta}
	if value.Operation.CollectionID != "" {
		local := delta
		change.Collection = &local
	}
	after, err := planCollectionQuota(usage, change)
	if err != nil {
		return CollectionOwnerReceipt{}, err
	}
	if err := saveCollectionQuota(ctx, tx, value.Operation.CollectionID, after); err != nil {
		return CollectionOwnerReceipt{}, err
	}
	if err := appendCollectionOrder(ctx, tx, "owner_policy_sequence", policySequence); err != nil {
		return CollectionOwnerReceipt{}, err
	}
	sequence, err := nextSequence(ctx, tx, "catalog_admin_events")
	if err != nil {
		return CollectionOwnerReceipt{}, err
	}
	receipt := CollectionOwnerReceipt{1, s.library, op.ID, sequence, digest, s.now().UnixNano(), CollectionOwnerResult{a.Incarnation, a.OwnerSubjectID, value.Operation.ID}}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return CollectionOwnerReceipt{}, err
	}
	if err := insertCollectionAdmin(ctx, tx, s.library, op.ID, data, digest, receipt.RecordedUnixNano, encoded, sequence, "collection_owner1"); err != nil {
		return CollectionOwnerReceipt{}, err
	}
	if err := ctx.Err(); err != nil {
		return CollectionOwnerReceipt{}, err
	}
	if err := s.checkPaths(); err != nil {
		return CollectionOwnerReceipt{}, err
	}
	if err := tx.Commit(); err != nil {
		return CollectionOwnerReceipt{}, fmt.Errorf("collection owner commit outcome uncertain: %w", err)
	}
	return receipt, nil
}

func equalStringActions(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

package catalog

import (
    "bytes"
    "context"
    "crypto/rand"
    "encoding/hex"
    "encoding/json"
    "errors"
    "fmt"
    "math"
)

func (o *LocalOwnerService) lookupIssuanceV3(ctx context.Context, q queryer, op AdminRequestV3, data []byte) (IssuanceResultV3, bool, error) {
	row, found, err := readCollectionAdmin(ctx, q, op.ID)
	if err != nil || !found {
		return IssuanceResultV3{}, found, err
	}
	if err := requireAdminCodec(ctx, q, op.ID, "issue3"); err != nil {
		return IssuanceResultV3{}, false, err
	}
	if !bytes.Equal(row.request, data) {
		return IssuanceResultV3{}, false, ErrChangedRequest
	}
	receipt, err := decodeAdminReceiptV3(row.receipt, op, data, row.digest, row.sequence, row.timestamp)
	if err != nil {
		return IssuanceResultV3{}, false, err
	}
	var selector string
	if err := q.QueryRowContext(ctx, `SELECT selector FROM credential_issuance_receipts WHERE operation_id=?`, op.ID).Scan(&selector); err != nil || selector != receipt.Result.Selector {
		return IssuanceResultV3{}, false, corrupt("issuance selector link")
	}
	stored, err := readCredentialVersion(ctx, q, o.library, selector, o.store.version)
	var value IssueCredentialRequestV3
	if decodeCanonicalValue(op.Value, &value) != nil {
		return IssuanceResultV3{}, false, corrupt("issuance request")
	}
	if err != nil || stored.codec != 3 || stored.subject != receipt.Result.SubjectID || stored.incarnation != receipt.Result.Incarnation || stored.scope != value.Scope || !equalStringActions(stored.actions, value.Actions) || stored.expires != value.ExpiresUnix {
		return IssuanceResultV3{}, false, corrupt("issuance credential link")
	}
	status, err := credentialStatusVersion(ctx, q, o.library, receipt.Result.Selector, o.store.now().Unix(), o.store.version)
	return IssuanceResultV3{Receipt: receipt, Status: status}, true, err
}
func (o *LocalOwnerService) LookupIssuanceV3(ctx context.Context, op AdminRequestV3) (result IssuanceResultV3, found bool, resultErr error) {
	if err := o.enter(); err != nil {
		return result, false, err
	}
	defer o.store.leave()
	if !supportsDelivery(o.store.version) {
		return result, false, ErrUpgradeRequired
	}
	data, _, err := CanonicalAdminRequestV3(op)
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
	return o.lookupIssuanceV3(ctx, tx, op, data)
}
func (o *LocalOwnerService) IssuanceStatusV3(ctx context.Context, id string) (result IssuanceResultV3, resultErr error) {
	if err := o.enter(); err != nil {
		return result, err
	}
	defer o.store.leave()
	if !supportsDelivery(o.store.version) {
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
	if err := requireAdminCodec(ctx, tx, id, "issue3"); err != nil {
		return result, err
	}
	op, err := DecodeAdminRequestV3(row.request)
	if err != nil {
		return result, corrupt("issuance request")
	}
	result, _, err = o.lookupIssuanceV3(ctx, tx, op, row.request)
	return result, err
}
func (o *LocalOwnerService) IssueCredentialV3(ctx context.Context, input AdminRequestV3) (IssuanceResultV3, error) {
	if err := o.enter(); err != nil {
		return IssuanceResultV3{}, err
	}
	defer o.store.leave()
	s := o.store
	if !supportsDelivery(s.version) {
		return IssuanceResultV3{}, ErrUpgradeRequired
	}
	data, digest, err := CanonicalAdminRequestV3(input)
	if err != nil {
		return IssuanceResultV3{}, err
	}
	op, err := DecodeAdminRequestV3(data)
	if err != nil {
		return IssuanceResultV3{}, err
	}
	if op.LibraryID != o.library {
		return IssuanceResultV3{}, invalid("issuance library")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return IssuanceResultV3{}, err
	}
	defer tx.Rollback()
	if err := s.checkVersion(ctx, tx); err != nil {
		return IssuanceResultV3{}, err
	}
	a, err := readAuthority(ctx, tx, s.library)
	if err != nil {
		return IssuanceResultV3{}, err
	}
	if a.Incarnation == "" {
		return IssuanceResultV3{}, ErrUnauthenticated
	}
	prior, found, err := o.lookupIssuanceV3(ctx, tx, op, data)
	if err != nil || found {
		return prior, err
	}
	if a.Incarnation != op.ExpectedIncarnation {
		return IssuanceResultV3{}, ErrConflict
	}
	var value IssueCredentialRequestV3
	if decodeCanonicalValue(op.Value, &value) != nil {
		return IssuanceResultV3{}, invalid("issuance value")
	}
	subject, err := readSubject(ctx, tx, s.library, value.SubjectID)
	if err != nil {
		return IssuanceResultV3{}, err
	}
	if !subject.Value.Enabled || subject.Value.Kind == "service" && containsAction(value.Actions, "activity.record") {
		return IssuanceResultV3{}, ErrConflict
	}
	now := s.now().Unix()
	if now > math.MaxInt64-86400 || value.ExpiresUnix < now+60 || value.ExpiresUnix > now+86400 {
		return IssuanceResultV3{}, invalid("credential expiry")
	}
	selector := ""
	for i := 0; i < 8; i++ {
		candidate, err := randomIdentity("cred_")
		if err != nil {
			return IssuanceResultV3{}, err
		}
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM catalog_credentials WHERE selector=?)`, candidate).Scan(&exists); err != nil {
			return IssuanceResultV3{}, err
		}
		if !exists {
			selector = candidate
			break
		}
	}
	if selector == "" {
		return IssuanceResultV3{}, errors.New("catalog selector generation failed")
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return IssuanceResultV3{}, errors.New("catalog randomness unavailable")
	}
	var assetID any
	if value.Scope.Kind == "asset" {
		if _, err := readAsset(ctx, tx, s.library, value.Scope.AssetID); err != nil { return IssuanceResultV3{}, err }
		assetID = value.Scope.AssetID
	}
	verifier := credentialVerifier(s.library, a.Incarnation, selector, secret[:])
	if _, err := tx.ExecContext(ctx, `INSERT INTO catalog_credentials(selector,library_id,incarnation,subject_id,scope_kind,asset_id,actions,verifier,issued_unix,expires_unix,generation,revoked) VALUES(?,?,?,?,?,?,?,?,?,?,1,0)`, selector, s.library, a.Incarnation, value.SubjectID, value.Scope.Kind, assetID, actionsBytes(value.Actions), verifier[:], now, value.ExpiresUnix); err != nil {
		return IssuanceResultV3{}, errors.New("catalog credential insertion failed")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO credential_codecs(selector,codec) VALUES(?,3)`, selector); err != nil {
		return IssuanceResultV3{}, err
	}
	sequence, err := nextSequence(ctx, tx, "catalog_admin_events")
	if err != nil {
		return IssuanceResultV3{}, err
	}
	receipt := AdminReceiptV3(AdminReceipt{3, s.library, op.ID, sequence, digest, s.now().UnixNano(), AdminResult{Incarnation: a.Incarnation, OwnerSubjectID: a.OwnerSubjectID, SubjectID: value.SubjectID, Selector: selector}})
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return IssuanceResultV3{}, err
	}
	if err := insertCollectionAdmin(ctx, tx, s.library, op.ID, data, digest, receipt.RecordedUnixNano, encoded, sequence, "issue3"); err != nil {
		return IssuanceResultV3{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO credential_issuance_receipts(operation_id,selector) VALUES(?,?)`, op.ID, selector); err != nil {
		return IssuanceResultV3{}, err
	}
	if err := ctx.Err(); err != nil {
		return IssuanceResultV3{}, err
	}
	if err := s.checkPaths(); err != nil {
		return IssuanceResultV3{}, err
	}
	if err := tx.Commit(); err != nil {
		return IssuanceResultV3{}, fmt.Errorf("catalog issuance commit uncertain (operation %s selector %s): %w", op.ID, selector, err)
	}
	return IssuanceResultV3{receipt, "active", []byte("jmc1_" + selector[5:] + "." + hex.EncodeToString(secret[:]))}, nil
}

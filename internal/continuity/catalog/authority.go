package catalog

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func checkPrivatePaths(path string) error {
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		info, err := os.Lstat(path + suffix)
		if suffix != "" && os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if err := checkFileIdentity(info); err != nil {
			return err
		}
		if info.Mode().Perm()&0077 != 0 {
			return corrupt("authority file permissions")
		}
	}
	return nil
}

// LocalOwnerService is privileged local administration through OS catalog access.
// It is deliberately unrelated to bearer authentication and Manager admin keys.
type LocalOwnerService struct {
	store   *Store
	library continuity.LibraryID
}

func (s *Store) LocalOwner(library continuity.LibraryID) (*LocalOwnerService, error) {
	if err := s.enter(); err != nil {
		return nil, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return nil, err
	}
	if s.version != authoritySchemaVersion {
		return nil, ErrUpgradeRequired
	}
	return &LocalOwnerService{s, library}, nil
}
func (o *LocalOwnerService) enter() error {
	if o == nil || o.store == nil {
		return invalid("local owner capability")
	}
	if err := o.store.enter(); err != nil {
		return err
	}
	if o.store.version != authoritySchemaVersion {
		o.store.leave()
		return ErrUpgradeRequired
	}
	if err := o.store.requireLibrary(o.library); err != nil {
		o.store.leave()
		return err
	}
	return nil
}

func randomIdentity(prefix string) (string, error) {
	for i := 0; i < 8; i++ {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			return "", errors.New("catalog randomness unavailable")
		}
		id := prefix + hex.EncodeToString(b[:])
		if validRandomID(id, prefix) {
			return id, nil
		}
	}
	return "", errors.New("catalog randomness unavailable")
}
func NewSubjectID() (SubjectID, error) { id, err := randomIdentity("sub_"); return SubjectID(id), err }

func (o *LocalOwnerService) Bootstrap(ctx context.Context, op AdminRequest) (AdminReceipt, error) {
	return o.mutateKind(ctx, op, "bootstrap")
}
func (o *LocalOwnerService) Recover(ctx context.Context, op AdminRequest) (AdminReceipt, error) {
	return o.mutateKind(ctx, op, "recover")
}
func (o *LocalOwnerService) ApplyPolicy(ctx context.Context, op AdminRequest) (AdminReceipt, error) {
	return o.mutateKind(ctx, op, "apply_policy")
}
func (o *LocalOwnerService) RevokeCredential(ctx context.Context, op AdminRequest) (AdminReceipt, error) {
	return o.mutateKind(ctx, op, "revoke_credential")
}
func (o *LocalOwnerService) mutateKind(ctx context.Context, op AdminRequest, kind string) (AdminReceipt, error) {
	if op.Kind != kind {
		return AdminReceipt{}, invalid("admin method kind")
	}
	result, err := o.mutate(ctx, op)
	return result.Receipt, err
}
func (o *LocalOwnerService) IssueCredential(ctx context.Context, op AdminRequest) (IssuanceResult, error) {
	if op.Kind != "issue_credential" {
		return IssuanceResult{}, invalid("admin method kind")
	}
	return o.mutate(ctx, op)
}

func adminRetry(ctx context.Context, q queryer, library continuity.LibraryID, op AdminRequest, data []byte) (AdminReceipt, bool, error) {
	var request, receiptData []byte
	var digest string
	var sequence, timestamp int64
	err := q.QueryRowContext(ctx, `SELECT CASE WHEN length(request)<=1048576 THEN request END,CASE WHEN length(receipt)<=2048 THEN receipt END,payload_sha256,sequence,recorded_unix_nano FROM catalog_admin_events WHERE operation_id=? AND library_id=?`, op.ID, library).Scan(&request, &receiptData, &digest, &sequence, &timestamp)
	if errors.Is(err, sql.ErrNoRows) {
		return AdminReceipt{}, false, nil
	}
	if err != nil {
		return AdminReceipt{}, false, err
	}
	if !bytes.Equal(request, data) {
		return AdminReceipt{}, false, ErrChangedRequest
	}
	var receipt AdminReceipt
	if decodeCanonicalValue(receiptData, &receipt) != nil || receipt.Version != 1 || receipt.LibraryID != library || receipt.OperationID != op.ID || receipt.Sequence != sequence || receipt.RecordedUnixNano != timestamp || receipt.PayloadSHA256 != digest || digest != domainDigest("juicemount.catalog.authority.operation\x00\x01", request) {
		return AdminReceipt{}, false, corrupt("admin receipt identity")
	}
	a, err := readAuthority(ctx, q, library)
	if err != nil {
		return AdminReceipt{}, false, err
	}
	if op.Kind == "bootstrap" || op.Kind == "recover" {
		if a.Incarnation != receipt.Result.Incarnation || a.OwnerSubjectID != receipt.Result.OwnerSubjectID {
			return AdminReceipt{}, false, ErrConflict
		}
	}
	if op.Kind == "apply_policy" && a.Incarnation != op.ExpectedIncarnation {
		return AdminReceipt{}, false, ErrConflict
	}
	return receipt, true, nil
}

// LookupIssuance performs exact-request owner-only lookup before CLI output
// reservation. It neither generates a secret nor promises a later first issuance.
func (o *LocalOwnerService) LookupIssuance(ctx context.Context, op AdminRequest) (result IssuanceResult, found bool, resultErr error) {
	if err := o.enter(); err != nil {
		return IssuanceResult{}, false, err
	}
	defer o.store.leave()
	if op.Kind != "issue_credential" || op.LibraryID != o.library {
		return IssuanceResult{}, false, invalid("issuance lookup")
	}
	data, _, err := CanonicalAdminRequest(op)
	if err != nil {
		return IssuanceResult{}, false, err
	}
	tx, done, err := o.store.readSnapshot(ctx)
	if err != nil {
		return IssuanceResult{}, false, err
	}
	defer func() { resultErr = errors.Join(resultErr, done()) }()
	r, found, err := adminRetry(ctx, tx, o.library, op, data)
	if err != nil || !found {
		return IssuanceResult{}, found, err
	}
	status, err := credentialStatus(ctx, tx, o.library, r.Result.Selector, o.store.now().Unix())
	return IssuanceResult{Receipt: r, Status: status}, true, err
}

func (o *LocalOwnerService) CredentialStatus(ctx context.Context, selector string) (status string, resultErr error) {
	if err := o.enter(); err != nil {
		return "", err
	}
	defer o.store.leave()
	if !validRandomID(selector, "cred_") {
		return "", invalid("selector")
	}
	tx, done, err := o.store.readSnapshot(ctx)
	if err != nil {
		return "", err
	}
	defer func() { resultErr = errors.Join(resultErr, done()) }()
	return credentialStatus(ctx, tx, o.library, selector, o.store.now().Unix())
}

func (o *LocalOwnerService) IssuanceStatus(ctx context.Context, id string) (result IssuanceResult, resultErr error) {
	if err := o.enter(); err != nil {
		return result, err
	}
	defer o.store.leave()
	if !validOperationID(id) {
		return result, invalid("operation identity")
	}
	tx, done, err := o.store.readSnapshot(ctx)
	if err != nil {
		return result, err
	}
	defer func() { resultErr = errors.Join(resultErr, done()) }()
	var data []byte
	err = tx.QueryRowContext(ctx, `SELECT CASE WHEN length(request)<=1048576 THEN request END FROM catalog_admin_events WHERE operation_id=? AND library_id=?`, id, o.library).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return result, ErrNotFound
	}
	if err != nil {
		return result, err
	}
	op, err := DecodeAdminRequest(data)
	if err != nil || op.Kind != "issue_credential" {
		return result, ErrNotFound
	}
	receipt, found, err := adminRetry(ctx, tx, o.library, op, data)
	if err != nil {
		return result, err
	}
	if !found {
		return result, ErrNotFound
	}
	status, err := credentialStatus(ctx, tx, o.library, receipt.Result.Selector, o.store.now().Unix())
	return IssuanceResult{Receipt: receipt, Status: status}, err
}

func (o *LocalOwnerService) mutate(ctx context.Context, input AdminRequest) (IssuanceResult, error) {
	if err := o.enter(); err != nil {
		return IssuanceResult{}, err
	}
	defer o.store.leave()
	if input.LibraryID != o.library {
		return IssuanceResult{}, invalid("admin library")
	}
	data, digest, err := CanonicalAdminRequest(input)
	if err != nil {
		return IssuanceResult{}, err
	}
	op, err := DecodeAdminRequest(data)
	if err != nil {
		return IssuanceResult{}, err
	}
	value, _ := adminValue(op)
	s := o.store
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return IssuanceResult{}, err
	}
	defer tx.Rollback()
	if err := s.checkPaths(); err != nil {
		return IssuanceResult{}, err
	}
	prior, found, err := adminRetry(ctx, tx, o.library, op, data)
	if err != nil {
		return IssuanceResult{}, err
	}
	if found {
		result := IssuanceResult{Receipt: prior}
		if prior.Result.Selector != "" {
			result.Status, err = credentialStatus(ctx, tx, o.library, prior.Result.Selector, s.now().Unix())
		}
		return result, err
	}
	a, err := readAuthority(ctx, tx, o.library)
	if err != nil {
		return IssuanceResult{}, err
	}
	if op.Kind == "bootstrap" {
		if a.Incarnation != "" {
			return IssuanceResult{}, ErrConflict
		}
	} else if a.Incarnation == "" || a.Incarnation != op.ExpectedIncarnation {
		return IssuanceResult{}, ErrConflict
	}
	result := IssuanceResult{}
	r := AdminResult{Incarnation: a.Incarnation, OwnerSubjectID: a.OwnerSubjectID}
	switch v := value.(type) {
	case *BootstrapRequest:
		if v.CreateLabel != nil {
			policy := PolicyOperation{1, s.library, op.ID, "local_owner", v.OwnerSubjectID, []PolicyChange{NewPolicyChange("create_subject", CreateSubject{v.OwnerSubjectID, SubjectValue{"person", *v.CreateLabel, true}})}}
			p, err := s.applyPolicy(ctx, tx, policy)
			if err != nil {
				return result, err
			}
			r.PolicySequence = p.Sequence
		} else {
			sub, err := readSubject(ctx, tx, s.library, v.OwnerSubjectID)
			if err != nil {
				return result, err
			}
			if sub.Value.Kind != "person" || !sub.Value.Enabled {
				return result, ErrConflict
			}
		}
		inc, err := randomIdentity("auth_")
		if err != nil {
			return result, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO catalog_authority(singleton,library_id,incarnation,owner_subject_id) VALUES(1,?,?,?)`, s.library, inc, v.OwnerSubjectID); err != nil {
			return result, err
		}
		r.Incarnation, r.OwnerSubjectID = inc, v.OwnerSubjectID
	case *RecoverRequest:
		if v.OwnerSubjectID != a.OwnerSubjectID {
			return result, ErrConflict
		}
		inc := ""
		for i := 0; i < 8; i++ {
			candidate, err := randomIdentity("auth_")
			if err != nil {
				return result, err
			}
			if candidate != a.Incarnation {
				inc = candidate
				break
			}
		}
		if inc == "" {
			return result, errors.New("catalog incarnation generation failed")
		}
		if err := changedOne(tx.ExecContext(ctx, `UPDATE catalog_authority SET incarnation=? WHERE singleton=1 AND incarnation=? AND owner_subject_id=?`, inc, a.Incarnation, v.OwnerSubjectID)); err != nil {
			return result, err
		}
		r.Incarnation, r.OwnerSubjectID = inc, v.OwnerSubjectID
	case *ApplyPolicyRequest:
		if v.Operation.OperatorSubjectID != a.OwnerSubjectID {
			return result, ErrConflict
		}
		p, err := s.applyPolicy(ctx, tx, v.Operation)
		if err != nil {
			return result, err
		}
		r.PolicySequence = p.Sequence
	case *IssueCredentialRequest:
		sub, err := readSubject(ctx, tx, s.library, v.SubjectID)
		if err != nil {
			return result, err
		}
		if !sub.Value.Enabled || sub.Value.Kind == "service" && containsAction(v.Actions, "activity.record") {
			return result, ErrConflict
		}
		if v.Scope.Kind == "asset" {
			if _, err := readAsset(ctx, tx, s.library, v.Scope.AssetID); err != nil {
				return result, err
			}
		}
		now := s.now().Unix()
		if now > math.MaxInt64-86400 || v.ExpiresUnix < now+60 || v.ExpiresUnix > now+86400 {
			return result, invalid("credential expiry")
		}
		selector := ""
		for i := 0; i < 8; i++ {
			candidate, err := randomIdentity("cred_")
			if err != nil {
				return result, err
			}
			var exists bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM catalog_credentials WHERE selector=?)`, candidate).Scan(&exists); err != nil {
				return result, err
			}
			if !exists {
				selector = candidate
				break
			}
		}
		if selector == "" {
			return result, errors.New("catalog selector generation failed")
		}
		var secret [32]byte
		if _, err := rand.Read(secret[:]); err != nil {
			return result, errors.New("catalog randomness unavailable")
		}
		verifier := credentialVerifier(s.library, a.Incarnation, selector, secret[:])
		if _, err := tx.ExecContext(ctx, `INSERT INTO catalog_credentials(selector,library_id,incarnation,subject_id,scope_kind,asset_id,actions,verifier,issued_unix,expires_unix,generation,revoked) VALUES(?,?,?,?,?,?,?,?,?,?,1,0)`, selector, s.library, a.Incarnation, v.SubjectID, v.Scope.Kind, nullableAsset(v.Scope), actionsBytes(v.Actions), verifier[:], now, v.ExpiresUnix); err != nil {
			return result, errors.New("catalog credential insertion failed")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO credential_issuance_receipts(operation_id,selector) VALUES(?,?)`, op.ID, selector); err != nil {
			return result, err
		}
		result.Bearer = []byte("jmc1_" + selector[5:] + "." + hex.EncodeToString(secret[:]))
		r.Incarnation, r.SubjectID, r.Selector = a.Incarnation, v.SubjectID, selector
		result.Status = "active"
	case *RevokeCredentialRequest:
		c, err := readCredential(ctx, tx, s.library, v.Selector)
		if err != nil {
			return result, err
		}
		if c.incarnation != a.Incarnation || c.revoked || c.generation != v.ExpectedGeneration || c.generation == math.MaxInt64 {
			return result, ErrConflict
		}
		if err := changedOne(tx.ExecContext(ctx, `UPDATE catalog_credentials SET revoked=1,generation=generation+1 WHERE selector=? AND generation=? AND revoked=0`, v.Selector, v.ExpectedGeneration)); err != nil {
			return result, err
		}
		r.Incarnation, r.SubjectID, r.Selector = c.incarnation, c.subject, v.Selector
		result.Status = "revoked"
	}
	sequence, err := nextSequence(ctx, tx, "catalog_admin_events")
	if err != nil {
		return IssuanceResult{}, err
	}
	receipt := AdminReceipt{1, s.library, op.ID, sequence, digest, s.now().UnixNano(), r}
	b, _ := json.Marshal(receipt)
	if _, err := tx.ExecContext(ctx, `INSERT INTO catalog_admin_events(sequence,library_id,operation_id,request,payload_sha256,recorded_unix_nano,receipt) VALUES(?,?,?,?,?,?,?)`, sequence, s.library, op.ID, data, digest, receipt.RecordedUnixNano, b); err != nil {
		return IssuanceResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return IssuanceResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return IssuanceResult{}, fmt.Errorf("catalog authority commit outcome uncertain: %w", err)
	}
	result.Receipt = receipt
	return result, nil
}

func credentialVerifier(library continuity.LibraryID, incarnation, selector string, secret []byte) [32]byte {
	h := sha256.New()
	h.Write([]byte("juicemount.catalog.credential\x00\x01"))
	for _, id := range []string{string(library)[4:], incarnation[5:], selector[5:]} {
		raw, _ := hex.DecodeString(id)
		h.Write(raw)
	}
	h.Write(secret)
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

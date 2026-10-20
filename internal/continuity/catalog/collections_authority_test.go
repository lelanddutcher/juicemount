package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestCollectionAuthorityV2IssueRetryRevokeAndRecovery(t *testing.T) {
	s, o, a, _, _, _ := collectionTxStore(t)
	ctx := context.Background()
	first, op := collectionTxIssue(t, o, a, "new-v2", ownerSubject, collectionTxActions)
	retry, err := o.IssueCredentialV2(ctx, op)
	if err != nil || len(retry.Bearer) != 0 || retry.Receipt != first.Receipt || retry.Status != "active" {
		t.Fatalf("v2 retry: %v", err)
	}
	lookup, found, err := o.LookupIssuanceV2(ctx, op)
	if err != nil || !found || lookup.Receipt != first.Receipt || len(lookup.Bearer) != 0 {
		t.Fatalf("v2 lookup: %v", err)
	}
	status, err := o.IssuanceStatusV2(ctx, op.ID)
	if err != nil || status.Status != "active" || len(status.Bearer) != 0 {
		t.Fatalf("v2 status: %v", err)
	}
	old := AdminRequest{1, op.LibraryID, op.ID, op.ExpectedIncarnation, op.Kind, collectionTxRaw(t, IssueCredentialRequest{ownerSubject, Scope{"library", ""}, []string{"assets.discover"}, s.now().Unix() + 3600})}
	if _, err := o.IssueCredential(ctx, old); !errors.Is(err, ErrChangedRequest) {
		t.Fatalf("cross-codec ID accepted: %v", err)
	}
	if _, err := s.Authenticate(ctx, testLibrary, first.Bearer); err != nil {
		t.Fatal(err)
	}
	if _, err := o.RevokeCredential(ctx, adminTestRequest("revoke-v2", a.Incarnation, "revoke_credential", RevokeCredentialRequest{first.Receipt.Result.Selector, 1})); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Authenticate(ctx, testLibrary, first.Bearer); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("v2 revoke: %v", err)
	}
	retry, err = o.IssueCredentialV2(ctx, op)
	if err != nil || retry.Status != "revoked" || len(retry.Bearer) != 0 {
		t.Fatalf("historical receipt claims active: %v", err)
	}
	second, op2 := collectionTxIssue(t, o, a, "before-recover-v2", ownerSubject, collectionTxActions)
	if _, err := o.Recover(ctx, adminTestRequest("recover-v2", a.Incarnation, "recover", RecoverRequest{ownerSubject})); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Authenticate(ctx, testLibrary, second.Bearer); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("recovery failed: %v", err)
	}
	status, err = o.IssuanceStatusV2(ctx, op2.ID)
	if err != nil || status.Status != "invalidated" {
		t.Fatalf("recovery status: %v", err)
	}
}
func TestCollectionAuthorityMixedLegacyGenerationAndLastAdmin(t *testing.T) {
	s, o, a, d, owner, _ := collectionTxStore(t)
	ctx := context.Background()
	created := collectionTxCall(t, d, owner, collectionTxCreate(t, 40))
	sub, err := readSubject(ctx, s.db, testLibrary, ownerSubject)
	if err != nil {
		t.Fatal(err)
	}
	policyTest(t, o, a, "legacy-after-collection", NewPolicyChange("update_subject", UpdateSubject{ownerSubject, sub.Expected(), SubjectValue{"person", "owner renamed", true}}))
	if err := checkCollectionOrder(ctx, s.db); err != nil {
		t.Fatal(err)
	}
	grant, err := readCollectionGrant(ctx, s.db, "cgr_22222222222222222222222222222222")
	if err != nil {
		t.Fatal(err)
	}
	removal := CollectionRequest{1, testLibrary, "cop_00000000000000000000000000000029", "col_00000000000000000000000000000028", created.ResultVersion, "grants", collectionTxRaw(t, EditCollectionGrants{[]CollectionGrantChange{{"remove", grant.id, &grant.expected, nil}}})}
	collectionTxDenied(t, d, owner, removal, ErrConflict)
	p := collectionTxPolicy(42, removal.CollectionID, "security_remove", CollectionGrantChange{"remove", grant.id, &grant.expected, nil})
	request := collectionTxOwner(t, a, p)
	first, err := o.ApplyCollectionPolicy(ctx, request)
	if err != nil {
		t.Fatalf("owner last-admin removal refused: %v", err)
	}
	retry, err := o.ApplyCollectionPolicy(ctx, request)
	if err != nil || retry != first {
		t.Fatalf("owner exact retry: %v", err)
	}
	p.ID = "cpo_0000000000000000000000000000002b"
	if _, err := o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, p)); !errors.Is(err, ErrConflict) {
		t.Fatalf("credit double spend: %v", err)
	}
}
func TestCollectionGeneratedChildCorruptionRefused(t *testing.T) {
	s, _, _, d, owner, _ := collectionTxStore(t)
	ctx := context.Background()
	collectionTxCall(t, d, owner, collectionTxCreate(t, 50))
	if _, err := s.db.ExecContext(ctx, `DROP TRIGGER collection_policy_events_no_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE collection_policy_events SET operator_subject_id=?,namespace_subject_id=? WHERE administration='collection_service'`, readerSubject, readerSubject); err != nil {
		t.Fatal(err)
	}
	if err := validateCollectionHistory(ctx, s.db, testLibrary); err == nil || !strings.Contains(err.Error(), "child binding") {
		t.Fatalf("linked wrong actor admitted: %v", err)
	}
}
func TestCollectionOwnerEnvelopeCanonicalSeparation(t *testing.T) {
	_, _, a, _, _, _ := collectionTxStore(t)
	value := CollectionGrantValue{ownerSubject, CollectionGrantScope{"library_create", ""}, []string{"collections.create"}}
	request := collectionTxOwner(t, a, collectionTxPolicy(60, "", "library_create", CollectionGrantChange{"put", "cgr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil, &value}))
	encoded, _, err := CanonicalCollectionOwnerRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCollectionOwnerRequest(encoded)
	if err != nil || decoded.ID != request.ID {
		t.Fatalf("owner envelope: %v", err)
	}
	var input map[string]any
	if err := json.Unmarshal(encoded, &input); err != nil {
		t.Fatal(err)
	}
	input["operator_subject_id"] = ownerSubject
	bad, _ := json.Marshal(input)
	if _, err := DecodeCollectionOwnerRequest(bad); err == nil {
		t.Fatal("caller actor admitted")
	}
}

func TestCollectionIssuanceReceiptResultAndLinkBinding(t *testing.T) {
	for _, kind := range []string{"subject", "incarnation", "mapping"} {
		t.Run(kind, func(t *testing.T) {
			s, o, a, _, _, _ := collectionTxStore(t)
			ctx := context.Background()
			issued, op := collectionTxIssue(t, o, a, "bound-result", ownerSubject, collectionTxActions)
			request, digest, err := CanonicalAdminRequestV2(op)
			if err != nil {
				t.Fatal(err)
			}
			receipt := issued.Receipt
			if kind == "subject" {
				receipt.Result.SubjectID = readerSubject
			} else if kind == "incarnation" {
				receipt.Result.Incarnation = "auth_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			}
			if kind != "mapping" {
				altered, err := json.Marshal(receipt)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := decodeAdminReceiptV2(altered, op, request, digest, receipt.Sequence, receipt.RecordedUnixNano); err == nil {
					t.Fatalf("altered %s accepted", kind)
				}
			} else {
				if _, err := s.db.ExecContext(ctx, `DROP TRIGGER credential_issuance_receipts_no_delete`); err != nil {
					t.Fatal(err)
				}
				if _, err := s.db.ExecContext(ctx, `DELETE FROM credential_issuance_receipts WHERE operation_id=?`, op.ID); err != nil {
					t.Fatal(err)
				}
				if _, _, err := o.LookupIssuanceV2(ctx, op); err == nil || !strings.Contains(err.Error(), "selector link") {
					t.Fatalf("missing mapping accepted: %v", err)
				}
			}
		})
	}
}
func TestCollectionQuotaGuardsAndReadyValidation(t *testing.T) {
	for _, kind := range []string{"missing", "local-cap"} {
		t.Run(kind, func(t *testing.T) {
			s, _, _, d, owner, _ := collectionTxStore(t)
			ctx := context.Background()
			op := collectionTxCreate(t, 70)
			collectionTxCall(t, d, owner, op)
			if _, err := s.db.ExecContext(ctx, `DELETE FROM collection_quota WHERE collection_id=?`, op.CollectionID); err == nil {
				t.Fatal("quota lifetime delete admitted")
			}
			if kind == "missing" {
				if _, err := s.db.ExecContext(ctx, `DROP TRIGGER collection_quota_no_delete`); err != nil {
					t.Fatal(err)
				}
				if _, err := s.db.ExecContext(ctx, `DELETE FROM collection_quota WHERE collection_id=?`, op.CollectionID); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := s.db.ExecContext(ctx, `UPDATE collection_library_quota SET active_entries=4097,lifetime_entries=4097 WHERE singleton=1`); err != nil {
					t.Fatal(err)
				}
				if _, err := s.db.ExecContext(ctx, `UPDATE collection_quota SET active_entries=4097,lifetime_entries=4097 WHERE collection_id=?`, op.CollectionID); err != nil {
					t.Fatal(err)
				}
			}
			if err := checkCollectionState(ctx, s.db, testLibrary); err == nil || !strings.Contains(err.Error(), "local quota") {
				t.Fatalf("invalid ready local quota admitted: %v", err)
			}
		})
	}
}
func TestCollectionGeneratedBindingAndDeclarationCorruption(t *testing.T) {
	for _, kind := range []string{"request-hash", "declaration-actor"} {
		t.Run(kind, func(t *testing.T) {
			s, _, _, d, owner, _ := collectionTxStore(t)
			ctx := context.Background()
			collectionTxCall(t, d, owner, collectionTxCreate(t, 80))
			if kind == "declaration-actor" {
				if _, err := s.db.ExecContext(ctx, `DROP TRIGGER collection_identities_identity`); err != nil {
					t.Fatal(err)
				}
				if _, err := s.db.ExecContext(ctx, `UPDATE collection_identities SET creator_subject_id=?`, readerSubject); err != nil {
					t.Fatal(err)
				}
			} else {
				var data []byte
				var sequence int64
				if err := s.db.QueryRowContext(ctx, `SELECT sequence,request FROM collection_policy_events WHERE administration='collection_service'`).Scan(&sequence, &data); err != nil {
					t.Fatal(err)
				}
				request, err := DecodeCollectionPolicyRequest(data)
				if err != nil {
					t.Fatal(err)
				}
				request.BindingPayloadSHA256 = strings.Repeat("a", 64)
				encoded, digest, err := CanonicalCollectionPolicyRequest(request)
				if err != nil {
					t.Fatal(err)
				}
				receipt, err := CanonicalCollectionPolicyReceipt(CollectionPolicyReceipt{2, testLibrary, request.ID, digest, s.now().UnixNano()})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.db.ExecContext(ctx, `DROP TRIGGER collection_policy_events_no_update`); err != nil {
					t.Fatal(err)
				}
				if _, err := s.db.ExecContext(ctx, `UPDATE collection_policy_events SET request=?,payload_sha256=?,receipt=? WHERE sequence=?`, encoded, digest, receipt, sequence); err != nil {
					t.Fatal(err)
				}
			}
			if err := validateCollectionHistory(ctx, s.db, testLibrary); err == nil {
				t.Fatalf("mismatched %s accepted", kind)
			}
		})
	}
}

func TestCollectionReadyOrphanDeclarationRefused(t *testing.T) {
	s, _, _, d, owner, _ := collectionTxStore(t)
	ctx := context.Background()
	create := collectionTxCreate(t, 160)
	collectionTxCall(t, d, owner, create)
	if err := checkCollectionState(ctx, s.db, testLibrary); err != nil {
		t.Fatalf("valid source state: %v", err)
	}
	var originalSequence int64
	if err := s.db.QueryRowContext(ctx, `SELECT created_sequence FROM collection_definitions WHERE collection_id=?`, create.CollectionID).Scan(&originalSequence); err != nil {
		t.Fatal(err)
	}
	// Every FK remains valid, including created_sequence, but that event creates
	// the original collection. No canonical event creates this extra declaration.
	orphan := CollectionID("col_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if _, err := s.db.ExecContext(ctx, `INSERT INTO collection_identities(collection_id,library_id,creator_subject_id,creation_operation_id,creation_payload_sha256,policy_generation) VALUES(?,?,?,?,?,1)`, orphan, testLibrary, ownerSubject, "cop_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO collection_definitions(collection_id,kind,name,archived,definition_version,created_sequence,updated_sequence) VALUES(?,'manual','orphan',0,?,?,?)`, orphan, "cv_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", originalSequence, originalSequence); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO collection_quota(collection_id,`+collectionQuotaColumns+`) VALUES(?,0,0,0,0,0,0,0,0,0,0,0,0)`, orphan); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE collection_library_quota SET collections=collections+1 WHERE singleton=1`); err != nil {
		t.Fatal(err)
	}
	if err := checkIntegrity(ctx, s.db); err != nil {
		t.Fatalf("corruption fixture violated FK/integrity instead of reverse binding: %v", err)
	}
	if err := checkCollectionOrder(ctx, s.db); err != nil {
		t.Fatalf("timeline positive control: %v", err)
	}
	if err := validateCollectionHistory(ctx, s.db, testLibrary); err != nil {
		t.Fatalf("forward binding positive control: %v", err)
	}
	if err := checkCollectionState(ctx, s.db, testLibrary); err == nil || !strings.Contains(err.Error(), "collection declaration create binding") {
		t.Fatalf("orphan declaration not refused for exact reverse binding: %v", err)
	}
}

func TestCollectionReadyWrongCreatedSequenceRefused(t *testing.T) {
	s, _, _, d, owner, _ := collectionTxStore(t)
	ctx := context.Background()
	create := collectionTxCreate(t, 170)
	created := collectionTxCall(t, d, owner, create)
	rename := CollectionRequest{1, testLibrary, "cop_000000000000000000000000000000ab", create.CollectionID, created.ResultVersion, "rename", collectionTxRaw(t, RenameCollection{"renamed"})}
	collectionTxCall(t, d, owner, rename)
	if err := checkCollectionState(ctx, s.db, testLibrary); err != nil {
		t.Fatalf("valid source state: %v", err)
	}
	var renameSequence int64
	if err := s.db.QueryRowContext(ctx, `SELECT sequence FROM collection_events WHERE actor_subject_id=? AND operation_id=?`, ownerSubject, rename.ID).Scan(&renameSequence); err != nil {
		t.Fatal(err)
	}
	// The deliberate corruption bypasses only the immutable-column guard. The
	// reference names a real canonical rename of this same collection/actor.
	if _, err := s.db.ExecContext(ctx, `DROP TRIGGER collection_definitions_identity`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE collection_definitions SET created_sequence=? WHERE collection_id=?`, renameSequence, create.CollectionID); err != nil {
		t.Fatal(err)
	}
	if err := checkIntegrity(ctx, s.db); err != nil {
		t.Fatalf("fixture violated FK/integrity instead of create-kind binding: %v", err)
	}
	if err := checkCollectionOrder(ctx, s.db); err != nil {
		t.Fatalf("timeline positive control: %v", err)
	}
	if err := validateCollectionHistory(ctx, s.db, testLibrary); err != nil {
		t.Fatalf("forward binding positive control: %v", err)
	}
	if err := checkCollectionState(ctx, s.db, testLibrary); err == nil || !strings.Contains(err.Error(), "collection declaration create binding") {
		t.Fatalf("wrong created_sequence not refused for exact reverse binding: %v", err)
	}
}

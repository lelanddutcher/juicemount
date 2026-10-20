package catalog

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func collectionTestGrantValue() CollectionGrantValue {
	return CollectionGrantValue{collectionTestSubject, CollectionGrantScope{"collection", collectionTestID}, []string{"collections.admin", "collections.edit", "collections.read"}}
}
func collectionTestPut() CollectionGrantChange {
	value := collectionTestGrantValue()
	return CollectionGrantChange{"put", collectionTestGrant, nil, &value}
}
func collectionTestPolicy(t *testing.T) CollectionPolicyRequest {
	t.Helper()
	return CollectionPolicyRequest{2, collectionTestLibrary, generatedCollectionPolicyID(collectionTestOperation), "collection", collectionTestID, "", "", []CollectionGrantChange{collectionTestPut()}}
}
func collectionTestPolicyRecord(t *testing.T) CollectionPolicyRecord {
	return CollectionPolicyRecord{2, collectionTestPolicy(t), "local_owner", collectionTestSubject, []CollectionGrantResult{{collectionTestGrant, collectionTestGrantVersion, false}}, 123}
}
func collectionTestPolicyReceipt(t *testing.T, request CollectionPolicyRequest) CollectionPolicyReceipt {
	t.Helper()
	_, digest, err := CanonicalCollectionPolicyRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	return CollectionPolicyReceipt{2, request.LibraryID, request.ID, digest, 123}
}
func collectionTestAdmin(t *testing.T) AdminRequestV2 {
	return AdminRequestV2{2, collectionTestLibrary, "credential-op", "auth_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "issue_credential", collectionTestRaw(t, IssueCredentialRequestV2{collectionTestSubject, Scope{"library", ""}, []string{"activity.record", "assets.discover", "collections.admin", "collections.create", "collections.edit", "collections.read", "locations.read", "relationships.read", "revisions.read"}, 1800000000})}
}

func TestCollectionPolicyFullValueAndFreshResult(t *testing.T) {
	op := collectionTestPolicy(t)
	original := collectionTestGrantValue()
	expected := ExpectedCollectionGrant{original, collectionTestGrantVersion, false}
	changed := original
	changed.Actions = []string{"collections.read"}
	op.Changes = []CollectionGrantChange{{"put", collectionTestGrant, &expected, &changed}}
	if _, _, err := CanonicalCollectionPolicyRequest(op); err != nil {
		t.Fatal(err)
	}
	same := original
	op.Changes[0].Value = &same
	if _, _, err := CanonicalCollectionPolicyRequest(op); err == nil {
		t.Fatal("active full-value no-op admitted")
	}
	op.Changes[0].Expected.Removed = true
	if _, _, err := CanonicalCollectionPolicyRequest(op); err != nil {
		t.Fatalf("reactivation refused: %v", err)
	}
	op.Changes[0].Value = &changed
	changed.SubjectID = "sub_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, _, err := CanonicalCollectionPolicyRequest(op); err == nil {
		t.Fatal("grant grantee retarget admitted")
	}
	changed = original
	changed.Scope = CollectionGrantScope{"collection", "col_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	op.Changes[0].Value = &changed
	if _, _, err := CanonicalCollectionPolicyRequest(op); err == nil {
		t.Fatal("grant scope retarget admitted")
	}

	record := collectionTestPolicyRecord(t)
	record.Request.Changes[0].Expected = &ExpectedCollectionGrant{original, collectionTestGrantVersion, true}
	if _, _, err := CanonicalCollectionPolicyRecord(record); err == nil {
		t.Fatal("stale result version admitted")
	}
	record.Results[0].Version = "cgv_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	b, _, err := CanonicalCollectionPolicyRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCollectionPolicyRecord(b)
	if err != nil || !reflect.DeepEqual(decoded, record) {
		t.Fatalf("policy round trip: %v", err)
	}
	for _, mutate := range []func(*CollectionPolicyRecord){
		func(r *CollectionPolicyRecord) { r.Results = nil },
		func(r *CollectionPolicyRecord) { r.Results[0].Removed = true },
		func(r *CollectionPolicyRecord) { r.Results[0].ID = "cgr_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" },
		func(r *CollectionPolicyRecord) { r.RecordedUnixNano = -1 },
		func(r *CollectionPolicyRecord) { r.Administration = "collection_service" },
	} {
		bad := record
		bad.Results = append([]CollectionGrantResult(nil), record.Results...)
		mutate(&bad)
		if _, _, err := CanonicalCollectionPolicyRecord(bad); err == nil {
			t.Fatal("malformed policy record admitted")
		}
	}
}

func TestCollectionPolicyCategoriesAndNamespaces(t *testing.T) {
	owner := collectionTestPolicyRecord(t)
	ownerNS, err := owner.Namespace()
	if err != nil {
		t.Fatal(err)
	}
	if ownerNS.SubjectID != "" || ownerNS.Administration != "local_owner" {
		t.Fatal("owner namespace includes actor")
	}
	generated := owner
	_, digest, err := CanonicalCollectionRequest(collectionTestCreate(t))
	if err != nil {
		t.Fatal(err)
	}
	generated.Request.BindingOperationID = collectionTestOperation
	generated.Request.BindingPayloadSHA256 = digest
	generated.Administration = "collection_service"
	a, err := generated.Namespace()
	if err != nil {
		t.Fatal(err)
	}
	generated.OperatorSubjectID = "sub_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	b, err := generated.Namespace()
	if err != nil {
		t.Fatal(err)
	}
	if a == b || a == ownerNS || b == ownerNS || a.OperationID != b.OperationID {
		t.Fatal("same operation ID namespaces collide")
	}
	wrong := generated
	wrong.Request.ID = "cpo_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, _, err := CanonicalCollectionPolicyRecord(wrong); err == nil {
		t.Fatal("wrong generated policy ID admitted")
	}
	wrong = generated
	wrong.Request.BindingPayloadSHA256 = ""
	if _, _, err := CanonicalCollectionPolicyRecord(wrong); err == nil {
		t.Fatal("partial binding admitted")
	}
	wrong = generated
	wrong.Administration = "local_owner"
	if _, _, err := CanonicalCollectionPolicyRecord(wrong); err == nil {
		t.Fatal("generated owner label admitted")
	}

	library := collectionTestPolicy(t)
	library.Category = "library_create"
	library.CollectionID = ""
	value := CollectionGrantValue{collectionTestSubject, CollectionGrantScope{"library_create", ""}, []string{"collections.create"}}
	library.Changes = []CollectionGrantChange{{"put", collectionTestGrant, nil, &value}}
	if _, _, err := CanonicalCollectionPolicyRequest(library); err != nil {
		t.Fatalf("library provisioning: %v", err)
	}
	for _, mutate := range []func(*CollectionPolicyRequest){
		func(r *CollectionPolicyRequest) { r.CollectionID = collectionTestID },
		func(r *CollectionPolicyRequest) { r.Category = "collection" },
		func(r *CollectionPolicyRequest) {
			r.BindingOperationID = collectionTestOperation
			r.BindingPayloadSHA256 = digest
		},
	} {
		bad := library
		mutate(&bad)
		if _, _, err := CanonicalCollectionPolicyRequest(bad); err == nil {
			t.Fatal("scope/category mismatch admitted")
		}
	}
}

func TestCollectionPolicySortedResultsAndLimits(t *testing.T) {
	op := collectionTestPolicy(t)
	op.Changes = make([]CollectionGrantChange, 16)
	// Fixed increasing hex suffixes also exercise the complete result order.
	suffixes := []string{"00000000000000000000000000000001", "00000000000000000000000000000002", "00000000000000000000000000000003", "00000000000000000000000000000004", "00000000000000000000000000000005", "00000000000000000000000000000006", "00000000000000000000000000000007", "00000000000000000000000000000008", "00000000000000000000000000000009", "0000000000000000000000000000000a", "0000000000000000000000000000000b", "0000000000000000000000000000000c", "0000000000000000000000000000000d", "0000000000000000000000000000000e", "0000000000000000000000000000000f", "00000000000000000000000000000010"}
	record := collectionTestPolicyRecord(t)
	record.Results = make([]CollectionGrantResult, 16)
	for i, s := range suffixes {
		op.Changes[i] = collectionTestPut()
		op.Changes[i].ID = CollectionGrantID("cgr_" + s)
		record.Results[i] = CollectionGrantResult{op.Changes[i].ID, collectionTestGrantVersion, false}
	}
	record.Request = op
	if _, _, err := CanonicalCollectionPolicyRecord(record); err != nil {
		t.Fatalf("16 grants: %v", err)
	}
	bad := record
	bad.Results = append([]CollectionGrantResult(nil), record.Results...)
	bad.Results[0], bad.Results[1] = bad.Results[1], bad.Results[0]
	if _, _, err := CanonicalCollectionPolicyRecord(bad); err == nil {
		t.Fatal("results reordered")
	}
	op.Changes = append(op.Changes, collectionTestPut())
	if _, _, err := CanonicalCollectionPolicyRequest(op); err == nil {
		t.Fatal("17 grants admitted")
	}
	op = collectionTestPolicy(t)
	op.Changes = append(op.Changes, op.Changes[0])
	if _, _, err := CanonicalCollectionPolicyRequest(op); err == nil {
		t.Fatal("duplicate grant admitted")
	}
	op = collectionTestPolicy(t)
	op.Changes[0].Value.Actions = []string{"collections.read", "collections.edit"}
	if _, _, err := CanonicalCollectionPolicyRequest(op); err == nil {
		t.Fatal("unsorted actions admitted")
	}
}

func TestCollectionSecurityRemovalCostMatchesReceipt(t *testing.T) {
	op := collectionTestPolicy(t)
	op.Category = "security_remove"
	value := collectionTestGrantValue()
	op.Changes = []CollectionGrantChange{{"remove", collectionTestGrant, &ExpectedCollectionGrant{value, collectionTestGrantVersion, false}, nil}}
	for _, library := range []bool{false, true} {
		t.Run(map[bool]string{false: "collection", true: "library"}[library], func(t *testing.T) {
			request := op
			if library {
				request.CollectionID = ""
				v := CollectionGrantValue{collectionTestSubject, CollectionGrantScope{"library_create", ""}, []string{"collections.create"}}
				request.Changes = []CollectionGrantChange{{"remove", collectionTestGrant, &ExpectedCollectionGrant{v, collectionTestGrantVersion, false}, nil}}
			}
			receipt := collectionTestPolicyReceipt(t, request)
			cost, err := collectionSecurityRemovalCost(request, receipt)
			if err != nil || cost.RequestBytes <= 0 || cost.ReceiptBytes <= 0 || cost.RequestBytes > 2048 || cost.ReceiptBytes > 1024 || cost.RequestBytes+cost.ReceiptBytes > 3072 || cost.PolicyEvents != 1 || cost.PolicyReceipts != 1 {
				t.Fatalf("security cost: %+v %v", cost, err)
			}
			for _, mutate := range []func(*CollectionPolicyReceipt){
				func(r *CollectionPolicyReceipt) { r.PayloadSHA256 = strings.Repeat("a", 64) },
				func(r *CollectionPolicyReceipt) { r.OperationID = "cpo_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" },
				func(r *CollectionPolicyReceipt) { r.LibraryID = "lib_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" },
				func(r *CollectionPolicyReceipt) { r.RecordedUnixNano = 0 },
			} {
				bad := receipt
				mutate(&bad)
				if _, err := collectionSecurityRemovalCost(request, bad); err == nil {
					t.Fatal("mismatched receipt accepted")
				}
			}
		})
	}
	receipt := collectionTestPolicyReceipt(t, op)
	bad := op
	bad.Category = "collection"
	if _, err := collectionSecurityRemovalCost(bad, receipt); err == nil {
		t.Fatal("ordinary policy accepted as security removal")
	}
	bad = op
	bad.Changes = append([]CollectionGrantChange(nil), op.Changes...)
	bad.Changes[0].Value = &value
	if _, _, err := CanonicalCollectionPolicyRequest(bad); err == nil {
		t.Fatal("security widening admitted")
	}
	bad = op
	bad.Changes = append(bad.Changes, bad.Changes[0])
	if _, _, err := CanonicalCollectionPolicyRequest(bad); err == nil {
		t.Fatal("multi-removal security admitted")
	}
}

func TestCollectionCredentialV2Separation(t *testing.T) {
	request := collectionTestAdmin(t)
	b, digest, err := CanonicalAdminRequestV2(request)
	if err != nil || len(digest) != 64 {
		t.Fatalf("all9 issuance: %v", err)
	}
	decoded, err := DecodeAdminRequestV2(b)
	if err != nil || !reflect.DeepEqual(decoded, request) {
		t.Fatalf("issuance round trip: %v", err)
	}
	// v1 cannot acquire collection actions just by changing the envelope version.
	old := AdminRequest{1, request.LibraryID, request.ID, request.ExpectedIncarnation, request.Kind, request.Value}
	if _, _, err := CanonicalAdminRequest(old); err == nil {
		t.Fatal("v1 collection ceiling admitted")
	}
	if _, err := DecodeAdminRequest(bytes.Replace(b, []byte(`"version":2`), []byte(`"version":1`), 1)); err == nil {
		t.Fatal("v1 decoded collection actions")
	}
	for _, mutate := range []func(*IssueCredentialRequestV2){
		func(v *IssueCredentialRequestV2) { v.Scope = Scope{"asset", collectionTestAsset} },
		func(v *IssueCredentialRequestV2) { v.Actions = []string{"collections.read", "collections.admin"} },
		func(v *IssueCredentialRequestV2) { v.Actions = []string{"collections.read", "collections.read"} },
		func(v *IssueCredentialRequestV2) { v.Actions = []string{"collections.unknown"} },
		func(v *IssueCredentialRequestV2) { v.Actions = nil },
		func(v *IssueCredentialRequestV2) { v.ExpiresUnix = 0 },
	} {
		var value IssueCredentialRequestV2
		if err := json.Unmarshal(request.Value, &value); err != nil {
			t.Fatal(err)
		}
		mutate(&value)
		bad := request
		bad.Value = collectionTestRaw(t, value)
		if _, _, err := CanonicalAdminRequestV2(bad); err == nil {
			t.Fatal("invalid v2 issuance admitted")
		}
	}
	bad := request
	bad.Kind = "recover"
	if _, _, err := CanonicalAdminRequestV2(bad); err == nil {
		t.Fatal("v2 generalized beyond issuance")
	}
	// Existing v1 action set remains independently usable.
	value := IssueCredentialRequest{collectionTestSubject, Scope{"library", continuity.AssetID("")}, []string{"assets.discover"}, 1800000000}
	old.Value = collectionTestRaw(t, value)
	if _, _, err := CanonicalAdminRequest(old); err != nil {
		t.Fatalf("v1 issuance regressed: %v", err)
	}
}

package catalog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const (
	collectionTestLibrary      = continuity.LibraryID("lib_11111111111111111111111111111111")
	collectionTestOperation    = CollectionOperationID("cop_22222222222222222222222222222222")
	collectionTestID           = CollectionID("col_33333333333333333333333333333333")
	collectionTestGrant        = CollectionGrantID("cgr_44444444444444444444444444444444")
	collectionTestEntry        = CollectionEntryID("cent_55555555555555555555555555555555")
	collectionTestAsset        = continuity.AssetID("asset_66666666666666666666666666666666")
	collectionTestVersion      = CollectionVersion("cv_77777777777777777777777777777777")
	collectionTestSubject      = SubjectID("sub_88888888888888888888888888888888")
	collectionTestGrantVersion = CollectionGrantVersion("cgv_99999999999999999999999999999999")
)

func collectionTestRaw(t *testing.T, value any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func collectionTestCreate(t *testing.T) CollectionRequest {
	return CollectionRequest{1, collectionTestLibrary, collectionTestOperation, collectionTestID, "", "create", collectionTestRaw(t, CreateCollection{"A <&é>", collectionTestGrant, []NewCollectionEntry{{collectionTestEntry, collectionTestAsset}}})}
}
func collectionTestRecord(t *testing.T) CollectionRecord {
	op := collectionTestCreate(t)
	return CollectionRecord{1, op, collectionTestSubject, collectionTestVersion, generatedCollectionPolicyID(op.ID), 123}
}
func collectionTestContentReceipt() CollectionReceipt {
	return CollectionReceipt{1, collectionTestOperation, collectionTestID, collectionTestVersion}
}

func TestCollectionCanonicalContentLiteral(t *testing.T) {
	op := collectionTestCreate(t)
	const literal = `{"version":1,"library_id":"lib_11111111111111111111111111111111","operation_id":"cop_22222222222222222222222222222222","collection_id":"col_33333333333333333333333333333333","expected_version":"","kind":"create","value":{"name":"A \u003c\u0026é\u003e","creator_grant_id":"cgr_44444444444444444444444444444444","entries":[{"entry_id":"cent_55555555555555555555555555555555","asset_id":"asset_66666666666666666666666666666666"}]}}`
	b, digest, err := CanonicalCollectionRequest(op)
	if err != nil || string(b) != literal {
		t.Fatalf("canonical content: %s %v", b, err)
	}
	sum := sha256.Sum256(append([]byte("juicemount.catalog.collection.operation\x00\x01"), []byte(literal)...))
	if digest != hex.EncodeToString(sum[:]) {
		t.Fatalf("content digest: %s", digest)
	}
	decoded, err := DecodeCollectionRequest(b)
	if err != nil || !reflect.DeepEqual(decoded, op) {
		t.Fatalf("content round trip: %v", err)
	}
	// Encoded output and decoded RawMessage cannot alias borrowed input storage.
	op.Value[0] = '['
	if b[0] != '{' || decoded.Value[0] != '{' {
		t.Fatal("canonical output aliases input")
	}
	for i := range b {
		b[i] = 'x'
	}
	if decoded.Value[0] != '{' {
		t.Fatal("decoded value aliases input")
	}
}

func TestCollectionIdentityGrammar(t *testing.T) {
	validators := []struct {
		prefix   string
		validate func(string) error
	}{
		{"col_", func(s string) error { return CollectionID(s).Validate() }},
		{"cent_", func(s string) error { return CollectionEntryID(s).Validate() }},
		{"cv_", func(s string) error { return CollectionVersion(s).Validate() }},
		{"cgr_", func(s string) error { return CollectionGrantID(s).Validate() }},
		{"cgv_", func(s string) error { return CollectionGrantVersion(s).Validate() }},
		{"cop_", func(s string) error { return CollectionOperationID(s).Validate() }},
		{"cpo_", func(s string) error { return CollectionPolicyOperationID(s).Validate() }},
	}
	for _, v := range validators {
		t.Run(v.prefix, func(t *testing.T) {
			if v.validate(v.prefix+strings.Repeat("a", 32)) != nil {
				t.Fatal("valid identity refused")
			}
			for _, bad := range []string{"", v.prefix + strings.Repeat("0", 32), v.prefix + strings.Repeat("A", 32), v.prefix + strings.Repeat("a", 31), v.prefix + strings.Repeat("a", 33), v.prefix + strings.Repeat("a", 31) + "g", v.prefix + strings.Repeat("a", 31) + "é", "x" + v.prefix + strings.Repeat("a", 32)} {
				if v.validate(bad) == nil {
					t.Errorf("invalid identity admitted: %q", bad)
				}
			}
		})
	}
}

func TestCollectionContentVariantsAndRefusal(t *testing.T) {
	base := collectionTestCreate(t)
	cases := []struct {
		kind  string
		value any
	}{
		{"create", CreateCollection{"Empty", collectionTestGrant, []NewCollectionEntry{}}},
		{"rename", RenameCollection{"Renamed"}},
		{"entries", EditCollectionEntries{[]CollectionEntryChange{{"remove", collectionTestEntry, collectionTestAsset}}}},
		{"archive", SetCollectionArchived{false}},
		{"grants", EditCollectionGrants{[]CollectionGrantChange{collectionTestPut()}}},
	}
	for _, c := range cases {
		t.Run(c.kind, func(t *testing.T) {
			op := base
			op.Kind = c.kind
			op.Value = collectionTestRaw(t, c.value)
			if c.kind != "create" {
				op.ExpectedVersion = collectionTestVersion
			}
			b, _, err := CanonicalCollectionRequest(op)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = DecodeCollectionRequest(b); err != nil {
				t.Fatal(err)
			}
		})
	}
	mutations := []struct {
		name   string
		change func(*CollectionRequest)
	}{
		{"chosen actor", func(op *CollectionRequest) { op.Value = []byte(`{"actor_subject_id":"chosen"}`) }},
		{"create expected", func(op *CollectionRequest) { op.ExpectedVersion = collectionTestVersion }},
		{"other version", func(op *CollectionRequest) { op.Version = 2 }},
		{"unknown kind", func(op *CollectionRequest) { op.Kind = "replace" }},
		{"nil entries", func(op *CollectionRequest) {
			op.Value = collectionTestRaw(t, CreateCollection{"Name", collectionTestGrant, nil})
		}},
		{"empty name", func(op *CollectionRequest) {
			op.Value = collectionTestRaw(t, CreateCollection{"", collectionTestGrant, []NewCollectionEntry{}})
		}},
		{"oversized name", func(op *CollectionRequest) {
			op.Value = collectionTestRaw(t, CreateCollection{strings.Repeat("é", 129), collectionTestGrant, []NewCollectionEntry{}})
		}},
		{"nul name", func(op *CollectionRequest) {
			op.Value = collectionTestRaw(t, CreateCollection{"a\x00b", collectionTestGrant, []NewCollectionEntry{}})
		}},
		{"invalid utf8", func(op *CollectionRequest) {
			op.Value = []byte("{\"name\":\"\xff\",\"creator_grant_id\":\"" + string(collectionTestGrant) + "\",\"entries\":[]}")
		}},
		{"duplicate assets", func(op *CollectionRequest) {
			op.Value = collectionTestRaw(t, CreateCollection{"Name", collectionTestGrant, []NewCollectionEntry{{collectionTestEntry, collectionTestAsset}, {CollectionEntryID("cent_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), collectionTestAsset}}})
		}},
		{"unordered entries", func(op *CollectionRequest) {
			op.Value = collectionTestRaw(t, CreateCollection{"Name", collectionTestGrant, []NewCollectionEntry{{CollectionEntryID("cent_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), continuity.AssetID("asset_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")}, {collectionTestEntry, collectionTestAsset}}})
		}},
		{"duplicate delta", func(op *CollectionRequest) {
			op.Kind = "entries"
			op.ExpectedVersion = collectionTestVersion
			op.Value = collectionTestRaw(t, EditCollectionEntries{[]CollectionEntryChange{{"remove", collectionTestEntry, collectionTestAsset}, {"reactivate", collectionTestEntry, collectionTestAsset}}})
		}},
		{"noncreate missing expected", func(op *CollectionRequest) {
			op.Kind = "rename"
			op.Value = collectionTestRaw(t, RenameCollection{"New"})
		}},
	}
	for _, m := range mutations {
		t.Run(m.name, func(t *testing.T) {
			op := base
			m.change(&op)
			if _, _, err := CanonicalCollectionRequest(op); err == nil {
				t.Fatal("invalid content admitted")
			}
		})
	}
}

func TestCollectionRecordsFreshVersionsAndBindings(t *testing.T) {
	record := collectionTestRecord(t)
	b, _, err := CanonicalCollectionRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeCollectionRecord(b)
	if err != nil || !reflect.DeepEqual(got, record) {
		t.Fatalf("record round trip: %v", err)
	}
	for _, change := range []func(*CollectionRecord){
		func(r *CollectionRecord) { r.RecordedUnixNano = 0 },
		func(r *CollectionRecord) { r.ActorSubjectID = "" },
		func(r *CollectionRecord) { r.GeneratedPolicyOperationID = "" },
		func(r *CollectionRecord) { r.GeneratedPolicyOperationID = "cpo_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" },
		func(r *CollectionRecord) { r.Version = 2 },
		func(r *CollectionRecord) {
			r.Request.Kind = "rename"
			r.Request.ExpectedVersion = r.ResultVersion
			r.Request.Value = collectionTestRaw(t, RenameCollection{"New"})
			r.GeneratedPolicyOperationID = ""
		},
	} {
		bad := record
		change(&bad)
		if _, _, err := CanonicalCollectionRecord(bad); err == nil {
			t.Fatal("invalid record admitted")
		}
	}
	receipt := collectionTestContentReceipt()
	encoded, err := CanonicalCollectionReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	const expected = `{"version":1,"operation_id":"cop_22222222222222222222222222222222","collection_id":"col_33333333333333333333333333333333","result_version":"cv_77777777777777777777777777777777"}`
	if string(encoded) != expected {
		t.Fatalf("receipt shape: %s", encoded)
	}
	if decoded, err := DecodeCollectionReceipt(encoded); err != nil || decoded != receipt {
		t.Fatalf("receipt round trip: %v", err)
	}
}

func TestCollectionDecodersCanonicalAndZeroOnError(t *testing.T) {
	request := collectionTestCreate(t)
	record := collectionTestRecord(t)
	receipt := collectionTestContentReceipt()
	policy := collectionTestPolicy(t)
	policyRecord := collectionTestPolicyRecord(t)
	policyReceipt := collectionTestPolicyReceipt(t, policy)
	admin := collectionTestAdmin(t)
	enc := func(b []byte, _ string, err error) []byte {
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	encReceipt := func(b []byte, err error) []byte {
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	cases := []struct {
		name   string
		data   []byte
		zero   any
		decode func([]byte) (any, error)
	}{
		{"content", enc(CanonicalCollectionRequest(request)), CollectionRequest{}, func(b []byte) (any, error) { return DecodeCollectionRequest(b) }},
		{"record", enc(CanonicalCollectionRecord(record)), CollectionRecord{}, func(b []byte) (any, error) { return DecodeCollectionRecord(b) }},
		{"receipt", encReceipt(CanonicalCollectionReceipt(receipt)), CollectionReceipt{}, func(b []byte) (any, error) { return DecodeCollectionReceipt(b) }},
		{"policy", enc(CanonicalCollectionPolicyRequest(policy)), CollectionPolicyRequest{}, func(b []byte) (any, error) { return DecodeCollectionPolicyRequest(b) }},
		{"policy record", enc(CanonicalCollectionPolicyRecord(policyRecord)), CollectionPolicyRecord{}, func(b []byte) (any, error) { return DecodeCollectionPolicyRecord(b) }},
		{"policy receipt", encReceipt(CanonicalCollectionPolicyReceipt(policyReceipt)), CollectionPolicyReceipt{}, func(b []byte) (any, error) { return DecodeCollectionPolicyReceipt(b) }},
		{"admin v2", enc(CanonicalAdminRequestV2(admin)), AdminRequestV2{}, func(b []byte) (any, error) { return DecodeAdminRequestV2(b) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			data := string(c.data)
			version := "1"
			if strings.HasPrefix(data, `{"version":2,`) {
				version = "2"
			}
			bad := []string{"", data + "\n", data + "{}", " " + data, strings.Replace(data, `"version":`+version+`,`, "", 1), strings.Replace(data, `"version":`+version, `"version":`+version+`,"version":`+version, 1), strings.Replace(data, `"version":`+version, `"version":`+version+`.0`, 1), data[:len(data)-1] + `,"unknown":"DO_NOT_ECHO"}`, strings.Repeat("x", MaxCollectionPolicyRecordBytes+1)}
			for _, raw := range bad {
				got, err := c.decode([]byte(raw))
				if err == nil || !reflect.DeepEqual(got, c.zero) {
					t.Fatalf("invalid decode not zero: %T %v", got, err)
				}
				if strings.Contains(err.Error(), "DO_NOT_ECHO") {
					t.Fatal("input reflected in error")
				}
			}
			// Moving version to the end preserves JSON meaning but not canonical order.
			reordered := `{` + strings.TrimPrefix(data, `{"version":`+version+`,`)
			reordered = reordered[:len(reordered)-1] + `,"version":` + version + `}`
			if got, err := c.decode([]byte(reordered)); err == nil || !reflect.DeepEqual(got, c.zero) {
				t.Fatal("field-order change admitted")
			}
		})
	}
	// Required empty array and alternate escapes stay distinguishable.
	empty := collectionTestCreate(t)
	empty.Value = collectionTestRaw(t, CreateCollection{"é", collectionTestGrant, []NewCollectionEntry{}})
	data, _, _ := CanonicalCollectionRequest(empty)
	for _, raw := range [][]byte{bytes.Replace(data, []byte(`"entries":[]`), []byte(`"entries":null`), 1), bytes.Replace(data, []byte(`"name":"é"`), []byte(`"name":"\u00e9"`), 1)} {
		if _, err := DecodeCollectionRequest(raw); err == nil {
			t.Fatal("noncanonical array/escape admitted")
		}
	}
}

func TestCollectionEntryBatchBoundaries(t *testing.T) {
	entries := make([]NewCollectionEntry, 64)
	changes := make([]CollectionEntryChange, 64)
	for i := range entries {
		entries[i] = NewCollectionEntry{CollectionEntryID(fmt.Sprintf("cent_%032x", i+1)), continuity.AssetID(fmt.Sprintf("asset_%032x", i+1))}
		changes[i] = CollectionEntryChange{"add", entries[i].ID, entries[i].AssetID}
	}
	create := collectionTestCreate(t)
	create.Value = collectionTestRaw(t, CreateCollection{strings.Repeat("é", 128), collectionTestGrant, entries})
	if _, _, err := CanonicalCollectionRequest(create); err != nil {
		t.Fatalf("64 create entries/256B name: %v", err)
	}
	entries = append(entries, NewCollectionEntry{"cent_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "asset_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	create.Value = collectionTestRaw(t, CreateCollection{"Name", collectionTestGrant, entries})
	if _, _, err := CanonicalCollectionRequest(create); err == nil {
		t.Fatal("65 create entries admitted")
	}
	edit := collectionTestCreate(t)
	edit.Kind = "entries"
	edit.ExpectedVersion = collectionTestVersion
	edit.Value = collectionTestRaw(t, EditCollectionEntries{changes})
	if _, _, err := CanonicalCollectionRequest(edit); err != nil {
		t.Fatalf("64 deltas: %v", err)
	}
	changes = append(changes, CollectionEntryChange{"add", "cent_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "asset_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	edit.Value = collectionTestRaw(t, EditCollectionEntries{changes})
	if _, _, err := CanonicalCollectionRequest(edit); err == nil {
		t.Fatal("65 deltas admitted")
	}
}

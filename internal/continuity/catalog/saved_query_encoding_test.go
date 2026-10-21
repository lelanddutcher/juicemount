package catalog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func requireSavedQueryConstructorError(t *testing.T, value any) {
	t.Helper()
	b, digest, err := canonicalSavedQueryForTest(t, value)
	if !errors.Is(err, ErrInvalid) || b != nil || digest != "" {
		t.Fatalf("expected ErrInvalid, nil bytes and empty digest for %T: %v", value, err)
	}
}

func TestSavedQueryDefinitionConstructorRefusals(t *testing.T) {
	var base SavedQueryDefinition
	savedQueryLiteral(t, "definition_nonempty", &base)
	cases := []struct {
		name   string
		change func(*SavedQueryDefinition)
	}{
		{"version", func(d *SavedQueryDefinition) { d.Version = 2 }},
		{"kind", func(d *SavedQueryDefinition) { d.Kind = "manual" }},
		{"nil_anchors", func(d *SavedQueryDefinition) { d.ContextAssetIDs = nil }},
		{"invalid_utf8_text", func(d *SavedQueryDefinition) { d.Text = "\xff" }},
		{"nul_text", func(d *SavedQueryDefinition) { d.Text = "a\x00b" }},
		{"ascii_257", func(d *SavedQueryDefinition) { d.Text = strings.Repeat("a", 257) }},
		{"utf8_258", func(d *SavedQueryDefinition) { d.Text = strings.Repeat("é", 129) }},
		{"invalid_anchor", func(d *SavedQueryDefinition) { d.ContextAssetIDs[0] = "asset_00000000000000000000000000000000" }},
		{"duplicate_anchor", func(d *SavedQueryDefinition) { d.ContextAssetIDs[1] = d.ContextAssetIDs[0] }},
		{"unsorted_anchors", func(d *SavedQueryDefinition) {
			d.ContextAssetIDs[0], d.ContextAssetIDs[1] = d.ContextAssetIDs[1], d.ContextAssetIDs[0]
		}},
		{"anchors_65", func(d *SavedQueryDefinition) {
			d.ContextAssetIDs = make([]continuity.AssetID, 65)
			for i := range d.ContextAssetIDs {
				d.ContextAssetIDs[i] = continuity.AssetID(fmt.Sprintf("asset_%032x", i+1))
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := base
			d.ContextAssetIDs = append([]continuity.AssetID(nil), base.ContextAssetIDs...)
			c.change(&d)
			before := d
			if d.ContextAssetIDs != nil {
				before.ContextAssetIDs = append([]continuity.AssetID{}, d.ContextAssetIDs...)
			}
			requireSavedQueryConstructorError(t, d)
			if !reflect.DeepEqual(d, before) {
				t.Fatal("constructor mutated definition")
			}
		})
	}
}

func TestSavedQueryRequestConstructorRefusals(t *testing.T) {
	var base SavedQueryRequest
	savedQueryLiteral(t, "request_create_empty", &base)
	cases := []struct {
		name   string
		change func(*SavedQueryRequest)
	}{
		{"version1", func(q *SavedQueryRequest) { q.Version = 1 }},
		{"library", func(q *SavedQueryRequest) { q.LibraryID = "" }},
		{"operation", func(q *SavedQueryRequest) { q.ID = "cop_00000000000000000000000000000000" }},
		{"collection", func(q *SavedQueryRequest) { q.CollectionID = "bad" }},
		{"create_expected_version", func(q *SavedQueryRequest) { q.ExpectedVersion = "cv_55555555555555555555555555555555" }},
		{"unknown_kind", func(q *SavedQueryRequest) { q.Kind = "unknown" }},
		{"manual_entries", func(q *SavedQueryRequest) {
			q.Kind = "entries"
			q.ExpectedVersion = "cv_55555555555555555555555555555555"
			q.Value = []byte(`{"changes":[]}`)
		}},
		{"nil_value", func(q *SavedQueryRequest) { q.Value = nil }},
		{"null_value", func(q *SavedQueryRequest) { q.Value = []byte(`null`) }},
		{"duplicate_member", func(q *SavedQueryRequest) {
			q.Value = bytes.Replace(q.Value, []byte(`"name":"Work"`), []byte(`"name":"Work","name":"Work"`), 1)
		}},
		{"value_whitespace", func(q *SavedQueryRequest) { q.Value = append([]byte(" "), q.Value...) }},
		{"value_over_cap", func(q *SavedQueryRequest) { q.Value = bytes.Repeat([]byte("x"), MaxSavedQueryRequestBytes+1) }},
		{"invalid_utf8_name", func(q *SavedQueryRequest) { q.Value = bytes.Replace(q.Value, []byte("Work"), []byte{0xff}, 1) }},
		{"empty_name", func(q *SavedQueryRequest) { q.Value = bytes.Replace(q.Value, []byte(`"Work"`), []byte(`""`), 1) }},
		{"name_257", func(q *SavedQueryRequest) {
			q.Value = bytes.Replace(q.Value, []byte("Work"), []byte(strings.Repeat("a", 257)), 1)
		}},
		{"invalid_creator_grant", func(q *SavedQueryRequest) {
			q.Value = bytes.Replace(q.Value, []byte("cgr_66666666666666666666666666666666"), []byte("cgr_00000000000000000000000000000000"), 1)
		}},
		{"null_definition_anchors", func(q *SavedQueryRequest) {
			q.Value = bytes.Replace(q.Value, []byte(`"context_asset_ids":[]`), []byte(`"context_asset_ids":null`), 1)
		}},
		{"replacement_missing_definition", func(q *SavedQueryRequest) {
			q.Kind = "replace_definition"
			q.ExpectedVersion = "cv_55555555555555555555555555555555"
			q.Value = []byte(`{}`)
		}},
		{"rename_missing_expected", func(q *SavedQueryRequest) { q.Kind = "rename"; q.Value = []byte(`{"name":"New"}`) }},
		{"archive_missing_boolean", func(q *SavedQueryRequest) {
			q.Kind = "archive"
			q.ExpectedVersion = "cv_55555555555555555555555555555555"
			q.Value = []byte(`{}`)
		}},
		{"grants_empty", func(q *SavedQueryRequest) {
			q.Kind = "grants"
			q.ExpectedVersion = "cv_55555555555555555555555555555555"
			q.Value = []byte(`{"changes":[]}`)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := base
			q.Value = append(json.RawMessage(nil), base.Value...)
			c.change(&q)
			before := q
			if q.Value != nil {
				before.Value = append(json.RawMessage{}, q.Value...)
			}
			requireSavedQueryConstructorError(t, q)
			if !reflect.DeepEqual(q, before) {
				t.Fatal("constructor mutated request")
			}
		})
	}
}

func TestSavedQueryRecordConstructorRefusals(t *testing.T) {
	var create, replace SavedQueryRecord
	savedQueryLiteral(t, "record_create_empty", &create)
	savedQueryLiteral(t, "record_replace_nonempty", &replace)
	cases := []struct {
		name   string
		base   SavedQueryRecord
		change func(*SavedQueryRecord)
	}{
		{"outer_version", create, func(r *SavedQueryRecord) { r.Version = 1 }},
		{"nested_version", create, func(r *SavedQueryRecord) { r.Request.Version = 1 }},
		{"invalid_request", create, func(r *SavedQueryRecord) { r.Request.Value = []byte(`null`) }},
		{"actor", create, func(r *SavedQueryRecord) { r.ActorSubjectID = "" }},
		{"result", create, func(r *SavedQueryRecord) { r.ResultVersion = "cv_00000000000000000000000000000000" }},
		{"same_result", replace, func(r *SavedQueryRecord) { r.ResultVersion = r.Request.ExpectedVersion }},
		{"zero_time", create, func(r *SavedQueryRecord) { r.RecordedUnixNano = 0 }},
		{"negative_time", create, func(r *SavedQueryRecord) { r.RecordedUnixNano = -1 }},
		{"missing_child", create, func(r *SavedQueryRecord) { r.GeneratedPolicyOperationID = "" }},
		{"wrong_child", create, func(r *SavedQueryRecord) { r.GeneratedPolicyOperationID = "cpo_88888888888888888888888888888888" }},
		{"unexpected_child", replace, func(r *SavedQueryRecord) { r.GeneratedPolicyOperationID = "cpo_88888888888888888888888888888888" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := c.base
			r.Request.Value = append(json.RawMessage(nil), c.base.Request.Value...)
			c.change(&r)
			before := r
			before.Request.Value = append(json.RawMessage(nil), r.Request.Value...)
			requireSavedQueryConstructorError(t, r)
			if !reflect.DeepEqual(r, before) {
				t.Fatal("constructor mutated record")
			}
		})
	}
}

func TestSavedQueryReceiptConstructorRefusals(t *testing.T) {
	var base SavedQueryReceipt
	savedQueryLiteral(t, "receipt_create", &base)
	cases := []struct {
		name   string
		change func(*SavedQueryReceipt)
	}{
		{"version", func(r *SavedQueryReceipt) { r.Version = 1 }},
		{"operation", func(r *SavedQueryReceipt) { r.OperationID = "" }},
		{"collection", func(r *SavedQueryReceipt) { r.CollectionID = "col_00000000000000000000000000000000" }},
		{"result", func(r *SavedQueryReceipt) { r.ResultVersion = "bad" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := base
			c.change(&r)
			before := r
			requireSavedQueryConstructorError(t, r)
			if r != before {
				t.Fatal("constructor mutated receipt")
			}
		})
	}
}

func mutateSavedQueryStorageForTest(value any) {
	switch v := value.(type) {
	case SavedQueryDefinition:
		v.ContextAssetIDs[0] = "asset_cccccccccccccccccccccccccccccccc"
	case SavedQueryRequest:
		v.Value[0] = '!'
	case SavedQueryRecord:
		v.Request.Value[0] = '!'
	}
}

func TestSavedQueryCodecOwnership(t *testing.T) {
	cases := []struct {
		codec, literal string
		zero           any
	}{
		{"definition", "definition_nonempty", SavedQueryDefinition{}},
		{"request", "request_create_empty", SavedQueryRequest{}},
		{"record", "record_create_empty", SavedQueryRecord{}},
		{"receipt", "receipt_create", SavedQueryReceipt{}},
	}
	for _, c := range cases {
		t.Run(c.codec, func(t *testing.T) {
			wantPointer := reflect.New(reflect.TypeOf(c.zero))
			literal := savedQueryLiteral(t, c.literal, wantPointer.Interface())
			want := wantPointer.Elem().Interface()
			input := append([]byte(nil), literal...)
			first, err := decodeSavedQueryForTest(t, c.codec, input)
			if err != nil {
				t.Fatal(err)
			}
			second, err := decodeSavedQueryForTest(t, c.codec, input)
			if err != nil {
				t.Fatal(err)
			}
			encoded, _, err := canonicalSavedQueryForTest(t, want)
			if err != nil || !bytes.Equal(encoded, literal) {
				t.Fatal("owned canonical output")
			}
			for i := range input {
				input[i] = 'x'
			}
			if !reflect.DeepEqual(first, want) || !reflect.DeepEqual(second, want) {
				t.Fatal("decoded data aliases input bytes")
			}
			mutateSavedQueryStorageForTest(first)
			if !reflect.DeepEqual(second, want) || !bytes.Equal(encoded, literal) {
				t.Fatal("decoded storage aliases another decode or encoding")
			}
			mutateSavedQueryStorageForTest(want)
			if !bytes.Equal(encoded, literal) {
				t.Fatal("encoded output aliases constructor argument")
			}
			// Snapshot the mutated argument independently before changing output bytes.
			argumentBefore := fmt.Sprintf("%#v", want)
			for i := range encoded {
				encoded[i] = 'y'
			}
			if fmt.Sprintf("%#v", want) != argumentBefore {
				t.Fatal("constructor argument aliases returned bytes")
			}
			untouchedPointer := reflect.New(reflect.TypeOf(c.zero))
			if err := json.Unmarshal(literal, untouchedPointer.Interface()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(second, untouchedPointer.Elem().Interface()) {
				t.Fatal("other returned value was mutated")
			}
		})
	}
}

func TestSavedQueryDecoderByteBounds(t *testing.T) {
	cases := []struct {
		codec, positive string
		limit           int
	}{
		{"definition", "definition_empty", MaxSavedQueryDefinitionBytes},
		{"request", "request_create_empty", MaxSavedQueryRequestBytes},
		{"record", "record_create_empty", MaxSavedQueryRecordBytes},
		{"receipt", "receipt_create", MaxSavedQueryReceiptBytes},
	}
	if MaxSavedQueryDefinitionBytes != 8192 || MaxSavedQueryRequestBytes != 32768 || MaxSavedQueryRecordBytes != 33792 || MaxSavedQueryReceiptBytes != 1024 {
		t.Fatal("codec byte caps changed")
	}
	for _, c := range cases {
		t.Run(c.codec, func(t *testing.T) {
			if _, err := decodeSavedQueryForTest(t, c.codec, savedQueryLiteral(t, c.positive, nil)); err != nil {
				t.Fatal(err)
			}
			for _, n := range []int{c.limit, c.limit + 1} {
				t.Run(fmt.Sprintf("malformed_%d", n), func(t *testing.T) {
					input := bytes.Repeat([]byte("x"), n)
					value, err := decodeSavedQueryForTest(t, c.codec, input)
					requireSavedQueryZeroError(t, value, err)
					if !bytes.Equal(input, bytes.Repeat([]byte("x"), n)) {
						t.Fatal("decoder modified oversize input")
					}
				})
			}
		})
	}
	// Field/count limits may make a valid exact-cap object impossible. These
	// refusal cases do not by themselves causally isolate the raw size guard.
}

func TestSavedQueryManualFamilyIsolation(t *testing.T) {
	// Fixed v1 literal from the existing manual content golden, not produced by
	// either codec under test. Its record and minimal receipt retain v1 fields.
	const manualRequest = `{"version":1,"library_id":"lib_11111111111111111111111111111111","operation_id":"cop_22222222222222222222222222222222","collection_id":"col_33333333333333333333333333333333","expected_version":"","kind":"create","value":{"name":"A \u003c\u0026é\u003e","creator_grant_id":"cgr_44444444444444444444444444444444","entries":[{"entry_id":"cent_55555555555555555555555555555555","asset_id":"asset_66666666666666666666666666666666"}]}}`
	const manualRecord = `{"version":1,"request":` + manualRequest + `,"actor_subject_id":"sub_88888888888888888888888888888888","result_version":"cv_77777777777777777777777777777777","generated_policy_operation_id":"cpo_22222222222222222222222222222222","recorded_unix_nano":123}`
	const manualReceipt = `{"version":1,"operation_id":"cop_22222222222222222222222222222222","collection_id":"col_33333333333333333333333333333333","result_version":"cv_77777777777777777777777777777777"}`
	cases := []struct{ codec, manual, saved string }{
		{"request", manualRequest, "request_create_empty"},
		{"record", manualRecord, "record_create_empty"},
		{"receipt", manualReceipt, "receipt_create"},
	}
	for _, c := range cases {
		t.Run(c.codec, func(t *testing.T) {
			old := []byte(c.manual)
			newBytes := savedQueryLiteral(t, c.saved, nil)
			v, err := decodeSavedQueryForTest(t, c.codec, old)
			requireSavedQueryZeroError(t, v, err)
			if _, err := decodeSavedQueryForTest(t, c.codec, newBytes); err != nil {
				t.Fatal(err)
			}
			switch c.codec {
			case "request":
				v1, err := DecodeCollectionRequest(old)
				if err != nil {
					t.Fatal(err)
				}
				b, _, err := CanonicalCollectionRequest(v1)
				if err != nil || !bytes.Equal(b, old) {
					t.Fatal("manual request bytes changed")
				}
				rejected, err := DecodeCollectionRequest(newBytes)
				requireSavedQueryZeroError(t, rejected, err)
				v, err := DecodeSavedQueryRequest(bytes.Replace(old, []byte(`"version":1`), []byte(`"version":2`), 1))
				requireSavedQueryZeroError(t, v, err)
			case "record":
				v1, err := DecodeCollectionRecord(old)
				if err != nil {
					t.Fatal(err)
				}
				b, _, err := CanonicalCollectionRecord(v1)
				if err != nil || !bytes.Equal(b, old) {
					t.Fatal("manual record bytes changed")
				}
				rejected, err := DecodeCollectionRecord(newBytes)
				requireSavedQueryZeroError(t, rejected, err)
				v, err := DecodeSavedQueryRecord(bytes.ReplaceAll(old, []byte(`"version":1`), []byte(`"version":2`)))
				requireSavedQueryZeroError(t, v, err)
			case "receipt":
				v1, err := DecodeCollectionReceipt(old)
				if err != nil {
					t.Fatal(err)
				}
				b, err := CanonicalCollectionReceipt(v1)
				if err != nil || !bytes.Equal(b, old) {
					t.Fatal("manual receipt bytes changed")
				}
				rejected, err := DecodeCollectionReceipt(newBytes)
				requireSavedQueryZeroError(t, rejected, err)
			}
		})
	}
}

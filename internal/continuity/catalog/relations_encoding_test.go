package catalog

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func testRelationshipValue() RelationshipValue {
	return RelationshipValue{SourceAssetID: testAsset, SourceRevision: rootRevision, TargetAssetID: otherAsset,
		Predicate: "derived-from", Provenance: Claimed, Source: "owner input"}
}

func testLocationValue() LocationValue {
	return LocationValue{AssetID: testAsset, Revision: rootRevision, Source: "NAS <A>", Locator: "opaque:Library/Scene α.mov"}
}

func relationalOperation(id string) Operation {
	return testOperation(id, Change{PutRelationship: &PutRelationship{ID: "edge_scene_001", Value: testRelationshipValue()}},
		Change{PutLocation: &PutLocation{ID: "location_scene_001", Value: testLocationValue()}})
}

func TestRelationalCanonicalFixedVector(t *testing.T) {
	const expected = `{"version":1,"library_id":"lib_000102030405060708090a0b0c0d0e0f","operation_id":"relationship_location_001","actor":"owner-demo","changes":[{"put_relationship":{"id":"edge_scene_001","expected":null,"value":{"source_asset_id":"asset_101112131415161718191a1b1c1d1e1f","source_revision":"rev1_08360b41fa64c32ba1decf26ca750f4d74748870d2b3b9c8b682f22b8ca51b2b","target_asset_id":"asset_202122232425262728292a2b2c2d2e2f","target_revision":"","predicate":"derived-from","provenance":"claimed","source":"owner input"}}},{"put_location":{"id":"location_scene_001","expected":null,"value":{"asset_id":"asset_101112131415161718191a1b1c1d1e1f","revision":"rev1_08360b41fa64c32ba1decf26ca750f4d74748870d2b3b9c8b682f22b8ca51b2b","source":"NAS \u003cA\u003e","locator":"opaque:Library/Scene α.mov"}}}]}`
	data, digest, err := CanonicalRequest(relationalOperation("relationship_location_001"))
	if err != nil || string(data) != expected || digest != "cd69d915bb513e5f1de578e2e63a2585f733ce1e04e642fe75a3f13d01118875" {
		t.Fatalf("canonical %s\ndigest %s, %v", data, digest, err)
	}
	if _, err := decodeOperation(data); err != nil {
		t.Fatal(err)
	}
}

func TestRelationalCanonicalFreezesExpectedPointers(t *testing.T) {
	rExpected := &RelationshipExpected{Value: testRelationshipValue(), UpdatedSequence: 7, Removed: true}
	lExpected := &LocationExpected{Value: testLocationValue(), UpdatedSequence: 8, Removed: true}
	op := testOperation("freeze",
		Change{PutRelationship: &PutRelationship{ID: "r", Expected: rExpected, Value: testRelationshipValue()}},
		Change{RemoveRelationship: &RemoveRelationship{ID: "r", Expected: rExpected}},
		Change{PutLocation: &PutLocation{ID: "l", Expected: lExpected, Value: testLocationValue()}},
		Change{RemoveLocation: &RemoveLocation{ID: "l", Expected: lExpected}})
	frozen, data, _, err := freezeOperation(op)
	if err != nil {
		t.Fatal(err)
	}
	rExpected.Value.Source = "changed"
	lExpected.UpdatedSequence = 99
	op.Changes[0].PutRelationship.Value.Predicate = "changed"
	again, _, err := CanonicalRequest(frozen)
	if err != nil || !bytes.Equal(data, again) {
		t.Fatalf("frozen request aliases input: %v", err)
	}
	if !bytes.Contains(data, []byte(`"expected":{"value":`)) || !bytes.Contains(data, []byte(`"updated_sequence":7,"removed":true`)) {
		t.Fatalf("expected state encoding: %s", data)
	}
	decoded, err := decodeOperation(data)
	if err != nil {
		t.Fatal(err)
	}
	decoded.Changes[0].PutRelationship.Expected.Removed = false
	if frozen.Changes[0].PutRelationship.Expected.Removed != true {
		t.Fatal("decoded pointers alias frozen request")
	}
}

func TestRelationalCanonicalRejectsInvalidValues(t *testing.T) {
	for name, change := range map[string]func(*Operation){
		"id":                func(o *Operation) { o.Changes[0].PutRelationship.ID = "bad/id" },
		"id bound":          func(o *Operation) { o.Changes[1].PutLocation.ID = strings.Repeat("a", 129) },
		"source asset":      func(o *Operation) { o.Changes[0].PutRelationship.Value.SourceAssetID = "asset_bad" },
		"source revision":   func(o *Operation) { o.Changes[0].PutRelationship.Value.SourceRevision = "rev1_bad" },
		"target revision":   func(o *Operation) { o.Changes[0].PutRelationship.Value.TargetRevision = "rev2_bad" },
		"predicate empty":   func(o *Operation) { o.Changes[0].PutRelationship.Value.Predicate = "" },
		"predicate bound":   func(o *Operation) { o.Changes[0].PutRelationship.Value.Predicate = strings.Repeat("a", 129) },
		"provenance":        func(o *Operation) { o.Changes[0].PutRelationship.Value.Provenance = "authenticated" },
		"source empty":      func(o *Operation) { o.Changes[0].PutRelationship.Value.Source = "" },
		"source bound":      func(o *Operation) { o.Changes[1].PutLocation.Value.Source = strings.Repeat("a", 257) },
		"source nul":        func(o *Operation) { o.Changes[0].PutRelationship.Value.Source = "a\x00b" },
		"source UTF8":       func(o *Operation) { o.Changes[1].PutLocation.Value.Source = string([]byte{0xff}) },
		"locator empty":     func(o *Operation) { o.Changes[1].PutLocation.Value.Locator = "" },
		"locator bound":     func(o *Operation) { o.Changes[1].PutLocation.Value.Locator = strings.Repeat("a", 8193) },
		"locator nul":       func(o *Operation) { o.Changes[1].PutLocation.Value.Locator = "x\x00y" },
		"location revision": func(o *Operation) { o.Changes[1].PutLocation.Value.Revision = "rev_bad" },
		"expected zero sequence": func(o *Operation) {
			o.Changes[0].PutRelationship.Expected = &RelationshipExpected{Value: testRelationshipValue()}
		},
		"expected invalid value": func(o *Operation) {
			o.Changes[1].PutLocation.Expected = &LocationExpected{Value: LocationValue{}, UpdatedSequence: 1}
		},
		"remove nil relationship": func(o *Operation) { o.Changes[0] = Change{RemoveRelationship: &RemoveRelationship{ID: "r"}} },
		"remove nil location":     func(o *Operation) { o.Changes[0] = Change{RemoveLocation: &RemoveLocation{ID: "l"}} },
		"multiple variants":       func(o *Operation) { o.Changes[0].PutLocation = &PutLocation{ID: "l", Value: testLocationValue()} },
	} {
		t.Run(name, func(t *testing.T) {
			op := relationalOperation("invalid")
			change(&op)
			if _, _, err := CanonicalRequest(op); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid request: %v", err)
			}
		})
	}
}

func TestRelationalStoredJSONRefusesMissingNullAndExcessFields(t *testing.T) {
	data, _, err := CanonicalRequest(relationalOperation("strict"))
	if err != nil {
		t.Fatal(err)
	}
	for _, malformed := range [][]byte{
		bytes.Replace(data, []byte(`"expected":null,`), nil, 1),
		bytes.Replace(data, []byte(`"target_revision":""`), []byte(`"target_revision":null`), 1),
		bytes.Replace(data, []byte(`"source_revision":`), []byte(`"extra":0,"source_revision":`), 1),
		bytes.Replace(data, []byte(`"put_relationship"`), []byte(`"future_relationship"`), 1),
		bytes.Replace(data, []byte(`"put_relationship"`), []byte(`"remove_relationship"`), 1),
		append(append([]byte(nil), data...), ' '),
	} {
		if _, err := decodeOperation(malformed); !errors.Is(err, ErrFormat) {
			t.Fatalf("accepted JSON: %s, %v", malformed, err)
		}
	}
}

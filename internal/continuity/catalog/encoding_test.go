package catalog

import (
	"bytes"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const testLibrary continuity.LibraryID = "lib_000102030405060708090a0b0c0d0e0f"
const testAsset continuity.AssetID = "asset_101112131415161718191a1b1c1d1e1f"
const otherAsset continuity.AssetID = "asset_202122232425262728292a2b2c2d2e2f"
const otherLibrary continuity.LibraryID = "lib_202122232425262728292a2b2c2d2e2f"
const testSHA = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
const rootRevision continuity.RevisionID = "rev1_08360b41fa64c32ba1decf26ca750f4d74748870d2b3b9c8b682f22b8ca51b2b"

func testManifest(asset continuity.AssetID, digest string, parents ...continuity.RevisionID) continuity.Manifest {
	return continuity.Manifest{Version: 1, LibraryID: testLibrary, AssetID: asset,
		Content: continuity.Content{SHA256: digest, Size: 3}, Parents: parents}
}

func testOperation(id string, changes ...Change) Operation {
	return Operation{Version: 1, LibraryID: testLibrary, ID: id, Actor: "owner-demo", Changes: changes}
}

func initialOperation() Operation {
	m := testManifest(testAsset, testSHA)
	return testOperation("initial",
		Change{CreateAsset: &CreateAsset{AssetID: testAsset, Label: "original"}},
		Change{RecordRevision: &m},
		Change{AdvanceHead: &AdvanceHead{AssetID: testAsset, Revision: rootRevision}})
}

func TestCanonicalOperationFixedVector(t *testing.T) {
	op := initialOperation()
	op.ID = "create_scene_001"
	op.Changes[0].CreateAsset.Label = "Scene <A> & café\u2028next"
	// Independently generated with fixed-order Python JSON and Go-compatible escaping.
	const expected = `{"version":1,"library_id":"lib_000102030405060708090a0b0c0d0e0f","operation_id":"create_scene_001","actor":"owner-demo","changes":[{"create_asset":{"asset_id":"asset_101112131415161718191a1b1c1d1e1f","label":"Scene \u003cA\u003e \u0026 café\u2028next"}},{"record_revision":{"version":1,"library_id":"lib_000102030405060708090a0b0c0d0e0f","asset_id":"asset_101112131415161718191a1b1c1d1e1f","content":{"sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad","size":3},"parents":[]}},{"advance_head":{"asset_id":"asset_101112131415161718191a1b1c1d1e1f","expected_revision":"","revision":"rev1_08360b41fa64c32ba1decf26ca750f4d74748870d2b3b9c8b682f22b8ca51b2b"}}]}`
	data, digest, err := CanonicalRequest(op)
	if err != nil || string(data) != expected || digest != "260069e3c1382a1b20cbb618a247285f2fe32c7a46a62b9e6693dd23a5b63530" {
		t.Fatalf("canonical = %s\ndigest = %s, error %v", data, digest, err)
	}
	decoded, err := decodeOperation(data)
	if err != nil {
		t.Fatal(err)
	}
	again, _, err := CanonicalRequest(decoded)
	if err != nil || !bytes.Equal(data, again) {
		t.Fatalf("roundtrip: %s, %v", again, err)
	}
}

func TestCanonicalOperationCopiesAndNormalizesParents(t *testing.T) {
	first := continuity.RevisionID("rev1_" + strings.Repeat("1", 64))
	second := continuity.RevisionID("rev1_" + strings.Repeat("2", 64))
	m := testManifest(testAsset, testSHA, second, first)
	op := testOperation("parents", Change{RecordRevision: &m})
	frozen, data, _, err := freezeOperation(op)
	if err != nil {
		t.Fatal(err)
	}
	if m.Parents[0] != second {
		t.Fatal("input parents mutated")
	}
	m.Parents[0] = first
	if frozen.Changes[0].RecordRevision.Parents[1] != second {
		t.Fatal("frozen parents alias input")
	}
	m.Parents = []continuity.RevisionID{first, second}
	again, _, err := CanonicalRequest(op)
	if err != nil || !bytes.Equal(data, again) {
		t.Fatalf("parent ordering: %v", err)
	}
	m.Parents = nil
	a, _, err := CanonicalRequest(op)
	if err != nil {
		t.Fatal(err)
	}
	m.Parents = []continuity.RevisionID{}
	b, _, err := CanonicalRequest(op)
	if err != nil || !bytes.Equal(a, b) || !bytes.Contains(a, []byte(`"parents":[]`)) {
		t.Fatalf("empty parents: %s, %v", b, err)
	}
}

func TestCanonicalOperationRejectsInvalidAndExcessInput(t *testing.T) {
	for name, mutate := range map[string]func(*Operation){
		"unknown version":        func(o *Operation) { o.Version = 2 },
		"library":                func(o *Operation) { o.LibraryID = "lib_bad" },
		"operation ID":           func(o *Operation) { o.ID = "not/an/id" },
		"operation ID bound":     func(o *Operation) { o.ID = strings.Repeat("a", 129) },
		"empty actor":            func(o *Operation) { o.Actor = "" },
		"actor bound":            func(o *Operation) { o.Actor = strings.Repeat("a", 257) },
		"actor nul":              func(o *Operation) { o.Actor = "a\x00b" },
		"actor UTF8":             func(o *Operation) { o.Actor = string([]byte{0xff}) },
		"label bound":            func(o *Operation) { o.Changes[0].CreateAsset.Label = strings.Repeat("a", 4097) },
		"label UTF8":             func(o *Operation) { o.Changes[0].CreateAsset.Label = string([]byte{0xff}) },
		"label nul":              func(o *Operation) { o.Changes[0].CreateAsset.Label = "\x00" },
		"no variants":            func(o *Operation) { o.Changes[0] = Change{} },
		"multiple variants":      func(o *Operation) { o.Changes[0].RenameAsset = &RenameAsset{AssetID: testAsset} },
		"no changes":             func(o *Operation) { o.Changes = nil },
		"too many changes":       func(o *Operation) { o.Changes = make([]Change, MaxChanges+1) },
		"cross library manifest": func(o *Operation) { o.Changes[1].RecordRevision.LibraryID = otherLibrary },
		"duplicate parents": func(o *Operation) {
			o.Changes[1].RecordRevision.Parents = []continuity.RevisionID{rootRevision, rootRevision}
		},
		"bad head":          func(o *Operation) { o.Changes[2].AdvanceHead.Revision = "rev2_bad" },
		"bad expected head": func(o *Operation) { o.Changes[2].AdvanceHead.ExpectedRevision = "rev1_bad" },
		"request byte bound": func(o *Operation) {
			o.Changes = make([]Change, MaxChanges)
			for i := range o.Changes {
				o.Changes[i].CreateAsset = &CreateAsset{AssetID: testAsset, Label: strings.Repeat("<", 4096)}
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			op := initialOperation()
			mutate(&op)
			if data, digest, err := CanonicalRequest(op); err == nil || data != nil || digest != "" {
				t.Fatalf("accepted: %s, %s, %v", data, digest, err)
			}
		})
	}
}

func TestStoredOperationStrictDecoding(t *testing.T) {
	canonical, _, err := CanonicalRequest(initialOperation())
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{
		append(append([]byte(nil), canonical...), []byte(`{}`)...),
		append([]byte(" "), canonical...),
		bytes.Replace(canonical, []byte(`"version":1`), []byte(`"version":1,"extra":0`), 1),
		bytes.Replace(canonical, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		bytes.Replace(canonical, []byte(`"parents":[]`), []byte(`"parents":null`), 1),
		bytes.Replace(canonical, []byte(`"create_asset"`), []byte(`"future_kind"`), 1),
		bytes.Replace(canonical, []byte(`"expected_revision":"",`), nil, 1),
	} {
		if _, err := decodeOperation(data); err == nil {
			t.Fatalf("accepted noncanonical JSON: %s", data)
		}
	}
}

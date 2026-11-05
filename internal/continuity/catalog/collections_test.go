package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// A real format 2 catalog: written by the slice-1 catalog code through Create
// and Apply (commit cbc3035c), then dumped. Not produced by this code.
//
//go:embed testdata/continuity-s3-v2-fixture.sql
var v2FixtureSQL string

// v2Fingerprint is format 2's DDL fingerprint as slice 1 recorded it.
const v2Fingerprint = "1d67308e722a028662374a942e4b19bebe836df7d5f903a27a4ac3412e944a6d"

const (
	testCollection  CollectionID = "col_303132333435363738393a3b3c3d3e3f"
	otherCollection CollectionID = "col_404142434445464748494a4b4c4d4e4f"
)

func v2Fixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "v2.sqlite")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	db, err := openDB(path, false)
	if err != nil {
		t.Fatal(err)
	}
	_, execErr := db.Exec(v2FixtureSQL)
	if err := errors.Join(execErr, db.Close()); err != nil {
		t.Fatal(err)
	}
	return path
}

// v2Records captures every format 2 row, ledger included, as detached values.
func v2Records(t *testing.T, q queryer) []byte {
	t.Helper()
	var out []any
	for _, spec := range []struct{ name, order string }{
		{"schema_migrations", "version"}, {"catalog", "singleton"}, {"assets", "asset_id"}, {"revisions", "revision_id"},
		{"revision_parents", "revision_id,parent_id"}, {"events", "sequence"}, {"relationships", "relationship_id"}, {"locations", "location_id"},
	} {
		rows, err := q.QueryContext(context.Background(), "SELECT * FROM "+spec.name+" ORDER BY "+spec.order)
		if err != nil {
			t.Fatal(err)
		}
		columns, _ := rows.Columns()
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			for i, v := range values {
				if b, ok := v.([]byte); ok {
					values[i] = hex.EncodeToString(b)
				}
			}
			out = append(out, spec.name, values)
		}
		rows.Close()
	}
	data, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestCollectionV2FixtureIsFrozen(t *testing.T) {
	digest := sha256.Sum256([]byte(v2FixtureSQL))
	if hex.EncodeToString(digest[:]) != "948355988926951a59a1dc08b0aa57c684247f6789e5457be323ebd816a8d2c1" {
		t.Fatal("frozen format 2 fixture changed")
	}
	if got := schemaFingerprint(schemaV2()); got != v2Fingerprint {
		t.Fatalf("format 2 DDL changed: %s", got)
	}
	lib, version, err := Inspect(context.Background(), v2Fixture(t))
	if err != nil || version != 2 || lib != testLibrary {
		t.Fatalf("format 2 recognition: %s %d %v", lib, version, err)
	}
}

func TestCollectionUpgradeFromSliceOneCatalog(t *testing.T) {
	ctx := context.Background()
	path := v2Fixture(t)
	db, err := openDB(path, true)
	if err != nil {
		t.Fatal(err)
	}
	before := v2Records(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(path); !errors.Is(err, ErrUpgradeRequired) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("format 2 Open: %v", err)
	}
	if err := Upgrade(ctx, path, otherLibrary); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong library upgraded: %v", err)
	}
	if err := Upgrade(ctx, path, testLibrary); err != nil {
		t.Fatal(err)
	}
	if lib, version, err := Inspect(ctx, path); err != nil || version != 3 || lib != testLibrary {
		t.Fatalf("after upgrade: %s %d %v", lib, version, err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	after := v2Records(t, s.db)
	// The ledger gains exactly the format 3 entry; nothing else changes.
	want := strings.Replace(string(before), `"catalog"`, `"schema_migrations",[3,"`+schemaFingerprint(expectedSchema())+`"],"catalog"`, 1)
	if string(after) != want {
		t.Fatalf("upgrade changed format 2 rows:\n%s\n%s", before, after)
	}
	for _, object := range schemaV2() {
		var ddl string
		if err := s.db.QueryRow(`SELECT sql FROM sqlite_schema WHERE name=?`, object.Name).Scan(&ddl); err != nil || ddl != object.SQL {
			t.Fatalf("format 2 DDL changed %s: %v", object.Name, err)
		}
	}
	// The binder's records still read, and collections work on them.
	if l := getLocation(t, s, "jfs-clip"); l.Removed || l.Value.Locator != "ino:42" {
		t.Fatalf("inode location: %+v", l)
	}
	applyTest(t, s, testOperation("col-after-upgrade",
		Change{CreateCollection: &CreateCollection{CollectionID: testCollection, Name: "Selects"}},
		Change{AddCollectionMember: &AddCollectionMember{CollectionID: testCollection, AssetID: otherAsset}}))
	records := v2Records(t, s.db)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// An upgrade of a current catalog changes nothing.
	if err := Upgrade(ctx, path, testLibrary); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !bytes.Equal(records, v2Records(t, s.db)) {
		t.Fatal("idempotent upgrade changed rows")
	}
	if c, err := s.GetCollection(ctx, testLibrary, testCollection); err != nil || c.Members != 1 {
		t.Fatalf("collection after re-upgrade: %+v %v", c, err)
	}
}

// A failed format 3 step leaves the format 2 catalog exactly as it was.
func TestCollectionUpgradeRollsBackAFailedStep(t *testing.T) {
	path := v2Fixture(t)
	editFixture(t, path, `CREATE TABLE collections(x)`)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := Upgrade(context.Background(), path, testLibrary); !errors.Is(err, ErrFormat) {
		t.Fatalf("altered format 2 upgraded: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("refused upgrade changed the file: %v", err)
	}
}

func collectionStore(t *testing.T) *Store {
	t.Helper()
	s, _ := newTestStore(t)
	applyTest(t, s, testOperation("assets",
		Change{CreateAsset: &CreateAsset{AssetID: testAsset, Label: "a"}},
		Change{CreateAsset: &CreateAsset{AssetID: otherAsset, Label: "b"}}))
	return s
}

func getCollection(t *testing.T, s *Store, id CollectionID) Collection {
	t.Helper()
	c, err := s.GetCollection(context.Background(), testLibrary, id)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func memberIDs(t *testing.T, s *Store, id CollectionID) []continuity.AssetID {
	t.Helper()
	page, err := s.ListCollectionMembers(context.Background(), testLibrary, id, "", MaxPageSize)
	if err != nil {
		t.Fatal(err)
	}
	var out []continuity.AssetID
	for _, m := range page {
		out = append(out, m.AssetID)
	}
	return out
}

func TestCollectionLifecycle(t *testing.T) {
	ctx := context.Background()
	s := collectionStore(t)
	create := func(id string, name string, members ...continuity.AssetID) Operation {
		changes := []Change{{CreateCollection: &CreateCollection{CollectionID: testCollection, Name: name}}}
		for _, m := range members {
			changes = append(changes, Change{AddCollectionMember: &AddCollectionMember{CollectionID: testCollection, AssetID: m}})
		}
		return testOperation(id, changes...)
	}
	r := applyTest(t, s, create("c1", "Selects", testAsset))
	if c := getCollection(t, s, testCollection); c.Name != "Selects" || c.Members != 1 || c.CreatedSequence != r.Sequence || c.Removed {
		t.Fatalf("created: %+v", c)
	}
	if _, err := s.Apply(ctx, create("c2", "Again")); !errors.Is(err, ErrConflict) {
		t.Fatalf("second create: %v", err)
	}
	// A missing asset rolls the whole operation back.
	_, err := s.Apply(ctx, testOperation("bad-add",
		Change{RenameCollection: &RenameCollection{CollectionID: testCollection, Name: "Lost"}},
		Change{AddCollectionMember: &AddCollectionMember{CollectionID: testCollection, AssetID: "asset_ffffffffffffffffffffffffffffffff"}}))
	if !errors.Is(err, ErrNotFound) || getCollection(t, s, testCollection).Name != "Selects" {
		t.Fatalf("missing asset: %v", err)
	}
	// Adding an active member changes nothing; removing an absent one neither.
	first, err := s.GetCollectionMember(ctx, testLibrary, testCollection, testAsset)
	if err != nil {
		t.Fatal(err)
	}
	applyTest(t, s, testOperation("add-again",
		Change{AddCollectionMember: &AddCollectionMember{CollectionID: testCollection, AssetID: testAsset}},
		Change{AddCollectionMember: &AddCollectionMember{CollectionID: testCollection, AssetID: otherAsset}},
		Change{AddCollectionMember: &AddCollectionMember{CollectionID: testCollection, AssetID: otherAsset}}))
	if m, _ := s.GetCollectionMember(ctx, testLibrary, testCollection, testAsset); m != first {
		t.Fatalf("active member changed: %+v", m)
	}
	if got := memberIDs(t, s, testCollection); len(got) != 2 || got[0] != testAsset || got[1] != otherAsset {
		t.Fatalf("members: %v", got)
	}
	removed := applyTest(t, s, testOperation("remove",
		Change{RemoveCollectionMember: &RemoveCollectionMember{CollectionID: testCollection, AssetID: testAsset}}))
	m, err := s.GetCollectionMember(ctx, testLibrary, testCollection, testAsset)
	if err != nil || !m.Removed || m.UpdatedSequence != removed.Sequence || m.CreatedSequence != first.CreatedSequence {
		t.Fatalf("removed member: %+v %v", m, err)
	}
	applyTest(t, s, testOperation("remove-again",
		Change{RemoveCollectionMember: &RemoveCollectionMember{CollectionID: testCollection, AssetID: testAsset}}))
	if again, _ := s.GetCollectionMember(ctx, testLibrary, testCollection, testAsset); again != m {
		t.Fatalf("removing a removed member changed it: %+v", again)
	}
	back := applyTest(t, s, testOperation("readd",
		Change{AddCollectionMember: &AddCollectionMember{CollectionID: testCollection, AssetID: testAsset}}))
	if m, _ := s.GetCollectionMember(ctx, testLibrary, testCollection, testAsset); m.Removed || m.UpdatedSequence != back.Sequence || m.CreatedSequence != first.CreatedSequence {
		t.Fatalf("reactivated member: %+v", m)
	}
	applyTest(t, s, testOperation("rename", Change{RenameCollection: &RenameCollection{CollectionID: testCollection, Name: "Final"}}))
	// A second collection, then remove the first.
	applyTest(t, s, testOperation("other", Change{CreateCollection: &CreateCollection{CollectionID: otherCollection, Name: "B"}}))
	gone := applyTest(t, s, testOperation("drop", Change{RemoveCollection: &RemoveCollection{CollectionID: testCollection}}))
	if c := getCollection(t, s, testCollection); !c.Removed || c.Name != "Final" || c.UpdatedSequence != gone.Sequence || c.Members != 2 {
		t.Fatalf("removed collection: %+v", c)
	}
	list, err := s.ListCollections(ctx, testLibrary, "", 10)
	if err != nil || len(list) != 1 || list[0].ID != otherCollection {
		t.Fatalf("list after removal: %+v %v", list, err)
	}
	for name, change := range map[string]Change{
		"rename": {RenameCollection: &RenameCollection{CollectionID: testCollection, Name: "x"}},
		"remove": {RemoveCollection: &RemoveCollection{CollectionID: testCollection}},
		"add":    {AddCollectionMember: &AddCollectionMember{CollectionID: testCollection, AssetID: testAsset}},
		"drop":   {RemoveCollectionMember: &RemoveCollectionMember{CollectionID: testCollection, AssetID: testAsset}},
		"absent": {AddCollectionMember: &AddCollectionMember{CollectionID: "col_00000000000000000000000000000009", AssetID: testAsset}},
	} {
		if _, err := s.Apply(ctx, testOperation("after-"+name, change)); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s on a removed collection: %v", name, err)
		}
	}
	// Removal reuses no identity.
	if _, err := s.Apply(ctx, create("recreate", "New")); !errors.Is(err, ErrConflict) {
		t.Fatalf("recreate removed id: %v", err)
	}
	if _, err := s.db.Exec(`DELETE FROM collection_members`); err == nil {
		t.Fatal("member rows deleted")
	}
	if _, err := s.db.Exec(`UPDATE collections SET created_sequence=created_sequence+1`); err == nil {
		t.Fatal("collection chronology rewritten")
	}
}

func TestCollectionRetryIdentityAndValidation(t *testing.T) {
	ctx := context.Background()
	s := collectionStore(t)
	op := testOperation("col-create",
		Change{CreateCollection: &CreateCollection{CollectionID: testCollection, Name: "Selects <&>"}},
		Change{AddCollectionMember: &AddCollectionMember{CollectionID: testCollection, AssetID: testAsset}})
	first := applyTest(t, s, op)
	// An identical retry returns the original receipt, before preconditions
	// (the collection exists now) and without another event.
	if again := applyTest(t, s, op); again != first {
		t.Fatalf("retry receipt: %+v", again)
	}
	events, err := s.Events(ctx, testLibrary, 0, 10)
	if err != nil || len(events) != 2 {
		t.Fatalf("events: %d %v", len(events), err)
	}
	changed := op
	changed.Actor = "p-0000000000000001"
	if _, err := s.Apply(ctx, changed); !errors.Is(err, ErrChangedRequest) {
		t.Fatalf("changed actor: %v", err)
	}
	// Wire names are fixed; the stored request decodes back exactly.
	data, _, err := CanonicalRequest(op)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"version":1,"library_id":"lib_000102030405060708090a0b0c0d0e0f","operation_id":"col-create","actor":"owner-demo","changes":[{"create_collection":{"collection_id":"col_303132333435363738393a3b3c3d3e3f","name":"Selects \u003c\u0026\u003e"}},{"add_collection_member":{"collection_id":"col_303132333435363738393a3b3c3d3e3f","asset_id":"asset_101112131415161718191a1b1c1d1e1f"}}]}`
	if string(data) != want {
		t.Fatalf("canonical request:\n%s", data)
	}
	event, err := s.GetEvent(ctx, testLibrary, "col-create")
	if err != nil || event.Operation.Changes[0].CreateCollection.Name != "Selects <&>" || event.Operation.Changes[1].AddCollectionMember.AssetID != testAsset {
		t.Fatalf("event readback: %+v %v", event, err)
	}
	for name, change := range map[string]Change{
		"id prefix":   {CreateCollection: &CreateCollection{CollectionID: "asset_303132333435363738393a3b3c3d3e3f", Name: "x"}},
		"zero id":     {CreateCollection: &CreateCollection{CollectionID: "col_00000000000000000000000000000000", Name: "x"}},
		"upper hex":   {RemoveCollection: &RemoveCollection{CollectionID: "col_303132333435363738393A3B3C3D3E3F"}},
		"empty name":  {CreateCollection: &CreateCollection{CollectionID: otherCollection, Name: ""}},
		"long name":   {RenameCollection: &RenameCollection{CollectionID: testCollection, Name: strings.Repeat("n", 257)}},
		"nul name":    {RenameCollection: &RenameCollection{CollectionID: testCollection, Name: "a\x00b"}},
		"bad member":  {AddCollectionMember: &AddCollectionMember{CollectionID: testCollection, AssetID: "asset_x"}},
		"bad removal": {RemoveCollectionMember: &RemoveCollectionMember{CollectionID: testCollection}},
		"two variants": {CreateCollection: &CreateCollection{CollectionID: otherCollection, Name: "x"},
			RemoveCollection: &RemoveCollection{CollectionID: otherCollection}},
	} {
		if _, err := s.Apply(ctx, testOperation("invalid", change)); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := s.ListCollections(ctx, testLibrary, "col_x", 10); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad cursor: %v", err)
	}
	if _, err := s.ListCollectionMembers(ctx, testLibrary, otherCollection, "", 10); !errors.Is(err, ErrNotFound) {
		t.Fatalf("members of a missing collection: %v", err)
	}
}

func TestCollectionMemberLimitAndPaging(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	assets := make([]continuity.AssetID, MaxCollectionMembers+1)
	for i := range assets {
		assets[i] = continuity.AssetID(fmt.Sprintf("asset_%032x", i+1))
	}
	applyTest(t, s, testOperation("col", Change{CreateCollection: &CreateCollection{CollectionID: testCollection, Name: "Big"}}))
	for start := 0; start < len(assets); start += MaxChanges / 2 {
		end := min(start+MaxChanges/2, len(assets)-1)
		var changes []Change
		for _, a := range assets[start:end] {
			changes = append(changes, Change{CreateAsset: &CreateAsset{AssetID: a, Label: "f"}},
				Change{AddCollectionMember: &AddCollectionMember{CollectionID: testCollection, AssetID: a}})
		}
		if len(changes) > 0 {
			applyTest(t, s, testOperation(fmt.Sprintf("fill-%d", start), changes...))
		}
	}
	last := assets[len(assets)-1]
	applyTest(t, s, testOperation("last-asset", Change{CreateAsset: &CreateAsset{AssetID: last, Label: "f"}}))
	if c := getCollection(t, s, testCollection); c.Members != MaxCollectionMembers {
		t.Fatalf("members: %d", c.Members)
	}
	over := testOperation("over", Change{AddCollectionMember: &AddCollectionMember{CollectionID: testCollection, AssetID: last}})
	if _, err := s.Apply(ctx, over); !errors.Is(err, ErrCollectionFull) {
		t.Fatalf("over the limit: %v", err)
	}
	// Removing one first makes room within the same operation.
	over.Changes = append([]Change{{RemoveCollectionMember: &RemoveCollectionMember{CollectionID: testCollection, AssetID: assets[0]}}}, over.Changes...)
	applyTest(t, s, over)
	var seen int
	var after continuity.AssetID
	for {
		page, err := s.ListCollectionMembers(ctx, testLibrary, testCollection, after, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, m := range page {
			if m.AssetID <= after || m.AssetID == assets[0] || m.Removed {
				t.Fatalf("page order or tombstone: %+v after %s", m, after)
			}
			after = m.AssetID
			seen++
		}
	}
	if seen != MaxCollectionMembers || after != last {
		t.Fatalf("paged %d members, last %s", seen, after)
	}
}

// reseal recomputes a stream's digests after a test edited its frames.
func reseal(t *testing.T, frames [][]byte) []byte {
	t.Helper()
	h := streamHash()
	_, _ = h.Write([]byte(streamMagic))
	for _, f := range frames[:len(frames)-1] {
		_, _ = h.Write(f)
	}
	var footer streamFooter
	if err := json.Unmarshal(frames[len(frames)-1][5:], &footer); err != nil {
		t.Fatal(err)
	}
	digest, err := finishStreamHash(h, footer.EventCount, footer.ProjectionRows, footer.ProjectionSHA256)
	if err != nil {
		t.Fatal(err)
	}
	footer.StreamSHA256 = digest
	data, _ := json.Marshal(footer)
	frames[len(frames)-1] = testFrame(3, data)
	return joinFrames(frames)
}

func TestCollectionPortability(t *testing.T) {
	ctx := context.Background()
	s := collectionStore(t)
	limits := Limits{MaxBytes: 1 << 20, MaxEvents: 100, MaxProjectionRows: 100}
	var plain bytes.Buffer
	if err := s.Export(ctx, testLibrary, &plain, limits); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(plain.Bytes(), []byte(`"schema_version":2`)) {
		t.Fatal("a catalog without collections exports format 2")
	}
	applyTest(t, s, testOperation("col",
		Change{CreateCollection: &CreateCollection{CollectionID: testCollection, Name: "Selects"}},
		Change{AddCollectionMember: &AddCollectionMember{CollectionID: testCollection, AssetID: testAsset}},
		Change{AddCollectionMember: &AddCollectionMember{CollectionID: testCollection, AssetID: otherAsset}}))
	applyTest(t, s, testOperation("col-edit",
		Change{RemoveCollectionMember: &RemoveCollectionMember{CollectionID: testCollection, AssetID: testAsset}},
		Change{RenameCollection: &RenameCollection{CollectionID: testCollection, Name: "Final"}},
		Change{CreateCollection: &CreateCollection{CollectionID: otherCollection, Name: "Gone"}},
		Change{RemoveCollection: &RemoveCollection{CollectionID: otherCollection}}))
	var output bytes.Buffer
	if err := s.Export(ctx, testLibrary, &output, limits); err != nil {
		t.Fatal(err)
	}
	stream := output.Bytes()
	if !bytes.Contains(stream, []byte(`"schema_version":3`)) {
		t.Fatal("collections export as format 3")
	}
	sourceDigest, rows, err := projectionDigest(ctx, s.db, testLibrary, 100)
	if err != nil || rows != 1+2+2+2 {
		t.Fatalf("projection rows %d: %v", rows, err)
	}
	path := filepath.Join(t.TempDir(), "restored.sqlite")
	if err := Restore(ctx, path, testLibrary, bytes.NewReader(stream), limits); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if digest, _, err := projectionDigest(ctx, restored.db, testLibrary, 100); err != nil || digest != sourceDigest {
		t.Fatalf("restored projection: %v", err)
	}
	if c := getCollection(t, restored, testCollection); c.Name != "Final" || c.Members != 1 {
		t.Fatalf("restored collection: %+v", c)
	}
	var again bytes.Buffer
	if err := restored.Export(ctx, testLibrary, &again, limits); err != nil || !bytes.Equal(again.Bytes(), stream) {
		t.Fatalf("export after restore differs: %v", err)
	}
	// The row budget counts collections and members.
	tight := limits
	tight.MaxProjectionRows = rows - 1
	if err := Restore(ctx, filepath.Join(t.TempDir(), "tight.sqlite"), testLibrary, bytes.NewReader(stream), tight); !errors.Is(err, ErrLimit) {
		t.Fatalf("row budget: %v", err)
	}
	// A format 2 header cannot carry collection changes...
	frames := streamFrames(t, stream)
	frames[0] = testFrame(1, bytes.Replace(frames[0][5:], []byte(`"schema_version":3`), []byte(`"schema_version":2`), 1))
	if err := Restore(ctx, filepath.Join(t.TempDir(), "v2.sqlite"), testLibrary, bytes.NewReader(reseal(t, frames)), limits); !errors.Is(err, ErrFormat) || !strings.Contains(err.Error(), "collection change in a format 2 stream") {
		t.Fatalf("collections under a format 2 header: %v", err)
	}
	// ...and a format 3 header needs them: the exporter names the oldest format.
	frames = streamFrames(t, plain.Bytes())
	frames[0] = testFrame(1, bytes.Replace(frames[0][5:], []byte(`"schema_version":2`), []byte(`"schema_version":3`), 1))
	if err := Restore(ctx, filepath.Join(t.TempDir(), "v3.sqlite"), testLibrary, bytes.NewReader(reseal(t, frames)), limits); !errors.Is(err, ErrFormat) || !strings.Contains(err.Error(), "stream format does not match") {
		t.Fatalf("format 3 header without collections: %v", err)
	}
}

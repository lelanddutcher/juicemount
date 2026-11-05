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
	"reflect"
	"strings"
	"testing"
)

// A real format 3 catalog: the format 2 fixture upgraded and given
// collections by the slice-3 catalog code (commit 6c89e261), then dumped.
// Not produced by this code.
//
//go:embed testdata/continuity-s5-v3-fixture.sql
var v3FixtureSQL string

// v3Fingerprint is format 3's DDL fingerprint as slice 3 recorded it.
const v3Fingerprint = "6a2f395cf9f3a6fce947afefaee03ab386ef7bec55410f5b2daad1495902f7b3"

const (
	testQuery  QueryID = "qry_505152535455565758595a5b5c5d5e5f"
	otherQuery QueryID = "qry_606162636465666768696a6b6c6d6e6f"
)

func v3Fixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "v3.sqlite")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	db, err := openDB(path, false)
	if err != nil {
		t.Fatal(err)
	}
	_, execErr := db.Exec(v3FixtureSQL)
	if err := errors.Join(execErr, db.Close()); err != nil {
		t.Fatal(err)
	}
	return path
}

// v3Records captures every format 3 row, ledger included, as detached values.
func v3Records(t *testing.T, q queryer) []byte {
	t.Helper()
	out := []any{json.RawMessage(v2Records(t, q))}
	for _, spec := range []struct{ name, order string }{{"collections", "collection_id"}, {"collection_members", "collection_id,asset_id"}} {
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

func TestQueryV3FixtureIsFrozen(t *testing.T) {
	digest := sha256.Sum256([]byte(v3FixtureSQL))
	if hex.EncodeToString(digest[:]) != "19affe1aae5ccfe94974d2140ac4b70749a63f876c4b4194740f74fd4929bbb5" {
		t.Fatal("frozen format 3 fixture changed")
	}
	if got := schemaFingerprint(schemaV3()); got != v3Fingerprint {
		t.Fatalf("format 3 DDL changed: %s", got)
	}
	lib, version, err := Inspect(context.Background(), v3Fixture(t))
	if err != nil || version != 3 || lib != testLibrary {
		t.Fatalf("format 3 recognition: %s %d %v", lib, version, err)
	}
}

func TestQueryUpgradeFromSliceThreeCatalog(t *testing.T) {
	ctx := context.Background()
	path := v3Fixture(t)
	db, err := openDB(path, true)
	if err != nil {
		t.Fatal(err)
	}
	before := v3Records(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Format 3 opens and works, without queries: the catalog stays
	// readable by the slice-3 binary until queries are used.
	s, err := Open(path)
	if err != nil {
		t.Fatalf("format 3 Open: %v", err)
	}
	defer s.Close()
	if s.Format() != 3 {
		t.Fatalf("format %d", s.Format())
	}
	if page, err := s.ListQueries(ctx, testLibrary, "", 10); err != nil || len(page) != 0 {
		t.Fatalf("queries of a format 3 catalog: %v %v", page, err)
	}
	if _, err := s.GetQuery(ctx, testLibrary, testQuery); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get: %v", err)
	}
	create := testOperation("qry-after-upgrade", Change{CreateQuery: &CreateQuery{QueryID: testQuery, Name: "Recent",
		Definition: QueryDefinition{Prefixes: []string{"Projects"}, Limit: 200}}})
	if _, err := s.Apply(ctx, create); !errors.Is(err, ErrUpgradeRequired) {
		t.Fatalf("query on format 3: %v", err)
	}
	var stream bytes.Buffer
	if err := s.Export(ctx, testLibrary, &stream, Limits{MaxBytes: 1 << 20, MaxEvents: 100, MaxProjectionRows: 100}); err != nil || !bytes.Contains(stream.Bytes(), []byte(`"schema_version":3`)) {
		t.Fatalf("format 3 export: %v", err)
	}
	if got := string(v3Records(t, s.db)); got != string(before) {
		t.Fatalf("a refused query changed the catalog:\n%s\n%s", before, got)
	}
	// Enabled in place, on the open store: the ledger gains exactly the
	// format 4 entry; nothing else changes.
	if err := s.EnableQueries(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.EnableQueries(ctx); err != nil || s.Format() != 4 {
		t.Fatalf("again: %v, format %d", err, s.Format())
	}
	if lib, version, err := Inspect(ctx, path); err != nil || version != 4 || lib != testLibrary {
		t.Fatalf("after enabling: %s %d %v", lib, version, err)
	}
	want := strings.Replace(string(before), `"catalog"`, `"schema_migrations",[4,"`+schemaFingerprint(expectedSchema())+`"],"catalog"`, 1)
	if got := string(v3Records(t, s.db)); got != want {
		t.Fatalf("enabling changed format 3 rows:\n%s\n%s", before, got)
	}
	for _, object := range schemaV3() {
		var ddl string
		if err := s.db.QueryRow(`SELECT sql FROM sqlite_schema WHERE name=?`, object.Name).Scan(&ddl); err != nil || ddl != object.SQL {
			t.Fatalf("format 3 DDL changed %s: %v", object.Name, err)
		}
	}
	if c := getCollection(t, s, testCollection); c.Name != "Selects" || c.Members != 1 {
		t.Fatalf("collection after upgrade: %+v", c)
	}
	applyTest(t, s, create)
	if q := getQuery(t, s, testQuery); q.Name != "Recent" || q.Definition.Prefixes[0] != "Projects" {
		t.Fatalf("query after upgrade: %+v", q)
	}
}

// Upgrade takes format 3 to 4 too; UpgradeTo stops at a target.
func TestQueryUpgradeAndUpgradeTo(t *testing.T) {
	ctx := context.Background()
	path := v3Fixture(t)
	if err := Upgrade(ctx, path, testLibrary); err != nil {
		t.Fatal(err)
	}
	if _, version, err := Inspect(ctx, path); err != nil || version != 4 {
		t.Fatalf("Upgrade: %d %v", version, err)
	}
	if err := UpgradeTo(ctx, path, testLibrary, 3); err != nil {
		t.Fatalf("UpgradeTo an older format of a newer catalog: %v", err)
	}
	old := v2Fixture(t)
	if err := UpgradeTo(ctx, old, testLibrary, 3); err != nil {
		t.Fatal(err)
	}
	if _, version, err := Inspect(ctx, old); err != nil || version != 3 {
		t.Fatalf("UpgradeTo 3: %d %v", version, err)
	}
	for _, target := range []int{2, 5} {
		if err := UpgradeTo(ctx, old, testLibrary, target); !errors.Is(err, ErrInvalid) {
			t.Fatalf("target %d: %v", target, err)
		}
	}
}

// At most MaxQueries active queries per library.
func TestQueryLibraryLimit(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	def := QueryDefinition{Prefixes: []string{"A"}, Limit: 1}
	var changes []Change
	for i := 1; i <= MaxQueries; i++ {
		changes = append(changes, Change{CreateQuery: &CreateQuery{QueryID: QueryID(fmt.Sprintf("qry_%032x", i)), Name: "q", Definition: def}})
	}
	applyTest(t, s, testOperation("many", changes...))
	over := testOperation("over", Change{CreateQuery: &CreateQuery{QueryID: QueryID(fmt.Sprintf("qry_%032x", MaxQueries+1)), Name: "q", Definition: def}})
	if _, err := s.Apply(ctx, over); !errors.Is(err, ErrQueryLimit) {
		t.Fatalf("over the limit: %v", err)
	}
	applyTest(t, s, testOperation("drop", Change{RemoveQuery: &RemoveQuery{QueryID: QueryID(fmt.Sprintf("qry_%032x", 1))}}))
	applyTest(t, s, over)
}

// A failed format 4 step leaves the format 3 catalog exactly as it was.
func TestQueryUpgradeRollsBackAFailedStep(t *testing.T) {
	path := v3Fixture(t)
	editFixture(t, path, `CREATE TABLE queries(x)`)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := Upgrade(context.Background(), path, testLibrary); !errors.Is(err, ErrFormat) {
		t.Fatalf("altered format 3 upgraded: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("refused upgrade changed the file: %v", err)
	}
}

func getQuery(t *testing.T, s *Store, id QueryID) Query {
	t.Helper()
	q, err := s.GetQuery(context.Background(), testLibrary, id)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestQueryLifecycleAndCanonicalForm(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	def := QueryDefinition{Prefixes: []string{"Projects/B", "Projects/A"}, NameContains: "Take <1>", Extensions: []string{"mov", "braw"},
		ModifiedWithinSeconds: 3600, Limit: 50}
	op := testOperation("q1", Change{CreateQuery: &CreateQuery{QueryID: testQuery, Name: "Takes", Definition: def}})
	r := applyTest(t, s, op)
	// Stored canonical: lists sorted, wire order fixed, HTML escaped.
	data, _, err := CanonicalRequest(op)
	if err != nil {
		t.Fatal(err)
	}
	const wantWire = `{"version":1,"library_id":"lib_000102030405060708090a0b0c0d0e0f","operation_id":"q1","actor":"owner-demo","changes":[{"create_query":{"query_id":"qry_505152535455565758595a5b5c5d5e5f","name":"Takes","definition":{"prefixes":["Projects/A","Projects/B"],"name_contains":"Take \u003c1\u003e","extensions":["braw","mov"],"modified_within_seconds":3600,"limit":50}}}]}`
	if string(data) != wantWire {
		t.Fatalf("canonical request:\n%s", data)
	}
	q := getQuery(t, s, testQuery)
	wantDef := QueryDefinition{Prefixes: []string{"Projects/A", "Projects/B"}, NameContains: "Take <1>", Extensions: []string{"braw", "mov"}, ModifiedWithinSeconds: 3600, Limit: 50}
	if q.Name != "Takes" || q.CreatedSequence != r.Sequence || q.Removed || !reflect.DeepEqual(q.Definition, wantDef) {
		t.Fatalf("created: %+v", q)
	}
	// A retry is the identical request; a second create conflicts.
	if again := applyTest(t, s, op); again != r {
		t.Fatalf("retry: %+v", again)
	}
	if _, err := s.Apply(ctx, testOperation("q1-again", Change{CreateQuery: &CreateQuery{QueryID: testQuery, Name: "x", Definition: def}})); !errors.Is(err, ErrConflict) {
		t.Fatalf("second create: %v", err)
	}
	// No lists: empty, never null.
	applyTest(t, s, testOperation("q2", Change{CreateQuery: &CreateQuery{QueryID: otherQuery, Name: "All", Definition: QueryDefinition{Prefixes: []string{"X"}, Limit: 1}}}))
	if q := getQuery(t, s, otherQuery); q.Definition.Extensions == nil || len(q.Definition.Extensions) != 0 {
		t.Fatalf("empty extensions: %#v", q.Definition)
	}
	rr := applyTest(t, s, testOperation("q1-rename", Change{RenameQuery: &RenameQuery{QueryID: testQuery, Name: "Selects"}}))
	if q := getQuery(t, s, testQuery); q.Name != "Selects" || q.UpdatedSequence != rr.Sequence || !reflect.DeepEqual(q.Definition, wantDef) {
		t.Fatalf("renamed: %+v", q)
	}
	applyTest(t, s, testOperation("q2-remove", Change{RemoveQuery: &RemoveQuery{QueryID: otherQuery}}))
	if q := getQuery(t, s, otherQuery); !q.Removed {
		t.Fatalf("removed: %+v", q)
	}
	page, err := s.ListQueries(ctx, testLibrary, "", 10)
	if err != nil || len(page) != 1 || page[0].ID != testQuery {
		t.Fatalf("list: %+v %v", page, err)
	}
	for name, change := range map[string]Change{
		"rename removed": {RenameQuery: &RenameQuery{QueryID: otherQuery, Name: "x"}},
		"remove removed": {RemoveQuery: &RemoveQuery{QueryID: otherQuery}},
		"rename missing": {RenameQuery: &RenameQuery{QueryID: "qry_0000000000000000000000000000000f", Name: "x"}},
	} {
		if _, err := s.Apply(ctx, testOperation("gone-"+strings.ReplaceAll(name, " ", "-"), change)); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// The definition is immutable in storage too.
	if _, err := s.db.Exec(`UPDATE queries SET definition='{}' WHERE query_id=?`, testQuery); err == nil {
		t.Fatal("definition changed in place")
	}
}

func TestQueryDefinitionValidation(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	ok := QueryDefinition{Prefixes: []string{"A"}, Limit: 200}
	with := func(f func(*QueryDefinition)) QueryDefinition {
		d := ok
		d.Prefixes = append([]string(nil), ok.Prefixes...)
		f(&d)
		return d
	}
	for name, d := range map[string]QueryDefinition{
		"no prefix":        with(func(d *QueryDefinition) { d.Prefixes = nil }),
		"root prefix":      with(func(d *QueryDefinition) { d.Prefixes = []string{""} }),
		"leading slash":    with(func(d *QueryDefinition) { d.Prefixes = []string{"/A"} }),
		"trailing slash":   with(func(d *QueryDefinition) { d.Prefixes = []string{"A/"} }),
		"dot dot":          with(func(d *QueryDefinition) { d.Prefixes = []string{"A/../B"} }),
		"hidden":           with(func(d *QueryDefinition) { d.Prefixes = []string{"A/.trash"} }),
		"duplicate prefix": with(func(d *QueryDefinition) { d.Prefixes = []string{"A", "A"} }),
		"too many":         with(func(d *QueryDefinition) { d.Prefixes = strings.Split("a b c d e f g h i j k l m n o p q", " ") }),
		"long prefix":      with(func(d *QueryDefinition) { d.Prefixes = []string{strings.Repeat("a", MaxQueryPrefixBytes+1)} }),
		"slash in name":    with(func(d *QueryDefinition) { d.NameContains = "a/b" }),
		"nul in name":      with(func(d *QueryDefinition) { d.NameContains = "a\x00" }),
		"control in name":  with(func(d *QueryDefinition) { d.NameContains = "a\tb" }),
		"control in path":  with(func(d *QueryDefinition) { d.Prefixes = []string{"A/b\nc"} }),
		"DEL in path":      with(func(d *QueryDefinition) { d.Prefixes = []string{"A\x7f"} }),
		"upper extension":  with(func(d *QueryDefinition) { d.Extensions = []string{"MOV"} }),
		"dotted extension": with(func(d *QueryDefinition) { d.Extensions = []string{".mov"} }),
		"dup extension":    with(func(d *QueryDefinition) { d.Extensions = []string{"mov", "mov"} }),
		"negative window":  with(func(d *QueryDefinition) { d.ModifiedWithinSeconds = -1 }),
		"huge window":      with(func(d *QueryDefinition) { d.ModifiedWithinSeconds = MaxQueryWithinSeconds + 1 }),
		"zero limit":       with(func(d *QueryDefinition) { d.Limit = 0 }),
		"limit over max":   with(func(d *QueryDefinition) { d.Limit = MaxQueryLimit + 1 }),
	} {
		if _, err := s.Apply(ctx, testOperation("invalid", Change{CreateQuery: &CreateQuery{QueryID: testQuery, Name: "x", Definition: d}})); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, change := range map[string]Change{
		"bad id":     {CreateQuery: &CreateQuery{QueryID: "qry_x", Name: "x", Definition: ok}},
		"zero id":    {RemoveQuery: &RemoveQuery{QueryID: "qry_00000000000000000000000000000000"}},
		"empty name": {CreateQuery: &CreateQuery{QueryID: testQuery, Name: "", Definition: ok}},
		"long name":  {RenameQuery: &RenameQuery{QueryID: testQuery, Name: strings.Repeat("n", 257)}},
		"two variants": {CreateQuery: &CreateQuery{QueryID: testQuery, Name: "x", Definition: ok},
			RemoveQuery: &RemoveQuery{QueryID: testQuery}},
	} {
		if _, err := s.Apply(ctx, testOperation("invalid", change)); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := s.ListQueries(ctx, testLibrary, "qry_x", 10); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad cursor: %v", err)
	}
	// A stored definition that is not canonical reads as corrupt.
	applyTest(t, s, testOperation("q", Change{CreateQuery: &CreateQuery{QueryID: testQuery, Name: "x", Definition: ok}}))
	if _, err := s.db.Exec(`DROP TRIGGER queries_identity`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE queries SET definition='{"prefixes":["B","A"],"name_contains":"","extensions":[],"modified_within_seconds":0,"limit":200}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(triggerDDLFor(t, "queries_identity")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetQuery(ctx, testLibrary, testQuery); !errors.Is(err, ErrFormat) {
		t.Fatalf("non-canonical stored definition: %v", err)
	}
}

func triggerDDLFor(t *testing.T, name string) string {
	t.Helper()
	for _, o := range querySchema() {
		if o.Name == name {
			return o.SQL
		}
	}
	t.Fatalf("no trigger %s", name)
	return ""
}

func TestQueryPortability(t *testing.T) {
	ctx := context.Background()
	s := collectionStore(t)
	limits := Limits{MaxBytes: 1 << 20, MaxEvents: 100, MaxProjectionRows: 100}
	applyTest(t, s, testOperation("col", Change{CreateCollection: &CreateCollection{CollectionID: testCollection, Name: "Selects"}}))
	var collections bytes.Buffer
	if err := s.Export(ctx, testLibrary, &collections, limits); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(collections.Bytes(), []byte(`"schema_version":3`)) {
		t.Fatal("a catalog without queries exports format 3")
	}
	applyTest(t, s, testOperation("qry",
		Change{CreateQuery: &CreateQuery{QueryID: testQuery, Name: "Recent", Definition: QueryDefinition{Prefixes: []string{"A"}, Extensions: []string{"mov"}, Limit: 20}}},
		Change{CreateQuery: &CreateQuery{QueryID: otherQuery, Name: "Gone", Definition: QueryDefinition{Prefixes: []string{"B"}, Limit: 20}}}))
	applyTest(t, s, testOperation("qry-edit",
		Change{RenameQuery: &RenameQuery{QueryID: testQuery, Name: "Latest"}},
		Change{RemoveQuery: &RemoveQuery{QueryID: otherQuery}}))
	var output bytes.Buffer
	if err := s.Export(ctx, testLibrary, &output, limits); err != nil {
		t.Fatal(err)
	}
	stream := output.Bytes()
	if !bytes.Contains(stream, []byte(`"schema_version":4`)) {
		t.Fatal("queries export as format 4")
	}
	sourceDigest, rows, err := projectionDigest(ctx, s.db, testLibrary, 100)
	if err != nil || rows != 1+2+1+2 {
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
	if q := getQuery(t, restored, testQuery); q.Name != "Latest" || q.Definition.Extensions[0] != "mov" {
		t.Fatalf("restored query: %+v", q)
	}
	var again bytes.Buffer
	if err := restored.Export(ctx, testLibrary, &again, limits); err != nil || !bytes.Equal(again.Bytes(), stream) {
		t.Fatalf("export after restore differs: %v", err)
	}
	// A format 3 header cannot carry query changes...
	frames := streamFrames(t, stream)
	frames[0] = testFrame(1, bytes.Replace(frames[0][5:], []byte(`"schema_version":4`), []byte(`"schema_version":3`), 1))
	if err := Restore(ctx, filepath.Join(t.TempDir(), "v3.sqlite"), testLibrary, bytes.NewReader(reseal(t, frames)), limits); !errors.Is(err, ErrFormat) || !strings.Contains(err.Error(), "query change in a format 2 or 3 stream") {
		t.Fatalf("queries under a format 3 header: %v", err)
	}
	// ...and a format 4 header needs them.
	frames = streamFrames(t, collections.Bytes())
	frames[0] = testFrame(1, bytes.Replace(frames[0][5:], []byte(`"schema_version":3`), []byte(`"schema_version":4`), 1))
	if err := Restore(ctx, filepath.Join(t.TempDir(), "v4.sqlite"), testLibrary, bytes.NewReader(reseal(t, frames)), limits); !errors.Is(err, ErrFormat) || !strings.Contains(err.Error(), "stream format does not match") {
		t.Fatalf("format 4 header without queries: %v", err)
	}
}

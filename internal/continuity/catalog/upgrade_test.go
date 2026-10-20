package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// These are frozen source fixtures, not produced by Create/initialize/Apply.
//
//go:embed testdata/continuity-c02b-v1-fixture.sql
var legacyFixtureSQL string

//go:embed testdata/continuity-c02b-v1-fixture.json
var legacyFixtureJSON []byte

type legacyGolden struct {
	Fingerprint string                `json:"schema_fingerprint"`
	Library     continuity.LibraryID  `json:"library_id"`
	Asset       continuity.AssetID    `json:"asset_id"`
	Label       string                `json:"label"`
	Head        continuity.RevisionID `json:"head"`
	Created     int64                 `json:"created_sequence"`
	Updated     int64                 `json:"updated_sequence"`
	Revisions   []struct {
		ID        continuity.RevisionID `json:"id"`
		Canonical string                `json:"canonical_hex"`
		Manifest  string                `json:"manifest_json"`
	} `json:"revisions"`
	Events []struct {
		Sequence  int64  `json:"sequence"`
		ID        string `json:"operation_id"`
		Request   string `json:"request_json"`
		Digest    string `json:"payload_sha256"`
		Timestamp int64  `json:"recorded_unix_nano"`
		Receipt   string `json:"receipt_json"`
	} `json:"events"`
}

func legacyFixture(t *testing.T) (string, legacyGolden) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "legacy.sqlite")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	db, err := openDB(path, false)
	if err != nil {
		t.Fatal(err)
	}
	_, execErr := db.Exec(legacyFixtureSQL)
	closeErr := db.Close()
	if err := errors.Join(execErr, closeErr); err != nil {
		t.Fatal(err)
	}
	var golden legacyGolden
	if err := json.Unmarshal(legacyFixtureJSON, &golden); err != nil {
		t.Fatal(err)
	}
	return path, golden
}

// Capture every original projection/history field as detached values, excluding
// only the ledger replaced by migration. Original DDL is checked separately.
func legacyRecords(t *testing.T, q queryer) []byte {
	t.Helper()
	type table struct {
		Name string
		Rows [][]any
	}
	var tables []table
	for _, spec := range []struct{ name, order string }{
		{"catalog", "singleton"}, {"assets", "asset_id"}, {"revisions", "revision_id"}, {"revision_parents", "revision_id,parent_id"}, {"events", "sequence"},
	} {
		rows, err := q.QueryContext(context.Background(), "SELECT * FROM "+spec.name+" ORDER BY "+spec.order)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		current := table{Name: spec.name}
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
			for i, value := range values {
				if blob, ok := value.([]byte); ok {
					values[i] = append([]byte(nil), blob...)
				}
			}
			current.Rows = append(current.Rows, values)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		tables = append(tables, current)
	}
	data, err := json.Marshal(tables)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestLegacyFixtureIsIndependentAndExact(t *testing.T) {
	for _, item := range []struct {
		data []byte
		want string
	}{
		{[]byte(legacyFixtureSQL), "590c71f4632a8bfd695b79967e17a8a959e987a62a227188b3cfff2d610a63e5"},
		{legacyFixtureJSON, "b3a8ea849648c6568b46d28f31d8bfa9f8e320eb6df0141fdcbb5e434117e55e"},
	} {
		digest := sha256.Sum256(item.data)
		if hex.EncodeToString(digest[:]) != item.want {
			t.Fatal("frozen fixture bytes changed")
		}
	}
	if got := schemaFingerprint(legacySchema()); got != "752faa61a0812eef6fb7cd2a3d71a2aac3cee19f660b2d129e78cfc1a1b86f26" {
		t.Fatalf("legacy DDL fingerprint: %s", got)
	}
	path, golden := legacyFixture(t)
	lib, version, err := snapshotCatalog(context.Background(), path)
	if err != nil || version != 1 || lib != golden.Library {
		t.Fatalf("legacy recognition: %s, %d, %v", lib, version, err)
	}
}

func TestCatalogExplicitUpgradePreservesLegacyIdentities(t *testing.T) {
	path, golden := legacyFixture(t)
	db, err := openDB(path, true)
	if err != nil {
		t.Fatal(err)
	}
	before := legacyRecords(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if store, err := Open(path); !errors.Is(err, ErrUpgradeRequired) {
		if store != nil {
			store.Close()
		}
		t.Fatalf("v1 Open: %v", err)
	}
	if err := Upgrade(context.Background(), path, golden.Library); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !bytes.Equal(before, legacyRecords(t, s.db)) {
		t.Fatal("upgrade changed original records or history bytes")
	}
	for _, object := range legacySchema() {
		if object.Table == "schema_migrations" {
			continue
		}
		var ddl string
		if err := s.db.QueryRow(`SELECT sql FROM sqlite_schema WHERE name=?`, object.Name).Scan(&ddl); err != nil || ddl != object.SQL {
			t.Fatalf("old DDL changed %s: %v", object.Name, err)
		}
	}
	a, err := s.GetAsset(context.Background(), golden.Library, golden.Asset)
	if err != nil || a.Label != golden.Label || a.Head != golden.Head || a.CreatedSequence != golden.Created || a.UpdatedSequence != golden.Updated {
		t.Fatalf("legacy asset: %+v, %v", a, err)
	}
	for _, expected := range golden.Revisions {
		r, err := s.GetRevision(context.Background(), golden.Library, expected.ID)
		if err != nil || hex.EncodeToString(r.Canonical) != expected.Canonical {
			t.Fatalf("legacy revision %s: %v", expected.ID, err)
		}
		var data []byte
		if err := s.db.QueryRow(`SELECT manifest_json FROM revisions WHERE revision_id=?`, expected.ID).Scan(&data); err != nil || string(data) != expected.Manifest {
			t.Fatalf("legacy manifest bytes: %v", err)
		}
	}
	for _, expected := range golden.Events {
		raw, err := scanEvent(s.db.QueryRow(`SELECT `+eventColumns+` FROM events WHERE operation_id=?`, expected.ID))
		if err != nil || string(raw.request) != expected.Request || string(raw.receipt) != expected.Receipt || raw.digest != expected.Digest || raw.timestamp != expected.Timestamp {
			t.Fatalf("legacy event bytes %s: %v", expected.ID, err)
		}
		event, err := raw.decode(golden.Library)
		if err != nil {
			t.Fatal(err)
		}
		if retry := applyTest(t, s, event.Operation); retry != event.Receipt {
			t.Fatal("old operation retry changed receipt")
		}
	}
	self := RelationshipValue{SourceAssetID: golden.Asset, SourceRevision: golden.Head, TargetAssetID: golden.Asset, TargetRevision: rootRevision, Predicate: "related", Provenance: Observed, Source: "fixture"}
	if receipt := applyTest(t, s, testOperation("new-edge", Change{PutRelationship: &PutRelationship{ID: "legacy-self", Value: self}})); receipt.Sequence != 3 {
		t.Fatalf("upgrade consumed event sequence: %+v", receipt)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := getRelationship(t, s, "legacy-self"); got.Value != self {
		t.Fatalf("new relation after reopen: %+v", got)
	}
}

func TestCatalogUpgradeRetriesDirectoryBarrierAndIsIdempotent(t *testing.T) {
	path, _ := legacyFixture(t)
	barrierErr := errors.New("injected directory sync failure")
	calls := 0
	barrier := func(path string) error {
		calls++
		if calls == 1 {
			return barrierErr
		}
		return syncParent(path)
	}
	if err := upgradeCatalog(context.Background(), path, testLibrary, barrier); !errors.Is(err, barrierErr) {
		t.Fatalf("final barrier error: %v", err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal("committed migration lost after barrier error:", err)
	}
	before := legacyRecords(t, s.db)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := upgradeCatalog(context.Background(), path, testLibrary, barrier); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("v2 retry skipped directory barrier: %d calls", calls)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !bytes.Equal(before, legacyRecords(t, s.db)) {
		t.Fatal("idempotent upgrade changed history")
	}
	var entries int
	if err := s.db.QueryRow(`SELECT count(*) FROM schema_migrations`).Scan(&entries); err != nil || entries != 2 {
		t.Fatalf("ledger lineage: %d, %v", entries, err)
	}
}

func TestCatalogMigrationRollbackAndConcurrentUpgraders(t *testing.T) {
	path, _ := legacyFixture(t)
	db, err := openDB(path, false)
	if err != nil {
		t.Fatal(err)
	}
	before := legacyRecords(t, db)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := migrateV1(context.Background(), tx); err != nil {
		tx.Rollback()
		db.Close()
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO catalog(singleton,library_id,ready) VALUES(2,?,1)`, testLibrary); err == nil {
		tx.Rollback()
		db.Close()
		t.Fatal("injected later constraint did not fail")
	}
	if err := tx.Rollback(); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if !bytes.Equal(before, legacyRecords(t, db)) {
		db.Close()
		t.Fatal("rolled-back migration changed old rows")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, version, err := snapshotCatalog(context.Background(), path); err != nil || version != 1 {
		t.Fatalf("rollback left incomplete/mixed format: %d, %v", version, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start, done := make(chan struct{}), make(chan error, 2)
	for range 2 {
		go func() { <-start; done <- Upgrade(ctx, path, testLibrary) }()
	}
	close(start)
	first, second := <-done, <-done
	if first != nil || second != nil {
		t.Fatalf("concurrent upgrades: %v, %v", first, second)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !bytes.Equal(before, legacyRecords(t, s.db)) {
		t.Fatal("concurrent migration changed old rows")
	}
}

func TestCatalogUpgradeRefusesWrongIncompleteAndUnsafeTargets(t *testing.T) {
	for name, statements := range map[string][]string{
		"unknown version":     {`PRAGMA user_version=3`},
		"foreign application": {`PRAGMA application_id=17`},
		"altered DDL":         {`CREATE TABLE unrelated(value TEXT)`},
		"incomplete v1":       {`DROP TRIGGER catalog_no_update`, `UPDATE catalog SET ready=0`, triggerDDL("catalog_no_update")},
		"unknown ledger":      {`DROP TRIGGER schema_migrations_no_update`, `UPDATE schema_migrations SET fingerprint='0000000000000000000000000000000000000000000000000000000000000000'`, triggerDDL("schema_migrations_no_update")},
	} {
		t.Run(name, func(t *testing.T) {
			path, _ := legacyFixture(t)
			editFixture(t, path, statements...)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := Upgrade(context.Background(), path, testLibrary); !errors.Is(err, ErrFormat) {
				t.Fatalf("invalid upgrade: %v", err)
			}
			if s, err := Open(path); !errors.Is(err, ErrFormat) {
				if s != nil {
					s.Close()
				}
				t.Fatalf("invalid v1 Open: %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("invalid target changed: %v", err)
			}
		})
	}
	path, _ := legacyFixture(t)
	if err := Upgrade(context.Background(), path, otherLibrary); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong library: %v", err)
	}
	if _, version, err := snapshotCatalog(context.Background(), path); err != nil || version != 1 {
		t.Fatalf("wrong-identity migration changed format: %d, %v", version, err)
	}
	for _, input := range []string{":memory:", "file:catalog?mode=memory", filepath.Join(t.TempDir(), "missing")} {
		if err := Upgrade(context.Background(), input, testLibrary); err == nil {
			t.Fatalf("unsafe input accepted %q", input)
		}
	}
	for _, kind := range []string{"symlink", "hardlink", "sidecar"} {
		t.Run(kind, func(t *testing.T) {
			path, _ := legacyFixture(t)
			input := filepath.Join(filepath.Dir(path), "alias.sqlite")
			var err error
			switch kind {
			case "symlink":
				err = os.Symlink(path, input)
			case "hardlink":
				err = os.Link(path, input)
			case "sidecar":
				input = path
				err = os.Symlink(path, path+"-wal")
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := Upgrade(context.Background(), input, testLibrary); err == nil {
				t.Fatal("unsafe target upgraded")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Upgrade(ctx, path, testLibrary); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled upgrade: %v", err)
	}
}

func TestCatalogNewV2LedgerHasExactLegacyLineage(t *testing.T) {
	s, _ := newTestStore(t)
	var version int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != 2 {
		t.Fatalf("new schema: %d, %v", version, err)
	}
	var fingerprint string
	if err := s.db.QueryRow(`SELECT fingerprint FROM schema_migrations WHERE version=1`).Scan(&fingerprint); err != nil || fingerprint != "752faa61a0812eef6fb7cd2a3d71a2aac3cee19f660b2d129e78cfc1a1b86f26" {
		t.Fatalf("legacy lineage: %s, %v", fingerprint, err)
	}
	if _, err := s.db.Exec(`INSERT INTO schema_migrations(version,fingerprint) VALUES(3,?)`, strings.Repeat("0", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(context.Background(), initialOperation()); !errors.Is(err, ErrFormat) {
		t.Fatalf("unknown future ledger accepted: %v", err)
	}
}

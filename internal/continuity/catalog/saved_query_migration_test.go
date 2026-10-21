package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// Frozen schema5 table inventory. Typed cells and sorted full rows retain BLOB
// versus TEXT, NULL versus empty, integer values, ordering and duplicate rows.
var savedQueryOldTables = []string{"assets", "catalog", "catalog_admin_codecs", "catalog_admin_events", "catalog_authority", "catalog_credentials", "collection_definitions", "collection_entries", "collection_events", "collection_grants", "collection_identities", "collection_library_quota", "collection_order", "collection_policy_events", "collection_policy_results", "collection_quota", "credential_codecs", "credential_issuance_receipts", "events", "locations", "policy_events", "policy_grants", "policy_subjects", "preference_control", "preference_receipts", "preference_subjects", "preferences", "relationships", "revision_parents", "revisions", "schema_migrations"}

func savedQueryTestSnapshot(t *testing.T, s *Store, queries bool) []byte {
	t.Helper()
	tables := append([]string(nil), savedQueryOldTables...)
	if queries {
		tables = append(tables, "saved_query_definitions")
	}
	var all []any
	for _, table := range tables {
		statement := "SELECT * FROM " + table
		if table == "schema_migrations" {
			statement += " WHERE version<=5"
		}
		rows, err := s.db.Query(statement)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		records := []string{}
		for rows.Next() {
			values := make([]any, len(columns))
			dest := make([]any, len(values))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			cells := make([]string, len(values))
			for i, v := range values {
				switch v := v.(type) {
				case nil:
					cells[i] = "null"
				case []byte:
					cells[i] = "blob:" + hex.EncodeToString(v)
				case string:
					cells[i] = "text:" + hex.EncodeToString([]byte(v))
				case int64:
					cells[i] = fmt.Sprintf("integer:%d", v)
				default:
					rows.Close()
					t.Fatalf("unexpected SQLite storage value %T", v)
				}
			}
			row, err := json.Marshal(cells)
			if err != nil {
				rows.Close()
				t.Fatal(err)
			}
			records = append(records, string(row))
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
		sort.Strings(records)
		all = append(all, table, columns, records)
	}
	b, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func savedQueryAssertMigration(t *testing.T, s *Store) *Store {
	t.Helper()
	ctx := context.Background()
	path, library := s.path, s.library
	before := savedQueryTestSnapshot(t, s, false)
	inode, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeSavedQueries(ctx, path, library); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeSavedQueries(ctx, path, library); err != nil {
		t.Fatalf("idempotent schema6 upgrade: %v", err)
	}
	after, err := os.Stat(path)
	if err != nil || !os.SameFile(inode, after) {
		t.Fatalf("migration replaced inode: %v", err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	if reopened.version != 6 || !bytes.Equal(before, savedQueryTestSnapshot(t, reopened, false)) {
		t.Fatal("schema5 table bytes, storage types or duplicate multiplicity changed")
	}
	var hash string
	if err := reopened.db.QueryRow(`SELECT fingerprint FROM schema_migrations WHERE version=6`).Scan(&hash); err != nil || hash != "8b37a984f3c6035219494c95bbd76e9837947ad6b1a5c19f92ea23ab3d9d22e3" {
		t.Fatalf("independent schema6 fingerprint: %s %v", hash, err)
	}
	var count int
	if err := reopened.db.QueryRow(`SELECT count(*) FROM saved_query_definitions`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("migration invented query rows: %d %v", count, err)
	}
	return reopened
}

func TestSavedQuerySchemaFingerprintsAndExplicitCreate(t *testing.T) {
	expected := []string{"752faa61a0812eef6fb7cd2a3d71a2aac3cee19f660b2d129e78cfc1a1b86f26", "1d67308e722a028662374a942e4b19bebe836df7d5f903a27a4ac3412e944a6d", "7b4a3ba35d87d80b9fd16f9c161f72a148ffcd55effd8b9199ba152d75213582", "ec4f4c3de045dd7adbd37da0a0381d0aa17b294550b6f2135c88b60a6453b65d", "2ff1baac18e411c7e22b7320e37b079869d61ca24e04087fc82c0a6cf2fa6133", "8b37a984f3c6035219494c95bbd76e9837947ad6b1a5c19f92ea23ab3d9d22e3"}
	for i, want := range expected {
		if got := schemaFingerprint(expectedSchemaFor(i + 1)); got != want {
			t.Fatalf("schema%d fingerprint %s != %s", i+1, got, want)
		}
	}
	if len(savedQueryOldTables) != 31 || len(expectedSchemaFor(5)) != 112 || len(expectedSchemaFor(6)) != 118 {
		t.Fatal("frozen object/table inventory changed")
	}
	s, err := CreateSavedQueries(context.Background(), filepath.Join(t.TempDir(), "six.sqlite"), testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.version != 6 {
		t.Fatal("wrong initialization version")
	}
	var out bytes.Buffer
	if err := s.Export(context.Background(), testLibrary, &out, Limits{MaxStreamBytes, MaxStreamEvents, MaxStreamProjectionRows}); err != nil || !bytes.HasPrefix(out.Bytes(), []byte(streamMagic4)) {
		t.Fatalf("schema6 format4 export: %v", err)
	}
}

func TestSavedQueryMigrationFrozenV4AndMixedArchivePreservesRows(t *testing.T) {
	ctx := context.Background()
	t.Run("independent_v4", func(t *testing.T) {
		data, err := os.ReadFile("testdata/continuity-c04b-v4-fixture.sql")
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		if hex.EncodeToString(digest[:]) != "3a3f419e55e7c9a86112bb5b4b550d23e0433b7838ae2fd65c88b94f7d8ccc74" {
			t.Fatal("frozen v4 source changed")
		}
		path := filepath.Join(t.TempDir(), "four.sqlite")
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
		db, err := openDB(path, false)
		if err != nil {
			t.Fatal(err)
		}
		_, execErr := db.Exec(string(data))
		if err := errors.Join(execErr, db.Close()); err != nil {
			t.Fatal(err)
		}
		four, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		library := four.library
		if err := four.Close(); err != nil {
			t.Fatal(err)
		}
		if err := UpgradeSavedQueries(ctx, path, library); !errors.Is(err, ErrUpgradeRequired) {
			t.Fatalf("skipped explicit schema5: %v", err)
		}
		if err := UpgradeCollections(ctx, path, library); err != nil {
			t.Fatal(err)
		}
		five, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, table := range []string{"events", "policy_events", "preferences", "preference_receipts"} {
			var n int
			if err := five.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n == 0 {
				t.Fatalf("v4 fixture not nonempty in %s: %d %v", table, n, err)
			}
		}
		savedQueryAssertMigration(t, five)
	})
	t.Run("independent_mixed_format3", func(t *testing.T) {
		fixture := readCollectionWireFixture(t, "shared")
		wire := collectionFixtureHex(t, fixture.StreamHex)
		path := filepath.Join(t.TempDir(), "mixed.sqlite")
		limits := Limits{int64(len(wire)), MaxStreamEvents, fixture.ProjectionRows}
		if err := Restore(ctx, path, fixture.LibraryID, bytes.NewReader(wire), limits); err != nil {
			t.Fatal(err)
		}
		five, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := five.Export(ctx, fixture.LibraryID, &out, limits); err != nil || !bytes.Equal(out.Bytes(), wire) {
			t.Fatalf("old format3 changed: %v", err)
		}
		for _, table := range []string{"collection_events", "collection_policy_events", "collection_policy_results", "collection_order", "collection_definitions", "collection_entries"} {
			var n int
			if err := five.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n == 0 {
				t.Fatalf("mixed fixture not nonempty in %s: %d %v", table, n, err)
			}
		}
		savedQueryAssertMigration(t, five)
	})
}

func TestSavedQueryMigrationLiveCredentialPreferenceAndManualRetry(t *testing.T) {
	s, o, a, d, owner, _ := collectionTxStore(t)
	ctx := context.Background()
	manual := collectionTxCreate(t, 200)
	receipt := collectionTxCall(t, d, owner, manual)
	discoveryGrantTest(t, o, a, "saved-preference-rights", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	oldBearer := discoveryIssueTest(t, o, a, "saved-preference", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	selected := recordPreferenceTest(t, d, oldBearer, preferenceRequestTest("saved-migration-select", "select", testAsset, nil, s.now().Unix()))
	if selected.Token == nil {
		t.Fatal("preference fixture has no selection token")
	}
	issue, request := collectionTxIssue(t, o, a, "saved-retry-issuance", ownerSubject, collectionTxActions)
	if err := d.Close(ctx); err != nil {
		t.Fatal(err)
	}
	before := savedQueryTestSnapshot(t, s, false)
	// A still-owned schema5 Store must refuse to operate after external migration.
	if err := UpgradeSavedQueries(ctx, s.path, testLibrary); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Authenticate(ctx, testLibrary, owner); !errors.Is(err, ErrUpgradeRequired) {
		t.Fatalf("stale handle accepted schema6: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.clock = s.clock
	if !bytes.Equal(before, savedQueryTestSnapshot(t, reopened, false)) {
		t.Fatal("live migration changed prior typed rows")
	}
	for _, bearer := range [][]byte{owner, oldBearer, issue.Bearer} {
		if _, err := reopened.Authenticate(ctx, testLibrary, bearer); err != nil {
			t.Fatalf("migration changed live credential: %v", err)
		}
	}
	newOwner, err := reopened.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := newOwner.IssueCredentialV2(ctx, request)
	if err != nil || len(retry.Bearer) != 0 || retry.Receipt != issue.Receipt {
		t.Fatalf("issuance retry changed: %+v %v", retry, err)
	}
	service, err := NewDiscoveryService(ctx, reopened, testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(ctx)
	if got := collectionTxCall(t, service, owner, manual); got != receipt {
		t.Fatal("manual retained receipt changed")
	}
	token := preferenceStatusTest(t, service, oldBearer, testAsset)
	if token == nil || *token != *selected.Token {
		t.Fatal("private preference changed")
	}
}

func TestSavedQueryMigrationRollbackRefusalAndBarrierUncertainty(t *testing.T) {
	s, _, _, d, _, _ := collectionTxStore(t)
	ctx := context.Background()
	if err := d.Close(ctx); err != nil {
		t.Fatal(err)
	}
	before := savedQueryTestSnapshot(t, s, false)
	// Abort the actual ledger6 insert after its table/triggers were created.
	if _, err := s.db.Exec(`CREATE TEMP TRIGGER saved_migration_fault BEFORE INSERT ON main.schema_migrations WHEN NEW.version=6 BEGIN SELECT RAISE(ABORT,'saved migration fault'); END`); err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	migrateErr := initializeSavedQueries(ctx, tx)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if migrateErr == nil {
		t.Fatal("migration fault did not reject ledger insert")
	}
	if _, err := s.db.Exec(`DROP TRIGGER saved_migration_fault`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name='saved_query_definitions'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("DDL survived rollback: %d %v", n, err)
	}
	if !bytes.Equal(before, savedQueryTestSnapshot(t, s, false)) {
		t.Fatal("rollback changed schema5 rows")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := UpgradeSavedQueries(canceled, s.path, testLibrary); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled upgrade: %v", err)
	}
	sentinel := errors.New("saved query directory barrier")
	if err := upgradeCatalogVersion(ctx, s.path, testLibrary, 6, func(string) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("barrier uncertainty lost: %v", err)
	}
	reopened, err := Open(s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.version != 6 || !bytes.Equal(before, savedQueryTestSnapshot(t, reopened, false)) {
		t.Fatal("uncertain committed migration lost prior rows")
	}
	if err := UpgradeCollections(ctx, s.path, testLibrary); !errors.Is(err, ErrUpgradeRequired) {
		t.Fatalf("schema6 downgrade accepted: %v", err)
	}
}

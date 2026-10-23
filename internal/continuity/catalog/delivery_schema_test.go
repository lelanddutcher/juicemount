package catalog

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Project every preexisting schema-6 table through its old columns, retaining
// SQLite storage classes and duplicate multiplicity. collection_order's new
// schema-7 column has no schema-6 counterpart; the old four columns must be
// identical after migration.
func deliveryV6Rows(t *testing.T, s *Store) []byte {
	t.Helper()
	tables := append([]string(nil), savedQueryOldTables...)
	tables = append(tables, "saved_query_definitions")
	sort.Strings(tables)
	var all []any
	for _, table := range tables {
		statement := "SELECT * FROM " + table
		switch table {
		case "collection_order":
			statement = "SELECT ordinal,legacy_sequence,owner_policy_sequence,content_sequence FROM collection_order"
		case "schema_migrations":
			statement += " WHERE version<=6"
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
			dest := make([]any, len(columns))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			cells := make([]string, len(values))
			for i, value := range values {
				switch v := value.(type) {
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
					t.Fatalf("unexpected SQLite value %T in %s", value, table)
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
	result, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func deliveryPopulatedV6(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	five, owner, authority, discovery, bearer, _ := collectionTxStore(t)
	collectionTxCall(t, discovery, bearer, collectionTxCreate(t, 401))
	issueTest(t, owner, authority, "delivery-legacy-codec1", Scope{"library", ""}, ownerSubject)
	for _, condition := range []struct{ name, query string }{
		{"legacy policy", "SELECT count(*) FROM collection_order WHERE legacy_sequence IS NOT NULL"},
		{"collection owner policy", "SELECT count(*) FROM collection_order WHERE owner_policy_sequence IS NOT NULL"},
		{"collection content", "SELECT count(*) FROM collection_order WHERE content_sequence IS NOT NULL"},
		{"codec 1", "SELECT count(*) FROM credential_codecs WHERE codec=1"},
		{"codec 2", "SELECT count(*) FROM credential_codecs WHERE codec=2"},
	} {
		var count int
		if err := five.db.QueryRow(condition.query).Scan(&count); err != nil || count == 0 {
			t.Fatalf("missing populated %s: count=%d err=%v", condition.name, count, err)
		}
	}
	if err := discovery.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := five.Close(); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeSavedQueries(ctx, five.path, testLibrary); err != nil {
		t.Fatal(err)
	}
	six, err := Open(five.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := six.Close(); err != nil {
			t.Error(err)
		}
	})
	if six.version != 6 {
		t.Fatalf("prepared version = %d", six.version)
	}
	return six
}

func TestDeliverySchemaExplicitCreateAndIdempotentUpgrade(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "delivery.sqlite")
	s, err := CreateDelivery(ctx, path, testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if s.version != 7 {
		t.Fatalf("created version = %d", s.version)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeDelivery(ctx, path, testLibrary); err != nil {
		t.Fatalf("idempotent upgrade: %v", err)
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
	if reopened.version != 7 {
		t.Fatalf("reopened version = %d", reopened.version)
	}
	var count int
	if err := reopened.db.QueryRow(`SELECT count(*) FROM byte_policy_events`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("new parent table unavailable or nonempty: count=%d err=%v", count, err)
	}
	var fingerprint string
	if err := reopened.db.QueryRow(`SELECT fingerprint FROM schema_migrations WHERE version=7`).Scan(&fingerprint); err != nil || fingerprint != schemaFingerprint(expectedSchemaFor(7)) {
		t.Fatalf("schema-7 fingerprint: %s %v", fingerprint, err)
	}
}

func TestDeliverySchemaPopulatedV6UpgradePreservesRowsAndOrder(t *testing.T) {
	ctx := context.Background()
	six := deliveryPopulatedV6(t)
	before := deliveryV6Rows(t, six)
	oldInfo, err := os.Stat(six.path)
	if err != nil {
		t.Fatal(err)
	}
	path := six.path
	if err := six.Close(); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeDelivery(ctx, path, testLibrary); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeDelivery(ctx, path, testLibrary); err != nil {
		t.Fatalf("idempotent upgrade: %v", err)
	}
	newInfo, err := os.Stat(path)
	if err != nil || !os.SameFile(oldInfo, newInfo) {
		t.Fatalf("migration replaced catalog file: %v", err)
	}
	seven, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := seven.Close(); err != nil {
			t.Error(err)
		}
	})
	if seven.version != 7 {
		t.Fatalf("upgraded version = %d", seven.version)
	}
	if !bytes.Equal(before, deliveryV6Rows(t, seven)) {
		t.Fatal("schema-6 row bytes, types, multiplicity or old chronology columns changed")
	}
	var parentCount, orphanCount, newByteOrder int
	if err := seven.db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='byte_policy_events'`).Scan(&parentCount); err != nil {
		t.Fatal(err)
	}
	if err := seven.db.QueryRow(`SELECT count(*) FROM collection_order WHERE byte_policy_sequence IS NOT NULL`).Scan(&newByteOrder); err != nil {
		t.Fatal(err)
	}
	if err := seven.db.QueryRow(`SELECT count(*) FROM collection_order WHERE (legacy_sequence IS NOT NULL)+(owner_policy_sequence IS NOT NULL)+(content_sequence IS NOT NULL)+(byte_policy_sequence IS NOT NULL)<>1`).Scan(&orphanCount); err != nil {
		t.Fatal(err)
	}
	if parentCount != 1 || newByteOrder != 0 || orphanCount != 0 {
		t.Fatalf("new parent/old chronology mismatch: parent=%d byte=%d invalid=%d", parentCount, newByteOrder, orphanCount)
	}
}

func TestDeliverySchemaLedgerFailureRollsBackParentAndRebuilds(t *testing.T) {
	ctx := context.Background()
	six := deliveryPopulatedV6(t)
	before := deliveryV6Rows(t, six)
	// This connection-local fault fires after parent creation, three table
	// rebuilds, remaining DDL, quota inserts and the user_version update.
	if _, err := six.db.Exec(`CREATE TEMP TRIGGER delivery_migration_fault BEFORE INSERT ON main.schema_migrations WHEN NEW.version=7 BEGIN SELECT RAISE(ABORT,'delivery migration fault'); END`); err != nil {
		t.Fatal(err)
	}
	tx, err := six.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	migrateErr := migrateV6Delivery(ctx, tx, testLibrary)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if migrateErr == nil || !strings.Contains(migrateErr.Error(), "delivery migration fault") {
		t.Fatalf("migration did not reach the injected late ledger fault: %v", migrateErr)
	}
	if _, err := six.db.Exec(`DROP TRIGGER delivery_migration_fault`); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, deliveryV6Rows(t, six)) {
		t.Fatal("failed migration changed schema-6 rows")
	}
	var version, newTables, newLedger int
	if err := six.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := six.db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name='byte_policy_events'`).Scan(&newTables); err != nil {
		t.Fatal(err)
	}
	if err := six.db.QueryRow(`SELECT count(*) FROM schema_migrations WHERE version=7`).Scan(&newLedger); err != nil {
		t.Fatal(err)
	}
	if version != 6 || newTables != 0 || newLedger != 0 {
		t.Fatalf("migration rollback incomplete: version=%d table=%d ledger=%d", version, newTables, newLedger)
	}
	if library, recognized, err := recognizeSchema(ctx, six.db, false); err != nil || library != testLibrary || recognized != 6 {
		t.Fatalf("rolled-back schema unrecognized: library=%s version=%d err=%v", library, recognized, err)
	}
}

func TestDeliverySchemaOwnedAttemptBindingsAndNarrowCorrection(t *testing.T) {
	f := newDeliveryClockFixture(t, 3600)
	_, selection := f.selectLatest(t, "schema-owned-selection", deliveryClockBase+600)
	prepared, _, attempt := clockPrepared(t, f, selection.SelectionID)
	if prepared.AttemptIdentity().Inode == 0 || prepared.LockIdentity().Inode == 0 {
		t.Fatal("prepared descriptor bindings missing")
	}
	var root, dir, lock, partial []byte
	if err := f.store.db.QueryRow(`SELECT root_identity,attempt_identity,lock_identity,partial_identity FROM delivery_materializations WHERE attempt_id=?`, attempt.ID).Scan(&root, &dir, &lock, &partial); err != nil {
		t.Fatal(err)
	}
	for label, value := range map[string][]byte{"root": root, "attempt directory": dir, "owner lock": lock, "partial": partial} {
		if len(value) == 0 {
			t.Fatalf("%s binding not persisted", label)
		}
	}
	if _, err := f.store.db.Exec(`UPDATE delivery_materializations SET state='COMPLETED' WHERE attempt_id=?`, attempt.ID); err == nil {
		t.Fatal("completed without recorded ready inode accepted")
	}
	if _, err := f.store.db.Exec(`UPDATE delivery_materializations SET attempt_identity=? WHERE attempt_id=?`, []byte("foreign"), attempt.ID); err == nil {
		t.Fatal("attempt directory identity edit accepted")
	}
	if _, err := f.store.db.Exec(`UPDATE delivery_materializations SET lock_identity=? WHERE attempt_id=?`, []byte("foreign"), attempt.ID); err == nil {
		t.Fatal("owner lock identity edit accepted")
	}
	if _, err := f.store.db.Exec(`UPDATE delivery_materializations SET state='COMPLETED',ready_identity=partial_identity WHERE attempt_id=?`, attempt.ID); err != nil {
		t.Fatalf("prepared completion schema transition: %v", err)
	}
	if _, err := f.store.db.Exec(`UPDATE delivery_materializations SET state='UNCERTAIN' WHERE attempt_id=?`, attempt.ID); err != nil {
		t.Fatalf("narrow completed correction: %v", err)
	}
	for _, edit := range []struct{ label, statement string }{
		{"ready inode", `UPDATE delivery_materializations SET ready_identity=? WHERE attempt_id=?`},
		{"partial inode", `UPDATE delivery_materializations SET partial_identity=? WHERE attempt_id=?`},
		{"owner lock", `UPDATE delivery_materializations SET lock_identity=? WHERE attempt_id=?`},
		{"attempt directory", `UPDATE delivery_materializations SET attempt_identity=? WHERE attempt_id=?`},
		{"root", `UPDATE delivery_materializations SET root_identity=? WHERE attempt_id=?`},
	} {
		if _, err := f.store.db.Exec(edit.statement, []byte("foreign"), attempt.ID); err == nil {
			t.Fatalf("%s changed during uncertain correction", edit.label)
		}
	}
	if _, err := f.store.db.Exec(`UPDATE delivery_materializations SET updated_unix_nano=created_unix_nano-1 WHERE attempt_id=?`, attempt.ID); err == nil {
		t.Fatal("time rollback accepted")
	}
	var state string
	var actualDir, actualLock, ready []byte
	if err := f.store.db.QueryRow(`SELECT state,attempt_identity,lock_identity,ready_identity FROM delivery_materializations WHERE attempt_id=?`, attempt.ID).Scan(&state, &actualDir, &actualLock, &ready); err != nil {
		t.Fatal(err)
	}
	if state != "UNCERTAIN" || !bytes.Equal(actualDir, dir) || !bytes.Equal(actualLock, lock) || !bytes.Equal(ready, partial) {
		t.Fatalf("identity or correction did not persist: state=%s", state)
	}
}

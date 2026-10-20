package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const applicationID = 0x4a4d4341 // JMCA
const schemaVersion = 2

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type schemaObject struct {
	Type  string
	Name  string
	Table string
	SQL   string
}

// DDL is part of the format: the ledger alone cannot bless changed constraints,
// extra tables, or missing immutable-row triggers.
var v1TableDDL = []string{
	`CREATE TABLE schema_migrations (
 version INTEGER PRIMARY KEY CHECK(version=1),
 fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64 AND fingerprint NOT GLOB '*[^0-9a-f]*')
)`,
	`CREATE TABLE catalog (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 library_id TEXT NOT NULL UNIQUE CHECK(length(library_id)=36 AND substr(library_id,1,4)='lib_' AND substr(library_id,5) NOT GLOB '*[^0-9a-f]*' AND substr(library_id,5)<>'00000000000000000000000000000000'),
 ready INTEGER NOT NULL CHECK(ready IN (0,1))
)`,
	`CREATE TABLE assets (
 asset_id TEXT PRIMARY KEY NOT NULL CHECK(length(asset_id)=38 AND substr(asset_id,1,6)='asset_' AND substr(asset_id,7) NOT GLOB '*[^0-9a-f]*' AND substr(asset_id,7)<>'00000000000000000000000000000000'),
 library_id TEXT NOT NULL REFERENCES catalog(library_id),
 label TEXT NOT NULL CHECK(length(CAST(label AS BLOB))<=4096 AND instr(label,char(0))=0),
 head TEXT,
 created_sequence INTEGER NOT NULL REFERENCES events(sequence) DEFERRABLE INITIALLY DEFERRED,
 updated_sequence INTEGER NOT NULL REFERENCES events(sequence) DEFERRABLE INITIALLY DEFERRED,
 CHECK(created_sequence>0 AND updated_sequence>=created_sequence),
 FOREIGN KEY(asset_id,head) REFERENCES revisions(asset_id,revision_id) DEFERRABLE INITIALLY DEFERRED
)`,
	`CREATE TABLE revisions (
 revision_id TEXT PRIMARY KEY NOT NULL CHECK(length(revision_id)=69 AND substr(revision_id,1,5)='rev1_' AND substr(revision_id,6) NOT GLOB '*[^0-9a-f]*'),
 library_id TEXT NOT NULL REFERENCES catalog(library_id),
 asset_id TEXT NOT NULL REFERENCES assets(asset_id),
 canonical BLOB NOT NULL CHECK(typeof(canonical)='blob' AND length(canonical) BETWEEN 95 AND 2143),
 manifest_json BLOB NOT NULL CHECK(typeof(manifest_json)='blob' AND length(manifest_json) BETWEEN 1 AND 8192),
 created_sequence INTEGER NOT NULL CHECK(created_sequence>0) REFERENCES events(sequence) DEFERRABLE INITIALLY DEFERRED,
 UNIQUE(asset_id,revision_id)
)`,
	`CREATE TABLE revision_parents (
 revision_id TEXT NOT NULL,
 parent_id TEXT NOT NULL,
 asset_id TEXT NOT NULL,
 PRIMARY KEY(revision_id,parent_id),
 CHECK(revision_id<>parent_id),
 FOREIGN KEY(asset_id,revision_id) REFERENCES revisions(asset_id,revision_id),
 FOREIGN KEY(asset_id,parent_id) REFERENCES revisions(asset_id,revision_id)
)`,
	`CREATE TABLE events (
 sequence INTEGER PRIMARY KEY CHECK(sequence>0),
 library_id TEXT NOT NULL REFERENCES catalog(library_id),
 operation_id TEXT NOT NULL UNIQUE CHECK(length(operation_id) BETWEEN 1 AND 128 AND operation_id NOT GLOB '*[^a-zA-Z0-9_-]*'),
 request BLOB NOT NULL CHECK(typeof(request)='blob' AND length(request) BETWEEN 1 AND 1048576),
 payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256)=64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
 recorded_unix_nano INTEGER NOT NULL,
 receipt BLOB NOT NULL CHECK(typeof(receipt)='blob' AND length(receipt) BETWEEN 1 AND 1024)
)`,
}

func legacySchema() []schemaObject {
	objects := make([]schemaObject, 0, len(v1TableDDL)+10)
	for _, ddl := range v1TableDDL {
		name := strings.Fields(ddl)[2]
		objects = append(objects, schemaObject{"table", name, name, ddl})
	}
	for _, table := range []string{"catalog", "schema_migrations", "revisions", "revision_parents", "events"} {
		for _, action := range []string{"UPDATE", "DELETE"} {
			name := table + "_no_" + strings.ToLower(action)
			ddl := fmt.Sprintf("CREATE TRIGGER %s BEFORE %s ON %s BEGIN SELECT RAISE(ABORT,'immutable catalog row'); END", name, action, table)
			if table == "catalog" && action == "UPDATE" {
				ddl = "CREATE TRIGGER catalog_no_update BEFORE UPDATE ON catalog WHEN OLD.ready<>0 OR NEW.ready<>1 OR NEW.singleton<>OLD.singleton OR NEW.library_id<>OLD.library_id BEGIN SELECT RAISE(ABORT,'immutable catalog row'); END"
			}
			objects = append(objects, schemaObject{"trigger", name, table, ddl})
		}
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Name < objects[j].Name })
	return objects
}

const ledgerV2DDL = `CREATE TABLE schema_migrations (
 version INTEGER PRIMARY KEY CHECK(version>=1),
 fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64 AND fingerprint NOT GLOB '*[^0-9a-f]*')
)`

func expectedSchema() []schemaObject {
	objects := legacySchema()
	for i := range objects {
		if objects[i].Name == "schema_migrations" {
			objects[i].SQL = ledgerV2DDL
		}
	}
	objects = append(objects, relationalSchema()...)
	sort.Slice(objects, func(i, j int) bool { return objects[i].Name < objects[j].Name })
	return objects
}

func schemaFingerprint(objects []schemaObject) string {
	data, _ := json.Marshal(objects)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func initialize(ctx context.Context, db *sql.DB, library continuity.LibraryID) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	tables := append([]string{ledgerV2DDL}, v1TableDDL[1:]...)
	for _, object := range relationalSchema() {
		if object.Type == "table" {
			tables = append(tables, object.SQL)
		}
	}
	for _, ddl := range tables {
		if _, err := tx.ExecContext(ctx, ddl); err != nil {
			return err
		}
	}
	for _, object := range expectedSchema() {
		if object.Type != "table" {
			if _, err := tx.ExecContext(ctx, object.SQL); err != nil {
				return err
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA application_id=0x4a4d4341`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA user_version=2`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version,fingerprint) VALUES(1,?),(2,?)`, schemaFingerprint(legacySchema()), schemaFingerprint(expectedSchema())); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO catalog(singleton,library_id,ready) VALUES(1,?,0)`, library); err != nil {
		return err
	}
	return tx.Commit()
}

func validateSchema(ctx context.Context, q queryer) (continuity.LibraryID, error) {
	return validateSchemaState(ctx, q, false)
}

func validateSchemaState(ctx context.Context, q queryer, initializing bool) (continuity.LibraryID, error) {
	library, version, err := recognizeSchema(ctx, q, initializing)
	if err == nil && version == 1 {
		return "", ErrUpgradeRequired
	}
	return library, err
}

func recognizeSchema(ctx context.Context, q queryer, initializing bool) (continuity.LibraryID, int, error) {
	var app, version int
	if err := q.QueryRowContext(ctx, `PRAGMA application_id`).Scan(&app); err != nil {
		return "", 0, formatError(err)
	}
	if err := q.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return "", 0, formatError(err)
	}
	if app != applicationID || (version != 1 && version != schemaVersion) {
		return "", 0, corrupt("unsupported application or schema version")
	}
	rows, err := q.QueryContext(ctx, `SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE NOT(type='index' AND substr(name,1,17)='sqlite_autoindex_' AND sql IS NULL) ORDER BY name`)
	if err != nil {
		return "", 0, formatError(err)
	}
	var actual []schemaObject
	for rows.Next() {
		var object schemaObject
		if err := rows.Scan(&object.Type, &object.Name, &object.Table, &object.SQL); err != nil {
			rows.Close()
			return "", 0, formatError(err)
		}
		actual = append(actual, object)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", 0, formatError(err)
	}
	expected := expectedSchema()
	if version == 1 {
		expected = legacySchema()
	}
	if len(actual) != len(expected) {
		return "", 0, corrupt("unexpected schema objects")
	}
	for i := range expected {
		if actual[i] != expected[i] {
			return "", 0, corrupt("schema DDL mismatch: " + expected[i].Name)
		}
	}
	rows, err = q.QueryContext(ctx, `SELECT version,fingerprint FROM schema_migrations ORDER BY version`)
	if err != nil {
		return "", 0, formatError(err)
	}
	ledgerCount := 0
	for rows.Next() {
		var entry int
		var fingerprint string
		if err := rows.Scan(&entry, &fingerprint); err != nil {
			rows.Close()
			return "", 0, formatError(err)
		}
		ledgerCount++
		want := schemaFingerprint(legacySchema())
		if entry == 2 {
			want = schemaFingerprint(expectedSchema())
		}
		if entry != ledgerCount || entry > version || fingerprint != want {
			rows.Close()
			return "", 0, corrupt("migration fingerprint lineage mismatch")
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", 0, formatError(err)
	}
	if ledgerCount != version {
		return "", 0, corrupt("migration ledger count")
	}
	var count, ready int
	var library continuity.LibraryID
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM catalog`).Scan(&count); err != nil {
		return "", 0, formatError(err)
	}
	if count != 1 {
		return "", 0, corrupt("missing library identity")
	}
	if err := q.QueryRowContext(ctx, `SELECT library_id,ready FROM catalog WHERE singleton=1`).Scan(&library, &ready); err != nil {
		return "", 0, formatError(err)
	}
	if (ready != 1 && !(initializing && ready == 0)) || library.Validate() != nil {
		return "", 0, corrupt("incomplete catalog or invalid library")
	}
	return library, version, nil
}

func checkIntegrity(ctx context.Context, q queryer) error {
	rows, err := q.QueryContext(ctx, `PRAGMA quick_check`)
	if err != nil {
		return formatError(err)
	}
	count := 0
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			rows.Close()
			return formatError(err)
		}
		count++
		if result != "ok" {
			rows.Close()
			return corrupt("SQLite integrity check failed")
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return formatError(err)
	}
	if count != 1 {
		return corrupt("missing SQLite integrity result")
	}
	rows, err = q.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		return formatError(err)
	}
	defer rows.Close()
	if rows.Next() {
		return corrupt("foreign key violation")
	}
	return formatError(rows.Err())
}

func formatError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrFormat, err)
}

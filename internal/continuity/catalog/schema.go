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
const authoritySchemaVersion = 3
const preferencesSchemaVersion = 4

func supportsAuthority(version int) bool {
	return version == 3 || version == 4 || version == 5 || version == 6 || version == 7 || version == 8
}
func portableSchemaVersion(version int) int {
	if supportsDeliveryLifecycle(version) { return deliverySchemaVersion }
	if version == 4 {
		return 3
	}
	return version
}

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

// expectedSchema remains the frozen v2 definition, including its ledger hash.
func expectedSchemaFor(version int) []schemaObject {
	if version == 1 {
		return legacySchema()
	}
	objects := expectedSchema()
	if supportsAuthority(version) {
		objects = append(objects, authoritySchema()...)
	}
	if supportsPreferences(version) {
		objects = append(objects, preferencesSchema()...)
	}
	if supportsCollections(version) {
		objects = append(objects, collectionsSchema()...)
	}
	if supportsSavedQueries(version) {
		objects = append(objects, savedQuerySchema()...)
	}
	if supportsDelivery(version) {
		for i := range objects {
			if replacement, ok := deliveryOverrides()[objects[i].Name]; ok {
				objects[i].SQL = replacement
			}
		}
		objects = append(objects, deliverySchema()...)
	}
	if supportsDeliveryLifecycle(version) { objects = append(objects, deliveryLifecycleSchema()...) }
	sort.Slice(objects, func(i, j int) bool { return objects[i].Name < objects[j].Name })
	return objects
}

func schemaFingerprint(objects []schemaObject) string {
	data, _ := json.Marshal(objects)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func initialize(ctx context.Context, db *sql.DB, library continuity.LibraryID) error {
	return initializeVersion(ctx, db, library, schemaVersion)
}

func initializeVersion(ctx context.Context, db *sql.DB, library continuity.LibraryID, version int) error {
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
	if supportsAuthority(version) {
		for _, object := range authoritySchema() {
			if object.Type == "table" {
				tables = append(tables, object.SQL)
			}
		}
	}
	if supportsPreferences(version) {
		for _, object := range preferencesSchema() {
			if object.Type == "table" {
				tables = append(tables, object.SQL)
			}
		}
	}
	if supportsCollections(version) {
		for _, object := range collectionsSchema() {
			if object.Type == "table" {
				tables = append(tables, object.SQL)
			}
		}
	}
	if supportsSavedQueries(version) {
		for _, object := range savedQuerySchema() {
			if object.Type == "table" {
				tables = append(tables, object.SQL)
			}
		}
	}
	for _, ddl := range tables {
		if _, err := tx.ExecContext(ctx, ddl); err != nil {
			return err
		}
	}
	initialVersion := version
	if supportsDelivery(version) { initialVersion = savedQueriesSchemaVersion }
	for _, object := range expectedSchemaFor(initialVersion) {
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
	if supportsAuthority(version) {
		if _, err := tx.ExecContext(ctx, `PRAGMA user_version=3`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version,fingerprint) VALUES(3,?)`, schemaFingerprint(expectedSchemaFor(3))); err != nil {
			return err
		}
	}
	if supportsPreferences(version) {
		if _, err := tx.ExecContext(ctx, `PRAGMA user_version=4`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version,fingerprint) VALUES(4,?)`, schemaFingerprint(expectedSchemaFor(4))); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO catalog(singleton,library_id,ready) VALUES(1,?,0)`, library); err != nil {
		return err
	}
	if supportsPreferences(version) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO preference_control(singleton,library_id,clock_floor_unix,subject_count,entry_count,receipt_count) VALUES(1,?,0,0,0,0)`, library); err != nil {
			return err
		}
	}
	if supportsCollections(version) {
		if err := initializeCollections(ctx, tx, string(library), false); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `PRAGMA user_version=5`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version,fingerprint) VALUES(5,?)`, schemaFingerprint(expectedSchemaFor(5))); err != nil {
			return err
		}
	}
	if supportsSavedQueries(version) {
		if _, err := tx.ExecContext(ctx, `PRAGMA user_version=6`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version,fingerprint) VALUES(6,?)`, schemaFingerprint(expectedSchemaFor(6))); err != nil {
			return err
		}
	}
	if supportsDelivery(version) {
		if err := migrateV6Delivery(ctx, tx, library); err != nil { return err }
	}
	if supportsDeliveryLifecycle(version) {
		if err := initializeDeliveryLifecycle(ctx, tx, library); err != nil { return err }
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
	if app != applicationID || (version != 1 && version != schemaVersion && !supportsAuthority(version)) {
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
	expected := expectedSchemaFor(version)
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
		if entry == 3 {
			want = schemaFingerprint(expectedSchemaFor(3))
		}
		if entry == 4 {
			want = schemaFingerprint(expectedSchemaFor(4))
		}
		if entry == 5 {
			want = schemaFingerprint(expectedSchemaFor(5))
		}
		if entry == 6 {
			want = schemaFingerprint(expectedSchemaFor(6))
		}
		if entry == 7 {
			want = schemaFingerprint(expectedSchemaFor(7))
		}
		if entry == 8 { want = schemaFingerprint(expectedSchemaFor(8)) }
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
	if supportsPreferences(version) {
		if err := checkPreferences(ctx, q, library); err != nil {
			return "", 0, err
		}
	}
	if supportsCollections(version) && ready == 1 {
		if err := checkCollectionState(ctx, q, library); err != nil {
			return "", 0, err
		}
	}
	if supportsDeliveryLifecycle(version) {
		if err := checkDeliveryLifecycleState(ctx, q, library); err != nil { return "", 0, err }
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

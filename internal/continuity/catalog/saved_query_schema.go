package catalog

import (
	"context"
	"database/sql"
)

const savedQueriesSchemaVersion = 6

func supportsSavedQueries(v int) bool { return v == savedQueriesSchemaVersion }

// The additive objects preserve every earlier schema fingerprint.
func savedQuerySchema() []schemaObject {
	return []schemaObject{
		{"trigger", "collection_definitions_saved_query_disjoint", "collection_definitions", `CREATE TRIGGER collection_definitions_saved_query_disjoint BEFORE INSERT ON collection_definitions WHEN EXISTS(SELECT 1 FROM saved_query_definitions WHERE collection_id=NEW.collection_id) BEGIN SELECT RAISE(ABORT,'collection family collision'); END`},
		{"trigger", "collection_entries_saved_query_family", "collection_entries", `CREATE TRIGGER collection_entries_saved_query_family BEFORE INSERT ON collection_entries WHEN EXISTS(SELECT 1 FROM saved_query_definitions WHERE collection_id=NEW.collection_id) BEGIN SELECT RAISE(ABORT,'saved query has no stored entries'); END`},
		{"table", "saved_query_definitions", "saved_query_definitions", `CREATE TABLE saved_query_definitions (collection_id TEXT PRIMARY KEY NOT NULL REFERENCES collection_identities(collection_id),kind TEXT NOT NULL CHECK(kind='saved_query'),name TEXT NOT NULL CHECK(length(CAST(name AS BLOB)) BETWEEN 1 AND 256 AND instr(name,char(0))=0),archived INTEGER NOT NULL CHECK(archived IN(0,1)),definition_version TEXT NOT NULL CHECK(length(definition_version)=35 AND substr(definition_version,1,3)='cv_' AND substr(definition_version,4) NOT GLOB '*[^0-9a-f]*' AND substr(definition_version,4)<>'00000000000000000000000000000000'),created_sequence INTEGER NOT NULL REFERENCES collection_events(sequence) DEFERRABLE INITIALLY DEFERRED,updated_sequence INTEGER NOT NULL REFERENCES collection_events(sequence) DEFERRABLE INITIALLY DEFERRED,definition BLOB NOT NULL CHECK(typeof(definition)='blob' AND length(definition) BETWEEN 1 AND 8192),CHECK(created_sequence>0 AND updated_sequence>=created_sequence))`},
		{"trigger", "saved_query_definitions_disjoint", "saved_query_definitions", `CREATE TRIGGER saved_query_definitions_disjoint BEFORE INSERT ON saved_query_definitions WHEN EXISTS(SELECT 1 FROM collection_definitions WHERE collection_id=NEW.collection_id) OR EXISTS(SELECT 1 FROM collection_entries WHERE collection_id=NEW.collection_id) BEGIN SELECT RAISE(ABORT,'saved query family collision'); END`},
		{"trigger", "saved_query_definitions_identity", "saved_query_definitions", `CREATE TRIGGER saved_query_definitions_identity BEFORE UPDATE ON saved_query_definitions WHEN NEW.collection_id<>OLD.collection_id OR NEW.kind<>OLD.kind OR NEW.created_sequence<>OLD.created_sequence BEGIN SELECT RAISE(ABORT,'immutable saved query identity'); END`},
		{"trigger", "saved_query_definitions_no_delete", "saved_query_definitions", `CREATE TRIGGER saved_query_definitions_no_delete BEFORE DELETE ON saved_query_definitions BEGIN SELECT RAISE(ABORT,'saved query lifetime identity'); END`},
	}
}

func initializeSavedQueries(ctx context.Context, tx *sql.Tx) error {
	for _, kind := range []string{"table", "trigger"} {
		for _, o := range savedQuerySchema() {
			if o.Type == kind {
				if _, err := tx.ExecContext(ctx, o.SQL); err != nil {
					return err
				}
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA user_version=6`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version,fingerprint) VALUES(6,?)`, schemaFingerprint(expectedSchemaFor(6)))
	return err
}

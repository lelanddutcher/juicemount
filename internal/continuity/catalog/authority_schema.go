package catalog

import (
	"fmt"
	"strings"
)

func authoritySchema() []schemaObject {
	ddls := []string{
		`CREATE TABLE policy_events (
 sequence INTEGER PRIMARY KEY CHECK(sequence>0),
 library_id TEXT NOT NULL REFERENCES catalog(library_id),
 operation_id TEXT NOT NULL UNIQUE CHECK(length(operation_id) BETWEEN 1 AND 128 AND operation_id NOT GLOB '*[^a-zA-Z0-9_-]*'),
 request BLOB NOT NULL CHECK(typeof(request)='blob' AND length(request) BETWEEN 1 AND 1048576),
 payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256)=64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
 recorded_unix_nano INTEGER NOT NULL,
 receipt BLOB NOT NULL CHECK(typeof(receipt)='blob' AND length(receipt) BETWEEN 1 AND 1024)
)`,
		`CREATE TABLE policy_subjects (
 subject_id TEXT PRIMARY KEY NOT NULL CHECK(length(subject_id)=36 AND substr(subject_id,1,4)='sub_' AND substr(subject_id,5) NOT GLOB '*[^0-9a-f]*' AND substr(subject_id,5)<>'00000000000000000000000000000000'),
 library_id TEXT NOT NULL REFERENCES catalog(library_id),
 kind TEXT NOT NULL CHECK(kind IN ('person','service')),
 label TEXT NOT NULL CHECK(length(CAST(label AS BLOB))<=256 AND instr(label,char(0))=0),
 enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
 authorization_generation INTEGER NOT NULL CHECK(authorization_generation>0),
 created_sequence INTEGER NOT NULL REFERENCES policy_events(sequence) DEFERRABLE INITIALLY DEFERRED,
 updated_sequence INTEGER NOT NULL REFERENCES policy_events(sequence) DEFERRABLE INITIALLY DEFERRED,
 CHECK(created_sequence>0 AND updated_sequence>=created_sequence)
)`,
		`CREATE TABLE policy_grants (
 grant_id TEXT PRIMARY KEY NOT NULL CHECK(length(grant_id) BETWEEN 1 AND 128 AND grant_id NOT GLOB '*[^a-zA-Z0-9_-]*'),
 library_id TEXT NOT NULL REFERENCES catalog(library_id),
 subject_id TEXT NOT NULL REFERENCES policy_subjects(subject_id),
 scope_kind TEXT NOT NULL CHECK(scope_kind IN ('library','asset')),
 asset_id TEXT REFERENCES assets(asset_id),
 actions BLOB NOT NULL CHECK(typeof(actions)='blob' AND length(actions) BETWEEN 1 AND 256),
 created_sequence INTEGER NOT NULL REFERENCES policy_events(sequence) DEFERRABLE INITIALLY DEFERRED,
 updated_sequence INTEGER NOT NULL REFERENCES policy_events(sequence) DEFERRABLE INITIALLY DEFERRED,
 removed INTEGER NOT NULL CHECK(removed IN (0,1)),
 CHECK((scope_kind='library' AND asset_id IS NULL) OR (scope_kind='asset' AND asset_id IS NOT NULL)),
 CHECK(created_sequence>0 AND updated_sequence>=created_sequence)
)`,
		`CREATE TABLE catalog_authority (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 library_id TEXT NOT NULL REFERENCES catalog(library_id),
 incarnation TEXT NOT NULL CHECK(length(incarnation)=37 AND substr(incarnation,1,5)='auth_' AND substr(incarnation,6) NOT GLOB '*[^0-9a-f]*' AND substr(incarnation,6)<>'00000000000000000000000000000000'),
 owner_subject_id TEXT NOT NULL REFERENCES policy_subjects(subject_id)
)`,
		`CREATE TABLE catalog_admin_events (
 sequence INTEGER PRIMARY KEY CHECK(sequence>0),
 library_id TEXT NOT NULL REFERENCES catalog(library_id),
 operation_id TEXT NOT NULL UNIQUE CHECK(length(operation_id) BETWEEN 1 AND 128 AND operation_id NOT GLOB '*[^a-zA-Z0-9_-]*'),
 request BLOB NOT NULL CHECK(typeof(request)='blob' AND length(request) BETWEEN 1 AND 1048576),
 payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256)=64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
 recorded_unix_nano INTEGER NOT NULL,
 receipt BLOB NOT NULL CHECK(typeof(receipt)='blob' AND length(receipt) BETWEEN 1 AND 2048)
)`,
		`CREATE TABLE catalog_credentials (
 selector TEXT PRIMARY KEY NOT NULL CHECK(length(selector)=37 AND substr(selector,1,5)='cred_' AND substr(selector,6) NOT GLOB '*[^0-9a-f]*' AND substr(selector,6)<>'00000000000000000000000000000000'),
 library_id TEXT NOT NULL REFERENCES catalog(library_id),
 incarnation TEXT NOT NULL,
 subject_id TEXT NOT NULL REFERENCES policy_subjects(subject_id),
 scope_kind TEXT NOT NULL CHECK(scope_kind IN ('library','asset')),
 asset_id TEXT REFERENCES assets(asset_id),
 actions BLOB NOT NULL CHECK(typeof(actions)='blob' AND length(actions) BETWEEN 1 AND 256),
 verifier BLOB NOT NULL CHECK(typeof(verifier)='blob' AND length(verifier)=32),
 issued_unix INTEGER NOT NULL,
 expires_unix INTEGER NOT NULL CHECK(expires_unix>issued_unix),
 generation INTEGER NOT NULL CHECK(generation>0),
 revoked INTEGER NOT NULL CHECK(revoked IN (0,1)),
 CHECK((scope_kind='library' AND asset_id IS NULL) OR (scope_kind='asset' AND asset_id IS NOT NULL))
)`,
		`CREATE TABLE credential_issuance_receipts (
 operation_id TEXT PRIMARY KEY NOT NULL REFERENCES catalog_admin_events(operation_id) DEFERRABLE INITIALLY DEFERRED,
 selector TEXT NOT NULL UNIQUE REFERENCES catalog_credentials(selector)
)`,
	}
	var objects []schemaObject
	for _, ddl := range ddls {
		name := strings.Fields(ddl)[2]
		objects = append(objects, schemaObject{"table", name, name, ddl})
	}
	for _, table := range []string{"policy_events", "catalog_admin_events", "credential_issuance_receipts"} {
		for _, action := range []string{"UPDATE", "DELETE"} {
			name := table + "_no_" + strings.ToLower(action)
			objects = append(objects, schemaObject{"trigger", name, table, fmt.Sprintf("CREATE TRIGGER %s BEFORE %s ON %s BEGIN SELECT RAISE(ABORT,'immutable authority row'); END", name, action, table)})
		}
	}
	for _, guard := range []struct{ table, condition string }{
		{"policy_subjects", "NEW.subject_id<>OLD.subject_id OR NEW.library_id<>OLD.library_id OR NEW.kind<>OLD.kind OR NEW.created_sequence<>OLD.created_sequence"},
		{"policy_grants", "NEW.grant_id<>OLD.grant_id OR NEW.library_id<>OLD.library_id OR NEW.subject_id<>OLD.subject_id OR NEW.created_sequence<>OLD.created_sequence"},
		{"catalog_authority", "NEW.singleton<>OLD.singleton OR NEW.library_id<>OLD.library_id OR NEW.owner_subject_id<>OLD.owner_subject_id"},
		{"catalog_credentials", "NEW.selector<>OLD.selector OR NEW.library_id<>OLD.library_id OR NEW.incarnation<>OLD.incarnation OR NEW.subject_id<>OLD.subject_id OR NEW.scope_kind<>OLD.scope_kind OR NEW.asset_id IS NOT OLD.asset_id OR NEW.actions<>OLD.actions OR NEW.verifier<>OLD.verifier OR NEW.issued_unix<>OLD.issued_unix OR NEW.expires_unix<>OLD.expires_unix OR OLD.revoked<>0 OR NEW.revoked<>1 OR NEW.generation<>OLD.generation+1"},
	} {
		name := guard.table + "_identity"
		objects = append(objects, schemaObject{"trigger", name, guard.table, fmt.Sprintf("CREATE TRIGGER %s BEFORE UPDATE ON %s WHEN %s BEGIN SELECT RAISE(ABORT,'immutable authority identity'); END", name, guard.table, guard.condition)})
		name = guard.table + "_no_delete"
		objects = append(objects, schemaObject{"trigger", name, guard.table, fmt.Sprintf("CREATE TRIGGER %s BEFORE DELETE ON %s BEGIN SELECT RAISE(ABORT,'authority lifetime identity'); END", name, guard.table)})
	}
	objects = append(objects, schemaObject{"index", "policy_grants_access", "policy_grants", `CREATE INDEX policy_grants_access ON policy_grants(subject_id,removed,scope_kind,asset_id)`})
	return objects
}

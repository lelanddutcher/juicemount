package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const deliverySchemaVersion = 7

func supportsDelivery(v int) bool { return v == deliverySchemaVersion }

// Only these three constrained tables change in schema 7. Prior schema
// definitions and fingerprints remain byte-for-byte unchanged.
func deliveryOverrides() map[string]string {
	var old = map[string]string{}
	for _, o := range collectionsSchema() {
		if o.Name == "credential_codecs" || o.Name == "catalog_admin_codecs" || o.Name == "collection_order" {
			old[o.Name] = o.SQL
		}
	}
	return map[string]string{
		"credential_codecs":    strings.Replace(old["credential_codecs"], "codec IN(1,2)", "codec IN(1,2,3)", 1),
		"catalog_admin_codecs": strings.Replace(old["catalog_admin_codecs"], "'collection_owner1'", "'collection_owner1','issue3','byte_owner1'", 1),
		"collection_order":     strings.Replace(strings.Replace(old["collection_order"], "content_sequence INTEGER UNIQUE REFERENCES collection_events(sequence),", "content_sequence INTEGER UNIQUE REFERENCES collection_events(sequence),byte_policy_sequence INTEGER UNIQUE REFERENCES byte_policy_events(sequence),", 1), "+(content_sequence IS NOT NULL)=1", "+(content_sequence IS NOT NULL)+(byte_policy_sequence IS NOT NULL)=1", 1),
	}
}

func deliverySchema() []schemaObject {
	ddls := []string{
		`CREATE TABLE byte_grants (
 grant_id TEXT PRIMARY KEY NOT NULL CHECK(length(grant_id)=36 AND substr(grant_id,1,4)='bgr_' AND substr(grant_id,5) NOT GLOB '*[^0-9a-f]*' AND substr(grant_id,5)<>'00000000000000000000000000000000'),
 library_id TEXT NOT NULL REFERENCES catalog(library_id),subject_id TEXT NOT NULL REFERENCES policy_subjects(subject_id),scope_kind TEXT NOT NULL CHECK(scope_kind IN('library','asset')),asset_id TEXT REFERENCES assets(asset_id),action TEXT NOT NULL CHECK(action='bytes.read'),grant_version TEXT NOT NULL CHECK(length(grant_version)=36 AND substr(grant_version,1,4)='bgv_' AND substr(grant_version,5) NOT GLOB '*[^0-9a-f]*' AND substr(grant_version,5)<>'00000000000000000000000000000000'),removed INTEGER NOT NULL CHECK(removed IN(0,1)),created_sequence INTEGER NOT NULL REFERENCES byte_policy_events(sequence) DEFERRABLE INITIALLY DEFERRED,updated_sequence INTEGER NOT NULL REFERENCES byte_policy_events(sequence) DEFERRABLE INITIALLY DEFERRED,CHECK((scope_kind='library' AND asset_id IS NULL) OR (scope_kind='asset' AND asset_id IS NOT NULL)),CHECK(created_sequence>0 AND updated_sequence>=created_sequence))`,
		`CREATE TABLE byte_policy_events (
 sequence INTEGER PRIMARY KEY CHECK(sequence>0),library_id TEXT NOT NULL REFERENCES catalog(library_id),operator_subject_id TEXT NOT NULL REFERENCES policy_subjects(subject_id),operation_id TEXT NOT NULL CHECK(length(operation_id) BETWEEN 1 AND 128 AND operation_id NOT GLOB '*[^a-zA-Z0-9_-]*'),request BLOB NOT NULL CHECK(typeof(request)='blob' AND length(request) BETWEEN 1 AND 32768),payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256)=64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),receipt BLOB NOT NULL CHECK(typeof(receipt)='blob' AND length(receipt) BETWEEN 1 AND 2048),recorded_unix_nano INTEGER NOT NULL CHECK(recorded_unix_nano>0),UNIQUE(library_id,operation_id))`,
		`CREATE TABLE byte_policy_results (
 policy_sequence INTEGER NOT NULL REFERENCES byte_policy_events(sequence) DEFERRABLE INITIALLY DEFERRED,grant_id TEXT NOT NULL REFERENCES byte_grants(grant_id) DEFERRABLE INITIALLY DEFERRED,result_version TEXT NOT NULL CHECK(length(result_version)=36 AND substr(result_version,1,4)='bgv_' AND substr(result_version,5) NOT GLOB '*[^0-9a-f]*' AND substr(result_version,5)<>'00000000000000000000000000000000'),removed INTEGER NOT NULL CHECK(removed IN(0,1)),PRIMARY KEY(policy_sequence,grant_id),UNIQUE(grant_id,result_version))`,
		`CREATE TABLE byte_policy_quota (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),library_id TEXT NOT NULL REFERENCES catalog(library_id),lifetime_grants INTEGER NOT NULL CHECK(lifetime_grants>=0),events INTEGER NOT NULL CHECK(events>=0),canonical_bytes INTEGER NOT NULL CHECK(canonical_bytes>=0),reserved_removals INTEGER NOT NULL CHECK(reserved_removals>=0))`,
		`CREATE TABLE delivery_sources (
 source_id TEXT NOT NULL CHECK(length(source_id)=37 AND substr(source_id,1,5)='dsrc_' AND substr(source_id,6) NOT GLOB '*[^0-9a-f]*' AND substr(source_id,6)<>'00000000000000000000000000000000'),source_version INTEGER NOT NULL CHECK(source_version>0),library_id TEXT NOT NULL REFERENCES catalog(library_id),asset_id TEXT NOT NULL REFERENCES assets(asset_id),revision_id TEXT NOT NULL REFERENCES revisions(revision_id),content_size INTEGER NOT NULL CHECK(content_size BETWEEN 0 AND 1073741824),content_sha256 TEXT NOT NULL CHECK(length(content_sha256)=64 AND content_sha256 NOT GLOB '*[^0-9a-f]*'),authority_incarnation TEXT NOT NULL,route_id TEXT NOT NULL CHECK(length(route_id) BETWEEN 1 AND 128),route_digest TEXT NOT NULL CHECK(length(route_digest)=64 AND route_digest NOT GLOB '*[^0-9a-f]*'),reader_leaf_sha256 TEXT NOT NULL CHECK(length(reader_leaf_sha256)=64 AND reader_leaf_sha256 NOT GLOB '*[^0-9a-f]*'),image_view BLOB NOT NULL CHECK(typeof(image_view)='blob' AND length(image_view) BETWEEN 1 AND 524288),enabled INTEGER NOT NULL CHECK(enabled IN(0,1)),created_unix_nano INTEGER NOT NULL CHECK(created_unix_nano>0),PRIMARY KEY(source_id,source_version),UNIQUE(library_id,asset_id,revision_id,source_version))`,
		`CREATE TABLE delivery_source_heads (
 library_id TEXT NOT NULL REFERENCES catalog(library_id),asset_id TEXT NOT NULL REFERENCES assets(asset_id),revision_id TEXT NOT NULL REFERENCES revisions(revision_id),source_id TEXT NOT NULL,source_version INTEGER NOT NULL CHECK(source_version>0),PRIMARY KEY(library_id,asset_id,revision_id),FOREIGN KEY(source_id,source_version) REFERENCES delivery_sources(source_id,source_version))`,
		`CREATE TABLE delivery_source_events (
 operation_id TEXT PRIMARY KEY NOT NULL CHECK(length(operation_id) BETWEEN 1 AND 128 AND operation_id NOT GLOB '*[^a-zA-Z0-9_-]*'),library_id TEXT NOT NULL REFERENCES catalog(library_id),request BLOB NOT NULL CHECK(typeof(request)='blob' AND length(request) BETWEEN 1 AND 32768),payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256)=64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),receipt BLOB NOT NULL CHECK(typeof(receipt)='blob' AND length(receipt) BETWEEN 1 AND 2048),source_id TEXT NOT NULL,source_version INTEGER NOT NULL,recorded_unix_nano INTEGER NOT NULL CHECK(recorded_unix_nano>0),FOREIGN KEY(source_id,source_version) REFERENCES delivery_sources(source_id,source_version))`,
		`CREATE TABLE delivery_selections (
 selection_id TEXT PRIMARY KEY NOT NULL CHECK(length(selection_id)=37 AND substr(selection_id,1,5)='dsel_' AND substr(selection_id,6) NOT GLOB '*[^0-9a-f]*' AND substr(selection_id,6)<>'00000000000000000000000000000000'),library_id TEXT NOT NULL REFERENCES catalog(library_id),subject_id TEXT NOT NULL REFERENCES policy_subjects(subject_id),operation_id TEXT NOT NULL CHECK(length(operation_id) BETWEEN 1 AND 128 AND operation_id NOT GLOB '*[^a-zA-Z0-9_-]*'),request BLOB NOT NULL CHECK(typeof(request)='blob' AND length(request) BETWEEN 1 AND 32768),payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256)=64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),asset_id TEXT NOT NULL REFERENCES assets(asset_id),revision_id TEXT NOT NULL REFERENCES revisions(revision_id),source_id TEXT NOT NULL,source_version INTEGER NOT NULL,credential_selector TEXT NOT NULL REFERENCES catalog_credentials(selector),credential_generation INTEGER NOT NULL CHECK(credential_generation>0),authority_incarnation TEXT NOT NULL,subject_generation INTEGER NOT NULL CHECK(subject_generation>0),content_size INTEGER NOT NULL CHECK(content_size BETWEEN 0 AND 1073741824),content_sha256 TEXT NOT NULL,expires_unix INTEGER NOT NULL,byte_budget INTEGER NOT NULL CHECK(byte_budget>=0),canceled INTEGER NOT NULL CHECK(canceled IN(0,1)),created_unix_nano INTEGER NOT NULL CHECK(created_unix_nano>0),UNIQUE(library_id,subject_id,operation_id),FOREIGN KEY(source_id,source_version) REFERENCES delivery_sources(source_id,source_version))`,
		`CREATE TABLE delivery_materializations (
 attempt_id TEXT PRIMARY KEY NOT NULL CHECK(length(attempt_id)=37 AND substr(attempt_id,1,5)='dmat_' AND substr(attempt_id,6) NOT GLOB '*[^0-9a-f]*' AND substr(attempt_id,6)<>'00000000000000000000000000000000'),selection_id TEXT NOT NULL REFERENCES delivery_selections(selection_id),operation_id TEXT NOT NULL CHECK(length(operation_id) BETWEEN 1 AND 128 AND operation_id NOT GLOB '*[^a-zA-Z0-9_-]*'),state TEXT NOT NULL CHECK(state IN('RESERVED','PREPARED','COMPLETED','UNCERTAIN')),root_identity BLOB,attempt_identity BLOB,lock_identity BLOB,partial_identity BLOB,ready_identity BLOB,content_size INTEGER NOT NULL CHECK(content_size BETWEEN 0 AND 1073741824),content_sha256 TEXT NOT NULL,created_unix_nano INTEGER NOT NULL CHECK(created_unix_nano>0),updated_unix_nano INTEGER NOT NULL CHECK(updated_unix_nano>=created_unix_nano),UNIQUE(selection_id,operation_id),CHECK((state='RESERVED' AND root_identity IS NULL AND attempt_identity IS NULL AND lock_identity IS NULL AND partial_identity IS NULL AND ready_identity IS NULL) OR (state='PREPARED' AND root_identity IS NOT NULL AND attempt_identity IS NOT NULL AND lock_identity IS NOT NULL AND partial_identity IS NOT NULL AND ready_identity IS NULL) OR (state IN('COMPLETED','UNCERTAIN') AND root_identity IS NOT NULL AND attempt_identity IS NOT NULL AND lock_identity IS NOT NULL AND partial_identity IS NOT NULL AND (state='UNCERTAIN' OR ready_identity IS NOT NULL) AND (ready_identity IS NULL OR ready_identity=partial_identity))))`,
		`CREATE TABLE delivery_clock (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),library_id TEXT NOT NULL REFERENCES catalog(library_id),high_water_unix INTEGER NOT NULL CHECK(high_water_unix>=0))`,
		`CREATE TABLE delivery_quota (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),library_id TEXT NOT NULL REFERENCES catalog(library_id),source_versions INTEGER NOT NULL CHECK(source_versions BETWEEN 0 AND 1024),selections INTEGER NOT NULL CHECK(selections BETWEEN 0 AND 4096),attempts INTEGER NOT NULL CHECK(attempts BETWEEN 0 AND 4096),canonical_bytes INTEGER NOT NULL CHECK(canonical_bytes BETWEEN 0 AND 67108864),reserved_logical_bytes INTEGER NOT NULL CHECK(reserved_logical_bytes BETWEEN 0 AND 4294967296))`,
	}
	objects := make([]schemaObject, 0, len(ddls)+40)
	for _, ddl := range ddls {
		name := strings.Fields(ddl)[2]
		objects = append(objects, schemaObject{"table", name, name, ddl})
	}
	for _, table := range []string{"byte_policy_events", "byte_policy_results", "delivery_source_events"} {
		for _, action := range []string{"UPDATE", "DELETE"} {
			name := table + "_no_" + strings.ToLower(action)
			objects = append(objects, schemaObject{"trigger", name, table, fmt.Sprintf("CREATE TRIGGER %s BEFORE %s ON %s BEGIN SELECT RAISE(ABORT,'immutable delivery row'); END", name, action, table)})
		}
	}
	for _, table := range []string{"byte_grants", "delivery_sources", "delivery_selections", "delivery_materializations", "delivery_source_heads", "byte_policy_quota", "delivery_clock", "delivery_quota"} {
		name := table + "_no_delete"
		objects = append(objects, schemaObject{"trigger", name, table, fmt.Sprintf("CREATE TRIGGER %s BEFORE DELETE ON %s BEGIN SELECT RAISE(ABORT,'delivery lifetime identity'); END", name, table)})
	}
	objects = append(objects, schemaObject{"trigger", "byte_grants_identity", "byte_grants", `CREATE TRIGGER byte_grants_identity BEFORE UPDATE ON byte_grants WHEN NEW.grant_id<>OLD.grant_id OR NEW.library_id<>OLD.library_id OR NEW.subject_id<>OLD.subject_id OR NEW.scope_kind<>OLD.scope_kind OR NEW.asset_id IS NOT OLD.asset_id OR NEW.action<>OLD.action OR NEW.created_sequence<>OLD.created_sequence OR NEW.updated_sequence<=OLD.updated_sequence OR OLD.removed=1 AND NEW.removed=1 BEGIN SELECT RAISE(ABORT,'immutable byte grant identity'); END`})
	objects = append(objects, schemaObject{"trigger", "delivery_sources_no_update", "delivery_sources", `CREATE TRIGGER delivery_sources_no_update BEFORE UPDATE ON delivery_sources BEGIN SELECT RAISE(ABORT,'immutable source version'); END`})
	objects = append(objects, schemaObject{"trigger", "delivery_source_heads_identity", "delivery_source_heads", `CREATE TRIGGER delivery_source_heads_identity BEFORE UPDATE ON delivery_source_heads WHEN NEW.library_id<>OLD.library_id OR NEW.asset_id<>OLD.asset_id OR NEW.revision_id<>OLD.revision_id OR NEW.source_id<>OLD.source_id OR NEW.source_version<>OLD.source_version+1 BEGIN SELECT RAISE(ABORT,'source head CAS'); END`})
	objects = append(objects, schemaObject{"trigger", "delivery_selections_identity", "delivery_selections", `CREATE TRIGGER delivery_selections_identity BEFORE UPDATE ON delivery_selections WHEN NEW.selection_id<>OLD.selection_id OR NEW.library_id<>OLD.library_id OR NEW.subject_id<>OLD.subject_id OR NEW.operation_id<>OLD.operation_id OR NEW.request<>OLD.request OR NEW.payload_sha256<>OLD.payload_sha256 OR NEW.asset_id<>OLD.asset_id OR NEW.revision_id<>OLD.revision_id OR NEW.source_id<>OLD.source_id OR NEW.source_version<>OLD.source_version OR NEW.credential_selector<>OLD.credential_selector OR NEW.credential_generation<>OLD.credential_generation OR NEW.authority_incarnation<>OLD.authority_incarnation OR NEW.subject_generation<>OLD.subject_generation OR NEW.content_size<>OLD.content_size OR NEW.content_sha256<>OLD.content_sha256 OR NEW.expires_unix<>OLD.expires_unix OR NEW.byte_budget<>OLD.byte_budget OR NEW.created_unix_nano<>OLD.created_unix_nano OR OLD.canceled<>0 OR NEW.canceled<>1 BEGIN SELECT RAISE(ABORT,'immutable selection identity'); END`})
	objects = append(objects, schemaObject{"trigger", "delivery_materializations_identity", "delivery_materializations", `CREATE TRIGGER delivery_materializations_identity BEFORE UPDATE ON delivery_materializations WHEN NEW.attempt_id<>OLD.attempt_id OR NEW.selection_id<>OLD.selection_id OR NEW.operation_id<>OLD.operation_id OR NEW.content_size<>OLD.content_size OR NEW.content_sha256<>OLD.content_sha256 OR NEW.created_unix_nano<>OLD.created_unix_nano OR NEW.updated_unix_nano<OLD.updated_unix_nano OR (OLD.state='RESERVED' AND NEW.state NOT IN('RESERVED','PREPARED')) OR (OLD.state='PREPARED' AND NEW.state NOT IN('PREPARED','COMPLETED','UNCERTAIN')) OR (OLD.state='COMPLETED' AND NEW.state<>'UNCERTAIN') OR (OLD.state='UNCERTAIN' AND NEW.state NOT IN('UNCERTAIN','COMPLETED')) OR (NOT (OLD.state='RESERVED' AND NEW.state='PREPARED') AND (NEW.root_identity IS NOT OLD.root_identity OR NEW.attempt_identity IS NOT OLD.attempt_identity OR NEW.lock_identity IS NOT OLD.lock_identity OR NEW.partial_identity IS NOT OLD.partial_identity)) OR (NOT (OLD.state IN('PREPARED','UNCERTAIN') AND NEW.state IN('COMPLETED','UNCERTAIN')) AND NEW.ready_identity IS NOT OLD.ready_identity) BEGIN SELECT RAISE(ABORT,'immutable materialization identity'); END`})
	objects = append(objects, schemaObject{"trigger", "delivery_clock_floor", "delivery_clock", `CREATE TRIGGER delivery_clock_floor BEFORE UPDATE ON delivery_clock WHEN NEW.singleton<>OLD.singleton OR NEW.library_id<>OLD.library_id OR NEW.high_water_unix<OLD.high_water_unix BEGIN SELECT RAISE(ABORT,'delivery clock rollback'); END`})
	objects = append(objects, schemaObject{"trigger", "delivery_quota_identity", "delivery_quota", `CREATE TRIGGER delivery_quota_identity BEFORE UPDATE ON delivery_quota WHEN NEW.singleton<>OLD.singleton OR NEW.library_id<>OLD.library_id OR NEW.source_versions<OLD.source_versions OR NEW.selections<OLD.selections OR NEW.attempts<OLD.attempts OR NEW.canonical_bytes<OLD.canonical_bytes OR NEW.reserved_logical_bytes<OLD.reserved_logical_bytes BEGIN SELECT RAISE(ABORT,'delivery quota rollback'); END`})
	objects = append(objects, schemaObject{"index", "byte_grants_access", "byte_grants", `CREATE INDEX byte_grants_access ON byte_grants(subject_id,removed,scope_kind,asset_id)`})
	objects = append(objects, schemaObject{"index", "delivery_sources_revision", "delivery_sources", `CREATE INDEX delivery_sources_revision ON delivery_sources(library_id,asset_id,revision_id,enabled,source_version)`})
	objects = append(objects, schemaObject{"index", "delivery_materializations_selection", "delivery_materializations", `CREATE INDEX delivery_materializations_selection ON delivery_materializations(selection_id,state)`})
	return objects
}

func migrateV6Delivery(ctx context.Context, tx *sql.Tx, library continuity.LibraryID) error {
	objects := deliverySchema()
	// collection_order's new foreign key must have its parent before any
	// preserved chronology row is inserted into the rebuilt table. Create
	// exactly the schema-7 parent definition, then skip it in the later pass.
	parentCreated := false
	for _, object := range objects {
		if object.Type == "table" && object.Name == "byte_policy_events" {
			if _, err := tx.ExecContext(ctx, object.SQL); err != nil {
				return err
			}
			parentCreated = true
			break
		}
	}
	if !parentCreated {
		return corrupt("missing byte policy parent definition")
	}
	overrides := deliveryOverrides()
	columns := map[string]string{
		"credential_codecs":    "selector,codec",
		"catalog_admin_codecs": "operation_id,codec",
		"collection_order":     "ordinal,legacy_sequence,owner_policy_sequence,content_sequence",
	}
	for _, table := range []string{"credential_codecs", "catalog_admin_codecs", "collection_order"} {
		if _, err := tx.ExecContext(ctx, "CREATE TEMP TABLE saved_"+table+" AS SELECT "+columns[table]+" FROM "+table); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DROP TABLE "+table); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, overrides[table]); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+table+"("+columns[table]+") SELECT "+columns[table]+" FROM saved_"+table); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DROP TABLE saved_"+table); err != nil {
			return err
		}
	}
	for _, table := range []string{"credential_codecs", "catalog_admin_codecs", "collection_order"} {
		for _, object := range collectionsSchema() {
			if object.Table == table && object.Type == "trigger" {
				if _, err := tx.ExecContext(ctx, object.SQL); err != nil {
					return err
				}
			}
		}
	}
	for _, kind := range []string{"table", "index", "trigger"} {
		for _, object := range objects {
			if object.Type == kind && !(kind == "table" && object.Name == "byte_policy_events") {
				if _, err := tx.ExecContext(ctx, object.SQL); err != nil {
					return err
				}
			}
		}
	}
	for _, stmt := range []string{
		`INSERT INTO byte_policy_quota(singleton,library_id,lifetime_grants,events,canonical_bytes,reserved_removals) VALUES(1,?,0,0,0,0)`,
		`INSERT INTO delivery_clock(singleton,library_id,high_water_unix) VALUES(1,?,0)`,
		`INSERT INTO delivery_quota(singleton,library_id,source_versions,selections,attempts,canonical_bytes,reserved_logical_bytes) VALUES(1,?,0,0,0,0,0)`,
	} {
		if _, err := tx.ExecContext(ctx, stmt, library); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA user_version=7`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version,fingerprint) VALUES(7,?)`, schemaFingerprint(expectedSchemaFor(7)))
	return err
}

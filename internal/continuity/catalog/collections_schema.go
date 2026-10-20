package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const collectionsSchemaVersion = 5

func supportsPreferences(v int) bool { return v == 4 || v == 5 }
func supportsCollections(v int) bool { return v == 5 }

func collectionSQLID(column, prefix string) string {
	return fmt.Sprintf("CHECK(length(%s)=%d AND substr(%s,1,%d)='%s' AND substr(%s,%d) NOT GLOB '*[^0-9a-f]*' AND substr(%s,%d)<>'00000000000000000000000000000000')", column, len(prefix)+32, column, len(prefix), prefix, column, len(prefix)+1, column, len(prefix)+1)
}

const collectionQuotaColumns = `collections,active_entries,lifetime_entries,lifetime_grants,content_events,content_receipts,policy_events,policy_receipts,canonical_bytes,reserved_policy_events,reserved_policy_receipts,reserved_bytes`

func collectionQuotaDDL() string {
	var columns []string
	for _, n := range strings.Split(collectionQuotaColumns, ",") {
		columns = append(columns, n+" INTEGER NOT NULL CHECK("+n+">=0)")
	}
	return strings.Join(columns, ",") + `,CHECK(active_entries<=lifetime_entries),CHECK(content_events=content_receipts),CHECK(policy_events=policy_receipts),CHECK(reserved_policy_events=reserved_policy_receipts),CHECK(reserved_policy_events<=lifetime_grants),CHECK(reserved_bytes=3072*reserved_policy_events)`
}
func collectionsSchema() []schemaObject {
	id := collectionSQLID
	ddls := []string{
		`CREATE TABLE credential_codecs (selector TEXT PRIMARY KEY REFERENCES catalog_credentials(selector),codec INTEGER NOT NULL CHECK(codec IN(1,2)))`,
		`CREATE TABLE catalog_admin_codecs (operation_id TEXT PRIMARY KEY REFERENCES catalog_admin_events(operation_id),codec TEXT NOT NULL CHECK(codec IN('admin1','issue2','collection_owner1')))`,
		`CREATE TABLE collection_identities (collection_id TEXT PRIMARY KEY NOT NULL ` + id("collection_id", "col_") + `,library_id TEXT NOT NULL REFERENCES catalog(library_id),creator_subject_id TEXT NOT NULL ` + id("creator_subject_id", "sub_") + `,creation_operation_id TEXT NOT NULL ` + id("creation_operation_id", "cop_") + `,creation_payload_sha256 TEXT NOT NULL CHECK(length(creation_payload_sha256)=64 AND creation_payload_sha256 NOT GLOB '*[^0-9a-f]*'),policy_generation INTEGER NOT NULL CHECK(policy_generation>=0),UNIQUE(library_id,creator_subject_id,creation_operation_id))`,
		`CREATE TABLE collection_definitions (collection_id TEXT PRIMARY KEY REFERENCES collection_identities(collection_id),kind TEXT NOT NULL CHECK(kind='manual'),name TEXT NOT NULL CHECK(length(CAST(name AS BLOB)) BETWEEN 1 AND 256 AND instr(name,char(0))=0),archived INTEGER NOT NULL CHECK(archived IN(0,1)),definition_version TEXT NOT NULL ` + id("definition_version", "cv_") + `,created_sequence INTEGER NOT NULL REFERENCES collection_events(sequence) DEFERRABLE INITIALLY DEFERRED,updated_sequence INTEGER NOT NULL REFERENCES collection_events(sequence) DEFERRABLE INITIALLY DEFERRED,CHECK(created_sequence>0 AND updated_sequence>=created_sequence))`,
		`CREATE TABLE collection_entries (entry_id TEXT PRIMARY KEY NOT NULL ` + id("entry_id", "cent_") + `,library_id TEXT NOT NULL REFERENCES catalog(library_id),collection_id TEXT NOT NULL REFERENCES collection_identities(collection_id),asset_id TEXT NOT NULL REFERENCES assets(asset_id),removed INTEGER NOT NULL CHECK(removed IN(0,1)),created_sequence INTEGER NOT NULL REFERENCES collection_events(sequence) DEFERRABLE INITIALLY DEFERRED,updated_sequence INTEGER NOT NULL REFERENCES collection_events(sequence) DEFERRABLE INITIALLY DEFERRED,CHECK(created_sequence>0 AND updated_sequence>=created_sequence))`,
		`CREATE TABLE collection_events (sequence INTEGER PRIMARY KEY CHECK(sequence>0),library_id TEXT NOT NULL REFERENCES catalog(library_id),actor_subject_id TEXT NOT NULL REFERENCES policy_subjects(subject_id),operation_id TEXT NOT NULL ` + id("operation_id", "cop_") + `,collection_id TEXT NOT NULL REFERENCES collection_identities(collection_id),request BLOB NOT NULL CHECK(typeof(request)='blob' AND length(request) BETWEEN 1 AND 32768),payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256)=64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),receipt BLOB NOT NULL CHECK(typeof(receipt)='blob' AND length(receipt) BETWEEN 1 AND 1024),result_version TEXT NOT NULL ` + id("result_version", "cv_") + `,generated_policy_sequence INTEGER UNIQUE REFERENCES collection_policy_events(sequence) DEFERRABLE INITIALLY DEFERRED,recorded_unix_nano INTEGER NOT NULL CHECK(recorded_unix_nano>0),UNIQUE(library_id,actor_subject_id,operation_id),UNIQUE(collection_id,result_version))`,
		`CREATE TABLE collection_policy_events (sequence INTEGER PRIMARY KEY CHECK(sequence>0),library_id TEXT NOT NULL REFERENCES catalog(library_id),administration TEXT NOT NULL CHECK(administration IN('local_owner','collection_service')),namespace_subject_id TEXT NOT NULL,operator_subject_id TEXT NOT NULL REFERENCES policy_subjects(subject_id),operation_id TEXT NOT NULL ` + id("operation_id", "cpo_") + `,category TEXT NOT NULL CHECK(category IN('library_create','collection','security_remove')),collection_id TEXT REFERENCES collection_identities(collection_id),request BLOB NOT NULL CHECK(typeof(request)='blob' AND length(request) BETWEEN 1 AND 32768 AND (category<>'security_remove' OR length(request)<=2048)),payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256)=64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),receipt BLOB NOT NULL CHECK(typeof(receipt)='blob' AND length(receipt) BETWEEN 1 AND 1024),recorded_unix_nano INTEGER NOT NULL CHECK(recorded_unix_nano>0),CHECK((administration='local_owner' AND namespace_subject_id='') OR (administration='collection_service' AND namespace_subject_id=operator_subject_id)),UNIQUE(library_id,administration,namespace_subject_id,operation_id))`,
		`CREATE TABLE collection_grants (grant_id TEXT PRIMARY KEY NOT NULL ` + id("grant_id", "cgr_") + `,library_id TEXT NOT NULL REFERENCES catalog(library_id),subject_id TEXT NOT NULL REFERENCES policy_subjects(subject_id),scope_kind TEXT NOT NULL CHECK(scope_kind IN('library_create','collection')),collection_id TEXT REFERENCES collection_identities(collection_id),actions BLOB NOT NULL CHECK(typeof(actions)='blob' AND length(actions) BETWEEN 1 AND 128),grant_version TEXT NOT NULL ` + id("grant_version", "cgv_") + `,removed INTEGER NOT NULL CHECK(removed IN(0,1)),created_sequence INTEGER NOT NULL REFERENCES collection_policy_events(sequence) DEFERRABLE INITIALLY DEFERRED,updated_sequence INTEGER NOT NULL REFERENCES collection_policy_events(sequence) DEFERRABLE INITIALLY DEFERRED,CHECK((scope_kind='library_create' AND collection_id IS NULL) OR (scope_kind='collection' AND collection_id IS NOT NULL)),CHECK(created_sequence>0 AND updated_sequence>=created_sequence))`,
		`CREATE TABLE collection_policy_results (policy_sequence INTEGER NOT NULL REFERENCES collection_policy_events(sequence) DEFERRABLE INITIALLY DEFERRED,grant_id TEXT NOT NULL REFERENCES collection_grants(grant_id) DEFERRABLE INITIALLY DEFERRED,result_version TEXT NOT NULL ` + id("result_version", "cgv_") + `,removed INTEGER NOT NULL CHECK(removed IN(0,1)),PRIMARY KEY(policy_sequence,grant_id),UNIQUE(grant_id,result_version))`,
		`CREATE TABLE collection_order (ordinal INTEGER PRIMARY KEY CHECK(ordinal>0),legacy_sequence INTEGER UNIQUE REFERENCES policy_events(sequence),owner_policy_sequence INTEGER UNIQUE REFERENCES collection_policy_events(sequence),content_sequence INTEGER UNIQUE REFERENCES collection_events(sequence),CHECK((legacy_sequence IS NOT NULL)+(owner_policy_sequence IS NOT NULL)+(content_sequence IS NOT NULL)=1))`,
		`CREATE TABLE collection_library_quota (singleton INTEGER PRIMARY KEY CHECK(singleton=1),library_id TEXT NOT NULL REFERENCES catalog(library_id),` + collectionQuotaDDL() + `)`,
		`CREATE TABLE collection_quota (collection_id TEXT PRIMARY KEY REFERENCES collection_identities(collection_id),` + collectionQuotaDDL() + `,CHECK(collections=0))`,
	}
	var objects []schemaObject
	for _, ddl := range ddls {
		name := strings.Fields(ddl)[2]
		objects = append(objects, schemaObject{"table", name, name, ddl})
	}
	indexes := []struct{ name, table, sql string }{
		{"collection_entries_active", "collection_entries", `CREATE UNIQUE INDEX collection_entries_active ON collection_entries(collection_id,asset_id) WHERE removed=0`},
		{"collection_entries_page", "collection_entries", `CREATE INDEX collection_entries_page ON collection_entries(collection_id,removed,entry_id)`},
		{"collection_grants_access", "collection_grants", `CREATE INDEX collection_grants_access ON collection_grants(subject_id,removed,scope_kind,collection_id)`},
		{"collection_grants_roster", "collection_grants", `CREATE INDEX collection_grants_roster ON collection_grants(collection_id,removed,grant_id)`},
		{"collection_events_history", "collection_events", `CREATE INDEX collection_events_history ON collection_events(collection_id,sequence)`},
		{"collection_policy_history", "collection_policy_events", `CREATE INDEX collection_policy_history ON collection_policy_events(collection_id,sequence)`},
	}
	for _, v := range indexes {
		objects = append(objects, schemaObject{"index", v.name, v.table, v.sql})
	}
	for _, table := range []string{"credential_codecs", "catalog_admin_codecs", "collection_events", "collection_policy_events", "collection_policy_results", "collection_order"} {
		for _, action := range []string{"UPDATE", "DELETE"} {
			name := table + "_no_" + strings.ToLower(action)
			objects = append(objects, schemaObject{"trigger", name, table, fmt.Sprintf("CREATE TRIGGER %s BEFORE %s ON %s BEGIN SELECT RAISE(ABORT,'immutable collection row'); END", name, action, table)})
		}
	}
	for _, g := range []struct{ table, condition string }{
		{"collection_identities", "NEW.collection_id<>OLD.collection_id OR NEW.library_id<>OLD.library_id OR NEW.creator_subject_id<>OLD.creator_subject_id OR NEW.creation_operation_id<>OLD.creation_operation_id OR NEW.creation_payload_sha256<>OLD.creation_payload_sha256 OR NEW.policy_generation<>OLD.policy_generation+1"},
		{"collection_definitions", "NEW.collection_id<>OLD.collection_id OR NEW.kind<>OLD.kind OR NEW.created_sequence<>OLD.created_sequence"},
		{"collection_entries", "NEW.entry_id<>OLD.entry_id OR NEW.library_id<>OLD.library_id OR NEW.collection_id<>OLD.collection_id OR NEW.asset_id<>OLD.asset_id OR NEW.created_sequence<>OLD.created_sequence"},
		{"collection_grants", "NEW.grant_id<>OLD.grant_id OR NEW.library_id<>OLD.library_id OR NEW.subject_id<>OLD.subject_id OR NEW.scope_kind<>OLD.scope_kind OR NEW.collection_id IS NOT OLD.collection_id OR NEW.created_sequence<>OLD.created_sequence"},
	} {
		name := g.table + "_identity"
		objects = append(objects, schemaObject{"trigger", name, g.table, fmt.Sprintf("CREATE TRIGGER %s BEFORE UPDATE ON %s WHEN %s BEGIN SELECT RAISE(ABORT,'immutable collection identity'); END", name, g.table, g.condition)})
		name = g.table + "_no_delete"
		objects = append(objects, schemaObject{"trigger", name, g.table, fmt.Sprintf("CREATE TRIGGER %s BEFORE DELETE ON %s BEGIN SELECT RAISE(ABORT,'collection lifetime identity'); END", name, g.table)})
	}
	for _, g := range []struct{ table, condition string }{
		{"collection_library_quota", "NEW.singleton<>OLD.singleton OR NEW.library_id<>OLD.library_id"},
		{"collection_quota", "NEW.collection_id<>OLD.collection_id"},
	} {
		condition := g.condition
		for _, column := range []string{"collections", "lifetime_entries", "lifetime_grants", "content_events", "content_receipts", "policy_events", "policy_receipts", "canonical_bytes"} {
			condition += " OR NEW." + column + "<OLD." + column
		}
		name := g.table + "_identity"
		objects = append(objects, schemaObject{"trigger", name, g.table, fmt.Sprintf("CREATE TRIGGER %s BEFORE UPDATE ON %s WHEN %s BEGIN SELECT RAISE(ABORT,'collection quota identity'); END", name, g.table, condition)})
		name = g.table + "_no_delete"
		objects = append(objects, schemaObject{"trigger", name, g.table, fmt.Sprintf("CREATE TRIGGER %s BEFORE DELETE ON %s BEGIN SELECT RAISE(ABORT,'collection quota lifetime'); END", name, g.table)})
	}
	objects = append(objects, schemaObject{"trigger", "collection_results_bound", "collection_policy_results", `CREATE TRIGGER collection_results_bound BEFORE INSERT ON collection_policy_results WHEN (SELECT count(*) FROM collection_policy_results WHERE policy_sequence=NEW.policy_sequence)>=16 BEGIN SELECT RAISE(ABORT,'collection result bound'); END`})
	objects = append(objects, schemaObject{"trigger", "collection_order_append", "collection_order", `CREATE TRIGGER collection_order_append BEFORE INSERT ON collection_order WHEN NEW.ordinal<>(SELECT COALESCE(max(ordinal),0)+1 FROM collection_order) OR (NEW.owner_policy_sequence IS NOT NULL AND NOT EXISTS(SELECT 1 FROM collection_policy_events WHERE sequence=NEW.owner_policy_sequence AND administration='local_owner')) BEGIN SELECT RAISE(ABORT,'collection order append'); END`})
	return objects
}

func initializeCollections(ctx context.Context, tx *sql.Tx, library string, backfill bool) error {
	if backfill {
		for _, kind := range []string{"table", "index", "trigger"} {
			for _, o := range collectionsSchema() {
				if o.Type == kind {
					if _, err := tx.ExecContext(ctx, o.SQL); err != nil {
						return err
					}
				}
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO collection_library_quota(singleton,library_id,`+collectionQuotaColumns+`) VALUES(1,?,0,0,0,0,0,0,0,0,0,0,0,0)`, library); err != nil {
		return err
	}
	if backfill {
		for _, statement := range []string{`INSERT INTO credential_codecs(selector,codec) SELECT selector,1 FROM catalog_credentials`, `INSERT INTO catalog_admin_codecs(operation_id,codec) SELECT operation_id,'admin1' FROM catalog_admin_events`, `INSERT INTO collection_order(ordinal,legacy_sequence) SELECT sequence,sequence FROM policy_events ORDER BY sequence`} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) checkVersion(ctx context.Context, q queryer) error {
	version, err := catalogVersion(ctx, q)
	if err != nil {
		return err
	}
	if version != s.version {
		return ErrUpgradeRequired
	}
	return nil
}

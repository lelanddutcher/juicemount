package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const deliveryLifecycleSchemaVersion = 8
const MaxTerminalRequestBytes = 1024
const MaxTerminalReceiptBytes = 2048
const TerminalReservationBytes int64 = 3072
const MaxTerminalReservedBytes int64 = 15728640

func supportsDeliveryLifecycle(v int) bool { return v == deliveryLifecycleSchemaVersion }

// Schema8 only adds operational custody. All schema7 objects remain exact.
func deliveryLifecycleSchema() []schemaObject {
	ddls := []string{
		`CREATE TABLE delivery_terminal_quota (singleton INTEGER PRIMARY KEY CHECK(singleton=1),library_id TEXT NOT NULL REFERENCES catalog(library_id),source_slots INTEGER NOT NULL CHECK(source_slots BETWEEN 0 AND 1024),selection_slots INTEGER NOT NULL CHECK(selection_slots BETWEEN 0 AND 4096),used_bytes INTEGER NOT NULL CHECK(used_bytes>=0 AND used_bytes<=3072*(source_slots+selection_slots)))`,
		`CREATE TABLE delivery_terminal_slots (
 library_id TEXT NOT NULL REFERENCES catalog(library_id),target_kind TEXT NOT NULL CHECK(target_kind IN('source_disable','selection_cancel')),source_id TEXT,source_version INTEGER,selection_id TEXT UNIQUE,terminal_bit INTEGER NOT NULL CHECK(terminal_bit IN(0,1)),legacy_terminal INTEGER NOT NULL CHECK(legacy_terminal IN(0,1)),operation_id TEXT CHECK(operation_id IS NULL OR (length(operation_id) BETWEEN 1 AND 128 AND operation_id NOT GLOB '*[^a-zA-Z0-9_-]*')),actor_subject_id TEXT REFERENCES policy_subjects(subject_id),authority_incarnation TEXT,recorded_unix_nano INTEGER,request BLOB CHECK(request IS NULL OR (typeof(request)='blob' AND length(request) BETWEEN 1 AND 1024)),payload_sha256 TEXT,receipt BLOB CHECK(receipt IS NULL OR (typeof(receipt)='blob' AND length(receipt) BETWEEN 1 AND 2048)),
 UNIQUE(source_id,source_version),FOREIGN KEY(source_id,source_version) REFERENCES delivery_sources(source_id,source_version),FOREIGN KEY(selection_id,terminal_bit) REFERENCES delivery_selections(selection_id,canceled) DEFERRABLE INITIALLY DEFERRED,
 CHECK((target_kind='source_disable' AND source_id IS NOT NULL AND source_version>0 AND selection_id IS NULL) OR (target_kind='selection_cancel' AND source_id IS NULL AND source_version IS NULL AND selection_id IS NOT NULL)),
 CHECK((operation_id IS NULL AND actor_subject_id IS NULL AND authority_incarnation IS NULL AND recorded_unix_nano IS NULL AND request IS NULL AND payload_sha256 IS NULL AND receipt IS NULL AND terminal_bit=legacy_terminal) OR (operation_id IS NOT NULL AND actor_subject_id IS NOT NULL AND authority_incarnation IS NOT NULL AND length(authority_incarnation)=37 AND substr(authority_incarnation,1,5)='auth_' AND substr(authority_incarnation,6) NOT GLOB '*[^0-9a-f]*' AND substr(authority_incarnation,6)<>'00000000000000000000000000000000' AND recorded_unix_nano IS NOT NULL AND recorded_unix_nano>0 AND request IS NOT NULL AND payload_sha256 IS NOT NULL AND length(payload_sha256)=64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*' AND receipt IS NOT NULL AND terminal_bit=1 AND legacy_terminal=0)))`,
	}
	objects := []schemaObject{}
	for _, ddl := range ddls { name := strings.Fields(ddl)[2]; objects = append(objects, schemaObject{"table", name, name, ddl}) }
	objects = append(objects,
		schemaObject{"index", "delivery_selection_terminal_pair", "delivery_selections", `CREATE UNIQUE INDEX delivery_selection_terminal_pair ON delivery_selections(selection_id,canceled)`},
		schemaObject{"index", "delivery_source_terminal_operations", "delivery_terminal_slots", `CREATE UNIQUE INDEX delivery_source_terminal_operations ON delivery_terminal_slots(library_id,operation_id) WHERE target_kind='source_disable' AND operation_id IS NOT NULL`},
		schemaObject{"index", "delivery_selection_terminal_operations", "delivery_terminal_slots", `CREATE UNIQUE INDEX delivery_selection_terminal_operations ON delivery_terminal_slots(library_id,actor_subject_id,operation_id) WHERE target_kind='selection_cancel' AND operation_id IS NOT NULL`},
		schemaObject{"trigger", "delivery_terminal_slots_no_delete", "delivery_terminal_slots", `CREATE TRIGGER delivery_terminal_slots_no_delete BEFORE DELETE ON delivery_terminal_slots BEGIN SELECT RAISE(ABORT,'terminal custody retained'); END`},
		schemaObject{"trigger", "delivery_terminal_quota_no_delete", "delivery_terminal_quota", `CREATE TRIGGER delivery_terminal_quota_no_delete BEFORE DELETE ON delivery_terminal_quota BEGIN SELECT RAISE(ABORT,'terminal quota retained'); END`},
		schemaObject{"trigger", "delivery_terminal_slots_insert", "delivery_terminal_slots", `CREATE TRIGGER delivery_terminal_slots_insert BEFORE INSERT ON delivery_terminal_slots WHEN NEW.operation_id IS NOT NULL OR (NEW.target_kind='source_disable' AND NOT EXISTS(SELECT 1 FROM delivery_sources WHERE source_id=NEW.source_id AND source_version=NEW.source_version AND library_id=NEW.library_id AND NEW.legacy_terminal=1-enabled)) OR (NEW.target_kind='selection_cancel' AND NOT EXISTS(SELECT 1 FROM delivery_selections WHERE selection_id=NEW.selection_id AND library_id=NEW.library_id AND canceled=NEW.legacy_terminal)) BEGIN SELECT RAISE(ABORT,'terminal reservation binding'); END`},
		schemaObject{"trigger", "delivery_terminal_slots_fill", "delivery_terminal_slots", `CREATE TRIGGER delivery_terminal_slots_fill BEFORE UPDATE ON delivery_terminal_slots WHEN OLD.operation_id IS NOT NULL OR OLD.legacy_terminal<>0 OR OLD.terminal_bit<>0 OR NEW.operation_id IS NULL OR NEW.terminal_bit<>1 OR NEW.library_id<>OLD.library_id OR NEW.target_kind<>OLD.target_kind OR NEW.source_id IS NOT OLD.source_id OR NEW.source_version IS NOT OLD.source_version OR NEW.selection_id IS NOT OLD.selection_id OR NEW.legacy_terminal<>OLD.legacy_terminal OR NOT EXISTS(SELECT 1 FROM policy_subjects WHERE subject_id=NEW.actor_subject_id AND library_id=NEW.library_id) OR (NEW.target_kind='selection_cancel' AND NOT EXISTS(SELECT 1 FROM delivery_selections WHERE selection_id=NEW.selection_id AND library_id=NEW.library_id AND subject_id=NEW.actor_subject_id)) BEGIN SELECT RAISE(ABORT,'immutable terminal custody'); END`},
		schemaObject{"trigger", "delivery_terminal_slots_reserved", "delivery_terminal_slots", `CREATE TRIGGER delivery_terminal_slots_reserved AFTER INSERT ON delivery_terminal_slots BEGIN UPDATE delivery_terminal_quota SET source_slots=source_slots+(NEW.target_kind='source_disable'),selection_slots=selection_slots+(NEW.target_kind='selection_cancel') WHERE singleton=1 AND library_id=NEW.library_id; SELECT CASE WHEN changes()<>1 THEN RAISE(ABORT,'missing terminal quota') END; END`},
		schemaObject{"trigger", "delivery_terminal_slots_charged", "delivery_terminal_slots", `CREATE TRIGGER delivery_terminal_slots_charged AFTER UPDATE ON delivery_terminal_slots BEGIN UPDATE delivery_terminal_quota SET used_bytes=used_bytes+length(NEW.request)+length(NEW.receipt) WHERE singleton=1 AND library_id=NEW.library_id; SELECT CASE WHEN changes()<>1 THEN RAISE(ABORT,'missing terminal quota') END; END`},
		schemaObject{"trigger", "delivery_terminal_quota_monotonic", "delivery_terminal_quota", `CREATE TRIGGER delivery_terminal_quota_monotonic BEFORE UPDATE ON delivery_terminal_quota WHEN NEW.singleton<>OLD.singleton OR NEW.library_id<>OLD.library_id OR NEW.source_slots<OLD.source_slots OR NEW.selection_slots<OLD.selection_slots OR NEW.used_bytes<OLD.used_bytes BEGIN SELECT RAISE(ABORT,'terminal quota rollback'); END`},
		schemaObject{"trigger", "delivery_source_terminal_reserve", "delivery_sources", `CREATE TRIGGER delivery_source_terminal_reserve AFTER INSERT ON delivery_sources BEGIN INSERT INTO delivery_terminal_slots(library_id,target_kind,source_id,source_version,terminal_bit,legacy_terminal) VALUES(NEW.library_id,'source_disable',NEW.source_id,NEW.source_version,1-NEW.enabled,1-NEW.enabled); END`},
		schemaObject{"trigger", "delivery_selection_terminal_reserve", "delivery_selections", `CREATE TRIGGER delivery_selection_terminal_reserve AFTER INSERT ON delivery_selections BEGIN INSERT INTO delivery_terminal_slots(library_id,target_kind,selection_id,terminal_bit,legacy_terminal) VALUES(NEW.library_id,'selection_cancel',NEW.selection_id,NEW.canceled,NEW.canceled); END`},
	)
	return objects
}

func initializeDeliveryLifecycle(ctx context.Context, tx *sql.Tx, library continuity.LibraryID) error {
	// Parent uniqueness must exist before inserting deferred paired FK rows.
	for _, kind := range []string{"table", "index", "trigger"} {
		for _, o := range deliveryLifecycleSchema() { if o.Type == kind { if _, err := tx.ExecContext(ctx, o.SQL); err != nil { return err } } }
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO delivery_terminal_quota VALUES(1,?,0,0,0)`, library); err != nil { return err }
	for _, statement := range []string{
		`INSERT INTO delivery_terminal_slots(library_id,target_kind,source_id,source_version,terminal_bit,legacy_terminal) SELECT library_id,'source_disable',source_id,source_version,1-enabled,1-enabled FROM delivery_sources`,
		`INSERT INTO delivery_terminal_slots(library_id,target_kind,selection_id,terminal_bit,legacy_terminal) SELECT library_id,'selection_cancel',selection_id,canceled,canceled FROM delivery_selections`,
	} { if _, err := tx.ExecContext(ctx, statement); err != nil { return err } }
	if err := checkDeliveryLifecycleState(ctx, tx, library); err != nil { return err }
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version,fingerprint) VALUES(8,?)`, schemaFingerprint(expectedSchemaFor(8))); err != nil { return err }
	_, err := tx.ExecContext(ctx, `PRAGMA user_version=8`)
	return err
}

func checkDeliveryLifecycleState(ctx context.Context, q queryer, library continuity.LibraryID) error {
	var sources, selections, used, actualSources, actualSelections, slotsSource, slotsSelection, actualUsed, bad int64
	var lib continuity.LibraryID
	var normalSources, normalSelections int64
	if err := q.QueryRowContext(ctx, `SELECT source_versions,selections FROM delivery_quota WHERE singleton=1 AND library_id=?`, library).Scan(&normalSources, &normalSelections); err != nil { return corrupt("normal delivery quota fields") }
	if err := q.QueryRowContext(ctx, `SELECT library_id,source_slots,selection_slots,used_bytes,(SELECT count(*) FROM delivery_sources),(SELECT count(*) FROM delivery_selections),(SELECT count(*) FROM delivery_terminal_slots WHERE target_kind='source_disable'),(SELECT count(*) FROM delivery_terminal_slots WHERE target_kind='selection_cancel'),(SELECT COALESCE(sum(length(request)+length(receipt)),0) FROM delivery_terminal_slots),(SELECT count(*) FROM delivery_terminal_slots t LEFT JOIN delivery_sources d ON t.source_id=d.source_id AND t.source_version=d.source_version LEFT JOIN delivery_selections s ON s.selection_id=t.selection_id WHERE t.library_id<>? OR (t.target_kind='source_disable' AND (d.library_id IS NULL OR d.library_id<>t.library_id OR t.legacy_terminal<>1-d.enabled)) OR (t.target_kind='selection_cancel' AND (s.library_id IS NULL OR s.library_id<>t.library_id OR s.canceled<>t.terminal_bit OR (t.operation_id IS NOT NULL AND t.actor_subject_id<>s.subject_id)))) FROM delivery_terminal_quota WHERE singleton=1`, library).Scan(&lib, &sources, &selections, &used, &actualSources, &actualSelections, &slotsSource, &slotsSelection, &actualUsed, &bad); err != nil { return corrupt("terminal quota fields") }
	if lib != library || sources != normalSources || selections != normalSelections || sources != actualSources || sources != slotsSource || selections != actualSelections || selections != slotsSelection || used != actualUsed || sources < 0 || sources > 1024 || selections < 0 || selections > 4096 || used < 0 || used > TerminalReservationBytes*(sources+selections) || bad != 0 { return corrupt("terminal reservation or coupling identity") }
	// Every committed scalar/request/receipt link is checked, bounded one row at
	// a time; no operational row enters a portable projection.
	return walkRows(ctx, q, `SELECT `+terminalSlotColumns+` FROM delivery_terminal_slots WHERE operation_id IS NOT NULL`, func(rows *sql.Rows) error {
		_, err := scanTerminalSlot(rows, library)
		return err
	})
}

func (s *Store) requireLifecycle(ctx context.Context, q queryer) error {
	if !supportsDeliveryLifecycle(s.version) { return ErrUpgradeRequired }
	return s.checkVersion(ctx, q)
}

// CreateDeliveryLifecycle is explicit: older creation targets keep their meaning.
func CreateDeliveryLifecycle(ctx context.Context, path string, library continuity.LibraryID) (*Store, error) {
	if library.Validate() != nil { return nil, invalid("library identity") }
	return openStoreVersionContext(ctx, path, library, true, true, deliveryLifecycleSchemaVersion)
}
func UpgradeDeliveryLifecycle(ctx context.Context, path string, library continuity.LibraryID) error {
	return upgradeCatalogVersion(ctx, path, library, deliveryLifecycleSchemaVersion, syncParent)
}

func terminalCommitError(kind string, err error) error { return fmt.Errorf("%s commit outcome uncertain: %w", kind, err) }

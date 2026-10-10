package metadata

import (
	"database/sql"
	"fmt"
	"strings"
)

// spoolSchema defines the spool_entries table.
//
// One row per file-write-in-progress or file-pending-upload. Lives in the
// same SQLite database as the entries cache so they share a WAL and the
// same writeMu serialization. Independent of the entries table — the
// spool models in-flight transfer state, not durable file metadata.
//
// drain_state lifecycle (canonical transitions only):
//
//	writing  -> ready    (writer closed the file cleanly)
//	writing  -> failed   (crash mid-write, surfaced by boot-time scrubber)
//	ready    -> draining (drainer picked up)
//	draining -> done     (copied to FUSE and SHA-verified)
//	draining -> ready    (drainer was interrupted; boot scrubber resets)
//	draining -> failed   (drain attempts exhausted)
//	failed   -> ready    (operator manual retry)
//
// done rows are retained briefly (audit trail) then garbage-collected by
// the drainer on a separate sweep — they carry no in-flight state.
const spoolSchema = `
CREATE TABLE IF NOT EXISTS spool_entries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    nfs_path        TEXT NOT NULL,
    spool_file      TEXT NOT NULL UNIQUE,
    size            INTEGER NOT NULL DEFAULT 0,
    sha256          BLOB,
    drain_state     TEXT NOT NULL CHECK(drain_state IN ('writing','ready','draining','done','failed')),
    drain_attempts  INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    suspect_zero_tail TEXT,
    punched_end     INTEGER NOT NULL DEFAULT 0,
    base_ino        INTEGER NOT NULL DEFAULT 0,
    base_mtime_ns   INTEGER NOT NULL DEFAULT 0,
    base_ctime_ns   INTEGER NOT NULL DEFAULT 0,
    base_len        INTEGER NOT NULL DEFAULT 0,
    accepted_epoch  INTEGER NOT NULL DEFAULT 0,
    wait_project    TEXT NOT NULL DEFAULT '',
    clone_hold      TEXT NOT NULL DEFAULT '',
    pause_since_ns  INTEGER NOT NULL DEFAULT 0,
    retry_after_ns  INTEGER NOT NULL DEFAULT 0,
    qualification_hold TEXT NOT NULL DEFAULT '',
    retired_by      INTEGER NOT NULL DEFAULT 0,
    replacement_witness BLOB,
    replacement_publication_required INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_spool_ready_fifo ON spool_entries(drain_state, created_at);
CREATE INDEX IF NOT EXISTS idx_spool_path ON spool_entries(nfs_path);
-- Accepted replacement publication is local durable intent, not queue dispatch.
-- Keep terminal rows so a retired predecessor's exact operation remains auditable.
CREATE TABLE IF NOT EXISTS spool_replacement_publications (
    spool_id INTEGER PRIMARY KEY,
    operation_id TEXT NOT NULL UNIQUE,
    phase TEXT NOT NULL CHECK(phase IN ('queued','intent','arming','armed','committed','settled','cleaning','cleaned','superseded')),
    payload BLOB NOT NULL
);
-- Refusal is separate from the immutable publication journal and private
-- image. A wait may be rescheduled; writer_changed is terminal for this save.
CREATE TABLE IF NOT EXISTS spool_replacement_dispositions (
    spool_id INTEGER PRIMARY KEY,
    operation_id TEXT NOT NULL,
    witness_sha256 TEXT NOT NULL,
    journal_revision INTEGER NOT NULL,
    journal_payload BLOB NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('waiting_writer','writer_changed')),
    observed_principal TEXT NOT NULL DEFAULT '',
    observed_epoch INTEGER NOT NULL CHECK(observed_epoch > 0),
    updated_at INTEGER NOT NULL
);
-- Local completion acknowledgement only; deliberately separate from the
-- birth-bound media publication/custody protocol.
CREATE TABLE IF NOT EXISTS spool_completion_holds (
    completion_id TEXT PRIMARY KEY,
    spool_id INTEGER NOT NULL UNIQUE,
    nfs_path TEXT NOT NULL,
    spool_file TEXT NOT NULL UNIQUE,
    size INTEGER NOT NULL CHECK(size >= 0),
    kind TEXT NOT NULL DEFAULT 'legacy',
    birth_id TEXT NOT NULL DEFAULT '',
    birth_identity TEXT NOT NULL DEFAULT '',
    incarnation TEXT NOT NULL DEFAULT '',
    expected_revision INTEGER NOT NULL DEFAULT 0,
    backing_identity TEXT NOT NULL DEFAULT '',
    source_sha256 BLOB,
    published_revision INTEGER NOT NULL DEFAULT 0,
    original_inode INTEGER NOT NULL DEFAULT 0,
    original_mode INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_spool_completion_path ON spool_completion_holds(nfs_path);
-- A bound completion's local effects and bounded offline sidecar image remain
-- generation-keyed after its private source and completion hold retire.
CREATE TABLE IF NOT EXISTS bound_completion_effects (
    completion_id TEXT PRIMARY KEY,
    spool_id INTEGER NOT NULL UNIQUE,
    birth_id TEXT NOT NULL,
    nfs_path TEXT NOT NULL,
    incarnation TEXT NOT NULL,
    published_revision INTEGER NOT NULL,
    original_inode INTEGER NOT NULL,
    size INTEGER NOT NULL CHECK(size >= 0),
    source_sha256 BLOB NOT NULL,
    sidecar_body BLOB CHECK(sidecar_body IS NULL OR length(sidecar_body)<=131072),
	phase TEXT NOT NULL CHECK(phase IN ('ready','awaiting_publication','retired')),
    reconcile_pending INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_bound_effects_path ON bound_completion_effects(nfs_path);
CREATE TABLE IF NOT EXISTS spool_bindings (
    spool_id INTEGER PRIMARY KEY,
    incarnation TEXT NOT NULL,
    phase TEXT NOT NULL DEFAULT 'bound',
    stage_path TEXT NOT NULL DEFAULT '',
    publish_path TEXT NOT NULL DEFAULT '',
    birth_id TEXT NOT NULL DEFAULT '',
    birth_path TEXT NOT NULL DEFAULT '',
    birth_file TEXT NOT NULL DEFAULT '',
    birth_identity TEXT NOT NULL DEFAULT ''
);
-- Additive, opt-in operation custody. A legacy spool row has no custody row
-- and must never be inferred to own a stage or a public destination.
CREATE TABLE IF NOT EXISTS spool_custody (
    operation_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL UNIQUE,
    spool_id INTEGER NOT NULL UNIQUE,
    spool_file TEXT NOT NULL,
    source_identity TEXT NOT NULL,
    store_id TEXT NOT NULL,
    incarnation TEXT NOT NULL,
    target_path TEXT NOT NULL,
    target_revision INTEGER NOT NULL,
    epoch INTEGER NOT NULL CHECK(epoch > 0),
    phase TEXT NOT NULL CHECK(phase IN ('allocating','staged','checkpoint_pending','publish_pending','published','canceled','quarantined')),
    stage_path TEXT NOT NULL UNIQUE,
    stage_identity TEXT NOT NULL DEFAULT '',
    target_eof INTEGER NOT NULL CHECK(target_eof >= 0),
    stage_eof INTEGER NOT NULL DEFAULT 0 CHECK(stage_eof >= 0),
    verified_end INTEGER NOT NULL DEFAULT 0 CHECK(verified_end >= 0),
    punched_end INTEGER NOT NULL DEFAULT 0 CHECK(punched_end >= 0),
    public_identity TEXT NOT NULL DEFAULT '',
    published_revision INTEGER NOT NULL DEFAULT 0,
    source_cleanup_claimed INTEGER NOT NULL DEFAULT 0 CHECK(source_cleanup_claimed IN (0,1)),
    stage_cleanup_claimed INTEGER NOT NULL DEFAULT 0 CHECK(stage_cleanup_claimed IN (0,1))
);
CREATE INDEX IF NOT EXISTS idx_spool_custody_target ON spool_custody(target_path, phase);
DROP INDEX IF EXISTS idx_spool_drain_state;
`

// pendingSymlinkSchema defines the pending_symlinks table.
//
// This is the persistence half of the offline-symlink fix. A symlink created
// while offline (pin.IsOffline) cannot be os.Symlink'd onto the FUSE mount —
// that needs JuiceFS→Redis/backend, which is unreachable; the failed FUSE op
// maps to NFSStatusAccess and Finder aborts the WHOLE bundle copy with "you
// don't have permission to access some of the items". So offline we record the
// link in the metadata cache (instant NFS visibility) AND persist (link_path,
// target) here; the drainer materializes the FUSE symlink on reconnect.
//
// It deliberately does NOT live in spool_entries: that table models in-flight
// FILE-DATA transfer (a real spool file on disk, a SHA, a capacity reservation,
// an os.Create+copy drain path, a boot scrubber that reconciles disk files
// against rows). A symlink carries no data — only its target string needs
// persisting — so a dedicated table keeps the symlink path fully isolated from
// the file-data drain invariants and the hot offload path.
//
// link_path is UNIQUE: it's the in-mount path of the link, the natural key, and
// uniqueness lets the offline Symlink upsert be idempotent if the same link is
// re-created (last writer's target wins) without piling up rows. Materialization
// on reconnect is idempotent too — os.Symlink tolerating IsExist, then delete
// the row — so a crash between os.Symlink and the row delete just re-materializes
// harmlessly next reconnect.
const pendingSymlinkSchema = `
CREATE TABLE IF NOT EXISTS pending_symlinks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    link_path   TEXT NOT NULL UNIQUE,
    target      TEXT NOT NULL,
    created_at  INTEGER NOT NULL
);
`

const replacementPublicationRetireGuardV1 = `CREATE TRIGGER IF NOT EXISTS spool_replacement_guard_retire BEFORE UPDATE OF retired_by ON spool_entries
WHEN OLD.replacement_publication_required=1 AND OLD.retired_by=0 AND NEW.retired_by<>0
AND COALESCE((SELECT phase FROM spool_replacement_publications WHERE spool_id=OLD.id),'')<>'superseded'
BEGIN SELECT RAISE(ABORT,'replacement publication not superseded'); END`

const replacementPublicationRetireGuard = `CREATE TRIGGER IF NOT EXISTS spool_replacement_guard_retire BEFORE UPDATE OF retired_by ON spool_entries
WHEN OLD.replacement_publication_required=1 AND OLD.retired_by=0 AND NEW.retired_by<>0
AND COALESCE((SELECT phase FROM spool_replacement_publications WHERE spool_id=OLD.id),'') NOT IN ('superseded','cleaned')
BEGIN SELECT RAISE(ABORT,'replacement publication not superseded or cleaned'); END`

const replacementPublicationDeleteGuard = `CREATE TRIGGER IF NOT EXISTS spool_replacement_guard_delete BEFORE DELETE ON spool_entries
WHEN OLD.replacement_publication_required=1
AND COALESCE((SELECT phase FROM spool_replacement_publications WHERE spool_id=OLD.id),'')<>'superseded'
BEGIN SELECT RAISE(ABORT,'replacement publication not superseded'); END`

// InitSpoolSchema applies the spool_entries + pending_symlinks schema to db.
// Safe to call repeatedly — uses CREATE TABLE IF NOT EXISTS / CREATE INDEX IF
// NOT EXISTS. Returns nil on success, or a wrapped error if the apply fails.
func InitSpoolSchema(db *sql.DB) error {
	if _, err := db.Exec(spoolSchema); err != nil {
		return fmt.Errorf("init spool schema: %w", err)
	}
	if err := migrateReplacementPublicationPhase(db); err != nil {
		return fmt.Errorf("init spool schema (replacement publication phase migration): %w", err)
	}
	if err := migrateBoundEffectsPhase(db); err != nil {
		return fmt.Errorf("init spool schema (bound effects phase migration): %w", err)
	}
	// Column migration for pre-existing databases (CREATE TABLE IF NOT EXISTS
	// never alters an existing table). Attempt-and-ignore-duplicate is the
	// established pattern (internal/derivatives/store.go): SQLite's ALTER TABLE
	// ADD COLUMN is idempotent-by-error — a second run fails with "duplicate
	// column name", which we treat as already-migrated.
	//
	// suspect_zero_tail (#104 zero-tail detection): NULL/empty = clean;
	// non-empty = compact JSON detail {detected_at,size,contiguous,holes}
	// recorded at finalize when the entry closed with unwritten hole(s) below
	// its written size (an interrupted preallocate-then-write download drains
	// FULL-SIZE with a zero tail). Detection/surfacing only — no code path
	// reads it to gate a drain.
	if _, err := db.Exec(`ALTER TABLE spool_entries ADD COLUMN suspect_zero_tail TEXT`); err != nil &&
		!strings.Contains(err.Error(), "duplicate column") {
		return fmt.Errorf("init spool schema (suspect_zero_tail migration): %w", err)
	}
	// punched_end (streaming drain): the end of the prefix that has been copied
	// to the backend AND punched out of the spool file. 0 for every row written
	// by a non-streaming build, which is exactly the right default — it means
	// "nothing punched", i.e. the historical assumption.
	//
	// THIS COLUMN IS A DATA-SAFETY REQUIREMENT, NOT BOOKKEEPING. F_PUNCHHOLE
	// leaves a file's LOGICAL size unchanged, and boot recovery decides a spool
	// file is intact by comparing sizes (nfs/spool.go, the failed→ready branch:
	// `r.Size > 0 && diskSize == r.Size`). A half-punched file passes that test
	// exactly. Without this column, a crash midway through a streamed copy looks
	// like a complete spool file on the next boot: recovery resets it to ready,
	// the drainer uploads the WHOLE file, and the punched prefix — zeros —
	// overwrites real bytes already durably written to the backend. Silent, total
	// loss of that prefix, with no error at any layer.
	//
	// Recovery must therefore treat punched_end > 0 as "this spool file is NOT
	// self-sufficient" and refuse to re-drain it from disk.
	if _, err := db.Exec(`ALTER TABLE spool_entries ADD COLUMN punched_end INTEGER NOT NULL DEFAULT 0`); err != nil &&
		!strings.Contains(err.Error(), "duplicate column") {
		return fmt.Errorf("init spool schema (punched_end migration): %w", err)
	}
	// H-8B base identity and project-lease columns (see SpoolBase in
	// spool_store.go). ADD COLUMN with a constant default is forward-safe on
	// an existing database: every pre-existing row reads as base "unknown"
	// (base_ino=0), accepted_epoch 0 and not waiting, which is exactly the
	// pre-0.5 drain behaviour. An older binary that opens a migrated database
	// ignores the extra columns; its INSERTs name their columns, so the
	// defaults fill them.
	for _, migration := range []string{
		`ALTER TABLE spool_entries ADD COLUMN base_ino INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE spool_entries ADD COLUMN base_mtime_ns INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE spool_entries ADD COLUMN base_ctime_ns INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE spool_entries ADD COLUMN base_len INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE spool_entries ADD COLUMN accepted_epoch INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE spool_entries ADD COLUMN wait_project TEXT NOT NULL DEFAULT ''`,
		// Non-empty = copied from another Mac (SpoolCloneHoldReason); never
		// dispatched or deleted automatically. See HoldRowsCopiedFromOtherMac.
		`ALTER TABLE spool_entries ADD COLUMN clone_hold TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE spool_entries ADD COLUMN pause_since_ns INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE spool_entries ADD COLUMN retry_after_ns INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE spool_entries ADD COLUMN qualification_hold TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE spool_entries ADD COLUMN retired_by INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE spool_entries ADD COLUMN replacement_witness BLOB`,
		`ALTER TABLE spool_entries ADD COLUMN replacement_publication_required INTEGER NOT NULL DEFAULT 0`,
	} {
		if _, err := db.Exec(migration); err != nil && !strings.Contains(err.Error(), "duplicate column") {
			return fmt.Errorf("init spool schema (base identity migration): %w", err)
		}
	}
	// Direct SQL retirement paths must not bypass an active publication intent.
	// This is installed before the legacy retirement sweep below.
	for _, guard := range []string{replacementPublicationRetireGuard, replacementPublicationDeleteGuard} {
		if _, err := db.Exec(guard); err != nil {
			return fmt.Errorf("init spool schema (replacement publication guards): %w", err)
		}
	}
	// A finalized successor retires older same-name history even if the
	// successor is later deleted or collected. A raw writing INSERT does not:
	// Delete(id) also rolls back reservations that never became saved writes.
	if _, err := db.Exec(`
DROP TRIGGER IF EXISTS spool_retire_ready_insert;
DROP TRIGGER IF EXISTS spool_retire_finalized;
DROP TRIGGER IF EXISTS spool_retire_accepted_delete;
CREATE TRIGGER spool_retire_ready_insert AFTER INSERT ON spool_entries
WHEN NEW.drain_state='ready' AND NEW.retired_by=0 BEGIN
 UPDATE spool_entries SET retired_by=NEW.id WHERE nfs_path=NEW.nfs_path AND id<NEW.id AND retired_by=0;
END;
CREATE TRIGGER spool_retire_finalized AFTER UPDATE OF drain_state ON spool_entries
WHEN OLD.drain_state='writing' AND NEW.drain_state='ready' AND NEW.retired_by=0 BEGIN
 UPDATE spool_entries SET retired_by=NEW.id WHERE nfs_path=NEW.nfs_path AND id<NEW.id AND retired_by=0;
END;
CREATE TRIGGER spool_retire_accepted_delete AFTER DELETE ON spool_entries
WHEN OLD.retired_by=0 AND (OLD.drain_state IN ('ready','draining','done') OR (OLD.drain_state='failed' AND length(OLD.sha256)=32)) BEGIN
 UPDATE spool_entries SET retired_by=OLD.id WHERE nfs_path=OLD.nfs_path AND id<OLD.id AND retired_by=0;
END;
`); err != nil {
		return fmt.Errorf("init spool schema (retirement triggers): %w", err)
	}
	// Upgrade databases may already contain coexisting failed/ready histories.
	// Resolve them before a done-row sweep can erase the only newer witness.
	if _, err := db.Exec(`UPDATE spool_entries AS older SET retired_by=(
 SELECT MIN(newer.id) FROM spool_entries newer WHERE newer.nfs_path=older.nfs_path AND newer.retired_by=0
 AND newer.id>older.id AND (newer.drain_state IN ('ready','draining','done')
 OR (newer.drain_state='failed' AND length(newer.sha256)=32)))
 WHERE retired_by=0 AND EXISTS (SELECT 1 FROM spool_entries newer
 WHERE newer.nfs_path=older.nfs_path AND newer.id>older.id AND newer.retired_by=0
 AND (newer.drain_state IN ('ready','draining','done') OR (newer.drain_state='failed' AND length(newer.sha256)=32)))`); err != nil {
		return fmt.Errorf("init spool schema (retire legacy histories): %w", err)
	}
	for _, migration := range []string{
		`ALTER TABLE spool_bindings ADD COLUMN birth_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE spool_bindings ADD COLUMN birth_path TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE spool_bindings ADD COLUMN birth_file TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE spool_bindings ADD COLUMN birth_identity TEXT NOT NULL DEFAULT ''`,
	} {
		if _, err := db.Exec(migration); err != nil && !strings.Contains(err.Error(), "duplicate column") {
			return fmt.Errorf("init spool schema (birth receipt migration): %w", err)
		}
	}
	for _, migration := range []string{
		`ALTER TABLE spool_completion_holds ADD COLUMN kind TEXT NOT NULL DEFAULT 'legacy'`,
		`ALTER TABLE spool_completion_holds ADD COLUMN birth_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE spool_completion_holds ADD COLUMN birth_identity TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE spool_completion_holds ADD COLUMN incarnation TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE spool_completion_holds ADD COLUMN expected_revision INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE spool_completion_holds ADD COLUMN backing_identity TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE spool_completion_holds ADD COLUMN source_sha256 BLOB`,
		`ALTER TABLE spool_completion_holds ADD COLUMN published_revision INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE spool_completion_holds ADD COLUMN original_inode INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE spool_completion_holds ADD COLUMN original_mode INTEGER NOT NULL DEFAULT 0`,
	} {
		if _, err := db.Exec(migration); err != nil && !strings.Contains(err.Error(), "duplicate column") {
			return fmt.Errorf("init spool schema (bound completion hold migration): %w", err)
		}
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_spool_birth_id ON spool_bindings(birth_id) WHERE birth_id<>''`); err != nil {
		return fmt.Errorf("init spool schema (birth ID index): %w", err)
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_spool_birth_file ON spool_bindings(birth_file) WHERE birth_file<>''`); err != nil {
		return fmt.Errorf("init spool schema (birth file index): %w", err)
	}
	if _, err := db.Exec(pendingSymlinkSchema); err != nil {
		return fmt.Errorf("init pending-symlink schema: %w", err)
	}
	return nil
}

func boundEffectsDDLSignature(ddl string) string {
	var out strings.Builder
	inLiteral := false
	for _, r := range ddl {
		if r == '\'' {
			inLiteral = !inLiteral
			out.WriteRune(r)
			continue
		}
		if !inLiteral {
			if r == ' ' || r == '\n' || r == '\t' || r == '\r' {
				continue
			}
			if r >= 'A' && r <= 'Z' {
				r += 'a' - 'A'
			}
		}
		out.WriteRune(r)
	}
	sig := strings.TrimSuffix(out.String(), ";")
	// SQLite's ALTER TABLE RENAME quotes this exact table identifier.
	sig = strings.Replace(sig, `createtable"bound_completion_effects"(`, "createtablebound_completion_effects(", 1)
	sig = strings.Replace(sig, "createtableifnotexistsbound_completion_effects(", "createtablebound_completion_effects(", 1)
	sig = strings.Replace(sig, "createindexifnotexistsidx_bound_effects_path", "createindexidx_bound_effects_path", 1)
	return sig
}

// Existing databases retain SQLite's original table CHECK after CREATE TABLE
// IF NOT EXISTS. Rebuild only known definitions, preserving explicit rowids.
func replacementPublicationDDLSignature(ddl string) string {
	var out strings.Builder
	inLiteral := false
	for _, r := range ddl {
		if r == '\'' {
			inLiteral = !inLiteral
			out.WriteRune(r)
			continue
		}
		if !inLiteral {
			if r == ' ' || r == '\n' || r == '\t' || r == '\r' {
				continue
			}
			if r >= 'A' && r <= 'Z' {
				r += 'a' - 'A'
			}
		}
		out.WriteRune(r)
	}
	sig := strings.TrimSuffix(out.String(), ";")
	sig = strings.Replace(sig, `createtable"spool_replacement_publications"(`, "createtablespool_replacement_publications(", 1)
	sig = strings.Replace(sig, "createtableifnotexistsspool_replacement_publications(", "createtablespool_replacement_publications(", 1)
	sig = strings.Replace(sig, "createtriggerifnotexists", "createtrigger", 1)
	return sig
}

// Rebuild only the known original publication table. The payload is copied as
// an opaque blob, so queued/intent version-one canonical bytes are unchanged.
// Unknown table definitions or attached objects fail closed.
func migrateReplacementPublicationPhase(db *sql.DB) (retErr error) {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			retErr = fmt.Errorf("%w; rollback: %v", retErr, tx.Rollback())
		}
	}()
	var ddl string
	if err := tx.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='spool_replacement_publications'`).Scan(&ddl); err != nil {
		return err
	}
	start := strings.Index(spoolSchema, "CREATE TABLE IF NOT EXISTS spool_replacement_publications (")
	if start < 0 {
		return fmt.Errorf("missing canonical replacement publication definition")
	}
	end := strings.Index(spoolSchema[start:], ");")
	if end < 0 {
		return fmt.Errorf("unterminated canonical replacement publication definition")
	}
	canonical := spoolSchema[start : start+end+2]
	legacyDDL := strings.Replace(canonical, "('queued','intent','arming','armed','committed','settled','cleaning','cleaned','superseded')", "('queued','intent','superseded')", 1)
	priorDDL := strings.Replace(canonical, "('queued','intent','arming','armed','committed','settled','cleaning','cleaned','superseded')", "('queued','intent','arming','armed','committed','superseded')", 1)
	legacy := replacementPublicationDDLSignature(ddl) == replacementPublicationDDLSignature(legacyDDL) || replacementPublicationDDLSignature(ddl) == replacementPublicationDDLSignature(priorDDL)
	if !legacy && replacementPublicationDDLSignature(ddl) != replacementPublicationDDLSignature(canonical) {
		return fmt.Errorf("unrecognized replacement publication table definition")
	}
	rows, err := tx.Query(`PRAGMA table_info(spool_replacement_publications)`)
	if err != nil {
		return err
	}
	want := []string{"spool_id", "operation_id", "phase", "payload"}
	var got []string
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			_ = rows.Close()
			return err
		}
		got = append(got, name)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(got) != len(want) {
		return fmt.Errorf("unexpected replacement publication columns")
	}
	for i := range want {
		if got[i] != want[i] {
			return fmt.Errorf("unexpected replacement publication column %d: %s", i, got[i])
		}
	}
	{
		objects, err := tx.Query(`SELECT type,name,sql FROM sqlite_master WHERE tbl_name='spool_replacement_publications' AND type IN ('index','trigger')`)
		if err != nil {
			return err
		}
		for objects.Next() {
			var typ, name string
			var objectDDL sql.NullString
			if err := objects.Scan(&typ, &name, &objectDDL); err != nil {
				_ = objects.Close()
				return err
			}
			if typ != "index" || !strings.HasPrefix(name, "sqlite_autoindex_spool_replacement_publications_") || objectDDL.Valid {
				_ = objects.Close()
				return fmt.Errorf("unexpected replacement publication object %s %s", typ, name)
			}
		}
		if err := objects.Err(); err != nil {
			_ = objects.Close()
			return err
		}
		if err := objects.Close(); err != nil {
			return err
		}
		guardDDL := map[string][]string{
			"spool_replacement_guard_retire": {replacementPublicationRetireGuardV1, replacementPublicationRetireGuard},
			"spool_replacement_guard_delete": {replacementPublicationDeleteGuard},
		}
		guards, err := tx.Query(`SELECT name,sql FROM sqlite_master WHERE type='trigger' AND sql LIKE '%spool_replacement_publications%'`)
		if err != nil {
			return err
		}
		var guardNames []string
		for guards.Next() {
			var name, gotDDL string
			if err := guards.Scan(&name, &gotDDL); err != nil {
				_ = guards.Close()
				return err
			}
			wantDDLs, known := guardDDL[name]
			matches := false
			for _, wantDDL := range wantDDLs {
				if replacementPublicationDDLSignature(gotDDL) == replacementPublicationDDLSignature(wantDDL) {
					matches = true
					break
				}
			}
			if !known || !matches {
				_ = guards.Close()
				return fmt.Errorf("unexpected replacement publication referencing trigger %s", name)
			}
			guardNames = append(guardNames, name)
		}
		if err := guards.Err(); err != nil {
			_ = guards.Close()
			return err
		}
		if err := guards.Close(); err != nil {
			return err
		}
		for _, name := range guardNames {
			if _, err := tx.Exec(`DROP TRIGGER ` + name); err != nil {
				return err
			}
		}
		if legacy {
			for _, statement := range []string{
				`CREATE TABLE spool_replacement_publications_next (
 spool_id INTEGER PRIMARY KEY, operation_id TEXT NOT NULL UNIQUE,
 phase TEXT NOT NULL CHECK(phase IN ('queued','intent','arming','armed','committed','settled','cleaning','cleaned','superseded')),
 payload BLOB NOT NULL)`,
				`INSERT INTO spool_replacement_publications_next(rowid,spool_id,operation_id,phase,payload)
 SELECT rowid,spool_id,operation_id,phase,payload FROM spool_replacement_publications ORDER BY rowid`,
			} {
				if _, err := tx.Exec(statement); err != nil {
					return err
				}
			}
			var differences int
			if err := tx.QueryRow(`SELECT count(*) FROM (
 SELECT a.rowid FROM spool_replacement_publications a LEFT JOIN spool_replacement_publications_next b ON a.rowid=b.rowid
 WHERE b.rowid IS NULL OR a.spool_id<>b.spool_id OR a.operation_id<>b.operation_id OR a.phase<>b.phase OR a.payload<>b.payload
 UNION ALL
 SELECT b.rowid FROM spool_replacement_publications_next b LEFT JOIN spool_replacement_publications a ON b.rowid=a.rowid WHERE a.rowid IS NULL)`).Scan(&differences); err != nil || differences != 0 {
				return fmt.Errorf("replacement publication migration copy mismatch: %v", err)
			}
			for _, statement := range []string{
				`DROP TABLE spool_replacement_publications`,
				`ALTER TABLE spool_replacement_publications_next RENAME TO spool_replacement_publications`,
			} {
				if _, err := tx.Exec(statement); err != nil {
					return err
				}
			}
		}
		for _, guard := range []string{replacementPublicationRetireGuard, replacementPublicationDeleteGuard} {
			if _, err := tx.Exec(guard); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func migrateBoundEffectsPhase(db *sql.DB) (retErr error) {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			retErr = fmt.Errorf("%w; rollback: %v", retErr, tx.Rollback())
		}
	}()
	var ddl string
	if err := tx.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='bound_completion_effects'`).Scan(&ddl); err != nil {
		return err
	}
	start := strings.Index(spoolSchema, "CREATE TABLE IF NOT EXISTS bound_completion_effects (")
	if start < 0 {
		return fmt.Errorf("missing canonical bound effects definition")
	}
	end := strings.Index(spoolSchema[start:], ");")
	if end < 0 {
		return fmt.Errorf("unterminated canonical bound effects definition")
	}
	canonical := spoolSchema[start : start+end+2]
	legacyDDL := strings.Replace(canonical, "('ready','awaiting_publication','retired')", "('ready','retired')", 1)
	gotSignature := boundEffectsDDLSignature(ddl)
	legacy := gotSignature == boundEffectsDDLSignature(legacyDDL)
	current := gotSignature == boundEffectsDDLSignature(canonical)
	if !legacy && !current {
		return fmt.Errorf("unrecognized bound effects table definition")
	}
	rows, err := tx.Query(`PRAGMA table_info(bound_completion_effects)`)
	if err != nil {
		return err
	}
	want := []string{"completion_id", "spool_id", "birth_id", "nfs_path", "incarnation", "published_revision",
		"original_inode", "size", "source_sha256", "sidecar_body", "phase", "reconcile_pending"}
	var got []string
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			_ = rows.Close()
			return err
		}
		got = append(got, name)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(got) != len(want) {
		return fmt.Errorf("unexpected bound effects columns")
	}
	for i := range want {
		if got[i] != want[i] {
			return fmt.Errorf("unexpected bound effects column %d: %s", i, got[i])
		}
	}
	if legacy {
		objects, err := tx.Query(`SELECT type,name,sql FROM sqlite_master
	 WHERE tbl_name='bound_completion_effects' AND type IN ('index','trigger')`)
		if err != nil {
			return err
		}
		for objects.Next() {
			var typ, name string
			var objectDDL sql.NullString
			if err := objects.Scan(&typ, &name, &objectDDL); err != nil {
				_ = objects.Close()
				return err
			}
			knownPathIndex := typ == "index" && name == "idx_bound_effects_path" && objectDDL.Valid &&
				boundEffectsDDLSignature(objectDDL.String) == boundEffectsDDLSignature(
					"CREATE INDEX idx_bound_effects_path ON bound_completion_effects(nfs_path)")
			autoIndex := typ == "index" && strings.HasPrefix(name, "sqlite_autoindex_bound_completion_effects_") && !objectDDL.Valid
			if !knownPathIndex && !autoIndex {
				_ = objects.Close()
				return fmt.Errorf("unexpected bound effects object %s %s", typ, name)
			}
		}
		if err := objects.Err(); err != nil {
			_ = objects.Close()
			return err
		}
		if err := objects.Close(); err != nil {
			return err
		}
		for _, statement := range []string{
			`CREATE TABLE bound_completion_effects_p54 (
			 completion_id TEXT PRIMARY KEY, spool_id INTEGER NOT NULL UNIQUE, birth_id TEXT NOT NULL,
			 nfs_path TEXT NOT NULL, incarnation TEXT NOT NULL, published_revision INTEGER NOT NULL,
			 original_inode INTEGER NOT NULL, size INTEGER NOT NULL CHECK(size >= 0),
			 source_sha256 BLOB NOT NULL, sidecar_body BLOB CHECK(sidecar_body IS NULL OR length(sidecar_body)<=131072),
			 phase TEXT NOT NULL CHECK(phase IN ('ready','awaiting_publication','retired')),
			 reconcile_pending INTEGER NOT NULL DEFAULT 1)`,
			`INSERT INTO bound_completion_effects_p54(rowid,completion_id,spool_id,birth_id,nfs_path,
			 incarnation,published_revision,original_inode,size,source_sha256,sidecar_body,phase,reconcile_pending)
			 SELECT rowid,completion_id,spool_id,birth_id,nfs_path,incarnation,published_revision,
			 original_inode,size,source_sha256,sidecar_body,phase,reconcile_pending
			 FROM bound_completion_effects ORDER BY rowid`,
			`DROP TABLE bound_completion_effects`,
			`ALTER TABLE bound_completion_effects_p54 RENAME TO bound_completion_effects`,
			`CREATE INDEX idx_bound_effects_path ON bound_completion_effects(nfs_path)`,
		} {
			if _, err := tx.Exec(statement); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

package metadata

import (
	"database/sql"
	"fmt"
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
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_spool_ready_fifo ON spool_entries(drain_state, created_at);
CREATE INDEX IF NOT EXISTS idx_spool_path ON spool_entries(nfs_path);
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

// InitSpoolSchema applies the spool_entries + pending_symlinks schema to db.
// Safe to call repeatedly — uses CREATE TABLE IF NOT EXISTS / CREATE INDEX IF
// NOT EXISTS. Returns nil on success, or a wrapped error if the apply fails.
func InitSpoolSchema(db *sql.DB) error {
	if _, err := db.Exec(spoolSchema); err != nil {
		return fmt.Errorf("init spool schema: %w", err)
	}
	if _, err := db.Exec(pendingSymlinkSchema); err != nil {
		return fmt.Errorf("init pending-symlink schema: %w", err)
	}
	return nil
}

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
CREATE INDEX IF NOT EXISTS idx_spool_drain_state ON spool_entries(drain_state);
CREATE INDEX IF NOT EXISTS idx_spool_path ON spool_entries(nfs_path);
`

// InitSpoolSchema applies the spool_entries schema to db. Safe to call
// repeatedly — uses CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS.
// Returns nil on success, or a wrapped error if the schema apply fails.
func InitSpoolSchema(db *sql.DB) error {
	if _, err := db.Exec(spoolSchema); err != nil {
		return fmt.Errorf("init spool schema: %w", err)
	}
	return nil
}

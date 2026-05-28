// Package manager — SLICE 4 state schema versioning.
//
// SLICE 0 persisted a flat persistedState{jobs, order}. SLICE 4
// introduces additional top-level sections (destinations, with
// schedules/settings/trash_audit reserved for later slices) and adds a
// schema_version field so loaders can detect old files and upgrade
// them in place.
//
// Compatibility rules (locked by §3.1 of docs/ROADMAP/juicemount-manager.md):
//
//   - v1: no schema_version field. The legacy persistedState shape
//         (jobs + order only) is still valid; loader treats it as v1
//         and upgrades to v2 on the next save.
//   - v2: schema_version == 2; destinations array added; other sections
//         reserved as empty arrays/objects.
//
// We deliberately use a single JSON shape for BOTH v1 and v2 — when a
// v1 file is loaded, the missing fields default to their zero values
// (nil destinations slice, empty other sections). The next save writes
// schema_version:2 and the full set of fields, so the upgrade is
// silent and atomic.
package manager

// schemaVersion is the current persisted-state schema version. Bumped
// when the on-disk shape grows new top-level sections that older
// readers wouldn't recognize. Loaders MUST handle the previous version
// for one release before dropping support (see manager-roadmap §3.6).
const schemaVersion = 2

// destinationsState is the persisted form of the SLICE-4 destinations
// list. Stored at the persistedState.Destinations slice.
//
// Each entry holds:
//   - Name: case-sensitive user-supplied identifier, unique across all
//     destinations. Used as the URL fragment in /api/destinations/{name}
//     and as the lookup key when a migration job references a saved
//     profile.
//   - Kind: one of {"file","s3","b2","sftp","webdav","jfs"}. Validated
//     at the API boundary (destinationKinds set).
//   - EncryptedConfig: base64-encoded <nonce||ciphertext||tag> blob from
//     encryptSecret. The plaintext (a JSON-encoded map[string]string)
//     never touches disk. Decryption requires the JM_ADMIN_KEY-derived
//     credKey; rotating that key invalidates every entry (admin must
//     re-encrypt — slice-8 will provide the rotation flow).
//   - CreatedAt / UpdatedAt: unix-ms timestamps for the destinations-tab
//     UI to display "created N ago / updated N ago".
//
// We use a slice (not a map) so the JSON shape is deterministic across
// writes — handy for state-file diffs in operator review.
type destinationsState struct {
	Name            string `json:"name"`
	Kind            string `json:"kind"`
	EncryptedConfig string `json:"encrypted_config"` // base64(<nonce||ciphertext||tag>)
	CreatedAt       int64  `json:"created_at"`
	UpdatedAt       int64  `json:"updated_at"`
}

// Note: the on-disk JSON shape is defined by persistedState in jobs.go;
// SLICE 4 extended that struct with SchemaVersion and Destinations.
// Because Go's json.Unmarshal silently leaves missing fields at zero
// value, the same struct deserializes both v1 (no schema_version) and
// v2 files. The loader logs the v1→v2 upgrade once per startup so the
// operator can grep for it.

// scheduleState is the persisted form of a SLICE-5 Backups schedule.
// Stored at the persistedState.Schedules slice (extends the v2 schema —
// schema_version stays at 2 because v1 readers already tolerate unknown
// top-level keys via Go's json.Unmarshal "missing field = zero value"
// behavior).
//
// All credential-bearing fields live on the referenced destination
// profile (resolved at fire time via the destinations store), NOT on
// the schedule itself. The schedule only carries the destination NAME
// — a non-secret reference — so the schedule row is safe to persist
// in plaintext alongside the rest of the state file.
//
// History is bounded by RetainHistory (per-schedule cap, default 20)
// and stored as a small list of HistoryEntry rows so the Backups tab
// can render "last N runs" without scanning the entire job list.
type scheduleState struct {
	Name          string               `json:"name"`
	Source        SourceSpec           `json:"source"`
	Destination   DestinationRef       `json:"destination"`
	Options       SyncOptions          `json:"options"`
	Cron          string               `json:"cron"`
	Paused        bool                 `json:"paused,omitempty"`
	RetainHistory int                  `json:"retain_history,omitempty"`
	LastRun       int64                `json:"last_run,omitempty"`
	NextRun       int64                `json:"next_run,omitempty"`
	History       []scheduleHistoryRow `json:"history,omitempty"`
}

// scheduleHistoryRow is one persisted entry in a schedule's run log.
// Kept compact — just the job id (so the UI can deep-link into the
// Migrations tab) plus the terminal state and timestamps. The full job
// record lives in the JobManager's jobs map.
type scheduleHistoryRow struct {
	JobID      string `json:"job_id"`
	State      string `json:"state"`
	StartedAt  int64  `json:"started_at"`
	FinishedAt int64  `json:"finished_at,omitempty"`
	Error      string `json:"error,omitempty"`
}

// settingsState is the persisted form of the SLICE-8 Settings struct.
// Stored at the persistedState.Settings field as a POINTER so that a
// state file with no `settings` key (every install before slice-8)
// decodes to nil and the loader can distinguish "never configured" from
// "configured to all-zero values". A nil settings pointer means "use
// code defaults"; the Settings handler materializes those on GET.
//
// Schema stays at v2 (extending within v2 — same convention as
// schedules, since older readers tolerate unknown keys via Go's
// json.Unmarshal "missing field = zero value" behavior).
//
// All fields here are plaintext / non-secret. The admin key itself is
// NEVER stored — only the operator's running JM_ADMIN_KEY env var
// holds it, and rotation re-encrypts every destination under a new
// HKDF-derived key without persisting either the old or new admin key
// to disk. DestinationsRedacted is a UX hint (whether the Destinations
// tab should default to redacted-list view) — informational, not a
// security boundary.
type settingsState struct {
	JobDefaults          SyncOptions `json:"job_defaults"`
	Theme                string      `json:"theme"`
	LogRetentionLines    int         `json:"log_retention_lines"`
	DestinationsRedacted bool        `json:"destinations_redacted,omitempty"`
}

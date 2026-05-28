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

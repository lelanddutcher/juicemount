package derivatives

// JM-ASSERT (#51) — the rebuildable Tier-B index over the portable
// `<media>.loupe.json` assertion sidecars (ASSERTIONS_SIDECAR.md). The sidecar
// next to the media is the SOURCE OF TRUTH for human metadata (ratings, person
// names, a deliberate log-profile pick); this table is a content-hash-asset_key-
// keyed accelerator the control plane answers GET /assertions from, never the
// silo that owns the data.
//
// Resolution is last-writer-wins per (asset_key, namespace, key) by asserted_at:
// an incoming triple with an OLDER-or-equal asserted_at than the stored winner is
// rejected, the stored value left untouched. value==null is a RETRACT (the row is
// kept so a later, even-newer assertion can re-resolve, and so the sidecar can
// record the deliberate un-assert) — distinct from a real force-negative value.
//
// The JuiceFS inode is a Tier-B ACCELERATOR COLUMN ONLY: it lets inode/path
// queries resolve to an asset_key, but it is never identity and never part of a
// portable assertion. It lives in the same SQLite DB as the derivative index so
// the control plane keeps a single handle.

import (
	"database/sql"
	"strings"
)

// Assertion is one resolved (namespace,key,value) triple with its author stamp —
// the verbatim JM-ASSERT wire shape (assertions-get / assertions-sidecar
// schemas). Value is `any` so it round-trips string|number|bool|null faithfully
// (null == retract).
type Assertion struct {
	Namespace  string `json:"namespace"`
	Key        string `json:"key"`
	Value      any    `json:"value"`
	AssertedBy string `json:"asserted_by"`
	AssertedAt string `json:"asserted_at"` // RFC-3339 UTC; the LWW ordering field
}

const assertionsSchema = `
CREATE TABLE IF NOT EXISTS assertions (
    asset_key   TEXT NOT NULL,
    namespace   TEXT NOT NULL,
    key         TEXT NOT NULL,
    value_json  TEXT,            -- JSON-encoded value; NULL row never used (null is "null")
    asserted_by TEXT NOT NULL,
    asserted_at TEXT NOT NULL,   -- RFC-3339; lexical sort == chronological for UTC Z
    inode       INTEGER,         -- Tier-B accelerator ONLY (nullable, non-identity)
    PRIMARY KEY (asset_key, namespace, key)
);
CREATE INDEX IF NOT EXISTS idx_assert_inode ON assertions(inode);
`

// AssertResult is the outcome of an AssertLWW write — the data the POST
// /assertions response is built from.
type AssertResult struct {
	Accepted          bool   // true when this triple won LWW (index upserted)
	WinningAssertedAt string // asserted_at of whoever now holds the slot
}

// initAssertions creates the assertions table. Called from Open alongside the
// derivative schema so one Store owns both.
func (s *Store) initAssertions() error {
	_, err := s.db.Exec(assertionsSchema)
	return err
}

// AssertLWW applies one assertion under last-writer-wins by asserted_at. It
// returns Accepted=true + the incoming asserted_at when the incoming triple is
// strictly newer than the stored one (or there is no stored one), having upserted
// the row; Accepted=false + the STORED winner's asserted_at on a reject-stale
// (incoming asserted_at <= stored), leaving the row untouched. valueJSON is the
// JSON encoding of the value ("null" for a retract). inode (0 ⇒ NULL) is the
// accelerator column, refreshed on every accept so an inode/path query resolves.
func (s *Store) AssertLWW(assetKey, namespace, key, valueJSON, assertedBy, assertedAt string, inode uint64) (AssertResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var storedAt string
	err := s.db.QueryRow(`SELECT asserted_at FROM assertions WHERE asset_key=? AND namespace=? AND key=?`,
		assetKey, namespace, key).Scan(&storedAt)
	switch {
	case err == sql.ErrNoRows:
		// No prior triple — accept unconditionally.
	case err != nil:
		return AssertResult{}, err
	default:
		// LWW: strictly-newer wins. Equal or older is rejected (stable under
		// replays). RFC-3339 UTC "Z" timestamps compare correctly lexically; we
		// still compare as strings, which the contract's fixtures all satisfy.
		if assertedAt <= storedAt {
			return AssertResult{Accepted: false, WinningAssertedAt: storedAt}, nil
		}
	}

	var inodeArg any
	if inode != 0 {
		inodeArg = int64(inode)
	}
	_, err = s.db.Exec(`
		INSERT INTO assertions (asset_key, namespace, key, value_json, asserted_by, asserted_at, inode)
		VALUES (?,?,?,?,?,?,?)
		ON CONFLICT(asset_key, namespace, key) DO UPDATE SET
		    value_json=excluded.value_json, asserted_by=excluded.asserted_by,
		    asserted_at=excluded.asserted_at, inode=COALESCE(excluded.inode, assertions.inode)`,
		assetKey, namespace, key, valueJSON, assertedBy, assertedAt, inodeArg)
	if err != nil {
		return AssertResult{}, err
	}
	return AssertResult{Accepted: true, WinningAssertedAt: assertedAt}, nil
}

// AssertionsByAssetKey returns every resolved triple for an asset_key, ordered
// for a stable response (namespace, then key). The slice is never nil — an asset
// with no assertions yields an empty slice (the schema's non-null array). The
// value_json column is returned RAW (a json.RawMessage) so the caller emits the
// value verbatim (string|number|bool|null) without a re-encode round-trip.
func (s *Store) AssertionsByAssetKey(assetKey string) ([]AssertionRaw, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query(`
		SELECT namespace, key, value_json, asserted_by, asserted_at
		FROM assertions WHERE asset_key=? ORDER BY namespace ASC, key ASC`, assetKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AssertionRaw{}
	for rows.Next() {
		var a AssertionRaw
		var v sql.NullString
		if err := rows.Scan(&a.Namespace, &a.Key, &v, &a.AssertedBy, &a.AssertedAt); err != nil {
			return nil, err
		}
		if v.Valid && v.String != "" {
			a.ValueJSON = v.String
		} else {
			a.ValueJSON = "null"
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// AssertionRaw is the read-side triple carrying the value as its raw JSON text,
// so /assertions emits the stored value byte-for-byte.
type AssertionRaw struct {
	Namespace  string
	Key        string
	ValueJSON  string // raw JSON text of the value
	AssertedBy string
	AssertedAt string
}

// AssetKeyForInode resolves a Tier-B accelerator inode to the asset_key its
// assertions are bound to (the most-recent one wins if — pathologically — an
// inode mapped to two keys). Empty string when the inode has no assertions:
// fail-closed (the GET-empty contract).
func (s *Store) AssetKeyForInode(inode uint64) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var key string
	err := s.db.QueryRow(`
		SELECT asset_key FROM assertions WHERE inode=? ORDER BY asserted_at DESC LIMIT 1`,
		int64(inode)).Scan(&key)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return key, nil
}

// InodeForAssetKey resolves an asset_key back to its Tier-B accelerator inode (the
// reverse of AssetKeyForInode), so a write that arrived by pure asset_key — after
// the key was first bound to an inode by an earlier inode/path write — can still
// locate the media + its sidecar. 0 when the key has no accelerator inode (a
// purely portable asset never seen via an inode/path query on this host).
func (s *Store) InodeForAssetKey(assetKey string) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var inode sql.NullInt64
	err := s.db.QueryRow(`
		SELECT inode FROM assertions WHERE asset_key=? AND inode IS NOT NULL ORDER BY asserted_at DESC LIMIT 1`,
		assetKey).Scan(&inode)
	if err == sql.ErrNoRows || !inode.Valid {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return uint64(inode.Int64), nil
}

// PathAssetKey derives the path/name fallback asset_key for an asset whose bytes
// have not been content-hashed yet: "path:<basename>". The portable hash form
// "xxh3:<16hex>" is always preferred; this is only the binding until a hash is
// stamped (ASSERTIONS_SIDECAR §2). Kept here so the handler and the sidecar
// writer derive it identically.
func PathAssetKey(basename string) string {
	return "path:" + basename
}

// IsHashAssetKey reports whether key is the portable content-hash form.
func IsHashAssetKey(key string) bool {
	return strings.HasPrefix(key, "xxh3:")
}

// Package derivatives is the Tier-B shared-derivative index (contract JM-14):
// per-asset (by durable JuiceFS inode) records of which machine-derived
// artifacts exist — thumbnails, filmstrips, waveforms, proxies, tech/EXIF,
// embeddings, transcripts — their status/producer/version/integrity-hash, and
// (for blob kinds) the volume-relative blob path.
//
// This is a REBUILDABLE CACHE: the media is the truth for machine-derived data,
// so a consumer that distrusts a row (hash mismatch) just regenerates locally.
// The server-side farm (JM-16) populates it; the Mac control plane serves it
// (mirror-synced via JM-15 in a later slice). Keyed on inode so it survives
// remounts and is shareable across machines.
package derivatives

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// DerivRow is one derivative's manifest entry. Pointer fields are nullable on
// the wire (omitted/serialized as null) per derivatives.schema.json.
type DerivRow struct {
	Kind        string  `json:"kind"`
	Status      string  `json:"status"`
	Producer    string  `json:"producer"`
	Version     int     `json:"version"`
	Hash        *string `json:"hash"`
	BlobRelPath *string `json:"blob_rel_path"`
	MediaType   *string `json:"media_type"`
	Model       *string `json:"model"`
	Dim         *int    `json:"dim"`
	UpdatedAt   int64   `json:"updated_at"`
	// SourceSize / SourceMtime are the source file's size (bytes) + mtime (unix)
	// at generation time. They let a consumer stat-verify a derivative directly
	// off the manifest (OpenLoupe's read-gate: live /lookup size == source_size)
	// without a separate `tech` row. omitempty for backward-compat with rows that
	// predate the columns + the closed-key conformance fixtures.
	SourceSize  *int64 `json:"source_size,omitempty"`
	SourceMtime *int64 `json:"source_mtime,omitempty"`
	// Filmstrip is the sprite-sheet geometry (JM-16), present ONLY for
	// kind=="filmstrip". Persisted in the row's kind-specific `extra` JSON
	// column. omitempty so every other kind omits it on the wire.
	Filmstrip *FilmstripGeo `json:"filmstrip,omitempty"`
}

// FilmstripGeo is the sprite-sheet geometry a scrubber needs to map a time to a
// cell rect. Schema: derivatives.schema.json derivative-entry `filmstrip`.
type FilmstripGeo struct {
	FrameCount int `json:"frame_count"`
	Cols       int `json:"cols"`
	Rows       int `json:"rows"`
	CellW      int `json:"cell_w"`
	CellH      int `json:"cell_h"`
	IntervalMS int `json:"interval_ms"`
	DurationMS int `json:"duration_ms"`
}

// TechMeta is the structured tech/EXIF metadata for one asset (kind=tech).
type TechMeta struct {
	Producer string
	Version  int
	Hash     *string
	Payload  json.RawMessage // the `tech` object, additive
}

const schema = `
CREATE TABLE IF NOT EXISTS source_assets (
    inode  INTEGER PRIMARY KEY,
    hash   TEXT
);
CREATE TABLE IF NOT EXISTS derivatives (
    inode         INTEGER NOT NULL,
    kind          TEXT NOT NULL,
    status        TEXT NOT NULL,
    producer      TEXT NOT NULL,
    version       INTEGER NOT NULL,
    hash          TEXT,
    blob_rel_path TEXT,
    media_type    TEXT,
    model         TEXT,
    dim           INTEGER,
    updated_at    INTEGER NOT NULL,
    extra         TEXT,
    source_size   INTEGER,
    source_mtime  INTEGER,
    PRIMARY KEY (inode, kind)
);
CREATE INDEX IF NOT EXISTS idx_deriv_updated ON derivatives(updated_at);
CREATE TABLE IF NOT EXISTS metadata (
    inode    INTEGER NOT NULL,
    kind     TEXT NOT NULL,
    producer TEXT NOT NULL,
    version  INTEGER NOT NULL,
    hash     TEXT,
    payload  TEXT NOT NULL,
    PRIMARY KEY (inode, kind)
);
`

// Store is the thread-safe SQLite-backed derivative index.
type Store struct {
	db *sql.DB
	mu sync.RWMutex
}

// Open creates or opens the derivative index. ":memory:" for tests.
func Open(dbPath string) (*Store, error) {
	if dbPath == ":memory:" {
		dbPath = "file::memory:?mode=memory&cache=shared"
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("derivatives: open sqlite: %w", err)
	}
	if _, err := db.Exec(`PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 30000;`); err != nil {
		db.Close()
		return nil, fmt.Errorf("derivatives: pragmas: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("derivatives: schema: %w", err)
	}
	// Migration: add the kind-specific `extra` JSON column (JM-16 filmstrip
	// geometry) to a derivatives table created before it existed. Fresh DBs
	// already have it from the CREATE above, so the duplicate-column error is
	// expected and ignored.
	if _, err := db.Exec(`ALTER TABLE derivatives ADD COLUMN extra TEXT`); err != nil && !strings.Contains(err.Error(), "duplicate column") {
		db.Close()
		return nil, fmt.Errorf("derivatives: migrate extra: %w", err)
	}
	// Migration: source_size / source_mtime (consumer read-gate). Same
	// duplicate-column-is-fine pattern as `extra`.
	for _, col := range []string{"source_size INTEGER", "source_mtime INTEGER"} {
		if _, err := db.Exec(`ALTER TABLE derivatives ADD COLUMN ` + col); err != nil && !strings.Contains(err.Error(), "duplicate column") {
			db.Close()
			return nil, fmt.Errorf("derivatives: migrate %s: %w", col, err)
		}
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Known reports whether the index has any record of this source asset (a
// source_assets row). Drives the manifest's `exists`.
func (s *Store) Known(inode uint64) (bool, *string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var hash sql.NullString
	err := s.db.QueryRow(`SELECT hash FROM source_assets WHERE inode = ?`, int64(inode)).Scan(&hash)
	if err != nil {
		return false, nil
	}
	if hash.Valid {
		h := hash.String
		return true, &h
	}
	return true, nil
}

// Manifest returns the derivative rows for an asset (nil slice if none).
func (s *Store) Manifest(inode uint64) ([]DerivRow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query(`
		SELECT kind, status, producer, version, hash, blob_rel_path, media_type, model, dim, updated_at, extra, source_size, source_mtime
		FROM derivatives WHERE inode = ? ORDER BY kind`, int64(inode))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DerivRow
	for rows.Next() {
		var d DerivRow
		var hash, blob, mt, model, extra sql.NullString
		var dim, srcSize, srcMtime sql.NullInt64
		if err := rows.Scan(&d.Kind, &d.Status, &d.Producer, &d.Version, &hash, &blob, &mt, &model, &dim, &d.UpdatedAt, &extra, &srcSize, &srcMtime); err != nil {
			return nil, err
		}
		d.Hash = nullStr(hash)
		d.BlobRelPath = nullStr(blob)
		d.MediaType = nullStr(mt)
		d.Model = nullStr(model)
		if dim.Valid {
			v := int(dim.Int64)
			d.Dim = &v
		}
		if srcSize.Valid {
			v := srcSize.Int64
			d.SourceSize = &v
		}
		if srcMtime.Valid {
			v := srcMtime.Int64
			d.SourceMtime = &v
		}
		// kind-specific `extra`: filmstrip geometry today (JM-16).
		if d.Kind == "filmstrip" && extra.Valid && extra.String != "" {
			var fg FilmstripGeo
			if json.Unmarshal([]byte(extra.String), &fg) == nil {
				d.Filmstrip = &fg
			}
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// Metadata returns the structured metadata for a kind (nil if absent).
func (s *Store) Metadata(inode uint64, kind string) (*TechMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var m TechMeta
	var hash sql.NullString
	var payload string
	err := s.db.QueryRow(`SELECT producer, version, hash, payload FROM metadata WHERE inode = ? AND kind = ?`,
		int64(inode), kind).Scan(&m.Producer, &m.Version, &hash, &payload)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	m.Hash = nullStr(hash)
	m.Payload = json.RawMessage(payload)
	return &m, nil
}

// --- write path (used by the farm / JM-15 sync / tests) ---

// PutSource records/updates a known source asset + its content hash.
func (s *Store) PutSource(inode uint64, hash *string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`INSERT INTO source_assets (inode, hash) VALUES (?, ?)
		ON CONFLICT(inode) DO UPDATE SET hash = excluded.hash`, int64(inode), strOrNil(hash))
	return err
}

// PutDeriv upserts one derivative manifest row.
func (s *Store) PutDeriv(inode uint64, d DerivRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d.UpdatedAt == 0 {
		d.UpdatedAt = time.Now().Unix()
	}
	_, err := s.db.Exec(`
		INSERT INTO derivatives (inode, kind, status, producer, version, hash, blob_rel_path, media_type, model, dim, updated_at, extra, source_size, source_mtime)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(inode, kind) DO UPDATE SET
		    status=excluded.status, producer=excluded.producer, version=excluded.version,
		    hash=excluded.hash, blob_rel_path=excluded.blob_rel_path, media_type=excluded.media_type,
		    model=excluded.model, dim=excluded.dim, updated_at=excluded.updated_at, extra=excluded.extra,
		    source_size=excluded.source_size, source_mtime=excluded.source_mtime`,
		int64(inode), d.Kind, d.Status, d.Producer, d.Version, strOrNil(d.Hash),
		strOrNil(d.BlobRelPath), strOrNil(d.MediaType), strOrNil(d.Model), intOrNil(d.Dim), d.UpdatedAt, extraJSON(d),
		int64OrNil(d.SourceSize), int64OrNil(d.SourceMtime))
	return err
}

// KindStat is the ready/failed tally for one derivative kind.
type KindStat struct {
	Ready  int `json:"ready"`
	Failed int `json:"failed"`
}

// Stats is a rollup of the index for an operator dashboard (the manager Farm tab):
// how many distinct assets are covered, the ready/failed counts per kind, and the
// most recent write. Cheap aggregate query; reflects what THIS index has (the
// server farm's own db, or the Mac's after JM-15 reconcile).
type Stats struct {
	TotalAssets int                 `json:"total_assets"`
	ByKind      map[string]KindStat `json:"by_kind"`
	LastUpdated int64               `json:"last_updated"`
}

// Stats returns the index rollup.
func (s *Store) Stats() (Stats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := Stats{ByKind: map[string]KindStat{}}

	rows, err := s.db.Query(`SELECT kind, status, COUNT(*) FROM derivatives GROUP BY kind, status`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var kind, status string
		var n int
		if err := rows.Scan(&kind, &status, &n); err != nil {
			return out, err
		}
		ks := out.ByKind[kind]
		switch status {
		case "ready":
			ks.Ready += n
		case "failed":
			ks.Failed += n
		}
		out.ByKind[kind] = ks
	}
	if err := rows.Err(); err != nil {
		return out, err
	}

	var total sql.NullInt64
	var last sql.NullInt64
	if err := s.db.QueryRow(`SELECT COUNT(DISTINCT inode), MAX(updated_at) FROM derivatives`).Scan(&total, &last); err != nil {
		return out, err
	}
	out.TotalAssets = int(total.Int64)
	out.LastUpdated = last.Int64
	return out, nil
}

// ProxyRow is a minimal proxy-derivative tuple for the farm's proxy-economics
// rollup: the asset inode + the source file size recorded at generation time.
// It carries only what the economics measure needs (stat the blob for the proxy
// bytes) — NO schema change, NO blob path (the farm derives it from the inode via
// DerivBlobDir).
type ProxyRow struct {
	Inode      uint64
	SourceSize *int64
}

// ListProxyRows returns the ready proxy derivative rows (inode + source_size).
// Used by the farm's proxy-economics rollup, which os.Stats each inode's
// proxy.mp4 blob to compare actual proxy bytes against the source size. Only
// `ready` rows are returned (a failed proxy has no blob to measure).
func (s *Store) ListProxyRows() ([]ProxyRow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.db.Query(`
		SELECT inode, source_size FROM derivatives
		WHERE kind = 'proxy' AND status = 'ready' ORDER BY inode`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProxyRow
	for rows.Next() {
		var inode int64
		var srcSize sql.NullInt64
		if err := rows.Scan(&inode, &srcSize); err != nil {
			return nil, err
		}
		pr := ProxyRow{Inode: uint64(inode)}
		if srcSize.Valid {
			v := srcSize.Int64
			pr.SourceSize = &v
		}
		out = append(out, pr)
	}
	return out, rows.Err()
}

// ChangeRow is one entry in the /derivatives/changes delta feed.
type ChangeRow struct {
	Inode     uint64  `json:"inode"`
	Kind      string  `json:"kind"`
	Status    string  `json:"status"`
	Hash      *string `json:"hash"`
	UpdatedAt int64   `json:"updated_at"`
}

// ListChangedSince returns derivative rows with updated_at > since, ascending, for
// a consumer's poll-based delta feed (the `since=` cursor). The consumer keys its
// cache by (inode,kind) so re-delivery is idempotent; a strict `>` excludes rows
// written in the same second as the cursor (the consumer can poll since-1 for
// at-least-once). Bounded by limit (default/cap 10000).
func (s *Store) ListChangedSince(since int64, limit int) ([]ChangeRow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 || limit > 10000 {
		limit = 10000
	}
	rows, err := s.db.Query(`
		SELECT inode, kind, status, hash, updated_at
		FROM derivatives WHERE updated_at > ? ORDER BY updated_at ASC, inode ASC, kind ASC LIMIT ?`,
		since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChangeRow
	for rows.Next() {
		var c ChangeRow
		var inode int64
		var hash sql.NullString
		if err := rows.Scan(&inode, &c.Kind, &c.Status, &hash, &c.UpdatedAt); err != nil {
			return nil, err
		}
		c.Inode = uint64(inode)
		c.Hash = nullStr(hash)
		out = append(out, c)
	}
	return out, rows.Err()
}

// PutMetadata upserts the structured metadata for a kind.
func (s *Store) PutMetadata(inode uint64, kind, producer string, version int, hash *string, payload json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`
		INSERT INTO metadata (inode, kind, producer, version, hash, payload) VALUES (?,?,?,?,?,?)
		ON CONFLICT(inode, kind) DO UPDATE SET
		    producer=excluded.producer, version=excluded.version, hash=excluded.hash, payload=excluded.payload`,
		int64(inode), kind, producer, version, strOrNil(hash), string(payload))
	return err
}

// IngestTech atomically writes an asset's source row, its `tech` metadata, and a
// set of derivative manifest rows in ONE transaction. This closes the
// half-written-asset window: a consumer never observes a source row (exists=true)
// without its tech/derivatives — they appear together or not at all (rollback on
// any error). The producer (internal/farm) and the JM-15 sync feed use this so
// /derivatives and /metadata stay mutually consistent under partial failures.
//
// hash is written as both the source_hash and the tech metadata hash; each row in
// rows carries its own hash (the producer sets them equal to hash so the
// consumer's hash==source_hash gate is exact). updated_at defaults to now per row.
func (s *Store) IngestTech(inode uint64, hash *string, producer string, version int, techPayload json.RawMessage, rows []DerivRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // no-op once Commit succeeds

	if _, err := tx.Exec(`INSERT INTO source_assets (inode, hash) VALUES (?, ?)
		ON CONFLICT(inode) DO UPDATE SET hash = excluded.hash`, int64(inode), strOrNil(hash)); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO metadata (inode, kind, producer, version, hash, payload) VALUES (?,?,?,?,?,?)
		ON CONFLICT(inode, kind) DO UPDATE SET
			producer=excluded.producer, version=excluded.version, hash=excluded.hash, payload=excluded.payload`,
		int64(inode), "tech", producer, version, strOrNil(hash), string(techPayload)); err != nil {
		return err
	}
	now := time.Now().Unix()
	for _, d := range rows {
		ts := d.UpdatedAt
		if ts == 0 {
			ts = now
		}
		if _, err := tx.Exec(`INSERT INTO derivatives (inode, kind, status, producer, version, hash, blob_rel_path, media_type, model, dim, updated_at, extra, source_size, source_mtime)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(inode, kind) DO UPDATE SET
				status=excluded.status, producer=excluded.producer, version=excluded.version,
				hash=excluded.hash, blob_rel_path=excluded.blob_rel_path, media_type=excluded.media_type,
				model=excluded.model, dim=excluded.dim, updated_at=excluded.updated_at, extra=excluded.extra,
				source_size=excluded.source_size, source_mtime=excluded.source_mtime`,
			int64(inode), d.Kind, d.Status, d.Producer, d.Version, strOrNil(d.Hash),
			strOrNil(d.BlobRelPath), strOrNil(d.MediaType), strOrNil(d.Model), intOrNil(d.Dim), ts, extraJSON(d),
			int64OrNil(d.SourceSize), int64OrNil(d.SourceMtime)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func nullStr(n sql.NullString) *string {
	if n.Valid {
		v := n.String
		return &v
	}
	return nil
}
func strOrNil(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}
func intOrNil(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

func int64OrNil(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

// extraJSON serializes a row's kind-specific sub-object into the `extra` column.
// Filmstrip geometry today (JM-16); nil for every other kind.
func extraJSON(d DerivRow) any {
	if d.Filmstrip != nil {
		b, err := json.Marshal(d.Filmstrip)
		if err == nil {
			return string(b)
		}
	}
	return nil
}

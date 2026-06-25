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
    PRIMARY KEY (inode, kind)
);
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
		SELECT kind, status, producer, version, hash, blob_rel_path, media_type, model, dim, updated_at
		FROM derivatives WHERE inode = ? ORDER BY kind`, int64(inode))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DerivRow
	for rows.Next() {
		var d DerivRow
		var hash, blob, mt, model sql.NullString
		var dim sql.NullInt64
		if err := rows.Scan(&d.Kind, &d.Status, &d.Producer, &d.Version, &hash, &blob, &mt, &model, &dim, &d.UpdatedAt); err != nil {
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
		INSERT INTO derivatives (inode, kind, status, producer, version, hash, blob_rel_path, media_type, model, dim, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(inode, kind) DO UPDATE SET
		    status=excluded.status, producer=excluded.producer, version=excluded.version,
		    hash=excluded.hash, blob_rel_path=excluded.blob_rel_path, media_type=excluded.media_type,
		    model=excluded.model, dim=excluded.dim, updated_at=excluded.updated_at`,
		int64(inode), d.Kind, d.Status, d.Producer, d.Version, strOrNil(d.Hash),
		strOrNil(d.BlobRelPath), strOrNil(d.MediaType), strOrNil(d.Model), intOrNil(d.Dim), d.UpdatedAt)
	return err
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

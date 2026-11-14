package metadata

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// MirrorToken identifies an observation of the local durable mirror, not the
// backing filesystem. A present row uses Incarnation and Revision. An absent
// path uses the store-wide epoch, so even create/delete between capture and
// publication invalidates the observation. Unrelated writes may conservatively
// invalidate an absent token. Tokens must be captured BEFORE external reads.
type MirrorToken struct {
	Path        string
	Incarnation string
	Revision    int64
	StoreID     string
	Epoch       int64
}

// StoreEpoch fences a full SCAN against any intervening durable entry write
// or cache-only namespace publication in this Store instance.
type StoreEpoch struct {
	StoreID         string
	Epoch           int64
	CacheGeneration uint64
}

func captureStoreEpoch(q versionQuerier, cacheGeneration uint64) (StoreEpoch, error) {
	v := StoreEpoch{CacheGeneration: cacheGeneration}
	err := q.QueryRow(`SELECT
 (SELECT value FROM store_meta WHERE key='mirror_store_id'),
 (SELECT CAST(value AS INTEGER) FROM store_meta WHERE key='mirror_epoch')`).Scan(&v.StoreID, &v.Epoch)
	return v, err
}

func (s *Store) CaptureStoreEpoch() (StoreEpoch, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.mu.RLock()
	generation := s.cacheGeneration
	s.mu.RUnlock()
	return captureStoreEpoch(s.db, generation)
}

func (v MirrorToken) IsZero() bool { return v.Incarnation == "" && v.StoreID == "" }

var ErrMirrorConflict = errors.New("mirror observation no longer current")

// Legacy upserts preserve an existing incarnation only for a same-inode,
// same-type update. An explicit delete/recreate always gets a fresh identity.
// This does not make an unversioned external observation safe against races.
const incarnationUpsertSQL = `INSERT OR REPLACE INTO entries
 (path, name, parent_path, is_dir, size, mtime, inode, mode, local_only, mtime_nsec, incarnation, revision)
 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
 COALESCE((SELECT incarnation FROM entries WHERE path=? AND inode=? AND is_dir=?), lower(hex(randomblob(16)))),
 COALESCE((SELECT revision+1 FROM entries WHERE path=?), 1))`

func initIncarnationSchema(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.Query(`PRAGMA table_info(entries)`)
	if err != nil {
		return err
	}
	columns := make(map[string]bool)
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var def any
		if err := rows.Scan(&cid, &name, &typ, &notnull, &def, &pk); err != nil {
			rows.Close()
			return err
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if !columns["mtime_nsec"] {
		if _, err := tx.Exec(`ALTER TABLE entries ADD COLUMN mtime_nsec INTEGER CHECK(mtime_nsec BETWEEN 0 AND 999999999)`); err != nil {
			return err
		}
	}
	if !columns["incarnation"] {
		if _, err := tx.Exec(`ALTER TABLE entries ADD COLUMN incarnation TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	if !columns["revision"] {
		if _, err := tx.Exec(`ALTER TABLE entries ADD COLUMN revision INTEGER NOT NULL DEFAULT 1`); err != nil {
			return err
		}
	}
	_, err = tx.Exec(`
 UPDATE entries SET incarnation=lower(hex(randomblob(16))) WHERE incarnation='';
 INSERT OR IGNORE INTO store_meta(key,value) VALUES ('mirror_store_id',lower(hex(randomblob(16))));
 INSERT OR IGNORE INTO store_meta(key,value) VALUES ('mirror_epoch','0');
 CREATE TRIGGER IF NOT EXISTS mirror_insert AFTER INSERT ON entries BEGIN
   UPDATE entries SET incarnation=lower(hex(randomblob(16))) WHERE path=NEW.path AND incarnation='';
   UPDATE store_meta SET value=CAST(value AS INTEGER)+1 WHERE key='mirror_epoch';
 END;
 CREATE TRIGGER IF NOT EXISTS mirror_update AFTER UPDATE ON entries BEGIN
   UPDATE entries SET revision=OLD.revision+1 WHERE path=NEW.path AND revision=OLD.revision;
   UPDATE store_meta SET value=CAST(value AS INTEGER)+1 WHERE key='mirror_epoch';
 END;
 CREATE TRIGGER IF NOT EXISTS mirror_delete AFTER DELETE ON entries BEGIN
   UPDATE store_meta SET value=CAST(value AS INTEGER)+1 WHERE key='mirror_epoch';
 END;`)
	if err != nil {
		return err
	}
	return tx.Commit()
}

type versionQuerier interface {
	QueryRow(string, ...any) *sql.Row
}

func loadEntryVersion(q versionQuerier, e *Entry) error {
	e.Version = MirrorToken{Path: e.Path}
	return q.QueryRow(`SELECT incarnation, revision FROM entries WHERE path=?`, e.Path).
		Scan(&e.Version.Incarnation, &e.Version.Revision)
}

func captureToken(q versionQuerier, name string) (MirrorToken, error) {
	v := MirrorToken{Path: name}
	err := q.QueryRow(`SELECT incarnation, revision FROM entries WHERE path=?`, name).
		Scan(&v.Incarnation, &v.Revision)
	if err == nil {
		return v, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return MirrorToken{}, err
	}
	err = q.QueryRow(`SELECT
 (SELECT value FROM store_meta WHERE key='mirror_store_id'),
 (SELECT CAST(value AS INTEGER) FROM store_meta WHERE key='mirror_epoch')`).Scan(&v.StoreID, &v.Epoch)
	return v, err
}

// CaptureMirror returns a durable observation. A RAM-only publication is not
// durable and cannot be used as an expected-token precondition.
func (s *Store) CaptureMirror(name string) (MirrorToken, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.mu.RLock()
	cached := s.pathCache[name]
	present := cached != nil
	var cachedVersion MirrorToken
	if present {
		cachedVersion = cached.Version
	}
	s.mu.RUnlock()
	v, err := captureToken(s.db, name)
	if err == nil {
		if present && cachedVersion != v {
			return MirrorToken{}, ErrMirrorConflict
		}
	}
	return v, err
}

// CaptureDirectoryForFUSE permits an absent durable directory with a
// cache-only shadow to be confirmed from a retained backing descriptor.
func (s *Store) CaptureDirectoryForFUSE(name string) (MirrorToken, StoreEpoch, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.mu.RLock()
	cached := s.pathCache[name]
	present := cached != nil
	var cachedIsDir bool
	var cachedVersion MirrorToken
	if present {
		cachedIsDir = cached.IsDir
		cachedVersion = cached.Version
	}
	generation := s.cacheGeneration
	s.mu.RUnlock()
	v, err := captureToken(s.db, name)
	if err != nil {
		return MirrorToken{}, StoreEpoch{}, err
	}
	if present {
		if !cachedIsDir || (v.Incarnation != "" && cachedVersion != v) ||
			(v.Incarnation == "" && !cachedVersion.IsZero()) {
			return MirrorToken{}, StoreEpoch{}, ErrMirrorConflict
		}
	}
	if v.Incarnation != "" {
		var isDir int
		if err := s.db.QueryRow(`SELECT is_dir FROM entries WHERE path=?`, name).Scan(&isDir); err != nil {
			return MirrorToken{}, StoreEpoch{}, err
		}
		if isDir != 1 {
			return MirrorToken{}, StoreEpoch{}, ErrMirrorConflict
		}
	}
	epoch, err := captureStoreEpoch(s.db, generation)
	return v, epoch, err
}

func validateExpected(tx *sql.Tx, expected MirrorToken) error {
	if expected.IsZero() {
		return ErrMirrorConflict
	}
	got, err := captureToken(tx, expected.Path)
	if err != nil {
		return err
	}
	if got != expected {
		return ErrMirrorConflict
	}
	return nil
}

// publishEntryLocked installs only committed state. Caller holds s.mu.
func (s *Store) publishEntryLocked(e *Entry) {
	old := s.pathCache[e.Path]
	if old != nil {
		s.removeFromChildrenIdx(old)
		s.evictPathOrphanLocked(old, e)
	}
	s.evictInodeOrphanLocked(e)
	s.subtreeUpsertLocked(old, e)
	s.inodeCache[e.Inode] = e
	s.markCachePublicationLocked(e)
	s.pathCache[e.Path] = e
	s.addToChildrenIdx(e)
}

// PublishExpected updates exactly the observed row (or creates the observed
// absent path) and returns the committed snapshot. It never stamps an old
// observation with a freshly captured token. Legacy zero-token Insert callers
// remain outside this guarantee and must capture before their external reads.
func (s *Store) PublishExpected(expected MirrorToken, entry *Entry) (*Entry, error) {
	if entry == nil || expected.Path != entry.Path || StreamPartialName(entry.Name) {
		return nil, ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.mu.RLock()
	cached := s.pathCache[entry.Path]
	conflict := cached != nil && cached.Version != expected
	s.mu.RUnlock()
	if conflict {
		return nil, ErrMirrorConflict
	}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := validateExpected(tx, expected); err != nil {
		return nil, err
	}
	e := entry.snapshot()
	e.ResetGetAttrCache()
	if err := ftsExternalUpsert(tx, e, s.ftsDefer); err != nil {
		return nil, err
	}
	if err := loadEntryVersion(tx, e); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.publishEntryLocked(e)
	s.mu.Unlock()
	s.wakeFTSCompactor()
	return e.snapshot(), nil
}

// ConfirmAbsentDirectoryFromFUSE conditionally promotes a descriptor-confirmed
// directory that has not yet reached the durable mirror. A cache-only entry
// may be replaced only if no cache publication occurred after observation.
func (s *Store) ConfirmAbsentDirectoryFromFUSE(expected MirrorToken, observed StoreEpoch, entry *Entry) (*Entry, error) {
	if entry == nil || !entry.IsDir || expected.Path != entry.Path || expected.Incarnation != "" ||
		expected.StoreID == "" || observed.StoreID != expected.StoreID || observed.Epoch != expected.Epoch {
		return nil, ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.cachePublicationMu.Lock()
	defer s.cachePublicationMu.Unlock()
	s.mu.RLock()
	conflict := s.cacheGeneration != observed.CacheGeneration || s.preparedCreateConflictLocked(entry.Path)
	cached := s.pathCache[entry.Path]
	present := cached != nil
	var cachedInode uint64
	if present {
		conflict = conflict || !cached.Version.IsZero() || !cached.IsDir
		cachedInode = cached.Inode
	}
	s.mu.RUnlock()
	if conflict {
		return nil, ErrMirrorConflict
	}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := validateExpected(tx, expected); err != nil {
		return nil, err
	}
	e := entry.snapshot()
	if present {
		e.Inode = cachedInode
	}
	e.ResetGetAttrCache()
	if err := ftsExternalUpsert(tx, e, s.ftsDefer); err != nil {
		return nil, err
	}
	if err := loadEntryVersion(tx, e); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.publishEntryLocked(e)
	s.mu.Unlock()
	s.wakeFTSCompactor()
	return e.snapshot(), nil
}

// SetSizeExactExpected is a mirror-only CAS. The caller remains responsible
// for the backing mutation, durability, recovery intent and namespace lease.
func (s *Store) SetSizeExactExpected(expected MirrorToken, size int64, mtime time.Time) (*Entry, error) {
	if expected.Incarnation == "" || size < 0 {
		return nil, ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if cached := s.pathCache[expected.Path]; cached != nil && cached.Version != expected {
		return nil, ErrMirrorConflict
	}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	result, err := tx.Exec(`UPDATE entries SET size=?, mtime=?, mtime_nsec=NULL, revision=revision+1
 WHERE path=? AND incarnation=? AND revision=?`, size, mtime.Unix(), expected.Path, expected.Incarnation, expected.Revision)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneRow(result); err != nil {
		return nil, err
	}
	e, err := scanEntry(tx.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, expected.Path))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	s.publishEntryLocked(e)
	return e.snapshot(), nil
}

func exactlyOneRow(result sql.Result) error {
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: affected %d rows, want 1", ErrMirrorConflict, n)
	}
	return nil
}

// DeleteExpected removes only the captured incarnation and returns the resulting
// absent-path token. The token can be passed to PublishExpected for recreation.
func (s *Store) DeleteExpected(expected MirrorToken) (MirrorToken, error) {
	if expected.Incarnation == "" {
		return MirrorToken{}, ErrMirrorConflict
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.pathCache[expected.Path]; e != nil && e.Version != expected {
		return MirrorToken{}, ErrMirrorConflict
	}
	tx, err := s.db.Begin()
	if err != nil {
		return MirrorToken{}, err
	}
	defer tx.Rollback()
	if err := validateExpected(tx, expected); err != nil {
		return MirrorToken{}, err
	}
	if err := deleteMirrorRow(tx, expected.Path, s.ftsDefer); err != nil {
		return MirrorToken{}, err
	}
	absent, err := captureToken(tx, expected.Path)
	if err != nil {
		return MirrorToken{}, err
	}
	if err := tx.Commit(); err != nil {
		return MirrorToken{}, err
	}
	if e := s.pathCache[expected.Path]; e != nil {
		s.removeFromChildrenIdx(e)
		s.subtreeRemoveLocked(e)
		if s.inodeCache[e.Inode] == e {
			delete(s.inodeCache, e.Inode)
		}
		delete(s.pathCache, expected.Path)
	}
	delete(s.dirVisibilityMtime, expected.Path)
	return absent, nil
}

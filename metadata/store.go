package metadata

import (
	"database/sql"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS entries (
    path        TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    parent_path TEXT NOT NULL,
    is_dir      INTEGER NOT NULL,
    size        INTEGER NOT NULL,
    mtime       INTEGER NOT NULL,
    inode       INTEGER NOT NULL,
    mode        INTEGER NOT NULL,
    local_only  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_parent ON entries(parent_path);
CREATE INDEX IF NOT EXISTS idx_inode ON entries(inode);
CREATE INDEX IF NOT EXISTS idx_name ON entries(name COLLATE NOCASE);
`

// ftsSchema creates a full-text search virtual table for instant filename search.
// Uses FTS5 with a trigram tokenizer so partial matches work (e.g. "explosion" matches
// "Big_Explosion_4K.mov"). NO triggers — FTS is rebuilt manually after bulk operations
// and updated incrementally for individual inserts/deletes. This avoids the massive
// overhead of per-row trigram indexing during BulkInsert (131K entries).
const ftsSchema = `
CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(
    name,
    path,
    content='entries',
    content_rowid='rowid',
    tokenize='trigram'
);
`

// dropFTSTriggers removes any legacy FTS triggers from previous versions.
const dropFTSTriggers = `
DROP TRIGGER IF EXISTS entries_ai;
DROP TRIGGER IF EXISTS entries_ad;
DROP TRIGGER IF EXISTS entries_au;
`

const pragmas = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -8000;
PRAGMA busy_timeout = 30000;
`

// DefaultMaxCacheSize is the default maximum number of entries in the in-memory caches.
const DefaultMaxCacheSize = 500_000

// Store is a SQLite-backed metadata store with in-memory caches.
type Store struct {
	db           *sql.DB
	writeMu      sync.Mutex   // serializes all SQLite write operations (eliminates SQLITE_BUSY)
	mu           sync.RWMutex // protects in-memory caches
	inodeCache   map[uint64]*Entry
	pathCache    map[string]*Entry
	childrenIdx  map[string]map[string]*Entry // parentPath → {path → *Entry}
	maxCacheSize int
}

// Open creates or opens a SQLite metadata database at the given path.
// Use ":memory:" for an in-memory database (tests).
func Open(dbPath string) (*Store, error) {
	return OpenWithMaxCacheSize(dbPath, DefaultMaxCacheSize)
}

// OpenWithMaxCacheSize creates or opens a SQLite metadata database with a
// configurable maximum cache size. When the in-memory caches exceed this
// limit, the oldest entries (by mtime) are evicted.
func OpenWithMaxCacheSize(dbPath string, maxCacheSize int) (*Store, error) {
	// For in-memory databases with multiple connections, use shared cache
	// so all connections see the same database.
	if dbPath == ":memory:" {
		dbPath = "file::memory:?mode=memory&cache=shared"
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	db.SetMaxOpenConns(8)

	if _, err := db.Exec(pragmas); err != nil {
		db.Close()
		return nil, fmt.Errorf("set pragmas: %w", err)
	}

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}

	if _, err := db.Exec(ftsSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create FTS schema: %w", err)
	}

	// Drop legacy triggers (FTS is maintained manually for performance)
	if _, err := db.Exec(dropFTSTriggers); err != nil {
		db.Close()
		return nil, fmt.Errorf("drop FTS triggers: %w", err)
	}

	if maxCacheSize <= 0 {
		maxCacheSize = DefaultMaxCacheSize
	}

	s := &Store{
		db:           db,
		inodeCache:   make(map[uint64]*Entry),
		pathCache:    make(map[string]*Entry),
		childrenIdx:  make(map[string]map[string]*Entry),
		maxCacheSize: maxCacheSize,
	}

	if err := s.rebuildCaches(); err != nil {
		db.Close()
		return nil, fmt.Errorf("rebuild caches: %w", err)
	}

	// Rebuild FTS index on startup to ensure consistency with the entries table.
	// This is fast (<1s for 131K entries) and only runs once.
	if err := s.RebuildFTS(); err != nil {
		db.Close()
		return nil, fmt.Errorf("rebuild FTS: %w", err)
	}

	return s, nil
}

// Close closes the database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// Insert adds or replaces an entry in the store.
func (s *Store) Insert(e *Entry) error {
	s.writeMu.Lock()
	// Cast inode to int64 for SQLite compatibility (modernc.org/sqlite
	// rejects uint64 values with the high bit set).
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO entries (path, name, parent_path, is_dir, size, mtime, inode, mode, local_only)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.Path, e.Name, e.ParentPath,
		boolToInt(e.IsDir), e.Size, e.Mtime.Unix(),
		int64(e.Inode), uint32(e.Mode), boolToInt(e.LocalOnly),
	)
	s.writeMu.Unlock()

	if err != nil {
		return fmt.Errorf("insert %q: %w", e.Path, err)
	}

	s.mu.Lock()
	// Remove old entry from children index if path existed with different parent
	if old, ok := s.pathCache[e.Path]; ok {
		s.removeFromChildrenIdx(old)
	}
	s.inodeCache[e.Inode] = e
	s.pathCache[e.Path] = e
	s.addToChildrenIdx(e)
	s.mu.Unlock()

	return nil
}

// Delete removes an entry by path.
func (s *Store) Delete(entryPath string) error {
	s.mu.RLock()
	e := s.pathCache[entryPath]
	s.mu.RUnlock()

	s.writeMu.Lock()
	_, err := s.db.Exec(`DELETE FROM entries WHERE path = ?`, entryPath)
	s.writeMu.Unlock()

	if err != nil {
		return fmt.Errorf("delete %q: %w", entryPath, err)
	}

	s.mu.Lock()
	if e != nil {
		delete(s.inodeCache, e.Inode)
		s.removeFromChildrenIdx(e)
	}
	delete(s.pathCache, entryPath)
	s.mu.Unlock()

	return nil
}

// InsertToCache adds an entry to the in-memory cache without touching SQLite.
// This makes the entry immediately visible to NFS LOOKUP/GETATTR while the
// SQLite write may be blocked by a concurrent BulkInsert transaction.
func (s *Store) InsertToCache(e *Entry) {
	s.mu.Lock()
	if old, ok := s.pathCache[e.Path]; ok {
		s.removeFromChildrenIdx(old)
	}
	s.inodeCache[e.Inode] = e
	s.pathCache[e.Path] = e
	s.addToChildrenIdx(e)
	s.mu.Unlock()
}

// DeleteFromCache removes an entry from the in-memory cache without touching SQLite.
func (s *Store) DeleteFromCache(entryPath string) {
	s.mu.Lock()
	if e, ok := s.pathCache[entryPath]; ok {
		delete(s.inodeCache, e.Inode)
		s.removeFromChildrenIdx(e)
		delete(s.pathCache, entryPath)
	}
	s.mu.Unlock()
}

// LookupByInode returns the entry with the given inode, or nil.
func (s *Store) LookupByInode(inode uint64) *Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inodeCache[inode]
}

// LookupByPath returns the entry at the given path, or nil.
func (s *Store) LookupByPath(entryPath string) *Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pathCache[entryPath]
}

// ListChildren returns all entries whose parent_path matches the given path.
// Uses the children index for O(children) lookup instead of O(total entries).
func (s *Store) ListChildren(parentPath string) ([]*Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	children := s.childrenIdx[parentPath]
	if len(children) == 0 {
		return nil, nil
	}
	entries := make([]*Entry, 0, len(children))
	for _, e := range children {
		entries = append(entries, e)
	}
	return entries, nil
}

// BulkInsert inserts entries in batches within transactions.
// batchSize controls how many entries per batch. Defaults to 500 if <= 0.
//
// Locking note: each batch is its own writeMu acquisition. This lets
// concurrent NFS write-back paths (UpdateSize, Insert, applyEvent) and
// pin-store writes get their turn between batches. Previously the entire
// multi-batch loop held writeMu — on a 131K-entry initial sync that meant
// ~seconds of blocked NFS writes, which the user perceived as a frozen
// mount during the "just started" window. With per-batch locking the
// worst-case block is one batch's worth of SQL exec (~ms).
//
// Cache + FTS rebuild done at the very end is left holding mu.Lock for the
// full iteration — that's an in-memory operation, no I/O, and it only
// blocks readers (NFS LOOKUP/GETATTR) not writers, so it's a different
// failure mode that's mitigated by the cache rebuild itself being fast.
func (s *Store) BulkInsert(entries []*Entry, batchSize int) error {
	if batchSize <= 0 {
		batchSize = 500
	}

	for i := 0; i < len(entries); i += batchSize {
		end := i + batchSize
		if end > len(entries) {
			end = len(entries)
		}
		batch := entries[i:end]

		if err := s.bulkInsertBatch(batch); err != nil {
			return err
		}
	}

	// Rebuild caches after all batches insert
	s.mu.Lock()
	for _, e := range entries {
		if old, ok := s.pathCache[e.Path]; ok {
			s.removeFromChildrenIdx(old)
		}
		s.inodeCache[e.Inode] = e
		s.pathCache[e.Path] = e
		s.addToChildrenIdx(e)
	}
	s.evictOldest()
	s.mu.Unlock()

	// Rebuild FTS index after bulk operation. This is much faster than
	// per-row trigger updates (~1s for 131K entries vs minutes with triggers).
	if err := s.RebuildFTS(); err != nil {
		// Log but don't fail BulkInsert — stale FTS is recoverable, lost
		// data isn't. Caller still got their rows.
		return nil
	}

	return nil
}

// bulkInsertBatch runs one transaction holding writeMu only for its own
// duration. Extracted so the outer loop can release between batches.
func (s *Store) bulkInsertBatch(batch []*Entry) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	stmt, err := tx.Prepare(
		`INSERT OR REPLACE INTO entries (path, name, parent_path, is_dir, size, mtime, inode, mode, local_only)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare: %w", err)
	}

	for _, e := range batch {
		_, err := stmt.Exec(
			e.Path, e.Name, e.ParentPath,
			boolToInt(e.IsDir), e.Size, e.Mtime.Unix(),
			int64(e.Inode), uint32(e.Mode), boolToInt(e.LocalOnly),
		)
		if err != nil {
			stmt.Close()
			tx.Rollback()
			return fmt.Errorf("exec %q: %w", e.Path, err)
		}
	}

	stmt.Close()
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// UpdateSize updates the size and mtime for an entry identified by path.
func (s *Store) UpdateSize(entryPath string, size int64, mtime time.Time) error {
	s.writeMu.Lock()
	_, err := s.db.Exec(
		`UPDATE entries SET size = ?, mtime = ? WHERE path = ?`,
		size, mtime.Unix(), entryPath,
	)
	s.writeMu.Unlock()

	if err != nil {
		return fmt.Errorf("update size %q: %w", entryPath, err)
	}

	s.mu.Lock()
	if e, ok := s.pathCache[entryPath]; ok {
		e.Size = size
		e.Mtime = mtime
		e.PreSerializedGetAttr = nil // [JM5] invalidate cached XDR bytes
	}
	s.mu.Unlock()

	return nil
}

// ClearLocalOnly marks an entry as no longer local-only (confirmed in Redis).
func (s *Store) ClearLocalOnly(entryPath string) error {
	s.writeMu.Lock()
	_, err := s.db.Exec(
		`UPDATE entries SET local_only = 0 WHERE path = ?`, entryPath,
	)
	s.writeMu.Unlock()

	if err != nil {
		return fmt.Errorf("clear local_only %q: %w", entryPath, err)
	}

	s.mu.Lock()
	if e, ok := s.pathCache[entryPath]; ok {
		e.LocalOnly = false
	}
	s.mu.Unlock()

	return nil
}

// BulkClearLocalOnly clears the local_only flag for all given paths in a
// single transaction. Much faster than calling ClearLocalOnly individually
// for each path (e.g. 146K entries after a full reconciliation).
func (s *Store) BulkClearLocalOnly(paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	batchSize := 500
	s.writeMu.Lock()
	for i := 0; i < len(paths); i += batchSize {
		end := i + batchSize
		if end > len(paths) {
			end = len(paths)
		}
		batch := paths[i:end]

		tx, err := s.db.Begin()
		if err != nil {
			s.writeMu.Unlock()
			return fmt.Errorf("begin tx: %w", err)
		}

		stmt, err := tx.Prepare(`UPDATE entries SET local_only = 0 WHERE path = ?`)
		if err != nil {
			tx.Rollback()
			s.writeMu.Unlock()
			return fmt.Errorf("prepare: %w", err)
		}

		for _, p := range batch {
			if _, err := stmt.Exec(p); err != nil {
				stmt.Close()
				tx.Rollback()
				s.writeMu.Unlock()
				return fmt.Errorf("exec clear local_only %q: %w", p, err)
			}
		}

		stmt.Close()
		if err := tx.Commit(); err != nil {
			s.writeMu.Unlock()
			return fmt.Errorf("commit: %w", err)
		}
	}
	s.writeMu.Unlock()

	// Update in-memory cache
	s.mu.Lock()
	for _, p := range paths {
		if e, ok := s.pathCache[p]; ok {
			e.LocalOnly = false
		}
	}
	s.mu.Unlock()

	return nil
}

// LocalOnlyEntries returns all entries marked as local_only.
func (s *Store) LocalOnlyEntries() ([]*Entry, error) {
	rows, err := s.db.Query(
		`SELECT path, name, parent_path, is_dir, size, mtime, inode, mode, local_only
		 FROM entries WHERE local_only = 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*Entry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// Count returns the total number of entries.
func (s *Store) Count() (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM entries`).Scan(&count)
	return count, err
}

// AllPaths returns every path in the store (used for reconciliation diffing).
func (s *Store) AllPaths() (map[string]struct{}, error) {
	rows, err := s.db.Query(`SELECT path FROM entries WHERE local_only = 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	paths := make(map[string]struct{})
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		paths[p] = struct{}{}
	}
	return paths, rows.Err()
}

// SearchResult is a single search hit with a relevance rank.
type SearchResult struct {
	Entry *Entry
	Rank  float64 // FTS5 rank (lower = more relevant)
}

// Search performs a full-text search on filenames using the FTS5 trigram index.
// Returns up to `limit` results ordered by relevance. The query matches partial
// substrings (e.g. "explo" matches "Big_Explosion_4K.mov").
// Pass an empty parentPath to search the entire tree, or a path to scope results
// to a subtree (e.g. "SFX/Impacts").
func (s *Store) Search(query string, limit int, parentPath string) ([]SearchResult, error) {
	if query == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 50
	}

	// FTS5 trigram tokenizer supports substring matching with double quotes.
	// Escape any existing double quotes in the query.
	ftsQuery := `"` + strings.ReplaceAll(query, `"`, `""`) + `"`

	var rows *sql.Rows
	var err error

	if parentPath != "" {
		// Scoped search: only entries under parentPath
		rows, err = s.db.Query(
			`SELECT e.path, e.name, e.parent_path, e.is_dir, e.size, e.mtime, e.inode, e.mode, e.local_only, rank
			 FROM entries_fts fts
			 JOIN entries e ON e.rowid = fts.rowid
			 WHERE entries_fts MATCH ?
			   AND e.path LIKE ?
			 ORDER BY rank
			 LIMIT ?`,
			ftsQuery, parentPath+"/%", limit,
		)
	} else {
		rows, err = s.db.Query(
			`SELECT e.path, e.name, e.parent_path, e.is_dir, e.size, e.mtime, e.inode, e.mode, e.local_only, rank
			 FROM entries_fts fts
			 JOIN entries e ON e.rowid = fts.rowid
			 WHERE entries_fts MATCH ?
			 ORDER BY rank
			 LIMIT ?`,
			ftsQuery, limit,
		)
	}

	if err != nil {
		return nil, fmt.Errorf("search %q: %w", query, err)
	}
	defer rows.Close()

	var results []SearchResult
	for rows.Next() {
		var (
			e         Entry
			isDir     int
			mtimeUnix int64
			inodeRaw  int64
			mode      uint32
			localOnly int
			rank      float64
		)
		err := rows.Scan(&e.Path, &e.Name, &e.ParentPath, &isDir, &e.Size, &mtimeUnix, &inodeRaw, &mode, &localOnly, &rank)
		if err != nil {
			return nil, err
		}
		e.Inode = uint64(inodeRaw)
		e.IsDir = isDir != 0
		e.Mtime = time.Unix(mtimeUnix, 0)
		e.Mode = fs.FileMode(mode)
		if e.IsDir {
			e.Mode |= fs.ModeDir
		}
		e.LocalOnly = localOnly != 0
		results = append(results, SearchResult{Entry: &e, Rank: rank})
	}
	return results, rows.Err()
}

// RebuildFTS rebuilds the FTS5 index from the entries table.
// Call this after a bulk data load where triggers may not have fired
// (e.g. initial startup with a pre-existing database).
func (s *Store) RebuildFTS() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	// Delete all FTS content, then re-insert from entries table
	if _, err := s.db.Exec(`INSERT INTO entries_fts(entries_fts) VALUES('delete-all')`); err != nil {
		return fmt.Errorf("fts delete-all: %w", err)
	}
	if _, err := s.db.Exec(`INSERT INTO entries_fts(rowid, name, path) SELECT rowid, name, path FROM entries`); err != nil {
		return fmt.Errorf("fts rebuild: %w", err)
	}
	return nil
}

// DeletePaths removes multiple entries by path in a single transaction.
func (s *Store) DeletePaths(paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	s.writeMu.Lock()
	tx, err := s.db.Begin()
	if err != nil {
		s.writeMu.Unlock()
		return err
	}

	stmt, err := tx.Prepare(`DELETE FROM entries WHERE path = ?`)
	if err != nil {
		tx.Rollback()
		s.writeMu.Unlock()
		return err
	}

	for _, p := range paths {
		if _, err := stmt.Exec(p); err != nil {
			stmt.Close()
			tx.Rollback()
			s.writeMu.Unlock()
			return err
		}
	}
	stmt.Close()

	if err := tx.Commit(); err != nil {
		s.writeMu.Unlock()
		return err
	}
	s.writeMu.Unlock()

	s.mu.Lock()
	for _, p := range paths {
		if e, ok := s.pathCache[p]; ok {
			delete(s.inodeCache, e.Inode)
			s.removeFromChildrenIdx(e)
			delete(s.pathCache, p)
		}
	}
	s.mu.Unlock()

	return nil
}

// CacheStats returns the current size of the path and inode caches.
func (s *Store) CacheStats() (pathCacheSize, inodeCacheSize int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.pathCache), len(s.inodeCache)
}

// evictOldest trims the caches to maxCacheSize by removing the entries
// with the oldest mtime. Must be called with s.mu held for writing.
func (s *Store) evictOldest() {
	if s.maxCacheSize <= 0 || len(s.pathCache) <= s.maxCacheSize {
		return
	}

	// Collect all entries, sort by mtime descending, keep the newest maxCacheSize.
	entries := make([]*Entry, 0, len(s.pathCache))
	for _, e := range s.pathCache {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Mtime.After(entries[j].Mtime)
	})

	// Build new caches from the newest entries.
	newPathCache := make(map[string]*Entry, s.maxCacheSize)
	newInodeCache := make(map[uint64]*Entry, s.maxCacheSize)
	newChildrenIdx := make(map[string]map[string]*Entry)
	for i := 0; i < s.maxCacheSize && i < len(entries); i++ {
		e := entries[i]
		newPathCache[e.Path] = e
		newInodeCache[e.Inode] = e
		children, ok := newChildrenIdx[e.ParentPath]
		if !ok {
			children = make(map[string]*Entry)
			newChildrenIdx[e.ParentPath] = children
		}
		children[e.Path] = e
	}

	s.pathCache = newPathCache
	s.inodeCache = newInodeCache
	s.childrenIdx = newChildrenIdx
}

// rebuildCaches loads all entries from SQLite into the in-memory caches.
func (s *Store) rebuildCaches() error {
	rows, err := s.db.Query(
		`SELECT path, name, parent_path, is_dir, size, mtime, inode, mode, local_only FROM entries`)
	if err != nil {
		return err
	}
	defer rows.Close()

	iCache := make(map[uint64]*Entry)
	pCache := make(map[string]*Entry)
	cIdx := make(map[string]map[string]*Entry)

	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return err
		}
		iCache[e.Inode] = e
		pCache[e.Path] = e
		children, ok := cIdx[e.ParentPath]
		if !ok {
			children = make(map[string]*Entry)
			cIdx[e.ParentPath] = children
		}
		children[e.Path] = e
	}

	s.mu.Lock()
	s.inodeCache = iCache
	s.pathCache = pCache
	s.childrenIdx = cIdx
	s.evictOldest()
	s.mu.Unlock()

	return rows.Err()
}

// MakeEntry is a convenience constructor.
func MakeEntry(entryPath string, isDir bool, size int64, mtime time.Time, inode uint64) *Entry {
	var mode fs.FileMode = 0644
	if isDir {
		mode = 0755 | fs.ModeDir
	}
	return &Entry{
		Path:       entryPath,
		Name:       path.Base(entryPath),
		ParentPath: path.Dir(entryPath),
		IsDir:      isDir,
		Size:       size,
		Mtime:      mtime,
		Inode:      inode,
		Mode:       mode,
	}
}

type scanner interface {
	Scan(dest ...any) error
}

func scanEntry(s scanner) (*Entry, error) {
	var (
		e         Entry
		isDir     int
		mtimeUnix int64
		inodeRaw  int64 // SQLite stores uint64 as int64; high-bit inodes become negative
		mode      uint32
		localOnly int
	)
	err := s.Scan(&e.Path, &e.Name, &e.ParentPath, &isDir, &e.Size, &mtimeUnix, &inodeRaw, &mode, &localOnly)
	if err != nil {
		return nil, err
	}
	e.Inode = uint64(inodeRaw) // reinterpret bits — negative int64 → high-bit uint64
	e.IsDir = isDir != 0
	e.Mtime = time.Unix(mtimeUnix, 0)
	e.Mode = fs.FileMode(mode)
	if e.IsDir {
		e.Mode |= fs.ModeDir
	}
	e.LocalOnly = localOnly != 0
	return &e, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// addToChildrenIdx adds an entry to the children index. Must be called with s.mu held for writing.
func (s *Store) addToChildrenIdx(e *Entry) {
	children, ok := s.childrenIdx[e.ParentPath]
	if !ok {
		children = make(map[string]*Entry)
		s.childrenIdx[e.ParentPath] = children
	}
	children[e.Path] = e
}

// removeFromChildrenIdx removes an entry from the children index. Must be called with s.mu held for writing.
func (s *Store) removeFromChildrenIdx(e *Entry) {
	if children, ok := s.childrenIdx[e.ParentPath]; ok {
		delete(children, e.Path)
		if len(children) == 0 {
			delete(s.childrenIdx, e.ParentPath)
		}
	}
}

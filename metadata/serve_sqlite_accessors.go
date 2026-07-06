package metadata

import (
	"database/sql"
	"errors"
	"log"
	"os"
	"sync"
	"time"

	"io/fs"
)

// SQLite-direct serve accessors (serving-layer-decision.md §Item 2).
//
// JM_SERVE_FROM_SQLITE (default OFF) switches the three serve accessors
// (LookupByPath, LookupByInode, ListChildren in store.go) to query SQLite-WAL
// directly instead of reading the in-RAM shadow maps. WAL readers snapshot
// around a contending writer, so serve-read p99 no longer stalls behind the
// writer's s.mu.Lock chunks (the felt "10GbE nav sluggish under a writer"
// problem). Both the RAM and SQLite code paths are compiled in; the flag picks
// per call INSIDE the accessor, so no handler.go call site changes and rollback
// is one env var on the same binary. See serve_sqlite.go for the prepared-stmt
// concurrency model (the crux).

// serveFromSQLite reports whether the serve accessors should read SQLite-WAL
// directly. Default OFF. Cached once at first read (sync.Once) so the per-RPC
// cost is a single load and the boot-time substrate log fires exactly once.
var (
	serveFromSQLiteOnce sync.Once
	serveFromSQLiteVal  bool
)

func serveFromSQLite() bool {
	serveFromSQLiteOnce.Do(func() {
		serveFromSQLiteVal = os.Getenv("JM_SERVE_FROM_SQLITE") == "1"
	})
	return serveFromSQLiteVal
}

// ServeFromSQLite exports the serve-substrate predicate for peer packages
// (the NFS protocol layer) that need to skip RAM-shadow-only fast paths — e.g.
// the GETATTR pre-serialization cache, which is a no-op when serving throwaway
// Entries from SQLite (review fix, LOW).
func ServeFromSQLite() bool { return serveFromSQLite() }

// logServeSubstrate logs the chosen serve substrate ONCE per boot (not per RPC)
// so an A/B run is attributable in the logs. Called from OpenWithMaxCacheSize
// after the store is wired.
var logServeSubstrateOnce sync.Once

func logServeSubstrate() {
	logServeSubstrateOnce.Do(func() {
		if serveFromSQLite() {
			log.Printf("metadata: serve substrate = SQLite-WAL (JM_SERVE_FROM_SQLITE=1); readdir-paginated=%v", readdirPaginatedEnabled())
		} else {
			log.Printf("metadata: serve substrate = RAM shadow maps (default); readdir-paginated=%v", readdirPaginatedEnabled())
		}
	})
}

// lookupByPathSQLite is the SQLite-backed LookupByPath (JM_SERVE_FROM_SQLITE=1).
// Prepared PRIMARY-KEY point lookup on entries(path). Returns nil on no-row,
// exactly like the RAM pathCache miss. On a stmt-init error it falls back to the
// RAM map so a prepare failure can never take down serving.
func (s *Store) lookupByPathSQLite(entryPath string) *Entry {
	if err := s.initServeStmts(); err != nil {
		log.Printf("metadata: serve stmt init failed, falling back to RAM for LookupByPath(%q): %v", entryPath, err)
		return s.lookupByPathRAM(entryPath)
	}
	e, err := scanRow(s.serve.byPath.QueryRow(entryPath))
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			log.Printf("metadata: LookupByPath SQLite query %q: %v", entryPath, err)
			return nil
		}
		// sql.ErrNoRows: SQLite may not have the row YET. Create/rename seed
		// the RAM map synchronously (InsertToCache) but commit to SQLite via
		// `go Insert` async, so during a BulkInsert flood the committed WAL
		// snapshot can miss a just-created path that RAM already has. RAM is
		// ALWAYS same-or-ahead of SQLite (never staler — SQLite is only ever
		// written after/with RAM), so back-stop the miss with the RAM map.
		// This closes the rename-drop (review MED) and the async cold-stat
		// window; the RLock is paid ONLY on a miss (the common HIT path stays
		// lock-free, preserving the write-preferring-RWMutex contention win).
		return s.lookupByPathRAM(entryPath)
	}
	return e
}

// lookupByInodeSQLite is the SQLite-backed LookupByInode. Prepared point lookup
// on idx_inode. The inode is passed as int64 (reinterpret bits) because
// modernc.org/sqlite rejects a uint64 with the high bit set — the same cast the
// writers use (ftsExternalUpsert). Returns nil on no-row.
func (s *Store) lookupByInodeSQLite(inode uint64) *Entry {
	if err := s.initServeStmts(); err != nil {
		log.Printf("metadata: serve stmt init failed, falling back to RAM for LookupByInode(%d): %v", inode, err)
		return s.lookupByInodeRAM(inode)
	}
	e, err := scanRow(s.serve.byInode.QueryRow(int64(inode)))
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			log.Printf("metadata: LookupByInode SQLite query %d: %v", inode, err)
			return nil
		}
		// sql.ErrNoRows: back-stop the async-commit window with the RAM map
		// (same rationale as lookupByPathSQLite — RAM is never staler than
		// SQLite; RLock only on the miss path).
		return s.lookupByInodeRAM(inode)
	}
	return e
}

// scanRow scans a single-row point query into an *Entry (same semantics as
// scanEntry / scanEntryPooled — high-bit inode reinterpret, time.Unix, ModeDir
// OR-in, local_only). Returns sql.ErrNoRows unwrapped for the caller to treat as
// a cache miss.
func scanRow(row *sql.Row) (*Entry, error) {
	var (
		e         Entry
		isDir     int
		mtimeUnix int64
		inodeRaw  int64
		mode      uint32
		localOnly int
	)
	if err := row.Scan(
		&e.Path, &e.Name, &e.ParentPath,
		&isDir, &e.Size, &mtimeUnix, &inodeRaw, &mode, &localOnly,
	); err != nil {
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
	return &e, nil
}

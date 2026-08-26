package metadata

import (
	"database/sql"
	"io/fs"
	"os"
	"sync"
	"time"
)

// SQLite-direct serve path (serving-layer-decision.md §Item 1 + §Item 2).
//
// Two independent, default-off env flags let the SAME binary A/B old-vs-new
// with one env var and roll back instantly (no rebuild, no redeploy churn —
// per feedback_testing_mount_safety a live mount must not be churned):
//
//   - JM_READDIR_PAGINATED (Item 1): ReadDir builds its listing by iterating
//     ListChildrenPage in bounded pages off idx_parent instead of one giant
//     whole-dir map copy. Kills the big-dir veto (the 10,774-child DCIM dir was
//     12.7ms/6.26MB/215k allocs as a single full scan+copy).
//
//   - JM_SERVE_FROM_SQLITE (Item 2): the three serve accessors (LookupByPath,
//     LookupByInode, ListChildren) query SQLite-WAL directly instead of reading
//     the in-RAM shadow maps. WAL readers snapshot around a contending writer,
//     so serve-read p99 no longer stalls behind the writer's s.mu.Lock chunks
//     (the felt "10GbE nav sluggish under a writer" problem). Both the RAM and
//     the SQLite code paths are compiled in; the flag picks per call INSIDE the
//     accessor, so no handler.go call site changes.
//
// CONCURRENCY MODEL (the crux — get this wrong and the whole benefit is lost):
// every SQLite serve query runs against a lazily-prepared *sql.Stmt owned by the
// Store and prepared against the *sql.DB pool (NOT a single conn). With
// modernc.org/sqlite, *sql.DB is concurrency-safe and pools connections
// (SetMaxOpenConns(8)); a *sql.Stmt from db.Prepare is safe for concurrent use
// by many goroutines — database/sql re-prepares it per pooled connection on
// demand and caches that per-conn, so N concurrent NFS goroutines each take a
// pooled conn and run the point/page query on it with NO shared Go-level lock in
// the hot path and NO per-RPC Prepare. The only synchronization is a one-time
// sync.Once for lazy stmt init. This satisfies feedback_perf_hot_path: a warm
// prepared point/index query against the WAL page cache is not a cold
// FUSE/Redis syscall-per-RPC.

// readdirPaginatedEnabled reports whether ReadDir should page the mirror via
// ListChildrenPage (Item 1). Default OFF. Read per call — cheap (a map lookup in
// the Go runtime's env cache); the value is stable for a process lifetime.
func readdirPaginatedEnabled() bool {
	return os.Getenv("JM_READDIR_PAGINATED") == "1"
}

// Prepared-statement handles for the SQLite serve path. Lazily prepared against
// the *sql.DB pool on first use (see concurrency note above). Nil until then.
//
// The column list MATCHES scanEntry exactly (9 columns incl. local_only) so a
// row scanned here is byte-for-byte the Entry that rebuildCaches builds from the
// same row — this is what makes the SQLite path a true parity substitute for the
// RAM maps (LocalOnly, high-bit inode reinterpret, ModeDir OR-in, mtime).
const serveColumns = `path, name, parent_path, is_dir, size, mtime, inode, mode, local_only`

type serveStmts struct {
	once        sync.Once
	err         error
	byPath      *sql.Stmt // WHERE path = ?           (PRIMARY KEY point lookup)
	byInode     *sql.Stmt // WHERE inode = ?           (idx_inode point lookup)
	childrenPg  *sql.Stmt // WHERE parent_path=? AND name>? ORDER BY name LIMIT ?  (idx_parent page)
	childrenAll *sql.Stmt // WHERE parent_path = ?      (idx_parent whole-dir, for SQLite ListChildren when not paginating)
}

// serve holds the lazily-prepared serve statements. Pointer so the zero value
// (all-nil, once-unfired) is valid and prepared on first serve query.
//
// NOTE: added as an unexported field on Store in store.go's struct; initialized
// to a &serveStmts{} in OpenWithMaxCacheSize.

// initServeStmts lazily prepares the serve statements against the DB pool.
// Idempotent; safe under concurrent first-callers (sync.Once). Returns the
// prepare error (once) if any statement failed — callers fall back to the RAM
// path on error so a prepare failure can never take down serving.
func (s *Store) initServeStmts() error {
	s.serve.once.Do(func() {
		var err error
		s.serve.byPath, err = s.db.Prepare(
			`SELECT ` + serveColumns + ` FROM entries WHERE path = ?`)
		if err != nil {
			s.serve.err = err
			return
		}
		s.serve.byInode, err = s.db.Prepare(
			`SELECT ` + serveColumns + ` FROM entries WHERE inode = ?`)
		if err != nil {
			s.serve.err = err
			return
		}
		s.serve.childrenPg, err = s.db.Prepare(
			`SELECT ` + serveColumns + ` FROM entries WHERE parent_path = ? AND name > ? ORDER BY name LIMIT ?`)
		if err != nil {
			s.serve.err = err
			return
		}
		s.serve.childrenAll, err = s.db.Prepare(
			`SELECT ` + serveColumns + ` FROM entries WHERE parent_path = ?`)
		if err != nil {
			s.serve.err = err
			return
		}
	})
	return s.serve.err
}

// scanScratchPool recycles the scalar scan destinations across rows so a big-dir
// page (or a whole-dir scan) does not allocate a fresh set per row. Each Entry
// itself is still heap-allocated (it escapes into the returned slice), but the
// int/int64/uint32 scan temporaries are pooled — that is the bulk of the
// 215k-allocs the design flags on the big DCIM dir.
type scanScratch struct {
	isDir     int
	mtimeUnix int64
	inodeRaw  int64
	mode      uint32
	localOnly int
}

var scanScratchPool = sync.Pool{New: func() any { return new(scanScratch) }}

// scanEntryPooled scans one row into a fresh *Entry using pooled scalar
// scratch. Semantics IDENTICAL to scanEntry (types.go) — same high-bit inode
// reinterpret, same time.Unix, same ModeDir OR-in, same local_only — so a page
// row here equals the RAM map's Entry for that row.
func scanEntryPooled(rows *sql.Rows) (*Entry, error) {
	sc := scanScratchPool.Get().(*scanScratch)
	defer scanScratchPool.Put(sc)

	e := &Entry{}
	if err := rows.Scan(
		&e.Path, &e.Name, &e.ParentPath,
		&sc.isDir, &e.Size, &sc.mtimeUnix, &sc.inodeRaw, &sc.mode, &sc.localOnly,
	); err != nil {
		return nil, err
	}
	e.Inode = uint64(sc.inodeRaw) // reinterpret bits — negative int64 → high-bit uint64
	e.IsDir = sc.isDir != 0
	e.Mtime = time.Unix(sc.mtimeUnix, 0)
	e.Mode = fs.FileMode(sc.mode)
	if e.IsDir {
		e.Mode |= fs.ModeDir
	}
	e.LocalOnly = sc.localOnly != 0
	e.prepareGetAttrCache()
	return e, nil
}

// ListChildrenPage returns one READDIR page of the children of parentPath:
// entries whose name sorts strictly after `cursor`, ordered by name, capped at
// `limit`. nextCursor is the name of the last returned entry (pass it back as
// `cursor` for the next page) or "" when this is the final page (fewer than
// `limit` rows returned). This is the Item 1 primitive: a prepared point-range
// query on idx_parent, one page per query, never materializing a 10k-entry dir.
//
// Item 1 does NOT change what ReadDir returns (still the whole dir, just built
// in pages); the protocol layer (nfs_onreaddir.go) still hashes+caches the full
// listing and paginates by index. Streaming the source in bounded pages is what
// removes the single-full-scan alloc/latency spike.
func (s *Store) ListChildrenPage(parentPath, cursor string, limit int) (entries []*Entry, nextCursor string, err error) {
	if limit <= 0 {
		limit = 1000
	}
	if err := s.initServeStmts(); err != nil {
		return nil, "", err
	}
	rows, err := s.serve.childrenPg.Query(parentPath, cursor, limit)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	entries = make([]*Entry, 0, limit)
	for rows.Next() {
		e, err := scanEntryPooled(rows)
		if err != nil {
			return nil, "", err
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	// A short page (< limit) is the last page → no next cursor. A full page
	// carries the last name as the cursor for the next call.
	if len(entries) == limit && limit > 0 {
		nextCursor = entries[len(entries)-1].Name
	}
	return entries, nextCursor, nil
}

// listChildrenSQLite is the SQLite-backed whole-dir ListChildren used by the
// serve accessor when JM_SERVE_FROM_SQLITE=1. It pages internally via
// ListChildrenPage (bounded scan, pooled scratch) and concatenates, so even a
// big dir never runs a single unbounded scan. Semantics match the RAM
// ListChildren: returns (nil, nil) for an empty/unmirrored dir, and the full
// child set otherwise (order is by name here vs map-iteration order in RAM —
// callers sort, see below).
//
// ORDER NOTE: the RAM ListChildren returns children in Go map-iteration
// (nondeterministic) order; every serve caller that cares sorts by name
// (ReadDir at handler.go, nfs_onreaddir.go's getDirListingWithVerifier). This
// returns them already name-sorted, which is a STRICT SUPERSET of the RAM
// contract (same set, a defined order) — never a divergence in the entry SET.
const listChildrenPageSize = 1000

func (s *Store) listChildrenSQLite(parentPath string) ([]*Entry, error) {
	var out []*Entry
	cursor := ""
	for {
		page, next, err := s.ListChildrenPage(parentPath, cursor, listChildrenPageSize)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if next == "" {
			break
		}
		cursor = next
	}
	if len(out) == 0 {
		// Match RAM ListChildren's (nil, nil) for an empty dir exactly.
		return nil, nil
	}
	return out, nil
}

// ListChildrenForReadDir returns the whole directory listing for the NFS
// ReadDir path, choosing the substrate per the flags:
//
//   - JM_SERVE_FROM_SQLITE=1: SQLite ListChildren (already internally paged).
//   - JM_READDIR_PAGINATED=1 (Item 1): build the whole listing by iterating
//     ListChildrenPage off idx_parent in bounded pages with pooled scratch —
//     this kills the single-full-scan alloc/latency spike on a big dir even
//     while the RAM maps remain the serve substrate for point lookups.
//   - both off (default): the RAM childrenIdx whole-dir copy, unchanged.
//
// The RETURNED SET is identical across all three (the mirror's children of
// parentPath); only how it is built differs. The NFS protocol layer still
// hashes+caches the full listing and paginates by index (nfs_onreaddir.go), so
// this returns the whole dir — Item 1 changes the BUILD cost, not the contract.
func (s *Store) ListChildrenForReadDir(parentPath string) ([]*Entry, error) {
	if serveFromSQLite() {
		// Item 2: SQLite serve substrate — listChildrenSQLite already pages
		// internally off idx_parent_name.
		return s.listChildrenSQLite(parentPath)
	}
	if readdirPaginatedEnabled() {
		// Item 1: paged idx_parent_name scan with pooled scratch — kills the
		// big-dir single-scan alloc/latency spike while RAM stays the point-
		// lookup substrate.
		return s.listChildrenSQLite(parentPath)
	}
	return s.listChildrenRAM(parentPath)
}

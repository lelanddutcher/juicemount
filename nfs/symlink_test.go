package nfs

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

// newSymlinkTestFS builds a juiceFS backed by a real temp dir standing in for
// the JuiceFS FUSE mount root (a POSIX fs that supports os.Symlink/Readlink/
// Lstat), with NO spool wired — symlinks carry no data and must not route
// through the write spool. Returns the juiceFS and the FUSE root path so a
// test can verify the on-disk symlink directly.
func newSymlinkTestFS(t *testing.T) (*juiceFS, string) {
	t.Helper()
	fuseRoot := t.TempDir()
	store, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	handler := NewHandler(store, fuseRoot)
	t.Cleanup(func() {
		handler.StopHandler()
		_ = store.Close()
	})
	return &juiceFS{handler: handler}, fuseRoot
}

// TestSymlinkCreateLstatReadlink covers the JM symlink fix end to end at the
// billy.Filesystem layer the go-nfs fork's onSymlink/onReadLink call into:
//   - Symlink creates a real symlink on FUSE,
//   - a cache-hit Lstat reports os.ModeSymlink (so NFS emits type=NF3LNK and
//     the client then issues READLINK),
//   - Readlink returns the target verbatim,
//   - ReadDir surfaces the entry with ModeSymlink,
//   - a RELATIVE target (e.g. "A", "Versions/Current/Lib") round-trips,
//   - the on-disk FUSE symlink matches.
func TestSymlinkCreateLstatReadlink(t *testing.T) {
	jfs, fuseRoot := newSymlinkTestFS(t)

	// Absolute-looking and relative targets are both stored verbatim.
	cases := []struct {
		name   string
		link   string
		target string
	}{
		{"relative-sibling", "Current", "A"},
		{"relative-nested", "Lib.dylib", "Versions/Current/Lib"},
		{"absolute", "absLink", "/usr/lib/libSystem.B.dylib"},
		{"dot-relative", "dotLink", "./peer"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := jfs.Symlink(tc.target, tc.link); err != nil {
				t.Fatalf("Symlink(%q, %q): %v", tc.target, tc.link, err)
			}

			// Cache-hit Lstat must report ModeSymlink — this is what makes
			// the NFS GETATTR/LOOKUP report type=symlink so the client
			// follows up with READLINK instead of treating it as a file.
			fi, err := jfs.Lstat(tc.link)
			if err != nil {
				t.Fatalf("Lstat(%q): %v", tc.link, err)
			}
			if fi.Mode()&os.ModeSymlink == 0 {
				t.Fatalf("Lstat(%q) mode = %v, want ModeSymlink set", tc.link, fi.Mode())
			}
			if fi.IsDir() {
				t.Fatalf("Lstat(%q) reports IsDir, want symlink", tc.link)
			}

			// Readlink returns the target VERBATIM — never resolved.
			got, err := jfs.Readlink(tc.link)
			if err != nil {
				t.Fatalf("Readlink(%q): %v", tc.link, err)
			}
			if got != tc.target {
				t.Fatalf("Readlink(%q) = %q, want verbatim %q", tc.link, got, tc.target)
			}

			// On-disk FUSE symlink must match (proves we created it on FUSE,
			// not just in the metadata mirror).
			onDisk, err := os.Readlink(filepath.Join(fuseRoot, tc.link))
			if err != nil {
				t.Fatalf("os.Readlink on FUSE %q: %v", tc.link, err)
			}
			if onDisk != tc.target {
				t.Fatalf("on-disk symlink target = %q, want %q", onDisk, tc.target)
			}
		})
	}

	// ReadDir must surface the symlinks with ModeSymlink so a directory
	// enumeration (Finder bundle copy) classifies them correctly.
	infos, err := jfs.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	seen := map[string]os.FileMode{}
	for _, fi := range infos {
		seen[fi.Name()] = fi.Mode()
	}
	for _, tc := range cases {
		mode, ok := seen[tc.link]
		if !ok {
			t.Fatalf("ReadDir missing symlink %q", tc.link)
		}
		if mode&os.ModeSymlink == 0 {
			t.Fatalf("ReadDir %q mode = %v, want ModeSymlink", tc.link, mode)
		}
	}
}

// TestSymlinkAlreadyExists verifies a SYMLINK over an existing path maps to an
// EEXIST-class error (onSymlink surfaces this as NFSStatusExist), not a silent
// success or a generic access error.
func TestSymlinkAlreadyExists(t *testing.T) {
	jfs, _ := newSymlinkTestFS(t)

	if err := jfs.Symlink("target-a", "dup"); err != nil {
		t.Fatalf("first Symlink: %v", err)
	}
	err := jfs.Symlink("target-b", "dup")
	if err == nil {
		t.Fatal("second Symlink over existing path: want error, got nil")
	}
	if !os.IsExist(err) {
		t.Fatalf("second Symlink error = %v, want os.IsExist", err)
	}

	// The original target must be untouched.
	got, err := jfs.Readlink("dup")
	if err != nil {
		t.Fatalf("Readlink after dup: %v", err)
	}
	if got != "target-a" {
		t.Fatalf("Readlink after dup = %q, want unchanged %q", got, "target-a")
	}
}

// TestSymlinkDanglingTarget confirms a symlink whose target does NOT exist is
// still created, Lstat'd as a symlink, and Readlink'd — Lstat must not follow
// the link (which would ENOENT) and the phantom-purge gate must not delete it.
func TestSymlinkDanglingTarget(t *testing.T) {
	jfs, _ := newSymlinkTestFS(t)

	if err := jfs.Symlink("does/not/exist", "dangling"); err != nil {
		t.Fatalf("Symlink dangling: %v", err)
	}

	fi, err := jfs.Lstat("dangling")
	if err != nil {
		t.Fatalf("Lstat dangling: %v", err)
	}
	if fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("Lstat dangling mode = %v, want ModeSymlink", fi.Mode())
	}

	got, err := jfs.Readlink("dangling")
	if err != nil {
		t.Fatalf("Readlink dangling: %v", err)
	}
	if got != "does/not/exist" {
		t.Fatalf("Readlink dangling = %q, want %q", got, "does/not/exist")
	}
}

// newSymlinkSpoolTestFS builds a juiceFS WITH a spool wired (so the offline
// symlink-defer path can persist + materialize) plus a Drainer, returning the
// fs, the FUSE root, the spool, and the drainer. Unlike newSymlinkTestFS this
// is the offline-ingest configuration: a symlink created while offline is NOT
// os.Symlink'd onto FUSE but deferred to the pending_symlinks table and
// materialized by the drainer on reconnect.
func newSymlinkSpoolTestFS(t *testing.T) (*juiceFS, string, *SpoolStore, *Drainer) {
	t.Helper()
	fuseRoot := t.TempDir()

	store, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}

	// Spool with its own SQLite DB (entries store and spool are independent in
	// the handler; only the production binary co-locates them in one file).
	dbPath := filepath.Join(t.TempDir(), "spool.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open spool db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;`); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if err := metadata.InitSpoolSchema(db); err != nil {
		t.Fatalf("init spool schema: %v", err)
	}
	meta := metadata.NewSpoolStore(db)
	spool, err := NewSpoolStore(t.TempDir(), 0, meta)
	if err != nil {
		t.Fatalf("new spool store: %v", err)
	}

	drainer, err := NewDrainer(spool, DrainerConfig{FuseRoot: fuseRoot})
	if err != nil {
		t.Fatalf("new drainer: %v", err)
	}

	handler := NewHandler(store, fuseRoot)
	handler.SetSpool(spool, drainer) // registers onSymlinkMaterialized
	t.Cleanup(func() {
		handler.StopHandler()
		_ = store.Close()
	})

	return &juiceFS{handler: handler}, fuseRoot, spool, drainer
}

// TestSymlinkOfflineDeferAndReconnect is the release-blocking offline-symlink
// fix end to end:
//
//   - OFFLINE: juiceFS.Symlink returns nil (NOT an NFSStatusAccess-class error),
//     does NOT touch FUSE, records the link in the metadata cache as a symlink
//     (so the Finder bundle copy's follow-up LOOKUP/Lstat/READLINK succeed), and
//     persists (link, target) to pending_symlinks.
//   - Readlink offline returns the verbatim target from the pending store even
//     though nothing is on FUSE yet.
//   - RECONNECT: the drainer's reconnect hook materializes the link on FUSE
//     (real os.Symlink), clears LocalOnly, and removes the pending row.
func TestSymlinkOfflineDeferAndReconnect(t *testing.T) {
	wasOffline := pin.IsOffline()
	t.Cleanup(func() { pin.SetOffline(wasOffline) })

	jfs, fuseRoot, spool, drainer := newSymlinkSpoolTestFS(t)

	const link = "Current"
	const target = "Versions/A" // relative target, the .framework shape

	// --- OFFLINE: defer, don't fail ---
	pin.SetOffline(true)

	if err := jfs.Symlink(target, link); err != nil {
		t.Fatalf("offline Symlink returned error (would abort Finder copy): %v", err)
	}

	// MUST NOT be on FUSE yet (a FUSE op offline is exactly what we avoid).
	if _, err := os.Lstat(filepath.Join(fuseRoot, link)); !os.IsNotExist(err) {
		t.Fatalf("offline Symlink wrote to FUSE; Lstat err = %v, want IsNotExist", err)
	}

	// Cache entry present and classified as a symlink (NFS type=NF3LNK).
	fi, err := jfs.Lstat(link)
	if err != nil {
		t.Fatalf("offline Lstat(%q): %v", link, err)
	}
	if fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("offline Lstat mode = %v, want ModeSymlink", fi.Mode())
	}

	// Readlink served from the pending store (nothing on FUSE).
	got, err := jfs.Readlink(link)
	if err != nil {
		t.Fatalf("offline Readlink(%q): %v", link, err)
	}
	if got != target {
		t.Fatalf("offline Readlink = %q, want %q", got, target)
	}

	// Persisted so it survives a restart.
	if pt, err := spool.Meta().GetPendingSymlink(link); err != nil || pt != target {
		t.Fatalf("pending symlink not persisted: target=%q err=%v, want %q", pt, err, target)
	}

	// --- RECONNECT: materialize on FUSE ---
	pin.SetOffline(false)
	drainer.materializePendingSymlinks()

	// Now a REAL symlink exists on FUSE with the verbatim target.
	onDisk, err := os.Readlink(filepath.Join(fuseRoot, link))
	if err != nil {
		t.Fatalf("post-reconnect os.Readlink on FUSE: %v", err)
	}
	if onDisk != target {
		t.Fatalf("materialized FUSE symlink target = %q, want %q", onDisk, target)
	}

	// Pending row removed (idempotent: re-running is a no-op).
	if _, err := spool.Meta().GetPendingSymlink(link); err == nil {
		t.Fatal("pending symlink row not removed after materialization")
	}

	// LocalOnly cleared on the cache entry (now a real backend entry).
	if e := jfs.handler.store.LookupByPath(link); e == nil {
		t.Fatal("cache entry missing after materialization")
	} else if e.LocalOnly {
		t.Fatal("LocalOnly still set after materialization")
	}

	// Idempotent second pass tolerates the already-existing on-FUSE link.
	drainer.materializePendingSymlinks()
}

// TestPendingSymlinkPersistsAcrossRestart proves a symlink deferred while
// offline survives a process restart (so a reboot mid-offline-session doesn't
// lose it): the pending_symlinks row is durable in SQLite and a fresh
// SpoolStore over the same DB still sees it. Simulates the restart by reopening
// the DB rather than the whole handler.
func TestPendingSymlinkPersistsAcrossRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "spool.db")

	// Session 1: persist a pending symlink, then "crash" (close the DB).
	db1, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db1: %v", err)
	}
	if _, err := db1.Exec(`PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;`); err != nil {
		t.Fatalf("pragma db1: %v", err)
	}
	if err := metadata.InitSpoolSchema(db1); err != nil {
		t.Fatalf("schema db1: %v", err)
	}
	meta1 := metadata.NewSpoolStore(db1)
	if err := meta1.PutPendingSymlink("Frameworks/Lib", "Versions/Current/Lib"); err != nil {
		t.Fatalf("put pending: %v", err)
	}
	_ = db1.Close()

	// Session 2: reopen the SAME DB file — the row must still be there.
	db2, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })
	if err := metadata.InitSpoolSchema(db2); err != nil { // idempotent
		t.Fatalf("schema db2: %v", err)
	}
	meta2 := metadata.NewSpoolStore(db2)

	got, err := meta2.GetPendingSymlink("Frameworks/Lib")
	if err != nil {
		t.Fatalf("pending symlink lost across restart: %v", err)
	}
	if got != "Versions/Current/Lib" {
		t.Fatalf("restart target = %q, want %q", got, "Versions/Current/Lib")
	}

	// UPSERT semantics: re-putting the same link updates the target, no dup.
	if err := meta2.PutPendingSymlink("Frameworks/Lib", "Versions/B/Lib"); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	all, err := meta2.ListPendingSymlinks()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("upsert produced %d rows, want 1", len(all))
	}
	if all[0].Target != "Versions/B/Lib" {
		t.Fatalf("upsert target = %q, want updated %q", all[0].Target, "Versions/B/Lib")
	}
}

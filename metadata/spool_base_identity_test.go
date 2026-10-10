package metadata

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// preH8BSpoolEntries is spool_entries exactly as 0.4/0.5-pre-H-8B created it.
const preH8BSpoolEntries = `
CREATE TABLE spool_entries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    nfs_path        TEXT NOT NULL,
    spool_file      TEXT NOT NULL UNIQUE,
    size            INTEGER NOT NULL DEFAULT 0,
    sha256          BLOB,
    drain_state     TEXT NOT NULL CHECK(drain_state IN ('writing','ready','draining','done','failed')),
    drain_attempts  INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    suspect_zero_tail TEXT,
    punched_end     INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);`

// The base-identity migration is forward-safe on an existing database: an
// old row keeps every field, reads as base "unknown" / epoch 0 / not waiting,
// stays dispatchable, and re-running the migration is a no-op.
func TestSpoolBaseIdentityMigrationForwardSafe(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "old.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(preH8BSpoolEntries); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO spool_entries(nfs_path,spool_file,size,sha256,drain_state,drain_attempts,last_error,punched_end,created_at,updated_at)
	 VALUES('Film/ProjA/old.mov','/spool/old',7,x'01',?,2,'boom',0,100,101)`, DrainReady); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := InitSpoolSchema(db); err != nil {
			t.Fatalf("migration run %d: %v", i+1, err)
		}
	}
	s := NewSpoolStore(db)
	rows, err := s.ListReady(10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("old ready row not dispatchable after migration: %v %+v", err, rows)
	}
	r := rows[0]
	if r.NFSPath != "Film/ProjA/old.mov" || r.Size != 7 || r.DrainAttempts != 2 || r.LastError != "boom" {
		t.Fatalf("old row changed: %+v", r)
	}
	base, epoch, err := s.DrainBase(r.ID)
	if err != nil || base.Kind != SpoolBaseUnknown || epoch != 0 {
		t.Fatalf("old row base=%+v epoch=%d err=%v, want unknown/0", base, epoch, err)
	}
	if w, _ := s.WaitProject(r.ID); w != "" {
		t.Fatalf("old row waiting on %q", w)
	}
}

func TestSpoolBaseRoundTripAndProjectHold(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "new.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := InitSpoolSchema(db); err != nil {
		t.Fatal(err)
	}
	s := NewSpoolStore(db)
	cases := []SpoolBase{
		{},
		{Kind: SpoolBaseAbsent},
		{Kind: SpoolBaseWeak, MtimeNs: 1_700_000_000_000_000_000, Len: 99},
		{Kind: SpoolBasePresent, Ino: 4242, MtimeNs: 1_700_000_000_123_456_789, CtimeNs: 1_700_000_001_000_000_001, Len: 12},
		{Kind: SpoolBaseClaimed, Ino: 77},
	}
	for i, want := range cases {
		id, err := s.InsertWithBase(filepath.Join("Film/ProjA", string(rune('a'+i))+".mov"), filepath.Join("/spool", string(rune('a'+i))), want, uint64(i+3))
		if err != nil {
			t.Fatal(err)
		}
		got, epoch, err := s.DrainBase(id)
		if err != nil || got != want || epoch != uint64(i+3) {
			t.Errorf("case %d: got %+v epoch %d err %v, want %+v epoch %d", i, got, epoch, err, want, i+3)
		}
	}

	id, err := s.Insert("Film/ProjB/x.mov", "/spool/x")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkReady(id, 1, []byte{1}); err != nil {
		t.Fatal(err)
	}
	// Only a draining row can be held.
	if held, err := s.HoldForProject(id, "Film/ProjB", "busy"); err != nil || held {
		t.Fatalf("held a ready row: %v %v", held, err)
	}
	if ok, err := s.MarkDraining(id); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if err := s.ClaimDrainBase(id, 555); err != nil {
		t.Fatal(err)
	}
	if b, _, _ := s.DrainBase(id); b.Kind != SpoolBaseClaimed || b.Ino != 555 {
		t.Fatalf("claim not recorded: %+v", b)
	}
	if held, err := s.HoldForProject(id, "Film/ProjB", "waiting_project: busy"); err != nil || !held {
		t.Fatalf("hold: %v %v", held, err)
	}
	row, _ := s.Get(id)
	if row.DrainState != DrainReady || row.DrainAttempts != 0 {
		t.Fatalf("held row state=%s attempts=%d", row.DrainState, row.DrainAttempts)
	}
	if rows, _ := s.ListReady(10); len(rows) != 0 {
		t.Fatalf("held row listed as ready to drain: %+v", rows)
	}
	if w, _ := s.WaitingProjects(); w["Film/ProjB"] != 1 {
		t.Fatalf("waiting projects %+v", w)
	}
	// It stays a live row for reads, deletes and renames.
	if r, err := s.LookupByPath("Film/ProjB/x.mov"); err != nil || r.ID != id {
		t.Fatalf("held row invisible to LookupByPath: %v", err)
	}
	paths, err := s.ActivePaths()
	if err != nil {
		t.Fatal(err)
	}
	expectedPaths := map[string]bool{"Film/ProjB/x.mov": true}
	for i := range cases {
		expectedPaths[filepath.Join("Film/ProjA", string(rune('a'+i))+".mov")] = true
	}
	for _, p := range paths {
		if !expectedPaths[p] {
			t.Fatalf("unexpected or duplicate active path %q in %v", p, paths)
		}
		delete(expectedPaths, p)
	}
	if len(expectedPaths) != 0 {
		t.Fatalf("missing active paths %v; got %v", expectedPaths, paths)
	}
	// A rename re-evaluates the project: the wait is cleared, the base kept.
	if _, err := s.MigrateActivePathsWithBase("Film/ProjB/x.mov", "Film/ProjC/x.mov", nil); err != nil {
		t.Fatal(err)
	}
	if w, _ := s.WaitProject(id); w != "" {
		t.Fatalf("renamed row still waiting on %q", w)
	}
	if b, _, _ := s.DrainBase(id); b.Kind != SpoolBaseClaimed || b.Ino != 555 {
		t.Fatalf("rename without resolver changed the base: %+v", b)
	}
	if ok, err := s.MarkDraining(id); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if held, _ := s.HoldForProject(id, "Film/ProjC", "busy"); !held {
		t.Fatal("re-hold failed")
	}
	if n, err := s.ReleaseProjectHold("Film/ProjC"); err != nil || n != 1 {
		t.Fatalf("release: %d %v", n, err)
	}
	if rows, _ := s.ListReady(10); len(rows) != 1 || rows[0].ID != id {
		t.Fatalf("released row not dispatchable: %+v", rows)
	}
}

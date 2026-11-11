package metadata

import (
	"fmt"
	"path"
	"testing"
	"time"
)

// A large copy under folders that are not on the volume yet must not slow
// the listing that unrelated saves come from. The review measured the old
// SQL filter (a per-row subquery over pending_dirs) at 40.7 s for 2,000
// folders and 100,000 rows.
func TestListReadySkippingScalesWithPendingFolders(t *testing.T) {
	if testing.Short() {
		t.Skip("seeds 100,000 rows")
	}
	if raceEnabled {
		// Its time bounds measure SQLite and the scan, which the race
		// detector slows about 25 times; CI runs it without -race.
		t.Skip("timing test; run without -race")
	}
	const dirs, rows = 2000, 100000
	db := openTestDB(t)
	s := NewSpoolStore(db)
	pending := make(map[string]int64, dirs)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(-time.Hour).Unix()
	for d := 0; d < dirs; d++ {
		p := fmt.Sprintf("w/copy/dir%04d", d)
		res, err := tx.Exec(`INSERT INTO pending_dirs (dir_path, inode, created_at) VALUES (?, ?, ?)`, p, 5000+d, now)
		if err != nil {
			t.Fatal(err)
		}
		pending[p], _ = res.LastInsertId()
	}
	ins, err := tx.Prepare(`INSERT INTO spool_entries (nfs_path, spool_file, drain_state, created_at, updated_at) VALUES (?, ?, 'ready', ?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		p := fmt.Sprintf("w/copy/dir%04d/f%06d.dng", i%dirs, i)
		if _, err := ins.Exec(p, fmt.Sprintf("/spool/%d", i), now+int64(i/1000), now); err != nil {
			t.Fatal(err)
		}
	}
	ins.Close()
	if _, err := tx.Exec(`INSERT INTO spool_entries (nfs_path, spool_file, drain_state, created_at, updated_at) VALUES ('w/edit.prproj', '/spool/edit', 'ready', ?, ?)`, now+1000, now); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	skip := func(p string) (int64, bool) {
		for d := path.Dir(p); d != "." && d != "/"; d = path.Dir(d) {
			if id, ok := pending[d]; ok {
				return id, true
			}
		}
		return 0, false
	}
	start := time.Now()
	got, err := s.ListReadySkipping(8, true, -1, skip)
	took := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].NFSPath != "w/edit.prproj" {
		t.Fatalf("listed %d rows (%v), want only w/edit.prproj", len(got), got)
	}
	t.Logf("2,000 pending folders, 100,000 waiting rows, first listing: %v", took)
	// The first listing marks every waiting row once (about 2 s here);
	// the old per-row SQL filter took 40 s on this data.
	if took > 15*time.Second {
		t.Fatalf("first listing took %v, want under 15 s", took)
	}
	// Later listings (the dispatcher lists twice per pass) pass over the
	// marked rows by index.
	start = time.Now()
	for i := 0; i < 10; i++ {
		got, err = s.ListReadySkipping(8, true, -1, skip)
		if err != nil || len(got) != 1 {
			t.Fatalf("listing %d: %d rows, %v", i, len(got), err)
		}
	}
	per := time.Since(start) / 10
	t.Logf("later listings: %v each", per)
	if per > 250*time.Millisecond {
		t.Fatalf("later listing took %v, want under 250 ms", per)
	}
	// One folder reaches the volume: only its 50 rows are listed again.
	if err := s.ClearFolderWaitsFor(pending["w/copy/dir0000"]); err != nil {
		t.Fatal(err)
	}
	delete(pending, "w/copy/dir0000")
	start = time.Now()
	got, err = s.ListReadySkipping(8, true, -1, skip)
	if err != nil || len(got) != 8 || path.Dir(got[0].NFSPath) != "w/copy/dir0000" {
		t.Fatalf("after one folder landed: %d rows, %v", len(got), err)
	}
	if took := time.Since(start); took > 250*time.Millisecond {
		t.Fatalf("listing after one folder landed took %v", took)
	}
}

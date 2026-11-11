package metadata

import (
	"fmt"
	"testing"
	"time"
)

func seedFolderWaitRow(t *testing.T, s *SpoolStore, p string, size int64) int64 {
	t.Helper()
	res, err := s.db.Exec(`INSERT INTO spool_entries (nfs_path, spool_file, size, drain_state, created_at, updated_at) VALUES (?, ?, ?, 'ready', ?, ?)`,
		p, "/spool/"+p, size, time.Now().Unix(), time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return id
}

// The listing reads a file's pending folder from memory and writes the mark
// later. If the folder lands in between (its row deleted and its marks
// cleared), or the file moves out, the late mark must not be written: it
// would hide the file until a restart.
func TestReviewProbeStaleMarkAfterLanding(t *testing.T) {
	for _, tc := range []string{"folder landed", "file moved out"} {
		t.Run(tc, func(t *testing.T) {
			s := NewSpoolStore(openTestDB(t))
			folder, err := s.PutPendingDir("w/new", 5000)
			if err != nil {
				t.Fatal(err)
			}
			id := seedFolderWaitRow(t, s, "w/new/f.txt", 10)
			skip := func(p string) (int64, bool) {
				// The race window: the folder or the file changes after the
				// scan read the in-memory set, before the mark is written.
				switch tc {
				case "folder landed":
					if _, err := s.DeletePendingDir("w/new", folder); err != nil {
						t.Fatal(err)
					}
					if err := s.ClearFolderWaitsFor(folder); err != nil {
						t.Fatal(err)
					}
				case "file moved out":
					if _, err := s.db.Exec(`UPDATE spool_entries SET nfs_path='w/f.txt' WHERE id=?`, id); err != nil {
						t.Fatal(err)
					}
				}
				return folder, true
			}
			if _, err := s.ListReadySkipping(8, true, -1, skip); err != nil {
				t.Fatal(err)
			}
			got, err := s.ListReadySkipping(8, true, -1, func(string) (int64, bool) { return 0, false })
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || got[0].ID != id {
				t.Fatalf("listed %d rows after the race, want the file (a stale mark hid it)", len(got))
			}
		})
	}
}

// The media lane lists rows larger than the small-file limit. With a large
// backlog of small rows and any folder pending, it must not read every
// small row in full on each pass.
func TestListReadySkippingMediaLaneOverSmallBacklog(t *testing.T) {
	if testing.Short() {
		t.Skip("seeds 100,000 rows")
	}
	if raceEnabled {
		t.Skip("timing test; run without -race")
	}
	s := NewSpoolStore(openTestDB(t))
	folder, err := s.PutPendingDir("elsewhere/new", 5000)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	ins, err := tx.Prepare(`INSERT INTO spool_entries (nfs_path, spool_file, size, drain_state, created_at, updated_at) VALUES (?, ?, 4096, 'ready', ?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	for i := 0; i < 100000; i++ {
		if _, err := ins.Exec(fmt.Sprintf("w/copy/f%06d.xmp", i), fmt.Sprintf("/spool/%d", i), now, now); err != nil {
			t.Fatal(err)
		}
	}
	ins.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	skip := func(p string) (int64, bool) {
		if len(p) > 14 && p[:14] == "elsewhere/new/" {
			return folder, true
		}
		return 0, false
	}
	start := time.Now()
	got, err := s.ListReadySkipping(4, true, 1<<20, skip)
	took := time.Since(start)
	if err != nil || len(got) != 0 {
		t.Fatalf("media listing: %d rows, %v", len(got), err)
	}
	t.Logf("media listing over 100,000 small rows with a folder pending: %v", took)
	if took > time.Second {
		t.Fatalf("media listing took %v, want under 1 s (was 7.4 s)", took)
	}
}

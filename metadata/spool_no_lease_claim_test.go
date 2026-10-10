package metadata

import (
	"reflect"
	"testing"
	"time"
)

// The ordinary claim must leave a project wait intact. The explicit no-client
// claim may clear it, but only as part of the same guarded state transition.
func TestSpoolNoLeaseClaimClearsWaitAtomically(t *testing.T) {
	db := openTestDB(t)
	s := NewSpoolStore(db)
	id, err := s.Insert("Film/ProjA/card/A001.mov", "/spool/held")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkReady(id, 9, []byte("hash")); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.MarkDraining(id); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if ok, err := s.HoldForProject(id, "Film/ProjA", "prior lease client"); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if ok, err := s.MarkDraining(id); err != nil || ok {
		t.Fatalf("ordinary claim bypassed wait: %v %v", ok, err)
	}
	if wait, err := s.WaitProject(id); err != nil || wait != "Film/ProjA" {
		t.Fatalf("ordinary claim changed wait: %q %v", wait, err)
	}
	if ok, err := s.MarkDrainingWithoutProjectLease(id); err != nil || !ok {
		t.Fatalf("no-client claim: %v %v", ok, err)
	}
	row, err := s.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if row.DrainState != DrainDraining {
		t.Fatalf("state after claim: %s", row.DrainState)
	}
	if wait, err := s.WaitProject(id); err != nil || wait != "" {
		t.Fatalf("wait after claim: %q %v", wait, err)
	}
	if ok, err := s.MarkDrainingWithoutProjectLease(id); err != nil || ok {
		t.Fatalf("double claim: %v %v", ok, err)
	}
}

func TestSpoolNoLeaseClaimKeepsOrdinaryCustodyGuards(t *testing.T) {
	guards := []struct {
		name    string
		prepare func(*testing.T, *SpoolStore, int64)
	}{
		{"clone", func(t *testing.T, s *SpoolStore, id int64) {
			_, err := s.db.Exec(`UPDATE spool_entries SET clone_hold='copied' WHERE id=?`, id)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"qualification", func(t *testing.T, s *SpoolStore, id int64) {
			_, err := s.db.Exec(`UPDATE spool_entries SET qualification_hold='pending' WHERE id=?`, id)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"punched", func(t *testing.T, s *SpoolStore, id int64) {
			_, err := s.db.Exec(`UPDATE spool_entries SET punched_end=1 WHERE id=?`, id)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"retired", func(t *testing.T, s *SpoolStore, id int64) {
			_, err := s.db.Exec(`UPDATE spool_entries SET retired_by=999 WHERE id=?`, id)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"retry", func(t *testing.T, s *SpoolStore, id int64) {
			_, err := s.db.Exec(`UPDATE spool_entries SET retry_after_ns=? WHERE id=?`, time.Now().Add(time.Hour).UnixNano(), id)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"newer", func(t *testing.T, s *SpoolStore, id int64) {
			// Production Insert correctly refuses a same-name writer while the
			// accepted ready row owns the path. Construct the guarded database
			// state directly, as the replacement dispatch tests do.
			res, err := s.db.Exec(`INSERT INTO spool_entries(nfs_path,spool_file,drain_state,created_at,updated_at)
			 VALUES (?,?,'writing',0,0)`, "Film/ProjA/card/A001.mov", "/spool/newer")
			if err != nil {
				t.Fatal(err)
			}
			newID, err := res.LastInsertId()
			if err != nil || newID <= id {
				t.Fatalf("newer fixture id=%d older=%d err=%v", newID, id, err)
			}
		}},
		{"replacement", func(t *testing.T, s *SpoolStore, id int64) {
			if _, err := s.db.Exec(`UPDATE spool_entries SET replacement_witness=X'00' WHERE id=?`, id); err != nil {
				t.Fatal(err)
			}
		}},
		{"completion", func(t *testing.T, s *SpoolStore, id int64) {
			_, err := s.db.Exec(`INSERT INTO spool_completion_holds(completion_id,spool_id,nfs_path,spool_file,size) VALUES('held',?,?,?,?)`, id, "Film/ProjA/card/A001.mov", "/spool/held", 9)
			if err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, guard := range guards {
		t.Run(guard.name, func(t *testing.T) {
			db := openTestDB(t)
			s := NewSpoolStore(db)
			id, err := s.Insert("Film/ProjA/card/A001.mov", "/spool/held")
			if err != nil {
				t.Fatal(err)
			}
			if err := s.MarkReady(id, 9, []byte("hash")); err != nil {
				t.Fatal(err)
			}
			if ok, err := s.MarkDraining(id); err != nil || !ok {
				t.Fatal(ok, err)
			}
			if ok, err := s.HoldForProject(id, "Film/ProjA", "prior lease client"); err != nil || !ok {
				t.Fatal(ok, err)
			}
			guard.prepare(t, s, id)
			before, err := s.Get(id)
			if err != nil {
				t.Fatal(err)
			}
			if ok, _ := s.MarkDrainingWithoutProjectLease(id); ok {
				t.Fatal("no-client claim bypassed custody guard")
			}
			row, err := s.Get(id)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(row, before) {
				t.Fatalf("refused claim changed guarded row: before=%+v after=%+v", before, row)
			}
			if row.DrainState != DrainReady {
				t.Fatalf("state changed: %s", row.DrainState)
			}
			if wait, err := s.WaitProject(id); err != nil || wait != "Film/ProjA" {
				t.Fatalf("wait changed: %q %v", wait, err)
			}
		})
	}
}

package metadata

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newContinuationMetadataFixture(t *testing.T) (*Store, *SpoolStore, SpoolBirthReceipt) {
	return newContinuationMetadataFixtureAt(t, "bound.mov")
}

func newContinuationMetadataFixtureAt(t *testing.T, path string) (*Store, *SpoolStore, SpoolBirthReceipt) {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	sp := NewSpoolStore(s.DB())
	s.SetSpoolStore(sp)
	file := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	v, err := s.CaptureMirror(path)
	if err != nil {
		t.Fatal(err)
	}
	id, err := NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := s.InsertBoundSpoolBirth(v, MakeEntry(path, false, 0, time.Now(), 91), file, id)
	if err != nil {
		t.Fatal(err)
	}
	return s, sp, r
}

func TestBoundContinuationCASRejectsTupleAndGenerationDrift(t *testing.T) {
	for _, kind := range []string{"binding", "revision", "cache", "prepared", "postcommit"} {
		t.Run(kind, func(t *testing.T) {
			s, sp, r := newContinuationMetadataFixture(t)
			if err := sp.MarkReady(r.SpoolID, 0, nil); err != nil {
				t.Fatal(err)
			}
			c, err := sp.InspectSpoolContinuation(r.SpoolID, r.Path, r.SpoolFile)
			if err != nil || c.Legacy {
				t.Fatalf("capture=(%+v,%v)", c, err)
			}
			switch kind {
			case "binding":
				if _, err := s.DB().Exec(`UPDATE spool_bindings SET birth_identity='wrong' WHERE spool_id=?`, r.SpoolID); err != nil {
					t.Fatal(err)
				}
			case "revision":
				if _, err := s.DB().Exec(`UPDATE entries SET revision=revision+1 WHERE path=?`, r.Path); err != nil {
					t.Fatal(err)
				}
			case "cache":
				s.mu.Lock()
				e := MakeEntry(r.Path, false, 0, time.Now(), 92)
				e.Version = MirrorToken{Path: r.Path, Incarnation: "other", Revision: 1}
				s.pathCache[r.Path] = e
				s.mu.Unlock()
			case "prepared":
				s.mu.Lock()
				s.preparedCreates[r.Path] = PreparedCreate{Path: r.Path}
				s.mu.Unlock()
			case "postcommit":
				sp.continuationPostCommit = func() error { return errors.New("lost return") }
			}
			admitted := false
			ok, err := sp.ReopenBoundReady(c, func() { admitted = true })
			if ok || admitted {
				t.Fatalf("drift %s admitted=(%v,%v)", kind, ok, err)
			}
			row, readErr := sp.Get(r.SpoolID)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if kind == "postcommit" {
				var pending *PendingSpoolContinuation
				if !errors.As(err, &pending) || row.DrainState != DrainWriting {
					t.Fatalf("uncertain=(%v,%+v)", err, row)
				}
			} else if !errors.Is(err, ErrSpoolContinuationConflict) || row.DrainState != DrainReady {
				t.Fatalf("precommit refusal=(%v,%+v)", err, row)
			}
		})
	}
}

func TestBirthMarkedMigrationRefusesWholeCohort(t *testing.T) {
	for _, state := range []DrainState{DrainWriting, DrainReady, DrainDraining} {
		t.Run(string(state), func(t *testing.T) {
			s, sp, r := newContinuationMetadataFixtureAt(t, "dir/bound.mov")
			if err := s.Insert(MakeEntry("dir/sibling.mov", false, 0, time.Now(), 92)); err != nil {
				t.Fatal(err)
			}
			legacyID, err := sp.Insert("dir/sibling.mov", filepath.Join(t.TempDir(), "legacy"))
			if err != nil {
				t.Fatal(err)
			}
			if state != DrainWriting {
				if err := sp.MarkReady(r.SpoolID, 0, nil); err != nil {
					t.Fatal(err)
				}
			}
			if state == DrainDraining {
				if ok, err := sp.MarkDraining(r.SpoolID); err != nil || !ok {
					t.Fatalf("claim=(%v,%v)", ok, err)
				}
			}
			if _, err := sp.MigrateActivePaths("dir", "moved"); err == nil {
				t.Fatal("birth cohort migrated")
			}
			row, err := sp.Get(r.SpoolID)
			if err != nil || row.NFSPath != r.Path || row.DrainState != state {
				t.Fatalf("birth row=(%+v,%v)", row, err)
			}
			legacy, err := sp.Get(legacyID)
			if err != nil || legacy.NFSPath != "dir/sibling.mov" {
				t.Fatalf("sibling moved=(%+v,%v)", legacy, err)
			}
		})
	}
}

func TestContinuationLegacyMeansNoBinding(t *testing.T) {
	s, sp, r := newContinuationMetadataFixture(t)
	if err := sp.MarkReady(r.SpoolID, 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`UPDATE spool_bindings SET birth_id='',birth_path='',birth_file='',birth_identity='' WHERE spool_id=?`, r.SpoolID); err != nil {
		t.Fatal(err)
	}
	if _, err := sp.InspectSpoolContinuation(r.SpoolID, r.Path, r.SpoolFile); !errors.Is(err, ErrSpoolContinuationConflict) {
		t.Fatalf("empty present binding classified legacy: %v", err)
	}
	if _, err := s.DB().Exec(`DELETE FROM spool_bindings WHERE spool_id=?`, r.SpoolID); err != nil {
		t.Fatal(err)
	}
	c, err := sp.InspectSpoolContinuation(r.SpoolID, r.Path, r.SpoolFile)
	if err != nil || !c.Legacy {
		t.Fatalf("absent binding=(%+v,%v)", c, err)
	}
	if ok, err := sp.ReopenReady(r.SpoolID); err != nil || !ok {
		t.Fatalf("legacy CAS=(%v,%v)", ok, err)
	}
}

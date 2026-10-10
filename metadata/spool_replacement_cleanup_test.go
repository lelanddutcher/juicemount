package metadata

import (
	"errors"
	"testing"
)

func settledCleanupMetadataFixture(t *testing.T) (*replacementFixture, *SpoolRow, SpoolReplacementPublication) {
	t.Helper()
	f, row, committed, gate, captured, proof, graph := committedSettlementFixture(t)
	settled, err := gate.SettleSpoolReplacement(committed, captured, proof, proof, graph, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	return f, row, *settled
}

func TestReplacementCleanupCurrentClaimHistoricalFinish(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		name := "before-claim"
		if claimed {
			name = "after-claim"
		}
		t.Run(name, func(t *testing.T) {
			f, row, p := settledCleanupMetadataFixture(t)
			if claimed {
				next, err := f.store.BeginReplacementCleanup(p, "1:2", "1:3", func() error { return nil })
				if err != nil {
					t.Fatal(err)
				}
				p = *next
			}
			// Model a later scan/rename; no current path or eligibility survives.
			if _, err := f.store.DB().Exec(`DELETE FROM entries WHERE path=?`, p.Expected.Path); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.DB().Exec(`DELETE FROM cache_path_eligibility WHERE path=?`, p.Expected.Path); err != nil {
				t.Fatal(err)
			}
			if !claimed {
				if _, err := f.store.BeginReplacementCleanup(p, "1:2", "1:3", func() error { return nil }); !errors.Is(err, ErrMirrorConflict) {
					t.Fatalf("stale claim: %v", err)
				}
			} else {
				next, err := f.store.FinishReplacementCleanup(p, func() error { return nil })
				if err != nil || next == nil || next.Phase != ReplacementPublicationCleaned {
					t.Fatalf("historical finish: %+v %v", next, err)
				}
				if got, _, _, err := readReplacementPublication(f.store.DB(), row.ID); err != nil || got == nil || *got != *next {
					t.Fatalf("history: %+v %v", got, err)
				}
			}
		})
	}
}

func TestReplacementCleanupSQLAndVerifierFailures(t *testing.T) {
	for _, phase := range []string{ReplacementPublicationCleaning, ReplacementPublicationCleaned} {
		for _, failure := range []string{"abort", "ignore", "rewind", "precommit", "postcommit"} {
			t.Run(phase+"/"+failure, func(t *testing.T) {
				f, row, p := settledCleanupMetadataFixture(t)
				if phase == ReplacementPublicationCleaned {
					next, err := f.store.BeginReplacementCleanup(p, "1:2", "1:3", func() error { return nil })
					if err != nil {
						t.Fatal(err)
					}
					p = *next
				}
				trigger := ""
				switch failure {
				case "abort":
					trigger = `CREATE TRIGGER cleanup_fault BEFORE UPDATE OF phase ON spool_replacement_publications WHEN NEW.phase='` + phase + `' BEGIN SELECT RAISE(ABORT,'cleanup fault'); END`
				case "ignore":
					trigger = `CREATE TRIGGER cleanup_fault BEFORE UPDATE OF phase ON spool_replacement_publications WHEN NEW.phase='` + phase + `' BEGIN SELECT RAISE(IGNORE); END`
				case "rewind":
					trigger = `CREATE TRIGGER cleanup_fault AFTER UPDATE OF phase ON spool_replacement_publications WHEN NEW.phase='` + phase + `' BEGIN UPDATE spool_replacement_publications SET phase=OLD.phase,payload=OLD.payload WHERE spool_id=NEW.spool_id; END`
				}
				if trigger != "" {
					if _, err := f.store.DB().Exec(trigger); err != nil {
						t.Fatal(err)
					}
				}
				calls := 0
				verify := func() error {
					calls++
					if failure == "precommit" && calls == 2 || failure == "postcommit" && calls == 3 {
						return errors.New("injected verification failure")
					}
					return nil
				}
				var err error
				if phase == ReplacementPublicationCleaning {
					_, err = f.store.BeginReplacementCleanup(p, "1:2", "1:3", verify)
				} else {
					_, err = f.store.FinishReplacementCleanup(p, verify)
				}
				if err == nil {
					t.Fatal("failure admitted")
				}
				got, _, _, readErr := readReplacementPublication(f.store.DB(), row.ID)
				if readErr != nil || got == nil {
					t.Fatalf("readback: %+v %v", got, readErr)
				}
				if failure == "postcommit" {
					var pending *PendingSpoolReplacement
					if !errors.As(err, &pending) || got.Phase != phase || calls != 3 {
						t.Fatalf("uncertainty lost: %+v %v checks=%d", got, err, calls)
					}
				} else if *got != p {
					t.Fatalf("failed transition changed journal: %+v", got)
				}
			})
		}
	}
}

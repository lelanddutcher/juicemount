//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

func settledCleanupFixture(t *testing.T) (*boundCacheFixture, *SpoolStore, *metadata.SpoolRow) {
	t.Helper()
	x, s, row := publishedSettlementSDKFixture(t, []byte("exact private backup for a published save"))
	if err := x.jfs.handler.settleReplacement(context.Background(), row.ID); err != nil {
		t.Fatal(err)
	}
	row, err := s.meta.Get(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	return x, s, row
}

func cleanupPhase(t *testing.T, s *SpoolStore, id int64) string {
	t.Helper()
	row, err := s.meta.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.meta.ReplacementPublication(row)
	if err != nil || p == nil {
		t.Fatalf("journal: %+v %v", p, err)
	}
	return p.Phase
}

func TestReplacementCleanupInterruptionReopen(t *testing.T) {
	for _, phase := range []string{"before-intent", "after-intent", "before-unlink", "after-unlink", "before-sync", "after-sync", "before-finish", "after-finish", "closed"} {
		t.Run(phase, func(t *testing.T) {
			x, s, row := settledCleanupFixture(t)
			before, _ := s.Capacity()
			hit := false
			s.replacementCleanupFault = func(at string) error {
				if at == phase {
					hit = true
					return errors.New("interrupted " + phase)
				}
				return nil
			}
			if err := x.jfs.handler.cleanupReplacement(context.Background(), row.ID); err == nil || !hit {
				t.Fatalf("fault not reached: hit=%v err=%v", hit, err)
			}
			if after, _ := s.Capacity(); after != before {
				t.Fatalf("uncertain cleanup refunded %d -> %d", before, after)
			}
			want := metadata.ReplacementPublicationCleaning
			if phase == "before-intent" {
				want = metadata.ReplacementPublicationSettled
			}
			if phase == "after-finish" || phase == "closed" {
				want = metadata.ReplacementPublicationCleaned
			}
			if got := cleanupPhase(t, s, row.ID); got != want {
				t.Fatalf("phase %s != %s", got, want)
			}
			s.replacementCleanupFault = nil
			if phase == "closed" {
				if err := x.jfs.handler.cleanupReplacement(context.Background(), row.ID); err == nil {
					t.Fatal("uncertain close refunded on retry")
				}
			}
			_, recovered := reopenReplacementPublicationFixture(t, x, s.root)
			if _, err := recovered.RecoverOnBoot(context.Background()); err != nil {
				t.Fatal(err)
			}
			expectedUsed := int64(0)
			if phase == "before-intent" {
				expectedUsed = before
			} else {
				want = metadata.ReplacementPublicationCleaned
			}
			if got := cleanupPhase(t, recovered, row.ID); got != want {
				t.Fatalf("recovered phase %s != %s", got, want)
			}
			if after, _ := recovered.Capacity(); after != expectedUsed {
				t.Fatalf("recovered capacity %d != %d", after, expectedUsed)
			}
			if phase != "before-intent" {
				if _, err := os.Lstat(row.SpoolFile); !os.IsNotExist(err) {
					t.Fatalf("source remained: %v", err)
				}
			}
		})
	}
}

func TestReplacementCleanupSubstitutionRefusesDeletion(t *testing.T) {
	for _, target := range []string{"leaf", "files", "root"} {
		t.Run(target, func(t *testing.T) {
			x, s, row := settledCleanupFixture(t)
			body, err := os.ReadFile(row.SpoolFile)
			if err != nil {
				t.Fatal(err)
			}
			before, _ := s.Capacity()
			saved := row.SpoolFile + ".saved"
			s.replacementCleanupFault = func(phase string) error {
				if phase != "before-unlink" {
					return nil
				}
				switch target {
				case "leaf":
					if err := os.Rename(row.SpoolFile, saved); err != nil {
						return err
					}
				case "files":
					old := filepath.Join(s.root, SpoolFilesSubdir)
					if err := os.Rename(old, old+".saved"); err != nil {
						return err
					}
					if err := os.Mkdir(old, 0700); err != nil {
						return err
					}
					saved = filepath.Join(old+".saved", filepath.Base(row.SpoolFile))
				case "root":
					if err := os.Rename(s.root, s.root+".saved"); err != nil {
						return err
					}
					if err := os.MkdirAll(filepath.Join(s.root, SpoolFilesSubdir), 0700); err != nil {
						return err
					}
					saved = filepath.Join(s.root+".saved", SpoolFilesSubdir, filepath.Base(row.SpoolFile))
				}
				return os.WriteFile(row.SpoolFile, []byte("unexpected replacement"), 0600)
			}
			if err := x.jfs.handler.cleanupReplacement(context.Background(), row.ID); err == nil {
				t.Fatal("substitution admitted")
			}
			s.replacementCleanupFault = nil
			if got, err := os.ReadFile(row.SpoolFile); err != nil || string(got) != "unexpected replacement" {
				t.Fatalf("unexpected file changed: %v", err)
			}
			if got, err := os.ReadFile(saved); err != nil || !bytes.Equal(got, body) {
				t.Fatalf("retained original changed: %v", err)
			}
			if after, _ := s.Capacity(); after != before {
				t.Fatal("substitution refunded capacity")
			}
			_, recovered := reopenReplacementPublicationFixture(t, x, s.root)
			recovered.used.Store(777) // failed boot must not reach its counter reset
			if _, err := recovered.RecoverOnBoot(context.Background()); err == nil {
				t.Fatal("boot adopted substituted custody")
			}
			if got, _ := recovered.Capacity(); got != 777 {
				t.Fatal("failed boot reset capacity")
			}
		})
	}
}

func TestReplacementCleanupSQLFailureRecovery(t *testing.T) {
	for _, phase := range []string{"cleaning", "cleaned"} {
		t.Run(phase, func(t *testing.T) {
			x, s, row := settledCleanupFixture(t)
			before, _ := s.Capacity()
			if _, err := x.store.DB().Exec(`CREATE TRIGGER cleanup_runtime_fault BEFORE UPDATE OF phase ON spool_replacement_publications WHEN NEW.phase='` + phase + `' BEGIN SELECT RAISE(ABORT,'cleanup runtime fault'); END`); err != nil {
				t.Fatal(err)
			}
			if err := x.jfs.handler.cleanupReplacement(context.Background(), row.ID); err == nil {
				t.Fatal("SQL failure admitted")
			}
			if after, _ := s.Capacity(); after != before {
				t.Fatal("SQL failure refunded")
			}
			if _, err := x.store.DB().Exec(`DROP TRIGGER cleanup_runtime_fault`); err != nil {
				t.Fatal(err)
			}
			if err := x.jfs.handler.cleanupReplacement(context.Background(), row.ID); err != nil {
				t.Fatal(err)
			}
			if after, _ := s.Capacity(); after != before-row.Size {
				t.Fatal("retry did not refund exactly once")
			}
		})
	}
}

func TestReplacementCleanupRefundPreservesSuccessorAndHistoricalFile(t *testing.T) {
	x, s, row := settledCleanupFixture(t)
	before, _ := s.Capacity()
	s.replacementCleanupFault = func(phase string) error {
		if phase == "after-finish" {
			return errors.New("lost completed cleanup acknowledgement")
		}
		return nil
	}
	if err := x.jfs.handler.cleanupReplacement(context.Background(), row.ID); err == nil {
		t.Fatal("fault not reached")
	}
	s.replacementCleanupFault = nil
	successor, err := s.OpenWrite(row.NFSPath)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("newer save must keep its reservation and index")
	if n, err := successor.WriteAt(body, 0); err != nil || n != len(body) {
		t.Fatalf("successor write %d %v", n, err)
	}
	if err := successor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(row.SpoolFile, []byte("unrelated historical name"), 0600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := x.jfs.handler.cleanupReplacement(context.Background(), row.ID); err != nil {
			t.Fatal(err)
		}
		if after, _ := s.Capacity(); after != before-row.Size+int64(len(body)) {
			t.Fatalf("refund consumed successor: %d", after)
		}
		if entry, ok := s.index.Lookup(row.NFSPath); !ok || entry != successor {
			t.Fatal("cleanup removed successor index")
		}
		if got, err := os.ReadFile(row.SpoolFile); err != nil || string(got) != "unrelated historical name" {
			t.Fatalf("historical file touched: %v", err)
		}
	}
	_, recovered := reopenReplacementPublicationFixture(t, x, s.root)
	if _, err := recovered.RecoverOnBoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if after, _ := recovered.Capacity(); after != int64(len(body)) {
		t.Fatalf("restart successor capacity %d", after)
	}
	if got, err := os.ReadFile(row.SpoolFile); err != nil || string(got) != "unrelated historical name" {
		t.Fatalf("recovery touched historical file: %v", err)
	}
}

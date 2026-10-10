package metadata

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type boundCompletionFixture struct {
	s    *Store
	sp   *SpoolStore
	f    *os.File
	item BoundDrainCommitItem
}

func newBoundCompletionStore(t *testing.T) (*Store, *SpoolStore) {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	sp := NewSpoolStore(s.DB())
	s.SetSpoolStore(sp)
	return s, sp
}

func bornDrainingFixture(t *testing.T, s *Store, sp *SpoolStore, path string, initial, final int64) boundCompletionFixture {
	t.Helper()
	file := filepath.Join(t.TempDir(), "private")
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if err := f.Truncate(initial); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	expected, err := s.CaptureMirror(path)
	if err != nil {
		t.Fatal(err)
	}
	nonce, err := NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	e := MakeEntry(path, false, initial, time.Unix(1700000000, 0), uint64(100+len(path)))
	e.Mode = 0444
	r, born, err := s.InsertBoundSpoolBirth(expected, e, file, nonce)
	if err != nil || born == nil {
		t.Fatalf("birth=(%+v,%+v,%v)", r, born, err)
	}
	if err := f.Truncate(final); err != nil {
		t.Fatal(err)
	}
	if final > 0 {
		body := bytes.Repeat([]byte{byte(final%251 + 1)}, int(final))
		if n, err := f.WriteAt(body, 0); err != nil || int64(n) != final {
			t.Fatalf("image=(%d,%v)", n, err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	body := make([]byte, final)
	if _, err := f.ReadAt(body, 0); err != nil && final > 0 {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	if err := sp.MarkReady(r.SpoolID, final, digest[:]); err != nil {
		t.Fatal(err)
	}
	if ok, err := sp.MarkDraining(r.SpoolID); err != nil || !ok {
		t.Fatalf("claim=(%v,%v)", ok, err)
	}
	current, err := s.CaptureMirror(path)
	if err != nil {
		t.Fatal(err)
	}
	item := BoundDrainCommitItem{SpoolID: r.SpoolID, BirthID: r.BirthID, Path: path, SpoolFile: file,
		FileIdentity: r.FileIdentity, BackingIdentity: "backing-dev:inode", Source: f, FullSize: final,
		Expected: current, Mtime: time.Unix(1800000000, 987654321)}
	return boundCompletionFixture{s, sp, f, item}
}

func TestBoundCompletionExactImageAndTypedReceipt(t *testing.T) {
	for _, size := range []int64{0, 128, 8192} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			s, sp := newBoundCompletionStore(t)
			f := bornDrainingFixture(t, s, sp, "film.mov", 4096, size)
			before := s.LookupByPath(f.item.Path)
			r, err := s.CompleteBoundSpool(f.item)
			if err != nil || r.CompletionID == "" || r.Published.Incarnation != before.Version.Incarnation || r.Published.Revision != before.Version.Revision+1 || r.OriginalInode != before.Inode || r.OriginalMode != before.Mode || r.FullSize != size {
				t.Fatalf("receipt=(%+v,%v) before=%+v", r, err, before)
			}
			got := s.LookupByPath(f.item.Path)
			if got == nil || got.Size != size || got.Version != r.Published || got.Mtime.Unix() != f.item.Mtime.Unix() || got.MtimeNsecKnown {
				t.Fatalf("RAM=%+v", got)
			}
			var durableSize, revision int64
			if err := s.DB().QueryRow(`SELECT size,revision FROM entries WHERE path=?`, f.item.Path).Scan(&durableSize, &revision); err != nil || durableSize != size || revision != r.Published.Revision {
				t.Fatalf("durable=(%d,%d,%v)", durableSize, revision, err)
			}
			row, err := sp.Get(f.item.SpoolID)
			if err != nil || row.DrainState != DrainDone {
				t.Fatalf("row=(%+v,%v)", row, err)
			}
			if err := s.ValidateBoundDrainCompletion(r); err != nil {
				t.Fatalf("validate=%v", err)
			}
			legacy := DrainCommitItem{SpoolID: r.SpoolID, NFSPath: r.Path, SpoolFile: r.SpoolFile, Size: r.FullSize}
			forged := DrainCommitResult{SpoolID: r.SpoolID, Done: true, CompletionID: r.CompletionID}
			if err := sp.ValidateDrainCompletion(legacy, forged); err == nil {
				t.Fatal("legacy validator adopted bound hold")
			}
			if err := sp.CleanupDrainCompletion(legacy, forged, func() error { t.Fatal("legacy cleanup ran"); return nil }); err == nil {
				t.Fatal("legacy cleanup adopted bound hold")
			}
			if _, err := os.Stat(f.item.SpoolFile); err != nil {
				t.Fatalf("source disposed: %v", err)
			}
		})
	}
}

func TestBoundCompletionRejectsStaleCohortBeforeHold(t *testing.T) {
	for _, kind := range []string{"birth", "phase", "identity", "revision", "cache", "prepared", "canceled", "requeued", "missing", "digest", "source", "body"} {
		t.Run(kind, func(t *testing.T) {
			s, sp := newBoundCompletionStore(t)
			f := bornDrainingFixture(t, s, sp, "a.mov", 4096, 128)
			keep := bornDrainingFixture(t, s, sp, "b.mov", 4096, 256)
			before := s.LookupByPath("b.mov").Version
			var err error
			switch kind {
			case "birth":
				f.item.BirthID = "ffffffffffffffffffffffffffffffff"
			case "phase":
				_, err = s.DB().Exec(`UPDATE spool_bindings SET phase='publishing' WHERE spool_id=?`, f.item.SpoolID)
			case "identity":
				_, err = s.DB().Exec(`UPDATE spool_bindings SET birth_identity='other' WHERE spool_id=?`, f.item.SpoolID)
			case "revision":
				_, err = s.DB().Exec(`UPDATE entries SET revision=revision+1 WHERE path='a.mov'`)
			case "cache":
				s.mu.Lock()
				e := MakeEntry("a.mov", false, 999, time.Now(), 900)
				e.Version = MirrorToken{Path: "a.mov", Incarnation: "other", Revision: 1}
				s.pathCache["a.mov"] = e
				s.mu.Unlock()
			case "prepared":
				s.mu.Lock()
				s.preparedCreates["a.mov"] = PreparedCreate{Path: "a.mov"}
				s.mu.Unlock()
			case "canceled":
				_, err = sp.DeleteActiveByPath("a.mov")
			case "requeued":
				err = sp.ResetToReady(f.item.SpoolID)
			case "missing":
				_, err = s.DB().Exec(`DELETE FROM spool_entries WHERE id=?`, f.item.SpoolID)
			case "digest":
				_, err = s.DB().Exec(`UPDATE spool_entries SET sha256=x'00' WHERE id=?`, f.item.SpoolID)
			case "source":
				f.item.FileIdentity = "other"
			case "body":
				_, err = f.f.WriteAt([]byte("different"), 0)
			}
			if err != nil {
				t.Fatal(err)
			}
			if r, err := s.BatchCompleteBoundSpools([]BoundDrainCommitItem{f.item, keep.item}); err == nil || len(r) != 0 {
				t.Fatalf("stale cohort=(%+v,%v)", r, err)
			}
			if got := s.LookupByPath("b.mov"); got == nil || got.Version != before || got.Size != 4096 {
				t.Fatalf("accepted sibling changed: %+v", got)
			}
			row, err := sp.Get(keep.item.SpoolID)
			if err != nil || row.DrainState != DrainDraining {
				t.Fatalf("sibling row=(%+v,%v)", row, err)
			}
			var holds int
			if err := s.DB().QueryRow(`SELECT count(*) FROM spool_completion_holds`).Scan(&holds); err != nil || holds != 0 {
				t.Fatalf("holds=(%d,%v)", holds, err)
			}
		})
	}
}

func TestBoundCompletionBatchOrderDuplicatesAndUncertainHolds(t *testing.T) {
	t.Run("second_transaction_sql_error", func(t *testing.T) {
		s, sp := newBoundCompletionStore(t)
		f := bornDrainingFixture(t, s, sp, "sql-error.mov", 4096, 0)
		before := s.LookupByPath(f.item.Path).Version
		if _, err := s.DB().Exec(`CREATE TRIGGER refuse_bound_done BEFORE UPDATE ON entries BEGIN SELECT RAISE(ABORT, 'injected bound completion failure'); END`); err != nil {
			t.Fatal(err)
		}
		_, err := s.CompleteBoundSpool(f.item)
		var pending *PendingBoundDrainCompletion
		if !errors.As(err, &pending) {
			t.Fatalf("missing typed pending: %v", err)
		}
		row, err := sp.Get(f.item.SpoolID)
		if err != nil || row.DrainState != DrainDraining {
			t.Fatalf("row escaped rollback=(%+v,%v)", row, err)
		}
		if got := s.LookupByPath(f.item.Path); got == nil || got.Version != before || got.Size != 4096 {
			t.Fatalf("mirror escaped rollback: %+v", got)
		}
		var kind string
		if err := s.DB().QueryRow(`SELECT kind FROM spool_completion_holds WHERE spool_id=?`, f.item.SpoolID).Scan(&kind); err != nil || kind != "bound" {
			t.Fatalf("FULL hold=(%q,%v)", kind, err)
		}
		if err := sp.CheckDrainCompletionRecovery(); err == nil {
			t.Fatal("startup crossed held SQL error")
		}
		if err := s.BeginSpoolPublish(row, f.item.Expected, filepath.Join(t.TempDir(), "stage")); err == nil {
			t.Fatal("old publication protocol crossed bound FULL hold")
		}
		if _, err := os.Stat(f.item.SpoolFile); err != nil {
			t.Fatalf("source lost: %v", err)
		}
	})
	t.Run("reordered", func(t *testing.T) {
		s, sp := newBoundCompletionStore(t)
		a := bornDrainingFixture(t, s, sp, "a.mov", 4096, 0)
		b := bornDrainingFixture(t, s, sp, "b.mov", 4096, 100)
		r, err := s.BatchCompleteBoundSpools([]BoundDrainCommitItem{b.item, a.item})
		if err != nil || len(r) != 2 || r[0].SpoolID != b.item.SpoolID || r[1].SpoolID != a.item.SpoolID {
			t.Fatalf("order=(%+v,%v)", r, err)
		}
	})
	t.Run("duplicate", func(t *testing.T) {
		s, sp := newBoundCompletionStore(t)
		a := bornDrainingFixture(t, s, sp, "a.mov", 4096, 0)
		if _, err := s.BatchCompleteBoundSpools([]BoundDrainCommitItem{a.item, a.item}); err == nil {
			t.Fatal("duplicate accepted")
		}
		var holds int
		_ = s.DB().QueryRow(`SELECT count(*) FROM spool_completion_holds`).Scan(&holds)
		if holds != 0 {
			t.Fatal("duplicate left hold")
		}
	})
	for _, phase := range []string{"hold", "done"} {
		t.Run(phase+"_lost_return", func(t *testing.T) {
			s, sp := newBoundCompletionStore(t)
			a := bornDrainingFixture(t, s, sp, "a.mov", 4096, 0)
			var postHold, postDone func() error
			if phase == "hold" {
				postHold = func() error { return errors.New("lost FULL hold return") }
			} else {
				postDone = func() error { return errors.New("lost done return") }
			}
			r, err := s.completeBoundSpools([]BoundDrainCommitItem{a.item}, postHold, postDone)
			var pending *PendingBoundDrainCompletion
			if len(r) != 0 || !errors.As(err, &pending) {
				t.Fatalf("uncertain=(%+v,%v)", r, err)
			}
			var kind string
			var published int64
			if err := s.DB().QueryRow(`SELECT kind,published_revision FROM spool_completion_holds WHERE spool_id=?`, a.item.SpoolID).Scan(&kind, &published); err != nil || kind != "bound" {
				t.Fatalf("hold=(%q,%d,%v)", kind, published, err)
			}
			row, err := sp.Get(a.item.SpoolID)
			if err != nil {
				t.Fatal(err)
			}
			if phase == "hold" && (row.DrainState != DrainDraining || published != 0) {
				t.Fatalf("hold barrier row=%+v revision=%d", row, published)
			}
			if phase == "done" && (row.DrainState != DrainDone || published != a.item.Expected.Revision+1) {
				t.Fatalf("done uncertainty row=%+v revision=%d", row, published)
			}
			if err := sp.CheckDrainCompletionRecovery(); err == nil {
				t.Fatal("startup did not fence retained hold")
			}
			var seq int
			var label, dbPath string
			if err := s.DB().QueryRow(`PRAGMA database_list`).Scan(&seq, &label, &dbPath); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if err := NewSpoolStore(reopened.DB()).CheckDrainCompletionRecovery(); err == nil {
				t.Fatal("fresh database reopen crossed retained FULL hold")
			}
			if _, err := os.Stat(a.item.SpoolFile); err != nil {
				t.Fatalf("uncertain source lost: %v", err)
			}
		})
	}
}

func TestBoundCompletionValidatorRefusesLaterGeneration(t *testing.T) {
	for _, kind := range []string{"token", "cache", "prepared", "durable_revision", "birth_path", "birth_file", "incarnation"} {
		t.Run(kind, func(t *testing.T) {
			s, sp := newBoundCompletionStore(t)
			f := bornDrainingFixture(t, s, sp, "later.mov", 4096, 128)
			r, err := s.CompleteBoundSpool(f.item)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "token":
				r.Published.StoreID = "forged"
			case "cache":
				s.mu.Lock()
				e := MakeEntry(r.Path, false, 5, time.Now(), 777)
				e.Version = MirrorToken{Path: r.Path, Incarnation: "successor", Revision: 1}
				s.pathCache[r.Path] = e
				s.mu.Unlock()
			case "prepared":
				s.mu.Lock()
				s.preparedCreates[r.Path] = PreparedCreate{Path: r.Path}
				s.mu.Unlock()
			case "durable_revision":
				_, err = s.DB().Exec(`UPDATE entries SET revision=revision+1 WHERE path=?`, r.Path)
				if err != nil {
					t.Fatal(err)
				}
			case "birth_path":
				_, err = s.DB().Exec(`UPDATE spool_bindings SET birth_path='elsewhere.mov' WHERE spool_id=?`, r.SpoolID)
				if err != nil {
					t.Fatal(err)
				}
			case "birth_file":
				_, err = s.DB().Exec(`UPDATE spool_bindings SET birth_file='/other/source' WHERE spool_id=?`, r.SpoolID)
				if err != nil {
					t.Fatal(err)
				}
			case "incarnation":
				_, err = s.DB().Exec(`UPDATE spool_bindings SET incarnation='other' WHERE spool_id=?`, r.SpoolID)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := s.ValidateBoundDrainCompletion(r); !errors.Is(err, ErrMirrorConflict) {
				t.Fatalf("%s accepted: %v", kind, err)
			}
		})
	}
}

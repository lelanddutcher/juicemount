package metadata

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func dispatchIDs(t *testing.T, s *SpoolStore, after, through int64, limit int) []int64 {
	t.Helper()
	ids, err := s.ReplacementDispatchPage(context.Background(), after, through, limit)
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func dispatchHighWater(t *testing.T, s *SpoolStore) int64 {
	t.Helper()
	id, err := s.ReplacementDispatchHighWater(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestReplacementDispatchQueuedCandidateKeepsOrdinaryFence(t *testing.T) {
	f, row, queued := acceptedPublicationFixture(t)
	ctx := context.Background()
	through := dispatchHighWater(t, f.spool)
	if got := dispatchIDs(t, f.spool, 0, through, 64); !reflect.DeepEqual(got, []int64{row.ID}) {
		t.Fatalf("queued page=%v", got)
	}
	if got, err := f.spool.ReplacementDispatchCandidate(ctx, row.ID); err != nil || got == nil || *got != *queued {
		t.Fatalf("queued candidate=%+v err=%v", got, err)
	}
	if ready, err := f.spool.ListReady(10); err != nil || len(ready) != 0 {
		t.Fatalf("ordinary ready=%v err=%v", ready, err)
	}
	if ok, err := f.spool.MarkDraining(row.ID); ok || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("ordinary claim=%t err=%v", ok, err)
	}
	for _, args := range [][3]int64{{-1, through, 1}, {through + 1, through, 1}, {0, through, 0}, {0, through, 65}} {
		if got, err := f.spool.ReplacementDispatchPage(ctx, args[0], args[1], int(args[2])); got != nil || !errors.Is(err, ErrMirrorConflict) {
			t.Fatalf("invalid page %v returned %v, %v", args, got, err)
		}
	}
}

func TestReplacementDispatchReadySchedulingHolds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*testing.T, *replacementFixture, *SpoolRow)
	}{
		{"project-wait", func(t *testing.T, f *replacementFixture, row *SpoolRow) {
			dispatchSQL(t, f, `UPDATE spool_entries SET wait_project='a' WHERE id=?`, row.ID)
		}},
		{"retry-delay", func(t *testing.T, f *replacementFixture, row *SpoolRow) {
			dispatchSQL(t, f, `UPDATE spool_entries SET retry_after_ns=? WHERE id=?`, time.Now().Add(time.Hour).UnixNano(), row.ID)
		}},
		{"newer-writing", func(t *testing.T, f *replacementFixture, row *SpoolRow) {
			// A fresh ordinary writer cannot be inserted through the public
			// path while an accepted READY replacement owns this name.
			dispatchSQL(t, f, `INSERT INTO spool_entries(nfs_path,spool_file,drain_state,created_at,updated_at)
			 VALUES (?,?,'writing',0,0)`, row.NFSPath, t.TempDir()+"/newer")
		}},
		{"clone-hold", func(t *testing.T, f *replacementFixture, row *SpoolRow) {
			dispatchSQL(t, f, `UPDATE spool_entries SET clone_hold='peer' WHERE id=?`, row.ID)
		}},
		{"qualification-hold", func(t *testing.T, f *replacementFixture, row *SpoolRow) {
			dispatchSQL(t, f, `UPDATE spool_entries SET qualification_hold='review' WHERE id=?`, row.ID)
		}},
		{"punched", func(t *testing.T, f *replacementFixture, row *SpoolRow) {
			dispatchSQL(t, f, `UPDATE spool_entries SET punched_end=1 WHERE id=?`, row.ID)
		}},
		{"retired", func(t *testing.T, f *replacementFixture, row *SpoolRow) {
			dispatchRawRetire(t, f, row.ID)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, row, _ := acceptedPublicationFixture(t)
			tc.change(t, f, row)
			if got := dispatchIDs(t, f.spool, 0, dispatchHighWater(t, f.spool), 64); len(got) != 0 {
				t.Fatalf("held row scheduled: %v", got)
			}
			if got, err := f.spool.ReplacementDispatchCandidate(context.Background(), row.ID); got != nil || !errors.Is(err, ErrMirrorConflict) {
				t.Fatalf("held row admitted: %+v %v", got, err)
			}
		})
	}
}

func dispatchSQL(t *testing.T, f *replacementFixture, query string, args ...any) {
	t.Helper()
	if _, err := f.store.DB().Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func dispatchRawRetire(t *testing.T, f *replacementFixture, id int64) {
	t.Helper()
	// Deliberate corruption bypasses the public retirement guard solely to
	// prove the scheduling predicate cannot surface this row.
	dispatchSQL(t, f, `UPDATE spool_entries SET replacement_publication_required=0 WHERE id=?`, id)
	dispatchSQL(t, f, `UPDATE spool_entries SET retired_by=999 WHERE id=?`, id)
	dispatchSQL(t, f, `UPDATE spool_entries SET replacement_publication_required=1 WHERE id=?`, id)
}

func TestReplacementDispatchMalformedHintsCannotAuthorize(t *testing.T) {
	for _, tc := range []struct{ name, query string }{
		{"witness", `UPDATE spool_entries SET replacement_witness=X'00' WHERE id=?`},
		{"journal", `UPDATE spool_replacement_publications SET payload=X'00' WHERE spool_id=?`},
		{"phase-mismatch", `UPDATE spool_replacement_publications SET phase='intent' WHERE spool_id=?`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, row, _ := acceptedPublicationFixture(t)
			dispatchSQL(t, f, tc.query, row.ID)
			if got := dispatchIDs(t, f.spool, 0, dispatchHighWater(t, f.spool), 1); !reflect.DeepEqual(got, []int64{row.ID}) {
				t.Fatalf("malformed hint page=%v", got)
			}
			if got, err := f.spool.ReplacementDispatchCandidate(context.Background(), row.ID); got != nil || !errors.Is(err, ErrMirrorConflict) {
				t.Fatalf("malformed hint admitted: %+v %v", got, err)
			}
		})
	}
}

func TestReplacementDispatchKeysetSkipsMalformedHeadAndFreezesHighWater(t *testing.T) {
	f, row, _ := acceptedPublicationFixture(t)
	// Turn the earlier inert history row into a malformed scheduling hint.
	// Its ID is below the valid accepted row; canonical admission must refuse
	// it without preventing the next keyset page from reaching the valid row.
	head := f.history[0]
	dispatchSQL(t, f, `UPDATE spool_entries SET drain_state='ready',retired_by=0,replacement_witness=X'00',replacement_publication_required=1 WHERE id=?`, head)
	dispatchSQL(t, f, `INSERT INTO spool_replacement_publications(spool_id,operation_id,phase,payload) VALUES (?,'malformed-head','queued',X'00')`, head)
	through := dispatchHighWater(t, f.spool)
	if got := dispatchIDs(t, f.spool, 0, through, 1); !reflect.DeepEqual(got, []int64{head}) {
		t.Fatalf("first page=%v", got)
	}
	if got, err := f.spool.ReplacementDispatchCandidate(context.Background(), head); got != nil || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("malformed head admitted: %+v %v", got, err)
	}
	if got := dispatchIDs(t, f.spool, head, through, 1); !reflect.DeepEqual(got, []int64{row.ID}) {
		t.Fatalf("valid later page=%v", got)
	}
	late, err := f.spool.Insert("later/new.mov", t.TempDir()+"/later")
	if err != nil {
		t.Fatal(err)
	}
	dispatchSQL(t, f, `UPDATE spool_entries SET drain_state='ready',replacement_witness=X'00',replacement_publication_required=1 WHERE id=?`, late)
	dispatchSQL(t, f, `INSERT INTO spool_replacement_publications(spool_id,operation_id,phase,payload) VALUES (?,'malformed-late','queued',X'00')`, late)
	if got := dispatchIDs(t, f.spool, row.ID, through, 64); len(got) != 0 {
		t.Fatalf("post-high-water row entered frozen pass: %v", got)
	}
	if got := dispatchIDs(t, f.spool, row.ID, dispatchHighWater(t, f.spool), 64); !reflect.DeepEqual(got, []int64{late}) {
		t.Fatalf("next pass missed later hint: %v", got)
	}
}

func TestReplacementDispatchLaterAcceptanceOfLowerIDNeedsNewPass(t *testing.T) {
	f := newReplacementFixture(t, false)
	if _, err := f.spool.Insert("unrelated/new.mov", t.TempDir()+"/later-id"); err != nil {
		t.Fatal(err)
	}
	through := dispatchHighWater(t, f.spool)
	if got := dispatchIDs(t, f.spool, 0, through, 64); len(got) != 0 {
		t.Fatalf("writing source scheduled: %v", got)
	}
	if _, err := f.store.AcceptSpoolReplacement(f.w, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := dispatchIDs(t, f.spool, 0, through, 64); !reflect.DeepEqual(got, []int64{f.w.SourceRowID}) {
		t.Fatalf("new pass missed lower-ID acceptance: %v", got)
	}
}

func TestReplacementDispatchDoneCleanupIgnoresReadyWaits(t *testing.T) {
	for _, phase := range []string{ReplacementPublicationSettled, ReplacementPublicationCleaning} {
		t.Run(phase, func(t *testing.T) {
			f, row, settled := settledCleanupMetadataFixture(t)
			p := settled
			if phase == ReplacementPublicationCleaning {
				next, err := f.store.BeginReplacementCleanup(settled, "1:2", "1:3", func() error { return nil })
				if err != nil {
					t.Fatal(err)
				}
				p = *next
			}
			dispatchSQL(t, f, `UPDATE spool_entries SET wait_project='a',retry_after_ns=? WHERE id=?`, time.Now().Add(time.Hour).UnixNano(), row.ID)
			if _, err := f.spool.Insert(row.NFSPath, t.TempDir()+"/newer"); err != nil {
				t.Fatal(err)
			}
			if got := dispatchIDs(t, f.spool, 0, dispatchHighWater(t, f.spool), 64); !reflect.DeepEqual(got, []int64{row.ID}) {
				t.Fatalf("done cleanup page=%v", got)
			}
			if got, err := f.spool.ReplacementDispatchCandidate(context.Background(), row.ID); err != nil || got == nil || *got != p {
				t.Fatalf("done cleanup=%+v err=%v", got, err)
			}
		})
	}
}

func TestReplacementDispatchTerminalAndCommonHolds(t *testing.T) {
	for _, hold := range []string{"clone", "qualification", "punched", "retired"} {
		t.Run(hold, func(t *testing.T) {
			f, row, _ := settledCleanupMetadataFixture(t)
			switch hold {
			case "clone":
				dispatchSQL(t, f, `UPDATE spool_entries SET clone_hold='peer' WHERE id=?`, row.ID)
			case "qualification":
				dispatchSQL(t, f, `UPDATE spool_entries SET qualification_hold='review' WHERE id=?`, row.ID)
			case "punched":
				dispatchSQL(t, f, `UPDATE spool_entries SET punched_end=1 WHERE id=?`, row.ID)
			case "retired":
				dispatchRawRetire(t, f, row.ID)
			}
			if got := dispatchIDs(t, f.spool, 0, dispatchHighWater(t, f.spool), 64); len(got) != 0 {
				t.Fatalf("held DONE row scheduled: %v", got)
			}
			if got, err := f.spool.ReplacementDispatchCandidate(context.Background(), row.ID); got != nil || !errors.Is(err, ErrMirrorConflict) {
				t.Fatalf("held DONE row admitted: %+v %v", got, err)
			}
		})
	}
	f, row, settled := settledCleanupMetadataFixture(t)
	cleaning, err := f.store.BeginReplacementCleanup(settled, "1:2", "1:3", func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.FinishReplacementCleanup(*cleaning, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := dispatchIDs(t, f.spool, 0, dispatchHighWater(t, f.spool), 64); len(got) != 0 {
		t.Fatalf("cleaned history scheduled: %v", got)
	}
	if got, err := f.spool.ReplacementDispatchCandidate(context.Background(), row.ID); got != nil || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("cleaned history admitted: %+v %v", got, err)
	}
	t.Run("superseded", func(t *testing.T) {
		f := newReplacementFixture(t, false)
		next := successorReplacement(t, f)
		if _, err := f.store.AcceptSpoolReplacement(next, func() error { return nil }); err != nil {
			t.Fatal(err)
		}
		if got := dispatchIDs(t, f.spool, 0, dispatchHighWater(t, f.spool), 64); !reflect.DeepEqual(got, []int64{next.SourceRowID}) {
			t.Fatalf("superseded predecessor scheduled: %v", got)
		}
		if got, err := f.spool.ReplacementDispatchCandidate(context.Background(), next.PredecessorRowID); got != nil || !errors.Is(err, ErrMirrorConflict) {
			t.Fatalf("superseded predecessor admitted: %+v %v", got, err)
		}
	})
}

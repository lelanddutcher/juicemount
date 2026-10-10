package mutationauthority

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func stageFixture(t *testing.T, j *Journal) (Request, StageIntent) {
	t.Helper()
	l, err := j.Claim(context.Background(), "volume", "session", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"kind":"fixture-media","payload":"body"}`)
	r := Request{Lease: l, ID: "stage", Digest: DigestBytes(body), Kind: MediaPublishOperation, FileID: "incarnation", ExpectedRevision: "parent@3,file@5"}
	return r, StageIntent{Body: body, Path: "/.juicemount-stage-" + DigestBytes([]byte(r.Volume+"\x00"+r.ID)), Backend: strings.Repeat("a", 64), Size: 4, SHA256: DigestBytes([]byte("body"))}
}

func TestMediaStageIntentAndPreparedAreAtomic(t *testing.T) {
	j, _ := testJournal(t)
	r, intent := stageFixture(t, j)
	if _, err := j.db.Exec(`CREATE TRIGGER stage_failure BEFORE INSERT ON media_stages BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
		t.Fatal(err)
	}
	calls := 0
	err := j.ExecuteStage(context.Background(), r, intent, backendFunc(func(context.Context, Request) (DurabilityProof, error) { calls++; return DurabilityProof{}, nil }))
	if err == nil || calls != 0 {
		t.Fatal("backend entered despite failed intent", err, calls)
	}
	if _, err := j.Inspect(context.Background(), r.Volume, r.ID); !errors.Is(err, ErrNoSuchOperation) {
		t.Fatal("partial prepared transaction survived", err)
	}
	if _, err := j.db.Exec(`DROP TRIGGER stage_failure`); err != nil {
		t.Fatal(err)
	}
}

func TestMediaPublishCannotBypassStageWithGenericProof(t *testing.T) {
	j, _ := testJournal(t)
	r, _ := stageFixture(t, j)
	calls := 0
	_, err := j.Execute(context.Background(), r, backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		calls++
		return DurabilityProof{Revision: "unqualified", Evidence: "generic"}, nil
	}))
	if !errors.Is(err, ErrInvalid) || calls != 0 {
		t.Fatal("generic media callback admitted", err, calls)
	}
	if _, err := j.Inspect(context.Background(), r.Volume, r.ID); !errors.Is(err, ErrNoSuchOperation) {
		t.Fatal("generic media operation persisted", err)
	}
}

func TestMediaStageClosureRejectsLateAttemptsAndNeverIssuesReceipt(t *testing.T) {
	j, path := testJournal(t)
	r, intent := stageFixture(t, j)
	ctx := context.Background()
	var checkpoint MediaStage
	err := j.ExecuteStage(ctx, r, intent, backendFunc(func(ctx context.Context, r Request) (DurabilityProof, error) {
		s, err := j.InspectStage(ctx, r.Volume, r.ID)
		if err != nil || !sameStageIntent(s.Intent, intent) {
			t.Fatal("intent missing before effect", err)
		}
		if err := j.BindStageInode(ctx, r, 9, r.Digest); err != nil {
			t.Fatal(err)
		}
		if err := j.RecordStageSlice(ctx, r, 9, 11); err != nil {
			t.Fatal(err)
		}
		if err := j.RecordSliceWritten(ctx, r, StageSlice{ID: 11, Inode: 9, Size: 4, Length: 4}); err != nil {
			t.Fatal(err)
		}
		a := ObjectAttempt{Slice: 11, Provider: "file", Bucket: "/fixture/", Key: "fixture/chunks/0/0/11_0_4", Size: 4, SHA256: intent.SHA256}
		id, err := j.BeginObjectAttempt(ctx, r, a)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := j.CloseStage(ctx, r, func(MediaStage) error { return nil }); !errors.Is(err, ErrUncertain) {
			t.Fatal("unsettled attempt closed", err)
		}
		if err := j.VerifyObjectAttempt(ctx, r, id); err != nil {
			t.Fatal(err)
		}
		checkpoint, err = j.CloseStage(ctx, r, func(s MediaStage) error {
			if len(s.Objects) != 1 || len(s.Slices) != 1 {
				return ErrInvalid
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		// This models an SDK callback scheduled earlier but only now entering the
		// raw gate. It must be denied before any provider effect can occur.
		if _, err := j.BeginObjectAttempt(ctx, r, a); !errors.Is(err, ErrUncertain) {
			t.Fatal("late attempt reopened closed admission", err)
		}
		if err := j.RecordStageSlice(ctx, r, 9, 12); !errors.Is(err, ErrUncertain) {
			t.Fatal("late allocation admitted", err)
		}
		return DurabilityProof{Revision: "must-not-commit", Evidence: "must-not-receipt"}, nil
	}))
	if !errors.Is(err, ErrUncertain) || !checkpoint.Closed || checkpoint.Manifest == "" {
		t.Fatal(err, checkpoint)
	}
	op, err := j.Inspect(ctx, r.Volume, r.ID)
	if err != nil || op.State != Prepared || op.Receipt != nil {
		t.Fatal("stage became publication", op, err)
	}
	calls := 0
	backend := backendFunc(func(context.Context, Request) (DurabilityProof, error) { calls++; return DurabilityProof{}, nil })
	if err := j.ExecuteStage(ctx, r, intent, backend); !errors.Is(err, ErrUncertain) || calls != 0 {
		t.Fatal("stage replayed", err, calls)
	}
	changed := intent
	changed.Backend = strings.Repeat("b", 64)
	if err := j.ExecuteStage(ctx, r, changed, backend); !errors.Is(err, ErrChangedRequest) {
		t.Fatal("changed backend adopted", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.InspectStage(ctx, r.Volume, r.ID)
	if err != nil || got.Manifest != checkpoint.Manifest || !got.Closed || got.Inode != 9 {
		t.Fatal("manifest changed on reopen", err)
	}
	if _, err := reopened.Claim(ctx, r.Volume, "successor", time.Minute); !errors.Is(err, ErrUnsettled) {
		t.Fatal("stage authorized succession", err)
	}
}

func TestVersionOneJournalMigratesWithoutAdoptingMedia(t *testing.T) {
	path, db := legacyFixture(t)
	for _, stmt := range []string{"ALTER TABLE operations ADD COLUMN " + kindColumn, "ALTER TABLE operations ADD COLUMN " + fileColumn, `PRAGMA user_version=1`} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	j, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	var version int
	if err := j.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != journalSchemaVersion {
		t.Fatal(version, err)
	}
	var stages int
	if err := j.db.QueryRow(`SELECT COUNT(*) FROM media_stages`).Scan(&stages); err != nil || stages != 0 {
		t.Fatal("legacy effects adopted", stages, err)
	}
	if _, err := j.Claim(context.Background(), "volume", "successor", time.Minute); !errors.Is(err, ErrUnsettled) {
		t.Fatal(err)
	}
}

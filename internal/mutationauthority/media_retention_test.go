package mutationauthority

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func retainedFixture(t *testing.T, count int) (*Journal, string, BackendProcess, []MediaStage) {
	t.Helper()
	j, path := testJournal(t)
	r, intent := stageFixture(t, j)
	ctx := context.Background()
	for i := 0; i < count; i++ {
		r.ID = string(rune('a' + i))
		intent.Path = "/.juicemount-stage-" + DigestBytes([]byte(r.Volume+"\x00"+r.ID))
		if err := j.ExecuteStage(ctx, r, intent, backendFunc(func(context.Context, Request) (DurabilityProof, error) { return DurabilityProof{}, nil })); !errors.Is(err, ErrUncertain) {
			t.Fatal(err)
		}
	}
	run := strings.Repeat("b", 40)
	if _, err := j.db.Exec(`INSERT INTO backend_processes VALUES(1,'volume',?,?)`, "jmgw_"+strings.Repeat("a", 64), run); err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec(`INSERT INTO backend_configuration VALUES(1,'volume',?)`, intent.Backend); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	// Unit fixture for the journal primitive only. Actual process fencing is
	// exercised by SDK acceptance; no production unpin capability is added.
	p := BackendProcess{2, "volume", "jmgw_" + strings.Repeat("c", 64), run}
	if _, err := j.db.Exec(`INSERT INTO backend_processes VALUES(?,?,?,?)`, p.Generation, p.Volume, p.Username, p.RedisRunID); err != nil {
		t.Fatal(err)
	}
	j.processPinned, j.backendStarted, j.backendRecorded, j.backendProcess = true, true, true, p
	t.Cleanup(func() { j.processPinned = false; j.Close() })
	stages, err := j.StageRecoveryCandidates(ctx, "volume")
	if err != nil || len(stages) != count {
		t.Fatal(err, len(stages))
	}
	return j, path, p, stages
}

func TestMediaRetentionRequiresCurrentFenceAndReopenBlocks(t *testing.T) {
	j, path, p, stages := retainedFixture(t, 1)
	ctx := context.Background()
	if _, err := j.Claim(ctx, "volume", "successor", time.Minute); !errors.Is(err, ErrUnsettled) {
		t.Fatal(err)
	}
	if err := j.CertifyRetainedStages(ctx, p, stages[0].Intent.Backend, stages, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal("record alone was treated as fence", err)
	}
	checks := 0
	if err := j.CertifyRetainedStages(ctx, p, stages[0].Intent.Backend, stages, func() error { checks++; return nil }); err != nil || checks != 2 {
		t.Fatal(err, checks)
	}
	l, err := j.Claim(ctx, "volume", "successor", time.Minute)
	if err != nil || l.Epoch != stages[0].Request.Epoch+1 {
		t.Fatal(l, err)
	}
	op, err := j.Inspect(ctx, "volume", stages[0].Request.ID)
	if err != nil || op.State != Prepared || op.Receipt != nil {
		t.Fatal("retention changed uncertain outcome", err)
	}
	if err := j.RequireRetainedStage(ctx, op.Request); err != nil {
		t.Fatal(err)
	}
	j.processPinned = false
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Claim(ctx, "volume", "third", time.Minute); !errors.Is(err, ErrUnsettled) {
		t.Fatal("persisted certificate admitted without new fence", err)
	}
	if err := reopened.RequireRetainedStage(ctx, op.Request); err == nil {
		t.Fatal("persisted certificate was trusted on reopen")
	}
}

func TestMediaRetentionAtomicAndExact(t *testing.T) {
	for _, mode := range []string{"snapshot", "binding", "generation", "run", "missing-stage", "fence", "sql-second-row", "no-prior-worker", "changed-prior-run"} {
		t.Run(mode, func(t *testing.T) {
			j, _, p, stages := retainedFixture(t, 2)
			binding := stages[0].Intent.Backend
			check := func() error { return nil }
			switch mode {
			case "snapshot":
				stages[0].Intent.Size++
			case "binding":
				binding = strings.Repeat("d", 64)
			case "generation":
				p.Generation++
			case "run":
				p.RedisRunID = strings.Repeat("d", 40)
			case "missing-stage":
				stages = stages[:1]
			case "fence":
				check = func() error { return ErrUncertain }
			case "sql-second-row":
				if _, err := j.db.Exec(`CREATE TRIGGER retention_failure BEFORE INSERT ON media_retention WHEN NEW.id='b' BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
					t.Fatal(err)
				}
			case "no-prior-worker":
				if _, err := j.db.Exec(`DELETE FROM backend_processes WHERE generation=1`); err != nil {
					t.Fatal(err)
				}
			case "changed-prior-run":
				if _, err := j.db.Exec(`UPDATE backend_processes SET redis_run_id=? WHERE generation=1`, strings.Repeat("d", 40)); err != nil {
					t.Fatal(err)
				}
			}
			if err := j.CertifyRetainedStages(context.Background(), p, binding, stages, check); err == nil {
				t.Fatal("invalid certificate accepted")
			}
			var count int
			if err := j.db.QueryRow(`SELECT COUNT(*) FROM media_retention`).Scan(&count); err != nil || count != 0 || j.retainedGeneration != 0 {
				t.Fatal("partial certificate enabled", count, err)
			}
			if _, err := j.Claim(context.Background(), "volume", "successor", time.Minute); !errors.Is(err, ErrUnsettled) {
				t.Fatal(err)
			}
		})
	}
}

func TestMediaRetentionRejectsSameProcessStageAndMissingManifest(t *testing.T) {
	j, _ := testJournal(t)
	r, intent := stageFixture(t, j)
	if err := j.ExecuteStage(context.Background(), r, intent, backendFunc(func(context.Context, Request) (DurabilityProof, error) { return DurabilityProof{}, nil })); !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
	delete(j.deadlines, r.Volume)
	if _, err := j.db.Exec(`UPDATE owners SET admission_closed=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := j.StageRecoveryCandidates(context.Background(), r.Volume); err == nil {
		t.Fatal("current-process stage accepted for restart recovery")
	}
	j.initialOperationRowID = 100 // independently exercise a missing persisted stage
	if _, err := j.db.Exec(`DELETE FROM media_stages`); err != nil {
		t.Fatal(err)
	}
	if _, err := j.StageRecoveryCandidates(context.Background(), r.Volume); err == nil {
		t.Fatal("unsupported media row silently filtered")
	}
}

func TestMediaRetentionSchema2Upgrade(t *testing.T) {
	j, path := testJournal(t)
	if _, err := j.db.Exec(`DROP TABLE media_publication_retention; DROP TABLE media_publications; DROP TABLE media_retention; PRAGMA user_version=2`); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	var version int
	if err := next.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != journalSchemaVersion {
		t.Fatal(version, err)
	}
}

func TestMediaRetentionRejectsStageAttachedToCustody(t *testing.T) {
	j, _, _, _ := retainedFixture(t, 1)
	if _, err := j.db.Exec(`UPDATE operations SET kind='custody-v1',file_id=''`); err != nil {
		t.Fatal(err)
	}
	if _, err := j.StageRecoveryCandidates(context.Background(), "volume"); !errors.Is(err, ErrInvalid) {
		t.Fatal("stage hidden behind custody kind", err)
	}
}

func TestMediaRetentionDoesNotExemptNewUncertainWork(t *testing.T) {
	j, _, p, stages := retainedFixture(t, 1)
	ctx := context.Background()
	if err := j.CertifyRetainedStages(ctx, p, stages[0].Intent.Backend, stages, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	l, err := j.Claim(ctx, "volume", "successor", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	r := stages[0].Request
	r.Lease, r.ID = l, "new-stage"
	intent := stages[0].Intent
	intent.Path = "/.juicemount-stage-" + DigestBytes([]byte(r.Volume+"\x00"+r.ID))
	if err := j.ExecuteStage(ctx, r, intent, backendFunc(func(context.Context, Request) (DurabilityProof, error) { return DurabilityProof{}, nil })); !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
	delete(j.deadlines, r.Volume)
	if _, err := j.db.Exec(`UPDATE owners SET admission_closed=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Claim(ctx, "volume", "third", time.Minute); !errors.Is(err, ErrUnsettled) {
		t.Fatal("old retention exempted new work", err)
	}
}

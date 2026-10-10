package mutationauthority

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func publicationFixture(t *testing.T, count int) (*Journal, string, BackendProcess, []MediaStage) {
	t.Helper()
	j, path, p, stages := retainedFixture(t, count)
	for i, s := range stages {
		if _, err := j.db.Exec(`UPDATE media_stages SET inode=?,marker=?,closed=1,manifest=? WHERE volume=? AND id=?`, i+2, strings.Repeat("e", 64), strings.Repeat("f", 64), s.Request.Volume, s.Request.ID); err != nil {
			t.Fatal(err)
		}
	}
	stages, err := j.StageRecoveryCandidates(context.Background(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	return j, path, p, stages
}

func TestPublicationIntentRevokesPrivateExemption(t *testing.T) {
	j, _, p, stages := publicationFixture(t, 1)
	ctx := context.Background()
	s := stages[0]
	if err := j.CertifyRetainedStages(ctx, p, s.Intent.Backend, stages, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	lease, err := j.Claim(ctx, "volume", "publisher", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	got, err := j.BeginPublication(ctx, lease, s.Request)
	if err != nil || StageSnapshotDigest(got) != StageSnapshotDigest(s) {
		t.Fatal(err)
	}
	pending, err := j.InspectPublication(ctx, "volume", s.Request.ID)
	if err != nil || pending == nil || pending.Observed || pending.Snapshot != StageSnapshotDigest(s) {
		t.Fatal(pending, err)
	}
	if err := j.RequireRecoveredStage(ctx, s.Request); err == nil {
		t.Fatal("private certificate survived publication intent")
	}
	if _, err := j.BeginPublication(ctx, lease, s.Request); !errors.Is(err, ErrUncertain) {
		t.Fatal("publication replay admitted", err)
	}
	if err := j.ObservePublication(ctx, s); err != nil {
		t.Fatal(err)
	}
	if err := j.RequireRecoveredStage(ctx, s.Request); err == nil {
		t.Fatal("observation manufactured recovery certificate")
	}
	delete(j.deadlines, "volume")
	if _, err := j.db.Exec(`UPDATE owners SET admission_closed=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Claim(ctx, "volume", "successor", time.Minute); !errors.Is(err, ErrUnsettled) {
		t.Fatal("observation settled uncertain operation", err)
	}
}

func TestPublicationRecoveryCertificatesAtomicAndCurrent(t *testing.T) {
	for _, mode := range []string{"success", "sql-failure", "changed-snapshot", "observed-to-private"} {
		t.Run(mode, func(t *testing.T) {
			j, path, p, stages := publicationFixture(t, 2)
			ctx := context.Background()
			s := stages[1]
			snapshot := StageSnapshotDigest(s)
			observed := 0
			if mode == "changed-snapshot" {
				snapshot = strings.Repeat("0", 64)
			}
			if mode == "observed-to-private" {
				observed = 1
			}
			if _, err := j.db.Exec(`INSERT INTO media_publications VALUES(?,?,?,?)`, s.Request.Volume, s.Request.ID, snapshot, observed); err != nil {
				t.Fatal(err)
			}
			if mode == "sql-failure" {
				if _, err := j.db.Exec(`CREATE TRIGGER fail_public BEFORE INSERT ON media_publication_retention BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
					t.Fatal(err)
				}
			}
			published := map[string]bool{s.Request.ID: true}
			if mode == "observed-to-private" {
				published = nil
			}
			err := j.CertifyRecoveredStages(ctx, p, s.Intent.Backend, stages, published, func() error { return nil })
			if mode != "success" {
				if err == nil {
					t.Fatal("invalid certification accepted")
				}
				var count int
				if err := j.db.QueryRow(`SELECT (SELECT COUNT(*) FROM media_retention)+(SELECT COUNT(*) FROM media_publication_retention)`).Scan(&count); err != nil || count != 0 || j.retainedGeneration != 0 {
					t.Fatal("partial certificate survived", count, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range stages {
				if err := j.RequireRecoveredStage(ctx, s.Request); err != nil {
					t.Fatal(err)
				}
			}
			if err := j.RequireRetainedStage(ctx, s.Request); err == nil {
				t.Fatal("published certificate accepted as private")
			}
			if _, err := j.Claim(ctx, "volume", "successor", time.Minute); err != nil {
				t.Fatal(err)
			}
			op, err := j.Inspect(ctx, "volume", s.Request.ID)
			if err != nil || op.State != Prepared || op.Receipt != nil {
				t.Fatal("recovery issued receipt", err)
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
			if _, err := reopened.Claim(ctx, "volume", "next", time.Minute); !errors.Is(err, ErrUnsettled) {
				t.Fatal("persisted certificate bypassed fresh fence", err)
			}
		})
	}
}

func TestPublicationSchema3Upgrade(t *testing.T) {
	j, path := testJournal(t)
	if _, err := j.db.Exec(`DROP TABLE media_publication_retention; DROP TABLE media_publications; PRAGMA user_version=3`); err != nil {
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

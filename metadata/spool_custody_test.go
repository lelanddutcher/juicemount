package metadata

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

const (
	custodyOperationID = "11111111111111111111111111111111"
	custodySourceID    = "22222222222222222222222222222222"
)

func custodyFixture(t *testing.T, size int64) (*Store, *SpoolStore, *SpoolRow, MirrorToken, SpoolCustody) {
	t.Helper()
	s, sp, row, v := boundSpoolFixture(t, size)
	c, err := s.BeginSpoolCustody(row, v, custodyOperationID, custodySourceID,
		"source-dev=2,inode=3", filepath.Join(t.TempDir(), "stage-"+custodyOperationID))
	if err != nil {
		t.Fatal(err)
	}
	return s, sp, row, v, c
}

func TestSpoolCustodyPublicationAndExactCleanup(t *testing.T) {
	s, _, row, v, c := custodyFixture(t, 4096)
	var pending *PendingSpoolCustody
	if err := s.CheckSpoolRecovery(); !errors.As(err, &pending) || len(pending.Operations) != 1 {
		t.Fatalf("stage intent not fenced: %v", err)
	}
	if c.Phase != "allocating" || c.Epoch != 1 || c.OperationID != custodyOperationID || c.SourceID != custodySourceID {
		t.Fatalf("bad stage intent: %+v", c)
	}
	old := c
	var err error
	c, err = s.BindSpoolStage(c, c.SourceIdentity, "stage-dev=2,inode=4")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.BindSpoolStage(old, old.SourceIdentity, "stage-dev=2,inode=5"); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("stale stage identity took custody: %v", err)
	}
	c, err = s.PrepareSpoolCheckpoint(c, c.SourceIdentity, c.StageIdentity, 4096, 4096, 4096, 0)
	if err != nil || c.Phase != "checkpoint_pending" {
		t.Fatalf("checkpoint intent %+v %v", c, err)
	}
	c, err = s.ConfirmSpoolCheckpoint(c, c.SourceIdentity, c.StageIdentity)
	if err != nil || c.Phase != "staged" {
		t.Fatalf("checkpoint confirm %+v %v", c, err)
	}
	c, err = s.BeginCustodyPublish(c, v, c.SourceIdentity, c.StageIdentity)
	if err != nil || c.Phase != "publish_pending" {
		t.Fatalf("publish intent %+v %v", c, err)
	}
	if _, err := s.ClaimSpoolCleanup(c, v, "public-dev=5,inode=6", "source", c.SourceIdentity); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("cleanup permitted before publication: %v", err)
	}
	if err := s.BeginSpoolPublish(row, v, c.StagePath); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteSpool(row, v, time.Now()); err != nil {
		t.Fatal(err)
	}
	published, err := s.CaptureMirror(v.Path)
	if err != nil {
		t.Fatal(err)
	}
	c, err = s.ConfirmCustodyPublished(c, published, "public-dev=5,inode=6")
	if err != nil || c.Phase != "published" || c.PublishedRevision != published.Revision {
		t.Fatalf("published %+v %v", c, err)
	}
	if err := s.CheckSpoolRecovery(); err != nil {
		t.Fatalf("published custody still blocks: %v", err)
	}
	c, err = s.ClaimSpoolCleanup(c, published, c.PublicIdentity, "stage", c.StageIdentity)
	if err != nil || !c.StageCleanupClaimed || c.SourceCleanupClaimed {
		t.Fatalf("stage claim %+v %v", c, err)
	}
	stale := c
	c, err = s.ClaimSpoolCleanup(c, published, c.PublicIdentity, "source", c.SourceIdentity)
	if err != nil || !c.SourceCleanupClaimed {
		t.Fatalf("source claim %+v %v", c, err)
	}
	if _, err := s.ClaimSpoolCleanup(stale, published, stale.PublicIdentity, "source", stale.SourceIdentity); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("stale cleanup replay accepted: %v", err)
	}
	got, err := s.SpoolCustodyForSource(c.SourceID)
	if err != nil || got != c {
		t.Fatalf("source lookup %+v %v", got, err)
	}
}

func TestSpoolCustodySQLIgnoredAndFailedTransitions(t *testing.T) {
	for _, fault := range []string{"RAISE(IGNORE)", "RAISE(FAIL,'injected')"} {
		t.Run(fault, func(t *testing.T) {
			s, _, row, v := boundSpoolFixture(t, 4096)
			if _, err := s.DB().Exec(`CREATE TRIGGER reject_custody BEFORE INSERT ON spool_custody BEGIN SELECT ` + fault + `; END`); err != nil {
				t.Fatal(err)
			}
			if _, err := s.BeginSpoolCustody(row, v, custodyOperationID, custodySourceID,
				"source-identity", filepath.Join(t.TempDir(), "stage")); err == nil {
				t.Fatal("ignored/failed INSERT authorized stage allocation")
			}
			var count int
			if err := s.DB().QueryRow(`SELECT COUNT(*) FROM spool_custody`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("partial custody row count=%d err=%v", count, err)
			}
		})
	}
	for _, fault := range []string{"RAISE(IGNORE)", "RAISE(FAIL,'injected')"} {
		t.Run("update-"+fault, func(t *testing.T) {
			s, _, _, _, c := custodyFixture(t, 4096)
			if _, err := s.DB().Exec(`CREATE TRIGGER reject_custody_update BEFORE UPDATE ON spool_custody BEGIN SELECT ` + fault + `; END`); err != nil {
				t.Fatal(err)
			}
			if _, err := s.BindSpoolStage(c, c.SourceIdentity, "stage-identity"); err == nil {
				t.Fatal("ignored/failed UPDATE authorized stage identity")
			}
			got, err := s.SpoolCustodyForSource(c.SourceID)
			if err != nil || got != c {
				t.Fatalf("intent changed after failed transition %+v %v", got, err)
			}
		})
	}
}

func TestSpoolCustodyRetargetAndReplacementFence(t *testing.T) {
	s, sp, row, v, c := custodyFixture(t, 4096)
	var err error
	c, err = s.BindSpoolStage(c, c.SourceIdentity, "stage-identity")
	if err != nil {
		t.Fatal(err)
	}
	source, dest, err := s.CaptureRename(row.NFSPath, "moved.mov")
	if err != nil {
		t.Fatal(err)
	}
	moved, err := sp.MigrateActivePaths(row.NFSPath, "moved.mov")
	if err != nil || len(moved) != 1 {
		t.Fatalf("requeue: %v %v", moved, err)
	}
	if _, err := s.RenameExpected(source, dest); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginCustodyPublish(c, v, c.SourceIdentity, c.StageIdentity); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("old target published after rename: %v", err)
	}
	if ok, err := sp.MarkDraining(moved[0].NewID); err != nil || !ok {
		t.Fatalf("new row claim %v %v", ok, err)
	}
	newRow, err := sp.Get(moved[0].NewID)
	if err != nil {
		t.Fatal(err)
	}
	newTarget, err := s.CaptureMirror("moved.mov")
	if err != nil {
		t.Fatal(err)
	}
	old := c
	c, err = s.RetargetSpoolCustody(c, newRow, newTarget)
	if err != nil || c.TargetPath != "moved.mov" || c.SpoolID != newRow.ID || c.SourceID != old.SourceID {
		t.Fatalf("retarget %+v %v", c, err)
	}
	if _, err := s.RetargetSpoolCustody(old, newRow, newTarget); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("stale rename won: %v", err)
	}
	if err := s.Delete(newTarget.Path); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(MakeEntry(newTarget.Path, false, 12, time.Now(), 999)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginCustodyPublish(c, newTarget, c.SourceIdentity, c.StageIdentity); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("replacement accepted: %v", err)
	}
	if _, err := s.ClaimSpoolCleanup(c, newTarget, "replacement", "source", c.SourceIdentity); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("replacement cleanup accepted: %v", err)
	}
}

func TestSpoolCustodyPunchedCrashAndQuarantine(t *testing.T) {
	file := filepath.Join(t.TempDir(), "mirror.db")
	s, err := Open(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	sp := NewSpoolStore(s.DB())
	s.SetSpoolStore(sp)
	if err := s.Insert(MakeEntry("reel.mov", false, 0, time.Now(), 42)); err != nil {
		t.Fatal(err)
	}
	v, err := s.CaptureMirror("reel.mov")
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.InsertBoundSpool(v, filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	if err := sp.MarkReady(id, 4096, nil); err != nil {
		t.Fatal(err)
	}
	if ok, err := sp.MarkDraining(id); err != nil || !ok {
		t.Fatalf("claim %v %v", ok, err)
	}
	row, err := sp.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.BeginSpoolCustody(row, v, custodyOperationID, custodySourceID, "source-identity",
		filepath.Join(t.TempDir(), "stage"))
	if err != nil {
		t.Fatal(err)
	}
	c, err = s.BindSpoolStage(c, c.SourceIdentity, "stage-identity")
	if err != nil {
		t.Fatal(err)
	}
	c, err = s.PrepareSpoolCheckpoint(c, c.SourceIdentity, c.StageIdentity, 4096, 2048, 2048, 2048)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(file)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	sp = NewSpoolStore(s.DB())
	s.SetSpoolStore(sp)
	var pending *PendingSpoolCustody
	if err := s.CheckSpoolRecovery(); !errors.As(err, &pending) || len(pending.Operations) != 1 ||
		pending.Operations[0].Phase != "checkpoint_pending" || pending.Operations[0].PunchedEnd != 2048 {
		t.Fatalf("restart lost possible punch: %v %+v", err, pending)
	}
	if _, err := s.BeginCustodyPublish(c, v, c.SourceIdentity, c.StageIdentity); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("ambiguous punch published: %v", err)
	}
	c, err = s.CancelSpoolCustody(c)
	if err != nil || c.Phase != "quarantined" || c.PunchedEnd != 2048 {
		t.Fatalf("cancel lost custody %+v %v", c, err)
	}
	if _, err := s.ClaimSpoolCleanup(c, v, "public", "stage", c.StageIdentity); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("quarantined stage deleted: %v", err)
	}
}

func TestSpoolCustodyStreamingGrowthBeforeFinalizedRow(t *testing.T) {
	s, sp := openStoreWithSpool(t)
	if err := s.Insert(MakeEntry("live.mov", false, 0, time.Now(), 43)); err != nil {
		t.Fatal(err)
	}
	v, err := s.CaptureMirror("live.mov")
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.InsertBoundSpool(v, filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	row, err := sp.Get(id)
	if err != nil || row.DrainState != DrainWriting || row.Size != 0 {
		t.Fatalf("stream row %+v %v", row, err)
	}
	c, err := s.BeginSpoolCustody(row, v, custodyOperationID, custodySourceID,
		"source-identity", filepath.Join(t.TempDir(), "stage"))
	if err != nil {
		t.Fatal(err)
	}
	c, err = s.BindSpoolStage(c, c.SourceIdentity, "stage-identity")
	if err != nil {
		t.Fatal(err)
	}
	c, err = s.PrepareSpoolCheckpoint(c, c.SourceIdentity, c.StageIdentity, 4096, 4096, 4096, 4096)
	if err != nil || c.TargetEOF != 4096 || c.PunchedEnd != 4096 {
		t.Fatalf("early stream checkpoint %+v %v", c, err)
	}
	if err := sp.SetPunchedEnd(id, 4096); err != nil {
		t.Fatal(err)
	}
	c, err = s.ConfirmSpoolCheckpoint(c, c.SourceIdentity, c.StageIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginCustodyPublish(c, v, c.SourceIdentity, c.StageIdentity); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("writing row published: %v", err)
	}
	c, err = s.PrepareSpoolCheckpoint(c, c.SourceIdentity, c.StageIdentity, 8192, 8192, 8192, 4096)
	if err != nil || c.TargetEOF != 8192 || c.VerifiedEnd != 8192 {
		t.Fatalf("growth checkpoint %+v %v", c, err)
	}
	c, err = s.ConfirmSpoolCheckpoint(c, c.SourceIdentity, c.StageIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if err := sp.MarkReady(id, 8192, nil); err != nil {
		t.Fatal(err)
	}
	c, err = s.ClaimPunchedSpool(c, v)
	if err != nil {
		t.Fatalf("custody drain claim: %v", err)
	}
	c, err = s.BeginCustodyPublish(c, v, c.SourceIdentity, c.StageIdentity)
	if err != nil || c.Phase != "publish_pending" {
		t.Fatalf("final stream publication %+v %v", c, err)
	}
}

func TestSpoolCustodyPunchedRenameRetargetAndPublish(t *testing.T) {
	s, sp, row, v, c := custodyFixture(t, 4096)
	var err error
	c, err = s.BindSpoolStage(c, c.SourceIdentity, "stage-identity")
	if err != nil {
		t.Fatal(err)
	}
	c, err = s.PrepareSpoolCheckpoint(c, c.SourceIdentity, c.StageIdentity, 4096, 4096, 4096, 2048)
	if err != nil {
		t.Fatal(err)
	}
	if err := sp.SetPunchedEnd(row.ID, 2048); err != nil {
		t.Fatal(err)
	}
	c, err = s.ConfirmSpoolCheckpoint(c, c.SourceIdentity, c.StageIdentity)
	if err != nil {
		t.Fatal(err)
	}
	source, dest, err := s.CaptureRename(row.NFSPath, "moved.mov")
	if err != nil {
		t.Fatal(err)
	}
	moved, err := sp.MigrateActivePaths(row.NFSPath, "moved.mov")
	if err != nil || len(moved) != 1 || !moved[0].Requeued {
		t.Fatalf("requeue %+v %v", moved, err)
	}
	newRow, err := sp.Get(moved[0].NewID)
	if err != nil || newRow.PunchedEnd != 2048 || newRow.DrainState != DrainReady {
		t.Fatalf("punched boundary lost on requeue %+v %v", newRow, err)
	}
	if rows, err := sp.ListReady(10); err != nil || len(rows) != 0 {
		t.Fatalf("ordinary ready queue admitted holey source %+v %v", rows, err)
	}
	if ok, err := sp.MarkDraining(newRow.ID); err != nil || ok {
		t.Fatalf("ordinary drain claimed holey source %v %v", ok, err)
	}
	if _, err := s.RenameExpected(source, dest); err != nil {
		t.Fatal(err)
	}
	newTarget, err := s.CaptureMirror("moved.mov")
	if err != nil {
		t.Fatal(err)
	}
	c, err = s.RetargetSpoolCustody(c, newRow, newTarget)
	if err != nil || c.TargetPath != newRow.NFSPath || c.PunchedEnd != 2048 {
		t.Fatalf("retarget %+v %v", c, err)
	}
	stale := c
	c, err = s.ClaimPunchedSpool(c, newTarget)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimPunchedSpool(stale, newTarget); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("stale punched claim: %v", err)
	}
	newRow, err = sp.Get(newRow.ID)
	if err != nil || newRow.DrainState != DrainDraining || newRow.PunchedEnd != 2048 {
		t.Fatalf("custody claim lost punched row %+v %v", newRow, err)
	}
	c, err = s.BeginCustodyPublish(c, newTarget, c.SourceIdentity, c.StageIdentity)
	if err != nil || c.Phase != "publish_pending" {
		t.Fatalf("publication intent %+v %v", c, err)
	}
	if v.Path == c.TargetPath {
		t.Fatal("old target still selected")
	}
}

func TestSpoolCustodyLegacyPunchedReadyCannotDrain(t *testing.T) {
	s, sp := openStoreWithSpool(t)
	id, err := sp.Insert("legacy.mov", filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	if err := sp.MarkReady(id, 4096, nil); err != nil {
		t.Fatal(err)
	}
	if err := sp.SetPunchedEnd(id, 2048); err != nil {
		t.Fatal(err)
	}
	if ok, err := sp.MarkDraining(id); err != nil || ok {
		t.Fatalf("legacy holey source claimed %v %v", ok, err)
	}
	if rows, err := sp.ListReady(10); err != nil || len(rows) != 0 {
		t.Fatalf("legacy holey source queued %+v %v", rows, err)
	}
	if _, err := s.SpoolCustodyForSource(custodySourceID); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("legacy source adopted: %v", err)
	}
}

func TestSpoolCustodyLegacyAndVolatileRefusal(t *testing.T) {
	s, _, _, _ := boundSpoolFixture(t, 4096)
	if _, err := s.SpoolCustodyForSource(custodySourceID); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("legacy row silently adopted: %v", err)
	}
	volatile, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer volatile.Close()
	if err := InitSpoolSchema(volatile.DB()); err != nil {
		t.Fatal(err)
	}
	sp := NewSpoolStore(volatile.DB())
	volatile.SetSpoolStore(sp)
	if err := volatile.Insert(MakeEntry("volatile.mov", false, 0, time.Now(), 11)); err != nil {
		t.Fatal(err)
	}
	v, err := volatile.CaptureMirror("volatile.mov")
	if err != nil {
		t.Fatal(err)
	}
	id, err := volatile.InsertBoundSpool(v, filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	if err := sp.MarkReady(id, 1, nil); err != nil {
		t.Fatal(err)
	}
	if ok, err := sp.MarkDraining(id); err != nil || !ok {
		t.Fatalf("claim %v %v", ok, err)
	}
	row, err := sp.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := volatile.BeginSpoolCustody(row, v, custodyOperationID, custodySourceID,
		"source-identity", filepath.Join(t.TempDir(), "stage")); err == nil {
		t.Fatal("volatile database accepted durable stage intent")
	}
}

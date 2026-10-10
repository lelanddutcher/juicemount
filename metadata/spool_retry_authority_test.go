package metadata

import (
	"database/sql"
	"testing"
	"time"
)

func TestSpoolRetryDueSurvivesHoldAndCompetingClaim(t *testing.T) {
	s := NewSpoolStore(openTestDB(t))
	id, err := s.Insert("proj/edit.bin", "/spool/files/edit")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkReady(id, 1, nil); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.MarkDraining(id); err != nil || !ok {
		t.Fatalf("initial claim: %v %v", ok, err)
	}
	since := time.Now().Add(-time.Minute)
	due := time.Now().Add(80 * time.Millisecond)
	if err := s.RequeueAfter(id, "saved bytes awaiting publication", since, due); err != nil {
		t.Fatal(err)
	}
	if rows, err := s.ListReady(1); err != nil || len(rows) != 0 {
		t.Fatalf("early list: %d %v", len(rows), err)
	}
	if ok, err := s.MarkDraining(id); err != nil || ok {
		t.Fatalf("early competing claim: %v %v", ok, err)
	}
	time.Sleep(time.Until(due) + time.Millisecond)
	if rows, err := s.ListReady(1); err != nil || len(rows) != 1 {
		t.Fatalf("due list: %d %v", len(rows), err)
	}
	if ok, err := s.MarkDraining(id); err != nil || !ok {
		t.Fatalf("due claim: %v %v", ok, err)
	}
	if ok, err := s.HoldForProject(id, "proj", "waiting_project: lease held"); err != nil || !ok {
		t.Fatalf("hold: %v %v", ok, err)
	}
	if n, err := s.ReleaseProjectHold("proj"); err != nil || n != 1 {
		t.Fatalf("release: %d %v", n, err)
	}
	r, err := s.Get(id)
	if err != nil || !r.PauseSince.Equal(since) || !r.RetryAfter.Equal(due) {
		t.Fatalf("hold changed pause/due authority: %+v %v", r, err)
	}
	var seq int
	var name, dbPath string
	if err := s.db.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &dbPath); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedDB.Close() })
	reopened := NewSpoolStore(reopenedDB)
	if after, err := reopened.Get(id); err != nil || !after.PauseSince.Equal(since) || !after.RetryAfter.Equal(due) {
		t.Fatalf("restart changed pause/due authority: %+v %v", after, err)
	}
	if ok, err := reopened.MarkDraining(id); err != nil || !ok {
		t.Fatalf("restarted claim: %v %v", ok, err)
	}
	migrations, err := reopened.MigrateActivePaths("proj/edit.bin", "proj/renamed.bin")
	if err != nil || len(migrations) != 1 || !migrations[0].Requeued {
		t.Fatalf("rename successor: %+v %v", migrations, err)
	}
	successor, err := reopened.Get(migrations[0].NewID)
	if err != nil || !successor.PauseSince.Equal(since) || !successor.RetryAfter.Equal(due) {
		t.Fatalf("rename reset pause/due authority: %+v %v", successor, err)
	}
}

func TestRetirementTriggerUpgradeReplacesUnsafeDeletionBody(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`DROP TRIGGER spool_retire_accepted_delete;
CREATE TRIGGER spool_retire_accepted_delete AFTER DELETE ON spool_entries
BEGIN UPDATE spool_entries SET retired_by=OLD.id WHERE nfs_path=OLD.nfs_path; END;`); err != nil {
		t.Fatal(err)
	}
	if err := InitSpoolSchema(db); err != nil {
		t.Fatal(err)
	}
	assertRetiredDoneCannotRetireRenamedWinner(t, NewSpoolStore(db))
}

func TestAtomicConflictBaseDowngradeDecodesAsAbsent(t *testing.T) {
	ino, mt, ct, size := (SpoolBase{Kind: SpoolBaseAtomicConflict}).columns()
	if got := spoolBaseFromColumns(ino, mt, ct, size); got.Kind != SpoolBaseAtomicConflict {
		t.Fatalf("atomic conflict roundtrip = %s", got.Kind)
	}
	if ino != spoolBaseInoAbsent {
		t.Fatalf("older decoder would not see absent base: %d", ino)
	}
}

func TestFailedLatestRowFencesDeleteAndRenameUnderStoreLock(t *testing.T) {
	s := NewSpoolStore(openTestDB(t))
	id, err := s.Insert("proj/edit.bin", "/spool/files/edit")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkReady(id, 4, nil); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.MarkDraining(id); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if ok, err := s.FailClaimedDrain(id, "proj/edit.bin", "/spool/files/edit", "retained save", false, nil); err != nil || !ok {
		t.Fatalf("terminal CAS: %v %v", ok, err)
	}
	if _, err := s.DeleteActiveByPath("proj/edit.bin"); err == nil {
		t.Fatal("delete admitted failed latest row")
	}
	if _, err := s.MigrateActivePaths("proj", "other"); err == nil {
		t.Fatal("parent rename admitted failed latest child")
	}
	if _, err := s.MigrateActivePaths("other", "proj"); err == nil {
		t.Fatal("destination rename admitted failed latest child")
	}
	if ok, err := s.FailClaimedDrain(id, "proj/edit.bin", "/spool/files/edit", "stale worker", false, nil); err != nil || ok {
		t.Fatalf("terminal CAS accepted stale worker: %v %v", ok, err)
	}
}

func TestRetiredDoneDeletionCannotRetireLowerIDRenamedWinner(t *testing.T) {
	assertRetiredDoneCannotRetireRenamedWinner(t, NewSpoolStore(openTestDB(t)))
}

func assertRetiredDoneCannotRetireRenamedWinner(t *testing.T, s *SpoolStore) {
	t.Helper()
	source, err := s.Insert("project/source.mov", "/spool/source")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkReady(source, 1, nil); err != nil {
		t.Fatal(err)
	}
	completed, err := s.Insert("project/dest.mov", "/spool/dest")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkReady(completed, 1, nil); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.MarkDraining(completed); err != nil || !ok {
		t.Fatalf("claim destination: %v %v", ok, err)
	}
	if ok, err := s.MarkDone(completed); err != nil || !ok {
		t.Fatalf("complete destination: %v %v", ok, err)
	}
	if _, err := s.MigrateActivePaths("project/source.mov", "project/dest.mov"); err != nil {
		t.Fatal(err)
	}
	old, err := s.Get(completed)
	if err != nil || old.RetiredBy != source {
		t.Fatalf("destination history not retired: %+v %v", old, err)
	}
	if _, err := s.DeleteDone(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	current, err := s.Get(source)
	if err != nil || current.RetiredBy != 0 || current.NFSPath != "project/dest.mov" {
		t.Fatalf("GC retired rename winner: %+v %v", current, err)
	}
	var seq int
	var name, dbPath string
	if err := s.db.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &dbPath); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedDB.Close() })
	if err := InitSpoolSchema(reopenedDB); err != nil {
		t.Fatal(err)
	}
	reopened := NewSpoolStore(reopenedDB)
	current, err = reopened.Get(source)
	if err != nil || current.RetiredBy != 0 {
		t.Fatalf("restart retired rename winner: %+v %v", current, err)
	}
	if ok, err := reopened.MarkDraining(source); err != nil || !ok {
		t.Fatalf("renamed winner not claimable: %v %v", ok, err)
	}
}

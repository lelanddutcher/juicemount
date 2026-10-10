package metadata

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func waitForPruneLadderStageGone(t *testing.T, s *Store, generation string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var rows, manifests int
		if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage WHERE generation=?`, generation).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage_state WHERE generation=?`, generation).Scan(&manifests); err != nil {
			t.Fatal(err)
		}
		if rows == 0 && manifests == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("prune ladder generation %q was not reclaimed", generation)
}

func TestPruneLadderGCProtectsCompleteUnpublishedStage(t *testing.T) {
	s := openStageTestStore(t, filepath.Join(t.TempDir(), "mirror.db"))
	generation, err := s.StagePruneLadderSnapshot(map[string]int{"pending.mov": 2})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.GCPruneLadderStages(100); err != nil || n != 0 {
		t.Fatalf("GC complete unpublished stage = %d, %v; want no deletion", n, err)
	}
	var rows, manifests int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage WHERE generation=?`, generation).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage_state WHERE generation=? AND complete=1`, generation).Scan(&manifests); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || manifests != 1 {
		t.Fatalf("GC removed complete unpublished stage: rows=%d manifests=%d", rows, manifests)
	}
	if err := s.AbandonPruneLadderStage(generation); err != nil {
		t.Fatal(err)
	}
	waitForPruneLadderStageGone(t, s, generation)
}

func TestPruneLadderGCEmptyPassDoesNotTakeWriteLock(t *testing.T) {
	s := openStageTestStore(t, filepath.Join(t.TempDir(), "mirror.db"))
	s.writeMu.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := s.GCPruneLadderStages(100)
		done <- err
	}()
	select {
	case err := <-done:
		s.writeMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		s.writeMu.Unlock()
		<-done
		t.Fatal("empty prune ladder GC waited for Store writeMu")
	}
}

func TestPruneLadderGCDoesNotAbandonActiveStage(t *testing.T) {
	s := openStageTestStore(t, filepath.Join(t.TempDir(), "mirror.db"))
	generation, err := s.StagePruneLadderSnapshot(map[string]int{"active.mov": 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ActivatePruneLadderStage(generation, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.AbandonPruneLadderStage(generation); err == nil {
		t.Fatal("abandoned active prune ladder generation")
	}
	if _, err := s.GCPruneLadderStages(100); err != nil {
		t.Fatal(err)
	}
	if got, err := s.LoadPruneLadder(); err != nil || got["active.mov"] != 3 {
		t.Fatalf("active ladder after abandon/GC = %v, %v", got, err)
	}
}

func TestH8AScanPublicationDoesNotWaitForLadderGC(t *testing.T) {
	t.Setenv("JM_PRUNE_LADDER_DURABLE", "1")
	s := openStageTestStore(t, filepath.Join(t.TempDir(), "mirror.db"))
	entered := make(chan struct{})
	release := make(chan struct{})
	var first sync.Once
	s.setPruneLadderGCBeforeBatch(func() {
		first.Do(func() {
			close(entered)
			<-release
		})
	})
	old := map[string]int{"old.mov": 1}
	oldGeneration, err := s.StagePruneLadderSnapshot(old)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ActivatePruneLadderStage(oldGeneration, time.Now()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("GC worker did not reach batch hook")
	}
	rc := &RedisClient{store: s, pruneAbsent: map[string]int{"old.mov": 2}, ladderPersisted: old}
	token := scanTokenForPublicationTest(t, rc)
	done := make(chan error, 1)
	go func() {
		completed, err := rc.completeScanPublication(token, time.Now())
		if err == nil && !completed {
			err = errScanPublicationInvalidated
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			close(release)
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		close(release)
		<-done
		t.Fatal("SCAN publication waited for blocked GC worker")
	}
	var oldRows int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage WHERE generation=?`, oldGeneration).Scan(&oldRows); err != nil {
		close(release)
		t.Fatal(err)
	}
	if oldRows != 1 {
		close(release)
		t.Fatalf("SCAN synchronously reclaimed %d old rows while GC worker was blocked", 1-oldRows)
	}
	close(release)
	waitForPruneLadderStageGone(t, s, oldGeneration)
}

func TestPruneLadderGCCloseJoinsWorker(t *testing.T) {
	s := openStageTestStore(t, filepath.Join(t.TempDir(), "mirror.db"))
	entered := make(chan struct{})
	release := make(chan struct{})
	s.setPruneLadderGCBeforeBatch(func() {
		close(entered)
		<-release
	})
	generation, err := s.StagePruneLadderSnapshot(map[string]int{"active.mov": 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ActivatePruneLadderStage(generation, time.Now()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("GC worker did not reach batch hook")
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		close(release)
		t.Fatalf("Store.Close returned before GC worker joined: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Store.Close did not join GC worker")
	}
}

func TestPruneLadderGCWorkerRetriesFailedBatch(t *testing.T) {
	s := openStageTestStore(t, filepath.Join(t.TempDir(), "mirror.db"))
	oldGeneration, err := s.StagePruneLadderSnapshot(map[string]int{"old.mov": 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ActivatePruneLadderStage(oldGeneration, time.Now()); err != nil {
		t.Fatal(err)
	}
	batchStarted := make(chan struct{}, 4)
	s.setPruneLadderGCBeforeBatch(func() {
		var retired int
		if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage_state WHERE generation=? AND complete=?`, oldGeneration, pruneLadderStageRetired).Scan(&retired); err == nil && retired != 0 {
			select {
			case batchStarted <- struct{}{}:
			default:
			}
		}
	})
	if _, err := s.DB().Exec(`CREATE TRIGGER fail_ladder_gc BEFORE DELETE ON prune_ladder_stage BEGIN SELECT RAISE(FAIL, 'injected GC failure'); END`); err != nil {
		t.Fatal(err)
	}
	newGeneration, err := s.StagePruneLadderSnapshot(map[string]int{"new.mov": 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ActivatePruneLadderStage(newGeneration, time.Now()); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		select {
		case <-batchStarted:
		case <-time.After(3 * time.Second):
			t.Fatalf("GC worker did not attempt batch %d", attempt)
		}
	}
	if _, err := s.DB().Exec(`DROP TRIGGER fail_ladder_gc`); err != nil {
		t.Fatal(err)
	}
	waitForPruneLadderStageGone(t, s, oldGeneration)
	if got, err := s.LoadPruneLadder(); err != nil || got["new.mov"] != 2 {
		t.Fatalf("active ladder after GC retry = %v, %v", got, err)
	}
}

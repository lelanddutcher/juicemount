package metadata

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

const (
	stageTestActiveGenerationMeta = "prune_ladder_active_generation"
	stageTestLastSyncMeta         = "last_sync_time"
	stageTestBaselineDebtMeta     = "push_baseline_incomplete"
)

func openStageTestStore(t *testing.T, dbPath string) *Store {
	t.Helper()
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func stageTestMeta(t *testing.T, s *Store, key string) string {
	t.Helper()
	value, ok, err := s.GetMeta(key)
	if err != nil || !ok {
		t.Fatalf("GetMeta(%q) = %q, %v, %v", key, value, ok, err)
	}
	return value
}

func TestPruneLadderStageLoadsLegacyUntilActivation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	s := openStageTestStore(t, dbPath)
	legacy := map[string]int{"legacy/a": 3, "legacy/b": 7}
	if err := s.PersistPruneLadderDelta(legacy, nil); err != nil {
		t.Fatalf("seed legacy ladder: %v", err)
	}

	got, err := s.LoadPruneLadder()
	if err != nil || !equalPruneLadder(got, legacy) {
		t.Fatalf("legacy load before activation = %v, %v; want %v", got, err, legacy)
	}

	next := map[string]int{"next/a": 9}
	generation, err := s.StagePruneLadderSnapshot(next)
	if err != nil {
		t.Fatalf("StagePruneLadderSnapshot: %v", err)
	}
	got, err = s.LoadPruneLadder()
	if err != nil || !equalPruneLadder(got, legacy) {
		t.Fatalf("staging changed visible ladder before activation: %v, %v", got, err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openStageTestStore(t, dbPath)
	got, err = s.LoadPruneLadder()
	if err != nil || !equalPruneLadder(got, legacy) {
		t.Fatalf("reopen before activation = %v, %v; want legacy %v (stage %q)", got, err, legacy, generation)
	}
}

func TestPruneLadderStageActivationPublishesSnapshotAndFreshnessTogether(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	s := openStageTestStore(t, dbPath)
	legacy := map[string]int{"legacy/a": 4}
	if err := s.PersistPruneLadderDelta(legacy, nil); err != nil {
		t.Fatalf("seed legacy ladder: %v", err)
	}
	if err := s.SetMeta(stageTestLastSyncMeta, "1700000000"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetMeta(stageTestBaselineDebtMeta, "1"); err != nil {
		t.Fatal(err)
	}

	want := map[string]int{"next/a": 8, "next/b": 2}
	generation, err := s.StagePruneLadderSnapshot(want)
	if err != nil {
		t.Fatalf("StagePruneLadderSnapshot: %v", err)
	}
	syncedAt := time.Unix(1700001234, 0)
	if err := s.ActivatePruneLadderStage(generation, syncedAt); err != nil {
		t.Fatalf("ActivatePruneLadderStage: %v", err)
	}

	if got, err := s.LoadPruneLadder(); err != nil || !equalPruneLadder(got, want) {
		t.Fatalf("active ladder = %v, %v; want %v", got, err, want)
	}
	if got := stageTestMeta(t, s, stageTestActiveGenerationMeta); got != generation {
		t.Fatalf("active generation = %q, want %q", got, generation)
	}
	if got := stageTestMeta(t, s, stageTestLastSyncMeta); got != fmt.Sprint(syncedAt.Unix()) {
		t.Fatalf("last sync = %q, want %d", got, syncedAt.Unix())
	}
	if got := stageTestMeta(t, s, stageTestBaselineDebtMeta); got != "0" {
		t.Fatalf("baseline debt = %q, want 0", got)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openStageTestStore(t, dbPath)
	if got, err := s.LoadPruneLadder(); err != nil || !equalPruneLadder(got, want) {
		t.Fatalf("reopened active ladder = %v, %v; want %v", got, err, want)
	}
	if got := stageTestMeta(t, s, stageTestActiveGenerationMeta); got != generation {
		t.Fatalf("reopened active generation = %q, want %q", got, generation)
	}
	if got := stageTestMeta(t, s, stageTestLastSyncMeta); got != fmt.Sprint(syncedAt.Unix()) {
		t.Fatalf("reopened last sync = %q, want %d", got, syncedAt.Unix())
	}
	if got := stageTestMeta(t, s, stageTestBaselineDebtMeta); got != "0" {
		t.Fatalf("reopened baseline debt = %q, want 0", got)
	}
}

func TestPruneLadderStageFailureKeepsOldGenerationAndFreshness(t *testing.T) {
	for _, failedKey := range []string{stageTestActiveGenerationMeta, stageTestLastSyncMeta, stageTestBaselineDebtMeta} {
		t.Run(failedKey, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "mirror.db")
			s := openStageTestStore(t, dbPath)
			old := map[string]int{"old/a": 5, "old/b": 6}
			if err := s.PersistPruneLadderDelta(old, nil); err != nil {
				t.Fatalf("seed legacy ladder: %v", err)
			}
			if err := s.SetMeta(stageTestLastSyncMeta, "1600000000"); err != nil {
				t.Fatal(err)
			}
			if err := s.SetMeta(stageTestBaselineDebtMeta, "1"); err != nil {
				t.Fatal(err)
			}
			oldGeneration, err := s.StagePruneLadderSnapshot(old)
			if err != nil {
				t.Fatalf("stage old snapshot: %v", err)
			}
			if err := s.ActivatePruneLadderStage(oldGeneration, time.Unix(1600000000, 0)); err != nil {
				t.Fatalf("activate old snapshot: %v", err)
			}
			// Model a later push event that creates fresh baseline debt while
			// the prior generation and sync timestamp remain active.
			if err := s.SetMeta(stageTestBaselineDebtMeta, "1"); err != nil {
				t.Fatal(err)
			}

			if _, err := s.DB().Exec(fmt.Sprintf(`CREATE TRIGGER fail_stage_activation BEFORE INSERT ON store_meta WHEN NEW.key = '%s' BEGIN SELECT RAISE(FAIL, 'injected activation failure'); END`, failedKey)); err != nil {
				t.Fatalf("create insert trigger: %v", err)
			}
			if _, err := s.DB().Exec(fmt.Sprintf(`CREATE TRIGGER fail_stage_activation_update BEFORE UPDATE ON store_meta WHEN NEW.key = '%s' BEGIN SELECT RAISE(FAIL, 'injected activation failure'); END`, failedKey)); err != nil {
				t.Fatalf("create update trigger: %v", err)
			}

			next := map[string]int{"new/a": 99}
			newGeneration, err := s.StagePruneLadderSnapshot(next)
			if err != nil {
				t.Fatalf("stage next snapshot: %v", err)
			}
			if err := s.ActivatePruneLadderStage(newGeneration, time.Unix(1700000000, 0)); err == nil {
				t.Fatalf("activation succeeded despite injected failure at %q", failedKey)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			s = openStageTestStore(t, dbPath)
			if got, err := s.LoadPruneLadder(); err != nil || !equalPruneLadder(got, old) {
				t.Fatalf("reopened ladder after activation failure = %v, %v; want %v", got, err, old)
			}
			if got := stageTestMeta(t, s, stageTestActiveGenerationMeta); got != oldGeneration {
				t.Fatalf("reopened generation = %q, want old %q", got, oldGeneration)
			}
			if got := stageTestMeta(t, s, stageTestLastSyncMeta); got != "1600000000" {
				t.Fatalf("reopened last sync = %q, want old timestamp", got)
			}
			if got := stageTestMeta(t, s, stageTestBaselineDebtMeta); got != "1" {
				t.Fatalf("reopened baseline debt = %q, want old 1", got)
			}
		})
	}
}

func TestPruneLadderIncompleteStageIsIgnoredAfterReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	s := openStageTestStore(t, dbPath)
	legacy := map[string]int{"legacy/a": 1}
	if err := s.PersistPruneLadderDelta(legacy, nil); err != nil {
		t.Fatalf("seed legacy ladder: %v", err)
	}
	if _, err := s.DB().Exec(`CREATE TRIGGER fail_prune_ladder_stage_second_chunk BEFORE INSERT ON prune_ladder_stage WHEN (SELECT COUNT(*) FROM prune_ladder_stage) >= 10000 BEGIN SELECT RAISE(FAIL, 'injected second chunk failure'); END`); err != nil {
		t.Fatalf("create stage trigger: %v", err)
	}

	partial := make(map[string]int, 10001)
	for i := 0; i < 10001; i++ {
		partial[fmt.Sprintf("partial/%05d", i)] = i + 1
	}
	if generation, err := s.StagePruneLadderSnapshot(partial); err == nil {
		t.Fatalf("stage unexpectedly completed with generation %q", generation)
	}
	var partialRows int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage`).Scan(&partialRows); err != nil {
		t.Fatal(err)
	}
	if partialRows == 0 {
		t.Fatal("stage failure left no committed partial rows to exercise incomplete-stage filtering")
	}
	var incompleteStages int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage_state WHERE complete = 0`).Scan(&incompleteStages); err != nil {
		t.Fatal(err)
	}
	if incompleteStages == 0 {
		t.Fatal("stage failure left no incomplete manifest to exercise reopen filtering")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openStageTestStore(t, dbPath)
	if got, err := s.LoadPruneLadder(); err != nil || !equalPruneLadder(got, legacy) {
		t.Fatalf("reopened load exposed incomplete stage: %v, %v; want legacy %v", got, err, legacy)
	}
}

func TestPruneLadderActiveStageMustBeComplete(t *testing.T) {
	for _, corruption := range []string{"missing-manifest", "incomplete-manifest", "missing-row"} {
		t.Run(corruption, func(t *testing.T) {
			s := openStageTestStore(t, filepath.Join(t.TempDir(), "mirror.db"))
			generation, err := s.StagePruneLadderSnapshot(map[string]int{"gone/a": 8, "gone/b": 8})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.ActivatePruneLadderStage(generation, time.Now()); err != nil {
				t.Fatal(err)
			}
			switch corruption {
			case "missing-manifest":
				_, err = s.DB().Exec(`DELETE FROM prune_ladder_stage_state WHERE generation=?`, generation)
			case "incomplete-manifest":
				_, err = s.DB().Exec(`UPDATE prune_ladder_stage_state SET complete=0 WHERE generation=?`, generation)
			case "missing-row":
				_, err = s.DB().Exec(`DELETE FROM prune_ladder_stage WHERE generation=? AND path='gone/b'`, generation)
			}
			if err != nil {
				t.Fatal(err)
			}
			if got, err := s.LoadPruneLadder(); err == nil {
				t.Fatalf("corrupt active generation loaded ladder %v", got)
			}
		})
	}
}

func TestPruneLadderStageGCDrainsReplacedGenerationAndPreservesActiveAcrossReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	s := openStageTestStore(t, dbPath)

	replaced := make(map[string]int, 10001)
	for i := 0; i < 10001; i++ {
		replaced[fmt.Sprintf("replaced/%05d", i)] = i + 1
	}
	replacedGeneration, err := s.StagePruneLadderSnapshot(replaced)
	if err != nil {
		t.Fatalf("stage replaced snapshot: %v", err)
	}
	if err := s.ActivatePruneLadderStage(replacedGeneration, time.Now()); err != nil {
		t.Fatalf("activate replaced snapshot: %v", err)
	}

	wantActive := map[string]int{"active/a": 11, "active/b": 13}
	activeGeneration, err := s.StagePruneLadderSnapshot(wantActive)
	if err != nil {
		t.Fatalf("stage active snapshot: %v", err)
	}
	if err := s.ActivatePruneLadderStage(activeGeneration, time.Now()); err != nil {
		t.Fatalf("activate active snapshot: %v", err)
	}
	for i := 0; i < 16; i++ {
		deleted, err := s.GCPruneLadderStages(2048)
		if err != nil {
			t.Fatalf("GC pass %d: %v", i+1, err)
		}
		if deleted == 0 {
			break
		}
	}
	waitForPruneLadderStageGone(t, s, replacedGeneration)
	var oldRows, oldManifests int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage WHERE generation=?`, replacedGeneration).Scan(&oldRows); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage_state WHERE generation=?`, replacedGeneration).Scan(&oldManifests); err != nil {
		t.Fatal(err)
	}
	if oldRows != 0 || oldManifests != 0 {
		t.Fatalf("replaced generation remains after GC: rows=%d manifests=%d", oldRows, oldManifests)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openStageTestStore(t, dbPath)
	if got, err := s.LoadPruneLadder(); err != nil || !equalPruneLadder(got, wantActive) {
		t.Fatalf("active snapshot after GC and reopen = %v, %v; want %v", got, err, wantActive)
	}
	if got := stageTestMeta(t, s, stageTestActiveGenerationMeta); got != activeGeneration {
		t.Fatalf("active generation after GC and reopen = %q, want %q", got, activeGeneration)
	}
}

func TestPruneLadderStageGCProtectsRecentIncompleteGeneration(t *testing.T) {
	s := openStageTestStore(t, filepath.Join(t.TempDir(), "mirror.db"))
	generation := "recent-incomplete"
	createdNS := time.Now().UnixNano()
	if _, err := s.DB().Exec(`INSERT INTO prune_ladder_stage_state(generation,complete,row_count,created_ns,completed_ns) VALUES(?,0,1,?,0)`, generation, createdNS); err != nil {
		t.Fatalf("insert incomplete manifest: %v", err)
	}
	if _, err := s.DB().Exec(`INSERT INTO prune_ladder_stage(generation,path,absent_cycles,updated_ns) VALUES(?,?,?,?)`, generation, "partial/a", 1, createdNS); err != nil {
		t.Fatalf("insert partial row: %v", err)
	}

	deleted, err := s.GCPruneLadderStages(100)
	if err != nil {
		t.Fatalf("GC recent incomplete generation: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("GC deleted %d rows from a recent incomplete generation, want 0", deleted)
	}
	var manifests, rows int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage_state WHERE generation=? AND complete=0`, generation).Scan(&manifests); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage WHERE generation=?`, generation).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if manifests != 1 || rows != 1 {
		t.Fatalf("recent incomplete generation after GC: manifests=%d rows=%d, want 1 each", manifests, rows)
	}
}

func equalPruneLadder(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for path, count := range b {
		if a[path] != count {
			return false
		}
	}
	return true
}

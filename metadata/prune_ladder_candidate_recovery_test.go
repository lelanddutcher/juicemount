package metadata

import (
	"path/filepath"
	"testing"
	"time"
)

func TestPruneLadderReopenReclaimsOnlyOwnedUnpublishedCandidate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mirror.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	active, err := s.StagePruneLadderSnapshot(map[string]int{"active.mov": 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ActivatePruneLadderStage(active, time.Now()); err != nil {
		t.Fatal(err)
	}
	owned, err := s.StagePruneLadderSnapshot(map[string]int{"orphan.mov": 2})
	if err != nil {
		t.Fatal(err)
	}
	const legacy = "legacy-unowned-candidate"
	if _, err := s.DB().Exec(`INSERT INTO prune_ladder_stage_state(generation,complete,row_count,created_ns,completed_ns) VALUES(?,?,?,?,?)`, legacy, pruneLadderStageCandidate, 1, time.Now().UnixNano(), time.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`INSERT INTO prune_ladder_stage(generation,path,absent_cycles,updated_ns) VALUES(?,?,?,?)`, legacy, "legacy.mov", 1, time.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	waitForPruneLadderStageGone(t, s, owned)
	var ownerRows int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage_owner WHERE generation=?`, owned).Scan(&ownerRows); err != nil {
		t.Fatal(err)
	}
	if ownerRows != 0 {
		t.Fatalf("reclaimed candidate retains %d ownership records", ownerRows)
	}
	if got, err := s.LoadPruneLadder(); err != nil || got["active.mov"] != 3 {
		t.Fatalf("active ladder after recovery = %v, %v", got, err)
	}
	var legacyRows int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage_state WHERE generation=? AND complete=?`, legacy, pruneLadderStageCandidate).Scan(&legacyRows); err != nil {
		t.Fatal(err)
	}
	if legacyRows != 1 {
		t.Fatalf("legacy unowned candidate changed: %d manifests", legacyRows)
	}
}

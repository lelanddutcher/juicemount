package metadata

import (
	"errors"
	"testing"
	"time"
)

func scanTokenForPublicationTest(t *testing.T, rc *RedisClient) scanBaselineToken {
	t.Helper()
	token := rc.scanToken()
	if rc.store != nil {
		epoch, err := rc.store.CaptureStoreEpoch()
		if err != nil {
			t.Fatal(err)
		}
		token.store = epoch
	}
	return token
}

func TestPruneLadderActivationRejectsCachePublicationAfterStage(t *testing.T) {
	s := openTestStore(t)
	expected, err := s.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	generation, err := s.StagePruneLadderSnapshot(map[string]int{"missing.mov": 1})
	if err != nil {
		t.Fatal(err)
	}
	s.InsertToCache(MakeEntry("foreground.mov", false, 1, time.Now(), 73))

	err = s.activatePruneLadderStage(generation, time.Now(), &expected)
	if !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("activation after cache publication = %v, want mirror conflict", err)
	}
	if _, ok, err := s.GetMeta(pruneLadderActiveGenerationKey); err != nil || ok {
		t.Fatalf("conflicted ladder marker present: ok=%v err=%v", ok, err)
	}
	if _, ok, err := s.GetMeta(metaKeyLastSyncTime); err != nil || ok {
		t.Fatalf("conflicted sync time present: ok=%v err=%v", ok, err)
	}
	if err := s.AbandonPruneLadderStage(generation); err != nil {
		t.Fatal(err)
	}
}

func TestScanPublicationAbandonsStageAfterStoreEpochConflict(t *testing.T) {
	t.Setenv("JM_PRUNE_LADDER_DURABLE", "1")
	s := openTestStore(t)
	rc := &RedisClient{store: s, pruneAbsent: map[string]int{"missing.mov": 1}}
	token := scanTokenForPublicationTest(t, rc)
	rc.testBeforePruneLadderPersist = func() {
		s.InsertToCache(MakeEntry("foreground.mov", false, 1, time.Now(), 74))
	}

	completed, err := rc.completeScanPublication(token, time.Now())
	if err != nil || completed {
		t.Fatalf("conflicted SCAN publication = %v, %v; want incomplete", completed, err)
	}
	var abandoned int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage_state WHERE complete=?`, pruneLadderStageAbandoned).Scan(&abandoned); err != nil {
		t.Fatal(err)
	}
	if abandoned != 1 {
		t.Fatalf("abandoned staged generations = %d, want 1", abandoned)
	}
	if _, ok, err := s.GetMeta(pruneLadderActiveGenerationKey); err != nil || ok {
		t.Fatalf("conflicted ladder marker present: ok=%v err=%v", ok, err)
	}
	if _, ok, err := s.GetMeta(metaKeyLastSyncTime); err != nil || ok {
		t.Fatalf("conflicted sync time present: ok=%v err=%v", ok, err)
	}
}

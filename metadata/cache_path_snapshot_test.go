package metadata

import (
	"testing"
	"time"
)

func TestSnapshotEligibleCachePathCopiesQualifiedAncestry(t *testing.T) {
	s, gate, original, id := qualifiedCachePathFixture(t, ":memory:")
	if _, ok := gate.SnapshotEligibleCachePath(testCachePathBinding, id); ok {
		t.Fatal("unconfirmed path produced a RAM snapshot")
	}
	if err := gate.ConfirmCachePath(testCachePathBinding, original, cachePathTestProof(original, testCachePathBinding.FormatUUID)); err != nil {
		t.Fatal(err)
	}
	got, ok := gate.SnapshotEligibleCachePath(testCachePathBinding, id)
	if !ok || got.Path != id.Path || len(got.Chain) != len(original.Chain) || !gate.CachePathObservationCurrent(got) {
		t.Fatalf("qualified RAM snapshot=%+v ok=%v", got, ok)
	}
	got.Chain[0].Inode++
	if !gate.CachePathEligible(testCachePathBinding, id) {
		t.Fatal("caller mutated the stored qualified chain")
	}
	if err := s.Insert(cachePathTestEntry("a", true, 11, 1700000019)); err != nil {
		t.Fatal(err)
	}
	if _, ok := gate.SnapshotEligibleCachePath(testCachePathBinding, id); ok {
		t.Fatal("changed ancestor produced a qualified RAM snapshot")
	}
}

func TestSnapshotEligibleCachePathDoesNotWaitForSQLWriter(t *testing.T) {
	s, gate, original, id := qualifiedCachePathFixture(t, ":memory:")
	if err := gate.ConfirmCachePath(testCachePathBinding, original, cachePathTestProof(original, testCachePathBinding.FormatUUID)); err != nil {
		t.Fatal(err)
	}
	s.writeMu.Lock()
	done := make(chan bool, 1)
	consumed := false
	defer func() {
		s.writeMu.Unlock()
		if !consumed {
			<-done // join the original worker before Store cleanup, even on Fatal
		}
	}()
	go func() { _, ok := gate.SnapshotEligibleCachePath(testCachePathBinding, id); done <- ok }()
	select {
	case ok := <-done:
		consumed = true
		if !ok {
			t.Fatal("qualified RAM snapshot refused while SQL writer was held")
		}
	case <-time.After(time.Second):
		t.Fatal("RAM snapshot waited for SQL writer")
	}
}

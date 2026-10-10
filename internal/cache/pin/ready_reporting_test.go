package pin

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// A durable producer witness survives a restart, but reporting resident bytes
// requires this process to validate and admit that exact generation locally.
type reportingRestoreProvider struct {
	*testOwnedPinProvider
	reject string
}

func (p *reportingRestoreProvider) ValidateReady(_ context.Context, e Entry) bool {
	return e.Path != p.reject && len(e.Authority) != 0
}

func TestReadyReportingRequiresLocalRestoreAdmission(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pin.db")
	initial, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = initial.Close() })
	if err := initial.SetReadyValidator(func(e Entry) bool { return len(e.Authority) != 0 }); err != nil {
		t.Fatal(err)
	}
	fixtures := []struct {
		path string
		size int64
		root string
	}{
		{"/volume/good.mov", 4, "/volume"},
		{"/volume/rejected.mov", 5, "/volume"},
		{"/other/unrelated.mov", 7, "/other"},
	}
	witnesses := make(map[string]Entry)
	for _, f := range fixtures {
		if err := initial.Pin(f.path, f.size, f.root); err != nil {
			t.Fatal(err)
		}
		witnesses[f.path] = markTestPinReady(t, initial, f.path)
	}
	if err := initial.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	validatorLive := true
	if err := reopened.SetReadyValidator(func(e Entry) bool {
		return validatorLive && len(e.Authority) != 0
	}); err != nil {
		t.Fatal(err)
	}
	checkWitnesses := func() {
		t.Helper()
		for _, f := range fixtures {
			got, ok := reopened.Get(f.path)
			want := witnesses[f.path]
			if !ok || got.Generation != want.Generation || got.Status != StatusReady ||
				got.BytesCached != f.size || string(got.Authority) != string(want.Authority) {
				t.Fatalf("durable generation/witness changed for %q: %+v", f.path, got)
			}
		}
	}
	checkReported := func(ready, pending int, cached int64, volumeReady, volumePending int, volumeCached int64) {
		t.Helper()
		a, err := reopened.AggregateStats()
		if err != nil {
			t.Fatal(err)
		}
		if a.TotalFiles != 3 || a.TotalBytes != 16 || a.ReadyFiles != ready ||
			a.PendingFiles != pending || a.CachedBytes != cached {
			t.Fatalf("aggregate reports unadmitted residency: %+v", a)
		}
		roots, err := reopened.PinRoots()
		if err != nil {
			t.Fatal(err)
		}
		if len(roots) != 2 || roots[0].Root != "/other" || roots[1].Root != "/volume" {
			t.Fatalf("unexpected root summaries: %+v", roots)
		}
		if roots[1].TotalFiles != 2 || roots[1].TotalBytes != 9 ||
			roots[1].ReadyFiles != volumeReady || roots[1].PendingFiles != volumePending ||
			roots[1].CachedBytes != volumeCached {
			t.Fatalf("/volume reports unadmitted residency: %+v", roots[1])
		}
	}
	checkWitnesses()
	for _, f := range fixtures {
		if reopened.IsPinnedReady(f.path) {
			t.Fatalf("SQL Ready admitted before restore: %q", f.path)
		}
	}
	checkReported(0, 3, 0, 0, 2, 0)

	provider := &reportingRestoreProvider{
		testOwnedPinProvider: &testOwnedPinProvider{store: reopened},
		reject:               "/volume/rejected.mov",
	}
	prefetcher := NewPrefetcher(reopened, "", "/volume", 1, provider)
	defer func() {
		if !prefetcher.StopWithin(5 * time.Second) {
			t.Error("restore worker did not join")
		}
	}()
	count, err := prefetcher.RestoreReady(context.Background())
	if err != nil || count != 2 {
		t.Fatalf("restore count=%d err=%v", count, err)
	}
	checkWitnesses() // Rejection must not destroy durable evidence or unrelated generations.
	if !reopened.IsPinnedReady("/volume/good.mov") ||
		reopened.IsPinnedReady("/volume/rejected.mov") ||
		!reopened.IsPinnedReady("/other/unrelated.mov") {
		t.Fatal("effective Ready admission differs from validated generations")
	}
	checkReported(2, 1, 11, 1, 1, 4)
	validatorLive = false
	checkReported(0, 3, 0, 0, 2, 0)
	validatorLive = true
	checkReported(2, 1, 11, 1, 1, 4)
	if err := reopened.Pin("/volume/good.mov", 4, "/volume"); err != nil {
		t.Fatal(err)
	}
	successor, ok := reopened.Get("/volume/good.mov")
	if !ok || successor.Generation == witnesses["/volume/good.mov"].Generation ||
		reopened.IsPinnedReady("/volume/good.mov") ||
		!reopened.IsPinnedReady("/other/unrelated.mov") {
		t.Fatalf("repin inherited admission or changed unrelated generation: %+v", successor)
	}
	checkReported(1, 2, 7, 0, 2, 0)
}

func TestReadyReportingValidatorRunsOutsideStoreLock(t *testing.T) {
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	validated := 0
	if err := s.SetReadyValidator(func(e Entry) bool {
		if !s.mu.TryLock() {
			return false
		}
		s.mu.Unlock()
		validated++
		return len(e.Authority) != 0
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Pin("/x.mov", 4, "/"); err != nil {
		t.Fatal(err)
	}
	markTestPinReady(t, s, "/x.mov")
	a, err := s.AggregateStats()
	if err != nil || a.ReadyFiles != 1 || a.CachedBytes != 4 {
		t.Fatalf("aggregate called validator under Store.mu: %+v %v", a, err)
	}
	roots, err := s.PinRoots()
	if err != nil || len(roots) != 1 || roots[0].ReadyFiles != 1 || validated < 2 {
		t.Fatalf("root validator lock/call: %+v %v calls=%d", roots, err, validated)
	}
}

func TestReadyReportingPreservesActiveProgressButNotFailedBytes(t *testing.T) {
	s := newTestStore(t)
	for _, e := range []struct {
		path string
		size int64
	}{{"/active.mov", 10}, {"/failed.mov", 8}} {
		if err := s.Pin(e.path, e.size, "/"); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.UpdateStatus("/active.mov", StatusPrefetching, 4, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateStatus("/failed.mov", StatusFailed, 8, "authority unavailable"); err != nil {
		t.Fatal(err)
	}
	a, err := s.AggregateStats()
	if err != nil || a.TotalFiles != 2 || a.FailedFiles != 1 || a.CachedBytes != 4 {
		t.Fatalf("active/failed reporting: %+v %v", a, err)
	}
	roots, err := s.PinRoots()
	if err != nil || len(roots) != 1 || roots[0].CachedBytes != 4 || roots[0].FailedFiles != 1 {
		t.Fatalf("root active/failed reporting: %+v %v", roots, err)
	}
}

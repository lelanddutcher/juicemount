package metadata

import (
	"fmt"
	"testing"
	"time"
)

// TestBulkInsertStaysIncrementalAfterInit is the QA-40 regression: once the FTS
// is built, a LARGE reconcile delta (a big SD-card offload landing >threshold
// paths in one cycle) must NOT take the full-RebuildFTS path — that holds the
// SQLite writer through a whole-catalog reindex and stalls concurrent NFS
// CREATEs into a soft-mount timeout (a failed copy). It must instead maintain
// FTS incrementally and stay fully searchable.
func TestBulkInsertStaysIncrementalAfterInit(t *testing.T) {
	old := FTSFullRebuildThreshold
	FTSFullRebuildThreshold = 3 // tiny, so the 2nd insert exceeds it
	t.Cleanup(func() { FTSFullRebuildThreshold = old })

	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	// Initial sync builds the FTS and sets ftsInitialized.
	if err := s.BulkInsert([]*Entry{
		MakeEntry("Shoot/IMG_0001.CR3", false, 1, now, 1),
		MakeEntry("Shoot/IMG_0002.CR3", false, 1, now, 2),
	}, 500); err != nil {
		t.Fatal(err)
	}
	if !s.ftsInitialized.Load() {
		t.Fatal("ftsInitialized should be set after the first BulkInsert")
	}

	// Baseline the full-rebuild counter AFTER init. This is the load-bearing
	// assertion the prior version lacked: searchability alone cannot tell an
	// incremental maintain from a delete-all+full-reindex (both leave every row
	// searchable), so the old test passed even on the broken pre-fix code. We
	// instead assert the BRANCH taken — a large post-init delta must trigger
	// ZERO RebuildFTS calls.
	rebuildsBefore := s.ftsFullRebuilds.Load()

	// A LARGE post-init delta (10 > threshold 3) must stay incremental.
	var big []*Entry
	for i := 0; i < 10; i++ {
		big = append(big, MakeEntry(fmt.Sprintf("Offload/CARDFILE_%04d.CR3", i), false, 1, now, uint64(100+i)))
	}
	if err := s.BulkInsert(big, 500); err != nil {
		t.Fatal(err)
	}

	// THE decisive check: the large post-init delta must NOT have run a full
	// RebuildFTS (the writeMu-long-hold QA-40 stall). On the pre-fix code
	// (incremental := len(entries) < FTSFullRebuildThreshold) this delta of 10
	// exceeds the threshold of 3, takes the !incremental branch, calls
	// RebuildFTS, and this assertion FAILS — which is exactly the regression the
	// old searchability-only test could not catch.
	if got := s.ftsFullRebuilds.Load() - rebuildsBefore; got != 0 {
		t.Fatalf("post-init large delta triggered %d full RebuildFTS call(s); want 0 (must stay incremental — QA-40 stall path)", got)
	}

	// Secondary: it must also actually be searchable (incremental maintenance
	// truly ran, not silently dropped).
	res, err := s.Search("CARDFILE", 50, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 10 {
		t.Fatalf("expected 10 'CARDFILE' results after a post-init large delta (incremental FTS), got %d", len(res))
	}
	// The initial entries remain searchable too (FTS not corrupted).
	res, err = s.Search("IMG_000", 50, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 'IMG_000' results, got %d", len(res))
	}
}

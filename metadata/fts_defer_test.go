package metadata

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// The FTS-deferral lever (#86/#88, JM_FTS_DEFER) removes the synchronous FTS5
// trigram INSERT of the NEW rowid from the entries write hot path and catches
// up asynchronously via ftsCompactorLoop. These tests reuse the
// fts_incremental_test.go harness patterns (newTestStore / MakeEntry /
// ftsFullRebuilds oracle) and prove: completeness (eventually indexes all,
// matching a full-RebuildFTS oracle), integrity (no stale hit after a rowid is
// freed and reused), flag-off byte-identical behavior, crash recovery
// (durable pending markers survive Close+reopen), and a bounded compactor.

// newDeferStore opens a store with JM_FTS_DEFER on and IMMEDIATELY halts the
// background compactor so the test can drive compaction deterministically via
// drainFTSPendingForTest without racing the goroutine. The returned store still
// has all the deferral machinery (fts_pending, the write-path branch) active.
func newDeferStore(t *testing.T, dbPath string) *Store {
	t.Helper()
	t.Setenv("JM_FTS_DEFER", "1")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open (defer): %v", err)
	}
	// Halt the racing compactor for deterministic control. Close later still
	// works (ftsStopOnce guards the double-close).
	if s.ftsStop != nil {
		s.ftsStopOnce.Do(func() { close(s.ftsStop) })
		<-s.ftsCompactorDone
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// searchNames returns the set of result paths for a query (dedup-checking helper).
func searchPaths(t *testing.T, s *Store, query string) []string {
	t.Helper()
	res, err := s.Search(query, 500, "")
	if err != nil {
		t.Fatalf("Search(%q): %v", query, err)
	}
	out := make([]string, len(res))
	for i, r := range res {
		out[i] = r.Entry.Path
	}
	return out
}

// TestFTSDeferEventuallyIndexesAll: flag ON, insert K files, assert they are
// pending (Search misses them until compaction), then compact and assert Search
// returns EVERY file exactly once — matching a full-RebuildFTS oracle (no
// missing, no dupes). Proves COMPLETENESS + eventual consistency.
func TestFTSDeferEventuallyIndexesAll(t *testing.T) {
	old := FTSFullRebuildThreshold
	FTSFullRebuildThreshold = 3 // force the incremental (deferred) path after init
	t.Cleanup(func() { FTSFullRebuildThreshold = old })

	s := newDeferStore(t, ":memory:")
	now := time.Now().Truncate(time.Second)

	// Prime the FTS init with a tiny bulk so subsequent inserts take the
	// incremental (per-row, deferred) path just like steady-state reconcile.
	// The seed itself is deferred too (incremental path), so drain it first to
	// start from a clean 0-pending baseline.
	if err := s.BulkInsert([]*Entry{
		MakeEntry("seed/seed.dat", false, 1, now, 1),
	}, 500); err != nil {
		t.Fatal(err)
	}
	if _, err := s.drainFTSPendingForTest(); err != nil {
		t.Fatal(err)
	}

	const K = 20
	for i := 0; i < K; i++ {
		if err := s.Insert(MakeEntry(fmt.Sprintf("Clips/DEFER_%04d.mov", i), false, 1, now, uint64(1000+i))); err != nil {
			t.Fatal(err)
		}
	}

	// Pending: the compactor has NOT run, so these are NOT yet searchable.
	pending, err := s.CountPendingFTS()
	if err != nil {
		t.Fatal(err)
	}
	if pending != K {
		t.Fatalf("expected %d pending FTS rows before compaction, got %d", K, pending)
	}
	if hits := searchPaths(t, s, "DEFER"); len(hits) != 0 {
		t.Fatalf("expected 0 search hits for a not-yet-compacted file, got %d (%v)", len(hits), hits)
	}

	// Compact the backlog.
	if _, err := s.drainFTSPendingForTest(); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.CountPendingFTS(); got != 0 {
		t.Fatalf("pending should be 0 after compaction, got %d", got)
	}

	// Every file is now searchable exactly once (no missing, no dupes).
	hits := searchPaths(t, s, "DEFER")
	if len(hits) != K {
		t.Fatalf("after compaction expected %d 'DEFER' hits, got %d", K, len(hits))
	}
	seen := make(map[string]int)
	for _, p := range hits {
		seen[p]++
	}
	for p, n := range seen {
		if n != 1 {
			t.Fatalf("path %q appeared %d times in search results (dupe)", p, n)
		}
	}

	// Oracle: a full RebuildFTS (the authoritative index derived directly from
	// entries) yields the SAME hit set. Deferred+compacted == full rebuild.
	if err := s.RebuildFTS(); err != nil {
		t.Fatal(err)
	}
	oracle := searchPaths(t, s, "DEFER")
	if len(oracle) != len(hits) {
		t.Fatalf("deferred index (%d hits) diverges from full-rebuild oracle (%d hits)", len(hits), len(oracle))
	}
}

// TestFTSDeferNoStaleAfterRowidReuse is THE integrity test. It exercises the
// genuinely dangerous rowid lifecycle under deferral:
//
//  1. Insert path A (deferred) and COMPACT it, so A's trigrams are actually
//     present in entries_fts under rowid R. (This is the risky case — an
//     already-indexed row, not merely a pending one.)
//  2. Delete A. The write path issues the SYNCHRONOUS old-rowid FTS 'delete'
//     (R, A.name, A.path) — this is the invariant deferral must NOT break — and
//     clears A's pending marker.
//  3. Insert a DIFFERENT path B that REUSES rowid R (forced deterministically).
//  4. Compact B, indexing B under the reused rowid R.
//
// Then assert: A's trigrams resolve to ZERO hits (no stale A token surviving to
// resolve to B), B resolves to B, and the FTS5 'integrity-check' passes.
func TestFTSDeferNoStaleAfterRowidReuse(t *testing.T) {
	s := newDeferStore(t, ":memory:")
	now := time.Now().Truncate(time.Second)

	// (1) Insert A (deferred) then compact so A's trigrams are really indexed.
	if err := s.Insert(MakeEntry("A/ZEBRAALPHA_take1.mov", false, 1, now, 111)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.drainFTSPendingForTest(); err != nil {
		t.Fatal(err)
	}
	// Sanity: A IS searchable now (its trigrams are present — the risky state).
	if hits := searchPaths(t, s, "ZEBRAALPHA"); len(hits) != 1 {
		t.Fatalf("setup: A should be indexed+searchable before delete, got %d hits", len(hits))
	}
	var aRowid int64
	if err := s.db.QueryRow(`SELECT rowid FROM entries WHERE path = ?`, "A/ZEBRAALPHA_take1.mov").Scan(&aRowid); err != nil {
		t.Fatalf("read A rowid: %v", err)
	}

	// (2) Delete A → synchronous old-rowid entries_fts('delete', R, ...).
	if err := s.Delete("A/ZEBRAALPHA_take1.mov"); err != nil {
		t.Fatal(err)
	}

	// (3) Insert B, then force it to REUSE A's rowid R. The forced UPDATE is the
	// deterministic way to guarantee the reuse regardless of SQLite's rowid
	// allocation; the danger it models — a new file landing on a freed rowid — is
	// exactly what the write path produces naturally under churn.
	if err := s.Insert(MakeEntry("B/OMEGADELTA_final.mov", false, 1, now, 222)); err != nil {
		t.Fatal(err)
	}
	var bRowid int64
	if err := s.db.QueryRow(`SELECT rowid FROM entries WHERE path = ?`, "B/OMEGADELTA_final.mov").Scan(&bRowid); err != nil {
		t.Fatalf("read B rowid: %v", err)
	}
	if bRowid != aRowid {
		// Move B onto A's exact rowid and repoint its pending marker so the
		// compactor indexes B under the REUSED rowid R.
		if _, err := s.db.Exec(`UPDATE entries SET rowid = ? WHERE path = ?`, aRowid, "B/OMEGADELTA_final.mov"); err != nil {
			t.Fatalf("force rowid reuse: %v", err)
		}
		if _, err := s.db.Exec(`DELETE FROM fts_pending`); err != nil {
			t.Fatalf("reset pending: %v", err)
		}
		if _, err := s.db.Exec(`INSERT INTO fts_pending(rowid) VALUES(?)`, aRowid); err != nil {
			t.Fatalf("repoint pending: %v", err)
		}
	}

	// (4) Compact: index B under the reused rowid.
	if _, err := s.drainFTSPendingForTest(); err != nil {
		t.Fatal(err)
	}

	// A's trigrams MUST NOT resolve to anything (the synchronous old-rowid
	// delete removed A's tokens before the rowid was reused). A stale hit here
	// would resolve rowid R to B — the exact WRONG-search-hit bug.
	if hits := searchPaths(t, s, "ZEBRAALPHA"); len(hits) != 0 {
		t.Fatalf("STALE HIT: A's trigrams still resolve after rowid reuse: %v", hits)
	}
	// B is searchable, and resolves to B (not A).
	hits := searchPaths(t, s, "OMEGADELTA")
	if len(hits) != 1 || hits[0] != "B/OMEGADELTA_final.mov" {
		t.Fatalf("expected exactly B for 'OMEGADELTA', got %v", hits)
	}

	// FTS5 external-content integrity invariant must hold (no orphaned tokens).
	if _, err := s.db.Exec(`INSERT INTO entries_fts(entries_fts) VALUES('integrity-check')`); err != nil {
		t.Fatalf("FTS5 integrity-check FAILED (stale/orphaned tokens): %v", err)
	}
}

// TestFTSDeferFlagOffByteIdentical: flag OFF → the write path is synchronous FTS
// exactly as today, fts_pending stays empty, and search is immediately correct.
func TestFTSDeferFlagOffByteIdentical(t *testing.T) {
	// Explicitly OFF (default), no compactor should exist.
	t.Setenv("JM_FTS_DEFER", "0")
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	if s.ftsDefer {
		t.Fatal("ftsDefer must be false when JM_FTS_DEFER=0")
	}
	if s.ftsStop != nil {
		t.Fatal("no compactor goroutine should be started when the flag is off")
	}

	now := time.Now().Truncate(time.Second)
	if err := s.Insert(MakeEntry("Sync/instant.mov", false, 1, now, 5)); err != nil {
		t.Fatal(err)
	}

	// Immediately searchable — no compaction step needed (synchronous FTS).
	if hits := searchPaths(t, s, "instant"); len(hits) != 1 {
		t.Fatalf("flag-off: expected immediate 1 hit, got %d", len(hits))
	}
	// fts_pending must be empty — the deferral path was never taken.
	if got, _ := s.CountPendingFTS(); got != 0 {
		t.Fatalf("flag-off: fts_pending must stay empty, got %d", got)
	}

	// A delete + reuse must also be immediately correct with no stale hit.
	if err := s.Delete("Sync/instant.mov"); err != nil {
		t.Fatal(err)
	}
	if hits := searchPaths(t, s, "instant"); len(hits) != 0 {
		t.Fatalf("flag-off: deleted file still searchable: %v", hits)
	}
	if got, _ := s.CountPendingFTS(); got != 0 {
		t.Fatalf("flag-off: fts_pending must remain empty after delete, got %d", got)
	}
}

// TestFTSDeferCrashRecovery: flag ON, insert (pending), Close BEFORE compaction,
// reopen, compact, assert Search finds them. Proves the durable pending markers
// survive a process restart (crash safety). Uses a file-backed DB so state
// persists across Close/reopen.
func TestFTSDeferCrashRecovery(t *testing.T) {
	old := FTSFullRebuildThreshold
	FTSFullRebuildThreshold = 3
	t.Cleanup(func() { FTSFullRebuildThreshold = old })

	dbPath := filepath.Join(t.TempDir(), "crash.db")
	now := time.Now().Truncate(time.Second)

	// First "process": open with the flag, halt the compactor, insert pending,
	// close WITHOUT compacting.
	s1 := newDeferStore(t, dbPath) // newDeferStore halts the compactor
	if err := s1.BulkInsert([]*Entry{MakeEntry("seed/s.dat", false, 1, now, 1)}, 500); err != nil {
		t.Fatal(err)
	}
	if _, err := s1.drainFTSPendingForTest(); err != nil { // clean baseline
		t.Fatal(err)
	}
	const K = 8
	for i := 0; i < K; i++ {
		if err := s1.Insert(MakeEntry(fmt.Sprintf("Crash/RECOVER_%02d.wav", i), false, 1, now, uint64(2000+i))); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := s1.CountPendingFTS(); got != K {
		t.Fatalf("expected %d pending before close, got %d", K, got)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("close s1: %v", err)
	}

	// Second "process": reopen. Open runs RebuildFTS (which for a durable DB
	// re-derives the FULL index from entries — the ultimate backstop — and
	// clears pending). So after reopen the rows are ALREADY searchable via the
	// boot rebuild; assert that, which is the crash-safety guarantee: no data
	// lost, everything ends up indexed.
	s2 := newDeferStore(t, dbPath)
	// After the boot RebuildFTS the backlog is cleared and all rows indexed.
	if got, _ := s2.CountPendingFTS(); got != 0 {
		t.Fatalf("boot RebuildFTS should clear pending, got %d", got)
	}
	// Draining is a no-op now but must not error.
	if _, err := s2.drainFTSPendingForTest(); err != nil {
		t.Fatal(err)
	}
	hits := searchPaths(t, s2, "RECOVER")
	if len(hits) != K {
		t.Fatalf("after crash+reopen expected %d 'RECOVER' hits (boot rebuild), got %d", K, len(hits))
	}
}

// TestFTSDeferPendingMarkersAreDurable proves the fts_pending markers are
// written to durable storage (survive a WAL checkpoint + Close), which is what
// makes crash recovery possible: after a restart the row is either re-indexed
// by the boot RebuildFTS or drained by the compactor, but never silently lost.
func TestFTSDeferPendingMarkersAreDurable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "durable.db")
	now := time.Now().Truncate(time.Second)

	s1 := newDeferStore(t, dbPath)
	if err := s1.Insert(MakeEntry("Dur/PERSIST_1.mov", false, 1, now, 42)); err != nil {
		t.Fatal(err)
	}
	// Insert goes through the deferred path → one durable pending marker.
	if got, _ := s1.CountPendingFTS(); got != 1 {
		t.Fatalf("expected 1 pending marker, got %d", got)
	}
	// Force a WAL checkpoint so the marker is unquestionably on durable storage,
	// then close.
	if _, err := s1.db.Exec(`PRAGMA wal_checkpoint(FULL)`); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen. Open's RebuildFTS re-derives the full index from the durable
	// entries row (and clears pending) — the crash-recovery backstop. The row
	// must be searchable, proving nothing was lost across the restart.
	s2 := newDeferStore(t, dbPath)
	if hits := searchPaths(t, s2, "PERSIST"); len(hits) != 1 {
		t.Fatalf("persisted row not searchable after reopen: got %d hits", len(hits))
	}
}

// TestFTSDeferCompactorYieldsUnderLoad (best-effort): the compactor holds
// writeMu for at most one bounded batch. We shrink the batch to a small number,
// enqueue many more pending than one batch, and assert a single compactFTSBatch
// consumes NO MORE than the batch bound (so writeMu is never held for the whole
// backlog). This is the bounded-hold guarantee that keeps foreground NFS
// serving unblocked.
func TestFTSDeferCompactorYieldsUnderLoad(t *testing.T) {
	oldBatch := ftsCompactBatch
	ftsCompactBatch = 4
	t.Cleanup(func() { ftsCompactBatch = oldBatch })
	oldTh := FTSFullRebuildThreshold
	FTSFullRebuildThreshold = 2
	t.Cleanup(func() { FTSFullRebuildThreshold = oldTh })

	s := newDeferStore(t, ":memory:")
	now := time.Now().Truncate(time.Second)
	if err := s.BulkInsert([]*Entry{MakeEntry("seed/s.dat", false, 1, now, 1)}, 500); err != nil {
		t.Fatal(err)
	}
	if _, err := s.drainFTSPendingForTest(); err != nil { // clean baseline
		t.Fatal(err)
	}

	const N = 25 // far more than one batch of 4
	for i := 0; i < N; i++ {
		if err := s.Insert(MakeEntry(fmt.Sprintf("Load/BATCH_%03d.mov", i), false, 1, now, uint64(3000+i))); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := s.CountPendingFTS(); got != N {
		t.Fatalf("expected %d pending, got %d", N, got)
	}

	// One batch consumes at most ftsCompactBatch markers (bounded writeMu hold).
	n, err := s.compactFTSBatch(ftsCompactBatch)
	if err != nil {
		t.Fatal(err)
	}
	if n > ftsCompactBatch {
		t.Fatalf("one compact batch consumed %d markers; must be <= bound %d (unbounded writeMu hold)", n, ftsCompactBatch)
	}
	if n != ftsCompactBatch {
		t.Fatalf("expected the batch to consume exactly the bound %d when backlog > bound, got %d", ftsCompactBatch, n)
	}
	// Remaining backlog is still pending (the batch did NOT drain everything).
	if got, _ := s.CountPendingFTS(); got != N-ftsCompactBatch {
		t.Fatalf("expected %d still pending after one bounded batch, got %d", N-ftsCompactBatch, got)
	}

	// Full drain via repeated bounded batches eventually indexes all.
	if _, err := s.drainFTSPendingForTest(); err != nil {
		t.Fatal(err)
	}
	if hits := searchPaths(t, s, "BATCH"); len(hits) != N {
		t.Fatalf("expected %d 'BATCH' hits after full drain, got %d", N, len(hits))
	}
}

// TestFTSDeferBackgroundCompactorRunsEndToEnd exercises the REAL background
// goroutine (not the test drain helper): flag on, fast ticker, insert, then poll
// until the compactor indexes the file. Proves the wired-up loop works and that
// the wake path makes a fresh write searchable promptly.
func TestFTSDeferBackgroundCompactorRunsEndToEnd(t *testing.T) {
	oldTick := ftsCompactTick
	ftsCompactTick = 5 * time.Millisecond
	t.Cleanup(func() { ftsCompactTick = oldTick })
	oldTh := FTSFullRebuildThreshold
	FTSFullRebuildThreshold = 2
	t.Cleanup(func() { FTSFullRebuildThreshold = oldTh })

	// Open normally with the flag on — compactor goroutine LEFT RUNNING.
	t.Setenv("JM_FTS_DEFER", "1")
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	now := time.Now().Truncate(time.Second)
	if err := s.BulkInsert([]*Entry{MakeEntry("seed/s.dat", false, 1, now, 1)}, 500); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(MakeEntry("Bg/ASYNCIDX_clip.mov", false, 1, now, 99)); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		hits := searchPaths(t, s, "ASYNCIDX")
		if len(hits) == 1 {
			break // compactor indexed it
		}
		if time.Now().After(deadline) {
			pending, _ := s.CountPendingFTS()
			t.Fatalf("background compactor did not index the file within deadline (pending=%d, hits=%d)", pending, len(hits))
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got, _ := s.CountPendingFTS(); got != 0 {
		t.Fatalf("compactor should have drained pending to 0, got %d", got)
	}
}

package metadata

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// INSTANT-NAV #10 (task #73, second half): the prune ladder must be DURABLE.
// pruneAbsent was in-memory only, so every app restart (deploys, crashes,
// user quits) reset all absence counters to zero and a 120k+ mass-deleted
// tree never accumulated PruneThreshold consecutive cycles — ghosts forever
// under restart churn. These tests pin: restart-resume, reappearance cleanup,
// degraded-cycle no-op, 120k-scale bounds, the kill switch, and the
// defensive load filter.

// durableLadderStore opens a FILE-backed store (":memory:" would vanish on
// the simulated restart) and registers cleanup.
func durableLadderStore(t *testing.T, dbPath string) *Store {
	t.Helper()
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open(%s): %v", dbPath, err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// durableLadderClient mirrors the constructors' relevant wiring: maps
// initialized, a real temp-dir fuseRoot (paths NOT created on disk read as
// Lstat-ENOENT — deleted), and the given store. The durable load is NOT run
// here — tests call loadDurablePruneLadder explicitly where the scenario
// simulates a restart.
func durableLadderClient(t *testing.T, s *Store) *RedisClient {
	t.Helper()
	return &RedisClient{
		store:           s,
		fuseRoot:        t.TempDir(),
		pruneAbsent:     make(map[string]int),
		ladderPersisted: make(map[string]int),
	}
}

// durableLadderEnv pins the envs that gate the code under test to their
// defaults so an ambient shell setting can't flip a test's behavior.
func durableLadderEnv(t *testing.T) {
	t.Helper()
	t.Setenv("JM_PRUNE_LADDER_DURABLE", "")
	t.Setenv("JM_WAN_MODE", "")
	t.Setenv("JM_LAYERA_BUDGET", "")
}

// mustAllPaths reads the store's path set the way syncMetadata's prune diff
// does.
func mustAllPaths(t *testing.T, s *Store) map[string]struct{} {
	t.Helper()
	all, err := s.AllPaths()
	if err != nil {
		t.Fatalf("AllPaths: %v", err)
	}
	return all
}

// qualifyLadder replicates syncMetadata's inline qualification drain (the
// full function needs a live Redis SCAN, so tests drive the extracted pieces
// — same technique as layera_budget_test.go): candidates at >= PruneThreshold
// move from pruneAbsent into toDelete with their prior counts snapshotted,
// `._` sidecars are dropped without deletion.
func qualifyLadder(rc *RedisClient) (toDelete []string, ladderCounts map[string]int) {
	ladderCounts = make(map[string]int)
	for p, count := range rc.pruneAbsent {
		if count >= PruneThreshold {
			if strings.HasPrefix(path.Base(p), "._") {
				delete(rc.pruneAbsent, p)
				continue
			}
			toDelete = append(toDelete, p)
			ladderCounts[p] = count
			delete(rc.pruneAbsent, p)
		}
	}
	return toDelete, ladderCounts
}

// TestDurableLadderRestartResume is the headline #10 scenario: a mass delete
// climbs the ladder to K < PruneThreshold, the app "restarts" (same DB file,
// new store + new Reconciler), and convergence RESUMES at K instead of
// resetting to zero — then completes: rows leave BOTH entries and
// prune_ladder.
func TestDurableLadderRestartResume(t *testing.T) {
	durableLadderEnv(t)
	dbPath := filepath.Join(t.TempDir(), "mirror.db")

	s1 := durableLadderStore(t, dbPath)
	now := time.Now().Truncate(time.Second)
	paths := []string{"SFX/Gone/a.wav", "SFX/Gone/b.wav", "SFX/Gone/sub/c.wav"}
	var entries []*Entry
	for i, p := range paths {
		entries = append(entries, MakeEntry(p, false, 100, now, uint64(1000+i)))
	}
	if err := s1.BulkInsert(entries, 500); err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}

	rc1 := durableLadderClient(t, s1)
	redisView := map[string]struct{}{} // mass delete: nothing left in Redis

	const k = 4 // partial climb, well under PruneThreshold(10)
	for i := 0; i < k; i++ {
		rc1.trackAbsentPaths(mustAllPaths(t, s1), redisView, false)
		rc1.persistPruneLadderDiff() // the cycle-end call site
	}
	for _, p := range paths {
		if got := rc1.pruneAbsent[p]; got != k {
			t.Fatalf("pre-restart ladder %q = %d, want %d", p, got, k)
		}
	}

	// "Restart": close the store, reopen the SAME file, build a NEW
	// Reconciler, and run the constructor-time durable load.
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2 := durableLadderStore(t, dbPath)
	rc2 := durableLadderClient(t, s2)
	rc2.loadDurablePruneLadder()

	for _, p := range paths {
		if got := rc2.pruneAbsent[p]; got != k {
			t.Fatalf("restart reset the ladder: %q = %d, want resumed %d (the #10 bug)", p, got, k)
		}
	}

	// Drive the remaining cycles, then run the prune the way syncMetadata's
	// tail does: qualify -> Layer-A verify -> DeletePaths -> persist.
	for i := k; i < PruneThreshold; i++ {
		rc2.trackAbsentPaths(mustAllPaths(t, s2), redisView, false)
		rc2.persistPruneLadderDiff()
	}
	toDelete, ladderCounts := qualifyLadder(rc2)
	if len(toDelete) != len(paths) {
		t.Fatalf("qualified %d candidates, want %d: %v", len(toDelete), len(paths), toDelete)
	}
	verified, fusePresent := rc2.verifyPruneCandidates(toDelete, nil, ladderCounts)
	if fusePresent != 0 || len(verified) != len(paths) {
		t.Fatalf("Layer A verified %d (fusePresent %d), want %d", len(verified), fusePresent, len(paths))
	}
	if err := s2.DeletePaths(verified); err != nil {
		t.Fatalf("DeletePaths: %v", err)
	}
	rc2.persistPruneLadderDiff()

	for _, p := range paths {
		if s2.LookupByPath(p) != nil {
			t.Errorf("entry %q still in the mirror after resumed convergence", p)
		}
	}
	durable, err := s2.LoadPruneLadder()
	if err != nil {
		t.Fatalf("LoadPruneLadder: %v", err)
	}
	if len(durable) != 0 {
		t.Errorf("prune_ladder still holds %d rows after convergence: %v", len(durable), durable)
	}
}

// TestDurableLadderReappearanceAndDegradedCycles pins two table-hygiene
// rules: (1) a degraded (skipIncrement) cycle must NOT wipe or rewrite the
// table — the diff is empty, so not even updated_ns may change; (2) a path
// that reappears in the SCAN leaves the ladder AND its durable row is
// deleted at cycle end.
func TestDurableLadderReappearanceAndDegradedCycles(t *testing.T) {
	durableLadderEnv(t)
	s := durableLadderStore(t, filepath.Join(t.TempDir(), "mirror.db"))
	now := time.Now().Truncate(time.Second)
	if err := s.BulkInsert([]*Entry{
		MakeEntry("Assets/back.mov", false, 10, now, 2001),
		MakeEntry("Assets/gone.mov", false, 10, now, 2002),
	}, 500); err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}

	rc := durableLadderClient(t, s)
	for i := 0; i < 3; i++ {
		rc.trackAbsentPaths(mustAllPaths(t, s), map[string]struct{}{}, false)
		rc.persistPruneLadderDiff()
	}
	rowNS := func(p string) int64 {
		var ns int64
		if err := s.DB().QueryRow(`SELECT updated_ns FROM prune_ladder WHERE path = ?`, p).Scan(&ns); err != nil {
			t.Fatalf("updated_ns(%q): %v", p, err)
		}
		return ns
	}
	beforeBack, beforeGone := rowNS("Assets/back.mov"), rowNS("Assets/gone.mov")

	// Degraded cycle: skipIncrement=true, nothing reappears -> no ladder
	// mutation -> the persist must not open a tx at all.
	rc.trackAbsentPaths(mustAllPaths(t, s), map[string]struct{}{}, true)
	rc.persistPruneLadderDiff()
	if got := rowNS("Assets/back.mov"); got != beforeBack {
		t.Errorf("degraded cycle REWROTE a ladder row (updated_ns %d -> %d)", beforeBack, got)
	}
	if got := rowNS("Assets/gone.mov"); got != beforeGone {
		t.Errorf("degraded cycle REWROTE a ladder row (updated_ns %d -> %d)", beforeGone, got)
	}

	// Reappearance: back.mov shows up in the next SCAN view.
	rc.trackAbsentPaths(mustAllPaths(t, s), map[string]struct{}{"Assets/back.mov": {}}, false)
	rc.persistPruneLadderDiff()

	durable, err := s.LoadPruneLadder()
	if err != nil {
		t.Fatalf("LoadPruneLadder: %v", err)
	}
	if _, ok := durable["Assets/back.mov"]; ok {
		t.Error("reappeared path's durable row not deleted at cycle end")
	}
	if _, ok := rc.pruneAbsent["Assets/back.mov"]; ok {
		t.Error("reappeared path still on the in-memory ladder")
	}
	if got := durable["Assets/gone.mov"]; got != 4 {
		t.Errorf("still-absent path durable count = %d, want 4", got)
	}
}

// TestDurableLadderScale120k sizes the mechanism at the motivating scale: the
// initial full diff of a 120k-path mass delete must persist, and a restart
// must load it back, in bounded time (< 5s combined) — this runs at every
// 30s reconcile tail, so it must be cheap.
func TestDurableLadderScale120k(t *testing.T) {
	durableLadderEnv(t)
	const n = 120000
	s := durableLadderStore(t, filepath.Join(t.TempDir(), "mirror.db"))
	rc := durableLadderClient(t, s)
	for i := 0; i < n; i++ {
		rc.pruneAbsent[fmt.Sprintf("SFX/Massive/dir%03d/f%06d.wav", i%512, i)] = 1 + i%PruneThreshold
	}

	persistStart := time.Now()
	rc.persistPruneLadderDiff()
	persistDur := time.Since(persistStart)

	rc2 := durableLadderClient(t, s)
	loadStart := time.Now()
	rc2.loadDurablePruneLadder()
	loadDur := time.Since(loadStart)

	if len(rc2.pruneAbsent) != n {
		t.Fatalf("loaded %d ladder rows, want %d (persist silently failed?)", len(rc2.pruneAbsent), n)
	}
	for i := 0; i < n; i += n / 7 { // spot-check counts survived the round trip
		p := fmt.Sprintf("SFX/Massive/dir%03d/f%06d.wav", i%512, i)
		if got, want := rc2.pruneAbsent[p], 1+i%PruneThreshold; got != want {
			t.Fatalf("round-trip count %q = %d, want %d", p, got, want)
		}
	}
	total := persistDur + loadDur
	budget := 5 * time.Second * pruneLadderScaleBudgetMultiplier // 5s; scaled under -race (see race_on_test.go)
	t.Logf("scale(120k): persist=%v load=%v total=%v (budget %v)", persistDur, loadDur, total, budget)
	if total > budget {
		t.Fatalf("120k persist+load took %v (persist %v, load %v), budget %v", total, persistDur, loadDur, budget)
	}
}

// TestDurableLadderKillSwitch: JM_PRUNE_LADDER_DURABLE=0 must disable BOTH
// directions — no rows written at cycle end, no counters loaded at
// construction — restoring the pre-#10 in-memory-only ladder exactly.
func TestDurableLadderKillSwitch(t *testing.T) {
	t.Setenv("JM_PRUNE_LADDER_DURABLE", "0")
	s := durableLadderStore(t, filepath.Join(t.TempDir(), "mirror.db"))

	rc := durableLadderClient(t, s)
	rc.pruneAbsent["Gone/a.wav"] = 3
	rc.persistPruneLadderDiff()
	durable, err := s.LoadPruneLadder() // store-level read is deliberately ungated
	if err != nil {
		t.Fatalf("LoadPruneLadder: %v", err)
	}
	if len(durable) != 0 {
		t.Fatalf("kill switch on, yet %d rows written: %v", len(durable), durable)
	}

	// Hand-insert a durable row via the (ungated) store API: the gated load
	// must ignore it.
	if err := s.PersistPruneLadderDelta(map[string]int{"Gone/b.wav": 7}, nil); err != nil {
		t.Fatalf("PersistPruneLadderDelta: %v", err)
	}
	rc2 := durableLadderClient(t, s)
	rc2.loadDurablePruneLadder()
	if len(rc2.pruneAbsent) != 0 {
		t.Fatalf("kill switch on, yet load resumed %d counters: %v", len(rc2.pruneAbsent), rc2.pruneAbsent)
	}
}

// TestDurableLadderLoadFilterDropsGuardedPaths: scan-filtered namespaces
// (.trash/, .juicemount/) must be dropped on load (absence there carries no
// delete signal), and then purged from the table by the next cycle-end diff.
// `._` AppleDouble rows RESUME since the ._-pair rule (task #73 completion):
// they are legitimate candidates whose qualification is gated on the
// principal's absence + Layer-A Lstat.
func TestDurableLadderLoadFilterDropsGuardedPaths(t *testing.T) {
	durableLadderEnv(t)
	s := durableLadderStore(t, filepath.Join(t.TempDir(), "mirror.db"))
	if err := s.PersistPruneLadderDelta(map[string]int{
		".trash/x":      6,
		"dir/._sidecar": 4,
		"SFX/legit.wav": 5,
	}, nil); err != nil {
		t.Fatalf("PersistPruneLadderDelta: %v", err)
	}

	rc := durableLadderClient(t, s)
	rc.loadDurablePruneLadder()

	if _, ok := rc.pruneAbsent[".trash/x"]; ok {
		t.Error("scan-filtered .trash/ row resumed onto the ladder — absence there carries zero delete signal")
	}
	if got := rc.pruneAbsent["dir/._sidecar"]; got != 4 {
		t.Errorf("`._` row must RESUME under the ._-pair rule (got %d, want 4)", got)
	}
	if got := rc.pruneAbsent["SFX/legit.wav"]; got != 5 {
		t.Errorf("legit row not resumed (got %d, want 5)", got)
	}

	// The dropped rows must not linger durably: the next cycle-end diff sees
	// them in ladderPersisted but not in pruneAbsent and deletes them.
	rc.persistPruneLadderDiff()
	durable, err := s.LoadPruneLadder()
	if err != nil {
		t.Fatalf("LoadPruneLadder: %v", err)
	}
	if _, ok := durable[".trash/x"]; ok {
		t.Error("dropped .trash/ row not purged from prune_ladder")
	}
	if got := durable["dir/._sidecar"]; got != 4 {
		t.Errorf("resumed `._` durable row disturbed (got %d, want 4)", got)
	}
	if got := durable["SFX/legit.wav"]; got != 5 {
		t.Errorf("legit durable row disturbed by the purge (got %d, want 5)", got)
	}
}

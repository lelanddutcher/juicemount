package metadata

import (
	"os"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// INSTANT-NAV #10 (task #73, second half): the RedisClient half of the
// durable prune ladder. Storage lives in prune_ladder_store.go; this file is
// the load-at-construction and persist-at-cycle-end glue.
//
// CONCURRENCY: both functions run strictly within the pruneAbsent
// single-writer discipline (see the invariant note in syncMetadata).
// loadDurablePruneLadder runs in the constructors, BEFORE Start() spawns the
// reconcile goroutine and before any SyncOnce, so no goroutine can observe
// the map mid-load. persistPruneLadderDiff runs only at syncMetadata's tail,
// on the reconcile goroutine, under syncMu. Neither adds any locking to the
// store's RAM read path (pathCache/childrenIdx/inodeCache untouched).

// pruneLadderDurableEnabled reports whether the durable prune ladder is on.
// Kill switch: JM_PRUNE_LADDER_DURABLE=0 disables BOTH the boot load and the
// cycle-end persist, restoring the pre-#10 in-memory-only ladder exactly (a
// stale prune_ladder table is then simply never read or written). Read per
// call, like JM_PRUNE_FASTPATH / JM_LAYERA_BUDGET. Default ON.
func pruneLadderDurableEnabled() bool {
	return os.Getenv("JM_PRUNE_LADDER_DURABLE") != "0"
}

// loadDurablePruneLadder seeds pruneAbsent (and ladderPersisted, the
// in-memory mirror of the table used for the cycle-end snapshot diff) from
// the persisted prune_ladder table, so a restart RESUMES mass-delete
// convergence instead of resetting every absence counter to zero.
//
// SAFETY — why resuming persisted counts across a restart is safe: a count
// alone never deletes anything. A prune STILL requires, at prune time in the
// CURRENT process, every verification the in-memory ladder required:
//
//   - a live full-SCAN cycle confirming the path is absent from Redis NOW
//     (qualification only happens inside syncMetadata against a fresh diff,
//     and only on a !skipIncrement cycle);
//   - per-path FUSE Lstat verification (Layer A, verifyPruneCandidates) or
//     fast-path subtree-root Lstat confirmation (collectFastPathPrunes);
//   - the G0 FUSE-identity gate (a wedged/absent mount skips the pass);
//   - the pin-store guard (Layer C) and the spool-pending guard (Layer D);
//   - not RecentlyDegraded (a partial Redis view never confirms a delete).
//
// The ladder is a rate limiter, not the authority — resuming its counts only
// removes the restart-reset that kept large ghost sets (120k+ paths under
// deploy/crash churn) from ever converging. It never removes a verification.
//
// Defensive load filter (mirrors the ladder's own guards): rows in
// scan-filtered namespaces (scanFilteredPath: .trash/, .juicemount/ — the
// SCAN can never return them, so absence carries zero delete signal) and `._`
// AppleDouble basenames (tracked but never pruned; qualification drops them)
// are NOT resumed. Dropped rows are still recorded in ladderPersisted so the
// next cycle-end diff deletes them from the table durably — the load itself
// stays read-only (no write on the boot path).
func (rc *RedisClient) loadDurablePruneLadder() {
	if !pruneLadderDurableEnabled() || rc.store == nil {
		return
	}
	loaded, err := rc.store.LoadPruneLadder()
	if err != nil {
		// Fail-open: the ladder starts empty, exactly the pre-#10 restart
		// behavior — convergence restarts but nothing is ever unsafe.
		jmlog.Warn("prune ladder: durable load failed — resuming with empty ladder",
			"error", err.Error())
		return
	}
	if len(loaded) == 0 {
		return
	}
	if rc.pruneAbsent == nil {
		rc.pruneAbsent = make(map[string]int, len(loaded))
	}
	if rc.ladderPersisted == nil {
		rc.ladderPersisted = make(map[string]int, len(loaded))
	}
	dropped, maxCount := 0, 0
	for p, c := range loaded {
		// Mirror the TABLE as-is (including rows the filter drops below):
		// the next cycle-end diff then sees dropped rows in ladderPersisted
		// but not in pruneAbsent and deletes them from the table.
		rc.ladderPersisted[p] = c
		// ._-pair rule (task #73 completion): `._` rows are now legitimate
		// ladder candidates (they prune when their principal is gone), so
		// their persisted progress must survive restarts — only the
		// scan-filtered namespaces stay dropped.
		if scanFilteredPath(p) {
			dropped++
			continue
		}
		rc.pruneAbsent[p] = c
		if c > maxCount {
			maxCount = c
		}
	}
	jmlog.Info("prune ladder: resumed durable absence counters (#10 mass-delete convergence)",
		"loaded", len(loaded)-dropped,
		"dropped", dropped,
		"max_count", maxCount,
	)
}

// persistPruneLadderDiff is the SINGLE persistence call site for the durable
// prune ladder, run at the tail of every completed syncMetadata cycle —
// after ladder increments (trackAbsentPaths), fast-path removals
// (collectFastPathPrunes), qualification drains, Layer-A deferral
// re-insertions (verifyPruneCandidates), and reappearance resets have all
// mutated pruneAbsent. It computes the diff between pruneAbsent and
// ladderPersisted (the in-memory mirror of the table) and applies it in one
// chunked batch.
//
// SNAPSHOT-DIFF, not per-mutation hooks: an O(n) walk of two maps at the 30s
// reconcile cadence is negligible, and it is correct by construction against
// every mutation site — including future ones — because it observes only the
// cycle's final state.
//
// Degraded cycles (skipIncrement / RecentlyDegraded) mutate nothing except
// possible reappearance clears, so the diff is naturally empty or
// deletes-only; an empty diff skips the transaction entirely — a degraded
// cycle can never wipe or rewrite the table. Cycles that exit EARLY on error
// (SCAN failure, DeletePaths failure) skip persistence for that cycle: the
// table lags at most one cycle behind and the next completed cycle's diff
// catches up (upserts are idempotent, deletes of missing rows are no-ops).
//
// Best-effort: a persist error is logged and swallowed — it must never fail
// the sync. ladderPersisted is NOT updated on error, so the next diff
// re-emits whatever didn't land.
func (rc *RedisClient) persistPruneLadderDiff() {
	if !pruneLadderDurableEnabled() || rc.store == nil {
		return
	}
	if rc.ladderPersisted == nil {
		rc.ladderPersisted = make(map[string]int)
	}
	var upserts map[string]int
	for p, c := range rc.pruneAbsent {
		if prev, ok := rc.ladderPersisted[p]; !ok || prev != c {
			if upserts == nil {
				upserts = make(map[string]int)
			}
			upserts[p] = c
		}
	}
	var deletes []string
	for p := range rc.ladderPersisted {
		if _, ok := rc.pruneAbsent[p]; !ok {
			deletes = append(deletes, p)
		}
	}
	if len(upserts) == 0 && len(deletes) == 0 {
		return // no-op diff — never open a tx (degraded/steady-state cycles)
	}
	if err := rc.store.PersistPruneLadderDelta(upserts, deletes); err != nil {
		jmlog.Warn("prune ladder: durable persist failed — will re-diff next cycle",
			"upserts", len(upserts),
			"deletes", len(deletes),
			"error", err.Error(),
		)
		return
	}
	for p, c := range upserts {
		rc.ladderPersisted[p] = c
	}
	for _, p := range deletes {
		delete(rc.ladderPersisted, p)
	}
}

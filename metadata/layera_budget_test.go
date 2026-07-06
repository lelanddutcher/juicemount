package metadata

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// V2.3 G6 (task #79): the prune ladder's Layer-A FUSE Lstat verification must
// be BOUNDED (probe cap + wall budget, same pattern as collectFastPathPrunes)
// and class-gated. Proven live 2026-07-02: ~112k permanent ladder candidates
// (SCAN-coverage bug) drove the unbounded loop to 86s on LAN and ~90 min over
// a cellular relay, saturating the link and pinning "Rebuilding index…".
// Deferred candidates must KEEP their ladder position (re-inserted into
// pruneAbsent at their prior count) — deferral, never a wrong prune and never
// a 10-cycle ladder restart.

// layerAHarness mirrors fastPathHarness: a RedisClient with a real temp-dir
// fuseRoot. Paths NOT created on disk read as Lstat-ENOENT (deleted); created
// ones read as present. Link class is forced to LAN unless a test overrides.
func layerAHarness(t *testing.T, iface string) *RedisClient {
	t.Helper()
	t.Setenv("JM_WAN_MODE", "")
	t.Setenv("JM_LAYERA_BUDGET", "")
	SetClassSignals(func() string { return iface }, nil)
	t.Cleanup(func() { SetClassSignals(nil, nil) })
	return &RedisClient{fuseRoot: t.TempDir(), pruneAbsent: make(map[string]int)}
}

// ladderBacklog builds n absent-on-disk ladder candidates plus their
// prior-count map (as syncMetadata's collection loop would have captured it).
func ladderBacklog(n, count int) (candidates []string, counts map[string]int) {
	counts = make(map[string]int, n)
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("Gone/f%05d.wav", i)
		candidates = append(candidates, p)
		counts[p] = count
	}
	return candidates, counts
}

func TestLayerABudgetDefersUnprobedCandidates(t *testing.T) {
	rc := layerAHarness(t, "en21") // LAN: class gate must NOT fire — only the budget
	const extra = 57
	priorCount := PruneThreshold + 3
	candidates, counts := ladderBacklog(layerAProbeCap+extra, priorCount)

	// One FUSE-PRESENT path inside the probed window: must be spared (not
	// pruned, not deferred) — the QA-30 protection the budget must not weaken.
	present := "Keep/still.mov"
	if err := os.MkdirAll(filepath.Join(rc.fuseRoot, "Keep"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rc.fuseRoot, present), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	candidates[0] = present
	delete(counts, "Gone/f00000.wav")
	counts[present] = priorCount

	verified, fusePresent := rc.verifyPruneCandidates(append([]string(nil), candidates...), nil, counts)

	if fusePresent != 1 {
		t.Errorf("fusePresent = %d, want 1 (the on-disk file must be spared)", fusePresent)
	}
	if len(verified) > layerAProbeCap {
		t.Errorf("verified %d candidates > layerAProbeCap %d — budget did not bound the probe loop", len(verified), layerAProbeCap)
	}
	if len(rc.pruneAbsent) < extra {
		t.Errorf("deferred %d < %d — beyond-cap candidates were not deferred", len(rc.pruneAbsent), extra)
	}
	// Conservation: every candidate is exactly one of verified (probed-absent),
	// spared (FUSE-present), or deferred (unprobed, back on the ladder).
	if got := len(verified) + fusePresent + len(rc.pruneAbsent); got != len(candidates) {
		t.Errorf("verified(%d) + spared(%d) + deferred(%d) = %d, want %d — candidates lost or double-counted",
			len(verified), fusePresent, len(rc.pruneAbsent), got, len(candidates))
	}
	verifiedSet := make(map[string]struct{}, len(verified))
	for _, p := range verified {
		verifiedSet[p] = struct{}{}
	}
	if _, ok := verifiedSet[present]; ok {
		t.Error("FUSE-PRESENT path pruned — this deletes live media from the mirror")
	}
	for p, c := range rc.pruneAbsent {
		if _, alsoPruned := verifiedSet[p]; alsoPruned {
			t.Errorf("deferred candidate %q ALSO in the prune set — deferral must mean not-pruned-this-cycle", p)
		}
		if c != priorCount {
			t.Errorf("deferred candidate %q re-inserted at count %d, want prior count %d — ladder position lost", p, c, priorCount)
		}
	}
}

func TestLayerAKillSwitchRestoresUnbounded(t *testing.T) {
	// Tunnel class + kill switch: JM_LAYERA_BUDGET=0 must restore today's
	// behavior in full — no probe cap, no wall budget, no class gate.
	rc := layerAHarness(t, "utun4")
	t.Setenv("JM_LAYERA_BUDGET", "0")
	candidates, counts := ladderBacklog(layerAProbeCap+57, PruneThreshold+1)

	verified, fusePresent := rc.verifyPruneCandidates(append([]string(nil), candidates...), nil, counts)

	if len(verified) != len(candidates) {
		t.Errorf("verified %d of %d — kill switch must disable the cap and the class gate", len(verified), len(candidates))
	}
	if fusePresent != 0 {
		t.Errorf("fusePresent = %d, want 0", fusePresent)
	}
	if len(rc.pruneAbsent) != 0 {
		t.Errorf("%d candidates deferred under the kill switch — must be unbounded", len(rc.pruneAbsent))
	}
}

func TestLayerAMeteredClassDefersLadderButNotFastPath(t *testing.T) {
	rc := layerAHarness(t, "utun4") // tunnel/cellular band
	t.Setenv("JM_PRUNE_FASTPATH", "")

	// Ladder candidates (each Lstat = a metered-link round-trip): defer ALL.
	counts := map[string]int{
		"Gone/a.wav": PruneThreshold,
		"Gone/b.wav": PruneThreshold + 5,
		"Gone/c.wav": PruneThreshold + 1,
	}
	// Fast-path entries were root-verified by collectFastPathPrunes this same
	// cycle — they skip Layer A and must still prune, gate or no gate.
	fastConfirmed := map[string]struct{}{
		"Trees/Gone":       {},
		"Trees/Gone/x.mov": {},
	}
	toDelete := []string{"Gone/a.wav", "Trees/Gone", "Gone/b.wav", "Trees/Gone/x.mov", "Gone/c.wav"}

	verified, fusePresent := rc.verifyPruneCandidates(toDelete, fastConfirmed, counts)

	if len(verified) != len(fastConfirmed) {
		t.Fatalf("verified = %v, want exactly the %d fast-confirmed entries", verified, len(fastConfirmed))
	}
	for _, p := range verified {
		if _, ok := fastConfirmed[p]; !ok {
			t.Errorf("ladder candidate %q pruned UNVERIFIED on a metered link — deferral is the fail-safe direction", p)
		}
	}
	if fusePresent != 0 {
		t.Errorf("fusePresent = %d, want 0 (no ladder probes may run on the metered band)", fusePresent)
	}
	for p, want := range counts {
		if got, ok := rc.pruneAbsent[p]; !ok || got != want {
			t.Errorf("ladder candidate %q not deferred at prior count (got %d,%v want %d) — position lost", p, got, ok, want)
		}
	}

	// The class gate must not leak into the G5 fast-path: a genuinely deleted
	// subtree still root-confirms on the tunnel band (its handful of root
	// Lstats is exactly the cheap alternative the gate preserves).
	rc2 := &RedisClient{fuseRoot: t.TempDir(), pruneAbsent: map[string]int{
		"Assets/Gone":       1,
		"Assets/Gone/a.wav": 1,
	}}
	confirmed, _ := rc2.collectFastPathPrunes(false)
	for _, want := range []string{"Assets/Gone", "Assets/Gone/a.wav"} {
		if _, ok := confirmed[want]; !ok {
			t.Errorf("fast-path stopped confirming %q under the tunnel class — G6 gate must not touch G5", want)
		}
	}
}

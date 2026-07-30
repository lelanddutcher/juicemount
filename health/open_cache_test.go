package health

import "testing"

// W2. The measured prize: first open of a file costs 1,226-5,660ms on cellular
// while the second costs 24-36ms (~66x), because juicefs re-validates against
// Redis on every open — `--open-cache` defaults to 0s and we never passed it.
//
// It is OPT-IN, not on by default, for two reasons that are easy to lose:
//   1. juicefs exposes NO external cache invalidation (verified against the
//      whole subcommand surface), so the TTL is a HARD staleness bound.
//   2. The exposure is a stale slice map = WRONG BYTES, the family behind #18
//      torn reads, #104 black frames and the membuf stale-partial image — not
//      the "new files appear late" trade that was accepted for slow links.
//
// If you are here because you want it on by default: get a measurement on a
// real link AND a multi-writer (farm + Mac) staleness test first.
func TestOpenCacheOffByDefault(t *testing.T) {
	t.Setenv("JM_OPEN_CACHE", "")
	if got := openCacheTTL(); got != "" {
		t.Fatalf("openCacheTTL() = %q with the knob unset, want \"\" — enabling a "+
			"hard, un-invalidatable staleness window must be an explicit choice", got)
	}
}

func TestOpenCacheExplicitZeroIsOff(t *testing.T) {
	for _, v := range []string{"0", "0s"} {
		t.Setenv("JM_OPEN_CACHE", v)
		if got := openCacheTTL(); got != "" {
			t.Errorf("JM_OPEN_CACHE=%q gave %q, want \"\" (off)", v, got)
		}
	}
}

func TestOpenCacheEnabledPassesThrough(t *testing.T) {
	t.Setenv("JM_OPEN_CACHE", "5s")
	if got := openCacheTTL(); got != "5s" {
		t.Errorf("openCacheTTL() = %q, want 5s", got)
	}
}

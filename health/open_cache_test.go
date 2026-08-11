package health

import "testing"

// W2. The measured prize: first open of a file costs 1,226-5,660ms on cellular
// while the second costs 24-36ms (~66x), because juicefs re-validates against
// Redis on every open — `--open-cache` defaults to 0s and we never passed it.
//
// It is OPT-IN, not on by default, for two reasons that are easy to lose:
//  1. juicefs exposes NO external cache invalidation (verified against the
//     whole subcommand surface), so the TTL is a HARD staleness bound.
//  2. The exposure is a stale slice map = WRONG BYTES, the family behind #18
//     torn reads, #104 black frames and the membuf stale-partial image — not
//     the "new files appear late" trade that was accepted for slow links.
//
// If you are here because you want it on by default: get a measurement on a
// real link AND a multi-writer (farm + Mac) staleness test first.
func TestOpenCacheOffByDefault(t *testing.T) {
	t.Setenv("JM_OPEN_CACHE", "")
	fm := &FUSEManager{}
	if got := fm.openCacheTTL(); got != "" {
		t.Fatalf("openCacheTTL() = %q with the knob unset, want \"\" — enabling a "+
			"hard, un-invalidatable staleness window must be an explicit choice", got)
	}
}

func TestOpenCacheExplicitZeroIsOff(t *testing.T) {
	fm := &FUSEManager{}
	for _, v := range []string{"0", "0s"} {
		t.Setenv("JM_OPEN_CACHE", v)
		if got := fm.openCacheTTL(); got != "" {
			t.Errorf("JM_OPEN_CACHE=%q gave %q, want \"\" (off)", v, got)
		}
	}
}

func TestOpenCacheEnabledPassesThrough(t *testing.T) {
	t.Setenv("JM_OPEN_CACHE", "5s")
	fm := &FUSEManager{}
	if got := fm.openCacheTTL(); got != "5s" {
		t.Errorf("openCacheTTL() = %q, want 5s", got)
	}
}

// THE ACTUATION TEST. Every JM_* knob in fuse.go is unreachable from a shipped
// app — Go snapshots os.Environ at c-archive init, so a Swift setenv() after
// that is invisible (ServerController.start says so in a comment). Knobs whose
// default is the wanted behaviour survive that; --open-cache does not, because
// its default is OFF and the 66x cellular win therefore could not be switched
// on by the person running the test. The config channel is the only one that
// reaches a shipped app, so it MUST work and it MUST outrank the env.
func TestOpenCacheIsSettableFromConfigNotJustEnv(t *testing.T) {
	t.Setenv("JM_OPEN_CACHE", "") // env unset, as in a shipped app
	fm := &FUSEManager{cfg: FUSEConfig{OpenCacheTTL: "5s"}}
	if got := fm.openCacheTTL(); got != "5s" {
		t.Fatalf("config OpenCacheTTL=5s gave %q — the lever is unreachable from a "+
			"shipped app, which is how JM_WAN_MODE silently never fired", got)
	}
}

func TestOpenCacheConfigOutranksEnv(t *testing.T) {
	t.Setenv("JM_OPEN_CACHE", "30s")
	fm := &FUSEManager{cfg: FUSEConfig{OpenCacheTTL: "5s"}}
	if got := fm.openCacheTTL(); got != "5s" {
		t.Errorf("got %q, want the CONFIG value 5s — config is the shipped-app "+
			"channel and must win", got)
	}
}

func TestOpenCacheExplicitZeroInConfigIsOff(t *testing.T) {
	t.Setenv("JM_OPEN_CACHE", "")
	for _, v := range []string{"0", "0s", ""} {
		fm := &FUSEManager{cfg: FUSEConfig{OpenCacheTTL: v}}
		if got := fm.openCacheTTL(); got != "" {
			t.Errorf("config %q gave %q, want off", v, got)
		}
	}
}

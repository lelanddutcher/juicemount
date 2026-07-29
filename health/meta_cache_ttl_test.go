package health

import "testing"

// The navigation fix, pinned.
//
// The user-visible bug: navigating a directory is instant while OFFLINE and
// terrible on cellular while ONLINE. Root cause was that all four juicefs FUSE
// metadata caches ran at 5s, so every directory entry re-validated against
// Redis every 5 seconds — ~0.4ms on a LAN, ~250ms per round trip on cellular.
//
// A 60s WAN value was already written, gated on JM_WAN_MODE=1 — a variable read
// in six production paths and set in NOTHING but tests. It shipped dead. There
// was no test asserting the effective values, which is exactly why nobody
// noticed. This file is that test.
//
// If you are here because this test failed: do not "fix" it by relaxing the
// assertion. Entry/dir-entry staleness only delays a NEWLY ADDED file becoming
// visible, a trade the user explicitly accepted for throttled links. attr is
// deliberately SHORTER because it backs LiveSize(), the anti-staleness
// authority for #85 (verified-offload discarding a good file after reading a
// transient short size).
func TestMetaCacheTTLDefaults(t *testing.T) {
	for _, k := range []string{
		"JM_META_CACHE_SECS", "JM_META_ENTRY_CACHE",
		"JM_META_DIR_ENTRY_CACHE", "JM_META_ATTR_CACHE", "JM_META_NEGATIVE_CACHE",
	} {
		t.Setenv(k, "")
	}

	attr, entry, dirEntry, negative := metaCacheTTLs()

	if entry != "60s" {
		t.Errorf("entry-cache = %q, want 60s — this is THE navigation fix; a short value here is the cellular bug", entry)
	}
	if dirEntry != "60s" {
		t.Errorf("dir-entry-cache = %q, want 60s", dirEntry)
	}
	if attr != "10s" {
		t.Errorf("attr-cache = %q, want 10s — must stay SHORT, it backs LiveSize()/#85", attr)
	}
	if negative != "5s" {
		t.Errorf("negative-entry-cache = %q, want 5s — a stale negative reads as 'my new file is missing'", negative)
	}

	// The regression that actually shipped: all four equal and short.
	if attr == entry && entry == dirEntry && dirEntry == negative {
		t.Errorf("all four TTLs collapsed to %q — that is the pre-fix single-scalar behavior", attr)
	}
}

// JM_WAN_MODE must no longer influence these values. It was the dead gate that
// hid the fix; leaving it wired would let the bug return for anyone who happens
// not to set it (i.e. everyone).
func TestMetaCacheTTLIgnoresDeadWANModeKnob(t *testing.T) {
	for _, k := range []string{
		"JM_META_CACHE_SECS", "JM_META_ENTRY_CACHE",
		"JM_META_DIR_ENTRY_CACHE", "JM_META_ATTR_CACHE", "JM_META_NEGATIVE_CACHE",
	} {
		t.Setenv(k, "")
	}

	t.Setenv("JM_WAN_MODE", "")
	offAttr, offEntry, offDir, offNeg := metaCacheTTLs()
	t.Setenv("JM_WAN_MODE", "1")
	onAttr, onEntry, onDir, onNeg := metaCacheTTLs()

	if offAttr != onAttr || offEntry != onEntry || offDir != onDir || offNeg != onNeg {
		t.Errorf("JM_WAN_MODE still changes the metadata TTLs (off=%s/%s/%s/%s on=%s/%s/%s/%s); "+
			"the fix must not depend on a knob nothing sets",
			offAttr, offEntry, offDir, offNeg, onAttr, onEntry, onDir, onNeg)
	}
}

// The documented escape hatch still collapses all four, for operators who need
// to force short TTLs (e.g. debugging a staleness complaint).
func TestMetaCacheTTLGlobalOverride(t *testing.T) {
	t.Setenv("JM_META_ENTRY_CACHE", "")
	t.Setenv("JM_META_DIR_ENTRY_CACHE", "")
	t.Setenv("JM_META_ATTR_CACHE", "")
	t.Setenv("JM_META_NEGATIVE_CACHE", "")
	t.Setenv("JM_META_CACHE_SECS", "2s")

	attr, entry, dirEntry, negative := metaCacheTTLs()
	if attr != "2s" || entry != "2s" || dirEntry != "2s" || negative != "2s" {
		t.Errorf("JM_META_CACHE_SECS did not override all four: got %s/%s/%s/%s",
			attr, entry, dirEntry, negative)
	}
}

// Per-cache overrides win over the defaults and are independent of each other.
func TestMetaCacheTTLPerCacheOverrides(t *testing.T) {
	t.Setenv("JM_META_CACHE_SECS", "")
	t.Setenv("JM_META_ENTRY_CACHE", "120s")
	t.Setenv("JM_META_DIR_ENTRY_CACHE", "")
	t.Setenv("JM_META_ATTR_CACHE", "")
	t.Setenv("JM_META_NEGATIVE_CACHE", "")

	attr, entry, dirEntry, negative := metaCacheTTLs()
	if entry != "120s" {
		t.Errorf("entry override ignored: %q", entry)
	}
	if dirEntry != "60s" || attr != "10s" || negative != "5s" {
		t.Errorf("one override leaked into the others: %s/%s/%s", dirEntry, attr, negative)
	}
}

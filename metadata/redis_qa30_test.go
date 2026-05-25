package metadata

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// QA-30 path-normalization regression: pin store keys are mountpoint-
// prefixed ("/Volumes/zpool/foo"); metadata store keys are internal
// ("foo"). Forgetting to normalize meant the filter was a no-op in
// production even though unit tests with matching paths passed.
func TestInternalFromMounted(t *testing.T) {
	rc := &RedisClient{}
	rc.SetPathConfig("/Volumes/zpool", "/Users/x/.juicemount/fuse-internal")

	cases := []struct{ in, want string }{
		{"/Volumes/zpool/Film Projects/X.MP4", "Film Projects/X.MP4"},
		{"/Volumes/zpool", ""},
		{"/Volumes/zpool/", ""},                       // trailing slash on mp; arrives as just mp
		{"/Volumes/something-else/y", "/Volumes/something-else/y"}, // no prefix → unchanged
		{"already-internal/x", "already-internal/x"},
	}
	// Normalize the trailing-slash case: TrimRight on the rhs side handles it.
	for _, c := range cases {
		if got := rc.internalFromMounted(c.in); got != c.want {
			// Special case for /Volumes/zpool/ trailing slash: with rc.mountPoint
			// stored as "/Volumes/zpool" (TrimRight applied), "/Volumes/zpool/"
			// matches the prefix and trims to "".
			if c.in == "/Volumes/zpool/" && got == "" {
				continue
			}
			t.Errorf("internalFromMounted(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// QA-30 path-normalization regression: with the mountPoint unset, the
// helper must NOT mangle paths (safe fallback for tests/old configs).
func TestInternalFromMounted_NoMountpointConfigured(t *testing.T) {
	rc := &RedisClient{}
	// no SetPathConfig — fields are empty
	if got := rc.internalFromMounted("/Volumes/zpool/foo"); got != "/Volumes/zpool/foo" {
		t.Errorf("unconfigured rc should pass paths through unchanged, got %q", got)
	}
}

// QA-30 Layer A helper: fusePathFor returns absolute FUSE-mount paths
// for syncMetadata's Lstat verification step.
func TestFusePathFor(t *testing.T) {
	rc := &RedisClient{}
	rc.SetPathConfig("/Volumes/zpool", "/Users/x/.juicemount/fuse-internal")

	cases := []struct{ in, want string }{
		{"Film Projects/X.MP4", "/Users/x/.juicemount/fuse-internal/Film Projects/X.MP4"},
		{"", "/Users/x/.juicemount/fuse-internal"},
		{"/leading/slash", "/Users/x/.juicemount/fuse-internal/leading/slash"},
	}
	for _, c := range cases {
		if got := rc.fusePathFor(c.in); got != c.want {
			t.Errorf("fusePathFor(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestFusePathFor_UnconfiguredReturnsEmpty(t *testing.T) {
	rc := &RedisClient{}
	if got := rc.fusePathFor("foo"); got != "" {
		t.Errorf("unconfigured rc should return empty fuse path, got %q", got)
	}
}

// QA-30 Layer A: lstatNotExistWithTimeout must report file-present
// correctly and bound its wall-clock on a stuck FS.
func TestLstatNotExistWithTimeout_PresentFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "exists.txt")
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	isAbsent, ok := lstatNotExistWithTimeout(p, time.Second)
	if !ok {
		t.Fatal("Lstat on a real file should not time out")
	}
	if isAbsent {
		t.Error("file exists, isAbsent should be false")
	}
}

func TestLstatNotExistWithTimeout_AbsentFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "does-not-exist.txt")
	isAbsent, ok := lstatNotExistWithTimeout(p, time.Second)
	if !ok {
		t.Fatal("Lstat on a missing file should not time out")
	}
	if !isAbsent {
		t.Error("absent file should report isAbsent=true")
	}
}

// QA-30 Layer A timeout-on-FUSE-wedge: use the lstatFn injection point
// (QA-30 code review HIGH-2 mitigation) to simulate a wedged FUSE
// deterministically. The original test relied on race conditions between
// time.After(0) and a sub-microsecond Lstat, which was flaky on fast
// hardware.
func TestLstatNotExistWithTimeout_TimesOut(t *testing.T) {
	// Inject a blocker that never returns within the test's lifetime.
	// The released channel lets the goroutine exit when the test ends,
	// so we don't actually leak a goroutine across tests.
	release := make(chan struct{})
	old := setLstatFn(func(string) (os.FileInfo, error) {
		<-release
		return nil, os.ErrNotExist
	})
	t.Cleanup(func() {
		close(release)
		setLstatFn(old)
	})

	// 50ms deadline is generous enough to clear normal scheduler
	// jitter (the test will block ~50ms, then return ok=false).
	_, ok := lstatNotExistWithTimeout("/whatever", 50*time.Millisecond)
	if ok {
		t.Error("blocked Lstat should return ok=false (timed out)")
	}
}

// QA-30 code review HIGH-1 validation: the bounded gate caps concurrent
// in-flight Lstats. With gate size = 8 and 16 simultaneous blocked
// requests, the 9th+ caller should bail on gate acquisition within its
// own timeout budget instead of queuing forever.
func TestLstatNotExistWithTimeout_GateBounded(t *testing.T) {
	release := make(chan struct{})
	old := setLstatFn(func(string) (os.FileInfo, error) {
		<-release
		return nil, os.ErrNotExist
	})
	t.Cleanup(func() {
		close(release)
		setLstatFn(old)
	})

	// Saturate the gate with 8 in-flight blocked Lstats. Each one will
	// time out after 1s, but until then they hold their gate slots.
	for i := 0; i < 8; i++ {
		go func() { _, _ = lstatNotExistWithTimeout("/blocked", 5*time.Second) }()
	}
	// Give the goroutines a moment to acquire their slots.
	time.Sleep(50 * time.Millisecond)

	// 9th caller: should bail on gate acquisition within 100ms.
	start := time.Now()
	_, ok := lstatNotExistWithTimeout("/blocked-9th", 100*time.Millisecond)
	elapsed := time.Since(start)
	if ok {
		t.Error("9th caller should return ok=false when gate is saturated")
	}
	if elapsed > 250*time.Millisecond {
		t.Errorf("9th caller should bail near its timeout (100ms), not wait for a slot to free; took %v", elapsed)
	}
}

// QA-30 path-format integration: simulate the production scenario where
// a pinned file's metadata path (internal form) and pin store path
// (mountpoint-prefixed) need to round-trip cleanly through the filter.
func TestQA30_PathNormalizationRoundTrip(t *testing.T) {
	rc := &RedisClient{}
	rc.SetPathConfig("/Volumes/zpool", "/dummy")

	// Simulate: pin store has these mounted paths
	pinnedMounted := []string{
		"/Volumes/zpool/Film Projects/A.MP4",
		"/Volumes/zpool/Other/B.mov",
	}
	// Metadata store has these internal paths and one is a pinned file
	candidates := []string{
		"Film Projects/A.MP4", // pinned — must be kept
		"junk/stale.tmp",      // not pinned — eligible for prune
		"Other/B.mov",         // pinned — must be kept
	}

	pinnedInternal := make(map[string]struct{}, len(pinnedMounted))
	for _, mp := range pinnedMounted {
		pinnedInternal[rc.internalFromMounted(mp)] = struct{}{}
	}

	var kept, pruned []string
	for _, p := range candidates {
		if _, isPinned := pinnedInternal[p]; isPinned {
			kept = append(kept, p)
		} else {
			pruned = append(pruned, p)
		}
	}

	if len(kept) != 2 {
		t.Errorf("expected 2 pinned files kept, got %d: %v", len(kept), kept)
	}
	if len(pruned) != 1 || pruned[0] != "junk/stale.tmp" {
		t.Errorf("expected only junk/stale.tmp pruned, got %v", pruned)
	}
	// Sanity: spell out the round-trip the production code does
	for _, mp := range pinnedMounted {
		internal := rc.internalFromMounted(mp)
		if strings.HasPrefix(internal, "/Volumes/zpool") {
			t.Errorf("internalFromMounted didn't strip prefix from %q -> %q", mp, internal)
		}
	}
}

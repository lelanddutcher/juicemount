package farm

import "testing"

const mib = 1 << 20

func TestExcludeReason(t *testing.T) {
	subs := skipDirSubstrings()
	cases := []struct {
		name string
		path string
		size int64
		minB int64
		want string
	}{
		// Proxy — either spelling, parent folder OR filename, any case.
		{"proxy-folder", "Footage/Proxy/clip.mov", 500 * mib, 20 * mib, "proxy"},
		{"proxies-folder", "Footage/Proxies/clip.mov", 500 * mib, 20 * mib, "proxy"},
		{"proxy-in-name", "Footage/clip_proxy.mov", 500 * mib, 20 * mib, "proxy"},
		{"proxy-upper", "Footage/PROXY/clip.mov", 500 * mib, 20 * mib, "proxy"},
		{"proxies-mixed", "A001/Proxies/x.mxf", 500 * mib, 20 * mib, "proxy"},
		// Proxy wins even when also too small (proxy is the reported reason).
		{"proxy-beats-small", "Proxy/tiny.mov", 1 * mib, 20 * mib, "proxy"},

		// Too-small — under the floor, not a proxy, not a skip-dir.
		{"too-small", "Footage/A001/clip.mov", 1 * mib, 20 * mib, "too-small"},
		{"exactly-floor-ok", "Footage/A001/clip.mov", 20 * mib, 20 * mib, ""},
		{"above-floor-ok", "Footage/A001/clip.mov", 21 * mib, 20 * mib, ""},
		{"size-unknown-passes", "Footage/A001/clip.mov", -1, 20 * mib, ""},
		{"floor-disabled", "Footage/A001/clip.mov", 1 * mib, 0, ""},

		// Skip-dir defaults (NLE ephemeral).
		{"media-cache", "Adobe/Media Cache Files/x.pek", 500 * mib, 20 * mib, "skip-dir:media cache"},
		{"peak-files", "Audio/Peak Files/x.pkf", 500 * mib, 20 * mib, "skip-dir:peak files"},
		{"farm-benchmark", "__bench_encode_42/sample.mp4", 500 * mib, 20 * mib, "skip-dir:__bench_"},

		// Clean content passes.
		{"clean", "Footage/A001/A001_C001.braw", 4000 * mib, 20 * mib, ""},
		// Guard against over-matching "proxy": these contain "proxi" but not
		// "proxy"/"proxies" and must NOT be excluded.
		{"proximity-ok", "Footage/proximity_test.mov", 500 * mib, 20 * mib, ""},
		{"approximately-ok", "Footage/approximately_final.mov", 500 * mib, 20 * mib, ""},
	}
	for _, c := range cases {
		if got := ExcludeReason(c.path, c.size, c.minB, subs); got != c.want {
			t.Errorf("%s: ExcludeReason(%q, %d, %d) = %q, want %q", c.name, c.path, c.size, c.minB, got, c.want)
		}
	}
}

func TestDirIsExcluded(t *testing.T) {
	cases := map[string]bool{
		"Footage/Proxy":        true,
		"Footage/Proxies":      true,
		"a/b/PROXY":            true,
		"Adobe/Media Cache":    true,
		"Audio/Peak Files":     true,
		"__bench_decode_17":    true,
		"Footage/A001":         false,
		"Projects/Client Work": false,
		"Footage/proximity":    false, // "proxi" but not proxy/proxies
	}
	for p, want := range cases {
		if got := DirIsExcluded(p); got != want {
			t.Errorf("DirIsExcluded(%q) = %v, want %v", p, got, want)
		}
	}
}

func TestSkipDirSubstringsEnv(t *testing.T) {
	// Defaults always present.
	base := skipDirSubstrings()
	if !contains(base, "proxy") || !contains(base, "peak files") {
		t.Fatalf("defaults missing: %v", base)
	}
	// Env extends (case-normalized, trimmed) and takes effect in the matcher.
	t.Setenv("JM_FARM_SKIP_DIRS", " Renders , Dailies ")
	ext := skipDirSubstrings()
	if !contains(ext, "renders") || !contains(ext, "dailies") {
		t.Fatalf("env dirs not added: %v", ext)
	}
	if r := ExcludeReason("Project/Renders/out.mov", 500*mib, 20*mib, ext); r != "skip-dir:renders" {
		t.Fatalf("env skip-dir not honored: %q", r)
	}
	if !DirIsExcluded("Project/Dailies") {
		t.Fatal("env skip-dir not honored by DirIsExcluded")
	}
}

// WatchPathAllowed must honor the same exclusion policy as collectTargets, so
// the live watcher never enqueues proxy / NLE-cache paths a sweep would skip.
func TestWatchPathAllowedExclusion(t *testing.T) {
	cases := map[string]bool{
		"Footage/A001/clip.braw":       true,  // clean content
		"Footage/Proxies/clip.mov":     false, // proxy folder
		"Footage/clip_proxy.mov":       false, // proxy in filename
		"Adobe/Media Cache Files/x":    false, // NLE cache dir
		"Audio/Peak Files/x.pkf":       false, // peak files
		"__bench_access_17/sample.mp4": false, // farm instrumentation
		"Footage/proximity_reel.mov":   true,  // not a proxy (no over-match)
	}
	for rel, want := range cases {
		if got := WatchPathAllowed(rel); got != want {
			t.Errorf("WatchPathAllowed(%q) = %v, want %v", rel, got, want)
		}
	}
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

package farm

import (
	"path/filepath"
	"testing"
)

// An in-flight STREAMED destination must never become a derivative target.
//
// The streaming spool drain writes a large file to a hidden sibling and renames
// it into place only when complete. Mid-copy that partial is a real, walkable
// file of substantial size sitting next to real footage.
//
// collectTargets does not otherwise catch it: its dot-prefix rule lives inside
// `if info.IsDir()` so it skips dot-DIRECTORIES only, the sole file-name rule is
// the "._" AppleDouble prefix, and a partial is far too big for min-size. So
// without this exclusion the farm generates a proxy or thumbnail from truncated
// bytes — black frames, arriving through the farm rather than through a read.
func TestExcludeReasonSkipsStreamedPartials(t *testing.T) {
	cases := []string{
		"/vol/DCIM/A001/.juicemount-streaming-42-clip.mov",
		"/vol/.juicemount-streaming-1-a.mov",
		filepath.Join("relative", "dir", ".juicemount-streaming-7-BIG.MOV"),
	}
	for _, p := range cases {
		// Large and otherwise perfectly eligible: only the name may exclude it.
		if got := ExcludeReason(p, 40<<30, 20<<20, nil); got != "streaming-partial" {
			t.Errorf("ExcludeReason(%q) = %q, want \"streaming-partial\" — the farm "+
				"would derive a proxy from a half-written file", p, got)
		}
	}
}

// The exclusion must be NARROW. Excluding real footage would silently stop
// derivative generation for ordinary media — quieter and worse than the bug.
func TestExcludeReasonKeepsOrdinaryMedia(t *testing.T) {
	cases := []string{
		"/vol/DCIM/A001/clip.mov",
		"/vol/DCIM/A001/juicemount-streaming-42-clip.mov", // no leading dot
		"/vol/DCIM/A001/.hidden.mov",                      // hidden, but not a partial
		"/vol/my.juicemount-streaming-notes.mov",          // prefix not at the base start
	}
	for _, p := range cases {
		if got := ExcludeReason(p, 40<<30, 20<<20, nil); got == "streaming-partial" {
			t.Errorf("ExcludeReason(%q) excluded ordinary media as a streamed "+
				"partial — real footage would silently stop getting derivatives", p)
		}
	}
}

// The prefix is duplicated across three packages that cannot import each other
// (nfs writes the partial, metadata keeps it out of the mirror, farm keeps it
// out of derivative generation). If they drift, each package keeps working in
// isolation while partials leak into whichever consumer fell behind. This pins
// the farm's copy to the literal the other two use.
func TestStreamPartialPrefixIsConsistent(t *testing.T) {
	const canonical = ".juicemount-streaming-"
	if streamPartialPrefix != canonical {
		t.Fatalf("farm streamPartialPrefix = %q, want %q — nfs writes partials with "+
			"the canonical prefix, so a divergence here means the farm resumes "+
			"deriving proxies from half-written files", streamPartialPrefix, canonical)
	}
}

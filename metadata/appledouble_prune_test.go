package metadata

import (
	"testing"
)

// TestAppleDoublePrincipal pins the sidecar→principal path mapping.
func TestAppleDoublePrincipal(t *testing.T) {
	cases := map[string]string{
		"a/b/._clip.mov": "a/b/clip.mov",
		"._top.txt":      "top.txt",
		"deep/x/._y":     "deep/x/y",
		"a/._":           "a/", // degenerate name; Layer-A Lstat still protects
	}
	for in, want := range cases {
		if got := appleDoublePrincipal(in); got != want {
			t.Errorf("appleDoublePrincipal(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestAppleDoubleQualifies pins the ._-pair rule's decision matrix.
func TestAppleDoubleQualifies(t *testing.T) {
	live := map[string]*Entry{
		"proj/clip.mov": {Path: "proj/clip.mov", Inode: 1},
	}
	lookup := func(p string) *Entry { return live[p] }

	// Principal PRESENT → sidecar row is a live pair → never qualifies.
	if appleDoubleQualifies("proj/._clip.mov", lookup) {
		t.Fatal("sidecar with a LIVE principal qualified for pruning")
	}
	// Principal ABSENT → residue → qualifies (downstream gates still apply).
	if !appleDoubleQualifies("proj/._gone.mov", lookup) {
		t.Fatal("sidecar of a deleted principal did not qualify")
	}
	// Kill switch restores the blanket refusal.
	t.Setenv("JM_PRUNE_APPLEDOUBLE", "0")
	if appleDoubleQualifies("proj/._gone.mov", lookup) {
		t.Fatal("kill switch did not disable the ._-pair rule")
	}
}

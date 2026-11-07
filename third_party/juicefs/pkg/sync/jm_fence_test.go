/*
 * JuiceMount local source (not upstream JuiceFS). Apache-2.0.
 */

package sync

import "testing"

func TestJMLinkLeavesProject(t *testing.T) {
	cases := []struct {
		key, target string
		depth       int
		leaves      bool
	}{
		// The root holds the projects (depth 1).
		{"A/l", "x", 1, false},
		{"A/l", "../B/x", 1, true},
		{"A/l", "../A/x", 1, true}, // names its own project only through its parent
		{"A/s/l", "../x", 1, false},
		{"A/s/l", "../../B", 1, true},
		{"A/s/l", "./.././x", 1, false},
		{"A/s/l", "a/../../..", 1, true},
		// ".." after a name: the name may be a link (y -> .).
		{"A/x", "y/../B", 1, true},
		{"A/s/x", "y/../z", 1, true},
		{"A/s/x", "../../A/z", 1, true},
		{"A/s/x", "../z/./w", 1, false},
		{"l", "A/x", 1, true}, // stored above the project level
		{"A/l", "/abs/path", 1, false},
		// The root is the project (depth 0) or inside it (depth -1).
		{"l", "x", 0, false},
		{"l", "../x", 0, true},
		{"l", "../x", -1, false},
		{"l", "../../x", -1, true},
		{"s/l", "../../x", -1, false},
		// Two levels down (depth 2, the volume root).
		{"data/A/l", "../B", 2, true},
		{"data/A/l", "x/y", 2, false},
		{"data/l", "A", 2, true},
	}
	for _, c := range cases {
		if got := jmLinkLeavesProject(c.key, c.target, c.depth); got != c.leaves {
			t.Errorf("jmLinkLeavesProject(%q, %q, %d) = %v, want %v", c.key, c.target, c.depth, got, c.leaves)
		}
	}
}

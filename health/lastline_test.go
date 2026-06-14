package health

import "testing"

// TestLastNonEmptyLine guards the stderr-reason extraction used to turn a bare
// "juicefs mount: exit status 1" into a self-diagnosing error (2026-06-14).
func TestLastNonEmptyLine(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"\n\n  \n", ""},
		{"only line", "only line"},
		{"info: starting\nfatal: mountpoint is not empty\n", "fatal: mountpoint is not empty"},
		// trailing blank lines are skipped to reach the real message
		{"2024 mount\nerror: address already in use\n\n\n", "error: address already in use"},
		{"  padded reason  ", "padded reason"},
	}
	for _, c := range cases {
		if got := lastNonEmptyLine(c.in); got != c.want {
			t.Errorf("lastNonEmptyLine(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

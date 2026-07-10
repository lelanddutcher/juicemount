package manager

import (
	"reflect"
	"testing"
)

// TestChownArgs pins the guard that decides whether a post-migration chown
// runs at all — the correctness-critical part of the permissions fix. It must
// NEVER chown to uid 0 (root-owned is exactly the bug being fixed) and must
// skip cleanly on the zero-value / empty target so a mis-wired spec can't
// accidentally re-root a migrated tree.
func TestChownArgs(t *testing.T) {
	cases := []struct {
		name     string
		uid, gid int
		target   string
		want     []string
	}{
		{"zero-value uid → skip (unset)", 0, 20, "/mnt/x", nil},
		{"negative uid → skip", -1, -1, "/mnt/x", nil},
		{"root uid → skip (never chown migrated data to root)", 0, 0, "/mnt/x", nil},
		{"empty target → skip", 501, 20, "", nil},
		{"uid+gid", 501, 20, "/mnt/x", []string{"-R", "501:20", "/mnt/x"}},
		{"uid only, gid<0 leaves group", 501, -1, "/mnt/x", []string{"-R", "501", "/mnt/x"}},
	}
	for _, c := range cases {
		got := chownArgs(c.uid, c.gid, c.target)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: chownArgs(%d,%d,%q) = %v, want %v", c.name, c.uid, c.gid, c.target, got, c.want)
		}
	}
}

func TestChownSpec(t *testing.T) {
	if got := chownSpec(501, 20); got != "501:20" {
		t.Errorf("chownSpec(501,20) = %q, want 501:20", got)
	}
	if got := chownSpec(501, -1); got != "501" {
		t.Errorf("chownSpec(501,-1) = %q, want 501 (gid<0 leaves group)", got)
	}
}

package main

import "testing"

func TestParseOwner(t *testing.T) {
	cases := []struct {
		in       string
		uid, gid int
	}{
		{"", 0, -1},               // unset
		{"   ", 0, -1},            // blank
		{"501", 501, -1},          // uid only → group unchanged
		{"501:20", 501, 20},       // uid:gid
		{" 501 : 20 ", 501, 20},   // whitespace tolerated
		{"garbage", 0, -1},        // unparseable uid → unset
		{"501:garbage", 501, -1},  // bad gid → leave group
		{"0:0", 0, 0},             // root parses but chownArgs guards it out
	}
	for _, c := range cases {
		uid, gid := parseOwner(c.in)
		if uid != c.uid || gid != c.gid {
			t.Errorf("parseOwner(%q) = (%d,%d), want (%d,%d)", c.in, uid, gid, c.uid, c.gid)
		}
	}
}

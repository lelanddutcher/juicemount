package main

import "testing"

func TestNeedsFUSEMonitorSettle(t *testing.T) {
	tests := []struct {
		name          string
		fusePath      string
		bootFUSEReady bool
		want          bool
	}{
		{name: "server only", fusePath: "", bootFUSEReady: false, want: false},
		{name: "verified new mount", fusePath: "/fuse", bootFUSEReady: true, want: false},
		{name: "verified reused mount", fusePath: "/fuse", bootFUSEReady: true, want: false},
		{name: "deferred or failed mount", fusePath: "/fuse", bootFUSEReady: false, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := needsFUSEMonitorSettle(tt.fusePath, tt.bootFUSEReady); got != tt.want {
				t.Fatalf("needsFUSEMonitorSettle(%q, %v) = %v, want %v",
					tt.fusePath, tt.bootFUSEReady, got, tt.want)
			}
		})
	}
}

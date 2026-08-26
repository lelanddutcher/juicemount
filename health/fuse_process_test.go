package health

import "testing"

func TestJuiceFSMountCommandTargetsExactMount(t *testing.T) {
	target := "/Users/test/.juicemount/fuse-internal"
	tests := []struct {
		name    string
		command string
		want    bool
	}{
		{
			name: "target daemon",
			command: "/opt/homebrew/bin/juicefs mount redis://127.0.0.1:53975/1 " +
				target + " -d --no-usage-report",
			want: true,
		},
		{
			name:    "unrelated isolated mount",
			command: "/opt/homebrew/bin/juicefs mount redis://127.0.0.1:6379/1 /tmp/jm-rc-isolated/fuse",
			want:    false,
		},
		{
			name:    "similar mount prefix",
			command: "/opt/homebrew/bin/juicefs mount redis://127.0.0.1:6379/1 " + target + "-other",
			want:    false,
		},
		{
			name:    "pgrep command is not a daemon",
			command: "pgrep -f juicefs mount.*" + target,
			want:    false,
		},
		{
			name:    "empty target never matches",
			command: "/opt/homebrew/bin/juicefs mount redis://127.0.0.1:6379/1 /tmp/fuse",
			want:    false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mount := target
			if tc.name == "empty target never matches" {
				mount = ""
			}
			if got := juiceFSMountCommandTargets(tc.command, mount); got != tc.want {
				t.Fatalf("juiceFSMountCommandTargets(%q, %q) = %v, want %v",
					tc.command, mount, got, tc.want)
			}
		})
	}
}

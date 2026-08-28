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
			name: "target daemon with global flag",
			command: "/opt/homebrew/bin/juicefs --verbose mount redis://127.0.0.1:53975/1 " +
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

func TestParseJuiceFSMountCommand(t *testing.T) {
	redisURL, mountPoint, ok := parseJuiceFSMountCommand(
		"/opt/homebrew/bin/juicefs mount redis://127.0.0.1:50093/1 /Users/test/.juicemount/fuse-internal --bucket http://127.0.0.1:50094/zpool",
	)
	if !ok || redisURL != "redis://127.0.0.1:50093/1" || mountPoint != "/Users/test/.juicemount/fuse-internal" {
		t.Fatalf("parse = (%q, %q, %v)", redisURL, mountPoint, ok)
	}
	if _, _, ok := parseJuiceFSMountCommand("pgrep -f juicefs mount /tmp/fuse"); ok {
		t.Fatal("unrelated command parsed as a JuiceFS daemon")
	}
}

func TestJuiceFSMountEndpointReuseRequiresCurrentProxy(t *testing.T) {
	processes := []juiceFSMountProcess{{
		pid: "42",
		command: "/opt/homebrew/bin/juicefs mount redis://127.0.0.1:54791/1 " +
			"/Users/test/.juicemount/fuse-internal --no-usage-report",
	}}
	if known, matches := juiceFSMountEndpointMatchesProcesses(processes, "redis://127.0.0.1:50093/1"); !known || matches {
		t.Fatalf("stale proxy state = known %v matches %v, want true/false", known, matches)
	}
	if known, matches := juiceFSMountEndpointMatchesProcesses(processes, "redis://127.0.0.1:54791/1"); !known || !matches {
		t.Fatalf("current proxy state = known %v matches %v, want true/true", known, matches)
	}
	if known, matches := juiceFSMountEndpointMatchesProcesses(nil, "redis://127.0.0.1:50093/1"); known || matches {
		t.Fatalf("uninspectable state = known %v matches %v, want false/false", known, matches)
	}
}

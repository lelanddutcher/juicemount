package health

import (
	"reflect"
	"slices"
	"testing"
)

func TestJuiceFSMountPlatformOptions(t *testing.T) {
	tests := []struct {
		name string
		goos string
		want []string
	}{
		{name: "macOS accepts xattrs and hides internal mount", goos: "darwin", want: []string{"--enable-xattr", "-o", "nobrowse,daemon_timeout=900"}},
		{name: "Linux does not receive macOS FUSE option", goos: "linux"},
		{name: "other platforms remain option-free", goos: "freebsd"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := juiceFSMountPlatformOptions(tt.goos); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("juiceFSMountPlatformOptions(%q) = %v, want %v", tt.goos, got, tt.want)
			}
		})
	}
}

func TestDesktopMountSelectsNativeXattrsOnlyOnMacOS(t *testing.T) {
	base, _ := desktopJuiceFSMountArgs("redis://127.0.0.1:6379/1", "/mount", 32, 1)
	mac := append(slices.Clone(base), juiceFSMountPlatformOptions("darwin")...)
	linux := append(slices.Clone(base), juiceFSMountPlatformOptions("linux")...)
	xattrCount := 0
	for _, arg := range mac {
		if arg == "--enable-xattr" {
			xattrCount++
		}
	}
	if xattrCount != 1 || slices.Index(mac, "--enable-xattr") != len(base) {
		t.Fatalf("macOS direct mount must enable native xattrs once: %v", mac)
	}
	if slices.Contains(linux, "--enable-xattr") || slices.Contains(linux, "nobrowse,daemon_timeout=900") {
		t.Fatalf("Linux direct mount inherited macOS options: %v", linux)
	}
	for _, args := range [][]string{mac, linux} {
		if !slices.Contains(args, "--no-bgjob") || !slices.Contains(args, "--max-deletes") {
			t.Fatalf("direct mount lost its no-background/no-delete policy: %v", args)
		}
	}
}

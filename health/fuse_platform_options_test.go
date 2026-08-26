package health

import (
	"reflect"
	"testing"
)

func TestJuiceFSMountPlatformOptions(t *testing.T) {
	tests := []struct {
		name string
		goos string
		want []string
	}{
		{name: "macOS hides internal mount", goos: "darwin", want: []string{"-o", "nobrowse"}},
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

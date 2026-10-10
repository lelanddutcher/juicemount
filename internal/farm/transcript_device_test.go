package farm

import (
	"reflect"
	"testing"
)

func TestWhisperDeviceArgs(t *testing.T) {
	for _, backend := range []string{"", "cpu", " CPU "} {
		if got := WhisperDeviceArgs(backend); got != nil {
			t.Fatalf("WhisperDeviceArgs(%q) = %v, want nil", backend, got)
		}
	}
	want := []string{"--device", "0"}
	for _, backend := range []string{"vulkan", "cuda", "sycl"} {
		if got := WhisperDeviceArgs(backend); !reflect.DeepEqual(got, want) {
			t.Fatalf("WhisperDeviceArgs(%q) = %v, want %v", backend, got, want)
		}
	}
}

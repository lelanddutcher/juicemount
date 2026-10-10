//go:build !darwin

package fuse

import "testing"

func TestNonDarwinRenameFlagsRemainUnchanged(t *testing.T) {
	if renameCapabilities() != 0 {
		t.Fatal("Darwin-only capability advertised on another platform")
	}
	for _, flags := range []uint32{0, 1, 2, 4, 6} {
		got, status := normalizeRenameFlags(flags)
		if got != flags || status != 0 {
			t.Fatalf("flags %#x became %#x, %v", flags, got, status)
		}
	}
}

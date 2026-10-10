package version

import "testing"

func TestReleaseVersionOfRecord(t *testing.T) {
	if Version != "0.5.0" {
		t.Fatalf("Version = %q, want 0.5.0", Version)
	}
	if Commit == "" {
		t.Fatal("Commit must never be empty")
	}
}

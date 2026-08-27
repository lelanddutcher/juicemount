package health

import (
	"strings"
	"testing"
)

func TestReclaimPurgeableSpaceRequiresExplicitAuthorization(t *testing.T) {
	freed, snapshots, source, err := ReclaimPurgeableSpace(0, "/", 0)
	if err == nil {
		t.Fatal("automatic/unauthorized reclaim unexpectedly succeeded")
	}
	if !strings.Contains(err.Error(), "explicit user authorization") {
		t.Fatalf("unexpected authorization error: %v", err)
	}
	if freed != 0 || snapshots != 0 {
		t.Fatalf("unauthorized reclaim reported mutation: freed=%d snapshots=%d", freed, snapshots)
	}
	if source != "Time Machine local snapshots" {
		t.Fatalf("source = %q, want Time Machine local snapshots", source)
	}
}

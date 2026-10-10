package pin

import (
	"errors"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/testguard"
)

func TestPrefetcherCellularGuardLeavesPinPendingWithoutOpeningFUSE(t *testing.T) {
	t.Setenv(testguard.RefuseFUSEContentReadsEnv, "1")
	store := newTestStore(t)
	const path = "/Volumes/zpool/cold.mov"
	if err := store.Pin(path, 4096, "/Volumes/zpool"); err != nil {
		t.Fatal(err)
	}
	p := NewPrefetcher(store, "/definitely/not/a/mount", "/Volumes/zpool", 1)
	defer p.Stop()

	err := p.prefetch(Entry{Path: path, Size: 4096, Status: StatusPending})
	if !errors.Is(err, testguard.ErrFUSEContentReadRefused) {
		t.Fatalf("prefetch error = %v, want test refusal", err)
	}
	pending, err := store.Pending(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Path != path || pending[0].Status != StatusPending {
		t.Fatalf("pin was not preserved as pending: %+v", pending)
	}
	if stats := p.LiveStats(); stats.BytesPrefetched != 0 || stats.FilesPrefetched != 0 {
		t.Fatalf("guarded prefetch moved bytes: %+v", stats)
	}
}

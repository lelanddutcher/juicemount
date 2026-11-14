package pin

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// sizedProvider reports the file's current size, as the mirror does after a
// teammate's append (nfs.OwnedPinProvider.CurrentSize).
type sizedProvider struct{ *testOwnedPinProvider }

func (s sizedProvider) CurrentSize(e Entry) (int64, bool) {
	info, err := os.Stat(filepath.Join(s.root, filepath.Base(e.Path)))
	if err != nil {
		return 0, false
	}
	return info.Size(), true
}

// P5 (laptop run 2026-11-14): a teammate appended to a pinned file. The pin
// row kept the size from pin time, every re-pin refused the grown file
// (prefetch: want.Size != e.Size), and the pin stayed Failed for good. The
// re-pin must adopt the current size and end Ready with the new version.
func TestPinRepinAfterPeerAppendAdoptsNewSize(t *testing.T) {
	for _, withSize := range []bool{false, true} {
		s, p, path := pinnedFixture(t, "clip.mov", []byte("version one"))
		base := p.provider.(*testOwnedPinProvider)
		if withSize {
			p.provider = sizedProvider{base}
		}
		if err := claimAndPrefetch(t, s, p, path); err != nil {
			t.Fatal(err)
		}
		if e := pinEntry(t, s, path); e.Status != StatusReady {
			t.Fatalf("first pin: %+v", e)
		}
		grown := []byte("version one, and the appended part")
		if err := os.WriteFile(filepath.Join(base.root, "clip.mov"), grown, 0600); err != nil {
			t.Fatal(err)
		}
		// The guard finds the mirror changed and requeues the pin.
		if ok, err := s.RequeueForRepair(pinEntry(t, s, path)); err != nil || !ok {
			t.Fatalf("requeue: %v %v", ok, err)
		}
		err := claimAndPrefetch(t, s, p, path)
		e := pinEntry(t, s, path)
		if !withSize {
			if !errors.Is(err, ErrAuthorityUnavailable) || e.Status != StatusFailed {
				t.Fatalf("the old behaviour is not reproduced: %v %+v", err, e)
			}
			continue
		}
		if err != nil || e.Status != StatusReady || e.Size != int64(len(grown)) || e.BytesCached != int64(len(grown)) {
			t.Fatalf("re-pin after an append: %v, %+v", err, e)
		}
	}
}

// AdoptSize changes only the exact Prefetching generation.
func TestAdoptSizeIsGenerationExact(t *testing.T) {
	s, _, path := pinnedFixture(t, "clip.mov", []byte("abc"))
	e := pinEntry(t, s, path)
	if _, ok, _ := s.AdoptSize(e, 9); ok {
		t.Fatal("adopted a size for a Pending row")
	}
	claimed, ok, err := s.ClaimPrefetch(e)
	if err != nil || !ok {
		t.Fatal(err)
	}
	stale := claimed
	stale.Generation = "other"
	if _, ok, _ := s.AdoptSize(stale, 9); ok {
		t.Fatal("adopted a size for another generation")
	}
	got, ok, err := s.AdoptSize(claimed, 9)
	if err != nil || !ok || got.Size != 9 || pinEntry(t, s, path).Size != 9 {
		t.Fatalf("adopt: %v %v %+v", ok, err, got)
	}
}

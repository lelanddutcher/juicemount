package nfs

import (
	"testing"
	"time"
)

// seedDiskAvail pins the free-disk sample so the test controls what
// effectiveCapacity sees. avail < 0 means "Statfs never succeeded".
func seedDiskAvail(s *SpoolStore, avail int64) {
	s.diskAvailBytes.Store(avail)
	s.diskAvailAt.Store(time.Now().UnixNano())
}

const gib = int64(1) << 30

// The founder's incident (2026-08-03): copying to the volume while local disk is
// low stalls instead of failing. Root cause — NewSpoolStore clamps capacity to
// free disk ONCE, so the budget goes stale as the disk fills underneath it, and
// admission keeps saying yes until the DISK (not the budget) runs out. Because a
// full spool is designed to stall for backpressure, that produced an unbounded
// hang rather than an ENOSPC.
func TestEffectiveCapacity_TracksFreeDiskAfterConstruction(t *testing.T) {
	// NewSpoolStore applies its OWN construction-time clamp against the real
	// disk, so read back what it actually stored rather than assuming our
	// literal survived — the whole point of this fix is that the stored value
	// is a snapshot.
	s := newTestSpoolStore(t, 200*gib)
	configured := s.ConfiguredCapacity()

	t.Run("plenty of disk keeps the configured cap", func(t *testing.T) {
		seedDiskAvail(s, 900*gib)
		if got := s.effectiveCapacity(); got != configured {
			t.Errorf("got %d GiB, want the configured %d GiB", got>>30, configured>>30)
		}
	})

	t.Run("shrinking disk clamps below the configured cap", func(t *testing.T) {
		// 50 GiB free, 20 GiB floor -> only 30 GiB may still be consumed.
		s.used.Store(5 * gib)
		seedDiskAvail(s, 50*gib)
		want := 5*gib + (50*gib - SpoolFreeFloorBytes)
		if got := s.effectiveCapacity(); got != want {
			t.Errorf("got %d GiB, want %d GiB (used + headroom above the floor)", got>>30, want>>30)
		}
		if s.effectiveCapacity() >= configured {
			t.Error("a nearly-full disk must NOT still report the configured capacity — that is the bug")
		}
	})

	t.Run("at the floor there is zero headroom, so admission stops", func(t *testing.T) {
		s.used.Store(12 * gib)
		seedDiskAvail(s, SpoolFreeFloorBytes) // exactly at the floor
		if got, want := s.effectiveCapacity(), 12*gib; got != want {
			t.Errorf("got %d, want %d (== used: no new bytes may be admitted)", got, want)
		}
	})

	t.Run("below the floor never goes negative", func(t *testing.T) {
		s.used.Store(3 * gib)
		seedDiskAvail(s, 1*gib) // already past the floor
		if got, want := s.effectiveCapacity(), 3*gib; got != want {
			t.Errorf("got %d, want %d — headroom must clamp at zero, not go negative", got, want)
		}
	})

	t.Run("unreadable statfs fails OPEN, not closed", func(t *testing.T) {
		s.used.Store(1 * gib)
		seedDiskAvail(s, -1) // Statfs never succeeded
		if got := s.effectiveCapacity(); got != configured {
			t.Errorf("got %d, want the configured cap — an unreadable statfs is not a full disk", got)
		}
	})
}

// The exact shape measured during the incident: a configured capacity that,
// added to the free-disk floor, already exceeded the disk.
func TestEffectiveCapacity_IncidentShape(t *testing.T) {
	s := newTestSpoolStore(t, 37*gib) // what /spool actually reported
	configured := s.ConfiguredCapacity()
	s.used.Store(0)
	seedDiskAvail(s, 50*gib) // what the volume actually had free

	got := s.effectiveCapacity()
	if got >= configured {
		t.Fatalf("effective %d GiB >= configured %d GiB: the budget still over-commits the disk", got>>30, configured>>30)
	}
	if want := 50*gib - SpoolFreeFloorBytes; got != want {
		t.Errorf("effective = %d GiB, want %d GiB (free minus the floor)", got>>30, want>>30)
	}
}

// Unlimited (capacity 0) stays truly unlimited — a deliberate scope limit.
// "total=0 means unlimited" is the documented /spool contract that consumers
// branch on, and production never runs unlimited (AutoSpoolCapacity always
// yields a positive cap), so the incident cannot occur there. Disk-clamping it
// would change a public contract to fix a case that does not arise.
func TestEffectiveCapacity_UnlimitedStaysUnlimited(t *testing.T) {
	s := newTestSpoolStore(t, 0)

	seedDiskAvail(s, -1)
	if got := s.effectiveCapacity(); got != 0 {
		t.Errorf("unknown disk: got %d, want 0 (unlimited)", got)
	}

	// Even with a nearly-full disk, unlimited stays unlimited.
	s.used.Store(2 * gib)
	seedDiskAvail(s, 1*gib)
	if got := s.effectiveCapacity(); got != 0 {
		t.Errorf("low disk: got %d, want 0 — see the SCOPE LIMIT note in effectiveCapacity", got)
	}
}

// Capacity() must report the enforced budget, not the startup snapshot, so the
// UI and /spool don't advertise headroom that admission will refuse.
func TestCapacityReportsEffectiveNotConfigured(t *testing.T) {
	s := newTestSpoolStore(t, 400*gib)
	configured := s.ConfiguredCapacity()
	s.used.Store(1 * gib)
	seedDiskAvail(s, 30*gib)

	used, total := s.Capacity()
	if used != 1*gib {
		t.Errorf("used = %d, want %d", used, 1*gib)
	}
	if total >= configured {
		t.Errorf("Capacity total = %d GiB, still the configured snapshot", total>>30)
	}
	if s.ConfiguredCapacity() != configured {
		t.Errorf("ConfiguredCapacity must be stable, got %d want %d", s.ConfiguredCapacity(), configured)
	}
}

// The sample must be cached: admission runs per write, and Statfs-per-write on a
// large copy is exactly the kind of syscall amplification this codebase bans on
// hot paths.
func TestCachedDiskAvail_IsCachedWithinTTL(t *testing.T) {
	s := newTestSpoolStore(t, 100*gib)

	seedDiskAvail(s, 42*gib)
	at := s.diskAvailAt.Load()
	for i := 0; i < 50; i++ {
		if v, ok := s.cachedDiskAvail(); !ok || v != 42*gib {
			t.Fatalf("call %d: got (%d,%v), want the cached 42 GiB", i, v, ok)
		}
	}
	if s.diskAvailAt.Load() != at {
		t.Error("cache was re-sampled inside the TTL — admission would Statfs per write")
	}

	// Expiring the stamp must allow a real resample (value will be the real
	// disk, so only assert that the timestamp moved).
	s.diskAvailAt.Store(time.Now().Add(-2 * diskAvailTTL).UnixNano())
	if _, ok := s.cachedDiskAvail(); !ok {
		t.Skip("Statfs unavailable in this environment")
	}
	if s.diskAvailAt.Load() == at {
		t.Error("cache did not resample after the TTL expired")
	}
}

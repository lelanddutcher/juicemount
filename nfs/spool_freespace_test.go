package nfs

import (
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// seedDiskAvail pins the sampled ceiling as if free disk were `avail` at this
// instant, mirroring refreshCeiling exactly. avail < 0 means "Statfs failed".
func seedDiskAvail(s *SpoolStore, avail int64) {
	if avail < 0 {
		s.capCeiling.Store(-1)
		s.diskAvailAt.Store(time.Now().UnixNano())
		return
	}
	headroom := avail - SpoolFreeFloorBytes
	if headroom < 0 {
		headroom = 0
	}
	ceiling := s.used.Load() + headroom
	if ceiling < minClampedCeiling {
		ceiling = minClampedCeiling
	}
	s.capCeiling.Store(ceiling)
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
func TestCachedCeiling_IsCachedWithinTTL(t *testing.T) {
	s := newTestSpoolStore(t, 100*gib)

	seedDiskAvail(s, 42*gib)
	at := s.diskAvailAt.Load()
	want := s.capCeiling.Load()
	for i := 0; i < 50; i++ {
		if v, ok := s.cachedCeiling(); !ok || v != want {
			t.Fatalf("call %d: got (%d,%v), want the cached ceiling %d", i, v, ok, want)
		}
	}
	if s.diskAvailAt.Load() != at {
		t.Error("cache was re-sampled inside the TTL — admission would Statfs per write")
	}

	// Expiring the stamp must allow a real resample (value will be the real
	// disk, so only assert that the timestamp moved).
	s.diskAvailAt.Store(time.Now().Add(-2 * diskAvailTTL).UnixNano())
	if _, ok := s.cachedCeiling(); !ok {
		t.Skip("Statfs unavailable in this environment")
	}
	if s.diskAvailAt.Load() == at {
		t.Error("cache did not resample after the TTL expired")
	}
}

// CRITICAL regression (2026-08-03 review, defect 1). The first version derived
// the ceiling from the LIVE `used` on every check, so `cur+delta > used+headroom`
// reduced to `delta > headroom` per call and cumulative admission inside one
// sample window was unbounded. The reviewer's reproduction: six 9 GiB
// reservations all cleared a 10 GiB headroom, driving used to 54 GiB.
//
// The ceiling must be pinned to a `used` snapshot taken with the disk reading,
// so a window admits at most `headroom` in TOTAL.
func TestTryReserveCapacity_CumulativeAdmissionIsBoundedWithinOneSample(t *testing.T) {
	s := newTestSpoolStore(t, 200*gib)
	s.used.Store(0)
	seedDiskAvail(s, 30*gib) // headroom = 10 GiB, pinned for this window

	var admitted int64
	for i := 0; i < 6; i++ {
		if s.tryReserveCapacity(9 * gib) {
			admitted += 9 * gib
		}
	}
	if headroom := 30*gib - SpoolFreeFloorBytes; admitted > headroom {
		t.Errorf("admitted %d GiB against %d GiB of headroom — the ceiling is chasing `used` again",
			admitted>>30, headroom>>30)
	}
	if admitted == 0 {
		t.Error("admitted nothing; the clamp is now too strict to accept a fitting write")
	}
}

// CRITICAL regression (2026-08-03 review, defect 2). Every admission gate spells
// "unlimited" as `ec <= 0`. A legitimately-computed ceiling of exactly zero —
// spool EMPTY and disk at/under the floor, the resting state of a chronically
// low-disk machine — collided with that sentinel and re-opened admission with no
// cap at all, at the worst possible moment.
func TestEffectiveCapacity_ZeroCeilingDoesNotReadAsUnlimited(t *testing.T) {
	s := newTestSpoolStore(t, 100*gib)
	s.used.Store(0)                       // empty spool
	seedDiskAvail(s, SpoolFreeFloorBytes) // exactly at the floor -> headroom 0

	ec := s.effectiveCapacity()
	if ec <= 0 {
		t.Fatalf("effectiveCapacity()=%d reads as the 'unlimited' sentinel at the floor", ec)
	}
	if s.tryReserveCapacity(50 * gib) {
		t.Error("admitted 50 GiB with an empty spool on a floor-constrained disk — sentinel collision is back")
	}
	if !s.DiskConstrained() {
		t.Error("DiskConstrained() should be true when the disk clamp is what blocks admission")
	}
}

// DiskConstrained must distinguish "the configured budget is full" (waiting on
// the drain can help) from "the volume is full" (it cannot). The offline stall
// branches on this to avoid parking forever on a hang reconnecting won't fix.
func TestDiskConstrained_OnlyWhenTheDiskIsTheBlocker(t *testing.T) {
	s := newTestSpoolStore(t, 8*gib)

	// Budget-full but plenty of disk: NOT disk-constrained.
	s.used.Store(8 * gib)
	seedDiskAvail(s, 500*gib)
	if s.DiskConstrained() {
		t.Error("budget-full with a roomy disk must not report disk-constrained")
	}

	// Disk at the floor: constrained.
	s.used.Store(1 * gib)
	seedDiskAvail(s, SpoolFreeFloorBytes)
	if !s.DiskConstrained() {
		t.Error("disk at the floor must report disk-constrained")
	}

	// Unlimited config is never disk-constrained (scope limit).
	u := newTestSpoolStore(t, 0)
	seedDiskAvail(u, 1*gib)
	if u.DiskConstrained() {
		t.Error("unlimited config must never report disk-constrained")
	}
}

// --- The offline + disk-constrained stall (2026-08-04 review, defects 1-3) ---
//
// This is the riskiest path in the free-space work and it originally shipped with
// NO test touching it: the existing offline-stall test never seeds a constrained
// ceiling, so DiskConstrained() reads false throughout and the branch never runs.

// pinConstrained parks the store permanently at the free-disk floor so
// DiskConstrained() is true and stays true for the length of a test.
func pinConstrained(t *testing.T, s *SpoolStore) {
	t.Helper()
	go func() {
		for i := 0; i < 4000; i++ { // outlive the test; refreshes every ~1s TTL
			seedDiskAvail(s, SpoolFreeFloorBytes)
			time.Sleep(2 * time.Millisecond)
		}
	}()
	seedDiskAvail(s, SpoolFreeFloorBytes)
	if !s.DiskConstrained() {
		t.Fatal("precondition: store should be disk-constrained")
	}
}

// (a) Offline + disk-constrained must TIME OUT. Reconnecting cannot free local
// disk, so waiting forever is an unbounded Finder hang on a hard mount.
func TestWaitForCapacity_OfflineButDiskConstrainedGivesUp(t *testing.T) {
	old := capacityWaitDeadline
	capacityWaitDeadline = 80 * time.Millisecond
	t.Cleanup(func() { capacityWaitDeadline = old })
	pin.SetOffline(true)
	t.Cleanup(func() { pin.SetOffline(false) })

	s := newTestSpoolStore(t, 4*gib)
	s.used.Store(1 * gib)
	pinConstrained(t, s)

	done := make(chan bool, 1)
	go func() { done <- s.waitForCapacity(func() bool { return false }, nil) }()

	select {
	case ok := <-done:
		if ok {
			t.Error("waitForCapacity reported success though try() never succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("offline + disk-constrained stalled forever — the unbounded hang is back")
	}
}

// (b) A GENUINE reconnect mid-stall must still grant a fresh window. Overwriting
// the `offline` var poisoned wasOffline, so the offline->online edge went
// undetected and the reconnect had no effect on the timer at all.
func TestWaitForCapacity_ReconnectStillResetsTheWindow(t *testing.T) {
	old := capacityWaitDeadline
	capacityWaitDeadline = 200 * time.Millisecond
	t.Cleanup(func() { capacityWaitDeadline = old })
	pin.SetOffline(true)
	t.Cleanup(func() { pin.SetOffline(false) })

	s := newTestSpoolStore(t, 4*gib)
	s.used.Store(1 * gib)
	pinConstrained(t, s)

	start := time.Now()
	done := make(chan bool, 1)
	go func() { done <- s.waitForCapacity(func() bool { return false }, nil) }()

	// Reconnect just before the first window would close. The edge must restart
	// the clock, so the total must exceed one bare deadline.
	time.Sleep(150 * time.Millisecond)
	pin.SetOffline(false)

	select {
	case <-done:
		if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
			t.Errorf("gave up after %v; a reconnect at 150ms must grant a fresh %v window "+
				"(wasOffline was poisoned, so the edge went undetected)", elapsed, capacityWaitDeadline)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("never returned")
	}
}

// (c) A FLAPPING DiskConstrained() while genuinely offline must not defeat the
// backstop. Resetting the window on STATE rather than PROGRESS meant a value
// that flaps — and this one is a ~1s statfs sampled right at the floor, so it
// flaps in normal operation — reset the clock forever.
func TestWaitForCapacity_FlappingDiskConstrainedStillTimesOut(t *testing.T) {
	old := capacityWaitDeadline
	capacityWaitDeadline = 100 * time.Millisecond
	t.Cleanup(func() { capacityWaitDeadline = old })
	pin.SetOffline(true)
	t.Cleanup(func() { pin.SetOffline(false) })

	s := newTestSpoolStore(t, 400*gib)
	s.used.Store(1 * gib)

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		roomy := true
		for {
			select {
			case <-stop:
				return
			default:
			}
			if roomy {
				seedDiskAvail(s, 900*gib) // not constrained
			} else {
				seedDiskAvail(s, SpoolFreeFloorBytes) // constrained
			}
			roomy = !roomy
			time.Sleep(2 * time.Millisecond)
		}
	}()

	done := make(chan bool, 1)
	go func() { done <- s.waitForCapacity(func() bool { return false }, nil) }()

	select {
	case <-done: // gave up — correct
	case <-time.After(3 * time.Second):
		t.Fatal("flapping DiskConstrained() reset the window forever — the CRITICAL hang is back")
	}
}

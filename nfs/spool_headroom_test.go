package nfs

import (
	"net"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// spool_headroom_test.go — B-1: the JuiceFS block cache counts as spool headroom.
//
// Every test here drives the REAL functions (reclaimableCacheBytes,
// spoolHeadroomBytes, SpoolStore.refreshCeiling). None restates the formula.

// fresh is a verdict age comfortably inside spoolCacheVerdictMaxAge.
const fresh = 30 * time.Second

func TestReclaimableCacheBytes(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   cacheReclaimInputs
		want int64
	}{
		{
			// THE SAFETY GATE. With --writeback a cache block can be the ONLY
			// durable copy of a just-written file (5 of 50 drained CR3s came back
			// corrupt from MinIO in the kill-9 test that got writeback disabled).
			// However much cache there is, none of it is ours to spend.
			name: "writeback ON makes the entire cache untouchable",
			in: cacheReclaimInputs{
				WritebackOn: true, HaveVerdict: true, VerdictAge: fresh,
				BlockCacheBytes: 100 * gib,
			},
			want: 0,
		},
		{
			name: "writeback ON overrides even a huge cache with nothing pinned",
			in: cacheReclaimInputs{
				WritebackOn: true, HaveVerdict: true, VerdictAge: fresh,
				BlockCacheBytes: 900 * gib, CacheDirBytes: 900 * gib,
			},
			want: 0,
		},
		{
			// No verdict yet => the pinned set is unknown => we cannot promise we
			// are staying above it, so nothing is reclaimable.
			name: "no capacity verdict yet yields nothing",
			in:   cacheReclaimInputs{BlockCacheBytes: 100 * gib},
			want: 0,
		},
		{
			name: "a verdict older than the max age is not trusted",
			in: cacheReclaimInputs{
				HaveVerdict: true, VerdictAge: spoolCacheVerdictMaxAge + time.Second,
				BlockCacheBytes: 100 * gib,
			},
			want: 0,
		},
		{
			name: "a negative age (clock went backwards) is not trusted",
			in: cacheReclaimInputs{
				HaveVerdict: true, VerdictAge: -time.Hour, BlockCacheBytes: 100 * gib,
			},
			want: 0,
		},
		{
			// The live measurement: ~100 GB of block cache, nothing pinned.
			name: "unpinned cache is reclaimable, less the flap margin",
			in: cacheReclaimInputs{
				HaveVerdict: true, VerdictAge: fresh, BlockCacheBytes: 100 * gib,
			},
			want: 100*gib - spoolCacheFlapMarginBytes,
		},
		{
			// PINNED CONTENT IS AN OFFLINE-AVAILABILITY PROMISE. Only the part of
			// the cache above the pinned set may be spent.
			name: "pinned bytes are never offered to the spool",
			in: cacheReclaimInputs{
				HaveVerdict: true, VerdictAge: fresh,
				BlockCacheBytes: 100 * gib, PinnedBytes: 60 * gib,
			},
			want: 40*gib - spoolCacheFlapMarginBytes,
		},
		{
			name: "a fully pinned cache yields nothing",
			in: cacheReclaimInputs{
				HaveVerdict: true, VerdictAge: fresh,
				BlockCacheBytes: 100 * gib, PinnedBytes: 100 * gib,
			},
			want: 0,
		},
		{
			name: "pinned exceeding the cache never goes negative",
			in: cacheReclaimInputs{
				HaveVerdict: true, VerdictAge: fresh,
				BlockCacheBytes: 10 * gib, PinnedBytes: 500 * gib,
			},
			want: 0,
		},
		{
			name: "a cache smaller than the flap margin yields nothing",
			in: cacheReclaimInputs{
				HaveVerdict: true, VerdictAge: fresh, BlockCacheBytes: spoolCacheFlapMarginBytes - 1,
			},
			want: 0,
		},
		{
			// Gauge unavailable (daemon metrics endpoint down): the cache-dir du
			// is the documented fallback.
			name: "falls back to the cache-dir du when the gauge is missing",
			in: cacheReclaimInputs{
				HaveVerdict: true, VerdictAge: fresh, CacheDirBytes: 50 * gib,
			},
			want: 50*gib - spoolCacheFlapMarginBytes,
		},
		{
			// Both available and they disagree: take the SMALLER. The du
			// over-counts staging chunks; the gauge lingers stale-high after a
			// cache clear. Under-claiming is the only safe direction.
			name: "with both figures the smaller wins (du lower)",
			in: cacheReclaimInputs{
				HaveVerdict: true, VerdictAge: fresh,
				BlockCacheBytes: 100 * gib, CacheDirBytes: 30 * gib,
			},
			want: 30*gib - spoolCacheFlapMarginBytes,
		},
		{
			name: "with both figures the smaller wins (gauge lower)",
			in: cacheReclaimInputs{
				HaveVerdict: true, VerdictAge: fresh,
				BlockCacheBytes: 30 * gib, CacheDirBytes: 100 * gib,
			},
			want: 30*gib - spoolCacheFlapMarginBytes,
		},
		{
			name: "no cache figure at all yields nothing",
			in:   cacheReclaimInputs{HaveVerdict: true, VerdictAge: fresh},
			want: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := reclaimableCacheBytes(tc.in); got != tc.want {
				t.Errorf("reclaimableCacheBytes = %d (%.1f GiB), want %d (%.1f GiB)",
					got, float64(got)/float64(gib), tc.want, float64(tc.want)/float64(gib))
			}
		})
	}
}

// THE BUG, in the numbers measured on the live machine: 25.8 GiB free, ~100 GB
// of JuiceFS block cache on the same SSD, 20 GiB spool floor. The old formula
// offered 5.79 GiB of headroom, so a 40 GB camera file could NEVER be copied —
// and it cannot free headroom for itself, because a spool entry only drains at
// close.
func TestSpoolHeadroomBytes_LiveIncidentShape(t *testing.T) {
	// 25.8 GiB, as measured.
	const avail = 25*gib + 819*(gib/1024)
	const cache = 100 * gib
	const cameraFile = 40 * gib

	blind := spoolHeadroomBytes(avail, cacheReclaimInputs{})
	if blind >= cameraFile {
		t.Fatalf("precondition: the old free-disk-only headroom (%.2f GiB) should not fit a %d GiB file",
			float64(blind)/float64(gib), cameraFile/gib)
	}

	aware := spoolHeadroomBytes(avail, cacheReclaimInputs{
		HaveVerdict: true, VerdictAge: fresh, BlockCacheBytes: cache,
	})
	if aware < cameraFile {
		t.Errorf("headroom with a %d GiB reclaimable cache = %.2f GiB — still cannot admit a %d GiB camera file",
			cache/gib, float64(aware)/float64(gib), cameraFile/gib)
	}
	if want := avail + cache - spoolCacheFlapMarginBytes - SpoolFreeFloorBytes; aware != want {
		t.Errorf("headroom = %d, want %d (avail + cache - margin - floor)", aware, want)
	}

	// The earlier documented incident: 14 GiB free, so free disk alone is BELOW
	// the floor and the old formula produced zero headroom (a 1-byte ceiling).
	if h := spoolHeadroomBytes(14*gib, cacheReclaimInputs{}); h != 0 {
		t.Errorf("precondition: 14 GiB free must give 0 headroom under the old formula, got %d", h)
	}
	if h := spoolHeadroomBytes(14*gib, cacheReclaimInputs{
		HaveVerdict: true, VerdictAge: fresh, BlockCacheBytes: cache,
	}); h < cameraFile {
		t.Errorf("14 GiB free with a 100 GiB reclaimable cache gave %.2f GiB — the incident is not fixed",
			float64(h)/float64(gib))
	}

	// And with writeback ON, both cases must be EXACTLY the old behaviour.
	for _, av := range []int64{avail, 14 * gib} {
		wb := spoolHeadroomBytes(av, cacheReclaimInputs{
			WritebackOn: true, HaveVerdict: true, VerdictAge: fresh, BlockCacheBytes: cache,
		})
		if old := spoolHeadroomBytes(av, cacheReclaimInputs{}); wb != old {
			t.Errorf("writeback ON at %.1f GiB free: headroom %d, want the historical %d",
				float64(av)/float64(gib), wb, old)
		}
	}
}

// Headroom must never go negative regardless of inputs — a negative would
// underflow into the ceiling arithmetic in refreshCeiling.
func TestSpoolHeadroomBytes_NeverNegative(t *testing.T) {
	for _, avail := range []int64{0, 1, 1 * gib, SpoolFreeFloorBytes, 900 * gib} {
		for _, in := range []cacheReclaimInputs{
			{},
			{WritebackOn: true},
			{HaveVerdict: true, VerdictAge: fresh},
			{HaveVerdict: true, VerdictAge: fresh, BlockCacheBytes: 1, PinnedBytes: 900 * gib},
		} {
			if got := spoolHeadroomBytes(avail, in); got < 0 {
				t.Errorf("avail=%d in=%+v -> %d", avail, in, got)
			}
		}
	}
}

// wantCeiling mirrors ONLY refreshCeiling's minClampedCeiling sentinel guard.
// The arithmetic under test comes from the real spoolHeadroomBytes.
func wantCeiling(used, avail int64, in cacheReclaimInputs) int64 {
	c := used + spoolHeadroomBytes(avail, in)
	if c < minClampedCeiling {
		c = minClampedCeiling
	}
	return c
}

// WIRING. The pure function above is worth nothing if refreshCeiling doesn't
// call it, so this drives the real refreshCeiling against the real statfs of the
// spool root and only substitutes the cache snapshot.
func TestRefreshCeiling_CountsReclaimableCacheAsHeadroom(t *testing.T) {
	s := newFixedCapSpool(t, 4000*gib) // configured cap far above any ceiling
	s.used.Store(3 * gib)

	// statfs drift between the two samples below is the only source of error.
	const tol = int64(1) << 30

	type ceilingSample struct {
		ceiling int64
		avail   int64
	}
	assertCeiling := func(label string, in cacheReclaimInputs) ceilingSample {
		t.Helper()
		setCacheReclaimHookForTest(func() cacheReclaimInputs { return in })
		s.refreshCeiling()
		got := s.capCeiling.Load()
		avail, err := spoolDiskAvail(s.root)
		if err != nil {
			t.Skipf("statfs unavailable in this environment: %v", err)
		}
		want := wantCeiling(s.used.Load(), avail, in)
		if d := got - want; d > tol || d < -tol {
			t.Errorf("%s: ceiling = %d (%.2f GiB), want ~%d (%.2f GiB)",
				label, got, float64(got)/float64(gib), want, float64(want)/float64(gib))
		}
		return ceilingSample{ceiling: got, avail: avail}
	}
	t.Cleanup(func() { setCacheReclaimHookForTest(nil) })

	blind := assertCeiling("cache unknown", cacheReclaimInputs{})

	const cache = 100 * gib
	awareInputs := cacheReclaimInputs{
		HaveVerdict: true, VerdictAge: fresh, BlockCacheBytes: cache,
	}
	aware := assertCeiling("100 GiB reclaimable cache", awareInputs)

	// The whole point: the ceiling must actually rise by the reclaimable amount.
	// Normalize each result against its own statfs sample: APFS free space can
	// move by several GiB while the test process is idle (purgeable snapshots and
	// concurrent build output), so subtracting the two raw ceilings made this a
	// host-activity test rather than a refreshCeiling wiring test.
	blindBase := wantCeiling(s.used.Load(), blind.avail, cacheReclaimInputs{})
	awareBase := wantCeiling(s.used.Load(), aware.avail, cacheReclaimInputs{})
	wantDelta := wantCeiling(s.used.Load(), aware.avail, awareInputs) - awareBase
	if d := (aware.ceiling - awareBase) - (blind.ceiling - blindBase); d < wantDelta-2*tol {
		t.Errorf("ceiling rose by only %.2f GiB with %d GiB of reclaimable cache — "+
			"refreshCeiling is still counting free disk alone",
			float64(d)/float64(gib), cache/gib)
	}

	// And admission must follow: a 40 GiB camera file that the free-disk-only
	// ceiling refused must now be reservable.
	if aware.ceiling-s.used.Load() < 40*gib {
		t.Fatalf("precondition: expected room for a 40 GiB file, ceiling %d used %d", aware.ceiling, s.used.Load())
	}
	if !s.tryReserveCapacity(40 * gib) {
		t.Error("a 40 GiB camera file was refused despite a 100 GiB reclaimable cache")
	}
}

// The safety gate, wired. With JM_FUSE_WRITEBACK=1 a cache block may be the only
// durable copy, so refreshCeiling must land on EXACTLY the historical number.
func TestRefreshCeiling_WritebackOnFallsBackToFreeDiskOnly(t *testing.T) {
	s := newFixedCapSpool(t, 4000*gib)
	s.used.Store(1 * gib)
	t.Cleanup(func() { setCacheReclaimHookForTest(nil) })

	const tol = int64(1) << 30

	setCacheReclaimHookForTest(func() cacheReclaimInputs { return cacheReclaimInputs{} })
	s.refreshCeiling()
	blind := s.capCeiling.Load()

	setCacheReclaimHookForTest(func() cacheReclaimInputs {
		return cacheReclaimInputs{
			WritebackOn: true, HaveVerdict: true, VerdictAge: fresh,
			BlockCacheBytes: 100 * gib, CacheDirBytes: 100 * gib,
		}
	})
	s.refreshCeiling()
	wb := s.capCeiling.Load()

	if d := wb - blind; d > tol || d < -tol {
		t.Errorf("writeback ON moved the ceiling by %.2f GiB — cache blocks may be the ONLY "+
			"durable copy in that mode and must never be counted as spool headroom",
			float64(d)/float64(gib))
	}
}

// liveCacheReclaimInputs runs on the write admission path, under a TryLock, with
// callers holding a path shard or the per-entry e.mu. It must NEVER perform I/O.
// The tempting mistake is to call pin.BlockCacheBytes(), which scrapes the FUSE
// daemon over HTTP with a 3s timeout — one wedged metrics endpoint would then
// stall every concurrent writer. Point the scraper at a socket that accepts and
// never answers, and require the snapshot to return immediately.
func TestLiveCacheReclaimInputs_NeverScrapesOnTheWritePath(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot listen: %v", err)
	}
	defer ln.Close() // never Accept: a connect succeeds, the GET then hangs.

	pin.SetBlockCacheMetricsAddr(ln.Addr().String())
	t.Cleanup(func() { pin.SetBlockCacheMetricsAddr("") })

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = liveCacheReclaimInputs()
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("liveCacheReclaimInputs blocked — it is doing I/O on the write admission path")
	}
}

package nfs

import "testing"

// AutoSpoolCapacity must NOT be a disk snapshot.
//
// It used to return max(8 GiB, avail - SpoolFreeFloorBytes) sampled once at boot,
// and that became s.capacity for the process lifetime. Since effectiveCapacity is
// min(capacity, ceiling), a boot on a low-disk machine pinned the budget at the
// 8 GiB floor — so the reclaimable-cache headroom could raise the live ceiling to
// ~100 GiB and a 40 GB camera file would STILL be refused, bounded by a number
// sampled at whatever moment the app happened to start. Same one-way ratchet
// removed from NewSpoolStore in b77df6c, one level up.
func TestAutoSpoolCapacityIsNotADiskSnapshot(t *testing.T) {
	got := AutoSpoolCapacity(t.TempDir())
	if got != AutoSpoolCapacityBudget {
		t.Errorf("AutoSpoolCapacity = %d, want the logical budget %d — a disk-derived "+
			"value here caps effectiveCapacity forever at whatever free disk was at boot",
			got, AutoSpoolCapacityBudget)
	}
	if got < int64(1)<<40 {
		t.Errorf("budget %d is small enough to bind on a real disk — it must not be the "+
			"binding constraint in Auto mode; the live ceiling is", got)
	}
}

// With the ceiling READABLE, Auto is bounded by the live ceiling and not by any
// startup number — this is the property that actually unblocks a large file.
func TestAutoBudgetIsBoundedByTheLiveCeiling(t *testing.T) {
	s := newFixedCapSpool(t, AutoSpoolCapacityBudget)
	seedDiskAvail(s, 120*gib) // plenty of headroom
	got := s.effectiveCapacity()
	if got >= AutoSpoolCapacityBudget {
		t.Fatalf("effective %d is the raw budget — the live ceiling must bind", got)
	}
	if got < 90*gib {
		t.Errorf("effective %d is far below the ~100 GiB the live headroom allows — "+
			"something other than the ceiling is capping Auto", got)
	}
}

// ...but Auto must NOT become "unlimited" when we lose sight of the disk.
// effectiveCapacity fails OPEN on an unreadable statfs (correct — an unreadable
// statfs is not a full disk), and with a 1 PiB budget an unbounded fail-open
// would mean no protection at exactly the moment we cannot see the disk.
func TestFailOpenIsBoundedForTheAutoBudget(t *testing.T) {
	s := newFixedCapSpool(t, AutoSpoolCapacityBudget)
	seedDiskAvail(s, -1) // Statfs never succeeded
	got := s.effectiveCapacity()
	if got > failOpenMaxBytes {
		t.Errorf("fail-open admitted %d bytes on the Auto budget — with no visibility of "+
			"the disk this must be bounded at %d, not effectively unlimited",
			got, failOpenMaxBytes)
	}
	if got <= 0 {
		t.Errorf("fail-open admitted %d — an unreadable statfs is not a full disk", got)
	}
}

// The bound applies ONLY to the Auto sentinel. An EXPLICIT user budget still
// fails open in FULL — the user chose that number, and losing sight of the disk
// is not a reason to override them. The first version of the Auto bound applied
// unconditionally and broke the pre-existing test asserting exactly this.
func TestFailOpenReturnsAnExplicitBudgetInFull(t *testing.T) {
	const explicit = 40 * gib // deliberately far above failOpenMaxBytes
	s := newFixedCapSpool(t, explicit)
	seedDiskAvail(s, -1)
	if got := s.effectiveCapacity(); got != explicit {
		t.Errorf("fail-open with an explicit %d budget returned %d — an unreadable statfs "+
			"is not a full disk, and must not silently shrink a limit the user set",
			explicit, got)
	}
}

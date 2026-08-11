package nfs

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// inplace_hole_guard_test.go — the NEGATIVE half of task #6.
//
// nfs/inplace_hole_probe_test.go proves the guard refuses a hole. These prove it
// refuses ONLY a hole. That is the harder and more dangerous half: turning hole
// reads into JUKEBOX holds is exactly what produced the #100 73-seconds-per-file
// stall, so a guard that over-fires is a worse bug than the one it fixes.
//
// Each test below corresponds to one way this could over-fire.

// A read of the VALID PREFIX — bytes that really are on disk, below the hole —
// must be served normally. Over-holding here would stall every reader of any
// file that is being appended to, which is the #100 failure.
func TestValidBytesBelowTheHoleAreStillServed(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)

	const (
		seedLen = 4096
		tailOff = 1 << 20
	)
	seed := bytes.Repeat([]byte{'A'}, seedLen)
	seedFile(t, jfs, store, fuseRoot, "reel.mov", seed)

	// Out-of-order tail: hole is [4096, 1 MiB).
	writeVia(t, jfs, "reel.mov", tailOff, bytes.Repeat([]byte{'B'}, seedLen))

	// Read the pre-existing, valid head.
	buf, got, rerr, _ := readAtVia(t, jfs, "reel.mov", 0, seedLen)
	if rerr != nil {
		t.Fatalf("read of the VALID prefix was refused with %v — the guard is over-firing, "+
			"which stalls every reader of an appended-to file (#100)", rerr)
	}
	if got != seedLen {
		t.Fatalf("read of the valid prefix returned n=%d, want %d", got, seedLen)
	}
	if !bytes.Equal(buf[:got], seed) {
		t.Fatal("read of the valid prefix returned the wrong bytes")
	}
}

// A read of the WRITTEN TAIL — above the hole, real bytes — is at or past the
// contiguous prefix, so planReadAt classifies it as a hold too. That is
// deliberate and matches the spool: serving it would let a client assemble a
// file out of a valid head and a valid tail with zeros in between and call it
// complete. This test pins that intent so nobody "fixes" it into a serve.
func TestTailAboveTheHoleIsAlsoHeldNotServed(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	const (
		seedLen = 4096
		tailOff = 1 << 20
	)
	seedFile(t, jfs, store, fuseRoot, "reel.mov", bytes.Repeat([]byte{'A'}, seedLen))
	writeVia(t, jfs, "reel.mov", tailOff, bytes.Repeat([]byte{'B'}, seedLen))

	_, _, rerr, _ := readAtVia(t, jfs, "reel.mov", tailOff, seedLen)
	if !pin.IsSpoolIncomplete(rerr) {
		t.Errorf("read above the hole returned %v; want a hold. Serving it lets a client "+
			"treat head+zeros+tail as a complete file", rerr)
	}
}

// ONCE THE HOLE FILLS, everything must serve again AND the tracker must be
// gone — otherwise the atomic gate stays non-zero and every read in the
// process pays for a map lookup forever.
func TestFilledHoleServesAgainAndStopsCostingReads(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	const (
		seedLen = 4096
		tailOff = 64 << 10
	)
	seedFile(t, jfs, store, fuseRoot, "reel.mov", bytes.Repeat([]byte{'A'}, seedLen))
	writeVia(t, jfs, "reel.mov", tailOff, bytes.Repeat([]byte{'B'}, seedLen))

	h := jfs.handler
	if h.inPlaceHoles.holes.Load() == 0 {
		t.Fatal("no hole tracked after an out-of-order write — the guard never armed")
	}

	// Fill the gap [4096, 64 KiB).
	writeVia(t, jfs, "reel.mov", seedLen, bytes.Repeat([]byte{'C'}, tailOff-seedLen))

	if n := h.inPlaceHoles.holes.Load(); n != 0 {
		t.Errorf("holes gate = %d after the hole filled, want 0 — every read in the "+
			"process now pays a map lookup for a file with no hole", n)
	}
	if _, _, rerr, _ := readAtVia(t, jfs, "reel.mov", tailOff/2, 4096); rerr != nil {
		t.Errorf("read of a filled region still refused with %v", rerr)
	}
}

// A PURELY SEQUENTIAL WRITER — the overwhelmingly common case, every ordinary
// copy — must never allocate a tracker at all. If it does, the hot-path gate is
// non-zero during every copy and the "one atomic load" claim is false.
func TestSequentialWriterNeverArmsTheGuard(t *testing.T) {
	jfs, _, _ := newIntegrityHarness(t)
	cf, err := jfs.Create("seq.mov")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cf.Close()

	const chunk = 4096
	for i := 0; i < 16; i++ {
		writeVia(t, jfs, "seq.mov", int64(i*chunk), bytes.Repeat([]byte{'A'}, chunk))
		if n := jfs.handler.inPlaceHoles.holes.Load(); n != 0 {
			t.Fatalf("sequential write #%d armed the guard (holes=%d) — a plain copy must "+
				"cost nothing on the read path", i, n)
		}
	}
}

// `._` APPLEDOUBLE SIDECARS MUST NEVER BE HELD. planReadAt carries the bypass
// and this proves it survives the in-place wiring. Holding sidecars is a named
// cause of the #100 stall: Finder reads them constantly during a listing.
func TestAppleDoubleSidecarIsNeverHeld(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	const (
		seedLen = 128
		tailOff = 64 << 10
	)
	seedFile(t, jfs, store, fuseRoot, "._reel.mov", bytes.Repeat([]byte{'A'}, seedLen))
	writeVia(t, jfs, "._reel.mov", tailOff, bytes.Repeat([]byte{'B'}, seedLen))

	_, _, rerr, _ := readAtVia(t, jfs, "._reel.mov", seedLen+16, 64)
	if pin.IsSpoolIncomplete(rerr) {
		t.Fatal("a ._ sidecar read was JUKEBOX-held. Finder reads sidecars constantly " +
			"during a listing, and holding them is a named cause of the #100 stall")
	}
}

// A DEAD WRITER MUST RELEASE THE HOLD. Without the liveness bound a client
// retries a JUKEBOX forever on a file nobody is writing — an unbounded stall.
func TestHoldReleasesOnceTheWriterGoesQuiet(t *testing.T) {
	var tr inPlaceTracker
	tr.noteWrite("reel.mov", 4096, 1<<20, 1<<20+4096)

	if plan, tracked := tr.inPlaceReadPlan("reel.mov", 64<<10); !tracked || plan != planHold {
		t.Fatalf("fresh writer: plan=%v tracked=%v, want a hold", plan, tracked)
	}

	// Age the writer past the liveness window without sleeping for it.
	tr.mu.Lock()
	tr.m["reel.mov"].lastWrite = time.Now().Add(-2 * pin.SpoolIncompleteStallWindow)
	tr.mu.Unlock()

	if plan, _ := tr.inPlaceReadPlan("reel.mov", 64<<10); plan == planHold {
		t.Fatal("a hole whose writer went quiet is STILL held — the client retries a " +
			"JUKEBOX forever on a file nobody is writing")
	}
}

// The stale sweep must actually drop the record, or a writer that died mid-hole
// taxes every read in the process for the life of the process.
func TestStaleHoleRecordsAreSweptAndTheGateReturnsToZero(t *testing.T) {
	var tr inPlaceTracker
	tr.noteWrite("a.mov", 0, 1<<20, 1<<20+4096)
	tr.noteWrite("b.mov", 0, 1<<20, 1<<20+4096)
	if got := tr.holes.Load(); got != 2 {
		t.Fatalf("holes = %d after two out-of-order writes, want 2", got)
	}

	if n := tr.evictStale(time.Hour); n != 0 {
		t.Errorf("evicted %d fresh records, want 0", n)
	}

	tr.mu.Lock()
	for _, h := range tr.m {
		h.lastWrite = time.Now().Add(-2 * time.Hour)
	}
	tr.mu.Unlock()

	if n := tr.evictStale(time.Hour); n != 2 {
		t.Errorf("evicted %d stale records, want 2", n)
	}
	if got := tr.holes.Load(); got != 0 {
		t.Errorf("holes gate = %d after the sweep, want 0 — a leaked increment taxes "+
			"every read in the process", got)
	}
}

// forget must keep the gate exactly in step with the map. A leaked increment is
// a permanent tax; a missed one silently disables the guard.
func TestForgetKeepsTheGateInStepWithTheMap(t *testing.T) {
	var tr inPlaceTracker
	tr.noteWrite("a.mov", 0, 1<<20, 1<<20+4096)
	tr.forget("a.mov")
	if got := tr.holes.Load(); got != 0 {
		t.Errorf("holes = %d after forget, want 0", got)
	}
	tr.forget("a.mov") // idempotent — must not go negative
	if got := tr.holes.Load(); got != 0 {
		t.Errorf("holes = %d after a repeat forget, want 0 (a negative gate disables "+
			"the guard entirely)", got)
	}
}

// Shrinking a file must pull the readable prefix DOWN with it. Leaving it high
// would mark truncated-away bytes readable.
func TestTruncateDownClampsTheReadablePrefix(t *testing.T) {
	var tr inPlaceTracker
	tr.noteTruncate("x.mov", 0, 8<<20) // preallocate: hole [0, 8 MiB)
	tr.noteWrite("x.mov", 0, 0, 4<<20) // fill the first half
	tr.noteTruncate("x.mov", 0, 1<<20) // shrink to 1 MiB

	cend, wend, _, ok := tr.routing("x.mov")
	if ok && cend > wend {
		t.Fatalf("after shrink cend=%d > wend=%d — the prefix outran the file", cend, wend)
	}
	if ok && cend > 1<<20 {
		t.Fatalf("after shrink to 1 MiB the readable prefix is %d — truncated-away bytes "+
			"are marked readable", cend)
	}
}

// A tracked hole must not survive the file being deleted, or the next file at
// that name inherits a phantom hole. Same class as the #104 black-frame bug,
// where a stale write-size mark outlived its file.
func TestDeleteForgetsTheHole(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	seedFile(t, jfs, store, fuseRoot, "gone.mov", bytes.Repeat([]byte{'A'}, 4096))
	writeVia(t, jfs, "gone.mov", 1<<20, bytes.Repeat([]byte{'B'}, 4096))
	if jfs.handler.inPlaceHoles.holes.Load() == 0 {
		t.Fatal("precondition: no hole armed")
	}
	if err := jfs.Remove("gone.mov"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if n := jfs.handler.inPlaceHoles.holes.Load(); n != 0 {
		t.Errorf("holes = %d after delete — the next file at this name inherits a "+
			"phantom hole (the #104 class)", n)
	}
}

func TestOffsetsInsideTheValidPrefixAreNotIncomplete(t *testing.T) {
	var tr inPlaceTracker
	tr.noteWrite("r.mov", 4096, 1<<20, 1<<20+4096)
	for _, off := range []int64{0, 1024, 4095} {
		if plan, tracked := tr.inPlaceReadPlan("r.mov", off); tracked && plan == planHold {
			t.Errorf("offset %d inside the valid prefix was held", off)
		}
	}
}

// An untracked path must cost nothing and never hold. This is the state the
// process is in essentially all the time.
func TestUntrackedPathIsNeverHeld(t *testing.T) {
	var tr inPlaceTracker
	if plan, tracked := tr.inPlaceReadPlan("anything.mov", 1<<30); tracked {
		t.Errorf("an untracked path reported tracked=%v plan=%v", tracked, plan)
	}
}

func TestSharedCoalescerMatchesTheSpoolEntry(t *testing.T) {
	// The spool now delegates to advanceContig. Drive both through the same
	// out-of-order sequence and require identical prefixes, so a future edit to
	// one cannot silently diverge from the other.
	seq := [][2]int64{{4096, 8192}, {1 << 20, 1<<20 + 4096}, {8192, 1 << 20}, {0, 4096}}

	var cend int64
	var exts []spoolExtent
	e := &SpoolEntry{}
	for _, w := range seq {
		cend, exts = advanceContig(cend, exts, w[0], w[1])
		e.advanceContiguousLocked(w[0], w[1])
		if e.contiguousEnd != cend {
			t.Fatalf("after write [%d,%d): SpoolEntry cend=%d, shared cend=%d",
				w[0], w[1], e.contiguousEnd, cend)
		}
	}
	if cend != 1<<20+4096 {
		t.Errorf("final contiguousEnd = %d, want %d (all four writes coalesce to one run)",
			cend, 1<<20+4096)
	}
	_ = exts
}

var _ = os.O_RDONLY

// What the guard costs the READ HOT PATH. The claim in inplace_contig.go is
// "one atomic load when no file anywhere has a hole", and that is the state the
// process is in essentially all the time — including during an ordinary
// sequential copy. Measure it rather than asserting it.
func BenchmarkInPlaceGuardIdle(b *testing.B) {
	var tr inPlaceTracker
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr.inPlaceReadPlan("some/reel.mov", 1<<20)
	}
}

// One file somewhere has a hole, but not the one being read. This is the
// realistic worst case during a copy that went out of order: every read in the
// process now pays a lock plus a map miss.
func BenchmarkInPlaceGuardOtherFileHasAHole(b *testing.B) {
	var tr inPlaceTracker
	tr.noteWrite("other/clip.mov", 0, 1<<20, 1<<20+4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr.inPlaceReadPlan("some/reel.mov", 1<<20)
	}
}

func BenchmarkInPlaceGuardThisFileHasAHole(b *testing.B) {
	var tr inPlaceTracker
	tr.noteWrite("some/reel.mov", 4096, 1<<20, 1<<20+4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr.inPlaceReadPlan("some/reel.mov", 64<<10)
	}
}

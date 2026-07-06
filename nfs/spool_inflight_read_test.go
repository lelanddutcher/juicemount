package nfs

import (
	"bytes"
	"io"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// TestSpoolInFlightReadNeverServesHoleAsZeros is the regression test for the
// data-corruption bug where an NLE reading a still-copying file (preallocated
// via ftruncate, or written out of order) received ZEROS from an unwritten
// hole as if they were real data — black frames / corrupt RAW. The read shadow
// must clamp to the contiguous-written prefix and never fabricate zeros.
func TestSpoolInFlightReadNeverServesHoleAsZeros(t *testing.T) {
	s := newTestSpoolStore(t, 0) // unlimited
	e, err := s.OpenWrite("/clip.mov")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}

	const allocated = 1 << 20                     // 1 MiB preallocated
	const written = 4096                          // only the first 4 KiB actually written
	if err := e.Truncate(allocated); err != nil { // preallocate (creates a hole)
		t.Fatalf("Truncate preallocate: %v", err)
	}
	data := bytes.Repeat([]byte{0xAB}, written)
	if _, err := e.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	if got := e.ContiguousEnd(); got != written {
		t.Fatalf("ContiguousEnd = %d, want %d (only the written prefix)", got, written)
	}
	if got := e.WrittenEnd(); got != allocated {
		t.Fatalf("WrittenEnd = %d, want %d (the preallocated high-water)", got, allocated)
	}
	// The GETATTR/Stat/Lstat FileInfo reports the WRITTEN HIGH-WATER (writtenEnd),
	// NOT the contiguous prefix (#85/#65/#38 fix). Here the writer preallocated to
	// `allocated` via ftruncate, so writtenEnd == allocated — an over-report the fix
	// deliberately accepts (see the read-clamp checks below: the reported size is
	// DECOUPLED from what the read path will actually serve, and a read into the
	// still-unfilled hole holds/JUKEBOXes rather than fabricating zeros).
	if fi := spoolFileInfoForEntry("clip.mov", e); fi.Size() != allocated {
		t.Fatalf("FileInfo.Size = %d, want %d (written high-water)", fi.Size(), allocated)
	}
	// DECOUPLING PROOF: despite the larger reported size above, a read into the
	// still-unfilled hole must STILL return ErrSpoolIncomplete (a JUKEBOX hold), not
	// zeros — the reported size and the readable prefix are independent. (Re-asserted
	// concretely at case (2) below; stated here to bind it to the size change.)
	{
		probe := make([]byte, 16)
		rf0 := &spoolReadFile{name: "/clip.mov", entry: e}
		n0, err0 := rf0.ReadAt(probe, int64(written)+16) // inside [contiguousEnd, writtenEnd)
		_ = rf0.Close()
		if n0 != 0 || !pin.IsSpoolIncomplete(err0) {
			t.Fatalf("hole read despite full reported size: n=%d err=%v, want n=0 err=ErrSpoolIncomplete (no zeros)", n0, err0)
		}
	}

	rf := &spoolReadFile{name: "/clip.mov", entry: e}
	defer rf.Close()

	// (1) Read within the written prefix: correct bytes.
	buf := make([]byte, written)
	n, err := rf.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt(0): %v", err)
	}
	if n != written || !bytes.Equal(buf[:n], data) {
		t.Fatalf("ReadAt(0): n=%d want %d, bytes match=%v", n, written, bytes.Equal(buf[:n], data))
	}

	// (2) Read ENTIRELY inside the hole (off in [contiguousEnd, writtenEnd), the
	// writer still active): 0 bytes — NEVER zeros — and now a JUKEBOX hold
	// (ErrSpoolIncomplete) rather than EOF (task #65). EOF here would let the
	// client treat the partially-arrived file as COMPLETE — a silent truncation;
	// the sentinel makes onRead hold + retry until the bytes land.
	hole := make([]byte, 16)
	n, err = rf.ReadAt(hole, allocated/2)
	if n != 0 || !pin.IsSpoolIncomplete(err) {
		t.Fatalf("hole read: got n=%d err=%v, want n=0 err=ErrSpoolIncomplete (no zeros, hold-not-EOF)", n, err)
	}
	if !rf.IncompleteAt(allocated / 2) {
		t.Fatalf("IncompleteAt(hole) = false, want true (in-flight hole must JUKEBOX)")
	}
	if rf.IncompleteAt(0) {
		t.Fatalf("IncompleteAt(in-prefix) = true, want false")
	}

	// (3) Read SPANNING the contiguous boundary: only the written part returns.
	span := make([]byte, 4096)
	n, err = rf.ReadAt(span, written-2048) // starts 2KiB before the boundary
	if err != nil && err != io.EOF {
		t.Fatalf("span read: %v", err)
	}
	if n != 2048 {
		t.Fatalf("span read: n=%d, want 2048 (clamped to contiguous end, no hole zeros)", n)
	}
	if !bytes.Equal(span[:n], data[written-2048:]) {
		t.Fatalf("span read returned wrong bytes")
	}

	// (4) Read at/past the WRITTEN high-water (off >= writtenEnd): a genuine
	// past-end — the file merely appears to grow — so io.EOF (NOT JUKEBOX), and
	// IncompleteAt is false there (nothing more is expected below the offset).
	end := make([]byte, 16)
	n, err = rf.ReadAt(end, allocated+4096)
	if n != 0 || err != io.EOF {
		t.Fatalf("past-end read: got n=%d err=%v, want n=0 err=EOF", n, err)
	}
	if rf.IncompleteAt(allocated + 4096) {
		t.Fatalf("IncompleteAt(past-end) = true, want false")
	}
}

// TestSpoolSequentialReadTracksContiguous confirms the common cp/Finder
// sequential write keeps contiguousEnd == writtenEnd (no false short reads).
func TestSpoolSequentialReadTracksContiguous(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, _ := s.OpenWrite("/seq.bin")
	chunk := bytes.Repeat([]byte{0x5A}, 65536)
	for i := 0; i < 8; i++ {
		if _, err := e.WriteAt(chunk, int64(i)*65536); err != nil {
			t.Fatalf("WriteAt %d: %v", i, err)
		}
	}
	if e.ContiguousEnd() != 8*65536 || e.WrittenEnd() != 8*65536 {
		t.Fatalf("sequential: contiguous=%d written=%d, want both %d", e.ContiguousEnd(), e.WrittenEnd(), 8*65536)
	}
	rf := &spoolReadFile{name: "/seq.bin", entry: e}
	defer rf.Close()
	buf := make([]byte, 8*65536)
	n, err := rf.ReadAt(buf, 0)
	if (err != nil && err != io.EOF) || n != 8*65536 {
		t.Fatalf("full read: n=%d err=%v, want %d", n, err, 8*65536)
	}
}

// TestSpoolOutOfOrderCoalescesContiguousEnd is the regression test for the
// end-of-export "connection interrupted" bug. Under macOS's parallel / out-of-
// order WRITE dispatch, contiguousEnd used to advance only to the filling
// write's end, leaving it STUCK below already-written higher bytes (a 1GiB cp
// was observed stuck at 6MiB) until finalize ~30s later — so an end-of-export
// re-READ of the already-written tail JUKEBOX-held for up to the 90s stall
// window, past the client's ~40s soft-mount timeout. advanceContiguousLocked
// now COALESCES: filling a gap pulls in every already-written extent above it,
// so contiguousEnd reaches writtenEnd IMMEDIATELY (no finalize) and the tail
// read serves real bytes at once. PRE-FIX this test fails at the post-fill
// ContiguousEnd assertion (stuck at 8192) and the tail JUKEBOXes.
func TestSpoolOutOfOrderCoalescesContiguousEnd(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, err := s.OpenWrite("/oof.bin")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}

	blk0 := bytes.Repeat([]byte{0xC0}, 4096)
	blk8 := bytes.Repeat([]byte{0xC8}, 4096)
	blk4 := bytes.Repeat([]byte{0xC4}, 4096)

	// Out-of-order: [0,4K), then [8K,12K) — leaves a GENUINE hole at [4K,8K).
	if _, err := e.WriteAt(blk0, 0); err != nil {
		t.Fatalf("WriteAt 0: %v", err)
	}
	if _, err := e.WriteAt(blk8, 8192); err != nil {
		t.Fatalf("WriteAt 8192: %v", err)
	}
	if got := e.ContiguousEnd(); got != 4096 {
		t.Fatalf("pre-fill ContiguousEnd = %d, want 4096 (hole below the far write)", got)
	}
	// A read into the GENUINE hole [4K,8K) must still JUKEBOX — never serve zeros.
	{
		rf := &spoolReadFile{name: "/oof.bin", entry: e}
		hole := make([]byte, 16)
		n, herr := rf.ReadAt(hole, 5000)
		_ = rf.Close()
		if n != 0 || !pin.IsSpoolIncomplete(herr) {
			t.Fatalf("genuine-hole read: n=%d err=%v, want n=0 ErrSpoolIncomplete (no zeros)", n, herr)
		}
	}

	// Fill the gap [4K,8K). THE FIX: contiguousEnd must jump straight to 12K,
	// coalescing the already-written [8K,12K) — not stop at 8K.
	if _, err := e.WriteAt(blk4, 4096); err != nil {
		t.Fatalf("WriteAt 4096 (fill): %v", err)
	}
	if got := e.ContiguousEnd(); got != 12288 {
		t.Fatalf("post-fill ContiguousEnd = %d, want 12288 (coalesced past already-written [8K,12K)); pre-fix it stuck at 8192 and the tail JUKEBOXed", got)
	}
	if got := e.WrittenEnd(); got != 12288 {
		t.Fatalf("WrittenEnd = %d, want 12288", got)
	}

	// The end-of-export re-READ of the already-written tail [8K,12K) must serve
	// the real bytes immediately — NO JUKEBOX, NO finalize. This is the exact
	// read that produced "connection interrupted" pre-fix.
	rf := &spoolReadFile{name: "/oof.bin", entry: e}
	defer rf.Close()
	tail := make([]byte, 4096)
	n, err := rf.ReadAt(tail, 8192)
	if err != nil && err != io.EOF {
		t.Fatalf("tail ReadAt: %v (want the bytes, not a JUKEBOX hold)", err)
	}
	if n != 4096 || !bytes.Equal(tail[:n], blk8) {
		t.Fatalf("tail read: n=%d match=%v, want 4096 + correct bytes (no JUKEBOX, no zeros)", n, bytes.Equal(tail[:n], blk8))
	}
}

// TestSpoolFinalizeForcesContiguousOverGenuineHole locks the finalize safety
// net: a preallocated-but-only-partially-filled file has a GENUINE sparse hole
// (contiguousEnd < writtenEnd even after coalescing — nothing filled it). On
// finalize the writer is gone and the file is complete (the hole's zeros ARE
// its content), so contiguousEnd advances to writtenEnd and the file is fully
// readable. Distinct from the out-of-order case, which now coalesces mid-write.
func TestSpoolFinalizeForcesContiguousOverGenuineHole(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, err := s.OpenWrite("/prealloc.bin")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	const size = 12288
	if err := e.Truncate(size); err != nil { // preallocate → hole [0,size)
		t.Fatalf("Truncate: %v", err)
	}
	if _, err := e.WriteAt(bytes.Repeat([]byte{0xAA}, 4096), 0); err != nil { // fill only [0,4K)
		t.Fatalf("WriteAt: %v", err)
	}
	if got := e.ContiguousEnd(); got != 4096 {
		t.Fatalf("pre-finalize ContiguousEnd = %d, want 4096 (genuine hole above)", got)
	}
	if err := e.Close(); err != nil { // finalize
		t.Fatalf("Close/finalize: %v", err)
	}
	if got := e.ContiguousEnd(); got != size {
		t.Fatalf("post-finalize ContiguousEnd = %d, want %d (finalize force-advance safety net)", got, size)
	}
}

// TestSpoolShadowReportsFullSizeImmediately is the direct regression for
// #85/#65/#38: after sequential writes past one JuiceFS block (>4 MiB) but
// BEFORE finalize/drain, the Stat/Lstat shadow must report the FULL written
// high-water — not the contiguous prefix, and not a block-boundary truncation.
// Pre-fix (size == ContiguousEnd) this was still green for a purely sequential
// writer, so the discriminating case is TestSpoolShadowSizeMonotonicOutOfOrder
// below; this test locks in the common cp/Finder shape and the >4 MiB scale.
func TestSpoolShadowReportsFullSizeImmediately(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, err := s.OpenWrite("/big.mov")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	defer e.Close()

	const blk = 1 << 20 // 1 MiB
	const total = 6 * blk
	chunk := bytes.Repeat([]byte{0x11}, blk)
	for i := 0; i < 6; i++ {
		if _, err := e.WriteAt(chunk, int64(i)*blk); err != nil {
			t.Fatalf("WriteAt %d: %v", i, err)
		}
	}
	// No finalize/drain: the entry is still in-flight.
	if got := e.WrittenEnd(); got != total {
		t.Fatalf("WrittenEnd = %d, want %d", got, total)
	}
	if fi := spoolFileInfoForEntry("big.mov", e); fi.Size() != total {
		t.Fatalf("shadow FileInfo.Size = %d, want %d (full written high-water, not a truncated block prefix)", fi.Size(), total)
	}
}

// TestSpoolShadowSizeMonotonicOutOfOrder is the discriminating regression that
// FAILS pre-fix: an out-of-order writer lands block 2 before block 1, so
// contiguousEnd LAGS at the first hole while writtenEnd already covers the
// far block. The shadow size must track writtenEnd (== max(off+len)), never
// regress, and never report the lagged contiguous prefix. Pre-fix the shadow
// reported ContiguousEnd() — here that would be 0 (nothing contiguous from 0
// yet), the exact ~4 MiB / block-boundary truncation the bug describes.
func TestSpoolShadowSizeMonotonicOutOfOrder(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, err := s.OpenWrite("/oof-size.bin")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	defer e.Close()

	const blk = 4096
	// Write block 2 ([blk,2*blk)) BEFORE block 1 ([0,blk)). After the first
	// write there is a hole at [0,blk): contiguousEnd == 0, writtenEnd == 2*blk.
	if _, err := e.WriteAt(bytes.Repeat([]byte{0x22}, blk), blk); err != nil {
		t.Fatalf("WriteAt block2: %v", err)
	}
	if got := e.ContiguousEnd(); got != 0 {
		t.Fatalf("ContiguousEnd = %d, want 0 (leading hole) — pre-condition for the discriminating case", got)
	}
	if got := e.WrittenEnd(); got != 2*blk {
		t.Fatalf("WrittenEnd = %d, want %d", got, 2*blk)
	}
	// Shadow size must be the high-water, NOT the lagged (0) contiguous prefix.
	if fi := spoolFileInfoForEntry("oof-size.bin", e); fi.Size() != 2*blk {
		t.Fatalf("shadow FileInfo.Size = %d, want %d (writtenEnd high-water, not lagged contiguousEnd=0)", fi.Size(), 2*blk)
	}
	prev := int64(2 * blk)

	// Now fill block 1 ([0,blk)): contiguousEnd advances to 2*blk, writtenEnd
	// unchanged. Reported size must not regress and stays == writtenEnd.
	if _, err := e.WriteAt(bytes.Repeat([]byte{0x11}, blk), 0); err != nil {
		t.Fatalf("WriteAt block1: %v", err)
	}
	if fi := spoolFileInfoForEntry("oof-size.bin", e); fi.Size() < prev || fi.Size() != e.WrittenEnd() {
		t.Fatalf("shadow FileInfo.Size = %d, want == writtenEnd %d and >= prev %d (monotonic, never regress)", fi.Size(), e.WrittenEnd(), prev)
	}
	prev = e.WrittenEnd()

	// Extend past both blocks with a far write: high-water grows, size follows.
	if _, err := e.WriteAt(bytes.Repeat([]byte{0x33}, blk), 4*blk); err != nil {
		t.Fatalf("WriteAt far: %v", err)
	}
	if got := e.WrittenEnd(); got != 5*blk {
		t.Fatalf("WrittenEnd after far write = %d, want %d (max(off+len))", got, 5*blk)
	}
	if fi := spoolFileInfoForEntry("oof-size.bin", e); fi.Size() != 5*blk || fi.Size() < prev {
		t.Fatalf("shadow FileInfo.Size = %d, want %d (>= max(off+len), never regress from %d)", fi.Size(), 5*blk, prev)
	}
}

// TestSpoolShadowTruncateShrinkHonored proves a client-commanded shrink is NOT
// masked by the reported high-water: writes grow writtenEnd, then an
// authoritative SETATTR{size}/Truncate down lowers it, and the shadow size must
// follow the truncate — never keep reporting the stale (larger) high-water.
func TestSpoolShadowTruncateShrinkHonored(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, err := s.OpenWrite("/shrink.bin")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	defer e.Close()

	const blk = 4096
	if _, err := e.WriteAt(bytes.Repeat([]byte{0xEE}, 4*blk), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if fi := spoolFileInfoForEntry("shrink.bin", e); fi.Size() != 4*blk {
		t.Fatalf("pre-truncate shadow size = %d, want %d", fi.Size(), 4*blk)
	}

	// Authoritative shrink to a single block.
	if err := e.Truncate(blk); err != nil {
		t.Fatalf("Truncate shrink: %v", err)
	}
	if got := e.WrittenEnd(); got != blk {
		t.Fatalf("WrittenEnd after Truncate = %d, want %d (shrink must lower the high-water)", got, blk)
	}
	if fi := spoolFileInfoForEntry("shrink.bin", e); fi.Size() != blk {
		t.Fatalf("post-truncate shadow size = %d, want %d (must follow the shrink, not the stale high-water)", fi.Size(), blk)
	}
}

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

// TestSpoolFinalizeAdvancesContiguousEnd is the regression test for task #65's
// CORE bug: under parallel / out-of-order NFS WRITEs, the in-flight contiguousEnd
// tracker advances only on in-order writes and LAGS far behind writtenEnd (a 1GiB
// cp was observed stuck at 6MiB). The Stat read-shadow reports contiguousEnd as
// the file SIZE, so a lagged value made the NFS client cap reads and silently
// TRUNCATE the tail of a file read while it drained (the client never even issued
// a tail READ). On finalize the writer has closed and every byte 0..writtenEnd is
// on disk, so contiguousEnd must advance to writtenEnd → the full file readable.
func TestSpoolFinalizeAdvancesContiguousEnd(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, err := s.OpenWrite("/oof.bin")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}

	// Out-of-order writes that leave contiguousEnd LAGGING: [0,4K), then [8K,12K)
	// (a hole at [4K,8K) → contiguousEnd stays 4K), then fill [4K,8K). The advance
	// only bumps contiguousEnd to the filling write's end (8K), NOT past the
	// already-written [8K,12K), so contiguousEnd lags at 8K while writtenEnd=12K.
	blk := bytes.Repeat([]byte{0xCD}, 4096)
	for _, off := range []int64{0, 8192, 4096} {
		if _, err := e.WriteAt(blk, off); err != nil {
			t.Fatalf("WriteAt %d: %v", off, err)
		}
	}
	if got := e.WrittenEnd(); got != 12288 {
		t.Fatalf("WrittenEnd = %d, want 12288", got)
	}
	if got := e.ContiguousEnd(); got >= e.WrittenEnd() {
		t.Fatalf("ContiguousEnd = %d — expected it to LAG behind writtenEnd %d pre-finalize (the bug)", got, e.WrittenEnd())
	}

	// Finalize (writer closed). Every byte is on disk now → contiguousEnd must
	// advance to writtenEnd.
	if err := e.Close(); err != nil {
		t.Fatalf("Close/finalize: %v", err)
	}
	if got := e.ContiguousEnd(); got != 12288 {
		t.Fatalf("ContiguousEnd after finalize = %d, want 12288 (= writtenEnd); otherwise the Stat shadow reports short and the NFS client truncates the tail", got)
	}

	// The read shadow now serves the tail that was previously past contiguousEnd.
	rf := &spoolReadFile{name: "/oof.bin", entry: e}
	defer rf.Close()
	tail := make([]byte, 4096)
	n, err := rf.ReadAt(tail, 8192)
	if err != nil && err != io.EOF {
		t.Fatalf("tail ReadAt: %v", err)
	}
	if n != 4096 || !bytes.Equal(tail[:n], blk) {
		t.Fatalf("tail read after finalize: n=%d match=%v, want 4096 + correct bytes (no truncation)", n, bytes.Equal(tail[:n], blk))
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

package nfs

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// TestInsertExtentCoalesces unit-tests the coalescing insert used by
// advanceContiguousLocked: disjoint ranges stay separate + sorted; touching or
// overlapping ranges merge.
func TestInsertExtentCoalesces(t *testing.T) {
	// Disjoint, inserted out of order → sorted, two extents.
	x := insertExtent(nil, 100, 200)
	x = insertExtent(x, 0, 50)
	if len(x) != 2 || x[0] != (spoolExtent{0, 50}) || x[1] != (spoolExtent{100, 200}) {
		t.Fatalf("disjoint insert: %+v", x)
	}
	// Adjacent (touching at 50 and 100) → all merge into one [0,200).
	x = insertExtent(x, 50, 100)
	if len(x) != 1 || x[0] != (spoolExtent{0, 200}) {
		t.Fatalf("adjacent merge: %+v", x)
	}
	// Overlapping → merge into the union.
	y := insertExtent(nil, 0, 100)
	y = insertExtent(y, 50, 150)
	if len(y) != 1 || y[0] != (spoolExtent{0, 150}) {
		t.Fatalf("overlap merge: %+v", y)
	}
}

// TestClipExtents unit-tests the shrink-time extent pruning.
func TestClipExtents(t *testing.T) {
	x := clipExtents([]spoolExtent{{0, 50}, {100, 200}, {300, 400}}, 150)
	if len(x) != 2 || x[0] != (spoolExtent{0, 50}) || x[1] != (spoolExtent{100, 150}) {
		t.Fatalf("clip: %+v", x)
	}
	if got := clipExtents([]spoolExtent{{100, 200}}, 100); got != nil {
		t.Fatalf("clip at boundary should drop start>=size, got %+v", got)
	}
}

// TestSpoolMultiGapCoalesce covers more than one out-of-order gap: filling the
// lower gap coalesces up to the NEXT gap, and filling that one coalesces to the
// end. The old single-chunk advance would have left contiguousEnd stuck at each
// filling write's end.
func TestSpoolMultiGapCoalesce(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, err := s.OpenWrite("/multi.bin")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	blk := bytes.Repeat([]byte{0xAB}, 4096)
	// [0,4K), [8K,12K), [16K,20K): two holes at [4K,8K) and [12K,16K).
	for _, off := range []int64{0, 8192, 16384} {
		if _, err := e.WriteAt(blk, off); err != nil {
			t.Fatalf("WriteAt %d: %v", off, err)
		}
	}
	if got := e.ContiguousEnd(); got != 4096 {
		t.Fatalf("initial ContiguousEnd = %d, want 4096", got)
	}
	// Fill [4K,8K): coalesce through [8K,12K) → 12K; the [12K,16K) hole blocks.
	if _, err := e.WriteAt(blk, 4096); err != nil {
		t.Fatalf("fill gap1: %v", err)
	}
	if got := e.ContiguousEnd(); got != 12288 {
		t.Fatalf("after gap1 fill ContiguousEnd = %d, want 12288", got)
	}
	// Fill [12K,16K): coalesce through [16K,20K) → 20K (fully contiguous).
	if _, err := e.WriteAt(blk, 12288); err != nil {
		t.Fatalf("fill gap2: %v", err)
	}
	if got, want := e.ContiguousEnd(), int64(20480); got != want {
		t.Fatalf("after gap2 fill ContiguousEnd = %d, want %d", got, want)
	}
	if got := e.WrittenEnd(); got != 20480 {
		t.Fatalf("WrittenEnd = %d, want 20480", got)
	}
}

// TestSpoolTruncateShrinkClipsExtents proves a SETATTR/Truncate shrink drops
// recorded out-of-order extents above the new size, so a later gap-fill can't
// advance contiguousEnd past bytes that no longer exist.
func TestSpoolTruncateShrinkClipsExtents(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, err := s.OpenWrite("/shrink-oof.bin")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	// [0,4K) then far [16K,20K): contiguousEnd=4K, an extent [16K,20K) recorded.
	if _, err := e.WriteAt(bytes.Repeat([]byte{0x11}, 4096), 0); err != nil {
		t.Fatalf("w0: %v", err)
	}
	if _, err := e.WriteAt(bytes.Repeat([]byte{0x22}, 4096), 16384); err != nil {
		t.Fatalf("w16: %v", err)
	}
	// Shrink to 8K — discards the [16K,20K) extent.
	if err := e.Truncate(8192); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	// Now fill [4K,8K). contiguousEnd must reach only 8K (the discarded far
	// extent must NOT resurrect and pull it to 20K).
	if _, err := e.WriteAt(bytes.Repeat([]byte{0x33}, 4096), 4096); err != nil {
		t.Fatalf("fill: %v", err)
	}
	if got := e.ContiguousEnd(); got != 8192 {
		t.Fatalf("ContiguousEnd = %d, want 8192 (clipped extent must not resurrect)", got)
	}
	if got := e.WrittenEnd(); got != 8192 {
		t.Fatalf("WrittenEnd = %d, want 8192", got)
	}
}

// TestErrSpoolBusyIsRetryable proves the cause-#3 wiring: the nfs-package
// ErrSpoolBusy is recognized by pin.IsSpoolBusy, so onWrite maps it to the
// retryable NFS3ERR_JUKEBOX instead of a hard EACCES that aborts an export at
// its final (faststart moov seek-back) WRITE.
func TestErrSpoolBusyIsRetryable(t *testing.T) {
	if !pin.IsSpoolBusy(ErrSpoolBusy) {
		t.Fatalf("pin.IsSpoolBusy(ErrSpoolBusy) = false — onWrite would return a hard EACCES, not a retryable JUKEBOX")
	}
	// Identity still works for in-package callers/tests.
	if !errors.Is(ErrSpoolBusy, ErrSpoolBusy) {
		t.Fatalf("errors.Is(ErrSpoolBusy, ErrSpoolBusy) = false")
	}
}

// TestSpoolAppleDoubleReadBypassesHole is the control from the audit: a ._
// sidecar read into an in-flight hole must NOT JUKEBOX (it serves the sparse
// spool file directly, #100) — proving the bypass is ._-name-only and that the
// coalesce fix, not the bypass, is what covers real media files.
func TestSpoolAppleDoubleReadBypassesHole(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, err := s.OpenWrite("/._sidecar")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	// Out-of-order leaving a genuine hole [4K,8K).
	if _, err := e.WriteAt(bytes.Repeat([]byte{0x11}, 4096), 0); err != nil {
		t.Fatalf("w0: %v", err)
	}
	if _, err := e.WriteAt(bytes.Repeat([]byte{0x22}, 4096), 8192); err != nil {
		t.Fatalf("w8: %v", err)
	}
	rf := &spoolReadFile{name: "/._sidecar", entry: e, isAppleDouble: true}
	defer rf.Close()
	buf := make([]byte, 16)
	_, rerr := rf.ReadAt(buf, 5000) // inside the hole
	if pin.IsSpoolIncomplete(rerr) {
		t.Fatalf("._ read JUKEBOXed at a hole offset — the ._ bypass is broken")
	}
}

// TestSpoolDrainEvictReadReturnsSpoolDrained is the cause-#4 regression: when
// the spool file is unlinked (drain completed + evicted) before a reader opens
// its fd, OpenForRead fails with a %w-WRAPPED ENOENT. The read path must map it
// to ErrSpoolDrained (→ NFS3ERR_NOENT → client reopens onto the drained FUSE
// copy), NOT a terminal I/O error. Pre-fix os.IsNotExist did not unwrap the %w,
// so the wrapped ENOENT fell through to NFS3ERR_IO and aborted the copy.
func TestSpoolDrainEvictReadReturnsSpoolDrained(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, err := s.OpenWrite("/evict.bin")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	if _, err := e.WriteAt(bytes.Repeat([]byte{0x5A}, 4096), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	// Unlink the spool file NAME (the writer's fd keeps the inode alive) so a
	// fresh OpenForRead sees ENOENT — the drain-evict race.
	if err := os.Remove(e.spoolFile); err != nil {
		t.Fatalf("unlink spool file: %v", err)
	}
	rf := &spoolReadFile{name: "/evict.bin", entry: e} // fd not yet opened
	defer rf.Close()
	buf := make([]byte, 16)
	n, rerr := rf.ReadAt(buf, 0)
	if n != 0 || !pin.IsSpoolDrained(rerr) {
		t.Fatalf("drain-evict read: n=%d err=%v, want n=0 ErrSpoolDrained (via errors.Is unwrap)", n, rerr)
	}
}

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

	const allocated = 1 << 20 // 1 MiB preallocated
	const written = 4096      // only the first 4 KiB actually written
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
	// The read-facing FileInfo must report the readable (contiguous) size, not
	// the preallocated size, so onRead clamps reads to real data.
	if fi := spoolFileInfoForEntry("clip.mov", e); fi.Size() != written {
		t.Fatalf("FileInfo.Size = %d, want %d (contiguous-written)", fi.Size(), written)
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

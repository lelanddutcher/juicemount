package nfs

import (
	"bytes"
	"io"
	"testing"
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

	// (2) Read ENTIRELY inside the hole: must be EOF/0 bytes, NEVER zeros.
	hole := make([]byte, 16)
	n, err = rf.ReadAt(hole, allocated/2)
	if n != 0 || err != io.EOF {
		t.Fatalf("hole read: got n=%d err=%v, want n=0 err=EOF (must not fabricate zeros)", n, err)
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

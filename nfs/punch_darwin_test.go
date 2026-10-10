//go:build darwin

package nfs

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// physicalBytes is the space the file actually occupies, which is the whole
// point of punching — st_size does not move, st_blocks does.
func physicalBytes(t *testing.T, path string) int64 {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return int64(st.Blocks) * 512
}

func writeFilled(t *testing.T, path string, size int, fill byte) {
	t.Helper()
	buf := bytes.Repeat([]byte{fill}, 1<<20)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for w := 0; w < size; w += len(buf) {
		n := len(buf)
		if size-w < n {
			n = size - w
		}
		if _, err := f.Write(buf[:n]); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

// Punching must actually return blocks. If it does not, the streaming drain
// frees capacity in its accounting that the disk never gets back — which turns
// a size cap into an ENOSPC further along, i.e. strictly worse than today.
func TestPunchRangeFreesPhysicalBlocks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spool.bin")
	const size = 8 << 20
	writeFilled(t, path, size, 0xAB)

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	before := physicalBytes(t, path)
	punched, err := punchRange(f, 0, 4<<20)
	if err != nil {
		t.Fatalf("punchRange: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	after := physicalBytes(t, path)

	if punched != 4<<20 {
		t.Errorf("reported %d bytes punched, want %d", punched, 4<<20)
	}
	if freed := before - after; freed < 3<<20 {
		t.Errorf("only %d bytes physically freed (%d -> %d) — the accounting would "+
			"release capacity the disk never returned", freed, before, after)
	}

	// Logical size must NOT move: the spool index, the Stat shadow and the
	// capacity accounting all key off it.
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != size {
		t.Errorf("logical size changed to %d, want %d — punching must be invisible "+
			"to everything that reads the file's size", fi.Size(), size)
	}
}

// Data ABOVE the punch must survive untouched. A primitive that over-punches
// destroys bytes that were never drained.
func TestPunchRangeLeavesTheTailIntact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spool.bin")
	const size = 8 << 20
	writeFilled(t, path, size, 0xAB)

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	const punchEnd = 4 << 20
	if _, err := punchRange(f, 0, punchEnd); err != nil {
		t.Fatalf("punchRange: %v", err)
	}

	tail := make([]byte, size-punchEnd)
	if _, err := f.ReadAt(tail, punchEnd); err != nil {
		t.Fatalf("read tail: %v", err)
	}
	for i, b := range tail {
		if b != 0xAB {
			t.Fatalf("tail corrupted at +%d: got 0x%02X want 0xAB — punch crossed "+
				"its requested end", punchEnd+i, b)
		}
	}
}

// A punched range reads as ZEROS with no error. This test does not assert a
// desirable property — it PINS the hazard, so nobody later assumes a read past
// a punched prefix will fail loudly. It is why the read shadow must route around
// punched ranges rather than rely on an error surfacing.
func TestPunchedRangeReadsAsSilentZeros(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spool.bin")
	const size = 8 << 20
	writeFilled(t, path, size, 0xAB)

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if _, err := punchRange(f, 0, 4<<20); err != nil {
		t.Fatalf("punchRange: %v", err)
	}

	buf := make([]byte, 4096)
	n, err := f.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("read of a punched range returned an error (%v) — if this ever "+
			"becomes true the read shadow could rely on it, so the change is "+
			"significant and this test should be re-thought, not deleted", err)
	}
	if n != len(buf) {
		t.Errorf("short read %d of %d", n, len(buf))
	}
	for i, b := range buf {
		if b != 0 {
			t.Fatalf("punched range byte %d is 0x%02X, expected 0 — the hazard model "+
				"assumes zeros", i, b)
		}
	}
}

// Alignment must move INWARD. Punching one block too few wastes 4 KiB; punching
// one block too many destroys undrained data. The asymmetry is total, so a
// range that is unaligned at either edge must shrink, never grow.
func TestPunchRangeAlignsInwardNeverOutward(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spool.bin")
	const size = 1 << 20
	writeFilled(t, path, size, 0xCD)

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Request [100, 8292) — both edges unaligned. Only [4096, 8192) is safely
	// punchable, so bytes 100..4095 and 8192..8291 must survive.
	punched, err := punchRange(f, 100, 8292)
	if err != nil {
		t.Fatalf("punchRange: %v", err)
	}
	if punched != 4096 {
		t.Errorf("punched %d bytes, want exactly one aligned block (4096)", punched)
	}

	head := make([]byte, 4096-100)
	if _, err := f.ReadAt(head, 100); err != nil {
		t.Fatal(err)
	}
	for i, b := range head {
		if b != 0xCD {
			t.Fatalf("byte %d below the first aligned block was punched — alignment "+
				"rounded OUTWARD and destroyed undrained data", 100+i)
		}
	}

	tail := make([]byte, 100)
	if _, err := f.ReadAt(tail, 8192); err != nil {
		t.Fatal(err)
	}
	for i, b := range tail {
		if b != 0xCD {
			t.Fatalf("byte %d above the last aligned block was punched — alignment "+
				"rounded OUTWARD", 8192+i)
		}
	}
}

// A sub-block request must punch nothing and report no error. Returning an error
// would make the caller treat an ordinary small-range no-op as a failure.
func TestPunchRangeSubBlockIsANoOp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spool.bin")
	writeFilled(t, path, 1<<20, 0xEE)

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	before := physicalBytes(t, path)
	for _, tc := range []struct{ off, end int64 }{
		{0, 100},    // entirely inside one block
		{100, 4000}, // unaligned both ends, same block
		{4096, 4096},
		{500, 400}, // end < off
		{-1, 4096}, // negative offset
	} {
		punched, err := punchRange(f, tc.off, tc.end)
		if err != nil {
			t.Errorf("punchRange(%d,%d) errored: %v", tc.off, tc.end, err)
		}
		if punched != 0 {
			t.Errorf("punchRange(%d,%d) punched %d bytes, want 0", tc.off, tc.end, punched)
		}
	}
	if after := physicalBytes(t, path); after != before {
		t.Errorf("physical size moved %d -> %d across no-op punches", before, after)
	}
}

// The capability probe must agree with reality on this filesystem, or the drain
// will pick the wrong strategy.
func TestPunchSupportedOnSpoolFilesystem(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "probe.bin")
	writeFilled(t, path, 1<<20, 0x01)

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if !punchSupported(f) {
		t.Skip("punching unsupported on this filesystem; streaming drain will use " +
			"the whole-file fallback here")
	}
	if _, err := punchRange(f, 0, 4096); err != nil {
		t.Errorf("punchSupported said yes but punchRange failed: %v — the probe and "+
			"the operation must agree or the drain picks the wrong strategy", err)
	}
}

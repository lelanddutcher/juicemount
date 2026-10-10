package nfs

// In-place FUSE write path: hole-read probe (2026-08-04).
//
// The SPOOL read path has an explicit never-serve-a-hole guard: every read is
// clamped to contiguousEnd (the end of the contiguously-written, hole-free
// prefix) and a read directed into [contiguousEnd, writtenEnd) is JUKEBOX-held
// via pin.ErrSpoolIncomplete rather than served as zeros
// (nfs/spool_readfile.go:110-159, nfs/spool.go advanceContiguousLocked).
//
// The IN-PLACE FUSE write path — taken when a WRITE RPC arrives for a path with
// NO active spool entry (nfs/handler.go:3124, `if _, active := spool.LookupActive`)
// — has no contiguousEnd analogue. This file establishes empirically what a read
// of an unwritten region returns on that path, and A/B's it against the spool
// path driven through the identical RPC call shapes.
//
// HISTORY: these began as CHARACTERIZATION assertions pinning the unguarded
// behavior, with the instruction "if the in-place path ever grows a hole guard,
// arm A/B here will fail — invert the assertion at that point". The guard
// landed (nfs/inplace_contig.go, task #6), all three in-place arms failed with
// exactly the intended error, and they are now inverted. They are REGRESSION
// tests: each asserts the hole is refused, so removing the guard fails them.
//
// ARM C (the spool control) is unchanged and still passes — proof the shared
// coalescer refactor did not disturb the path that was already correct.

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// holeGuard mirrors internal/nfs's unexported `incompleteReader` interface
// (internal/nfs/file.go:235). onRead consults it at nfs_onread.go:196; a read
// handle that does NOT implement it can never produce a JUKEBOX hold at a hole,
// so whatever its ReadAt returns is what the client receives as NFS3_OK data.
type holeGuard interface {
	IncompleteAt(off int64) bool
}

// readAtVia performs one NFS-READ-shaped read: OpenFile(O_RDONLY) → ReadAt →
// Close, exactly as internal/nfs/nfs_onread.go does per READ RPC. It returns
// the handle's concrete type alongside the read result so the caller can assert
// which read implementation served the RPC.
func readAtVia(t *testing.T, jfs *juiceFS, rel string, off int64, n int) (buf []byte, got int, err error, handle billyFileLike) {
	t.Helper()
	f, oerr := jfs.OpenFile(rel, os.O_RDONLY, 0)
	if oerr != nil {
		t.Fatalf("OpenFile(read %s): %v", rel, oerr)
	}
	defer f.Close()
	ra, ok := f.(io.ReaderAt)
	if !ok {
		t.Fatalf("read handle for %s is not an io.ReaderAt (%T)", rel, f)
	}
	buf = make([]byte, n)
	got, err = ra.ReadAt(buf, off)
	return buf, got, err, f
}

// billyFileLike is just an alias for the interface OpenFile returns, kept so the
// probe can report the concrete handle type without importing billy here.
type billyFileLike interface {
	Close() error
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// TestInPlaceFUSEOutOfOrderExtendServesHoleAsZeros is ARM A.
//
// Sequence (all real RPC call shapes):
//
//	existing 4 KiB file on FUSE   (an in-place modify — NOT a fresh CREATE, so
//	                               it never routes through the spool)
//	WRITE RPC @ 1 MiB             (the out-of-order tail: macOS dispatches WRITE
//	                               RPCs in parallel, and this codebase's entire
//	                               contiguousEnd machinery exists because the
//	                               high-offset RPC routinely lands first)
//	GETATTR                       (client now believes the file is 1 MiB + 4 KiB)
//	READ RPC @ 64 KiB             (inside the region between old EOF and the
//	                               landed write — bytes that are still in flight)
func TestInPlaceFUSEOutOfOrderExtendServesHoleAsZeros(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t) // NO spool wired

	const (
		seedLen  = 4096
		tailOff  = 1 << 20
		holeOff  = 64 << 10
		readSize = 4096
	)
	seed := bytes.Repeat([]byte{'A'}, seedLen)
	seedFile(t, jfs, store, fuseRoot, "reel.mov", seed)

	// Establish that this path really is the in-place fdPool route, not the spool.
	wf, err := jfs.OpenFile("reel.mov", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile(write): %v", err)
	}
	if _, isSpool := wf.(*spoolWriteFile); isSpool {
		wf.Close()
		t.Fatalf("precondition: write routed to the spool (%T); this probe needs the in-place path", wf)
	}
	if _, isInPlace := wf.(*writeFile); !isInPlace {
		wf.Close()
		t.Fatalf("precondition: write handle is %T, want *writeFile (in-place fdPool path)", wf)
	}
	wf.Close()

	// The out-of-order tail WRITE RPC lands first, extending the file and
	// leaving [4 KiB, 1 MiB) as an unwritten hole.
	writeVia(t, jfs, "reel.mov", tailOff, bytes.Repeat([]byte{'B'}, seedLen))

	// GETATTR: what the client is told the file contains.
	fi, err := jfs.Stat("reel.mov")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() != tailOff+seedLen {
		t.Fatalf("Stat reports %d, want %d (the client must believe the hole is file content "+
			"for this to be a fabrication rather than a short read)", fi.Size(), tailOff+seedLen)
	}

	// READ RPC directed into the hole.
	buf, got, rerr, handle := readAtVia(t, jfs, "reel.mov", holeOff, readSize)

	_, guarded := handle.(holeGuard)
	t.Logf("ARM A  handle=%T implements incompleteReader=%v  ReadAt(off=%d,len=%d) => n=%d err=%v allZero=%v",
		handle, guarded, holeOff, readSize, got, rerr, allZero(buf[:got]))

	if !guarded {
		t.Fatalf("ARM A: the in-place read handle %T lost its hole guard — onRead's "+
			"size-clamp branch can no longer tell genuine EOF from not-yet-arrived", handle)
	}
	if !pin.IsSpoolIncomplete(rerr) {
		t.Fatalf("ARM A REGRESSION: in-place path served the hole instead of holding it: "+
			"n=%d err=%v allZero=%v. An NLE reading a still-copying clip gets black frames "+
			"(want pin.ErrSpoolIncomplete -> NFS3ERR_JUKEBOX)", got, rerr, allZero(buf[:got]))
	}
	if got != 0 {
		t.Errorf("ARM A: held read returned n=%d, want 0 — no bytes may escape a hold", got)
	}
	t.Logf("ARM A RESULT: in-place FUSE path refused the hole with %v.", rerr)
}

// TestInPlaceFUSEPreallocateServesHoleAsZeros is ARM B: the same question via
// SETATTR{size} (F_PREALLOCATE / ftruncate-to-grow), which internal/nfs applies
// as fs.OpenFile(O_WRONLY|O_EXCL) → fp.Truncate(size) → Close
// (internal/nfs/file.go:365-385). No write race is needed here — the hole is
// created by a single client-issued preallocate and persists for as long as the
// app takes to fill it.
func TestInPlaceFUSEPreallocateServesHoleAsZeros(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t) // NO spool wired

	const (
		seedLen   = 4096
		allocSize = 8 << 20
		holeOff   = 1 << 20
		readSize  = 4096
	)
	seedFile(t, jfs, store, fuseRoot, "export.mov", bytes.Repeat([]byte{'A'}, seedLen))

	// SETATTR{size} — the exact shape internal/nfs/file.go Apply uses.
	fp, err := jfs.OpenFile("export.mov", os.O_WRONLY|os.O_EXCL, 0)
	if err != nil {
		t.Fatalf("OpenFile(O_WRONLY|O_EXCL): %v", err)
	}
	if err := fp.Truncate(allocSize); err != nil {
		fp.Close()
		t.Fatalf("Truncate(%d): %v", allocSize, err)
	}
	if err := fp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// What the backing FUSE file now reports (this is what a GETATTR that falls
	// through to the real filesystem, and what onRead's LiveSize rescue, see).
	onDiskInfo, err := os.Stat(fuseRoot + "/export.mov")
	if err != nil {
		t.Fatalf("stat fuse file: %v", err)
	}
	statInfo, err := jfs.Stat("export.mov")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	t.Logf("ARM B  after preallocate: on-FUSE size=%d  juiceFS.Stat size=%d",
		onDiskInfo.Size(), statInfo.Size())
	if onDiskInfo.Size() != allocSize {
		t.Fatalf("ARM B precondition: preallocate did not grow the backing file (%d)", onDiskInfo.Size())
	}

	buf, got, rerr, handle := readAtVia(t, jfs, "export.mov", holeOff, readSize)
	_, guarded := handle.(holeGuard)
	t.Logf("ARM B  handle=%T implements incompleteReader=%v  ReadAt(off=%d,len=%d) => n=%d err=%v allZero=%v",
		handle, guarded, holeOff, readSize, got, rerr, allZero(buf[:got]))

	if !guarded {
		t.Fatalf("ARM B: read handle %T lost its hole guard", handle)
	}
	if !pin.IsSpoolIncomplete(rerr) {
		t.Fatalf("ARM B REGRESSION: preallocated hole served instead of held => n=%d err=%v "+
			"allZero=%v. This hole needs no write race — it persists for as long as the app "+
			"takes to fill it", got, rerr, allZero(buf[:got]))
	}
	t.Logf("ARM B RESULT: preallocated hole refused with %v.", rerr)
}

// TestSpoolDisabledNewFileCopyServesHoleAsZeros is ARM D — the one that decides
// whether this is an edge case or the default.
//
// The spool is NOW on by default (Preferences.swift `spoolEnabled: Bool = true`),
// which closed the worst of the reachability — but this arm still matters,
// because the in-place path remains reachable with the spool on: an in-place
// modify of an existing file, a drain-evict reopen, a SETATTR{size}
// preallocate, and any install where the user turned the spool off. This arm
// covers the last of those, which is also the simplest: with the spool off,
// juiceFS.Create takes the legacy branch and every WRITE RPC takes the in-place
// fdPool branch, so a PLAIN NEW-FILE copy runs entirely on this path.
//
// The out-of-order tail write is not hypothetical: this codebase's contiguousEnd
// machinery exists specifically because macOS dispatches WRITE RPCs on parallel
// goroutines and the high-offset RPC routinely lands first (nfs/spool.go:1880-1888,
// :2313-2315; :2600 records contiguousEnd observed stuck at 6 MiB during a 1 GiB cp).
func TestSpoolDisabledNewFileCopyServesHoleAsZeros(t *testing.T) {
	jfs, _, _ := newIntegrityHarness(t) // spool OFF

	const (
		tailOff  = 1 << 20
		chunk    = 4096
		holeOff  = 64 << 10
		readSize = 4096
	)

	// CREATE RPC (internal/nfs/nfs_oncreate.go → fs.Create, then Close).
	cf, err := jfs.Create("newclip.mov")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, isSpool := cf.(*spoolWriteFile); isSpool {
		cf.Close()
		t.Fatalf("precondition: spool is wired; this arm needs the default (spool off)")
	}
	if _, isInPlace := cf.(*writeFile); !isInPlace {
		cf.Close()
		t.Fatalf("precondition: Create returned %T, want *writeFile (legacy FUSE path)", cf)
	}
	if err := cf.Close(); err != nil {
		t.Fatalf("Create.Close: %v", err)
	}

	// Head WRITE lands, then the parallel tail WRITE at 1 MiB lands ahead of
	// everything between them.
	writeVia(t, jfs, "newclip.mov", 0, bytes.Repeat([]byte{'A'}, chunk))
	writeVia(t, jfs, "newclip.mov", tailOff, bytes.Repeat([]byte{'B'}, chunk))

	fi, err := jfs.Stat("newclip.mov")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() != tailOff+chunk {
		t.Fatalf("Stat reports %d, want %d", fi.Size(), tailOff+chunk)
	}

	buf, got, rerr, handle := readAtVia(t, jfs, "newclip.mov", holeOff, readSize)
	_, guarded := handle.(holeGuard)
	t.Logf("ARM D  handle=%T implements incompleteReader=%v  GETATTR size=%d  ReadAt(off=%d,len=%d) => n=%d err=%v allZero=%v",
		handle, guarded, fi.Size(), holeOff, readSize, got, rerr, allZero(buf[:got]))

	if !guarded {
		t.Fatalf("ARM D: read handle %T lost its hole guard", handle)
	}
	if !pin.IsSpoolIncomplete(rerr) {
		t.Fatalf("ARM D REGRESSION: with the spool off, a plain new-file copy served a "+
			"not-yet-written region instead of holding it => n=%d err=%v allZero=%v",
			got, rerr, allZero(buf[:got]))
	}
	t.Logf("ARM D RESULT: spool-off new-file copy refused the hole with %v.", rerr)
}

// TestSpoolPathGuardsTheSameHole is ARM C, the control. Identical RPC shapes,
// spool wired as production wires it. The spool MUST refuse to serve the hole.
func TestSpoolPathGuardsTheSameHole(t *testing.T) {
	jfs, _, _, _ := newSpoolWiredHandler(t)

	const (
		headLen  = 4096
		tailOff  = 1 << 20
		holeOff  = 64 << 10
		readSize = 4096
	)

	// CREATE RPC → spool entry.
	cf, err := jfs.Create("clip.mov")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, isSpool := cf.(*spoolWriteFile); !isSpool {
		cf.Close()
		t.Fatalf("precondition: Create returned %T, want *spoolWriteFile", cf)
	}
	if err := cf.Close(); err != nil {
		t.Fatalf("Create.Close: %v", err)
	}

	// In-order head WRITE, then the out-of-order tail WRITE at 1 MiB.
	writeVia(t, jfs, "clip.mov", 0, bytes.Repeat([]byte{'A'}, headLen))
	writeVia(t, jfs, "clip.mov", tailOff, bytes.Repeat([]byte{'B'}, headLen))

	buf, got, rerr, handle := readAtVia(t, jfs, "clip.mov", holeOff, readSize)
	_, guarded := handle.(holeGuard)
	t.Logf("ARM C  handle=%T implements incompleteReader=%v  ReadAt(off=%d,len=%d) => n=%d err=%v allZero=%v",
		handle, guarded, holeOff, readSize, got, rerr, allZero(buf[:got]))

	if !guarded {
		t.Fatalf("ARM C: spool read handle %T lost its hole guard", handle)
	}
	if !pin.IsSpoolIncomplete(rerr) {
		t.Fatalf("ARM C REGRESSION: spool served the hole instead of holding it: n=%d err=%v allZero=%v "+
			"(want pin.ErrSpoolIncomplete → NFS3ERR_JUKEBOX)", got, rerr, allZero(buf[:got]))
	}
	t.Logf("ARM C RESULT: spool path correctly refused the identical hole with %v.", rerr)
}

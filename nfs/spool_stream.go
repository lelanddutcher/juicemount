package nfs

import (
	"fmt"
	"os"
)

// STREAMING DRAIN — the ordered advance (S2).
//
// This is the step that actually removes the single-file size cap: bytes are
// copied to the backend and reclaimed from the spool WHILE the writer is still
// appending, so peak spool usage becomes a working-set window instead of the
// whole file.
//
// THE ORDER IS THE DESIGN. Four operations, and every adjacent pair has a
// failure that the ordering exists to prevent:
//
//	1. write prefix to dest
//	2. fsync dest                  <- before 3, or a crash loses bytes we then punch
//	3. publish punchedEnd          <- before 4, or a reader hits a hole and gets zeros
//	4. punch the spool prefix      <- before 5, or capacity is freed that the disk still holds
//	5. release capacity
//
// Getting 2 and 3 backwards loses data on a crash. Getting 3 and 4 backwards
// serves silent zeros to a live reader. Neither is recoverable, and neither
// produces an error at the time it happens — which is why this is a separate,
// injectable unit rather than inline code in the drainer: the ordering is
// testable here, and untestable there.
//
// EVERY FAILURE MUST LEAVE THE PREFIX INTACT. If any step fails, punchedEnd must
// not have moved and nothing may have been punched. A half-applied advance that
// published a boundary it did not durably back is exactly the corruption this
// whole design is trying to avoid.

// streamDest is the backend side of a streaming drain. An interface rather than
// *os.File so the ordering can be tested against a recorder — the bugs here are
// sequencing bugs, and they are invisible to a test that only checks final bytes.
type streamDest interface {
	WriteAt(p []byte, off int64) (int, error)
	Sync() error
}

// streamCapacityReleaser frees reclaimed spool bytes from the capacity budget.
type streamCapacityReleaser interface {
	releaseCapacity(delta int64)
}

// punchRangeFn is a test seam for the punch step.
//
// IT EXISTS BECAUSE A NEUTER PASSED. Swapping the publish and punch steps — the
// ordering whose violation serves silent zeros to a live reader — left every
// test green, because publishPunchedEnd is a state mutation with no observable
// trace and punchRange is a syscall the test could not see. The ordering
// assertions were therefore coverage, not guards.
//
// With the seam, a test substitutes a punch that inspects PunchedEnd() AT THE
// MOMENT OF PUNCHING and fails if the boundary has not been published yet. That
// is the only point where the invariant is checkable, because the window it
// protects exists only between those two statements.
var punchRangeFn = punchRange

// streamDrainEnabled gates the whole feature. DEFAULT OFF.
//
// Off is byte-identical to the pre-streaming behaviour: punchedEnd never moves,
// so the read routing resolves exactly as before and the drainer takes its
// existing whole-file path. This stays off until the oversized-file gate test
// has actually run — a test that is currently deferred because the machine it
// would run on is below the spool floor.
func streamDrainEnabled() bool {
	return os.Getenv("JM_SPOOL_STREAM_DRAIN") == "1"
}

// streamChunkSize bounds one advance. Small enough that a failure wastes little
// work and the copy buffer stays modest; large enough that the per-advance fsync
// is amortised over real work.
const streamChunkSize = 32 << 20 // 32 MiB

// advanceStream copies [entry.punchedEnd, sealed) to dest, then reclaims it.
//
// Returns the number of bytes reclaimed. Zero with a nil error means there was
// nothing to do — the normal outcome for a file that has not accumulated a
// sealable prefix yet, and not a condition worth logging.
//
// The caller supplies sealed (rather than this function reading SealedEnd) so
// the decision and the action use the SAME value. Re-reading it here would let
// the boundary move between the two, which is how a punch could overtake what
// was actually copied.
//
// src MUST BE OPEN FOR WRITING (O_RDWR). It is used for both reading the prefix
// and punching it, and F_PUNCHHOLE requires write access — a read-only fd fails
// with EPERM. SpoolEntry.OpenForRead returns a READ-ONLY handle and is therefore
// NOT usable here; that mistake is why this paragraph exists, and
// TestAdvanceStreamRejectsAReadOnlySpoolHandle pins it.
func advanceStream(
	entry *SpoolEntry,
	src *os.File,
	dest streamDest,
	rel streamCapacityReleaser,
	sealed int64,
) (int64, error) {
	if entry == nil || src == nil || dest == nil {
		return 0, nil
	}

	start := entry.PunchedEnd()
	// Never let a punch overtake what has been published. See punchSafeCeiling:
	// a sealed end that retreated (Truncate) must not imply un-punching.
	sealed = punchSafeCeiling(sealed, start)
	if sealed <= start {
		return 0, nil
	}
	if sealed-start > streamChunkSize {
		sealed = start + streamChunkSize
	}

	// STEP 1 — copy the prefix to the destination.
	//
	// Read from the spool, not from any cached view: this range is below
	// contiguousEnd by construction, so the bytes are real and stable. A short
	// read here is a hard error rather than a partial advance, because advancing
	// punchedEnd past bytes we did not actually copy is unrecoverable.
	buf := make([]byte, sealed-start)
	if _, err := src.ReadAt(buf, start); err != nil {
		return 0, fmt.Errorf("stream: read spool [%d,%d): %w", start, sealed, err)
	}
	if _, err := dest.WriteAt(buf, start); err != nil {
		return 0, fmt.Errorf("stream: write dest [%d,%d): %w", start, sealed, err)
	}

	// STEP 2 — make it durable BEFORE anything depends on it.
	//
	// Without this, a crash between the write and the punch loses the bytes from
	// both sides at once: the dest never had them on stable storage and the spool
	// no longer holds them.
	if err := dest.Sync(); err != nil {
		return 0, fmt.Errorf("stream: sync dest at %d: %w", sealed, err)
	}

	// STEP 3 — publish, THEN punch. Never the reverse.
	//
	// Publishing first routes readers at the destination while the spool bytes
	// are still present, which is harmless — they simply read the same data from
	// the other side. Punching first would leave a window in which a reader is
	// routed at the spool for a range that is already a hole, and reads it as
	// zeros with no error.
	entry.publishPunchedEnd(sealed)

	// STEP 4 — reclaim the space.
	//
	// A punch failure is NOT fatal and must not roll back the published boundary:
	// the bytes are durable at the destination and readers are already correctly
	// routed. We simply failed to reclaim space, so report it and let the next
	// advance retry from the same start.
	punched, err := punchRangeFn(src, start, sealed)
	if err != nil {
		return 0, fmt.Errorf("stream: punch [%d,%d): %w", start, sealed, err)
	}

	// STEP 5 — release only what the filesystem actually gave back.
	//
	// punchRange aligns inward, so it commonly reclaims slightly less than the
	// range. Releasing the requested amount instead of the punched amount would
	// drift the capacity budget above the true occupancy, one partial block at a
	// time, until the spool believes it has room the disk does not.
	if punched > 0 && rel != nil {
		rel.releaseCapacity(punched)
	}
	return punched, nil
}

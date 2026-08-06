package nfs

import (
	"fmt"
	"os"

	"github.com/lelanddutcher/juicemount/metadata"
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
//	3. PERSIST punchedEnd          <- before 5, or the next boot uploads zeros over good data
//	4. publish punchedEnd          <- before 5, or a reader hits a hole and gets zeros
//	5. punch the spool prefix      <- before 6, or capacity is freed that the disk still holds
//	6. release capacity
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

// streamDurability persists the punched boundary so it survives a crash.
//
// ── THIS IS NOT OPTIONAL, AND HERE IS THE FAILURE IT PREVENTS ────────────────
//
// punchedEnd started life as an in-memory field. `spool_entries` has no column
// for it. Meanwhile the boot recovery scrubber decides whether a spool file is
// intact by comparing sizes (nfs/spool.go, the failed→ready branch):
//
//	case r.Size > 0 && diskSize == r.Size:  // "the full copy survived"
//
// A punched file passes that test EXACTLY, because F_PUNCHHOLE leaves the
// logical size untouched — that property is why punching is usable at all, and
// it is also what makes this lethal. So a crash midway through a streamed copy
// would look, on the next boot, like a complete spool file. Recovery resets it
// to `ready`, the drainer copies the WHOLE file to the backend, and the punched
// prefix — zeros — overwrites the real bytes already durably written there.
//
// Silent, total loss of the streamed prefix. No error at any layer. It is the
// same class as the incident documented at that very branch, where recovery
// "silently destroyed an intact photo on the next boot".
//
// THE ORDERING THAT FIXES IT is asymmetric, and the asymmetry is the point:
//
//	persisted but not punched -> recovery skips a prefix that is still present.
//	                             Wasteful (we re-copy it), completely safe.
//	punched but not persisted -> recovery uploads zeros over good data.
//	                             Unrecoverable.
//
// So the persist happens BEFORE the punch, always, and a persist failure aborts
// the advance. advanceStream REFUSES to run without a durability hook rather
// than defaulting to nil, because the whole bug is that this step is easy to
// forget — a fix for a forgotten step must not itself be forgettable.
type streamDurability interface {
	// persistPunchedEnd must be durable before it returns. A non-nil error
	// aborts the advance with nothing published and nothing punched.
	persistPunchedEnd(entryID int64, end int64) error
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
	dur streamDurability,
	sealed int64,
) (int64, error) {
	if entry == nil || src == nil || dest == nil {
		return 0, nil
	}
	// FAIL CLOSED without durability. See streamDurability: punching without a
	// persisted boundary makes the next boot upload zeros over good data, and a
	// nil default would make that the easy mistake rather than an impossible one.
	if dur == nil {
		return 0, fmt.Errorf("stream: refusing to punch without a durability hook: " +
			"an unpersisted punchedEnd makes boot recovery overwrite the destination " +
			"with the zeros of the punched prefix")
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

	// STEP 3 — PERSIST the boundary before anything destroys the spool bytes.
	//
	// Asymmetric on purpose: persisted-but-not-punched costs a redundant re-copy
	// on the next boot; punched-but-not-persisted overwrites the destination with
	// zeros. Only one of those is recoverable.
	if err := dur.persistPunchedEnd(entry.id, sealed); err != nil {
		return 0, fmt.Errorf("stream: persist punchedEnd %d: %w", sealed, err)
	}

	// STEP 4 — publish, THEN punch. Never the reverse.
	//
	// Publishing first routes readers at the destination while the spool bytes
	// are still present, which is harmless — they simply read the same data from
	// the other side. Punching first would leave a window in which a reader is
	// routed at the spool for a range that is already a hole, and reads it as
	// zeros with no error.
	entry.publishPunchedEnd(sealed)

	// STEP 5 — reclaim the space.
	//
	// A punch failure is NOT fatal and must not roll back the published boundary:
	// the bytes are durable at the destination and readers are already correctly
	// routed. We simply failed to reclaim space, so report it and let the next
	// advance retry from the same start.
	punched, err := punchRangeFn(src, start, sealed)
	if err != nil {
		return 0, fmt.Errorf("stream: punch [%d,%d): %w", start, sealed, err)
	}

	// STEP 6 — release only what the filesystem actually gave back.
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

// spoolMetaDurability is the production streamDurability: it persists the
// punched boundary into the spool row via SQLite.
//
// Wrapping the metadata store rather than passing it directly keeps the
// dependency of advanceStream narrow (one method), which is what let the whole
// ordering be tested against a fake.
type spoolMetaDurability struct{ meta *metadata.SpoolStore }

func (d spoolMetaDurability) persistPunchedEnd(entryID int64, end int64) error {
	if d.meta == nil {
		return fmt.Errorf("stream: no spool metadata store to persist punchedEnd")
	}
	return d.meta.SetPunchedEnd(entryID, end)
}

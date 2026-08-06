package nfs

import (
	"bytes"
	"fmt"
	"os"
)

// STREAM ELIGIBILITY AND THE COPY STEP.
//
// The remaining piece of the streaming drain. Everything it composes —
// advanceStream, the punch, the durability hook, the verifier, finish and
// rollback — is already built and individually tested; this decides WHEN to run
// and performs one step of it.

// streamIneligible explains why an entry is not streaming. Empty means eligible.
//
// A reason string rather than a bare bool because "streaming silently did not
// happen" is the failure mode that would waste the whole feature: a large copy
// would quietly fall back to the whole-file path and hit the very size cap this
// exists to remove, with nothing to point at.
func streamIneligible(entry *SpoolEntry) string {
	if entry == nil {
		return "no entry"
	}
	if !streamDrainEnabled() {
		return "disabled (JM_SPOOL_STREAM_DRAIN != 1)"
	}
	// OUT-OF-ORDER WRITES DISQUALIFY, and this is the load-bearing gate.
	//
	// Streaming reads the spool prefix sequentially and hashes as it goes, so the
	// accumulated hash is only a valid whole-file reference if the writer wrote
	// in order. StreamingHashValid goes false the moment any WriteAt arrives out
	// of offset order — a preallocating or scattering writer. Such an entry falls
	// back to today's whole-file drain, which re-hashes from disk and can afford
	// to, because it never punches anything.
	if !entry.StreamingHashValid() {
		return "streaming hash invalid (out-of-order writer)"
	}
	if entry.SealedEnd() <= 0 {
		return "nothing sealed yet"
	}
	return ""
}

// streamStep performs one advance for an entry that is already streaming.
//
// Returns bytes reclaimed. A zero return with a nil error is the normal
// "nothing new to do" outcome and must not be treated as failure — most polls of
// most entries land here.
//
// The sealed end is read ONCE and passed down, so the decision to advance and
// the advance itself use the same value. Re-reading it inside advanceStream
// would let the boundary move between the two, which is how a punch could
// overtake what was actually copied.
func streamStep(
	entry *SpoolEntry,
	src *os.File,
	dest streamDest,
	rel streamCapacityReleaser,
	dur streamDurability,
	ver streamVerifier,
) (int64, error) {
	if reason := streamIneligible(entry); reason != "" {
		return 0, nil
	}
	return advanceStream(entry, src, dest, rel, dur, ver, entry.SealedEnd())
}

// destReadbackVerifier is the production streamVerifier: it re-reads the range
// from the destination and compares it byte-for-byte with what was sent.
//
// Byte comparison rather than a rolling hash, deliberately. The range is bounded
// by streamChunkSize and the buffer is already in hand, so a compare costs
// nothing extra and — unlike a hash — reports WHERE the first difference is,
// which is the difference between a diagnosable corruption report and "sha
// mismatch".
type destReadbackVerifier struct{ f *os.File }

func (v destReadbackVerifier) verifyChunk(off int64, want []byte) error {
	if v.f == nil {
		return fmt.Errorf("stream verify: no destination handle")
	}
	got := make([]byte, len(want))
	if _, err := v.f.ReadAt(got, off); err != nil {
		return fmt.Errorf("stream verify: read back [%d,%d): %w", off, off+int64(len(want)), err)
	}
	if !bytes.Equal(got, want) {
		// Locate the first divergence. A bare "mismatch" turns an incident into a
		// bisect; an offset turns it into a lookup.
		bad := int64(-1)
		for i := range want {
			if got[i] != want[i] {
				bad = off + int64(i)
				break
			}
		}
		return fmt.Errorf("stream verify: destination differs from source at offset %d "+
			"(range [%d,%d)) — the spool copy is still intact, so this range is "+
			"retryable", bad, off, off+int64(len(want)))
	}
	return nil
}

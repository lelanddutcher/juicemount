package nfs

// READ ROUTING FOR A PARTIALLY-PUNCHED SPOOL FILE (S3).
//
// Once the streaming drain punches a drained prefix out of the spool file, that
// range still READS — as zeros, with no error (measured; see punch_darwin.go).
// A reader routed at it gets a silently-corrupt file: right length, wrong bytes,
// no diagnostic anywhere. That is the 2026-06-15 black-frame failure exactly.
//
// So routing cannot be "read the spool, and hope". Every read of an in-flight
// entry must first be classified against punchedEnd, and the classification has
// to be conservative in one specific direction: when in doubt, DO NOT read the
// spool. Serving from the destination is at worst slow; serving a punched range
// is data loss.
//
// This file is deliberately pure — no locks, no I/O, no file handles. The live
// read path is dense with hard-won special cases (drain-evict race, ._ sidecar
// JUKEBOX exemption, in-flight hole holds) and threading a new state through it
// blind is how the previous four rounds in this area each bred the next defect.
// The decision is separated so it can be exhaustively tested on its own, then
// wired in once it is known correct.

// readPlan says WHERE a read at a given offset must be served from.
type readPlan int

const (
	// planSpool: the bytes are present in the spool file and safe to read.
	planSpool readPlan = iota

	// planDest: the range has been drained and punched out of the spool. It must
	// be served from the backend copy. Reading the spool here returns zeros.
	planDest

	// planHold: an in-flight hole below the high-water mark that the writer is
	// still expected to fill — JUKEBOX-hold rather than serve a short read, or
	// the client treats a partially-arrived file as complete.
	planHold

	// planEOF: genuinely past the end of what exists.
	planEOF
)

func (p readPlan) String() string {
	switch p {
	case planSpool:
		return "spool"
	case planDest:
		return "dest"
	case planHold:
		return "hold"
	case planEOF:
		return "eof"
	}
	return "unknown"
}

// readPlanInputs is the consistent snapshot a routing decision needs. Taking
// these together (rather than re-reading each from the entry) is the same
// discipline as ReadableBounds: a decision made across two different snapshots
// can classify an offset against a state that never existed.
type readPlanInputs struct {
	// punchedEnd — everything below this has been drained AND punched. Monotonic.
	punchedEnd int64
	// contiguousEnd — end of the contiguously-written prefix.
	contiguousEnd int64
	// writtenEnd — high-water mark; may sit above contiguousEnd over a hole.
	writtenEnd int64
	// writerActive — the writer has touched this entry inside the stall window.
	writerActive bool
	// isAppleDouble — ._ sidecars never JUKEBOX-hold (#100); a hold there caused
	// the ~73s-per-file copy stall.
	isAppleDouble bool
}

// planReadAt classifies a read of one offset.
//
// ORDER IS THE WHOLE POINT. The punched check comes FIRST — before the
// contiguous-prefix clamp, before the sidecar exemption, before EOF. A punched
// range is, by construction, below contiguousEnd (only drained bytes are
// punched), so any ordering that consults the prefix first would route a punched
// offset straight at the spool and serve zeros. There is no offset for which
// reading a punched range is correct, so nothing may take precedence over it.
func planReadAt(off int64, in readPlanInputs) readPlan {
	if off < 0 {
		return planEOF
	}
	// FIRST, ALWAYS: punched bytes are gone from the spool.
	if off < in.punchedEnd {
		return planDest
	}

	readEnd := in.contiguousEnd
	if in.isAppleDouble {
		// ._ sidecars are served like a plain server — real bytes where written,
		// zeros in genuine (unpunched) holes, never a hold. Note this raises the
		// readable end to writtenEnd but does NOT bypass the punched check above:
		// a punched sidecar range would still be routed to dest.
		readEnd = in.writtenEnd
	}
	if off < readEnd {
		return planSpool
	}
	// At or past the readable prefix.
	if !in.isAppleDouble && off < in.writtenEnd && in.writerActive {
		// A still-fillable in-flight hole. Serving a short read here would let the
		// client treat a partially-arrived file as complete.
		return planHold
	}
	return planEOF
}

// punchSafeCeiling reports the highest offset the drain may punch up to right
// now, given what readers can still be routed at.
//
// Returns sealed unchanged in the normal case; the value exists as a single
// named place to add a reader-drain barrier when the copier is wired in, rather
// than discovering later that the barrier belongs in three call sites.
//
// A punch must never overtake the published punchedEnd, because readers are
// routed by that value: publishing must happen first, punching second. Passing a
// sealed end BELOW the current punchedEnd means something has gone backwards —
// return the existing value rather than "un-punching", which cannot be undone.
func punchSafeCeiling(sealed, publishedPunchedEnd int64) int64 {
	if sealed < publishedPunchedEnd {
		return publishedPunchedEnd
	}
	return sealed
}

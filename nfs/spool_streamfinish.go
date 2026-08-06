package nfs

import (
	"fmt"
	"os"
)

// PUBLISHING AND ABANDONING A STREAMED COPY.
//
// A streamed file lives at a hidden sibling until it is complete, then becomes
// real in one metadata operation. These are the two terminal transitions:
// finishStream publishes it, rollbackStream throws it away. Everything else in
// the streaming path is incremental and reversible; these are not.

// finishStream publishes a completed streamed copy at its real path.
//
// THE RENAME IS THE PUBLISH. Until it happens the file is invisible everywhere —
// filtered out of the mirror, skipped by the farm, hidden from Finder. After it,
// the file exists whole. There is no state in between that any reader can
// observe as a partially-written file, which is the entire point of the temp
// path.
//
// THE LOCK IS DELIBERATELY NOT HELD ACROSS THE RENAME. It is tempting: it would
// close the window where streamDest still points at the temp path that no longer
// exists. But rename here is a FUSE call over a link that may be cellular, and
// holding e.mu across an unbounded syscall is exactly the mistake that made
// cachedCeiling serialize every OpenWrite on a shard (2026-08-04, HIGH). A read
// landing in that window instead gets ErrSpoolDrained from
// readFromStreamDestLocked, which makes the client reopen onto the real file —
// the same proven recovery the drain-evict race (GAP A) already uses. A
// retryable blip is an acceptable price; a lock around a slow syscall is not.
func finishStream(entry *SpoolEntry, tempPath, realPath string) error {
	if entry == nil || tempPath == "" || realPath == "" {
		return fmt.Errorf("stream finish: missing entry or paths")
	}
	// Rename publishes. Everything below punchedEnd is already fsynced by
	// advanceStream, so there is nothing further to force here — a second fsync
	// would be a slow no-op on a link we are trying not to spend.
	if err := os.Rename(tempPath, realPath); err != nil {
		return fmt.Errorf("stream finish: rename %q -> %q: %w", tempPath, realPath, err)
	}
	// Re-point the read shadow at the real file rather than clearing it. Clearing
	// would make every punched read fail closed until the entry is evicted, and
	// those bytes are still only available from the destination — the spool's
	// copy of them is holes.
	entry.SetStreamDest(realPath)
	return nil
}

// rollbackStream discards a streamed copy that will never complete.
//
// WHY THIS CANNOT BE SKIPPED ON FAILURE. The partial is invisible to the mirror,
// to the farm, and to anyone browsing — so nothing else will ever notice it, and
// it can be hundreds of gigabytes on the backend. The boot sweep is the backstop
// for a crash; this is the in-process path for an ordinary cancel or error, and
// leaving it to the sweep would mean the space is held until the next restart.
//
// The spool file is NOT touched. Its punched prefix is unrecoverable, so an
// entry that has been streamed cannot fall back to a whole-file drain — the
// caller must fail the entry, not retry it from the spool. That is why
// punchedEnd is deliberately left set: it is the durable marker that tells boot
// recovery this spool file is not self-sufficient, and clearing it here would
// re-arm the exact "resume from a holey file" hazard the punched_end column
// exists to prevent.
func rollbackStream(entry *SpoolEntry, tempPath string) error {
	if entry == nil {
		return nil
	}
	if tempPath != "" {
		if err := os.Remove(tempPath); err != nil && !os.IsNotExist(err) {
			// Report it, but still clear the pointer: a partial we could not
			// delete is the boot sweep's problem, and leaving streamDest set
			// would route reads at a file we are abandoning.
			entry.SetStreamDest("")
			return fmt.Errorf("stream rollback: remove %q: %w", tempPath, err)
		}
	}
	entry.SetStreamDest("")
	return nil
}

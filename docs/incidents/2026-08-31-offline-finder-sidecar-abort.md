# Offline Finder copy abort after a drained metadata continuation

Date: 2026-08-31  
Release candidate: `0d8aaa70d94d75ca4bf66217d146fd0c20862ccc`  
Disposition: JuiceMount defect; not a JuiceFS storage defect

## Impact

The constrained-spool release battery copied a 300-file Finder tree and switched
JuiceMount offline mid-copy. The copy initially buffered successfully, but
Finder later removed the partial destination. The custody gate consequently
reported all 300 expected files missing even though the spool itself had no
failed or quarantined rows.

## Root cause

Finder writes small `._` AppleDouble files while copying directory metadata and
can revisit one after its first spool row has already drained. In this run the
route changed to offline between those operations. JuiceMount had already
removed the completed spool image, so the later positioned write looked like an
unsafe partial edit of a backend-resident file and correctly failed closed under
the general media policy. The NFS layer returned access denied; Finder treated
the metadata failure as a copy failure and removed the partial tree.

This was a policy granularity bug in JuiceMount: the generic rejection is right
for media whose untouched ranges are unavailable, but overly strict for a small
metadata file whose complete prior body had just passed through the local
durability spool. No JuiceFS orphan, slice leak, object deletion failure, or
space-accounting error was involved.

The QA error tally also omitted this access-denied signature, so the missing-file
custody gate caught the failure while the separate log tally incorrectly
reported zero gated errors.

## Correction

- Capture complete verified `._` and `.DS_Store` spool images before drain
  cleanup, bounded by the existing 128 KiB metadata-cache limit.
- When offline, permit an in-place metadata continuation only when that complete
  body still matches the live mirror metadata. Atomically seed a new spool entry
  before publishing it to concurrent NFS writers, then apply the positioned
  write locally.
- Continue to reject ordinary partial file edits, stale/incomplete metadata
  images, and uncached metadata files while offline.
- Gate `NFS3ERR_ACCES`, `media not available offline`, and the corresponding
  unsafe-write warning in the release battery log scan.

## Regression coverage

- Exact byte preservation for an offset-152 offline metadata continuation.
- Drain-time capture before both per-file and batched spool cleanup.
- Concurrent seeded opens never observe a partial initializer and charge the
  spool capacity exactly once.
- The existing unsafe partial-media test remains fail-closed.


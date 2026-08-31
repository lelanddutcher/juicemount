# Offline Finder copy abort after a drained metadata continuation

Date: 2026-08-31  
Release candidates: `0d8aaa70d94d75ca4bf66217d146fd0c20862ccc`, `7045db7d12c7f5fb979fe4a693cdf9c2ee36469b`
Disposition: JuiceMount defect; not a JuiceFS storage defect

## Impact

The constrained-spool release battery copied a 300-file Finder tree and switched
JuiceMount offline mid-copy. The copy initially buffered successfully, but
Finder later removed the partial destination. The custody gate consequently
reported all 300 expected files missing even though the spool itself had no
failed or quarantined rows.

## Root cause

The first reproduction exposed one valid case: Finder can revisit a small `._`
AppleDouble file after its first spool row has drained. The route changed to
offline between those operations, so the later positioned write had no local
base image and correctly failed closed under the general media policy. Candidate
`7045db7` corrected that case by retaining a bounded, verified complete metadata
body and atomically seeding a replacement spool image.

The exact 300-file Finder reproduction still produced one access denial on
`._dir2`. Narrow lifecycle tracing plus a deterministic NFS-layer regression
isolated a different sequence:

1. The sidecar was created while offline, so its NFS handle carried a synthetic
   inode.
2. Finder unlinked the sidecar name, which correctly cancelled its spool row and
   removed it from the live namespace.
3. Finder retained the old NFS handle and sent continuation WRITEs at offsets
   152 and 0. This is normal open-but-unlinked Unix handle behavior.
4. `FromHandle` skipped deleted-handle recovery for synthetic inodes and used
   the generic synthetic-inode-to-path fallback. The already-unlinked handle was
   therefore misclassified as an ordinary live pathname.
5. `OpenFile` saw no spool shadow and no complete prior body, treated the request
   as an unsafe offline in-place edit, and returned `NFS3ERR_ACCES`. Finder then
   aborted and removed the partial destination tree.

Both defects were in JuiceMount. The generic fail-closed policy remains correct
for media whose untouched ranges are unavailable; the bugs were incomplete
metadata copy-up coverage followed by incorrect synthetic-handle lifecycle
classification. No JuiceFS orphan, slice leak, object deletion failure, or
space-accounting error was involved, so this incident does not justify a
JuiceFS issue.

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
- Mark the bounded evicted-handle shadow as authoritatively deleted during the
  same cache transaction that removes a name for an NFS `REMOVE`.
- Resolve an explicitly deleted AppleDouble handle before ordinary eviction or
  synthetic-path recovery. Accept late positioned writes through a handle-only
  sink that cannot recreate the pathname, consume spool capacity, or relist the
  entry. Deleted principal media retain normal `ESTALE` behavior.
- Gate `NFS3ERR_ACCES`, `media not available offline`, and the corresponding
  unsafe-write warning in the release battery log scan.

## Regression coverage

- Exact byte preservation for an offset-152 offline metadata continuation.
- Drain-time capture before both per-file and batched spool cleanup.
- Concurrent seeded opens never observe a partial initializer and charge the
  spool capacity exactly once.
- Offline synthetic AppleDouble handle: create, unlink, resolve the retained
  handle, write at offsets 152 and 0, close, and prove both the mirror and FUSE
  name remain absent. The regression failed against `7045db7` because
  `FromHandle` returned ordinary `*juiceFS`; it passes only with the explicit
  deleted-handle shadow.
- Deleted principal media still return `NFSStatusStale`; deleted directories
  retain only the existing metadata-operation tombstone behavior.
- The existing unsafe partial-media test remains fail-closed.

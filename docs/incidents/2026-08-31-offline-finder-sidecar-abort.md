# Offline Finder copy abort after a drained metadata continuation

Date: 2026-08-31  
Release candidates: `0d8aaa70d94d75ca4bf66217d146fd0c20862ccc`, `7045db7d12c7f5fb979fe4a693cdf9c2ee36469b`, `f88bc59e89325773e0e174ec260542f54fa28892`, `bcd01ba30e33681a54c7cfcc763b735e9b7d256e`
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
`._dir2`. Narrow lifecycle tracing plus deterministic NFS-layer regressions
isolated a second sequence:

1. The sidecar was created while offline, so its NFS handle carried a synthetic
   inode.
2. Finder unlinked the sidecar name, which correctly cancelled its spool row and
   removed it from the live namespace.
3. Finder retained the old NFS handle and sent continuation WRITEs at offsets
   152 and 0. This is normal open-but-unlinked Unix handle behavior.
4. In the first variant, `FromHandle` skipped deleted-handle recovery for
   synthetic inodes and used the generic synthetic-inode-to-path fallback. The
   already-unlinked handle was therefore misclassified as an ordinary live
   pathname.
5. `OpenFile` saw no spool shadow and no complete prior body, treated the request
   as an unsafe offline in-place edit, and returned `NFS3ERR_ACCES`. Finder then
   aborted and removed the partial destination tree.

Candidate `f88bc59` proved another residual of that same lifecycle bug. Metadata
reconciliation had already redirected the synthetic inode-cache key to the
first real JuiceFS entry so the old NFS handle would survive inode replacement.
After unlink and same-path recreate, that alias still resolved successfully and
therefore bypassed the cache-miss-only deleted-handle branch. It pointed at the
unlinked entry object while the pathname cache owned a different live
generation. A deterministic regression reproduced the incorrect `*juiceFS`
resolution without JuiceFS, Redis, object storage, or a network transition.

Candidate `bcd01ba` fixed that stale-generation path, but the exact Finder
reproduction still emitted the same single access denial. High-volume NFS
tracing finally showed that this last `._dir2` was a different case:

1. macOS/FUSE materialized a real 4 KiB AppleDouble file beside a directory;
   it did not pass through NFS CREATE or the JuiceMount write spool.
2. While forced offline, Finder successfully read the entire local body before
   issuing its positioned rewrite. The live app's verified direct-SSD cache
   satisfied the read and returned from priority 2.5 before the sidecar-cache
   population hook on the later FUSE path. The bounded offline path also lacked
   a population hook, and the FUSE hook rejected the normal complete-read shape
   `n=4096, io.EOF`.
3. `cachedFile.ReadAt` consequently admitted a complete body only from one of
   its three authoritative local/FUSE serving paths, and only on `err == nil`.
4. JuiceMount therefore discarded the authoritative local body it had just
   served, then correctly refused the following offset-152 write because no
   safe base image appeared to exist.

The final defect was consequently a JuiceMount read-cache omission exposed by
normal macOS/FUSE metadata behavior. It was not caused by JuiceFS slice
retention, object storage, or JuiceFS space accounting.

All three defects were in JuiceMount. The generic fail-closed policy remains
correct for media whose untouched ranges are unavailable; the bugs were
incomplete metadata copy-up/read-cache coverage plus incorrect synthetic-handle
lifecycle classification. No JuiceFS orphan, slice leak, object deletion
failure, or space-accounting error was involved, so this incident does not
justify a JuiceFS issue.

The QA error tally originally omitted this access-denied signature. After that
signature was added, trace volume rotated `juicemount.log` during a test and
exposed a second false-green: the scanner kept only a line offset in the current
file and did not follow the marked inode into numbered rotated logs.

## Correction

- Capture complete verified `._` and `.DS_Store` spool images before drain
  cleanup, bounded by the existing 128 KiB metadata-cache limit.
- When offline, permit an in-place metadata continuation only when that complete
  body still matches the live mirror metadata. Atomically seed a new spool entry
  before publishing it to concurrent NFS writers, then apply the positioned
  write locally.
- Continue to reject ordinary partial file edits, stale/incomplete metadata
  images, and uncached metadata files while offline.
- Populate the bounded metadata cache after a complete size-matched read at
  offset zero from the checksum-verified direct-SSD path, the bounded
  offline-local path, or the FUSE fall-through, including when `ReaderAt`
  returns the normal final `io.EOF`. The existing name, size, active-writer,
  and mirror-generation checks remain authoritative.
- Mark the bounded evicted-handle shadow as authoritatively deleted during the
  same cache transaction that removes a name for an NFS `REMOVE`.
- Resolve an explicitly deleted AppleDouble handle before ordinary eviction or
  synthetic-path recovery. Accept late positioned writes through a handle-only
  sink that cannot recreate the pathname, consume spool capacity, or relist the
  entry. Deleted principal media retain normal `ESTALE` behavior.
- Before ordinary live-inode resolution, atomically detect a redirected inode
  alias whose entry object is no longer the current pathname generation and
  whose canonical inode has an authoritative deleted shadow. Route that old
  sidecar/directory generation to the same handle-only tombstone. Current live
  entries, including legitimate inode reuse, remain unaffected.
- Gate `NFS3ERR_ACCES`, `media not available offline`, and the corresponding
  unsafe-write warning in the release battery log scan.
- Mark log windows by inode plus line number, reconstruct windows across
  numbered rotations, and fail closed if a marked inode has aged out before it
  can be scanned.

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
- Redirected-generation AppleDouble handle: issue synthetic inode A, reconcile
  the pathname to real inode B, unlink B, recreate the pathname as inode C, and
  resolve A. The regression fails against `f88bc59` because A returns ordinary
  `*juiceFS`; it passes only when the displaced deleted generation returns
  `*deletedHandleFS` while C remains the live mirror entry.
- Deleted principal media still return `NFSStatusStale`; deleted directories
  retain only the existing metadata-operation tombstone behavior.
- FUSE-created sidecar: seed the real file and mirror row directly, force
  offline, read the complete 4 KiB body into an oversized buffer and require
  `n=size, io.EOF`, then prove an offset-152 rewrite creates a safe spool
  shadow. This regression fails before the complete-read correction.
- The existing unsafe partial-media test remains fail-closed.
- A synthetic rotated-log check proves an error written after the mark but
  moved into `.1` is still counted; loss of all files containing the marked
  inode makes the gate red instead of reporting zero errors.

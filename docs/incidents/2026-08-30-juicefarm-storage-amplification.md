# JuiceFarm storage amplification and blocked reclamation — 2026-08-30

## Summary

The resumed RC validation batch filled `zSSD` a second time and correctly engaged
the farm storage interlock. The pool remained ONLINE and the farm retained its
durable queue state, but three independent effects amplified or retained physical
storage while the visible derivative tree looked small:

1. FFmpeg wrote append/seek/`+faststart` MP4 output directly into JuiceFS,
   producing superseded object slices even for otherwise successful jobs.
2. JuiceFS retained stale slices from proxy replacements for the configured
   seven-day trash period.
3. A recursive TrueNAS ZFS snapshot task included the MinIO bucket dataset, so a
   successful JuiceFS/MinIO deletion still left the underlying blocks referenced by
   ZFS snapshots.

The evidence does **not** indicate a JuiceFS object leak or failed MinIO delete.
This was a JuiceMount application I/O and replacement-policy defect combined with
expected JuiceFS trash semantics and an unsuitable ZFS snapshot scope.

## Impact

- The farm auto-paused at revision 77 before further render claims.
- The pool reached roughly 12 GiB available and 99% allocation.
- No active claim or FFmpeg process remained after the pause.
- Redis persistence remained healthy and the queue was not lost.
- Development and validation were blocked until physical blocks were reclaimed.

## Evidence

At the second pause:

- `zSSD/juicemount/bucket` held about 16.72 TB of referenced live blocks and
  8.83 TB used by snapshots.
- `zSSD/juicemount/bucket@auto-2026-08-30_00-00` uniquely pinned about 7.29 TB.
- The mounted JuiceFS namespace reported about 13.27 TB used.
- `juicefs status --more` reported 3.475 TB in 324,257 invisible Trash Slices.
- A read-only GC inventory initially found about 16.80 TB of referenced slices.
- After the retention-zero cleanup phase, GC found about 13.32 TB of used slices,
  closely tracking the mounted namespace, while 2.33 TB had entered the pending
  deletion queue.
- The resumed farm had 897 durable proxy receipts: 674 HEVC and 223 H.264,
  totaling about 76 GB of current proxy bytes. Current receipts cannot reconstruct
  the size or codec of blobs that were already replaced, so the exact byte split
  between codec promotion and earlier replacement attempts is not recoverable.
- A later controlled RC run completed 70 visible `proxy.mp4` files totaling only
  2,340,868,142 bytes, yet worker/MinIO counters moved by tens of gigabytes and
  `zpool iostat` observed write bursts near 1.7 GB/s. Pausing new claims stopped
  the writes immediately. There were no new job failures, retry loops, temp files,
  missing hardware flags, or registration errors to explain that ratio.

## Root cause

### 1. Seek-heavy MP4 muxing targeted the object-backed FUSE mount

The farm handed FFmpeg a staged path inside JuiceFS. MP4 muxing appends and seeks,
and `-movflags +faststart` rewrites container metadata after encoding. JuiceFS
correctly translated those mutations into new object slices and retained the
superseded slices according to `TrashDays`. Atomic final rename protected namespace
correctness, but it did not make the preceding byte-level write pattern physically
idempotent.

The fix encodes both full proxies and Quick Look previews on node-local scratch,
closes the completed MP4, and copies it into a descriptor-relative JuiceFS stage in
one sequential pass before the existing receipt and atomic commit. The shipped
worker images and compose pin `JM_FARM_ENCODE_SCRATCH=/tmp`; runtime validation
rejects a scratch path that resolves inside the shared mount.

### 2. Automatic codec promotion was not storage-idempotent

Queue sweeps required the existing proxy codec to match the worker's selected codec.
A valid H.264 fallback therefore became "stale" when a compatible HEVC worker was
available. Replacing `proxy.mp4` is logically atomic, but JuiceFS intentionally keeps
the old slices for `TrashDays`. Repeated sweeps could consume physical storage while
the visible derivative count stayed constant.

The fix makes automatic queue sweeps accept a current, source-matched, byte-validated
H.264, HEVC, or AV1 proxy. Codec migration now requires explicit regeneration.
One-shot codec-specific commands retain exact-codec behavior.

### 3. ZFS retained blocks after JuiceFS deleted the objects

The TrueNAS periodic snapshot task recursively snapshotted `zSSD`, including the
MinIO bucket. JuiceFS GC successfully deleted stale objects, but ZFS snapshots still
referenced their blocks. This explains why the earlier GC reported successful MinIO
deletes while pool free space increased by much less than the logical deletion.

The live task now excludes `zSSD/juicemount/bucket`. The large 2026-08-30 bucket
snapshot was removed; older, much smaller bucket snapshots were left intact to age
out under the existing policy.

### 4. ZFS `statfs` total was mislabeled as physical pool capacity

Available bytes from `statfs` were accurate enough to trigger the safety pause, but
ZFS reports `f_blocks` for the mounted dataset, not the parent zpool including child
datasets and snapshot-only usage. Manager therefore displayed a tiny "total" when
the pool was nearly full.

The fix adds `JM_FARM_STORAGE_CAPACITY_BYTES`. Manager and the server worker use this
physical total for reporting and the default 1% reserve while continuing to take
available bytes from the live `statfs` probe. Responses also identify the capacity
source and retain the dataset-scoped filesystem total for diagnostics.

## Recovery performed

1. Kept the farm paused and verified zero durable processing claims.
2. Excluded the MinIO bucket dataset from the recursive TrueNAS snapshot task.
3. Removed only the 2026-08-30 bucket snapshot that pinned about 7.29 TB.
4. Temporarily changed JuiceFS `TrashDays` from 7 to 0.
5. Started `juicefs gc --delete` without compaction and confirmed the trash slices
   moved into deletion processing.
6. Restored `TrashDays=7` immediately after metadata cleanup.
7. The guarded GC completed without MinIO delete errors. It deleted 345,086
   trash files and 2,012,258 pending slices, representing about 12.02 TB of
   logical stale-slice records.
8. Restored the normal seven-day JuiceFS trash setting and `spa_slop_shift=5`.
   The pool remained ONLINE. A later read-only release audit measured about
   10.92 TB free at the pool and 10.76 TB available to the JuiceMount dataset.

## JuiceFS upstream disposition

No upstream issue should be filed from the current evidence. JuiceFS documents that
file overwrites create invisible stale slices, that those slices follow trash
retention, and that `gc --delete` is the supported cleanup path. Directly exposing
an object-backed filesystem to an append/seek/faststart writer was our I/O pattern,
not an upstream deletion failure. ZFS snapshot block retention is below JuiceFS and
cannot be released by JuiceFS GC.

Open a JuiceFS issue only if the completed GC reports leaked objects that have no
metadata/trash explanation, MinIO deletion errors, or a persistent object/metadata
gap after pending deletions drain. Attach the JuiceFS version, redacted GC summary,
object counts/bytes, and `status --more` before/after figures; do not attach media
paths or storage credentials.

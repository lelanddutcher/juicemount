# Honest state (the residency badge must not lie)

OpenLoupe's reputation rests on one invariant in the offload engine: **never report a false "safe to wipe."**
A green **"resident / available offline"** badge driven by stale or inferred data is the *same class of lie*.
The contract is designed so OpenLoupe can keep the same discipline for JuiceMount residency.

## The discipline OpenLoupe already enforces (and the badge must copy)

In `Database+OffloadStatus.swift`, `OffloadStatus` is built from three numbers whose honesty comes from
**where each is sourced**:

- `recordedDestinations` — `Set(rows.map(\.destinationID)).count` — **historical** (the ledger). Explicitly
  documented as "NOT that it still exists".
- `liveVerifiedVolumes` — `WipeManifest.liveSecuringVolumeCount(rows:fileManager:)` — **live, re-stat now**.
  "The only number that may inform a safety decision."
- `hasChecksum` — historical.

The live post-pass (`SearchEngine.search` → `matchesLiveOffload`, `SearchEngine.swift:26-36,97-117`) re-probes
per hit and **fails closed** on any unparseable value (`return false`). Underneath,
`liveSecuringVolumeCount` (`WipeManifest.swift:23-45`) requires, per row: a non-empty content hash, the
destination file **still present at the recorded size**, collapsed by **physical volume** — failing toward
NOT-safe when identity can't resolve.

## What that means for the contract

1. **`resident` is a live truth, never a stored flag.** OpenLoupe computes the residency badge from a live
   `GET /residency?path=` probe at render time, kept separate from any "was resident when recorded" ledger
   fact. This is why **`/residency` (JM-2) blocks the green badge**: until it exists, OpenLoupe must **not**
   paint "resident" — it shows only states it can prove (`streaming`, `warming` from `/cache-status`,
   `offline`). Inferring "resident" from `pin.db` ("pinned" ≠ "all bytes on SSD right now") would be the badge
   equivalent of a false safe-to-wipe. Don't.

2. **Fail closed on timeout.** Every JuiceMount probe runs inside `Deadline.bounded` /
   `AccessStore.resolveGrantedBounded` (`Deadline.swift:36-73`, `AccessStore.swift:100-120`). A wedged control
   plane **times out → badge shows "unknown/offline", asset is KEPT** (keep-on-timeout: a slow network is not
   a deleted source). Never freeze the actor; never treat a stall as proof of anything.

3. **Display-only states stay behind the firewall.** `warming`, `uploading`, `streaming`, "source present"
   are live UI states — like `DetectedVolume.isPresent` (`IngestVolume.swift:46-51`), they **never** feed the
   wipe verdict.

4. **A JuiceMount copy is only "securing" when it's actually resident + verified.** `warmBeforeScrubIntent`:
   a `streaming`/`offline` JuiceMount source must be **warmed (pulled local) and content-verified** before it
   can count toward `liveVerifiedVolumes` or be reported safe to wipe (`isSafeToWipe`,
   `Database+OffloadStatus.swift:124`). The contract echoes `destination_path + size + inode` (+ a content
   hash where available) so `liveSecuringVolumeCount` can re-stat a JuiceMount destination on the **same**
   honest path as a local one — under-claim, never over-claim.

## Badge → source mapping (v1)

| Badge | Source field | Honesty rule |
|---|---|---|
| **Resident** (green) | `/residency.resident == true` (live) | requires JM-2; never inferred from pins |
| **Streaming** (cloud) | `/residency.streaming` or `exists && !resident` | default when not resident |
| **Warming** (ring) | `/cache-status` per-root `cached_bytes / total_bytes` (the `/residency.bytes_cached/total` per-asset variant needs JM-2) | live progress |
| **Offline-unavailable** (grey) | `/offline.offline == true`, or probe timed out | fail-closed default |
| **Uploading** (to NAS) | `/residency.upload_state` ∈ writing/ready/draining, or `/spool` entry | from spool, live |

When in doubt, show the **lesser** claim. A clip shown as "streaming" that is actually resident costs a few
warm blocks; a clip shown as "resident" that is actually a remote stub is a broken promise.

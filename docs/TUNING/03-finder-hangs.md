# Finder hangs / beach balls

Catalog of where Finder blocks or errors, the mechanism, and the fix/status.

## H1 — "Server connection interrupted" on a cold ~60 MB read over WAN  (#39)

**Symptom:** preview/download a cold (uncached, unpinned) ~60 MB file on cellular →
Finder beach-balls ~30 s → "the server connection was interrupted."

**Mechanism (live log, 19:30–19:32):** the big read **saturates the cellular link** →
the reachability TCP-dial probe gets starved → declares "backend unreachable for
sustained window" → **auto-offline engages** → the in-flight cold read can no longer
fetch blocks → the read path exhausts its 4 retries/block and surfaces a **generic
`EIO`** for block after block → kernel NFS → Finder "connection interrupted." The read
**killed itself** by looking like an outage. Confirmed transient: the same read
completes cleanly (10 s) once the link is stable.

**Fix (#39):**
- B′ — don't engage auto-offline while bytes are actively flowing (reuse
  `DataXferActivity()`); a slow-but-progressing read then completes.
- A′ — when genuinely offline, fail FAST + CLEAN with `ErrOfflineNotAvailable`→`NXIO`
  (preserves the handle) instead of a 30 s retry-storm → `EIO`.

**Refinement (measured 2026-06-15, 56 ms cellular, clean link):** a *single sequential*
67 MB cold read completed in 11.8 s, byte-perfect, with **zero** metadata starvation —
warm `stat` stayed 0 ms, LOOKUP/GETATTR mean stayed 0.02/0.03 ms, offline never engaged,
`read_fails`=0 throughout. So "connection interrupted" is **not** triggered by a single
clean read; it needs either (a) a **degraded link** so the read's retry storm starves the
reachability heartbeat, or (b) **read concurrency** that fills the single TCP connection.
The xattr storm (§H2 — hundreds of near-parallel cold reads) is exactly case (b) and is
the more reliable repro. Implication: B′ (don't-offline-while-transferring) helps, but the
durable fix is **read-admission isolation** so neither a retry storm nor a read fan-out can
starve the heartbeat/metadata lane.

## H2 — Cold first-browse of a large dir: the xattr/AppleDouble READ-storm  (#35)

**This was previously mis-attributed to slow metadata.** Measured 2026-06-15 over 56 ms
cellular (mirror WARM), it is *not* a metadata cost — it's a per-file **content READ**
storm triggered by macOS extended-attribute probing. The metadata path is already
instant; the reads bypass it.

**The discriminator (same 692-file dir `101EOSR5`, warm mirror):**

| Operation | new READ RPCs | wall time |
|---|---|---|
| `ls` (readdir, names only) | 0 | **0 ms** (mirror) |
| `stat` all 692 in a loop (pure GETATTR) | 0 | **~1 s total (~1.4 ms/file)** (mirror) |
| `ls -la` (adds the per-file xattr `@` probe ×692) | **+240 in 18 s** | **times out >40 s** |

The *only* thing `ls -la` adds over the fast stat-storm is the per-file xattr probe —
and that alone produces the cold-read storm. Confirmed at single-file granularity:
`stat` of one file = **0 new READs**; `xattr -l` of one file = **+2 READs / +4 KB**.

**Mechanism:** NFSv3 has no xattr RPCs, so the macOS client emulates
`getxattr`/`listxattr` (which `ls -la`, and *especially Finder*, do for every entry to
show the `@` flag / FinderInfo / tags / icons) by issuing `LOOKUP ._<name>` + a small
**READ of the file's first block**. On a COLD file that 4 KB read is a cold MinIO chunk
fetch over the WAN (READ p99 measured **9.4 s**, mean ~1 s). 692 files → ~hundreds of
cold reads, serialized on the **single** macOS NFS TCP connection → the listing wedges.
(The backend has **zero** real `._` sidecars here — every one of these reads is pure
macOS-side xattr emulation, not user data.)

**Why it hid behind "metadata":** our mirror made readdir+getattr instant, so the
*remaining* browse cost is now entirely this xattr-read storm. Earlier notes that said
"per-entry cold GETATTR over the wire" were wrong — GETATTR is mirror-served.

**Levers (ranked):**
1. **Don't let these reads head-of-line-block heartbeat/metadata** (ties to H1/#39): a
   read-admission lane so the xattr/preview read storm can't starve LOOKUP/GETATTR/the
   reachability heartbeat. This is what turns "slow" into "connection interrupted."
2. **Negative/fast `._` + empty-xattr path:** when no real backend xattr/`._` exists
   (the common case), serve `LOOKUP ._<name>`→ENOENT from the mirror WITHOUT a backend
   READ, so xattr emulation completes locally. Biggest direct win — but touches the
   QA-13 `._` round-trip semantics, so needs care + a real Finder round-trip test.
3. **Warm first-blocks on readdir:** prefetch the first 4 KB of each child so the
   inevitable xattr probe is cache-served. Cheap blocks, bounded fan-out.
4. **Product guidance:** pin/prefetch a project before browsing it remotely; consider a
   mount option to suppress AppleDouble xattr emulation where safe.

## H3 — Post-deploy/remount settling window  (#37)

After a restart/remount, the reconcile rebuilds the 261k-entry mirror (~2 min Wi-Fi,
more on WAN). During it, nav is slow and fresh paths can transiently stale. The app
shows "Connected" with no progress signal. Fix: "rebuilding index (N%)" indicator.

## Tuning levers for hangs

- **Read-admission isolation (highest-leverage, cross-cutting):** H1 and H2 are the same
  disease — slow cold READs on the single NFS connection starve everything else. We
  already isolated WRITE admission (writeSem, separate from rpcSem) so parked writes
  can't block reads. The symmetric fix for reads: a bounded read lane + priority for
  heartbeat/LOOKUP/GETATTR so a preview/xattr read storm can't starve the reachability
  probe (→ false offline → "connection interrupted") or metadata.
- The subread deadline (2 s) bounds a single READ RPC so one slow block can't hang the
  whole mount — verify it's tight enough under WAN.
- Reachability must not engage offline from our own traffic (H1/B′).
- Warm the mirror so first-browse isn't cold — but note (H2) the mirror is already warm;
  the remaining cost is the xattr/AppleDouble READ storm, so also warm first-blocks
  and/or serve absent `._`/empty-xattr locally.

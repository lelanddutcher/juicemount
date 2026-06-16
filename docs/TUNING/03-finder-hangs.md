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

## H2 — Cold first-browse of a large dir (slow, ~beach-ball)  (#35)

First `ls -la` of a 692-entry dir = **65 s** at 50 ms Wi-Fi (then instant). Per-entry
cold GETATTR falls through to Redis/FUSE over the wire. Will be minutes at WAN. Fix:
prefetch/warm dir attrs, READDIRPLUS-style batch, or proactively populate the mirror.

## H3 — Post-deploy/remount settling window  (#37)

After a restart/remount, the reconcile rebuilds the 261k-entry mirror (~2 min Wi-Fi,
more on WAN). During it, nav is slow and fresh paths can transiently stale. The app
shows "Connected" with no progress signal. Fix: "rebuilding index (N%)" indicator.

## Tuning levers for hangs

- The subread deadline (2 s) bounds a single READ RPC so one slow block can't hang the
  whole mount — verify it's tight enough under WAN.
- Reachability must not engage offline from our own traffic (H1/B′).
- Warm the mirror so first-browse isn't cold (H2).

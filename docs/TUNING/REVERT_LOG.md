# Tuning Revert Log

Per the cellular-revert-safety discipline: every link-class-gated or
WAN/cellular-affecting tuning change is logged here with its kill switch and
the exact baseline it reverts to, so a regression can be backed out **without
a rebuild**.

---

## 2026-06-27 — Metadata Redis keyspace-notification push

**What:** Demote the full-tree Lua metadata SCAN from a fixed 30s cadence to a
rare, class-gated backstop, driven by Redis keyspace notifications
(`PSUBSCRIBE __keyspace@<db>__:d*`) so only **changed** directories are
incrementally reconciled. Motivation: the 30s full SCAN takes 87–178s over
cellular and finds zero changes ~93% of the time, saturating the link and
killing navigation.

**Kill switch (env, no rebuild):**

| `JM_METADATA_KEYSPACE_PUSH` | Effect |
|---|---|
| unset / `0` (default) | **DISABLED** — the keyspace loop never starts; `backstopNanos` stays at `DefaultReconcileInterval` (30s); `reconcileLoop` runs the exact production Lua SCAN. **Byte-identical to pre-change behavior.** |
| `1` | Engage the push subsystem (still self-disables if Redis lacks `notify-keyspace-events` — see auto-detect below). |

**Auto-detect (second safety net):** even with `=1`, the client runs
`CONFIG GET notify-keyspace-events` on every (re)connect. If the result is
insufficient (`sufficient := contains('K') && (contains('A') || (contains('g')
&& contains('h')))`) it stays DISABLED and runs the 30s SCAN. The live NAS
returns **empty** today, so the DISABLED path is what actually runs until the
durable NAS compose change lands (see `server/NOTIFY-KEYSPACE-EVENTS.md`).

**Baseline to revert to:** `DefaultReconcileInterval = 30 * time.Second` full
Lua SCAN (the proven production path). Set `JM_METADATA_KEYSPACE_PUSH=0` (or
unset) — no rebuild, no redeploy.

**Class-gated values (only active when ENABLED + reachable):**

| Link class | Active interface band | Rare-backstop SCAN interval | Coalescer debounce / max-wait |
|---|---|---|---|
| LAN / Ethernet | `en*` (≠ `en0`), `eth*` | 10 min | 200 ms / 2 s |
| WiFi | `en0` | 15 min | 400 ms / 3 s |
| Tunnel / cellular / WAN | `utun*`, `tailscale0`, or `JM_WAN_MODE=1` | 45 min | 1.5 s / 5 s |
| **DISABLED / DEGRADED / unreachable** | (any) | **30 s** (baseline) | n/a |

Coalescer burst ceiling: **200 distinct dirs** in one window → promote to a
single full SCAN (`TriggerSync`) instead of thousands of `HGETALL`s.

**Interaction with backoff:** the reconcile loop's failure backoff is computed
off the 30s base (a failure streak drives the loop *faster* to detect
recovery, never slower); on recovery it resets to the live `backstopNanos`, so
backoff and the long backstop don't fight.

**Convergence guarantees (push is fire-and-forget, never sole source of truth):**
(A) startup blocking `SyncOnce()` baseline; (B) every (re)connect runs
PSUBSCRIBE-then-full-SCAN gap-fill; (C) the rare class-gated periodic SCAN
ceiling above. Pure in-place `i{inode}` size/mtime edits fire no `d`-event and
are caught only by (C) — an accepted, documented tradeoff.

**Validated:** PARITY Go test (incremental store == fresh full-SCAN store,
byte-identical path/inode/size/mtime/isDir) over a seeded `d`/`i` fixture, plus
foreign create/delete/rename/move/rmdir mutations; QA-30 pin-safety on the
scoped prune path; coalescer collapse + burst-promotion; wrong-db channel
rejection. **Pending:** REAL Finder/VLC validation on a live mount with the NAS
reconfigured to `Kghx` (unit tests give false positives on this codebase per
testing-feedback discipline).

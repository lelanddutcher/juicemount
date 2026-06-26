# Loopback endpoint randomization — don't collide with Shade (or any other local-mount provider)

> **STATUS: EARMARKED — design only, not built. (2026-06-25)**
> Real interop bug: **Shade uses the same loopback NFS endpoint as JuiceMount, so a user can't run both at
> once.** We must assume *most* "mount-cloud-storage-as-a-local-drive" providers do the same, and pick our
> loopback endpoints so we never overlap with any of them.

## The problem (grounded)
JuiceMount runs a userspace NFS server on a **fixed** loopback endpoint and `mount_nfs`-mounts it locally:
- NFS server bind: **`127.0.0.1:11049`** — hardcoded default in `cmd/jm5/main.go:63` (`--listen`), `bridge/cbridge.go:203-204`, and the Swift pref `nfsListenAddr` (`Preferences.swift:99,218`).
- Mount: `sudo mount_nfs -o port=<p>,mountport=<p>,…,vers=3,tcp 127.0.0.1:/ <mountpoint>` (`cmd/jm5/main.go:537-539`). The port is **parsed from the listen addr** (`:523-526`), so it already follows whatever the server bound — good, the fix is localized.
- Control plane / metrics: **`127.0.0.1:11050`** (`bridge/cbridge.go:206-207`, `Preferences.swift:100`). juicefs metrics: `127.0.0.1:9567` (`internal/manager/sync.go:24`).

Two processes can't both `net.Listen` the **same** `127.0.0.1:11049` — the second fails with *address already in use* (`nfs/server.go:54-56`). So if Shade (or anything) also binds `:11049`, **whichever launches second can't start its NFS server**, and the user can run only one. A fixed well-known port is a guaranteed-collision design the moment a second vendor picks the same number — and we have zero control over what they pick.

## Principle
**Never bind a fixed, well-known loopback port for an endpoint another vendor might also choose.** Allocate a
**dynamic, OS-assigned free port** so collision is impossible by construction. Treat `11049`/`11050` as *legacy
defaults to retire*, not values to keep.

## The fix

### NFS server (do this first — it's OpenLoupe-transparent)
OpenLoupe consumes the **mount path** (`/Volumes/<vol>`), not the NFS port — so randomizing the NFS port needs
**zero cross-product coordination.**
1. Bind `127.0.0.1:0` (OS picks a free port) instead of `:11049`. After `net.Listen`, read back
   `l.Addr().String()` → the **resolved** `127.0.0.1:<port>`.
2. Feed the **resolved** addr to the mount command (the mount already parses host:port from the addr — just
   pass the resolved one, not the configured `:0`). Same in both paths: `cmd/jm5/main.go` and `bridge/cbridge.go`.
3. Default `nfsListenAddr` → `"127.0.0.1:0"` (an **"Auto" mode**). Keep manual override as an advanced escape
   hatch; surface the *resolved* port read-only in Preferences (the field shows `:11049` today, `Preferences.swift:213`).
4. **Stability (nice-to-have):** persist the last resolved port and try to rebind it first; fall back to `:0` if
   it's taken. Gives a stable port across launches without ever colliding.

**No sudoers change:** the scoped rule is `/sbin/mount_nfs` by binary path, not by port
(`OnboardingWindowView.swift:55`) — a dynamic port works under the same rule.

### Control plane / metrics (second — has a discovery contract)
`127.0.0.1:11050` is the surface **OpenLoupe discovers** via `defaults read com.juicemount.app metricsAddr`
(fallback `127.0.0.1:11050`). It's written to defaults today as the **configured** value (`Preferences.swift:241`).
To randomize it safely:
1. Bind `:0`, then write the **RESOLVED** addr to `defaults` (today it writes the configured value — which would
   be `:0` and break discovery). The resolved-addr write is the load-bearing change.
2. OpenLoupe must treat the `:11050` fallback as a **last resort only**, always preferring the defaults value.
   The `metricsAddr` key already exists, so this is mostly a "write the real port, document the contract" change —
   **coordinate via the contract repo** before flipping (it's a JM↔OL discovery touch-point).
3. juicefs metrics `:9567` is internal (manager probe only) — lower priority; randomize when convenient.

## Phasing
1. **NFS `:0` + resolved-addr-to-mount + Auto default** — the actual Shade fix; low-risk, OpenLoupe-transparent.
2. **Persist-and-prefer-last-port** — stability across launches.
3. **Control-plane `:0` + write-resolved-to-defaults + OL fallback-is-last-resort** — needs the contract note.

## Edge cases / don't-break
- **Re-mount / menubar "Re-mount" / diagnostics** must read the **resolved** port, not the configured one
  (`MenuPopoverView.swift:1473`, `DiagnosticsExporter.swift`).
- Unrelated to the **"never mount a 2nd loopback NFS of the same endpoint wedges juicefs"** gotcha — that's a
  duplicate mount of the *same* endpoint; this is about picking a *unique* port, still a single mount.
- A persisted port that's now taken by another vendor on next launch ⇒ must fall back to `:0`, never hard-fail.

## Cross-links
- The OpenLoupe discovery contract (`metricsAddr`) is noted in `CONSUMER_STATUS.md` (contract repo) — update it
  when the control-plane port goes dynamic.

# Cellular Finder saturation and stale recovery state

Date: 2026-09-01

Release: JuiceMount 0.5 RC

Disposition: JuiceMount policy/configuration defect; no evidence of a JuiceFS data-integrity defect

## User-visible symptoms

- A metadata-index rebuild over cellular took a long time and saturated the link.
- Entering media directories caused large download bursts for Finder previews.
- Preview work sometimes continued after leaving the directory and delayed the next directory.
- The menu-bar app continued to show “Started offline” after the backend and index recovered.
- Older, unpinned directories did not always appear locally complete, and an unpinned uncached clip had first-open network latency.
- One synthetic lifecycle restore left the loopback NFS mount readable but unable to create a 4 KiB file. The stale entry required a scoped force-unmount; in that already-poisoned boot session, a LaunchServices-launched app retained the bad client context while a sanitized direct relaunch recovered immediately.

## What went wrong

Seven independent behaviors compounded into one bad remote experience:

1. **The cellular route was hidden behind a low-latency VPN hop.** The initial backend RTT was 2–5 ms, so the adaptive profile selected `medium`. macOS Network.framework did not propagate the physical cellular path's `isExpensive` bit through the Tailscale route.
2. **A thumbnail miss could become a source-file read.** `/thumb-local` returned 404 when no farm poster was cached. Quick Look then invoked Apple's generator, which opened the original media over NFS. Once Apple owned that request, navigating away did not reliably cancel the underlying reads.
3. **A farm-output event could promote a full metadata SCAN.** An unknown ancestor in the filtered derivative namespace triggered the general “one full SCAN” recovery path. On cellular this transferred metadata and competed with foreground navigation even though keyspace push plus the durable backstop could converge later.
4. **The blue recovery banner depended on one endpoint.** The cache-status and health endpoints could already prove online recovery while a stale or failed `/offline` response left `offlineStartupReason` latched in Swift.
5. **The durable mirror was more complete than the RAM shadow.** The RAM metadata shadow is bounded at 500,000 entries and retains directories, pins, and the newest ordinary files. After file eviction, `childrenIdx` could present an incomplete directory even though the complete metadata remained in the local SQLite-WAL mirror.
6. **The NFS write-readiness verifier was still armed when teardown began.** `NFSServerStopMount` and `NFSServerShutdown` attempted to unmount before `stopServerLocked` invalidated the verifier generation. A retry could therefore launch or retain its bounded `dd` create probe against the same hard NFS mount during unmount. In the reproduced wedge, every 4 KiB NFS probe blocked in `open(2)` and the server observed no `CREATE` or `WRITE` RPC, while the same write through the private FUSE mount completed in about 44 ms and a fresh loopback NFS mount at a temporary mount point completed in about 7 ms. This isolates the fault to stale macOS client-session/lifecycle state, not JuiceFS, Redis, MinIO, or the NFS handler's write implementation.
7. **The synthetic runner made teardown harsher than the tested user flow.** It withdrew the impaired Redis proxy before asking the test app to quit, forcing an artificial backend outage during FUSE/NFS cleanup. It then sent termination signals after eight seconds even though the app's bounded unmount ladder can legitimately spend five seconds on ordinary `diskutil`, five on forced `diskutil`, and then reach scoped passwordless `umount`. That could kill the server before the force-unmount tier removed the old hard mount.

The last symptom is also a product-semantics mismatch: metadata being local does not mean every file's bytes are local. Only explicit pins guarantee offline bytes. Ordinary JuiceFS cache residency is best effort, so an uncached unpinned clip necessarily uses the link when explicitly opened.

## Fix

- Resolve the actual backend route before choosing immutable JuiceFS mount flags. `utun*` and `tailscale0` routes cap automatic classification at `slow`; an explicit measured/forced slower class still wins. Live route changes reapply the ceiling.
- Feed `NWPath.isExpensive` and `isConstrained` into the Go profile when macOS exposes them. A positive cost signal forces `metered`; the VPN-route ceiling covers the case where macOS hides it.
- On `slow` or `metered`, `/thumb-local` is strictly cache-only. A poster hit is served locally; a miss returns 204 and the Quick Look extension draws a local generic placeholder. It does not read through, warm the directory, or let Apple decode the source.
- Do not promote unknown-ancestor events to a full SCAN on a tunnel/high-latency route. Keyspace push remains active and the scheduled backstop provides convergence. `JM_UNKNOWN_ANCESTOR_SCAN_ON_WAN=1` is the incident rollback lever.
- Clear the “Started offline” banner when cache status says online and core health is true, as well as through the existing `/offline` path.
- Skip the redundant health-monitor FUSE settle only when boot mount/reuse has already passed its two-consecutive data-plane proof. Deferred or unproven mounts retain the settle check. This removes several seconds from constrained-link startup without weakening readiness.
- Disarm and generation-invalidate the NFS write verifier before the first unmount syscall, track/kill/reap its actual `dd` and `rm` children, wait boundedly for an in-flight probe to retire while the server is still alive, interrupt its exponential retry sleeps, reject late recovery callbacks from arming a replacement, and serialize every Start/Stop lifecycle across the complete unmount window.
- Keep the synthetic proxy alive until the exact test-app process has completed clean shutdown. Wait up to 90 seconds for that captured PID and both owned mounts to disappear before considering TERM/KILL; restore the exact original bundle only after cleanup is verified.
- Keep RAM point hits as the fast path, but after RAM eviction use the complete local SQLite mirror for point misses and NFS `READDIR`. This transfers metadata only from local disk and never opens file content. `JM_DURABLE_SERVE_FALLBACK=0` restores strict RAM-only behavior.
- Warn before “Reset local metadata cache” that a large first rebuild transfers metadata and should be done on LAN when possible. Ordinary starts do not require a rebuild.
- Make the steady warm-up copy explicit: navigation metadata is local, while file contents are instant only when pinned or already cached.

## Verification

Automated:

- `go test ./...`
- `go test -race ./internal/testguard ./internal/netprofile ./internal/cache/pin ./internal/metrics ./bridge`
- Added coverage for OS network-cost forcing, VPN-route ceilings, constrained cache-only thumbnail misses, tunnel suppression of unknown-ancestor SCAN promotion, and complete SQLite-backed serving after RAM eviction.
- Added lifecycle coverage proving shutdown waits for an active NFS write probe to retire, launches no teardown retry, tracks and reaps a killed helper, closes the helper start gate, serializes soft Stop behind an active Start, and interrupts a 30-second verifier backoff promptly after disarm.
- The patched Swift/Go app and Quick Look extension built successfully as an ad-hoc local acceptance artifact and passed exact embedded-version/commit verification. This dirty-tree artifact was deliberately not notarized, stapled, or treated as a distributable release.

Bounded synthetic cellular acceptance (metadata/control path only; no child file opened):

- At 180 ms RTT and 8/2 Mbps, startup reached control readiness in 6.09 seconds and serving readiness in 10.62 seconds. Cached root navigation was 3.99 ms cold, 0.13 ms warm p95, and 4.68 ms p95 with four concurrent workers. Offline engagement took 0.51 seconds and recovery 6.64 seconds.
- At 300 ms RTT and 3/1 Mbps, startup reached control readiness in 11.13 seconds and serving readiness in 15.77 seconds. Cached root navigation was 3.06 ms cold, 0.12 ms warm p95, and 3.64 ms p95 with four concurrent workers. Recovery took 8.67 seconds.
- Both profiles passed with zero object GET bytes, zero object PUT bytes during navigation, zero cache misses, zero cold subreads, zero thumbnail hydration, and zero prefetch. Their impaired Redis/control paths transferred 351,372 and 325,518 bytes respectively, below a 4 MiB hard cap.
- macOS issued small NFS reads in both runs, but backend/cache counters proved they were local-cache activity rather than cellular object downloads. The acceptance artifact reports those NFS counters separately and never treats them as unobserved network bytes.
- The lifecycle runner restored exactly one normal app after each run; the final app reported FUSE, Redis, MinIO, and NFS healthy, steady warm-up, and no offline latch.
- After the verifier/helper and runner lifecycle fixes, the hard-zero-content-read profile was repeated at 180 ms RTT and 8/2 Mbps. It passed with 325,167 control-path bytes, zero object GET/PUT bytes, zero cache misses, zero cold or warm subreads, zero thumbnail hydration, and zero prefetch. Control readiness was 10.15 seconds and serving readiness 11.17 seconds; cached root navigation was 6.27 ms cold, 0.25 ms warm p95, and 3.35 ms concurrent p95. Auto-offline engaged in 0.55 ms, cached offline root navigation took 2.89 ms, and online recovery took 6.63 seconds. Cleanup and exact normal-app restoration both passed, and the restored app independently reported all four components healthy with no test-only environment active.

The original poisoned macOS client context was not used as evidence of a new regression. On the same live server, a fresh temporary NFS client with identical options wrote and fsynced 4 KiB in 12.6 ms, while a direct relaunch of the patched app restored the normal mount to full health in four seconds. The final lifecycle run then shut down and restored cleanly. An installation already carrying the pre-fix stale client state may need one scoped force-unmount and relaunch (or a reboot); the fixed lifecycle prevents the runner/verifier race that created it.

Historical bounded real cellular + Tailscale evidence (no media directory
opened):

This is useful diagnostic evidence, but it is not the final release gate. Its
artifact did not combine an immediately prelaunch zero-app/zero-target-NFS/
zero-private-FUSE proof, one launch-origin clock for control/serving/first-root,
proved cellular underlay, and complete <=4 MiB app-attributed control bytes.
The final candidate must repeat that stricter cold-start protocol; a whole
`utun` delta cannot fill the attribution gap.

- Backend route: `utun6`; selected class: `slow`; measured startup RTT: 44 ms.
- JuiceFS FUSE ready in about 5 seconds; Finder NFS mount usable in about 11 seconds; all core components reported healthy immediately afterward.
- Root listing: 398 entries in 90 ms cold, then 20 ms.
- Boot SCAN skipped, boot gap-fill SCAN skipped, keyspace push subscribed, and the 24-hour remote backstop engaged.
- Logs for the session contained no content reads, no thumbnail hydration/read-through, no directory warmer activity, and no promoted metadata SCAN.
- `/offline` reported `offline=false`, `auto_offline=false`; `/activity` reported no reconcile or drain work.

The aggregate `utun6` counter increased substantially during that sample, but it is not an app attribution metric: with JuiceMount stopped, the same VPN interface moved 153 MB inbound and 36 MB outbound over a 12-second control window. `nettop` attributed the envelope to the Tailscale network extension/kernel path. Because the JuiceMount session logged no content reads or background hydration, the interface total cannot be used as evidence that JuiceMount transferred that payload. Future cellular acceptance should use JuiceMount's backend/cache and Redis byte counters plus a per-process delta, not the whole exit-node interface.

## Remaining constraints

- A deliberately deleted/empty metadata DB must repopulate from Redis once. That operation is bounded and resumable, but its bytes and round trips cannot be made zero; prefer LAN for a large rebuild.
- An explicitly opened file that is neither pinned nor already resident must fetch content. The product should not silently pin every visited media directory: doing so would consume unbounded cellular data and local disk.
- Rich previews on a constrained link are shown only for already-cached farm posters; other files use a local placeholder until a faster link can hydrate the poster cache.
- Playback start for an uncached remote clip can be improved separately with explicit foreground range scheduling and visible residency state, but it must remain user-initiated and data-budget aware.

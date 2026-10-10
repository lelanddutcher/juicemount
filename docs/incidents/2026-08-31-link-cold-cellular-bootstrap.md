# JuiceMount Link cold cellular bootstrap failure (2026-08-31)

## Summary

The signed 0.5 release candidate survived a warm LAN-to-cellular handoff, but a
fresh app launch on cellular did not mount within the 120-second acceptance
window. The embedded Link node remained unregistered and its Redis/object-store
proxies never reached the NAS.

This was a JuiceMount deployment-contract defect, not a JuiceFS defect. No
JuiceFS metadata, object, FUSE, or garbage-collection behavior caused the
failure, so there is no JuiceFS issue to file for this incident.

## Evidence

- The Mac had a cellular default route and no connected system VPN.
- The same installed identity had successfully changed from direct LAN to the
  encrypted Link path before the app was quit.
- After a fresh launch, persisted Link identity state was present, but the Link
  control state never became online.
- The paired configuration advertised a private-LAN HTTP coordination URL.
  That address is unreachable from cellular without an unrelated VPN or route
  back into the NAS LAN.
- The local adaptive proxies accepted TCP connections, which made the old
  diagnostic report about 0.2 ms and “LAN-class” even though their far side was
  unavailable. Redis protocol probes and object-store health probes correctly
  timed out, and neither FUSE nor NFS mounted.

## Root cause

`JM_NET_ALLOW_INSECURE=1` was intended only for a trusted development LAN, but
runtime validation treated it as sufficient authorization for any configured
Link URL. Manager status then equated an internally healthy Headscale listener
and NAS route with remote readiness. The canonical release environment did not
require a public Link URL, so a LAN-only value could be deployed, paired, and
shown as ready.

A warm handoff hid the defect because the running Link process retained its
control/map state. A cold process still needs to reach its coordination URL to
come online; durable node identity does not make a private control URL remotely
reachable.

## Corrective action

- Release rendering now requires `JM_NET_SERVER_URL` and
  `JM_NET_NAS_ROUTE`, enables embedded Link/NAS routing, and disables both
  insecure and LAN-only overrides.
- Runtime validation rejects private, loopback, link-local, carrier-grade NAT,
  single-label, and local-only coordination hosts unless
  `JM_NET_ALLOW_LAN_ONLY=1` is explicitly set for development.
- Plain HTTP additionally requires the insecure development override and can
  never represent a public release endpoint.
- Manager status now reports `remote_ready` and `lan_only`; the UI cannot show
  a LAN-only deployment as ready. Accidental LAN-only pairing and remote farm
  enrollment fail before a credential is minted.
- Persisted Headscale URL/TLS drift fails closed with instructions to regenerate
  only `config.yaml` while preserving identity keys and the device database.
- Link latency diagnostics use authenticated end-to-end samples for adaptive
  loopback proxies. Startup and FUSE policy selection no longer seed the network
  estimator with a loopback-only RTT.

## Follow-up: existing private-network route

The later Tailscale subnet-router test proved the NAS services themselves were
reachable from cellular through the Mac's system route. Ordinary Redis,
object-store, SSH, and Headscale health connections followed the Tailscale
packet tunnel successfully. The embedded tsnet control connection did not: it
bound to the physical cellular interface and remained in `SYN_SENT`, bypassing
the system tunnel's private-subnet route. This is an overlay-stacking conflict
in the JuiceMount integration, not a NAS, Headscale, Tailscale, or JuiceFS
failure.

JuiceMount now has an explicit **Use existing private network** mode. It keeps
the saved Link pairing intact but omits embedded-Link configuration when
building the server config, so Redis and object-store connections follow normal
macOS routing through Tailscale, NetBird, Firezone, ZeroTier, or another trusted
private network. No provider credential is stored by JuiceMount.

The same cellular run also found a fail-closed startup latch: when FUSE became
ready after the boot-time NFS decision, an already-present NFS mount could be
healthy while write-readiness remained unverified forever. Startup now always
arms the generation-scoped NFS read/write verifier for a configured user mount;
it retries safely and coalesces with later mount callbacks.

## Acceptance result and remaining product decision

The patched local build reached fully healthy from a cold cellular start in
about 69 seconds and after a clean restart in about 25 seconds. Redis, object
storage, FUSE, NFS, and the real NFS write/read probe passed. The app made no
embedded-Link control connection, stayed online, returned a cold top-level
listing in about 70 ms, and completed 16 concurrent top-level listings with a
16 ms median and 23 ms maximum. The test read metadata only; no source media or
large payload was transferred.

Existing-private-network remote access is therefore an accepted, zero-hosting
path. Native embedded Headscale remains a separate product option: it still
needs a remotely reachable HTTPS coordination endpoint (or a different
managed/brokered control plane) and must be accepted independently before it is
presented as seamless internet access.

## Follow-up: cold-start readiness amplification

The 69-second accepted start was safe but not an acceptable performance target.
The preserved JuiceFS and JuiceMount traces split that time precisely:

- JuiceMount launched JuiceFS at `23:47:15`.
- JuiceFS completed its session setup and published the macFUSE mount at
  `23:47:24`.
- The startup gate then ran `/bin/ls -A` over the entire JuiceFS root twice to
  prove data-plane stability. The cellular Redis application RTT was about
  813 ms and one lookup inside that enumeration took 20.9 seconds.
- JuiceMount did not declare FUSE ready until `23:48:14`, roughly 50 seconds
  after the mount was already published. The local metadata mirror and boot
  gap scan were not responsible; the mirror was fresh and the scan was skipped.

This was another JuiceMount readiness-policy defect, not a JuiceFS failure.
JuiceFS was asked to enumerate a remotely backed directory and correctly did
so at the available link latency. A cloud-drive-style client must publish its
locally mirrored namespace first and hydrate remote metadata on demand rather
than make global readiness depend on enumerating that namespace.

The readiness probe now performs two bounded root-inode checks in disposable
processes. It still crosses the macFUSE boundary and proves that the exact
JuiceFS process can answer kernel filesystem requests, but it cannot enumerate
children or warm the remote namespace. The popover's post-ready activity phase
is also labeled **Ready — optimizing** so background working-set activity is no
longer presented as unfinished startup.

## Follow-up: local-first cached serving and reconnect

The first shaped-link build exposed two more JuiceMount sequencing costs:

- after the boot probe had already selected cached-only mode, constructing the
  connected Redis client repeated a PING and delayed local publication by about
  9.1 seconds; and
- the first successful Redis SCAN batch globally erased every locally persisted
  inode/chunk-to-slice mapping before reconciliation knew whether any cached
  file had changed.

Neither behavior came from JuiceFS. The former was duplicate JuiceMount boot
policy; the latter was a conservative JuiceMount cache-invalidation policy.
Offline bytes remained checksum-verified, but erasing their address maps forced
the next read back through remote metadata and defeated the local working set.

The boot path now constructs a deferred Redis client without a second network
round trip whenever the bounded probe selects cached-only mode. It publishes
the SQLite-backed loopback NFS namespace while remote FUSE recovers, with
offline gates preventing any request from falling through to an unverified
backing directory.

The direct SSD reader now persists its verified working-set slice maps across
restart. On reconnect it pipelines bounded Redis `LRANGE` commands, compares
the exact resolved JuiceFS slice maps, retains unchanged entries, and discards
only changed or removed entries. No media bytes are transferred. Remote
fallthrough remains gated until this comparison and FUSE identity both pass;
validation failure clears all mappings as the conservative fallback. Full and
scoped metadata reconciliation also invalidate exact changed file inodes rather
than unrelated cache entries.

The signed local build passed a fresh-start test with only the NAS Redis and
object-store ports shaped to 800 ms RTT, 2 Mbit/s down, and 1 Mbit/s up:

- control HTTP: 1 second;
- cached namespace: 6 seconds;
- previously cached 4 KiB read: 10 ms, before remote recovery;
- exact map revalidation over the shaped link: about 7 seconds after launch;
- 16 concurrent root listings: 85 ms p50, 91 ms p95, 93 ms max; and
- full Redis/object/FUSE/NFS health: 36 seconds.

The persisted mapping stayed present after recovery and exact revalidation kept
one unchanged mapping while discarding none. The shaper was removed and the app
returned healthy. Large-file transfer remained explicitly out of scope for the
real-cellular acceptance run.

That run also found a cold adaptive-timeout loop: the startup TCP probe had
already measured 805 ms, but the background reachability monitor began with a
fresh 1-second budget for both a TCP handshake and a Redis PING. It therefore
could not obtain the first successful sample needed to enlarge its own timeout.
The monitor now seeds its RFC-6298-style RTT/variance estimator from the bounded
startup measurement. LAN behavior still clamps to the original one-second
floor, while an 800 ms cold path starts with enough budget for protocol-level
proof rather than remaining cached-only indefinitely.

## Real-cellular acceptance

The exact notarized `33425235ea7f81947540f2dc0832e7fa23e85915` build passed
the low-data cellular acceptance on 2026-09-01. The Mac had no associated Wi-Fi
network, its physical interface was an iPhone cellular tether, and both its
default route and the NAS route traversed Tailscale. The configured exit node
was online. Three tiny ICMP probes demonstrated real cellular variability:
304 ms minimum, 940 ms average, and 1,724 ms maximum RTT with no packet loss.

A full quit and fresh launch, without clearing any cache, produced:

- local control HTTP in 379 ms;
- the cached NFS namespace in 6.268 seconds;
- a requested cached 4 KiB read in 71 ms;
- 16 concurrent root listings at 29 ms p50, 42 ms p95, and 42 ms maximum,
  with zero failures; and
- all Redis, object-store, FUSE, and NFS health green about 8.4 seconds after
  launch.

The NFS client read ahead beyond the requested 4 KiB, but the startup and
outage-read measurements each showed new direct-SSD cache hits and zero new
direct-SSD cache misses. The fresh mirror skipped its remote boot scan, and the
mount policy classified the link as slow rather than treating remote root
enumeration as readiness work.

After warm-up, 20 repeated directory listings completed at 15 ms p50, 19 ms
p95, 23 ms maximum, and zero failures. Thirty sampled local health checks
remained green and online. Finder then opened the mounted volume and rendered
its 164-item root on the real cellular path.

Finder's normal background inspection of the visible root subsequently issued
small reads against entries outside that one verified working-set mapping. At
the end of the entire run, NFS had served 1.81 MB, of which 1.32 MB was
explicitly counted as direct-SSD cache hits. Therefore at most about 0.49 MB
was not attributed to the direct SSD reader; that is an upper bound, not proof
that every unattributed byte crossed the WAN. The counters stopped changing as
soon as the temporary Finder window closed. No large file or gigabyte-scale
payload was transferred, and there were no RPC or read failures.

The final resilience test stopped Tailscale entirely, which removed the private
route and made the backend ports unreachable. JuiceMount engaged offline mode
while keeping the loopback service available:

- root listing: 16 ms;
- cached read: 18 ms;
- 16 concurrent listings: 21 ms p50, 30 ms p95, 30 ms maximum, zero failures;
  and
- three additional direct-SSD hits with zero direct-SSD misses.

Restoring Tailscale restored the private route in under one polling interval.
The reachability monitor proved the backend after about 1.6 seconds, exact
mapping revalidation completed after about 1.9 seconds and retained the one
unchanged mapping, cached-only mode cleared after about 4.2 seconds, and every
health component was green after about 4.5 seconds. The same exit-node
selection was restored. No restart, remount command, cache clearing, media
transfer, or user intervention was required.

This closes the cellular startup, warming, cache-first, concurrency, tunnel
loss, and automatic recovery acceptance criteria. The remaining release work
is the independent farm soak and final release accounting, not another remote
startup fix.

## Explicit cellular mount-start acceptance

The user then requested one more literal mount start while the Mac remained on
cellular. Wi-Fi was powered off and the NAS route still traversed the Tailscale
interface. This was a full cold start: the app, loopback NFS listener, NFS
mount, internal FUSE mount, and prior JuiceFS process were all absent before
launch.

The first diagnostic start made the cached namespace usable in 6.846 seconds,
but it exposed a menu-bar health split brain after the data path had recovered.
The independent `/health` endpoint and real NFS operations were healthy while
the in-process Swift Stats snapshot retained one unhealthy bit. Every two
seconds Stats demoted the UI to **Disconnected**; every five seconds the
independent recovery watchdog restored **Healthy**. The volume worked, but the
red/green state oscillation made it look broken forever.

The Swift controller now asks the independent loopback health endpoint for a
confirming snapshot only when Stats reports stopped or unhealthy. A healthy
confirmation supplies the same FUSE/Redis/object-store core verdict used by
the recovery watchdog; an absent, malformed, or unhealthy confirmation remains
fail-closed. The steady healthy Stats path performs no extra HTTP request, and
the NFS mount remains a separately reported state.

The patched ad-hoc release build then passed a second full cold cellular start:

- process present: 192 ms;
- loopback health listener responding: 1.352 seconds;
- NFS mount and cached namespace usable: 7.319 seconds;
- FUSE, Redis, object store, and NFS all green: 8.823 seconds;
- 15 subsequent health samples over 30 seconds: 15 green, zero failures;
- accessibility state: `JuiceMount: Healthy`;
- post-launch UI-health watchdog corrections: zero;
- 16 simultaneous root listings: 3 ms p50, 4 ms p95/max, zero failures; and
- 20 warm root listings: below the 1 ms timer resolution, zero failures.

No large object was read. One old 4 KiB hidden test fixture returned EIO and
was excluded after the internal FUSE view proved the object itself no longer
existed; this was a stale ephemeral test entry, not a mount or tunnel loss.
The earlier real-cellular and forced-outage runs remain the byte-serving proof
for a verified resident cache entry (71 ms online and 18 ms with the route
removed, with direct-SSD hits and no misses).

Cold namespace publication is now consistently around seven seconds in this
386,000-entry installation. Most of that remaining floor is local startup work:
the current metadata store rebuilds its complete SQLite mirror into in-memory
path and inode maps before starting NFS. Removing that floor requires a larger
follow-up—serve the persisted SQLite namespace immediately, then rebuild the
RAM indexes and subtree aggregates in the background under an explicit
snapshot/write-ordering contract. It is not cellular transfer or JuiceFS root
enumeration, and it should not be folded into the 0.5 health-state correction
without its own concurrency and crash-recovery validation.

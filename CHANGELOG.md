# JuiceMount Changelog

## Unreleased

### App

- On a lossy but working link, the volume no longer goes offline by mistake.
  Before, one failed connection check marked the server unreachable. On a
  link with 200 ms delay and 1% packet loss, one lost packet was enough, so
  the volume went offline 38 times in 80 minutes. Each time, files that were
  not pinned or cached returned "Device not configured" (ENXIO), and an editor
  such as Premiere or Resolve could mark media offline. Now the app needs two
  failed checks in a row, gives each check time for one lost packet on a
  distant link, and treats any recent reply from the server as proof that the
  link works. A link that is really down still goes offline in about 4 s on a
  local network and about 6 to 10 s on a distant one. When the offline mode
  refuses a file, the app now writes one line per 10 s to the normal log with
  the reason.
- Over a slow or distant link, a file that is not in the local cache starts
  to play or open sooner. JuiceFS stores files in 4 MiB blocks. Before, the
  first read of a block waited until the whole block had downloaded. Now a
  small first read fetches only the bytes it needs, and the rest of the block
  downloads in the background into the cache. This applies when JuiceFS
  prefetch is on (every link class except metered) and the volume is not
  encrypted.
- On an encrypted volume, a small read inside a block now downloads that
  block once. Before, it downloaded the block twice: an encrypted object
  must be fetched whole to be decrypted, so the partial read fetched the
  block, and the background fill fetched it again.
- On a slow or distant link, a long file read that is not in the cache uses
  more of the link. Before, the app fetched about one 4 MiB block ahead at a
  time, so most of each block's request time left the link idle. Now it
  fetches up to three whole blocks ahead in parallel, keeps a window of six
  blocks (24 MiB) ahead of the reader, and never fetches a block twice. In a
  simulation of a 200 ms, 30 Mbit/s link, a cold sequential read went from
  about 11 to about 25 Mbit/s. The app still keeps half of its concurrent
  file reads and bulk-read slots for what you open, and leaves room for
  uploads and the background cache warmers, so they are not held behind the
  prefetch. Short reads (Finder previews) still do not start it, and it stops
  when the reader stops or seeks, at the end of the file, and when the link
  becomes metered. Metered (cellular) links and fast or LAN links behave as
  before.
- Saving many small files over a distant link is about twice as fast. Each
  change to the folder tree is a Redis transaction, and JuiceFS waited for a
  separate reply to every WATCH (the command that makes a transaction fail if
  another client changes the same data first) and to a final UNWATCH. Now
  each WATCH travels with the read that follows it, and the needless UNWATCH
  is not sent. Creating a small file and committing its data on close takes
  4 round trips to Redis instead of 12. In the remote lab, writing a 64 KiB
  file took 1.8 s instead of 3.4 s at 200 ms round-trip time, and 0.17 s
  instead of 0.36 s at 20 ms. Transactions watch, read and write the same
  keys as before. The bundled JuiceFS is now build `-jm5`.

### Server

- The Manager image ships the same JuiceFS build (`-jm5`). The Manager still
  accepts `-jm3` or later.
- New opt-in file `server/docker-compose.tcp-bbr.yml`: the object store and
  the Redis proxy send with TCP BBR instead of cubic. Downloads to a Mac on a
  distant link that loses packets become much faster. Nothing changes on a
  LAN, for uploads from a Mac (macOS has no BBR), or for the host's own
  settings. Use it only on a kernel that has BBR (`modinfo tcp_bbr`
  succeeds): on a kernel without it, Docker refuses to start the object
  store and the proxy. The setup command refuses the file on such a kernel.
  See "Faster downloads to remote Macs" in `server/INSTALL-TrueNAS.md`.

### Server: Link

- Link now starts when the NAS has no internet. Before, a NAS that booted
  before its router had a WAN link (for example after a power cut) had no
  Link until the internet came back. Macs at home that reach the NAS through
  Link lost access too. The cause: Headscale, Link's control server, must
  load a relay (DERP) map before it serves. The Manager's generated config
  told it to download Tailscale's public map, and Headscale exits when that
  download fails. Clearing `HEADSCALE_DERP_URLS` did not remove it.
- Now the Manager downloads the map itself and keeps it in
  `/data/headscale/derpmap.yaml`. Headscale reads only that file. Without
  internet, the Manager uses the copy from its last download, or on a first
  start the copy built into the Manager. It downloads the map again every
  minute until that works, then every hour. Headscale reads the file again
  every 5 minutes. With internet, the public relays are used as before.
- On start, the Manager rewrites the relay section of an existing
  `/data/headscale/config.yaml` only if it is exactly what an older Manager
  generated. A relay section you edited stays as it is.
  If you edited that section and it still lists a map URL, the Manager logs
  that Headscale will not start without internet, and how to fix it.
- New setting `JM_NET_DERP_MAP_URL` (in `server/docker-compose.link.yml`).
  Unset: Tailscale's public relays, as before. An https URL: only the relays
  in the DERP map at that URL, such as your own relay's; the public relays
  are not used, and a saved copy of the public map is not reused for it.
  Plain http needs `JM_NET_DERP_MAP_ALLOW_HTTP=1`, because anyone on the path
  could add relays. Any other value stops Link with an error in the Link
  status. `off`: no downloaded map; list your own map file in `config.yaml`
  (`derp.paths`) or `HEADSCALE_DERP_PATHS`. With `off` and no other map, Link
  stops with an error that says so.
- If you go back to an older Manager after this one, it keeps
  `derpmap.yaml` as last written and never updates it.
- The NAS node now runs in its own container, `juicemount-link`, with host
  networking. The NAS node is the Link member that Macs reach the NAS
  through. Before, it ran inside the Manager's container, behind Docker's
  NAT and then the router's NAT. There it could not ask the router for a
  port (NAT-PMP, UPnP, PCP), usually had no IPv6, and told other nodes only
  its container address. Result: a Mac at home could not reach it while the
  internet (and so the relay) was down, and paths from away fell back to the
  relay more often. Now it sits on the NAS's LAN: it maps a port on a router
  that allows it, uses IPv6, and tells other nodes the NAS's LAN address.
  The Manager, Headscale and every other service stay on the bridge network.
- The node container gets only what it needs: no Linux capabilities, a
  read-only root, userspace networking (no TUN device), its identity in its
  own volume (`juicemount-link-state`). It reaches the Redis proxy and the
  object store at the ports this host publishes on 127.0.0.1 (30179, 30151),
  and Headscale at 127.0.0.1:30193. It reaches the Manager only through two
  Unix sockets in the volume `juicemount-link-run`, which only the Manager
  and the node container mount. On the control socket the Manager mints the
  node's keys, tags it, approves its route, keeps a copy of its identity and
  reports its state in `GET /api/net/link`. Only the node uses it: nothing a
  Mac sends over Link reaches it. The Manager acts there only on the NAS
  node's own node key (the one in its copy of the identity, or a node with
  the NAS node's name that registered with a key it minted), and keeps at
  most one unused key outstanding. A posted identity must be a valid tsnet
  state file of that node. Requests to the Manager's port 30190 over Link
  go through the relay socket, which serves only the Manager's own API, so
  the Manager still sees each Mac's own tailnet address. The node restarts
  after a failure with the same backoff as before (1 s, doubling to 60 s).
- The Manager image is built without Tailscale's peer API server
  (`ts_omit_peerapiserver`). In userspace networking it listened on every
  host address; with host networking that would be the NAS's LAN. Link does
  not use it.
- `server/docker-compose.link.yml` sets `JM_NET_NAS_NODE=container` and adds
  the `juicemount-link` service. It no longer publishes UDP 41641: the node
  binds `JM_NET_NAS_UDP_PORT` (default 41641) on the host itself. If another
  program (for example a Tailscale install on the NAS) holds that port, the
  node uses a random port, and a router forward to 41641 then does not reach
  it: set `JM_NET_NAS_UDP_PORT` to a free port and forward that one.
  `JM_NET_NAS_NODE=on` still runs the node inside the Manager.
- The NAS node no longer sends logs to Tailscale's log server
  (`TS_NO_LOGS_NO_SUPPORT` on the node container and, in the overlay, on the
  Manager).
- With Headscale's own TLS (`JM_NET_TLS_CERT`), the node dials
  `JM_NET_SERVER_URL`, which needs hairpin NAT or a local DNS entry, as
  before. To avoid both, set `JM_NET_NAS_CONTROL_URL` to that URL and add
  `extra_hosts: ["<public-name>:127.0.0.1"]` to `juicemount-link`.
- In the Link lab (`test/link-lab`), with the node container: a Mac at home
  connects directly while the home has no internet; a Mac away connects
  directly through NAT-PMP port mapping with hard NAT at both ends.
  Details in the lab README.

### Upgrading to the Link node container

- An install with Link on moves to the new layout without pairing any Mac
  again. Update the overlay file with the rest of the release and run
  `docker compose -f server/docker-compose.yml -f server/docker-compose.link.yml up -d`
  (on a setup-made install, copy the new overlay into
  `<data>/compose/overlays/` and run `<data>/compose/jm-compose up -d`; on
  TrueNAS, render with `config` and paste). On its first start the node
  container takes the NAS node's identity (`/data/headscale/nas-node/tailscaled.state`)
  from the Manager over the socket. The node keeps its node key and tailnet
  address. The Manager keeps a copy, and the node container sends it the
  identity again whenever it changes, so the copy stays current also on an
  install that started with the node container.
- To go back: put back the previous overlay (or set `JM_NET_NAS_NODE=on`,
  publish `${JM_NET_NAS_UDP_PORT:-41641}:${JM_NET_NAS_UDP_PORT:-41641}/udp`
  on the Manager again and remove `juicemount-link`), stop `juicemount-link`
  first, then recreate the Manager. The Manager's node starts from its copy
  of the identity, so it is the same node. The volumes
  `juicemount-link-state` and `juicemount-link-run` can stay.
- Never run both: they hold the same identity. A Manager with
  `JM_NET_NAS_NODE=on` that still shares `juicemount-link-run` with a node
  container tells it to stop its node, and does not start its own node
  while that container still calls it (`GET /api/net/link` says why). With
  the previous overlay (no shared volume), the Manager waits up to 2 minutes
  50 seconds after the node container's last report, unless it stopped
  cleanly. That wait needs this release or later: an older Manager image
  does not check, so with an older image always stop and remove
  `juicemount-link` first. A node container that has not reached the Manager for 2 minutes
  stops its node. `docker compose up` with the previous overlay leaves the
  container running: remove it (`docker compose rm -sf juicemount-link`
  before switching, or `up -d --remove-orphans`).

## 0.5.1 — 2026-11-07 (build 38)

A reliability update to 0.5.0, with a security fix in the Redis proxy, the
first part of Teams (off until you add a person) and changes to JuiceMount
Link. Update checks remain manual through **Check for Updates…** in the app.
Read the **Upgrading** sections at the end before updating the server, Link
or farm workers.

### App and Link

- Starting the mount after updating the app works when the previous version's
  mount was stopped cleanly. Before, the new build refused the old build's
  stopped owner record, so Start failed after an upgrade. A previous owner that
  is still running, or whose stop cannot be proven, is still refused.
- A mount started by the volume owner now uses this Mac's own object-store key.
  Before, the key was dropped at that step, so the mount used whatever
  object-store credentials it inherited instead of this Mac's key.
- Over JuiceMount Link's adaptive path, a Redis reply that stops partway
  through no longer hangs the mount. The response watchdog now stays armed until the whole reply
  has arrived, so a stalled connection is closed after 2 seconds without
  progress and reconnects. Before, the first byte of a reply disarmed it, and a
  stall mid-reply could leave Finder waiting indefinitely. Waiting for a slow
  reader on the Mac does not count as a stall.
- A file whose stored name differs from the requested name only in Unicode
  normalization (for example an accented name created on another system in a
  different form) now opens instead of reporting that it does not exist.
  Stored names are never changed. If two names in a folder match the same
  canonical form, or the local view of the folder is incomplete, no match is
  guessed and the lookup reports the file missing as before.
- Support commands for a mount left behind after its processes stopped (issue
  #4), run from a standalone build and never as root:
  `--unmount-operation-status` reads the eject helper's progress for an
  unmount without changing anything. `--recover-retained-generation` runs that
  mount's ordinary retirement; with `--withdraw` it withdraws it instead, and
  `--eject-now` ejects it, but only after a withdrawal has completed. It works only on an isolated test install (`JM_APP_ISOLATION`), only
  after the owner and its NFS export are proven gone, and leaves queued saves
  and local databases in place. Ordinary installs are unchanged, and packaged
  recovery acceptance remains open.
- Handing a resident file's cached blocks over for upload now rereads any block
  written in the last two seconds and checks it against its recorded hash.
  Before, an in-place edit that kept the file's size and timestamp, and landed
  in the same timestamp tick as the block's last change, could hand over bytes
  that no longer matched the hash. Older blocks keep the metadata check.
- A rename, delete or file creation that the Mac sent again because the
  mount was busy, for example during a large copy onto it, no longer loses
  data. The first request had already finished, and running it a second time
  could discard a just-copied file's not-yet-uploaded bytes and leave its
  name reporting an I/O error while Finder showed no error, delete or empty
  a file another app had just put at that name, or replace a moved file with
  a new one. A resent request that already finished is now answered as the
  success it was, once, for up to 40 seconds, without running it again. The
  Mac cannot tell a resend from an identical new request within that time,
  so such a request is also answered without running; repeat it if needed.
  A rename of a missing source no longer touches the destination.
- A folder the Mac had listed only in part now lists again until it is
  complete. Before, a listing cut short by a slow reply (most often on a slow
  or metered link), or a folder the Mac knew only from a file it had opened,
  created or renamed there, could stay short of files until the server
  reported a change in that folder or the daily rescan ran. Retries back off,
  and a stuck one no longer holds up other folders. On a slow link a folder
  of a few dozen files also lists completely now: before, time spent waiting
  its turn counted against the listing's own time limit, so every attempt ran
  out.

### Server

- The object store image is now SILO (`pgsty/silo`, multi-arch, pinned by
  digest), a maintained community fork of MinIO. The `minio/minio` image that
  0.5.0 pinned can no longer be pulled, so fresh installs failed. SILO keeps
  the S3 API, the `MINIO_*` settings, `mc` and the on-disk format: an existing
  server keeps its bucket data, and the service is still named `minio`.
- Restarting the metadata Redis no longer loses recent file changes. Redis
  rebuilds its data at start by replaying its append-only file. In 0.5.0 the
  Redis ACL made it refuse every replayed transaction, and JuiceFS writes
  almost all file metadata in transactions. So each restart of Redis (a
  container recreate, a server update or a NAS reboot) dropped the file
  changes made since Redis last compacted that file: new and renamed files
  could disappear or point at missing data. Redis logged each refusal as
  `CRITICAL ... AOF-loading-client: '-NOPERM ACLs rules changed`. The fix is
  one line in `server/redis.acl`: the `default` user stays off, so nobody
  can log in as it and unauthenticated clients are still refused, but it now
  holds the permissions the replay needs. Transactions were not the only
  loss: Redis records JuiceFS's switch to its database inside a transaction,
  so after a refused one, plain writes could replay into the wrong database.
  Update a 0.5.0 server only in the order under **Upgrading the Redis proxy
  and Manager** below: in the wrong order, the update itself can corrupt
  the folder tree.
- Server scripts that recreate Redis first make it rewrite its append-only
  file, and stop before anything stops if that rewrite does not finish. So
  the restart replays no transaction that an earlier start refused. A
  release rollout does not restart Redis. But while the running Redis still
  has the 0.5.0 ACL, the rollout makes Redis rewrite the file while it keeps
  running, so a later NAS reboot is safe too. Before such a rewrite, the
  scripts ask you to confirm that you copied Redis's data folder.
- The object store image is now SILO (`pgsty/silo`, multi-arch, pinned by
  digest), a maintained community fork of MinIO. The `minio/minio` image that
  0.5.0 pinned can no longer be pulled, so fresh installs failed. SILO keeps
  the S3 API, the `MINIO_*` settings, `mc` and the on-disk format: an existing
  server keeps its bucket data, and the service is still named `minio`.
- A Manager migration or backup with **Preserve folder structure** off no
  longer copies sibling folders whose names start with the source folder's
  name (for example `photos-old/` next to `photos/`) into folders next to the
  destination. Both settings copy the source folder's contents into the
  destination, as before.
- The Manager keeps every path it works on inside the volume or inside the
  configured source folders, also when the volume holds symlinks that Macs
  wrote. Folder browsing, previews, migrations and backups, schedules,
  permission fixes and the trash all check a path the way the kernel will
  open it, and refuse a path that leads outside. Details are in this
  repository's GitHub security advisories.
- Migrations and backups run the JuiceFS copy confined to the folders they
  copy between. The Manager image ships a JuiceFS build (`-jm3`) whose local
  folder backend cannot open anything outside the folder it was given.
  Symlinks are copied as symlinks only into the volume. A symlink that cannot
  be followed inside the folder being copied (for example an absolute macOS
  path such as `/Volumes/Media/clip.mov`) is skipped, and the job reports how
  many it skipped and names the first few. Before, such a link could make a
  backup copy files from outside the volume, or fail the whole job.
- A migration into the volume stays inside the projects it has checked out.
  Before, a symlink in one project could lead the copy into another project
  without that project's lease. Now such a write is refused and the job
  fails. A symlink whose target leaves its project is not copied: it is
  skipped and reported like the other skipped links. A destination that
  leads into another project through a symlink is refused. A symlink at the
  project level itself, or above it, is neither followed nor copied.
  Symlinks that stay inside one project work as before. A migration into
  the volume now needs the JuiceFS build the Manager image ships (`-jm3`);
  with any other `juicefs` it is refused. Details are in this repository's
  GitHub security advisories.
- A sync request with an unknown direction is refused. Before, a request
  naming a saved destination skipped the source check.
- These checks need Linux 5.6 or later, and a Docker seccomp profile that
  allows `openat2` (Docker 20.10 or later). On an older kernel or profile,
  every migration or backup between local folders fails with a message
  saying so, and none runs unconfined.
- A fresh server install creates the `zpool` bucket before it sets up the
  object-store users. Before, setup used the restricted format key, which
  cannot create buckets, so a first install failed. An existing bucket and its
  data are left as they are, and setup stops unless the object store confirms
  the bucket.
- The Redis proxy applies tightened checks to records that the server's own
  background cleanup acts on. JuiceFS clients on Macs and farm workers write
  these records as before. The details are in this
  repository's GitHub security advisories.
- Approving, revoking or forgetting a Mac, and changing people or grants, now
  either takes effect everywhere or nowhere. The Manager writes the Redis
  proxy's login table first and its own record second. Before, if the second
  write failed (for example on a full disk), the change was reported as failed
  but the proxy already enforced it: a revoked Mac was locked out while the
  Manager still showed it approved, or the reverse. Now the proxy's table is
  put back and the proxy is not told to reload.

### JuiceFarm and Manager

- Experimental smart folders, off by default (`JM_SMART_FOLDERS=on` to try
  them): the Manager keeps a top-level **Recents** folder
  (`JM_SMART_FOLDERS_ROOT`) with one view per top-level folder of the volume,
  each listing that folder's most recently changed files as links. Every Mac
  and every app's file picker sees the same views. The feature never deletes,
  overwrites or adopts your files, respects project leases, and turns itself
  off rather than stopping the Manager if it fails.
- The first successful **Test Connection** of an SFTP destination stores the
  server's host key. Later connections must present the same key, or they stop
  before any password or key is sent. Sync jobs use the stored key and refuse
  to run until the destination has been tested once. Editing a destination
  keeps its key while the host and port are unchanged.
- A farm worker that was partitioned past its claim lease, reaped, and replaced
  can no longer publish a legacy job's outcome, dispatch, or acknowledgement
  over the worker that now owns the job. The reaper also waits for the legacy
  delivery lease instead of releasing a claim as soon as a heartbeat lapses.
- A legacy job claimed while the claim's reply was lost (a dropped
  connection, a Redis restart, or a client retry after a timeout) is returned
  by the worker's next claim, or put back in its queue if the worker has
  stopped claiming (disabled, paused, draining, or waiting on the storage
  permit). Before, it stayed queued in that worker's processing list, run by
  no one, until the worker exited. The fix runs in each worker's own claim, so
  every jmfarm worker must be upgraded to get it.
- A legacy job claimed for a worker that never learned of the claim (a claim
  request that reached Redis late, after the worker had moved on) is put back
  in its queue once it has waited a minute unstarted, even while that worker
  is alive and working on something else. Before, it waited in that worker's
  processing list until the worker exited. A worker that starts the job at the
  same moment either keeps it or is refused the start, so the job runs once.
- A worker whose request to put a job back in its queue was retried after a
  lost reply no longer stops as if it had lost the job. The retry now reports
  that the first request succeeded. This runs in the worker's own request, so
  an un-upgraded worker still stops.
- A worker retries a transient Redis fault while publishing a job's outcome. If
  the outcome still cannot be published, the worker stops so another worker
  recovers the job, instead of leaving it shown as running.
- Legacy job records no longer expire while queued or running. The 7-day
  retention starts when a job finishes or is dispatched.
- The job-index cleanup now walks the whole index from a shared cursor, runs
  from the workers as well as the Manager status page, sweeps at most once per
  15 seconds across the farm, and skips index entries that are not job records.
- Known limit: a queued or running legacy record whose job is no longer in any
  queue or worker receipt now stays in the jobs list until it is removed by
  hand. Before, it expired 7 days after it was queued.
- A poster, filmstrip, or waveform that could not be written because the
  derivative storage was full, read-only, or unmounted is retried. Before, it
  was recorded as a failure for the build, so the retry skipped the file as
  done and the preview stayed missing until the next farm update. A fault
  that has not cleared after three tries over 6 hours
  (`JM_FARM_BLOB_RETRY_WINDOW_SEC`; permission faults count as faults that may
  clear), a broken derivative folder (a symlink or a file where a folder
  belongs), and media that ffmpeg cannot decode are
  recorded as failed and tried again only after the next farm update.
- A worker's claim poll inspects at most 64 queued jobs per queue and continues
  where it stopped on the next poll, so an idle worker behind a large backlog
  meant for another GPU no longer holds the shared Redis for up to 0.3 s on
  every poll. Each poll also checks up to 64 jobs queued since the worker's
  last poll, after older jobs, so new work behind such a backlog is claimed
  within a poll or two once the worker is polling. An idle worker polls less
  often, up to every 2 seconds, so new work can wait up to that long, and it
  walks a backlog it cannot serve at up to 4 polls a second; a claim speeds it
  up again. Claiming a job found deep in a long queue costs more than a poll:
  up to about 2 ms in the middle of 100,000 jobs.
- The backend storage permit is timed by the Redis clock. A server whose clock
  ran a few seconds ahead of Redis blocked every render claim.
- A directory job replayed after a worker crash queues every file it finds. A
  file added between the crash and the replay could be left without a job.
- The watch backstop finds a folder copied to the top of the volume with its
  original dates (rsync -a, cp -p, Finder), and looks back up to 2 minutes
  before its cursor (`JM_FARM_WATCH_BACKSTOP_SKEW_SEC`) to catch files from
  editors whose clocks are behind the farm's. A change just before a scan can
  start one extra catch-up pass, which finds its previews current. To keep it
  to one, the look-back never reaches before the previous catch-up pass: with
  the default 15-minute interval that is the full 2 minutes, but when the
  backstop scans again soon after a catch-up pass (a restarted farm, or a short
  `JM_FARM_WATCH_BACKSTOP_SEC`) it can be shorter.

### Teams: people, write grants and the audit log

- Teams is in this release and stays off until you add the first person on
  the Manager's **People and access** page: until then every Mac writes
  everywhere and nothing is logged (unless `JM_AUDIT=on`), as in 0.5.0. This
  first version has two limits. Grants limit writing only: every Mac on the
  volume can still read every folder, granted or not. And while grants are in
  force, quotas work only on project folders and below (see the quotas entry
  below).
- The Manager has people (**People and access**). Each Mac belongs to one
  person. A person with access "all" writes everywhere; the first folder you
  grant limits that person to granted client folders or projects, in every
  proxy mode, also passthrough. Refused writes fail on the Mac as read-only.
- Once any grant exists, grants are in force: a Mac that belongs to no person
  (a new or auto-approved Mac, or the Macs of a person you removed) can read
  but not write until you assign it. Removing the last limited person does
  not lift this; **Let Macs with no person write again** on the People page
  does. A Mac that enrolls as a rotated copy of another Mac gets that Mac's
  person when you approve or confirm it, never from the auto-approve window.
- A limited person can no longer write into, change, or restore a file
  deleted from a project they have no grant on, can no longer remove the
  quota of a folder above their projects, and can no longer change another
  project's quota usage or folder statistics, also not inside a transaction
  that writes their own files (that let them make another project refuse
  every new file with "disk quota exceeded").
- Quotas and grants: quotas on project folders and below work as before.
  Quotas above the project folders (on the volume or on a client folder
  such as `/Clients`) are not supported while per-person grants are in
  force: such a quota adds up every project below it, so a limited person's
  Mac could not report its usage without being able to change it for the
  other projects. The Manager does not put grants in force while one exists
  and says which and how to remove it (`juicefs quota delete <META-URL>
  --path /Clients`), and it cannot put grants in force without a metadata
  URL to check with. While grants are in force the proxy refuses to create
  one, including through the admin's juicefs, and refuses writing the quota
  hashes as a whole; the admin's raw maintenance commands that name no key
  (FLUSHDB, SWAPDB, scripts) can still rewrite them, as any key. The
  Manager reads the project depth the proxy uses. If one exists anyway
  (made directly in Redis), the People page names it: until it is removed,
  the quota usage of limited people's writes does not update, because
  JuiceFS sends all quotas in one update and the proxy refuses it. Before, a limited person could add or
  subtract any amount to such a quota's usage, which made every other
  project in the folder refuse new files or lifted the folder's quota.
- No writer but the background jobs can store a value other than a whole
  number in the folder statistics or quota usage (for example with
  HINCRBYFLOAT), with or without people, in every proxy mode. One such value
  stopped every JuiceFS client that started afterwards from loading quotas,
  so no quota was enforced, and made the running clients count other
  folders' usage several times.
- The Redis proxy keeps a hash-chained audit log of who changed what in
  `respproxy/audit` beside the principals file. It starts when the first
  person is added (`JM_AUDIT=on` logs from the start, `off` never) and keeps
  at most 2 GB and 400 days (`JM_AUDIT_RETAIN_MB`, `JM_AUDIT_RETAIN_DAYS`;
  the oldest segments are removed with a record that keeps the chain
  verifiable). While grants are in force, a Mac's change reaches Redis only
  after its record is on disk, and Mac writes are refused when the log cannot
  write (a full disk, for example); the People page says so and why. On this
  test machine that wait added about 1.3 ms to a create. Content writes are
  summarized per file and 10 seconds and are not waited for. Refused writes
  are logged one by one up to 20 per Mac and minute, and after that the first
  refusal in each project (up to 64 more), and counted per project after
  that, so a Mac retrying a refused write in a loop cannot push the history
  out through the size limit, nor hide an attempt on another project behind
  it. A change with very many entries (a transaction that adds thousands of
  names) lists its first entries (at most 256, and at most 1 MB in all), how
  many it leaves out, and a SHA-256 of the change's commands as the Mac sent
  them, which matches a capture of its traffic (and Redis's AOF when every
  command of the change wrote something). The log never
  writes a line longer than 4 MB; one longer line once made the whole log
  unreadable. A log that fills its disk past its size limit
  removes its oldest segments and recovers; under the limit it stays failed
  and the People page says to free space or lower the limit.
- A volume with no person writes nothing for the log, not even its health
  file, as before.
- The Manager checks the log every minute and records its head in its own
  state (`audit-anchors.json`), so a rewritten log, one cut back to before
  the last check, or a removed audit directory is reported. It also checks
  every retention record against the size and age limits: a cut of the oldest
  records that the limits do not explain is reported, and a retention that
  removes records less than a week old is a warning (a flood of records, or
  a size limit too small for the volume), as is a size limit that, at the
  rate the log grows, will keep less than a week. A log the Manager cannot
  read in two checks in a row is reported until a read succeeds; before, its
  check stopped there and the page said the chain was fine. The full read
  the check sometimes needs no longer holds up the People page, and a
  failure of that read is reported as such, not as removed records. What it cannot catch: records
  written after the last check can be cut unnoticed, a cut of the oldest
  records that the limits could have made passes, and someone who can write
  both the proxy's directory and the Manager's state can rewrite both. If
  you delete the log on purpose, delete `audit-anchors.json` too.

### JuiceMount Link

- Link has two ways to expose the NAS, chosen by `JM_NET_NAS_EXPOSURE`:
  - `services` serves only the JuiceMount ports (Redis proxy 30179, object
    storage 30151, Manager 30190) on the NAS node's own tailnet address. The
    node carries `tag:jm-nas`, which is how a Mac app from this release finds
    it; while that node is online, the app dials it for the NAS's ports even
    when some other node's route covers the NAS's LAN address. Mac apps from
    earlier releases, and remote farm nodes, cannot reach it: they follow
    subnet routes only.
  - `subnet` advertises a LAN prefix through the NAS node, the only path
    Link had before. Every Mac app version and remote farm node reaches it.
    The prefix is `JM_NET_NAS_ROUTE`, or `192.168.0.0/24` when that is unset,
    as before. The Manager does not tag the node, so a Mac waits for the
    route instead of using the node's tailnet address. A NAS node that ran
    with `services` keeps `tag:jm-nas` after a switch to `subnet` (Headscale
    cannot take a tagged node's last tag away), so a current Mac reaches it
    at its tailnet address, which serves the same ports, rather than through
    the route.
  - `JM_NET_NAS_LAN_ADDR` is the NAS's own LAN address. Under `subnet` it
    must be inside the route, and narrows the policy to the JuiceMount ports
    on that address instead of on every host in the route. Under `services`
    it tells status which routes matter (see below).
- When `JM_NET_NAS_EXPOSURE` is unset, setting `JM_NET_NAS_ROUTE` selects
  `subnet`. Otherwise an install whose NAS node already ran
  (`/data/headscale/nas-node` exists) keeps `subnet`, and a new install uses
  `services`. Before the NAS node first starts, the Manager records the
  exposure it starts with (whichever of the above chose it) in
  `/data/headscale/nas-exposure`, and that record is the default from then
  on, so removing a variable later does not change the exposure. The node
  does not start until the record is written. To switch, set
  `JM_NET_NAS_EXPOSURE` and restart the Manager.
- `JM_NET_NAS_EXPOSURE=services` together with `JM_NET_NAS_ROUTE` stops only
  the NAS node, whose status names the conflict. The control server keeps
  running.
- The Manager finds its NAS node by the node's key, never by the name
  `juicemount-nas`, which any node can register under. It tags, approves
  routes for and reports on that node only. While another node carries
  `tag:jm-nas`, or (under `subnet`) advertises a route overlapping the NAS
  prefix, or (under `services`) holds an approved route over
  `JM_NET_NAS_LAN_ADDR` (any approved route while that is unset),
  `GET /api/net/link` reports the route not ready and the NAS node logs a
  warning, naming each such node. After the NAS node's state is reset, its
  earlier registration (same name, offline, older) is named as such.
  Remove one that is not a device of yours with
  `docker exec <Manager container> headscale nodes delete --identifier <id>`.
- Remote farm node enrollment needs `subnet`: the node's JuiceFS mount reaches
  object storage at the LAN address stored in the volume. Under `services` the
  Manager refuses the remote plan and says what to set.
- The generated policy gives no tag an owner, so no node can tag itself (an
  untagged Mac could otherwise claim `tag:jm-nas`). Keys the Manager mints
  still get their tags.
- Under `services`, the Manager on 30190 is served in-process on the tailnet
  address, so its per-address enrollment limits count each Mac once, at its
  IPv4 tailnet address, whichever of its addresses it connected from. The Redis proxy and object storage are forwarded from the Manager
  container, as is everything through a subnet route, so they see all remote
  clients as that one address. The Redis proxy's AUTH lockout is per address
  and principal, and each Mac has its own principal, so one Mac's wrong secret
  does not lock out another. An AUTH flood from one remote Mac can delay the
  others by up to the 10 second pre-auth timeout.

### Release integrity

- Built with Go 1.26.10 (crypto/x509 fix GO-2026-6809), go-jose v4.1.6 and
  x/net v0.62.0, which close the published advisories against the earlier
  versions. CI runs Redis at the same pinned 7.4.11 build the server ships.
- `scripts/install.sh` installs only a verified JuiceMount Developer ID signed
  bundle and keeps its signature intact. It refuses to run while JuiceMount or
  its serving processes are running, and never quits or kills them; Stop and
  Quit first. It verifies a staged copy, keeps the previous bundle, and leaves
  failed staging artifacts in place for inspection. `--source` installs an
  existing signed release and `--destination` installs to a disposable path.
- The Finder release battery has a strict mode (`QA_STRICT_RELEASE=1`) that
  passes only when every category and required case completes, verifies
  copied bytes with an independent backend read, and keeps all data and
  failed Finder operations until their custody is reconciled. A failed
  snappiness measurement during drain still waits for its Finder copy and
  checks it before reporting the failure.

### Upgrading the farm

Stop all old farm workers before starting new ones, and do not run v0.5.0 and
new workers against the same Redis at the same time:

- A stale v0.5.0 worker still writes outcomes by job ID alone, so it can
  overwrite the outcome of a job that a new worker now owns.
- v0.5.0 reapers ignore the delivery lease and release a legacy claim as soon
  as its worker's heartbeat lapses, so a live job can be run twice.
- Rolling the farm back to v0.5.0 leaves records written by the new version
  without a TTL: v0.5.0 sets retention only when a job is queued. Remove
  finished records with **Clear finished**.
- A v0.5.0 server publishes the storage permit by its own clock, so render
  claims still depend on that server's clock until it is upgraded.
- A v0.5.0 render worker still judges the storage permit by its own clock
  before it tries to claim, so a render host whose clock is off may wait or try
  claims that Redis refuses until it is upgraded.
- A directory job split by one version and replayed by the other queues its
  files a second time. The repeat finds the previews current and skips them.
- Jobs queued by a v0.5.0 worker or Manager are not counted as new, so a new
  worker finds them only by walking the queue, which can take minutes behind
  a large backlog meant for another GPU.
- The return of a legacy job a live worker never started runs in the
  workers' recovery scan, so it covers an un-upgraded worker's claims too, as
  long as one upgraded worker is running: its claims carry no claim time, so
  the minute counts from when an upgraded worker first sees the claim, up to
  15 seconds later. An un-upgraded worker that tries to start such a job after
  it was put back is refused the start and stops, as when a reaper recovers its
  job today, and its service manager restarts it.

### Upgrading the Redis proxy and Manager

- Update a 0.5.0 server's Redis in this order. The order matters: on a
  server whose Redis was already restarted under 0.5.0, starting Redis with
  the new `server/redis.acl` before step 3 replays the transactions refused
  at that earlier start on top of everything written since, which silently
  corrupts the folder tree (`juicefs fsck` does not report it). A NAS reboot
  after the new file is in place counts as such a start.
  1. Keep the 0.5.0 Redis running. Do not pull the new `server/redis.acl`
     yet.
  2. Copy Redis's data folder (its `appendonlydir`) somewhere safe. If Redis
     was restarted under 0.5.0 since its last rewrite, that copy is the only
     place the changes lost at that restart still exist.
  3. Make Redis rewrite its append-only file from what it holds now. First
     note the number in the name of the `.base.` file in `appendonlydir`
     (for example `appendonly.aof.3.base.rdb`). Then run
     `docker exec <redis container> sh -c 'REDISCLI_AUTH="$(cat
     /run/jm-redis/admin.pass)" redis-cli --user jm-admin BGREWRITEAOF'`
     (the password stays in the environment, out of `ps`).
     Wait until `INFO persistence` (same login) shows
     `aof_rewrite_scheduled:0`, `aof_rewrite_in_progress:0` and
     `aof_last_bgrewrite_status:ok`. A rewrite requested while a background
     save runs is only scheduled, so check all three. If Redis answers that a
     rewrite is already in progress, wait for it to finish, then ask again:
     only a rewrite started after step 2 counts. Then check that the
     `.base.` file in `appendonlydir` has a higher number than the one you
     noted. A rewrite that was stopped halfway passes the three checks
     above but leaves the old files in place; if the number did not rise,
     ask again.
  4. Now update `server/redis.acl` and restart Redis. The rewritten file
     holds no transactions to refuse, so nothing is lost or replayed twice.
  Changes lost at an earlier 0.5.0 restart are not recovered by these steps.
  Recovering them from the copy in step 2 is a manual restore, not a
  replay.
- If you update through `server/scripts/rollout-compose.sh`, the script can
  do step 3 for you, but not always in time:
  - First check whether Redis already had a lossy start. Run
    `docker logs <redis container> > redis.log 2>&1; echo $?`, then
    `grep -c 'Ready to accept connections' redis.log` and
    `grep -c AOF-loading-client redis.log`. You can tell only if the first
    command printed 0 (it read the log) and the log shows the current start
    (the `Ready to accept connections` count is at least 1). Otherwise you
    cannot tell, for example with a logging driver that cannot be read, or
    after log rotation dropped the start. `grep -c` prints 0 in both of
    those cases too, so 0 alone proves nothing.
  - If you cannot tell, or the `AOF-loading-client` count is not 0, take the
    manual path: do steps 1 to 3 by hand before you update the checkout.
    Redis restarts by itself if it crashes (`restart: unless-stopped`). A
    crash after the checkout is updated, but before the script's rewrite,
    would start Redis under the new ACL with no script involved. Then update
    the checkout and run the release rollout with `--ack-redis-aof-backup`.
    Its rewrite is then a second one, and harmless.
  - Only if you can tell and the `AOF-loading-client` count is 0: do step 2,
    update the checkout, and run the release rollout right away with
    `--ack-redis-aof-backup`, which confirms that you did step 2. Do not
    reboot the NAS or restart Redis in between. A running Redis does not
    read the new `server/redis.acl`.
  - Once the rollout's rewrite succeeds, a later NAS reboot or Redis restart
    is safe. In detail:
  - The script acts only on this checkout's Redis: the redis container of
    the `--project` you name whose Compose working directory is this
    checkout's `server/` folder. If a redis container of that project runs
    from another directory, the script names that directory and refuses.
  - A release rollout never restarts Redis. Right after it takes its lock,
    before any other check, it reads the ACL that the running Redis loaded
    at its start (`/run/jm-redis/users.acl` in the Redis container). If that
    ACL still has the 0.5.0 `user default ... -@all` line, the rollout
    refuses without `--ack-redis-aof-backup`, and names the host folder to
    copy: the rewrite deletes the old files, the only copy of changes lost
    at an earlier 0.5.0 restart. With the flag, it makes Redis rewrite its
    append-only file now, with no restart. A Redis whose ACL cannot be read
    is rewritten too, with no flag needed. A Redis that runs without an ACL
    file needs no rewrite. Every re-run checks again. After the rewrite, the
    file holds only the live state and later writes. The next start, a NAS
    reboot included, renders the fixed ACL from the checkout and replays
    only those writes, which it accepts.
  - The script counts a rewrite as done only when Redis reports it finished
    with no error and wrote a new base file. A rewrite that was stopped
    halfway leaves the old files, so it counts as failed.
  - Every exit, in every mode, warns while the running Redis may still have
    the 0.5.0 ACL and no rewrite succeeded in this run: do not reboot the
    NAS or restart Redis until a rewrite succeeds. That includes a usage
    error (a mistyped flag), a held lock, a missing flag, a refused rewrite,
    a rewrite that does not finish within 10 minutes
    (`JM_REDIS_COMPACT_TIMEOUT`, in seconds), and a dry run (which only reads
    the ACL). The rewrite runs before the release's other checks, so it has
    run even if the release then stops, for example for an unhealthy
    service.
  - `--cutover-proxy`, `--proxy-rollback` and `--proxy-restore` recreate
    Redis. They run the rewrite before they stop anything, and stop with
    the stack unchanged if it does not finish. They also stop unchanged if
    Redis is down by then and the script read the 0.5.0 ACL (or could not
    read it) and has not finished a rewrite. They need
    `--ack-redis-aof-backup` too while the running Redis has the 0.5.0 ACL.
  - Their automatic rollback, after a failure later in the change, also
    asks for a rewrite before it recreates Redis. Services are already
    stopped at that point. A failed rewrite there does not stop the
    rollback: it warns and goes on, because this run already finished a
    rewrite or read the fixed ACL. If Redis is down at that point, the
    script says that the start replays the whole file. It refuses to start
    Redis while `server/redis.acl` still has the 0.5.0 default line, unless
    you pass `--allow-aof-replay`. Only the automatic rollback can reach
    that refusal or use the flag: a forward change needs a running, healthy
    Redis, and a release never restarts Redis. If the rollback stops there,
    the services the change had stopped stay stopped, and the script keeps
    its lock.
  - `server/scripts/update-server.sh` never restarts Redis and does not do
    this check. If you update by hand (pull the checkout, then restart Redis
    or reboot the NAS yourself), follow steps 1 to 4 above.
- The security fix under **Server** is in the Redis proxy, so it protects a
  volume once that volume's proxy runs this release. Macs and farm workers
  need no update for it. `server/scripts/rollout-compose.sh` refuses a proxy
  image built before the fix.
- Upgrade both. Do not run an older Manager against a proxy directory or a
  principals store written by this one: an older Manager drops the people,
  their Mac assignments and grants when it next saves the principals file
  (and its own store), which lifts every grant.
- Grants reach a proxy only once it advertises the new file format. For about
  2 seconds (at most 4) after the upgraded proxy starts, the Manager still
  writes the old format, so grants are not enforced yet; until they are, the
  People page says the proxy has not applied the latest grants.
- The proxy exits at start if it rejects the principals file (one edited by
  hand, for example), and its container restarts until the file is fixed.
  A file the proxy rejects on a reload is refused and the old one kept.
- Do not downgrade the Redis proxy below this version while people exist. An
  older proxy cannot read person grants, so the Manager writes it the old
  file format without people: every grant is lifted (Macs with no person can
  write again) and nothing is logged per person until you upgrade. The
  Manager logs a warning and the People page says so.

### Upgrading Link

- Upgrading changes what linked devices can reach. With the embedded control
  server and NAS node (`JM_NET_HEADSCALE=on`, `JM_NET_NAS_NODE=on`), the
  Manager now enforces an ACL policy where earlier releases allowed all
  traffic: Macs and farm nodes reach only ports 30151, 30179 and 30190 on the
  NAS node and, under `subnet`, inside the route. Anything else used through
  Link (other ports or hosts on the NAS LAN, one Mac to another) stops
  working. `JM_NET_POLICY=off`, or a `policy:` stanza of your own in
  `config.yaml`, keeps the previous behaviour. With `JM_NET_NAS_NODE` off
  (your own subnet router carries the NAS) the Manager applies no policy and
  mints untagged keys, as before.
- Unless you set `JM_NET_NAS_EXPOSURE`, an install whose NAS node already
  ran keeps `subnet`, so every Mac and remote farm node keeps reaching it.
  Before switching an existing install to `services`, update every linked
  Mac: Macs with an earlier app, and remote farm nodes, reach the NAS only
  under `subnet`.
- With `services` under `JM_NET_POLICY=off` or your own Headscale policy, the
  NAS node's key carries `tag:jm-nas`. Headscale cannot add the tag to a NAS
  node registered before this release unless the policy defines the tag
  (`"tag:jm-nas": []` in `tagOwners`). Until then the node keeps retrying
  and `GET /api/net/link` shows why. Route readiness under `services` now
  also requires the control server to list the NAS node online with the tag.
- `POST /api/net/routes/approve` no longer takes `?host=`: it approves on
  the NAS node, or on `?id=<node id>` (IDs are in `GET /api/net/paired`).

## 0.5.0 — general release (build 37)

Update checks remain manual through **Check for Updates…** in the app.
Bounded packaged Finder copy and directory checks passed, along with focused
offline write and recovery checks. Full Finder, offline, Link, two-Mac, and
consumer acceptance remains open. See the [0.5 release notes](docs/release-0.5.md)
for installation and limits.

### App and Link

- The write spool is on by default, persists an explicit opt-out, protects
  acknowledged writes with local durability and SHA-256-verified drain, and
  exposes recovery controls for stalled or failed uploads.
- A late Finder continuation write now atomically reopens the same unclaimed
  spool image instead of waiting behind the drain queue and surfacing repeated
  retry errors. Rows already claimed by the drainer or partially streamed stay
  on the corruption-safe defer path.
- A verified bounded Finder-metadata image is retained across spool drain so an
  already-drained `._` sidecar or `.DS_Store` can be rewritten safely after an
  offline transition; ordinary partial file edits still fail closed.
- Large Finder directories keep each AppleDouble sidecar beside its principal
  at the final NFS protocol ordering layer with a linear merge, eliminating the
  redundant lookup storm and second full sort seen with 5,000-file folders.
  Bounded metadata mutations no longer share navigation's RPC admission pool,
  so a burst of slow sidecar removes cannot block READDIR/GETATTR. Late metadata
  updates on an already-unlinked sidecar retain handle semantics without
  resurrecting the deleted name or returning a stale-handle error. A directory
  removed while macOS still holds its NFS handle now does the same for late
  metadata requests, preventing a successful Finder tree deletion from ending
  in a stale-handle warning. Renames of new spool-only files no longer issue a
  remote FUSE rename solely to discover that the source is absent. The NFS
  response writer also flushes continuously refilled Finder traffic in bounded
  batches, preventing a ready navigation reply from waiting indefinitely for
  the response queue to become empty.
- JuiceMount Link includes persistent pairing authorization and fail-closed
  route checks. Its full route, reconnect, and revoke workflow still needs
  acceptance.
- Update checks are manual-only. JuiceMount makes no background Sparkle network
  request; choosing **Check for Updates…** starts the signed updater and contacts
  the documented GitHub appcast.

### JuiceFarm and Manager

- Automatic recursive discovery has a durable catch-up cursor, bounded server
  planning shards, renewable processing claims, GPU admission controls,
  HEVC-first proxy routing, explicit hardware H.264 and CPU H.264 fallback, and
  per-file incompatible-source isolation.
- Durable proxy commit receipts and codec-agnostic automatic freshness prevent
  retries or a returning HEVC worker from replacing valid proxies and amplifying
  JuiceFS trash. Storage permits and an atomic safety pause stop new render
  claims below physical-pool headroom.
- Proxy and Quick Look MP4 muxing now completes on node-local scratch before a
  single sequential publish into JuiceFS. This prevents FFmpeg append/seek and
  `+faststart` rewrites from creating tens of gigabytes of retained object slices
  for only a few gigabytes of visible output.
- Embedded audio cover artwork is excluded from motion-video classification, so
  audio-only assets cannot be routed into the MP4 proxy encoder.
- Farm mission control now shows every queue lane and each node's current
  `{job, kind, path, stage, percent}`; supports per-node pause, drain, resume and
  verified restart; returns a bounded 200-line runtime log; cooperatively
  cancels queued/running work; and requeues a failed job exactly once from its
  stored immutable payload while preserving the original terminal record.
- The TrueNAS release compose requires digest-pinned first-party artifacts, the
  existing Manager and MinIO credentials, and exact physical zpool capacity at
  render time. Installation guidance explicitly excludes the MinIO object
  dataset from recursive ZFS snapshots.

The actual GPU worker and deployed derivative queue recovery remain under
test; the consumer pin workflow is also unfinished.

### Release integrity

- Go dependency and reachable-vulnerability gates include the `go-billy` path.
  Linux CI runs Redis-backed Farm durability tests; macOS CI builds and verifies
  a signed-equivalent app and rejects stale source identity.
- The Finder release battery scales drain deadlines by both bytes and object
  count and avoids `pipefail`/early-exit false negatives when reading live
  offline and keyspace-push status. Its directory and Stat latency probes now
  start their clocks inside the process performing the syscall, so process
  launch scheduling under a hot copy cannot masquerade as filesystem latency.
- Container builds embed and execute exact version/commit identity, publish SBOM
  and provenance, and production deployment references accepted manifest
  digests rather than mutable tags. Distribution Mac builds fail closed unless
  Developer ID signing, notarization, stapling, Sparkle signing, and a matching
  appcast all succeed.

### Explicitly gated from 0.5

- The incomplete Teams/seats authorization surface remains fully feature-gated.
  Camera catalog and public website work are outside this release.

## 0.4.0 — 2026-07-15 — Resilience & recovery, faster navigation on slow links

### Added
- **The app tells you when macOS is blocking it.** If macOS's Local Network
  permission is denied, JuiceMount now detects it and says so with a clear,
  actionable alert — instead of silently appearing "offline" while the network
  is actually fine.
- **"Why is it slow?" self-diagnosis.** A new diagnostic surfaces *why* the mount
  feels slow at a given moment (warming up, on a metered link, backend
  unreachable) rather than leaving you guessing.
- **QuickLook thumbnails in Finder.** When the server has pre-rendered a poster
  for a clip, Finder shows it from a local cache instead of streaming and
  decoding the remote video just to draw an icon; on any miss it falls back to
  Apple's generator. (Thumbnail generation is a server-side feature.)
- **Instant folder sizes.** Folder sizes are aggregated incrementally, so a
  directory reports its total size immediately instead of after a full walk.

### Fixed
- **Automatic recovery when the mount drops.** If the volume disappears while the
  backend is still alive, the app now remounts it automatically instead of
  getting stuck showing the drive as unavailable.
- **Survives a backend restart mid-operation.** In-flight reads and writes now
  park and retry across a brief backend (Redis) restart instead of failing the
  copy or read outright.
- **Moving a folder keeps its contents.** Renaming or moving a folder now carries
  all of its children with it — no more items appearing to vanish after a move.
- **Copying bundles and packages is reliable.** Fixed the remaining cases where
  copying `.app`/`.framework`/NLE bundles could fail with a stale-file error,
  by keeping symlink identity and inode numbers stable.
- **Faster, more reliable startup.** Removed a startup deadlock and added a
  boot fast-path that skips redundant index work when the local mirror is
  already fresh — the mount becomes responsive sooner.
- **No more repeated disconnects on machines with a large local cache.** The
  mount health-check now allows enough time for a big on-disk cache to warm up
  at launch, so the app no longer mistakes a busy-but-healthy mount for a failed
  one and cycles it — which previously showed up as the drive dropping and
  reconnecting every couple of minutes.
- **Interrupted downloads are flagged, not served as good.** A file left
  zero-tailed by an interrupted download (e.g. a browser download that dropped)
  is detected at finalize so it can't masquerade as complete.

### Changed
- **Much faster navigation on cellular and other slow links.** Directory listings
  and first-visit folder opens are dramatically quicker over metered/high-latency
  connections: AppleDouble sidecar bodies are served from RAM (and persist across
  restarts), reads use a two-lane gate so a big transfer can't starve browsing,
  drains are serialized so uploads don't saturate the uplink, and the metadata
  push channel self-heals so the app no longer periodically drops into a long
  "rebuilding index" over a slow link.
- **Newly created content shows up in seconds, not up to an hour.** Tightened the
  directory attribute cache so files created on another machine become visible
  almost immediately.
- **A warm-up phase card with progress.** After launch or a reconnect, the popover
  now shows the warm-up phase and its progress, with a note that the mount may be
  slower until it reaches steady state.
- **Cache bar shows how much is pinned.** The menu-bar cache bar now overlays the
  portion of your cache budget consumed by pinned items.
- **Changing the SSD cache size prompts a restart.** The cache size is set when the
  daemon launches, so the app now offers Restart Now / Later when you change it —
  rather than silently not taking effect until the next launch.
- **Pinned items can't be evicted.** Hardened the guarantee that pinned files stay
  resident even under cache pressure, with eviction-triggered repair.

### Server & Manager (control-plane container)
- **Security — trash-restore and scheduled backups are now confined to the volume.**
  The manager runs as root on the NAS; two path-confinement holes are closed:
  trash-restore could move a caller-controlled file to any host path (a root
  code-execution path), and a scheduled backup could read outside its permitted
  source roots (arbitrary-file exfiltration). Both are now gated exactly like
  interactive migrations.
- **Trash retention is readable and settable in embedded mode.** Embedded
  deployments now surface and change the JuiceFS `--trash-days` setting (set
  `JM_OVERVIEW_META`); the UI distinguishes "disabled (0)" from "unknown".
- **Scheduled maintenance (GC / FSCK / compact).** Each maintenance lever can now
  run on a schedule — off by default, each pre-filled with a conservative
  recommended cadence and inline guidance. JuiceFS already auto-handles the
  routine cleanup (trash expiry + slice compaction), so these are opt-in hygiene.
- **Farm tab — "Clear finished" + running sweep to the top.** Clear terminal jobs
  from the recent-jobs list; the currently-running sweep floats to the top.
- **Under the hood.** Fixed a jobs-API data race and an unbounded Redis
  job-index leak.

## 0.3.0 — 2026-07-06 — Reliability: no more "connection interrupted", instant saves & exports

### Fixed
- **"The server connection was interrupted" is gone.** Finder no longer flashes a
  spurious connection-interrupted error during copies, at the moment a Premiere
  export finishes, or when working with unicode / emoji filenames. Root cause: the
  server could briefly report a just-written file as unavailable while it was still
  settling — it now stays consistently visible.
- **Saving and exporting feel instant.** Closing a file — a Premiere project save,
  a video export — now finalizes in a few seconds instead of up to ~30, and no
  longer stalls on "the last 1%". Large exports finalize as soon as the app signals
  it's done, not after a fixed wait.
- **No more black frames for images in Premiere.** A stale in-memory copy could
  serve a truncated image (fixed only by restarting the app); images now read from
  the always-fresh path and stay correct.
- **Copies no longer stall ~a minute per file.** A Finder copy of files carrying
  Finder tags or a quarantine flag no longer hangs ~73 s per file.
- **No data-integrity issues under heavy concurrent copies.** Hardened the write
  spool so a continuation write during a drain can never truncate an already-written
  file, and out-of-order writes are tracked correctly.

### Changed (app)
- **Clear Cache updates the number immediately.** The menu-bar "X cached" figure now
  drops as soon as the cache is cleared.
- **Clearer Cache & Storage settings.** Numeric fields use the standard bordered
  input styling so it's obvious where to type.
- **More complete diagnostics export.** The exported bundle now includes the full
  rotated logs (write-spool + NFS activity) and a live spool snapshot.
- **Removed the obsolete "Reconcile interval" setting.** Metadata now syncs in real
  time via change notifications; there's no interval to configure.

## 0.2.0 — 2026-06-29 — Bundles & packages, offline hardening, auto-updates, server-side generation

### Added
- **Automatic updates (Sparkle).** JuiceMount now checks for and installs signed
  updates in the background (Developer-ID + notarized builds; EdDSA-signed appcast).
- **App bundles & packages copy correctly.** `.app`, `.framework`, `.fcpbundle`, and
  NLE project folders — which contain internal symlinks — now copy without the
  "you don't have permission to access some of the items" error. NFS SYMLINK /
  READLINK are implemented.
- **JuiceFarm (advanced, optional).** Server-side generation of playback proxies,
  poster frames, filmstrips, audio waveforms, technical metadata, and transcripts, so
  clients don't transcode on-device. See [`docs/JUICEFARM.md`](docs/JUICEFARM.md).
- **OpenLoupe integration.** A versioned wire contract for portable derivatives and
  human assertions (ratings / picks / person names), shared with the OpenLoupe DAM.

### Changed
- **Faster, less-disruptive metadata sync.** The index updates from live Redis
  keyspace notifications (push) instead of a constant full re-scan, so a background
  index refresh no longer stalls Finder; the full scan is demoted to a rare backstop.
- **Snappier folder opening under load.** Opening a directory while a large copy
  drains returns instantly from the local index instead of showing a spinner.
- **Correct cache-size reporting.** "Cache used" reflects the JuiceFS block cache,
  not just pinned files.

### Fixed
- **Offline bundle copies.** Copying a package with internal symlinks while offline
  no longer fails — the symlinks are deferred and materialized on reconnect.
- **No spurious "connection interrupted" when toggling offline mid-copy.** The health
  watchdog is now offline-aware and won't remount a busy-but-healthy mount.
- **AppleDouble (`._`) sidecars** no longer trip a transient stale-handle warning
  during a metadata sync.
- Additional data-integrity, concurrency, and offline-ingest hardening.

## 2026-06-10 — Launch hardening, Phase 4 (OSS publication hygiene)

- **Apache-2.0.** `LICENSE` + `NOTICE` added (JuiceFS and go-nfs/go-nfs-client
  attribution); README license section matches.
- **Publication README** replaces the developer-oriented one (which moved to
  `docs/dev-setup.md`): verified feature/requirement claims, honest
  comparisons, author-attributed performance numbers.
- **`scripts/uninstall.sh` (new):** stops the app, unmounts NFS/FUSE with a
  refuses-to-rm-through-a-live-mount guard, inventories everything with sizes
  before one confirmation, requires a separate explicit confirmation (or
  `--delete-pending-uploads`) before touching a spool with un-uploaded files,
  shows the JuiceFS chunk-cache size before offering to remove it, and
  supports `--dry-run`/`--yes`. Refuses to run as root.
- **LaunchAgent fixed:** `scripts/com.juicemount.agent.plist` now launches via
  `open -a` (LaunchServices single-instance — can no longer double-launch
  against the in-app "Start at login" toggle) and logs to
  `~/Library/Logs/JuiceMount/agent.log` instead of world-readable /tmp.
- Personal LAN IPs/paths scrubbed from CLI help strings, QA scripts (NAS IP is
  now the `JM_QA_NAS_IP` env var), and example text; docs drift corrected
  (spool opt-in is the Preferences toggle, sudoers examples include
  `/sbin/umount`, Gatekeeper guidance modernized, MENU_BAR_APP reflects the
  current UI, OPEN_BUGS closed out against the hardening commits).

## 2026-06-08/10 — Launch hardening, Phases 1–3b (write-path correctness, spool durability + UX, identity/onboarding, preferences redesign)

Four serialized hardening batches preparing the open-source launch
(ledger: `docs/LAUNCH_PLAN.md`; commits `c29a19f`, `95882fc`, `848dd9e`,
`b47834e`). User-visible summary:

### Phase 1 — write-path correctness (`c29a19f`)
- **Finder mv/rename is now spool-aware.** Renaming a file or folder whose
  contents were still queued for upload previously broke the upload silently
  (data landed at the old path or not at all); renames now migrate queued
  spool entries, including a post-claim re-read that closes the race against
  an in-flight drain.
- **`cp` to the volume no longer exits 1** and truncate over NFS no longer
  fails with "RPC struct is bad" — the SETATTR{size} path is implemented for
  spooled files instead of stubbed.
- Fixed a spool file-handle refcount leak that left entries permanently
  "writing" after interrupted copies (the boot scrubber now also clears them).
- SQLite write contention under rsync-style load fixed (`_txlock=immediate`);
  stress run went from 2/5 failures to 0/10.

### Phase 2 — spool durability + UX (`95882fc`)
- **No more stranded uploads:** disabling the spool, quitting the app, or
  Stop-Everything now waits for (or explicitly hands off) pending uploads
  instead of silently orphaning them; "everything failed" no longer
  auto-quits past the problem.
- **Pending uploads UI:** the popover shows pending/in-flight counts, per-file
  age and last error, stalled/failed badges, and Retry failed / Recover
  stalled buttons (`GET /spool-recover?action=…` on the control plane).
- `/spool` reporting corrected (live size for in-progress writes; done rows
  no longer listed as pending).
- A full spool now reports "disk full" (NFS3ERR_NOSPC) to Finder instead of
  a generic I/O error.

### Phase 3 — identity, onboarding, mount honesty (`848dd9e`, assets `e557a3d`)
- **State-tinted menu-bar icon:** the citrus logo replaces the SF-Symbol
  drive — green healthy, amber degraded, blue offline-files mode, red fault,
  dimmed when idle — plus an upload-activity badge while drains run. Proper
  app icon (AppIcon.icns) from the same mark.
- **First-run Setup Assistant** with preflight checks (juicefs binary,
  macFUSE, backend reachability) and guided errors; reachable later via
  menu → Setup Assistant…. Existing configured installs are migrated as
  already-onboarded (no surprise first-run window).
- **Mount honesty:** NFS-mount state is part of the health model; the popover
  shows "Volume not mounted" with a Mount Now remedy row (single-flight
  `/mount-now` endpoint) instead of pretending all is well.
- At-a-glance popover header: one-word health, cache-vs-free bar, uploads row.

### Phase 3b — preferences redesign (`b47834e`)
- Preferences rebuilt as four grouped tabs (General / Connection /
  Cache & Storage / Maintenance) with sane sizing, clamped numeric fields,
  and whitespace-stripped URL fields.
- **Placebo settings eliminated:** memory-buffer budget, buffer file-size
  limit, and reconcile interval are now actually wired end-to-end (defaults
  byte-identical to before).
- Volume name now derives the mount point; hardcoded `127.0.0.1:11050` /
  mount-point strings removed from the UI (everything respects Preferences).
- Reset Local Metadata Cache flow (soft-stop → delete → Start Now/Later;
  pin database preserved).

## 2026-05-28 — Write spool (Option 2): local-SSD write intermediary

A foundational write-path change, behind `JM_SPOOL_ENABLE` (default off). Interposes
a JuiceMount-owned write spool on local SSD **between the NFS handler and JuiceFS's
raw staging**, so Finder writes ack the moment they're durable on local SSD
(`fsync()`) and a background drainer copies them into JuiceFS at MinIO's pace. Fixes
the WAN write cliff — 2 GB / 600-file Finder copies over Tailscale had hit ~2-hour
ETAs (~280 KB/s) because JuiceFS back-pressures on a RAM-tracked `--buffer-size`
budget even with 700+ GB of disk headroom. This is the Dropbox / LucidLink / Suite
write model, without forking JuiceFS. All 8 slices CI-green on `production-hardening`
(mirrored to `main`).

### What landed (slices A–H)

- **Spool primitives + SQLite index** (`nfs/spool.go`, `nfs/spool_index.go`,
  `metadata/spool_schema.go`, `metadata/spool_store.go`). `spool_entries` table in the
  existing metadata DB (shares its WAL + `writeMu`); capacity cap with atomic CAS
  reservation (default 50 GiB); refcounted `SpoolEntry` with streaming SHA-256.
- **Drainer** (`nfs/drainer.go`). Single dispatcher + bounded worker pool (default 4);
  `ListReady → MarkDraining → copy-to-FUSE → SHA-verify → MarkDrainComplete`.
  Exponential-backoff retry (out-of-worker timer so a slot isn't held during backoff),
  `MaxAttempts` 5 → `failed`; SHA mismatch → quarantine (never deleted).
- **Write-path integration** (`nfs/handler.go`, `nfs/spool_writefile.go`). `O_CREATE`
  routes through `spoolWriteFile`; acks at local SSD speed. nil-spool = unchanged
  `writeFile` path.
- **Read-path 3-tier lookup** (`nfs/handler.go`, `nfs/spool_readfile.go`). `spoolReadFile`
  + Stat/Lstat shadow serve not-yet-drained files from the spool. QA-35 perf-gated:
  empty-spool lookup benchmarked ~8 ns.
- **Runtime wiring + `GET /spool`** (`bridge/cbridge.go`, `cmd/jm5/main.go`,
  `nfs/spool_status.go`). Live pending/in-progress/failed/quarantined + per-entry list
  on the control plane (port 11050); 503 when disabled.
- **Crash recovery** (`SpoolStore.RecoverOnBoot`, `nfs/spool_recover_test.go`). Boot
  scrubber reconciles on-disk files vs SQL rows: orphan cleanup, `writing`→failed,
  `draining`→ready, capacity re-accounting. Runs before `drainer.Start`.
- **Integrity audit log** (`nfs/spool_manifest.go`). Append-only `manifest.log` with
  SHA-256 + timestamp for every drain-done and quarantine event. SHA-256 verified at
  three hops: streaming on write, on drain-read, and at-rest through FUSE after copy.
- **WAN-mode polish** (`health/fuse.go`). `JM_WAN_MODE=1` raises JuiceFS `--max-uploads`
  20 → 64; `JM_MAX_UPLOADS=<n>` overrides directly. `--buffer-size` stays at the QA-33
  value of 4096 — the spool, not a bigger RAM buffer, absorbs the write burst.

### Configuration

`JM_SPOOL_ENABLE=1` to opt in. `JM_SPOOL_DIR` (default
`~/Library/Application Support/JuiceMount/spool/`), `JM_SPOOL_SIZE_GB` (default 50),
`JM_WAN_MODE`, `JM_MAX_UPLOADS`. Rollback: set `JM_SPOOL_ENABLE=0` and restart.

### Deferred (not in this change)

Swift menu-bar "Pending uploads" section + icon badge, Manager web-UI tile,
`App.swift` graceful-quit dialog, Preferences "Sync & Upload" pane, 24-hour live soak.
The `/spool` JSON contract is stable, so these can land independently.

Full design + architecture: `docs/ROADMAP/option-2-spool.md`, `ARCHITECTURE_juicemount.md` § 15.

## 2026-05-13 — Phase A+B+C: production safeguards landed in parallel

Three agents working in parallel landed the next batch of production-readiness
work the day after the FileProvider ghost incident. Diff: 622 lines across
8 files; no unit-test regressions.

### Phase A — self-defense against environmental conflicts

- **Pre-mount conflict probe** (`bridge/cbridge.go:128`, `preMountConflictCheck` + `mountAt` helpers). Before the juicefs and NFS mounts run, walk the kernel mount table; if a foreign mount already owns the FUSE or NFS path, refuse to mount and surface a clear error string to Swift (including the source, type, and a `diskutil unmount` hint). Our own mounts (`JuiceFS:*` at FUSE path, `127.0.0.1:*` at NFS path) pass through and the existing soft-Stop reuse logic kicks in. Catches the "another FileProvider claimed /Volumes/zpool" failure mode at launch instead of in production.
- **Post-mount self-test** (`bridge/cbridge.go` `runSelfTest`/`SelfTestResult`, HTTP `GET /self-test` + `POST /self-test`). 10 MB read against the live NFS mount, classified green (≥200 MB/s) / yellow (50–200 MB/s) / red (<50 MB/s). Target selection: walks SQLite for any file ≥10 MB; falls back to a tmp file in the mount if none. Auto-runs once at server start, rerunnable via POST. Swift integration: `NFSBridge.SelfTestResult` + `selfTest(force:)` API; `ServerController.refreshSelfTest`; `MenuBarController` overlays a 5-pt colored dot in the icon's lower-right when self-test is yellow/red/error; `MenuPopoverView` `selfTestRow` shows "Self-test: 247 MB/s ✓" in the cache section.

### Phase B — observability

- **`/debug/pprof/*` endpoints** on the existing metrics server (port 11050). Five routes: `/`, `/cmdline`, `/profile`, `/symbol`, `/trace`. Live goroutine/heap/CPU/trace inspection without a separate listener. Would have saved hours of stack-archaeology during the 2026-05-12 incident.
- **Export Diagnostics button** in the menu-bar popover (`app/JuiceMount/Sources/JuiceMount/Core/DiagnosticsExporter.swift`, new). Bundles a 10-file zip onto the user's Desktop: tail of `juicemount.log` (5 MiB) and `juicefs.log` (1 MiB); current `/metrics` and `/cache-status` snapshots; `pluginkit -m`, `fileproviderctl dump` (first 200 lines), `df -h`, `mount`, `nfsstat -m`, plus a `system.txt` with macOS version, app version, anonymized hostname, and pid map. Cross-volume safe (copy+remove fallback), timeout-bounded subprocesses, deadlock-safe pipe draining on dispatch queues. `errors.txt` only when something fails — never crashes the export.

### Phase C1 — Developer ID signing + notarization

- **`scripts/build-app.sh` codesigning rewrite**. Auto-detects a Developer ID Application cert via `security find-identity`; falls back to ad-hoc with a yellow WARNING on the dev path. Env vars: `JM_SIGN_IDENTITY` (override cert), `JM_NOTARY_PROFILE` (default `JuiceMount`), `JM_QUICK=1` (skip notarization). Builds with `--timestamp --options runtime` when signing real, omits `--timestamp` on ad-hoc (Apple rejects it). Build footer now prints `Identity / Notarized / Staple` lines.
- **Notarization gated**. Only fires when: `JM_QUICK` unset, identity is not ad-hoc, AND a notary keychain profile is reachable. Uses `xcrun notarytool submit --wait` → `stapler staple` → `stapler validate`. Failures degrade to a WARNING (dev iteration never breaks).
- **`docs/signing.md`** (new) — first-time-setup walkthrough: how to create the Developer ID Application cert in Xcode, how to store the notarization credential profile (`xcrun notarytool store-credentials`), what each env var controls, verification commands (`spctl`, `stapler validate`), common errors, and a back-reference to `docs/no-fileprovider.md`.

### Coordination notes

- All three agents were briefed to work on non-overlapping file sections; merge was conflict-free.
- `bridge/cbridge.go` grew 391 lines (mostly Phase A's self-test + conflict probe logic).
- `MenuPopoverView.swift` grew 66 lines (Phase A self-test row + Phase B Export button).
- Pre-existing test failures in `nfs/` E2E tests confirmed environmental (need a live FUSE+Redis backend); unit packages `internal/jmlog`, `internal/cache/pin`, `cache`, `metadata` all green.

## 2026-05-07 / 2026-05-08 — Production-Hardening Branch (Phase 3 ship)

Eight commits on the `production-hardening` branch turning the prototype-quality menu-bar app into something a video editor can put real cellular sessions through. The headline features were already in earlier prototypes; this work is what made them survive contact with a 97%-full disk, an unreachable NAS at launch, and a pinned-set bigger than the configured cache.

### Offline mode + pin store (prototype 03 → production)

- `internal/cache/pin/` SQLite-backed pin registry with statuses Pending / Prefetching / Ready / Failed / Unpinned, indexed primary-key lookup via `IsPinnedReady(path)` (hot path on every NFS OpenFile when offline mode is on).
- 4-worker bounded prefetcher pool reading through FUSE in 1 MB chunks. JuiceFS LRU caches each block as it streams.
- `Prefetcher.PullPending(ctx, batchSize)` long-running daemon that drains Pending rows into the worker queue every 5s. Avoids the queue-overflow problem (256-slot ring) when the user pins large sets at once.
- `Prefetcher.ReWarmupLoop(ctx, ttl, batchSize)` periodic re-warmer (every 15 min, files older than 6 hours).
- `Prefetcher.VerifyAndRepair(ctx)` walks every Ready / Prefetching / Failed entry and marks them Pending so PullPending re-feeds the worker pool. The recovery path for "FUSE was momentarily unmounted at restart, every open() returned ENOENT, all 329 entries got marked Failed."
- Atomic `pin.IsOffline()` flag, set via `pin.SetOffline(bool)`. Read on every OpenFile and ReadAt; lock-free.
- **Open-time gate** in `nfs/handler.go` `OpenFile`: refuses un-pinned reads in offline mode in ~6 ms (vs. the 14-second NFS-retry timeout the read-time-only gate produced earlier).
- **Read-time gate** in `cachedFile.ReadAt`: only refuses reads on un-pinned files; pinned files fall through to FUSE/JuiceFS LRU at multi-GB/s.
- `Prefetcher.stripMountPrefix(path)` translates canonical pin keys back to FUSE-relative paths using the configured `mountPoint`, replacing the hardcoded `/Volumes/zpool/` constant.
- `JuiceMountHandler.canonicalize(filename)` translates in-mount filenames to canonical pin-store keys, handling plain-relative, leading-slash-relative, already-absolute, and trailing-slash-on-mount-point cases. Replaces the hardcoded `/Volumes/zpool/` + filename concat.

### macOS Services right-click + Pin Folder UI

- `app/JuiceMount/Sources/JuiceMount/Core/FinderService.swift` `@MainActor` class implementing `pinForOffline` and `toggleOfflineMode` via `@objc` methods, registered as `NSApp.servicesProvider`.
- `Info.plist` `NSServices` array declares both services with `public.file-url` send types. After enabling once in System Settings → Keyboard → Keyboard Shortcuts → Services, "JuiceMount: Pin for Offline" appears in Finder right-click → Services.
- `MenuPopoverView` Pin Folder button with native `NSOpenPanel` rooted at the mount point. Multiple-folder selection. Pin work runs on a background queue.

### JuiceFS cache config (the hardest-fought regression fix)

- **`--cache-size` plumbing.** GUI shim (`bridge/cbridge.go`) was constructing `FUSEManager` without forwarding `cfg.CacheSize`, so the user's `ssdCacheGB` preference was silently ignored. CLI (`cmd/jm5/main.go`) had been passing `100000` (100 GiB) all along.
- **`--free-space-ratio 0.01`.** JuiceFS default is 0.1 (refuse to cache when disk drops below 10% free). On a 97%-full video-editor's laptop, that meant every read went straight to S3. Lowered to 1% — the disk IS the cache.
- **`--cache-size` auto-expansion.** `FUSEManager.Mount()` now sizes the cache to 85% of total disk (with the user's configured value as a *floor*, not a ceiling). On a 926 GiB MacBook this expands a 100 GiB configured cache to 787 GiB, headroom enough for any realistic pinned set. Free-space-ratio still enforces the upper bound at write time.
- **`tailJuiceFSLog()`** goroutine watches `~/.juicefs/juicefs.log` and promotes WARNING/ERROR lines into `jmlog`. The chatty "space not enough on device" pattern is aggregated (one summary per 60 s with a count) so a flooded daemon log doesn't drown our log, but the user CAN see when the JuiceFS daemon is unhappy.
- **`checkCacheVolumeHealth()`** runs at mount time, emits a `jmlog.Warn` if the volume's free ratio is below the configured threshold.

### APFS purgeable-space reclamation

- `health.ReclaimPurgeableSpace(volume, targetBytes)` shells out to `tmutil thinlocalsnapshots <vol> <amount> 4` (urgency 4 = thin as much as possible). Measures freed bytes via statfs before/after.
- **Auto-reclaim before mount.** `FUSEManager.Mount()` checks free space; if below 50 GB it calls `ReclaimPurgeableSpace`. On the user's machine this freed 210.5 GB (49 GB → 260 GB free) by thinning a single Time Machine local snapshot.
- HTTP `POST /reclaim` endpoint on the control plane returns `{ok, freed_bytes, freed_gb}`.
- Swift popover shows a one-line "X GB free · Y GB reclaimable" with a Reclaim button when there's ≥ 5 GB reclaimable. Disk numbers are read via Foundation's `URLResourceKey.volumeAvailableCapacityForImportantUsageKey` — the only public macOS API that reports purgeable-aware capacity correctly.
- Disk-pressure banner in popover with three real states:
  - free < 1% of total → red, "JuiceFS has stopped caching"
  - free < 3% of total → orange, "JuiceFS will stop caching at 1% free"
  - pinned set > total disk capacity → red, "Pinned set exceeds disk capacity"

### Logging + control plane

- `internal/jmlog/jmlog.go` `rotatingFile` wrapper: 16 MB × 5 generations = 80 MB cap. Per-instance rotation flag (was package-level — re-Init bug). Auto-mkdir.
- `Preferences.defaultLogPath()` → `~/Library/Logs/JuiceMount/juicemount.log`. Swift app forwards `logFile` and `logLevel` to cbridge.
- HTTP control plane on the metrics port (11050):
  - `GET /metrics` — Prometheus-style counters
  - `GET /cache-status` — pin aggregate, per-root totals, live prefetch stats, offline mode flag
  - `POST /pin?path=...` — register a path/folder for offline pinning
  - `POST /unpin?path=...` — remove from registry
  - `GET /offline?on=1|0` — toggle offline mode
  - `POST /reclaim` — thin Time Machine local snapshots
  - `POST /verify-pins` — re-verify all pinned coverage by marking everything Pending

### Resilience

- `connectRedisWithRetry()` wraps `metadata.NewRedisClient` with exponential backoff (1 s, 2 s, 4 s, 8 s, 16 s = 5 attempts ≈ 31 s worst case). Survives wifi/cellular handoffs at launch. Logs each retry; emits "redis connect recovered" on success.
- Sync Now button now triggers BOTH metadata sync AND `verify-pins`. The user's mental model — "press Sync to make sure everything's current" — actually does what they expect.

### NLE project parsers (deferred, not wired into menu UI yet)

- `internal/nle/parser.go`, `premiere.go`, `resolve.go`, `fcpx.go` — extract media references from `.prproj` (gzipped XML), `.drp` (zip + SQLite), `.fcpxml`. 13/13 unit tests pass. Smoke-tested on a real Tampa Restaurants Premiere project. CLI exposed as `juicemount prefetch-project <file>` but no UI button yet.

### Tests added

- `internal/jmlog/TestRotation` — locks in 16 MB × 5 generations cap.
- `nfs/canonicalize_test.go` — 7 cases: plain, leading slash, absolute under mount, trailing slash on mount, non-default mount, empty fallback, root file.
- `nfs/billyfile_offline_test.go` — 4 states: online unpinned, offline unpinned, offline pinned, ReadAt and Read variants.
- `internal/cache/pin/TestIsPinnedReadyWindow` — 6 transition states including the late-Ready window where status is still Prefetching but bytes_cached >= size.
- `internal/cache/pin/TestStripMountPrefix` — 6 cases for the prefetcher's path translation.

### Bug fixes worth flagging

- **`OpenFile` open-time gate added.** Previously offline mode only refused reads at `cachedFile.ReadAt` time; small reads (<= 1 MB) succeeded silently because they'd hit the kernel page cache or memory buffer first. A 137 MB file took 14 seconds to fail. Open-time gate now fires in ~6 ms regardless of read size.
- **`cachedFile.pinned` snapshot** captured at OpenFile time. Without this, the read-time offline gate refused pinned files when the SSD cache reader missed (a separate cache layer from JuiceFS LRU), even though FUSE could have served the bytes locally.
- **`billyFile` offline-guard gap** (caught by independent code review). billyFile is the read path for files not yet in the SQLite metadata cache (fresh directory traversal). It had no read-time offline gate. Fixed: `billyFile.Read` and `billyFile.ReadAt` now both check `pin.IsOffline() && !f.pinned`.

### Verified live (snapshot at end of session)

- Pinned set: 331 files, 159 GB. All Ready after Sync triggers a verify-and-repair cycle.
- 200 MiB sequential read on a pinned file: **431 MB/s, 4.6 MB of network traffic** (Redis chunk-mapping lookups only — no bulk S3 fetches).
- Random-seek reads on pinned media: 50 MB blocks at 16–481 MB/s depending on cache hit rate.
- Pinned-offline reads: 215+ MB/s sustained (FUSE → JuiceFS LRU).
- Unpinned-offline reads: 4–67 ms fail-fast EIO (was 14 s pre-fix on a 137 MB file).

### Branch state

8 commits on `production-hardening`, parented on `prototype/offline-pin`. Squash-merge candidate to `main` once cellular soak test confirms no regressions.

```
9a1f229 feat(verify): Sync now also re-verifies pinned coverage; surface cache pressure
983b795 fix(cache): auto-expand --cache-size; user's 100 GiB cap was evicting all reads
56c2f6c feat(cache): reclaim APFS purgeable space for JuiceFS cache
21da8ee fix(cache): restore JuiceFS local caching — biggest GUI regression
f96ba16 production-hardening: address review findings + cold-start retry
a6db0c3 production-hardening: logging, path canonicalization, offline gate fixes
0c72399 prototype: baseline offline-pin + production hardening branch
c177cab prototype 03: offline-pin (power-user pre-cache + offline mode)
```

---

## 2026-05-07 — Phase 2 Menu Bar App + Phase 3 Observability

### Phase 2 — Native macOS Menu Bar App

JuiceMount now ships as a proper macOS menu bar application built in SwiftUI 6.2 with a Go c-archive backend. No more CLI-only operation.

**Architecture:**
- `app/JuiceMount/` — Swift Package with executable target
- `app/JuiceMount/Sources/JuiceMountCore/` — C interop layer that imports the Go-generated `libnfsd.h`
- `bridge/cbridge.go` — Go c-archive exports: `NFSServerStart`, `NFSServerStop`, `NFSServerIsRunning`, `NFSServerStats`, `NFSServerSyncNow`, `NFSServerSearch`, `NFSServerFreeString`
- All NFSBridge calls dispatched to a background queue so the UI never blocks

**Menu bar UI** (`MenuBarController` + `MenuPopoverView`):
- SF Symbol-based status icon with state-driven badge: idle, starting, running, syncing, degraded, disconnected, error
- Polished popover with header, volume info (entries count, last sync), per-backend health (Redis/MinIO/FUSE), and action buttons
- AppKit `NSStatusItem` for native feel; popover content is pure SwiftUI

**Search window** (`SearchWindowView`):
- Type-as-you-search with 150ms debounce and parent-path scope picker
- Native `Table` view with name/path/size columns and color-coded file-type icons
- **Spacebar opens `QLPreviewPanel`** for native Quick Look preview — the workflow you wanted for finding sound effects/footage by name and previewing instantly
- Enter / double-click reveals selection in Finder
- Drag to NLE (Premiere/Resolve/FCPX) supported via standard NSURL drag
- Result limit configurable: 50/100/250/1000

**Preferences window** (`PreferencesWindowView`):
- 4 tabs: General, Server, Cache, Advanced
- Persists to `UserDefaults`; volume name, mount point, Redis URL, NFS listen addr, SSD cache size, memory buffer budget, reconcile interval
- "Start at Login" via `SMAppService.mainApp` (macOS 13+)
- "Reset Local Metadata Cache" maintenance action

**Global hotkey** (`HotkeyManager`):
- ⌘⇧F opens search window from any app
- Carbon Event Manager (no Accessibility permission required)
- Toggleable in Preferences

**Build & deploy:**
- `scripts/build-app.sh` — full pipeline (Go c-archive → Swift build → `.app` bundle assembly → ad-hoc codesign)
- `scripts/build-cli.sh` — CLI-only build with entitlement codesigning
- `scripts/install.sh` — install to `/Applications`, optionally enable LaunchAgent
- `scripts/com.juicemount.agent.plist` — LaunchAgent for auto-start with crash-only relaunch

### Phase 1.4 — Codesigning

CLI binary is now codesigned with `com.apple.security.network.client` + `com.apple.security.network.server` entitlements via `scripts/build-cli.sh`. Eliminates the `ssh localhost` workaround needed to bypass macOS's 10GbE network filter.

### Phase 3 — Production Hardening (in progress)

Started during the same session by a background agent:

- **Structured logging** (`internal/jmlog`): replaces `log.Printf` with `log/slog` JSON handler. Levels: Debug/Info/Warn/Error. Optional file output via `--log-file`.
- **Per-RPC latency histograms** (`internal/metrics`): GETATTR, LOOKUP, READ, WRITE, READDIR, READDIRPLUS, CREATE, REMOVE, RENAME timed individually. Counters for total RPCs, errors, bytes.
- **/metrics endpoint**: small zero-deps HTTP server bound to `127.0.0.1:11050`. JSON output with p50/p95/p99 percentiles.
- **NFS auto-remount on stale mount**: Health monitor detects stale FUSE/NFS mounts via 5s `Stat()` timeout, gates remount on 3 consecutive failures + 60s cooldown.

### Files Added

| Path | Purpose |
|------|---------|
| `app/JuiceMount/Package.swift` | Swift Package definition |
| `app/JuiceMount/Sources/JuiceMountCore/include/JuiceMountCore.h` | C bridge header |
| `app/JuiceMount/Sources/JuiceMount/App.swift` | App entry point |
| `app/JuiceMount/Sources/JuiceMount/Core/NFSBridge.swift` | Idiomatic Swift wrapper around c-archive |
| `app/JuiceMount/Sources/JuiceMount/Core/ServerController.swift` | `@Observable` lifecycle controller |
| `app/JuiceMount/Sources/JuiceMount/Core/Preferences.swift` | UserDefaults-backed prefs |
| `app/JuiceMount/Sources/JuiceMount/Core/HotkeyManager.swift` | Global ⌘⇧F hotkey |
| `app/JuiceMount/Sources/JuiceMount/Core/LoginItemManager.swift` | SMAppService start-at-login |
| `app/JuiceMount/Sources/JuiceMount/UI/MenuBarController.swift` | NSStatusItem + popover host |
| `app/JuiceMount/Sources/JuiceMount/UI/MenuPopoverView.swift` | Menu popover SwiftUI view |
| `app/JuiceMount/Sources/JuiceMount/UI/SearchWindowView.swift` | Search window with QuickLook |
| `app/JuiceMount/Sources/JuiceMount/UI/QuickLookCoordinator.swift` | QLPreviewPanel bridge |
| `app/JuiceMount/Sources/JuiceMount/UI/PreferencesWindowView.swift` | Preferences tabs |
| `app/JuiceMount/Resources/Info.plist` | Bundle Info.plist (LSUIElement, sandbox keys) |
| `scripts/build-app.sh` | Full build pipeline |
| `scripts/build-cli.sh` | CLI-only build with codesigning |
| `scripts/install.sh` | Install to /Applications + LaunchAgent |
| `scripts/com.juicemount.agent.plist` | LaunchAgent definition |
| `internal/jmlog/` | Structured logging package (Phase 3) |
| `internal/metrics/` | Latency histograms + /metrics HTTP server (Phase 3) |

---

## 2026-03-31 — Code Audit & Full-Stack Overhaul

### Summary
Comprehensive code audit identified 8 correctness bugs, 8 performance issues, and 8 architectural gaps. All critical items addressed. JuiceFS FUSE mount management integrated into the JM5 process — JM5 is now a complete self-contained stack.

---

### Correctness Fixes

1. **Race condition in inode counter** (`nfs/handler.go`)
   - `inodeCounter` was bare `uint64` incremented without synchronization across goroutines
   - Fixed: changed to `atomic.Uint64`

2. **MemoryBuffer never invalidated on writes** (`nfs/handler.go`)
   - A file cached in MemoryBuffer would serve stale data after NFS writes, renames, or deletes
   - Fixed: `writeFile.Close()`, `Rename()`, and `Remove()` now call `memBuf.Invalidate()`

3. **writeSizes map leaked memory** (`nfs/handler.go`)
   - `trackWriteSize()` added entries but nothing ever removed them
   - Fixed: `writeFile.Close()` deletes the entry after use

4. **Remove() deleted from FUSE in background** (`nfs/handler.go`)
   - Returned success to NFS client before the file was actually removed on disk
   - Fixed: FUSE `os.Remove()` is now synchronous

5. **FDPool double-check race** (`nfs/fdpool.go`)
   - `Get()` and `GetWrite()` accessed entry fields outside the lock after the double-check
   - Fixed: all entry access is now under the lock

6. **UID/GID=0 on metadata-cached entries** (`metadata/types.go`, `nfs/handler.go`)
   - `metadata.FileInfo.Sys()` returned `*metadata.Entry` which the NFS attribute builder didn't recognize, falling back to UID=0/GID=0 (root:wheel)
   - macOS Finder showed red "no access" minus signs on every folder
   - Fixed: `Sys()` now returns `*syscall.Stat_t` with the current user's UID/GID
   - Same fix applied to `rootDirInfo`

### Performance Improvements

1. **O(1) ListChildren** (`metadata/store.go`)
   - Added `childrenIdx map[string]map[string]*Entry` (parent -> children)
   - `ListChildren()` was O(total entries) scanning all 147K entries per ReadDir
   - Now O(children count) — direct map lookup
   - All 8 mutation points (Insert, Delete, BulkInsert, etc.) maintain the index

2. **READ/WRITE buffer pools** (`internal/nfs/nfs_onread.go`, `nfs_onwrite.go`)
   - Added `sync.Pool` for 1MB read data buffers (eliminates per-RPC allocation)
   - Response writers use the existing `responseBufferPool` instead of `bytes.NewBuffer([]byte{})`

3. **Pre-serialized GETATTR XDR** (`internal/nfs/nfs_ongetattr.go`)
   - Already implemented in codebase; now works correctly with the UID/GID fix
   - 88-byte cached response body, zero XDR marshaling on cache hit

4. **BulkClearLocalOnly** (`metadata/store.go`, `metadata/redis.go`)
   - Discovered during testing: `syncMetadata()` called `ClearLocalOnly()` individually for 147K entries (147K SQLite UPDATE transactions)
   - Added `BulkClearLocalOnly()` that batches 500 per transaction
   - Redis sync time on cell hotspot: >60s timeout -> 85s pass; on 10GbE: ~7s

5. **Readahead uses FDPool** (`nfs/readahead.go`)
   - Prefetch goroutines now reuse pooled file descriptors instead of opening new ones

### Architecture Changes

1. **Removed double handle caching** (`nfs/server.go`)
   - `helpers.NewCachingHandler` wrapper was adding UUID-based LRU handles on top of our deterministic inode-based handles
   - Removed: NFS handler now served directly, eliminating UUID allocation, LRU eviction, and mutex contention

2. **Bounded prefetch goroutines** (`nfs/handler.go`)
   - Recursive `prefetchChildren` could spawn unbounded goroutines
   - Fixed: non-blocking semaphore acquisition before spawning sub-prefetches

3. **TCP keepalive** (`nfs/server.go`)
   - Added `SetKeepAlive(true)` + `SetKeepAlivePeriod(30s)` on NFS connections
   - Stale client connections (laptop sleep, WiFi drop) are now detected and cleaned up

4. **Graceful shutdown ordering** (`cmd/jm5/main.go`)
   - Replaced defer-based shutdown with explicit ordered sequence:
     unmount NFS -> stop server -> stop handler -> stop health -> stop Redis -> close SQLite -> unmount FUSE

5. **Stale prefetch map cleanup** (`nfs/handler.go`)
   - `prefetched` map entries now evicted after 2 minutes by the existing cleanup loop

6. **JuiceFS FUSE mount management** (`health/fuse.go`, `cmd/jm5/main.go`)
   - JM5 now mounts JuiceFS FUSE automatically on startup (no manual `juicefs mount` needed)
   - Background monitor checks mount health every 10s, auto-remounts on crash
   - Kills stale JuiceFS processes before remount
   - FUSE mounted at hidden `~/.juicemount/fuse-internal` (invisible to user)
   - Users only see the NFS volume at `/Volumes/zpool`
   - `--no-fuse` flag for testing/development

7. **Real FUSE health check** (`health/monitor.go`)
   - Old check: `os.Stat(fusePath)` — passes even when FUSE is unmounted (directory still exists)
   - New check: verifies mount table entry exists AND `ReadDir` responds within 5s

### Dead Code Cleanup
- Removed unused `cacheWarm` field from ReadaheadManager
- Removed dead `rdb` creation in main.go (was printing a pointer address)
- Removed unused `context` import in cbridge.go

### Test Fixes
- `TestReadCachedBlock`: added 30s context timeout and reduced scan iterations (was hanging forever over slow networks)
- `TestFileInfo`: updated to expect `*syscall.Stat_t` from `Sys()` instead of `*metadata.Entry`
- `ReadaheadManager` test calls updated for new constructor signature and `Stats()` return values

---

### Files Modified (16 files)

| File | Changes |
|------|---------|
| `cmd/jm5/main.go` | FUSE manager integration, graceful shutdown, dead code, flags |
| `nfs/handler.go` | atomic inode, membuf invalidation, writeSizes cleanup, sync Remove, bounded prefetch, prefetched cleanup, rootDirInfo UID/GID |
| `nfs/server.go` | Removed CachingHandler wrapper, TCP keepalive |
| `nfs/fdpool.go` | Double-check race fix |
| `nfs/readahead.go` | FDPool integration, removed unused field |
| `nfs/readahead_test.go` | Updated constructor and Stats() calls |
| `metadata/store.go` | Children index, BulkClearLocalOnly |
| `metadata/types.go` | Sys() returns *syscall.Stat_t with correct UID/GID |
| `metadata/redis.go` | Uses BulkClearLocalOnly |
| `metadata/store_test.go` | Updated TestFileInfo for new Sys() |
| `cache/reader_test.go` | Context timeout on TestReadCachedBlock |
| `internal/nfs/nfs_onread.go` | Buffer pool for read data + response |
| `internal/nfs/nfs_onwrite.go` | Buffer pool for response |
| `health/fuse.go` | NEW — JuiceFS FUSE mount lifecycle manager |
| `health/monitor.go` | Real FUSE health check (mount table + readdir) |
| `bridge/cbridge.go` | Dead code cleanup |

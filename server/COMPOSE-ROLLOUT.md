# Canonical Compose rollout (0.5)

`server/scripts/rollout-compose.sh` updates **only** `juicemount-manager` and
`juicefarm` in a project started from this repository's
`server/docker-compose.yml`. It is a host-local command, not the standalone
container updater. It never runs a full-stack `compose up`, never requests
`redis`, `minio`, `juicefs`, or `juicefs-init` as an update target, and never
edits their bind-mounted data. The existing `update-server.sh` continues to
refuse Compose-owned containers.

The operator must first:

1. Confirm the farm queue is quiesced and decide whether existing per-node
   pause/hold state should remain. Do not use this command to clear a hold.
2. Check out the exact, clean release commit **on the NAS host** in the same
   directory whose `server/docker-compose.yml` owns the running project. Keep
   the private `server/.env` there; do not commit or print its credentials. The
   rollout user must own this regular file. It must contain exactly one plain
   `JM_MANAGER_DIGEST=sha256:...`, `JM_FARM_DIGEST=sha256:...`, and
   `JM_FARM_RENDER_DIGEST=sha256:...` assignment matching the live images.
   Unset those variables in the invoking shell so `.env` remains the persistent
   Compose authority.
3. Have the three published, immutable manifest digests for that exact commit:
   Manager, CPU farm, and GPU farm. `docker`, Compose v2, `jq`, and Git must be
   available. Ensure there is enough free space to pull all three images.
4. Ensure no stale `.update-lock` remains. If one exists, inspect the project
   and previous rollout result before manually removing it. The lock is shared
   with `update-server.sh` when both use this checkout as their deployment root.
   Do not edit `.env` or run another Compose update during the maintenance
   window; the lock is advisory to other operators.

From that host checkout, first inspect the plan:

```bash
bash server/scripts/rollout-compose.sh \
  --project YOUR_COMPOSE_PROJECT \
  --manager-digest sha256:MANAGER_HEX \
  --farm-digest sha256:FARM_HEX \
  --render-digest sha256:GPU_HEX \
  --ack-worker-quiesced --dry-run
```

Remove `--dry-run` only after the operator verifies the project, credentials,
images, worker state, and maintenance window. The command refuses dirty source,
unknown or mutable prior images, a changed Manager key, nonmatching Compose
labels/config hashes, missing prior rollback images, and ambiguous or stopped
services. Redis, MinIO, and JuiceFS must be running and Docker-healthy;
`juicefs-init` must have exited successfully. It pulls and checks the
`--build-info` identity of all three candidate images before changing a
service. It snapshots `.env` under the private lock before preflight and refuses
to continue if it changes before replacement. It atomically replaces only
the three digest assignments with a mode-0600 file, and verifies plain Compose
rendering resolves to the new images. Credentials and other `.env` lines are
preserved and never logged. It then uses
`docker compose up -d --no-deps --no-build --pull never --force-recreate`
with only the Manager and farm service names. It verifies their running image
refs, binary identities, the authenticated Manager `/api/farm` response, and
unchanged **and healthy** protected containers. Failure restores the previous
`.env` atomically and, if service recreation began, attempts to recreate both
services from their previous digest-pinned images. A failed file restore or
service rollback leaves `.update-lock` and the credential-bearing private
backup for operator investigation. Treat the lock directory as sensitive.
Termination during final verification still restores both `.env` and the old
services; private temporary files are removed by the trap. After a host crash,
inspect `.env`, running image refs, and the lock backup
before deciding whether to restore or finish the interrupted rollout.

**Important boundary:** TrueNAS Apps often generate a different Compose file,
project directory, labels, or runtime environment. This script intentionally
rejects that topology. Do not bypass its config-hash/ownership checks or edit
the TrueNAS app YAML just to make them pass. A TrueNAS-aware rollout needs its
own export, credential-preservation, application ownership, and recovery
rehearsal. This path also does not deploy/restart the separate GPU worker node;
its digest is verified only because Manager advertises it for enrollment.

Offline failure-injection test:

```bash
bash server/scripts/rollout-compose.test.sh
```

The test covers dry-run, success, hash/key and persistent-digest preflight
rejection, concurrent `.env` editing, failed atomic replacement, failed Compose
up, new workload or protected-service health failure, post-verification error
and TERM, previous-image/file rollback, and lock
retention on rollback failure. A NAS rehearsal and post-deploy Mac
read/write/rollback acceptance are still required before calling this release
gate closed.

## H-8B RESP proxy: port swap, mode switch, and rollback

Since H-8B, Redis publishes no host port. The `respproxy` service owns host
port 30179 and is the only path to the volume metadata. Every client logs in
to it as a named principal: Macs enroll in the Manager, and the server
principals `farm`, `manager`, `bg` (the compose `juicefs` mount, which runs
the background jobs; the Manager also uses it for gc/compact/fsck), `webdav`
(no write rights) and `admin` (`juicefs config`, lease break, mode switch)
come from `server/.env`. Unauthenticated connections are refused in every
mode. There is no backward compatibility for 0.4 Macs.

A release rollout (the default mode above) refuses a stack that has no
`respproxy` and names the cutover instead. The port swap is its own staged
mode of the same script.

### Prerequisites

1. Every Mac runs the 0.5 app.
2. The `JM_RESPPROXY_DIGEST` image is built from a commit that descends from
   `server/respproxy/min-gate-commit` (the merged lease engine). The cutover
   script reads the image's `org.opencontainers.image.revision` label and
   refuses an older image: an older proxy lets any principal run `FLUSHDB` in
   passthrough, or enforces no project exclusivity.
3. The NAS checkout is the clean release commit that contains the proxy
   compose. `--previous-ref` names the commit or tag whose
   `server/docker-compose.yml` the running stack was created from (Redis
   publishing 30179, no `respproxy`).
4. `server/.env` has, once each, `JM_RESPPROXY_DIGEST=sha256:…` and the six
   secrets `JM_REDIS_PROXY_PASSWORD`, `JM_PROXY_FARM_PASSWORD`,
   `JM_PROXY_MANAGER_PASSWORD`, `JM_PROXY_BG_PASSWORD`,
   `JM_PROXY_WEBDAV_PASSWORD`, `JM_PROXY_ADMIN_PASSWORD`. Generate each with
   `openssl rand -hex 32`. The script refuses values that are shorter than 32
   characters or not URL-safe, and never prints them. See
   `server/release.env.example`.
   It **also** needs `JM_OBJ_CLIENT_SECRET_KEY` and `JM_OBJ_GC_SECRET_KEY`
   (each `openssl rand -hex 20`): the compose's `minio-keys` service requires
   them at every `up`, including this cutover, and the real run refuses at
   `compose config` without them (`--dry-run` stops before that check). Adding
   them changes nothing in use; see "Object keys" Step A.
5. The host directory `/mnt/zSSD/juicemount/respproxy` (or your edited path)
   exists. Manager and `respproxy` share it: principals file (mode 0600),
   rollback fragment, reload handshake. Back it up with the Manager state.

### Step 0: quiesce (maintenance window)

1. On **every** Mac: let JuiceMount drain its spool to zero (menu bar shows
   nothing pending), then quit JuiceMount. A Mac that keeps a session or a
   slice-id batch from before the swap owns no session record at the proxy:
   its heartbeats are refused `BG_ONLY` and its old-batch slice writes
   `ADMIN_ONLY`, and it does not recover by itself.
2. Quiesce the farm queue (`--ack-worker-quiesced`).
3. The script itself stops farm, Manager and `juicefs` before it restarts
   Redis, and Redis has `stop_grace_period: 120s`, so its RDB save on SIGTERM
   is not killed. The script refuses to run without `--ack-macs-drained`.

### Step 1: passthrough (the port swap)

```bash
bash server/scripts/rollout-compose.sh --project YOUR_COMPOSE_PROJECT \
  --cutover-proxy --previous-ref PRE_PROXY_COMMIT \
  --manager-digest sha256:MANAGER_HEX --farm-digest sha256:FARM_HEX \
  --render-digest sha256:GPU_HEX --ack-worker-quiesced --ack-macs-drained --dry-run
```

Remove `--dry-run` to run it. The stages are:

0. Stop farm, Manager and `juicefs` (quiesce).
1. Recreate `redis` with no host port and the ACL from `server/redis.acl`
   (`default off`, `jm-proxy`, loopback-only `jm-admin`). Reset
   `jmlease:v1:cfg` mode to `passthrough` (a mode stored before a rollback
   survives in Redis and would otherwise resume).
2. Start `respproxy`. It must become healthy and publish 30179.
3. Rerun `juicefs-init` through the proxy as `admin` (proves auth works).
4. Recreate `juicefs` (`bg` mount and `webdav`): fresh sessions through the
   proxy.
5. Recreate Manager and farm on the new digests (fresh sessions), then
   verify their identity and the Manager API.

Exactly one container holds 30179 at any time. If any stage fails, the script
removes `respproxy`, recreates every stage from the pre-proxy compose (kept at
`.h8b-rollback/docker-compose.pre-proxy.yml`) with the previous digests, and
restores `.env`. A failed rollback keeps `.update-lock` for the operator.

Then restart every client so each one registers its session through the
proxy:

- Start JuiceMount on every Mac (the app enrolls if it has no credential,
  then waits for approval). While the auto-approve window is **closed**, approve
  each Mac on `http://<nas>:30190/principals.html` after checking that the
  fingerprint the Mac shows matches the list. The Manager refuses Approve,
  and refuses to open the auto-approve window, unless the proxy is live:
  `reload.ack` repeats a whole `reload.request` line this Manager process
  wrote (sha256, time and nonce, so an ack left from before a Manager restart
  never counts), AND `reload.heartbeat`, which the entrypoint rewrites every
  2 s, is less than 6 s old (so a proxy that stopped while the Manager kept
  running stops counting within 6 s). Before the cutover it answers 409 "the
  Redis proxy is not live yet"; a stopped proxy gets "the Redis proxy has
  stopped"; a proxy image older than the heartbeat gets "Update the
  jm-respproxy image". A Mac approved early does not switch anyway: the app logs in as
  its principal first and keeps its current login, showing "Approved —
  waiting for the server cutover", until the server accepts the new one.
- The server mounts (`juicefs`, farm, Manager) were recreated by stages 4 and
  5.
- **Every processing node enrolled before the swap** (for example the GPU
  render node) was created with an unauthenticated `redis://<nas>:30179/1`.
  Restarting it only loops on `JMDENY AUTH_REQUIRED`: remove it and create
  it again from a fresh Manager enrollment, which now names the `farm`
  principal. Per node (`<name>` as enrolled, for example `gpu-rc0.5`):

  1. In the Manager, Farm tab → enroll the node again with the same name,
     role, state and cache paths (or
     `POST /api/farm/enrollment {"name":"<name>","role":"render","state_path":"…","cache_path":"…"}`
     with the admin key). Keep the plan it returns.
  2. On the node:

     ```bash
     ssh <user>@<render-host>
     sudo systemctl disable --now juicefarm-<name>.service
     docker rm -f juicefarm-<name>
     read -rs JM_META_PASSWORD && export JM_META_PASSWORD   # JM_PROXY_FARM_PASSWORD from the NAS .env
     <the plan's prepare command>
     <the plan's docker command>          # passes -e JM_META_PASSWORD by name only
     unset JM_META_PASSWORD
     <the plan's recovery command>        # reinstalls and enables juicefarm-<name>.service
     ```

  3. Confirm in the Manager that the node's heartbeat shows the expected
     commit and that `/metrics` shows no refusal for principal `farm`.

Verify:

```bash
redis-cli -h <nas> -p 30179 ping            # must answer JMDENY AUTH_REQUIRED
docker exec <respproxy> wget -qO- http://127.0.0.1:9180/metrics \
  | grep -E 'decisions_total.*reason="(BG_ONLY|ADMIN_ONLY)"'   # must print nothing
```

A `BG_ONLY` or `ADMIN_ONLY` line for a named principal means that client
still runs a pre-swap session: restart it. The Principals page must say the
proxy is running and answers reloads from this Manager.

**Auto-approve window.** Open it (Principals page, default 20 Macs, at most 2
per source address, 7 days) only when both are true: the deployed proxy
contains the every-mode gate (prerequisite 2), and you are present to review.
Do not open it on a proxy without that gate (the script already refuses to
start a proxy image whose revision does not descend from
`server/respproxy/min-gate-commit`). Everything the window approves is
listed as "needs review" until you confirm or revoke it. Close the window as
soon as the named Macs are in.

The per-address limits count the source address the Manager sees. Remote
Macs behind a Tailscale subnet router (or any NAT) all arrive from the
router's one address, so at most two of them are auto-approved and at most
four can wait pending at once: expect to approve the rest by hand, a few at a
time.

### Step 2: observe

Set the mode to `observe` on the same page (or
`POST /api/leases/mode {"mode":"observe"}` with the admin key). The proxy
reads `jmlease:v1:cfg` within seconds, no restart. It now decides every
lease and logs `WOULD_DENY` without refusing; command and key-class gates
already refuse in every mode. Watch
`jm_respproxy_decisions_total{outcome="would_deny",…}` on `/metrics`.

Exit criteria: 7 days with zero unexplained would-denies, every named Mac
enrolled, no auto-approved Mac left unreviewed, and the window closed.

### Step 3: enforce

Set the mode to `enforce`. The page asks you to type `enforce`; the API needs
`{"mode":"enforce","confirm":"enforce"}`. From then on, a write into a
project another principal holds is refused. Manager migrations and trash
restores take a checkout lease on their target projects first and are
refused up front (HTTP 409 or a failed job naming the holder); if the lease
answer is unknown (proxy unreachable, source unreadable) they fail closed
(HTTP 503 or a failed job). Scheduled gc/compact/fsck run as `bg`, and the
Trash retention write as `admin`.

**Testing a refusal by hand.** A write refused by the proxy reaches the
writer as `EROFS` ("Read-only file system") at `fsync` and `close`. Plain
shell redirection (`echo x >> file`) ignores `close` errors and reports exit
0 even though nothing was written. Test with `touch`, `mkdir`, or
`dd if=/dev/zero of=FILE bs=1k count=1 conv=fsync`, and check the content.

### Rollback, in increasing severity

1. Mode `observe`: immediate, no restart.
2. Mode `passthrough`: immediate, no restart.
3. Proxy out of the path (quiesce as in step 0 first):

   ```bash
   bash server/scripts/rollout-compose.sh --project YOUR_COMPOSE_PROJECT \
     --proxy-rollback --ack-worker-quiesced --ack-macs-drained
   ```

   The script builds the rollback ACL (server principals from `.env`,
   enrolled Macs from the Manager's `rollback-macs.acl`, sha256 only, every
   line validated), writes it into the Redis data directory, removes
   `respproxy`, and recreates Redis on 30179 with
   `server/respproxy/docker-compose.rollback.yml`. Every client, Macs
   included, keeps its credentials and talks to Redis directly. There is no
   lease enforcement in this state. Restart every Mac afterwards.

   Return with `--proxy-restore` (same flags). It refuses a proxy image
   without the every-mode gate, and it always resets the stored mode to
   `passthrough` before the proxy starts, so a mode of `enforce` that was
   active before the rollback cannot resume over sessions the new proxy does
   not own; step through observe and enforce again. After it: restart
   JuiceMount on every Mac (their sessions from the direct period have no
   owner at the proxy), check on the Principals page that each Mac is still
   approved and nothing unexpected was enrolled meanwhile, recreate any
   processing node as after the cutover, and repeat the `/metrics` check. If
   either command fails half way, the script returns to the topology it
   started from.

Never start the rollback override with a plain `docker compose up`.

### Host or script crash during a topology change

A crash (host, SSH session, or the script itself killed) during any stage of
`--cutover-proxy`, `--proxy-rollback` or `--proxy-restore` leaves
`.update-lock` in place and the stack between two topologies. Nothing may be
listening on 30179. Do not retry the script until you have restored one known
topology by hand from the checkout. Common setup:

```bash
P=YOUR_COMPOSE_PROJECT
C="docker compose --project-name $P --project-directory server --env-file server/.env"
UP="up -d --no-deps --no-build --pull never --force-recreate"
```

**During `--cutover-proxy` (any stage)**: return to the pre-proxy stack.
`server/.env` may already name the new digests; the backup is
`.update-lock/env.backup`.

```bash
cp -p .update-lock/env.backup server/.env
$C -f server/docker-compose.yml rm -sf respproxy
$C -f .h8b-rollback/docker-compose.pre-proxy.yml $UP redis
$C -f .h8b-rollback/docker-compose.pre-proxy.yml $UP juicefs-init juicefs juicemount-manager juicefarm
rm -rf .update-lock
```

**During `--proxy-rollback`**: return to the proxy (the state it started
from). `.env` is unchanged by this mode.

```bash
$C -f server/docker-compose.yml stop -t 60 juicefarm juicemount-manager juicefs
$C -f server/docker-compose.yml $UP redis          # no host port; frees 30179 if Redis held it
R="$($C -f server/docker-compose.yml ps -q redis)"
until [ "$(docker inspect -f '{{.State.Health.Status}}' "$R")" = healthy ]; do sleep 2; done
docker exec "$R" sh -ec \
  'REDISCLI_AUTH="$(cat /run/jm-redis/admin.pass)" redis-cli --no-auth-warning --user jm-admin -n 1 HSET jmlease:v1:cfg mode passthrough'
$C -f server/docker-compose.yml $UP respproxy
$C -f server/docker-compose.yml $UP juicefs-init juicefs juicemount-manager juicefarm
rm -rf .update-lock
```

**During `--proxy-restore`**: return to the direct (rolled-back) stack. The
rollback ACL is still in the Redis data directory.

```bash
$C -f server/docker-compose.yml stop -t 60 juicefarm juicemount-manager juicefs
$C -f server/docker-compose.yml rm -sf respproxy
$C -f server/docker-compose.yml -f server/respproxy/docker-compose.rollback.yml $UP redis
$C -f server/docker-compose.yml -f server/respproxy/docker-compose.rollback.yml $UP juicefs-init juicefs juicemount-manager juicefarm
rm -rf .update-lock
```

In every case check who publishes 30179 (`docker port`), that `juicefs-init`
exited 0, and that the Manager answers; then restart every Mac and run the
script again for the transition you wanted. Do not try to finish a transition
by hand.

### Transport security

The proxy speaks plain RESP and enrollment is plain HTTP. Every connection's
`AUTH` crosses the network in cleartext; the enrollment secret crosses once
(polls use the returned token). Keep 30179 and 30190 on a trusted path: the
LAN, or for remote Macs the 0.5 system private route (Tailscale subnet router
or WireGuard). For encryption on the LAN, put a TLS terminator in front of
the proxy (stunnel or a Caddy layer-4 listener forwarding to
`respproxy:6379`) and give Macs a `rediss://` URL. jm-respproxy has no TLS
listener of its own.

Offline tests for these modes:

```bash
bash server/scripts/rollout-compose-proxy.test.sh
```

### Object keys (no delete for clients)

**What this is.** JuiceFS stores the object-store key in the volume's Redis
`setting` key. Every client must read `setting` to mount, and its AES-GCM
wrapping uses a key derived from the volume UUID in the same JSON, so any
client can recover that key. Before this change it is the MinIO root key, and
the MinIO API is published on 30151.

After this change `setting` holds **no** object key. Stock juicefs 1.3.1 then
reads `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` from each client's environment
(it ignores them while `setting` holds a key). Two MinIO users:

| User | Policy file | May | Holder |
|---|---|---|---|
| `jm-client` | `server/minio/jm-client-policy.json` | GetObject (and HEAD), PutObject, ListBucket | Macs (from enrollment), farm, WebDAV, enrolled farm nodes |
| `jm-gc` | `server/minio/jm-gc-policy.json` | the above plus DeleteObject and multipart cleanup | server `bg` mount, Manager (gc, compact, fsck, sync) |

Neither policy has bucket or admin actions. A client delete with `jm-client`
fails with AccessDenied; the slices stay recorded as pending and the next
`bg` cleanup or Manager `gc --delete` (jm-gc) removes them. Objects of a write
the proxy refused (patch 0007) are unreferenced; `gc --delete` removes them
as "leaked" once they are an hour old. Both were measured against a recording
S3 endpoint on 2026-10-06; real MinIO has not been exercised.

New installs: `minio-keys` creates both users and `juicefs-init` formats with
no key in `setting`. Nothing below is needed.

**Existing volume, in this order.** `C` is your compose command, as above.

*Step A: create the users (changes nothing in use).* Add
`JM_OBJ_CLIENT_SECRET_KEY` and `JM_OBJ_GC_SECRET_KEY` (each
`openssl rand -hex 20`) to `server/.env`, then:

```bash
$C -f server/docker-compose.yml up --no-deps minio-keys      # one-shot, must exit 0
docker logs "$($C -f server/docker-compose.yml ps -aq minio-keys)"   # jm-client / jm-gc: policy ..., enabled
```

The script re-runs cleanly at every `up` and fails if a user carries any
policy other than its own. Rollback: none needed; nothing uses the users yet.

*Step B: move every client to its env key, then empty `setting`.*

1. **Prerequisites.** The proxy cutover (Step 1 above) is done, so pre-0.5
   Macs can no longer reach the volume. Every 0.5 Mac has polled its
   approved enrollment and holds the `jm-client` key (response fields below).
   **A client with no key, after step B.5, stalls in IMDS credential retries
   for 5+ minutes per upload instead of failing.** Enrolled farm nodes are
   re-created with `-e MINIO_ACCESS_KEY -e MINIO_SECRET_KEY` (the Manager's
   node plan prints this).
2. **Put the env keys on every server client.** Apply this compose: `juicefs`
   (bg mount: jm-gc; WebDAV: jm-client), `juicemount-manager` (jm-gc for its
   juicefs subprocesses, jm-client for enrollment) and `juicefarm` (jm-client).
3. **Restart them.**
   `$C -f server/docker-compose.yml up -d --no-deps juicefs juicemount-manager juicefarm`
   While `setting` still holds the root key, juicefs ignores the env keys, so
   this changes no behavior.
4. **Verify they mount.** `docker exec <juicefs> mountpoint /jfs`,
   `docker exec <juicefarm> mountpoint /jfs`, the Manager Overview answers.
5. **Only then empty `setting`**, as `admin` through the proxy:

   ```bash
   set -a; . server/.env; set +a
   bash server/minio/object-key-switch.sh clear --all-macs-have-keys
   ```

   The script refuses unless each server container carries its key and is
   mounted. It runs `juicefs config <admin URL> --access-key "" --secret-key ""`
   in the juicefs container (secrets passed by name, never on a command
   line), with jm-gc in the environment for config's storage test, then
   checks `juicefs status` shows no key. Running clients reload the volume
   format and switch to their env key.
6. **Check.** A Mac copy and readback work; the `juicefs` container log shows
   no `AccessDenied` on uploads; the next Manager GC reports deleted slices.

*Rollback of step B:* put a key back into `setting`:

```bash
set -a; . server/.env; set +a
bash server/minio/object-key-switch.sh rollback          # jm-gc key
bash server/minio/object-key-switch.sh rollback --root   # exact pre-switch state (root key)
```

Clients then ignore their env keys again. Two costs: whatever key is in
`setting` is readable by every client again, and `juicefs config` stores the
secret it is given without the format-time encryption. `rollback` hands
the secret to `juicefs config` as `--secret-key`, so it is in that one
process's argv inside the container while it runs (stock juicefs has no other
input).

**Mac enrollment response fields** (`POST /api/principals/enroll`, same
response as the principal status, `Cache-Control: no-store`):

- `object_access_key`: the `jm-client` access key.
- `object_secret_key`: its secret.

Both are present only when `status` is `approved` and the request proved
itself (poll token or enrollment secret); absent while pending or revoked,
and absent when the Manager has no `JM_OBJ_CLIENT_*` configured. The Mac
stores them in its Keychain and passes them to the bundled juicefs and the
in-process SDK as `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY`.

`manager_id` (every enroll and poll answer; also the `X-JM-Manager-ID`
header on every answer of the route, and `GET` returns only it) is a random
ID the Manager mints once. It keeps the ID in two places beside each other:
its own file `principals-store.manager-id` and the principal store
`principals-store.json`. On start a valid ID in the store wins, because the
store's enrollments were made under it. Without one, the ID file is used.
A new ID is minted only when both lack one. The result is written back to
both, so the ID file always matches the store after a start.

The Mac binds its enrollment to the ID:

- A new address that answers with the same ID keeps the Mac's secret. Any
  other answer at a new address starts a fresh request.
- At the same address, a 200 or 202 with the bound ID is accepted. A 200 or
  202 with another ID, or none (a Manager image from before IDs), is
  accepted only when its `fingerprint` is this Mac's fingerprint. Then the
  Mac rebinds to the ID it carries, or keeps the bound ID when it carries
  none. Without a matching fingerprint the Mac changes nothing: it adopts no
  status, no object-store key and no ID, and retries. A "revoked" answer
  (the only one that deletes the Mac's keys) is accepted only with the
  bound ID.
- The fingerprint proves the server knows this Mac's secret. The secret is
  also the Mac's Redis password, and every Redis endpoint the Mac logs in to
  receives it (plain `AUTH`). So the fingerprint stops accidents and
  off-network impostors that never saw the Mac's login. It does not stop a
  server the Mac authenticates to.
- At the same address, a 403 or 409 with a different ID starts a fresh
  request. One without an ID changes nothing (the Mac reports a failure and
  retries). A 403 makes the Mac re-send its secret only to the bound ID.

The ID is not a secret. It prevents accidents (a respelled address, a
reused address now leading to another NAS), not a deliberate impersonator;
only TLS with a pinned certificate does that.

Restoring the principal store alone, even from a backup taken before IDs
existed, keeps the ID (the ID file still has it). Copying a store onto a
Manager that has already started (a migration to a new NAS) also keeps the
store's ID: it replaces the new Manager's own ID, so Macs pointed at the new
address keep their requests.

When both copies of the ID are lost (the whole state directory restored from
before IDs), a new ID is minted:

- Macs that poll the same address and that the restored store still holds
  rebind to it at their next poll and stay approved.
- A Mac whose Manager address also changes asks the new address for its ID,
  sees a different one, and requests access afresh. The restored store
  still holds its old request, so the Mac reports a conflict. An admin must
  revoke and forget that principal, then approve the new request.

Losing the principal store makes every Mac request access again.

### Per-Mac object keys (not yet enabled)

> **Not yet enabled.** The server side exists (`minio-keyd`, the
> `JM_OBJ_PER_MAC_SERVER` switch), but the Manager and the Mac app do not
> use it yet. Leave `JM_OBJ_PER_MAC_SERVER` and `JM_OBJ_PER_MAC` unset.
> While they are unset, `minio-keys` behaves exactly as before and
> `minio-keyd` is never started.

There are two switches, one per step, so that enabling the server side
never also switches the Manager on at its next redeploy:

| Switch | Read by | Step |
|---|---|---|
| `JM_OBJ_PER_MAC_SERVER` | `minio-keys`, `minio-keyd` | M1: create `jm-macs` and `jm-keyadmin`, run keyd. No Mac is affected. |
| `JM_OBJ_PER_MAC` | the Manager | M2: mint and deliver per-Mac keys. Not built yet. |

Each takes exactly `1` or `0` (unset means `0`). Any other value (`true`,
`yes`, ` 1`) is logged as an error and treated as off.

**What this is.** Today every approved Mac gets the same `jm-client` key.
Revoking a Mac in the Manager removes its Redis principal, but the Mac keeps
a working object key: it can still read and write chunks on MinIO
(port 30151) directly. With per-Mac keys, each approved Mac gets its own
MinIO service account, and revoking the Mac deletes it.

| Name | Kind | Policy | Holder |
|---|---|---|---|
| `jm-macs` | user | `jm-client` (the same canned policy) | nobody: its random secret is made by `minio-keys` and never stored. It is only the parent of the Mac keys. |
| `mac-<12 hex>-<gen>` | service account of `jm-macs` | inherited from `jm-macs` | one Mac. `<12 hex>` is its Manager principal name; `<gen>` (1-999) increases on every re-approval, so a revoked name is never reused. |
| `jm-keyadmin` | user | `server/minio/jm-keyadmin-policy.json`: create, remove and list service accounts; no S3 action, no update | `minio-keyd` only. Never the Manager. |

**What the `jm-keyadmin` credential does not enforce.** "keyd mints keys
only under `jm-macs`" and "a keyd key cannot delete objects" are enforced
by `keyd.sh` (the hard-coded parent and the name check), not by the
credential. MinIO's `AddServiceAccount` accepts any non-root target user
(design check A7, read in older MinIO source), so a stolen `jm-keyadmin`
secret could mint a key under `jm-gc`, which can delete objects. The policy
has no `UpdateServiceAccount`: MinIO checks only the action for an update,
so it would let the holder reset any account's secret. keyd therefore
removes a key with `svcacct rm` alone, with no `disable` first. Treat the
`jm-keyadmin` secret like the `jm-gc` secret.

`minio-keyd` (`server/minio/keyd.sh`) runs the same pinned MinIO image and
talks to the Manager only through files in `/mnt/zSSD/juicemount/objkeys`
(mounted in `minio-keyd` and the Manager, nowhere else):

- `desired.json` (Manager): the names that must exist. No secrets.
- `results/<name>.json` (keyd): the secret MinIO generated for a new key.
  The Manager stores it, clears `needs_secret` for the key in
  `desired.json`, and only then deletes the file.
- `applied.json` (keyd): what MinIO reports for each key (`exists`,
  `status`, `parent`, `implied_policy`), read back with `svcacct info`.

`server/minio/KEYD_PROTOCOL.md` is the full contract the Manager must
follow.

keyd refuses, and changes nothing for, a `desired.json` with a name that is
not `mac-<12 lowercase hex>-<1-999>`, a parent other than `jm-macs`, or a
`seq` lower than the last applied one, and any `desired.json` that is not
strict JSON (a syntax error, a member twice in one object, an escape
sequence in a field keyd reads, a case variant such as `"Seq"`). Extra text
fields are allowed, and `"keys":null` means no keys. It refuses to remove
more than 3 keys, or more than one key and more than 25% of the keys, in
one step unless `desired.json` carries `"allow_mass_delete"` equal to its
`seq`. A missing `desired.json` removes nothing. A key marked
`"needs_secret":true` is re-created at most once per `seq` (keyd records
the `seq` of every key it creates in `objkeys/keyd-created-seq`), so the
Manager raises `seq` when it asks again. If the Manager takes a result while
`needs_secret` is still set, keyd ignores the flag for that key until a
`desired.json` without it arrives, and says so in `applied.json`.

A stop (`docker stop`) during a create waits for MinIO's reply so the new
secret reaches `results/`, for 50 seconds at most. Then keyd gives up, logs
`GAVE UP`, removes the key if MinIO made it (5 seconds at most) and exits.
`minio-keyd` has `stop_grace_period: 60s`, so docker waits for that. If a
key survives a give-up, it is in `applied.json` with no secret delivered;
the Manager asks again with `needs_secret` and a higher `seq`. keyd refuses
to start with the MinIO root password, `jm-gc` or `jm-client` in its
environment.

**Prerequisites (all of them, in this order).**

1. H-8B step B above is done: `setting` holds no object key. Before step B
   juicefs ignores every environment key, so per-Mac keys do nothing.
2. **M0: rotate the MinIO root password.** Before step B, `setting` held the
   MinIO root key, and every Mac that mounted then could recover it. A
   per-Mac key revokes nothing for a Mac that holds the root key. The root
   password is used only by `minio`, `minio-keys` and
   `object-key-switch.sh rollback --root`.

   ```bash
   # Put the new value in server/.env (openssl rand -hex 20), then:
   R="$($C -f server/docker-compose.yml ps -q minio)"
   read -rs OLD && export OLD          # type the previous root password
   docker exec -e OLD "$R" sh -c \
     'MC_HOST_old="http://juicemount:$OLD@127.0.0.1:9000" mc admin info old'
   ```

   The password reaches the container by name, never on a command line. If
   it contains characters other than letters and digits, percent-encode
   them in `OLD`. That command shows the old password still working. Apply the change with
   `$C -f server/docker-compose.yml up -d --no-deps minio`, then
   `$C -f server/docker-compose.yml up --no-deps minio-keys` (must exit 0).
   **Check:** the same `docker exec` now fails with an access error, and a
   Mac copy and readback still work (Macs do not use the root key after
   step B). **Rollback:** put the old value back in `server/.env` and
   recreate `minio`.
   Restarting `minio` interrupts every client's object I/O for the restart;
   do it in a maintenance window.
3. **M1: create `jm-macs` and `jm-keyadmin`, start keyd.** Add to
   `server/.env`:

   ```bash
   JM_OBJ_PER_MAC_SERVER=1
   JM_OBJ_KEYADMIN_SECRET_KEY=   # openssl rand -hex 20
   ```

   ```bash
   $C -f server/docker-compose.yml up --no-deps minio-keys    # one-shot, must exit 0
   $C -f server/docker-compose.yml up -d --no-deps minio-keyd
   ```

   `minio-keys` exits 0 even when the per-Mac part fails, so that a full
   `up` still mounts; the exit code does not prove M1. **Check:** the
   `minio-keys` log shows `jm-macs: policy jm-client, enabled` and
   `jm-keyadmin: policy jm-keyadmin, enabled`, and has no line
   `ERROR: per-Mac object keys NOT provisioned`; the keyd log shows
   `managing service accounts under jm-macs`; `objkeys/applied.json` exists
   with `"keys":[]`. Nothing uses the new users yet. Do not set
   `JM_OBJ_PER_MAC` here. **Rollback:**
   `$C -f server/docker-compose.yml stop minio-keyd` and unset
   `JM_OBJ_PER_MAC_SERVER`.

Steps M2-M5 (M2: `JM_OBJ_PER_MAC=1` for the Manager, which mints keys for
approved Macs; then Macs adopting them, and finally rotating the
`jm-client` secret) need the Manager and Mac changes that are not built
yet. Until then, stop after M1 and leave `JM_OBJ_PER_MAC` unset.

Offline tests for the scripts (fake `mc`, no MinIO needed):

```bash
go test ./server/minio/
```

The `mc` flags and JSON field names keyd relies on (`svcacct add
--access-key` with no `--secret-key`, `svcacct ls/info/rm`,
`parentUser`, `accountStatus`, `impliedPolicy`, `secretKey`), whether
the scoped `jm-keyadmin` policy may manage `jm-macs`'s service accounts
without `UpdateServiceAccount` (including `svcacct info`), and whether it
can also target `jm-gc` (A7) in the pinned MinIO release, have not been
run against a real MinIO.

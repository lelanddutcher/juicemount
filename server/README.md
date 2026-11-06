# JuiceMount server-side stack

The 0.5 server stack needs release image digests, persistent credentials,
storage paths, and a rendered Compose file before it can start. Use the
[TrueNAS install guide](INSTALL-TrueNAS.md) or follow the same render process
on a Docker host. Obtain the release image digests from its published manifest;
do not replace them with mutable tags.

## Production install (TrueNAS Scale)

Use **TrueNAS Apps → Discover → ⋮ → Install via YAML** with the rendered,
digest-pinned release compose. The setup and upgrade steps are in
[INSTALL-TrueNAS.md](INSTALL-TrueNAS.md).

The stack:

| Service               | What it does                                  | Port  |
|-----------------------|-----------------------------------------------|-------|
| `redis`               | JuiceFS metadata authority (AOF persistence), internal only | — |
| `respproxy`           | Authenticated Redis gateway for named principals | 30179 |
| `minio`               | S3-compatible object store for JuiceFS chunks | 30151 (S3), 30152 (console) |
| `minio-keys`          | Provisions separate client and GC object keys | — |
| `juicefs-init`        | One-shot pre-flight + first-time volume format | —    |
| `juicefs`             | Live FUSE mount + loopback-only WebDAV | 127.0.0.1:30180 |
| `juicemount-manager`  | Control-plane web UI (migrations, trash, backups, maintenance, settings) | 30190 |
| `juicefarm`           | NAS CPU derivative worker | — |

The Mac JuiceMount.app uses the server through:
- an enrolled Mac principal at the RESP proxy on port 30179;
- an S3 endpoint at port 30151 with a provisioned client object key;
- the Manager on port 30190 for enrollment and control.

The Manager delivers the Mac's credentials during enrollment. Do not put a
password in a volume URL or assume the old unauthenticated Redis URL works.
Keep these endpoints on a trusted LAN or private route.

## Plain Docker host (Ubuntu / Synology / laptop)

Use the canonical `docker-compose.yml`, adjusting its TrueNAS-flavored bind
paths for your host. Copy `release.env.example` outside the repository, fill
every required credential and digest from the accepted release manifest, and
keep that file private. Render and inspect it before starting containers:

```sh
docker compose --env-file /secure/path/juicemount-release.env \
  -f server/docker-compose.yml config > /secure/path/juicemount-rendered.yml
docker compose --env-file /secure/path/juicemount-release.env \
  -f server/docker-compose.yml up -d
docker compose --env-file /secure/path/juicemount-release.env \
  -f server/docker-compose.yml ps
docker compose --env-file /secure/path/juicemount-release.env \
  -f server/docker-compose.yml logs juicefs-init
```

## Migrating existing data

The included `juicemount-manager` service exposes a web UI at port
30190 with a Migrations tab that copies data from any bind-mounted
source directory into the JuiceFS volume. (SLICE 0 renamed the
service from `juicemount-migrator` — the legacy image tag still
publishes for one release as a compat alias and `/migrator/*` URLs
301-redirect to `/manager/*`.)

1. Bind-mount each existing dataset read-only at `/sources/<name>`
   in the `juicemount-manager` service.
2. Open `http://<host>:30190/` in a browser (Migrations tab is the
   default route).
3. Browse the source root, pick a folder, hit Start.

Features:

- **Live progress bar** with real percentage (driven by juicefs's
  Prometheus metrics, scraped every 2s).
- **Smart default destination** that strips the source-root bind-mount
  name so `/jfs/oldzpool/...` doesn't clutter the structure.
- **Destination preview** showing the resolved URLs + 3 example file
  destinations before you commit.
- **Junk-filter** for `.DS_Store`, `._*`, `Thumbs.db`, `.sync.ffs_db`,
  Spotlight/Trash metadata.
- **Permissions OFF by default** — destination files land with sensible
  modes the Mac user can open. Tick the option only if you need
  archival fidelity (matters for POSIX-to-POSIX with uid-preserving
  intent).
- Jobs queue and run sequentially.

## Smart folders (experimental, off by default)

Smart folders make the Manager keep a top-level `Recents` folder in the
volume: `Recents/<top-level folder>/` holds relative symlinks to the 100 most
recently changed files under that folder (entries older than 30 days drop
out). Every client sees it in Finder and in file pickers with no extra app.

**Turning it on (default deployment):** set `JM_SMART_FOLDERS=on` in
`server/.env` and redeploy the Manager. Nothing else is needed: in the default
standalone mode the Manager writes the views through a JuiceFS metadata
client logged in as its own proxy principal (`JM_META`, `manager@respproxy`).
It opens no JuiceFS session (a session's background check exits the process
if the volume format changes), and no FUSE mount or object-store key is
used. Smart folders run only in this standalone mode: a Manager in embedded
mode (`--fuse-mount`) logs that and does not start them, because a directory
created through a mount cannot be identified atomically.

| Variable | Default | Meaning |
|---|---|---|
| `JM_SMART_FOLDERS` | `off` | `on` enables smart folders. |
| `JM_SMART_FOLDERS_ROOT` | `Recents` | Name of the top-level folder. |
| `JM_SMART_FOLDERS_MAX_VIEWS` | `256` | Most views (top-level folders) kept, 1 to 4096. |
| `JM_SMART_FOLDERS_COLLECTIONS_ROOT` | `Collections` | Root of the collection views (below; they also need `JM_CONTINUITY=on`). |
| `JM_SMART_FOLDERS_QUERIES_ROOT` | `Queries` | Root of the saved-query views (below; they also need `JM_CONTINUITY=on`). |

How it behaves:

- **Ownership is recorded, never guessed.** Everything the Manager creates
  (the root, each view, each link) is recorded in `smart-folders.json` next to
  `JM_STATE_FILE` *before* it is created: links under a random hidden
  temporary name that is then renamed into place without replacing
  anything, directories with mode `0705`, which is how an interrupted
  creation is recognised later. A link is replaced only by an atomic
  exchange after re-checking it is still the Manager's, and swapped back if
  anything else got there first. It never modifies anything else: an
  existing `Recents` it did not create (even an empty one) is left alone and
  looked at again every 10 minutes, and a file someone drops into a view is
  never touched. It never removes a `Recents` directory: a view whose files
  all age out stays, empty. Without a state file the feature stays off.
- **Only real folders get views.** A top-level folder must exist on the
  volume before it is tracked, so change events naming made-up folders cost
  nothing. The state file is bounded (at most `MAX_VIEWS` x 100 entries).
- **Names stay put.** A file keeps its link name while it stays in the list;
  a newer file with the same name gets the suffix (`clip (B-Roll).mov`).
- **Each view update checks out the view's project lease**
  (`/Recents/<folder>`) as trash restore does, and only when the view needs
  to change. A refused checkout or failed write backs off (30 s doubling to
  30 min) and is logged once.

**Collection views (experimental, also need `JM_CONTINUITY=on`).** With
both switches on, the Manager also keeps `Collections/<collection name>/`: one
relative symlink per member file that is present, pointing at the file's
current path (found from its inode, so it follows renames and moves). The same
ownership rules apply, with records of their own in `smart-folders.json`
(version 3 once they exist; an older Manager refuses that file and leaves it
alone). A user-made `Collections` is left alone and only stops these views (for
`Recents` it is an ordinary folder).
Names: `/`, `:` and control characters become `-`, leading dots are dropped;
when two collections clash (ignoring case), the collection that already holds
the name keeps it and the other gets `(2)`; a name a user's folder already
takes is skipped; a rename that only changes case keeps the folder. Member
files that clash are labelled with their folder, as in `Recents`. A member
whose file is gone gets no link (it stays a member). A deleted or renamed
collection's view is emptied and then removed, unless a user put something in
it (left alone; a lone Finder `.DS_Store` does not count and is removed with
it) or the volume refuses. A view folder is a project root; the RESP proxy
lets the Manager remove one only directly (no rename, no trash) and only when
it saw the Manager create it in a smart root. A view it refuses (an older
proxy, or a view made before it kept that record) stays empty and is tried
again only every 6 hours or after a restart (at most 256 views are recorded,
left-behind ones included; beyond that none is made). A
member whose path cannot be read keeps its link. At most 128 views of 500
links; the rest, missing and unresolved members and views left behind are
counted in `GET /api/continuity/status` (`collection_views`). Views follow
collection changes and member-file events within seconds, and are all rebuilt
every 10 minutes, paced so `Recents` is not held up.

**Saved-query views (experimental, also need `JM_CONTINUITY=on`).** A saved
query (`/api/queries`, created by the admin or by a person whose project
grants cover all its folders) names one or more folders and optionally a name
fragment, file extensions and a modified-within window; it lists up to its
limit (default 200, at most 500) matching files, newest first. With both
switches on, the Manager keeps `Queries/<query name>/` with one relative
symlink per result, under the same rules as collection views (records in
`smart-folders.json`, version 4 once they exist; names, removal, the 6-hour
retry and the limits are the same). Candidates come from the continuity
catalog's path hints and every result is checked on the volume by its inode,
so a link always points at the file's current path; a folder renamed into a
query's folder is seen only once its files' next change events arrive. Each
query examines at most its 2,000 most recently changed candidates (bound,
moved or saved in place, the last noted at most every 10 minutes per file);
a capped query is named in `GET /api/continuity/status` (`query_views`) and
its results say `capped`, with how many were examined. Views follow query
changes within seconds and file changes under a query's folders at most once
a minute, and are all rebuilt every 10 minutes (files leave a
modified-within window without any event). A library holds at most 256
queries, a person at most 32. The continuity catalog stays at format 3 (still
readable by a Manager from before saved queries) until the first query is
created.

**Agent discovery (experimental, needs `JM_CONTINUITY=on`; smart folders
not required).** A person can let an AI agent (the JuiceMount MCP server on
their Mac) find files and edit collections as them. They mint an agent token
from one of their own approved Macs (`POST /api/agent-tokens` with the Mac's
collection credentials, `{name, expires_in_days}`; at most 90 days, the
default, and 10 live tokens a person). The token (`jma_...`) is shown once;
the principal store keeps only its hash. It is sent as
`Authorization: Bearer jma_...` and works only on an allowlist: listing and
reading collections and their members, creating and renaming collections and
adding or removing members (within the person's grants, as for their Mac),
listing saved queries and their results, and the two discovery routes below.
Every other route answers 403, including deleting a collection, query
changes, people, enrollment and anything that moves or returns file bytes.
A token stops working when it expires or is revoked, its person is removed,
or the Mac that minted it is revoked, flagged as cloned, unassigned or given
to someone else (it does not come back if that is undone): from then on
every new request is refused, and an edit already under way is checked again
just before it is written and refused; a read already running finishes. An
agent sees collection members and saved-query results only inside its
person's grants; the rest are counted in `withheld`, without names (Macs and
the admin see them whole). A refused use of a revoked or expired token is
recorded in the audit log (once a minute per token). Note: the Manager
treats any request carrying `Authorization: Bearer` as an agent, so a
reverse proxy in front of it must not forward its own bearer credentials
to the Manager (they would be refused, 401 or 403). The
person lists and revokes their tokens from a Mac (`GET /api/agent-tokens`,
`POST /api/agent-tokens/{id}/revoke`); the admin lists and revokes any (the
same routes with the admin key, and the People page). Mints, revokes and
every agent edit are recorded in the catalog audit log (`catalog-audit/`
beside the principal store; `GET /api/audit?log=catalog`) with the person,
the minting Mac and the token; a mint or an agent edit is refused if the log
cannot record it first.

`GET /api/continuity/find` pages through the catalogued files (admin key, a
Mac's credentials or an agent token) with filters `name` (a fragment,
ignoring case), `folder` (repeatable), `collection`, `modified_after` (RFC
3339 or unix ms), `seen_after` (a catalog sequence), `ext` and `kind`
(`video`, `audio`, `image`, `project`, `document`), `limit` (default 50, at
most 200) and `cursor` (the previous answer's `next`). Each result has the
asset ID, current name, first label, the catalog's path hint, the current
path under the mount root, size, modification time and its collections. A
person (or their agent) sees only files their grants cover (an agent's
request reads at most 20,000 hints and resolves 250 files within 10 s, so
agents, which share one request slot, take turns); for them
`path_hint` is shown only inside their grants and `label` only when it is
the file's current name. A request reads
at most 100,000 catalog hints and resolves at most 1,000 files on the
volume; a page cut short says `budget_exhausted` and its `next` (always
past the cursor given) continues it; with no `next` the scan is complete. A
request that could not examine a single file in time answers 503 with
`Retry-After`; retry it with the same cursor. `GET /api/continuity/asset/{asset_id}` returns one file (404 when it is
gone or outside the caller's grants). The Mac side, `juicemount-mcp` (minting a
token and adding the server to Claude Desktop or Claude Code), is described in
`docs/agent-mcp.md`.

Known gaps:

- Recency comes from the Macs' change events on the `juicemount:metadata`
  channel. Changes made while the Manager is down are missed. Writes that
  publish no event are invisible: Manager migrations, the render farm, the
  WebDAV/S3 gateway, and a Mac renaming a file it had not yet listed.
- Editing apps may remember the symlink path. When a file ages out of
  `Recents` the link goes away and media opened through it goes offline;
  relink to the real path.
- Name clashes are compared with simple lower-casing (no full Unicode case
  folding or normalisation), which only affects which file gets a suffix.
- The standalone metadata client opens no JuiceFS session, so JuiceFS
  directory quotas are not charged for the views' directories and links (a
  few bytes each). Usage counters are still flushed after each update.
- The final unlink of a link the Manager moved aside happens by name, under
  a random hidden temporary name only the Manager knows; only a client that
  learned that name could race it.
- An entry that cannot be moved back from a temporary name (its own name
  was taken again in the meantime) stays under that hidden name, is never
  deleted, and is named in the Manager log until it can go back.

## Persistence & upgrade

Data lives in the host paths set in `volumes:`, but a full-stack
`docker compose up -d` can restart Redis, MinIO, and JuiceFS and interrupt
live Mac clients. For a canonical Compose project owned by this checkout,
use the service-scoped [Compose rollout procedure](COMPOSE-ROLLOUT.md).
It verifies the previous configuration and digest-pinned images, updates only
Manager and farm, and restores previous images on failed verification.

TrueNAS Apps-generated Compose projects are **not** covered by that script.
Do not run the standalone `update-server.sh` against Compose-owned services
or use a full-stack reapply as a substitute for a rehearsed TrueNAS rollout.

## Security notes

- **The RESP proxy, not Redis, owns port 30179.** It authenticates named
  principals; keep the port on a trusted LAN or private route because RESP
  authentication is plaintext without a separate TLS layer.
- **MinIO console (30152) is HTTP-only.** Don't expose to public IPs.
- **`MINIO_ROOT_PASSWORD` is the master key.** Generate via
  `openssl rand -base64 24`. Anyone with this password can read every
  byte in the volume.
- **Manager admin key (`JM_ADMIN_KEY`)** gates write access to the
  manager's HTTP API. The production compose requires a 32+ character value
  in the deployment environment and refuses to render without it. Generate
  one via `openssl rand -hex 32`; preserve the same value across upgrades and
  never commit it to the repository. The binary still permits an empty key for
  explicit local-development runs while JuiceMount Link is disabled.
- **The default 0.5 compose leaves embedded Link off.** Keep Redis, MinIO,
  WebDAV, and Manager on a trusted LAN or private route. The full Link route,
  reconnect, and revoke workflow has not passed acceptance.

## Troubleshooting

| Symptom                               | Likely cause / fix |
|---------------------------------------|--------------------|
| `juicefs-init` exited 2               | MinIO unreachable from inside the container network |
| `juicefs-init` exited 4               | MinIO credentials empty, whitespace, or placeholder |
| `juicefs-init` exited 5               | `JM_BUCKET_URL` missing `http://` |
| `juicefs-init` exited 6               | `juicefs format` errored — read the log for the JuiceFS-side reason |
| Manager refuses to start with an admin-key error | Restore the prior durable `JM_ADMIN_KEY`; the persisted value is absent, short, or still a placeholder. |
| Manager copy reports "0 files / 0 B" | Check the job and accepted image digest; do not switch to a mutable `latest` tag. |
| Mac can't open copied files           | Source had restrictive perms; either un-tick Preserve permissions before migrating, or `chmod -R u+rwX,g+rwX,o+rX` on the destination |

Use `docker compose logs <service>` for the full output of any
container's startup. Each pre-flight log line starts with
`[precheck-N]` for easy grepping.

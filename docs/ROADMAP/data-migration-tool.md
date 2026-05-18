# Data migration tool — server-side helper

**Status:** future work (not started)
**Surface:** server-side Docker compose stack (TrueNAS / self-host)
**Filed:** 2026-05-17 from user request after the QA-18/19 fix loop

## What it should do

A tool that lives in the JuiceFS-server Docker compose stack and helps
move data INTO (and back OUT OF) the JuiceFS volume. Use cases the
user actually hits:

1. **Initial migration** — first-time import of an existing media
   library (SMB share, NFS export, raw ZFS dataset, the `/old-data:ro`
   mount) into JuiceFS.
2. **Recovery** — re-importing files when MinIO state is lost
   (exactly the QA-20 scenario: bucket got wiped, Redis kept metadata,
   nothing actually in object storage). The tool reads from a known-good
   source path and re-populates JuiceFS.
3. **Bulk move-in** — periodic dumps from a camera/recorder/render-farm
   that need to land in JuiceFS without fragile rsync over SMB.
4. **Verification** — confirm that what's in MinIO matches what's in
   the source (catches the "writes succeeded locally but never pushed
   to MinIO" failure mode the user already hit).

## Form factor

The user explicitly said "web-based, and/or maybe not even web-based,
but just some sort of data migration helper would be really helpful on
the server side."

Realistic shapes, ranked by minimum-viable-effort:

### Option A — CLI sidecar service (lowest effort, ~1 day)

Add a `migrator` service to the compose using `rclone` or `mc mirror`
plus a small wrapper script. Exec into the container to run jobs:

```bash
sudo docker exec -it juicefs-migrator migrate \
    --src /source/library \
    --dst /jfs/data \
    --resume
```

- Pros: trivially scriptable, hooks into existing compose
- Cons: no UI; ops only

### Option B — Web UI on a port (~3-5 days)

Tiny Go or Python server that:
- Browses both `/source` (any read-only mount) and `/jfs` (juicefs)
- Lets the user pick a source dir, see a preview of file count + bytes
- Kicks off a copy with progress (server-sent events or polling)
- Shows job history + per-job log
- Has a "verify" mode that compares src+dst by md5 (or size+mtime for speed)

Routes:
- `GET /` → static SPA
- `GET /api/sources` → list configured read-only mounts
- `POST /api/jobs` → start a job
- `GET /api/jobs/:id/events` → SSE stream
- `GET /api/jobs/:id` → job status

Stack: Go + embedded HTML (no node toolchain).
Container: small Alpine + the juicefs binary mounted via volume so
it can directly hit Redis for metadata (cheaper than going through FUSE).

### Option C — Built-in to JuiceMount.app (Mac-side, rejected)

Don't. JuiceMount is the Mac client; this is server-side data
operations. Wrong layer.

## Requirements / constraints

- **Don't trust `cp -R`** — must handle Finder-style `._sidecar` files,
  xattrs, mtime preservation, and resume-after-failure.
- **Bandwidth-aware** — should show MB/s, ETA, and let the user throttle
  if the box is also serving live edits.
- **Verification mode required** — without it, this tool can silently
  corrupt data in exactly the way the original QA-14/19 chain did.
  Optional md5 verify after copy, with the same harness the
  `scripts/qa-suite/01-smoke.sh` write-integrity test uses.
- **Resume support** — must persist progress so a 10-hour migration
  doesn't restart from zero on a power blip. State file in the
  juicefs-cache volume.
- **Idempotent** — running the same job twice on a partially-migrated
  source should skip already-copied files (by size+mtime+optional md5).
- **Read-only source by default** — `:ro` mount declarations in the
  compose. Tool must NEVER modify the source side.
- **Per-job log retention** — keep last N jobs' logs visible in the
  UI for after-the-fact debugging.

## Failure modes to guard against (lessons from this session)

These are concrete things that have actually broken in this project's
history and the migrator must handle:

1. **MinIO bucket missing** — verify the bucket exists before starting
   a job. If the new `bucket-init` sidecar somehow didn't run, refuse
   to start the migration with a clear error.
2. **Mac juicefs uses writeback** — but the migrator's juicefs mount
   should NOT use writeback. Synchronous writes to MinIO mean a crash
   mid-migration loses nothing past the last completed file.
3. **Stale NFS handles** (the QA-19 class) — the migrator should not
   reuse fd-pool entries across files. Open-write-close per file.
4. **Network shaping** — if the migration is from a remote source
   (mounted SMB share, remote NFS), bandwidth contention with live
   readers is real. Add a `--max-bandwidth` flag.
5. **Auto-offline interaction** — if the migrator's juicefs daemon
   thinks MinIO is unreachable, it would fail the migration with no
   clear cause. Pre-flight check the bucket connectivity AND surface
   any auto-offline state visibly in the UI.

## Why this matters

The QA-20 incident (MinIO empty, Redis full of metadata for chunks
that don't exist) would have been a multi-hour data-loss recovery
without the local cache happening to hold the working set. A
migration tool turns "we lost a terabyte of media" into "kick off a
re-import from the SMB share, walk away."

Even outside disaster recovery, the routine workflow of "new project
arrived on the camera cards, get it onto the editing volume" needs
something better than `rsync` over SMB or dragging in Finder.

## Decision needed before starting

1. Web UI vs CLI-only — affects effort by 5x. The user phrased it as
   "web-based and/or maybe not even" — so the appetite is "easy"
   regardless of which.
2. rclone vs mc vs handwritten copy loop — rclone has the best resume
   support and bandwidth control out of the box but adds a heavy
   dependency.
3. Run as a separate compose service or as part of an existing
   admin-UI service (none exists yet).

# Tier 7 — Collaboration (optional)

Goal: when two editors are on the same JuiceMount backend, the second
editor's existence is visible and useful — not just a data race. Live
presence, soft-locking for write conflicts, activity feed, per-folder
ACLs.

Tier 7 is **optional**. Like tier 6, the order of business is: a
solid single-user file system first, multi-user enhancements second.

## Acceptance tests

| # | Test | Pass criterion |
|---|---|---|
| 7.1 | Live presence | Editor A opens a file; Editor B sees an "in use by A" indicator within 2 s |
| 7.2 | Avid bin lock awareness | Premiere/Avid's native lock files are honored; second editor can't open the same bin until the first releases |
| 7.3 | Activity feed | "Jane updated `Scene 12.prproj`" appears in the popover within 5 s |
| 7.4 | Per-folder ACLs | Setting a folder to "review only" for a user causes their writes to fail with `EACCES`; reads still work |
| 7.5 | Conflict-free write queue (advanced) | Editor B writes while offline → on reconnect, the write replays atomically if no conflict; surfaces a clear merge UI on conflict |

## Architecture

```
Redis pub/sub channels:

  presence:<file-handle>      members = set of (editor, started_at)
  activity                    rolling log of (editor, op, path, ts)
  locks:<path>                exclusive holder (editor, expires_at)

NFS handler hooks:

  on Open:    SADD presence:<handle> me; PUBLISH activity
  on Close:   SREM presence:<handle> me
  on Write:   PUBLISH activity (rate-limited per file)
  on Open (write): SET locks:<path> NX EX 300; refuse if held

The handler also LISTENs to the channels and exposes its
view to the menu-bar UI.
```

## Feature backlog

### 7.A — Live presence per file

Open ↔ close events flow through Redis. The handler's
`cachedFile.Close()` SREM's the editor from the per-file presence
set; `cachedFile.OpenFile` SADDs.

Menu-bar UI subscribes to presence changes for files in the active
project. Avatar overlays on file rows in the popover; details ("in
use by Jane for 12 min") on hover.

Edge cases:

- App crashes without Close → expires from the set after a TTL
  (initially 5 min, refreshed by liveness heartbeat).
- Network drops mid-session → expires same way.
- Editor identity → derived from the local user's shared key (no
  account system, per VISION's non-negotiables). Show as `mac:host
  name` if no friendly name configured.

### 7.B — Soft-lock for write conflicts

Premiere creates `<project>.lock` files. Avid uses `.bin.lock`. FCP
has its own. The handler doesn't need to understand each format — it
just needs to NOT let two editors open them in write mode at the
same time.

On `OpenFile(... O_RDWR | O_WRONLY)`:

- Check `locks:<canonical path>` in Redis.
- If unset, `SET NX EX 300` and proceed.
- If set by another editor, refuse with `EAGAIN`. NLE shows "file
  in use."
- If set by us already, refresh TTL and proceed.

This is **soft** — a force-stop can leave a stale lock that the next
editor must override (UI button: "Override stale lock"). Per
VISION's non-negotiables, no real DRM, no enforcement beyond
politeness.

### 7.C — Activity feed

Appended to a Redis stream `activity` per project. Each entry:

```json
{
  "ts": "2026-05-16T15:30:00Z",
  "editor": "leland",
  "op": "write",
  "path": "Brand Spot Vol 3/Scene 12.prproj",
  "size_delta": 4096
}
```

Menu-bar popover shows the last 20 entries for the active project.
Filter / search via the in-app search window. Stream length capped
at 10K entries per project (rolling).

### 7.D — Per-folder ACLs

Stored in `metadata.Store`:

```sql
CREATE TABLE acl (
    path TEXT PRIMARY KEY,
    editor TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('read', 'write', 'admin')),
    granted_at INTEGER NOT NULL
);
```

NFS handler checks on every Lookup / Read / Write / Create / Remove:

- Walk up the path looking for the most-specific ACL entry for the
  current editor.
- If found and mode is insufficient for the op, refuse with EACCES.

Out of scope: organizational hierarchies, roles, groups. This is
"is editor X allowed to write into Y?" — flat. Anything richer is
either user-managed in MinIO bucket policy or a different product.

### 7.E — Conflict-free write queue (advanced, optional)

Builds on tier 4's offline mode. When the user writes while offline:

1. Bytes go to a local journal: `~/Library/Application Support/
   JuiceMount/queue/<random>/<original-path>`.
2. On reconnect, replay each queued write to the backend.
3. If the file changed on the backend since the local write started,
   pause replay and surface a merge UI:
   - "leland wrote to `Scene 12.prproj` at 14:32 while offline."
   - "jane wrote to the same file at 14:51."
   - Buttons: Keep mine / Keep theirs / Open both for diff.

This is genuinely hard. Real merge tools for binary project files
don't exist. Likely we expose this as "your offline write was
deferred — last-writer-wins, here's a backup of your version" rather
than auto-merge.

## Anti-patterns

- **No user account system.** The shared key is the credential. If
  a team needs richer identity, they can layer their own SSO in
  front via Caddy + a reverse-proxy auth header.
- **No hard locking.** NLEs that don't honor `EAGAIN` will overwrite
  each other regardless of what we do. Polite signaling is the goal.
- **No real-time collaborative editing.** That's a different product
  (Frame.io, Figma, Final Draft Collab). JuiceMount is the storage
  layer underneath.
- **No conflict-free CRDT machinery for binary files.** It doesn't
  work for video projects. Last-writer-wins with explicit user
  intervention is correct.

## Dependencies

- Tier 1's stability is hard prerequisite. Adding presence + locking
  to an unreliable mount makes both worse.
- Tier 3's docker-compose stack must provide a Redis pub/sub channel
  separate from the metadata channel — otherwise activity floods
  the reconcile loop.
- Tier 4's offline mode interacts with the write queue (7.E). Build
  4 first.

## Bottom line

This tier is the "team UX" layer that Suite Studio competes on. If
JuiceMount picks it up, the differentiation is "open-source +
self-hostable + plays nice with NLE-native locks" — vs. Suite's
"hosted + their own review tool." Order of business: 1-6 first, 7
only if the user base demands it.

## Iteration plan (deferred — community-driven if it happens)

| # | Slice | Hours | Files |
|---|---|---|---|
| 7.A.1 | Redis pub/sub for presence keyed by file handle; SADD on Open, SREM on Close | 4 | extend `nfs/handler.go` + `metadata/redis.go` |
| 7.A.2 | TTL-based liveness heartbeat (every 60s, expires at 5min on crash) | 3 | new goroutine in handler |
| 7.A.3 | Swift popover: per-file presence indicator (avatar overlay on row) | 4 | popover redesign |
| 7.B.1 | Per-path soft-lock store in Redis (SET NX EX 300) | 4 | extend handler open path |
| 7.B.2 | Override-stale-lock UI button | 2 | popover action |
| 7.C.1 | Activity stream in Redis (XADD, capped at 10K per project) | 3 | new event publisher |
| 7.C.2 | Activity feed view in popover | 3 | new section |
| 7.D.1 | ACL schema in metadata store + handler enforcement on every op | 6 | `metadata/store.go` + handler |
| 7.D.2 | ACL admin UI (per-folder + per-editor table) | 5 | new admin pane |
| 7.E.1 | Tier-7 + tier-4 write-queue integration: presence + lock checks before replay | 4 | extend `internal/writequeue/` |

Total: ~38 hours. Genuinely optional; only ship if a real multi-team
deployment demands it.

## Signals to watch

| Item | Signal |
|---|---|
| 7.A | Two editors open the same file → both see "in use by X" within 2s; one closes → the other sees release within 5s |
| 7.B | Editor A opens `.prproj` write mode; editor B's open returns EAGAIN immediately. A closes; B's retry succeeds. Force-stop A leaves stale lock; B's UI exposes override button |
| 7.C | Project activity feed shows last 20 events; filter by editor or op works |
| 7.D | Editor with read-only ACL on `/Footage/` can stat + read but cannot write or delete; writes return EACCES |
| 7.E | Offline write to a locked file: replay refuses; merge UI surfaces; no silent loss |

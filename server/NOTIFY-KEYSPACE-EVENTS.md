# Enabling Redis keyspace notifications for metadata push (NAS side)

> **Status: PRESCRIPTION ONLY — NOT APPLIED.** This document describes the
> server-side Redis config the Mac client's metadata keyspace-push feature
> wants. It is intentionally **not** deployed by this change. The client
> auto-detects whether the config is present and falls back to the proven
> 30s full-SCAN reconcile when it is absent, so the client is safe to ship
> before this lands. Apply it deliberately, on a maintenance window, and
> verify before relying on it.

## Why

The Mac core mirrors the JuiceFS metadata tree from Redis (`db 1`, the
`d{inode}` directory-entry hashes) into a local SQLite store. Historically it
did this with a **full Redis SCAN every ~30s**. Over cellular that SCAN takes
87–178s and finds zero changes ~93% of the time — it saturates the link and
kills navigation.

With **keyspace notifications** enabled, Redis publishes a message naming each
changed `d{inode}` key. The client subscribes (`PSUBSCRIBE __keyspace@1__:d*`)
and incrementally reconciles only the changed directories (one `HGETALL`
each). The full SCAN demotes to a rare, class-gated backstop.

## Recommended flag value: `KEA` (or at least `Kghx$`)

`notify-keyspace-events` is a set of single-character class flags:

| Flag | Meaning | Why we want it |
|------|---------|----------------|
| `K`  | Keyspace channel family (`__keyspace@<db>__:<key>`) | We learn **which** key changed from the channel name. **Required.** |
| `g`  | Generic commands (`DEL`, `RENAME_FROM/TO`, `EXPIRE`) | Whole-dir delete/rename (`DEL d{inode}`, rmdir). |
| `h`  | Hash commands (`HSET`, `HDEL`) | Child add/remove inside a dir hash — the common case. |
| `$`  | String commands (`SET`) | A write to an existing file changes only its `i{inode}` attr key. The client subscribes to `__keyspace@1__:i*` too, so a teammate's append or overwrite reaches the mirror (size, mtime) and the open file within about a second. |
| `x`  | Expired events | Near-zero cost; future-proofing. |

`server/redis.conf` ships `KEA` (all classes), which covers all of these.

**History (2026-11-13):** this document used to recommend `Kghx` and omit `$`,
accepting that an in-place overwrite stays stale until the rare backstop SCAN.
The laptop run showed the cost: a peer's edit stayed invisible on the Mac for as
long as the app held the file open (9+ minutes), and a peer's append never
changed the size. Without `$` the client logs `metadata attr push: off` and,
when it connects as an admin principal, adds `$` itself with `CONFIG SET`
(the same self-heal as below; `JM_KEYSPACE_AUTOCONFIG=0` disables it).

The client's sufficiency checks (so a partial config still falls back safely):

```
folders: contains('K') && (contains('A') || (contains('g') && contains('h')))
files:   contains('K') && (contains('A') || contains('$'))
```

Add `E` (keyevent channel family) only if you want keyevent-channel
observability → `KEghx`. The client does not require it.

The client's sufficiency check (so a partial config still falls back safely):

```
sufficient := contains('K') && (contains('A') || (contains('g') && contains('h')))
```

## DURABLE method (TrueNAS ix-app) — DO NOT use `CONFIG SET`

`CONFIG SET notify-keyspace-events KEA` is **non-durable** on this deployment:
Redis is launched from CLI args with an empty `config_file`, so
`CONFIG REWRITE` has nowhere to persist and the setting reverts on the next
container restart.

The durable change is to append the flag to the redis service command in the
TrueNAS ix-app **rendered** compose (matches the established
manager/farm rendered-compose deploy pattern):

1. Edit
   `/mnt/.ix-apps/app_configs/juicemount/versions/1.0.0/templates/rendered/docker-compose.yaml`
   (project `ix-juicemount`, service `redis`).

2. The redis `command` becomes:

   ```
   redis-server --appendonly yes --appendfsync everysec \
     --maxmemory-policy noeviction --save 900 1 \
     --notify-keyspace-events KEA
   ```

3. Recreate **only** the redis service (not the whole app).

4. Verify:

   ```
   redis-cli -n 1 config get notify-keyspace-events     # -> "AKE" (Redis reorders the flags)
   redis-cli -n 1 psubscribe '__keyspace@1__:d*'        # then touch a file; watch events
   ```

## Client behavior matrix (no rebuild needed to revert)

| NAS `notify-keyspace-events` | `JM_METADATA_KEYSPACE_PUSH` | Client behavior |
|---|---|---|
| empty / insufficient | anything | DISABLED — classic 30s full-SCAN, no keyspace path |
| sufficient (`KEA`) | unset / `0` | DISABLED (kill switch off) — classic 30s SCAN |
| sufficient (`KEA`) | `1` | ENABLED — push carries deltas, SCAN demotes to rare backstop |

The client re-probes `CONFIG GET notify-keyspace-events` on every (re)connect,
so a later NAS enablement is picked up without restarting the Mac app.

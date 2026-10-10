# `POST /derivatives/register` — typed rejection behavior

**CHANGED AT VERSION 3.** Every 409 now returns a JSON body, not prose. If your error handling was
written against the old strings it will NOT match — branch on `code`, never on `message`. The JSON
bodies below are illustrative shapes; message text is shortened or paraphrased, and exact messages
can vary with the request and underlying error. Schema:
[`register-conflict.schema.json`](../../spec/schema/register-conflict.schema.json).

`retryable` means strictly **"repeating the IDENTICAL request may succeed"**. A condition that needs
you to change the request, rewrite the blob, or recompute the derivative is `false` even though you
must act on it.

| code | retryable | what to do |
|---|---|---|
| `blob_not_visible` | **yes** | the write may still be draining (~6s); retry with THIS inode |
| `blob_empty` | **yes** | same window — see below, this one is not obvious |
| `source_unreadable` | conditional | retry transient mount/stat errors; permission and path-shape errors are permanent |
| `synthetic_inode` | no | re-resolve the inode; if `real_inode` is set, move the blob under it |
| `blob_not_regular` | no | something that is not a regular file holds the name; remove it |
| `blob_truncated` | no | when `blob_size` was supplied and differs from the visible blob size, rewrite atomically and register again |
| `source_stale` | no | RECOMPUTE against current bytes — do not retry |
| `producer_conflict` | no | a farm row holds this kind and wins by precedence |

## The blob must exist as a NON-EMPTY REGULAR file, checked at register time

```json
409 {"code":"blob_not_visible","retryable":true,
     "message":"blob \"poster.jpg\" not present as a regular file under the derivative dir — write it (atomically) BEFORE registering. RETRYABLE with THIS inode: the write may still be draining through the spool (~6s). ...",
     "inode":2000001,"real_inode":0}

409 {"code":"blob_not_regular","retryable":false,
     "message":"blob \"poster.jpg\" exists but is not a regular file (...) — remove it and write a regular file; retrying cannot change this",
     "inode":2000005,"real_inode":0}

409 {"code":"blob_empty","retryable":true,
     "message":"blob \"poster.jpg\" is 0 bytes — ... RETRYABLE with THIS inode: the drainer creates the destination at its final name and then copies into it, so a blob is legitimately 0 bytes for part of the drain window. Let the write land, then register.",
     "inode":2000002,"real_inode":0}
```

When the optional `blob_size` vouch is supplied and the visible non-empty blob has a different
size, the route returns `blob_truncated` with `retryable:false`. Rewrite the blob (prefer a temp
name followed by rename) and send a new registration. The mismatch cannot be fixed by repeating
the identical request.

**The 0-byte case is the one to design for, and we nearly got it wrong.** It is not a mistake you
can avoid by writing atomically: your atomic rename lands on the spool layer, but the last hop onto
FUSE is always create-then-copy at the final name, so the blob is legitimately 0 bytes for part of
the drain. We briefly marked this `retryable:false`, which would have permanently dropped any
contribution registered mid-drain — a proxy that cost minutes of transcode. Caught in review before
it shipped; the build you are talking to never had it. It is **retryable**. Let the write land.

## The vouch must match the LIVE source

```json
409 {"code":"source_stale","retryable":false,
     "message":"live size/mtime (2048/1700000500) != vouched (999/1700000500) — the derivative was computed against OLD bytes. NOT retryable: recompute it against the current source, then register. Retrying unchanged would publish a derivative that does not describe the file.",
     "inode":2000003,"real_inode":0,"path":"A/c.mov"}
```

Stat the source, compute against those exact bytes, send that exact (size, mtime). **Do not retry
this one** — that is the whole reason the codes exist. Under one undifferentiated 409 a uniform
retry masked it, and a masked `source_stale` publishes a derivative that does not describe the file.

## A freshly created file has a transient inode

```json
409 {"code":"synthetic_inode","retryable":false,
     "message":"inode is a transient pre-drain identifier, not a backend inode (high bit set). Registering under it would key the derivative to an identifier that ceases to exist. If real_inode is set, rewrite the blob under that inode's derivative directory and register with it. If it is absent the source has not reconciled yet: re-list the parent directory to force a fresh READDIR — a cached stat can hold a stale identifier indefinitely — then take the inode again.",
     "inode":9223372036854775817,"real_inode":0,"path":"A/d.mov"}
```

`real_inode` is **always present**; `0` means unresolved, not missing. Not retryable because the
branch is decided purely by the inode you sent — an unchanged retry can never take a different path,
and an offline-created file keeps a synthetic inode indefinitely.

## The farm's row wins

```json
409 {"code":"producer_conflict","retryable":false,
     "message":"kind \"thumbnail\" for inode 2000007 already has a farm-produced row; a \"on-device\" contribution may not replace it. PERMANENT — do not retry; the farm row wins by precedence, not by timing.",
     "inode":2000007,"real_inode":0}
```

## The path is not yours to choose

    400  blob_rel_path "../../../etc/passwd" is not the reserved name for kind "thumbnail"
         (expected "poster.jpg")

## You may not claim to be the farm

    400  producer must be on-device or macos-node

## Proxy: the H.264 floor is enforced, not advisory

    400  kind "proxy" requires an explicit "codec" — an absent codec is read as h264 by every
         downstream reader

    400  contributed proxies must meet the H.264 floor (got "hevc"). Encode H.264/AAC faststart
         for the contributed copy and keep richer codecs local; multi-rung support ...

An ABSENT codec is not neutral — `derivatives.schema.json` defines absent as h264, so omitting it
silently mislabels an HEVC blob to every reader. That is why it is required rather than defaulted.

## Verified round trip

Write blob → register (200) → `GET /derivatives?inode=N` shows the row → `GET /blob?inode=N&kind=K`
returns the exact bytes with the server-assigned `Content-Type`. Confirmed byte-identical on a real
volume.

## READ DISCOVERY DOES NOT WORK THROUGH THE FILESYSTEM

Derivatives the FARM wrote are **invisible over NFS** — `.juicemount` is filtered from the
provider's metadata mirror and NFS lookups are mirror-backed. Measured: `ls` of a farm-written
`poster.jpg` returns *No such file or directory* while `GET /blob` serves it fine.

Blobs YOU write are visible (they pass through the spool). So the filesystem gives you a partial,
misleading view. **Use `GET /derivatives?inode=N` to discover and `GET /blob` to read.** Never
probe the filesystem to decide whether a derivative already exists.

---

## MEASURED: there is a ~5.7 s window after writing a blob where register 409s. Retry it.

Confirmed on build 10, 2026-08-04, against a real volume:

```
t+0.02s  409  blob "proxy.mp4" not present as a regular file under the derivative dir
t+0.53s  409  ...
t+1.05s  409  ...
t+2.61s  409  ...
t+5.20s  409  ...
t+5.73s  200  ACCEPTED
```

**This is correct behaviour, not a bug, and it is now the DEFAULT path.** Writes to the volume
land in the provider's local write spool and are acked immediately; they become visible through
the FUSE mount only once the spool drains them. `register` stats the blob through FUSE — that is
deliberate, it is the same anchored walk `/blob` serves from, so the two can never disagree about
what exists.

The spool is **enabled by default as of 2026-08-04** (it previously shipped off), so every
consumer sees this window now, not just those who had opted in.

**What to do:** treat 409 "not present as a regular file" as **RETRYABLE**, with a bounded retry
of ~15-30 s. Do NOT treat it as terminal, and do NOT conclude the write failed — it did not. The
drain is idle-triggered, so the window scales with how recently you last wrote to that file, not
with its size.

The 0-byte rejection is the same family and the same answer: let the write land, then register. Both are `retryable:true`.
Both exist so that a row is never minted for bytes a reader cannot get — a missing blob 404s and
you regenerate locally, while a phantom row is a lie that also blocks repair.

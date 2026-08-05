# `POST /derivatives/register` — the rejections, as the live route actually emits them

Captured from a real run against a real volume (build 8, 2026-08-04), not written by hand. Every
line below is a verbatim response body. If your client's error handling is written against these
strings it will match production.

## The blob must exist as a NON-EMPTY REGULAR file, checked at register time

    409  blob "waveform.json" not present as a regular file under the derivative dir — write it
         (atomically) BEFORE registering: refusing non-regular derivative ...

    409  blob "waveform.json" is 0 bytes — write the bytes and let the write land BEFORE
         registering; an empty blob would be served as a real answer instead of 404ing so the
         reader can regenerate

**The 0-byte case is the one to design for.** Writing and registering in the same breath can race
the write becoming visible through the mount. A blob observed at 0 bytes was seen once during
validation. Let the write land — fsync the file AND the parent dir (§3) — before you register.

## The vouch must match the LIVE source

    409  stale: live size/mtime (69632/1785883406) != vouched (999/1785883406) — AI computed
         against old bytes

Stat the source, compute against those exact bytes, send that exact (size, mtime).

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

The 0-byte rejection is the same family and the same answer: let the write land, then register.
Both exist so that a row is never minted for bytes a reader cannot get — a missing blob 404s and
you regenerate locally, while a phantom row is a lie that also blocks repair.

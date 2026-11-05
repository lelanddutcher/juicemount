# Continuity volume binder

Experimental, behind `JM_CONTINUITY=on` (off by default). The Manager keeps one
continuity catalog in `<state dir>/continuity/catalog.sqlite` (a directory of its
own, mode 0700, owned by the Manager, never inside the volume) and binds every
visible regular file of its JuiceFS volume to a catalog asset. It reads volume
metadata through a read-only client (`jfsmeta.OpenReadOnly`: the JuiceFS client
refuses mutations and flushes no counters, no session); it never writes to the
volume, reads file contents or hashes anything.

## Identity

A file is its inode. JuiceFS never reuses an inode number: `baseMeta.nextInode`
hands out numbers from batches that `allocateInodes` reserves with `INCRBY` on the
volume's `nextinode` counter (`third_party/juicefs/pkg/meta/base.go`,
`nextInode`/`allocateInodes`; `redis.go`, `incrCounter`, `counterKey`), and a
freed inode is never returned to it. The exception is a metadata restore
(`juicefs load` of an older dump), which can roll the counter back; the binder
cannot detect that.

For volume UUID `U` and inode `I`:

| Record | Value |
| --- | --- |
| Asset | ID `asset_` + first 16 bytes of SHA-256(`juicemount.continuity.jfs-asset` NUL `U` NUL decimal `I`); label = the file name at creation, never changed by the binder |
| Inode location `jfs-U-I` | `{source: "jfs:U", locator: "ino:I"}` |
| Hint location `jfs-U-I-hint` | `{source: "jfs-path:U", locator: <volume-relative path>}` |

Both IDs are derived, so a replayed bind is the identical request. The hint is a
location record rather than a new `LocationValue` field: a field needs a new
column, so a catalog schema format, which this slice does not take on.

## Operations

Every event re-reads the volume and the catalog (`sync`), then plans at most one
operation that moves both location records to the volume's state: active with
the current hint while the inode has a visible name outside the trash,
tombstoned otherwise.

| Kind | When | Changes |
| --- | --- | --- |
| `bind` | no inode location yet | create asset, put both locations |
| `hint` | active, hint differs | put hint location (expecting its state) |
| `revive` | tombstoned but visible again | put both locations (expecting the tombstones) |
| `unlink` | gone, only in the trash, or only hidden names | remove both locations |

The operation ID is `jfs-U-I-<kind>-<inode seq>-<hint seq>`, the sequences being
the `UpdatedSequence` of the two records the plan starts from (0 when absent). A
replay starts from the moved state and plans nothing. A conflict re-plans (3
rounds). Any other `Apply` error may be an uncertain commit, so the identical
request is sent once more; if it had committed, the catalog answers with the
original receipt.

- **Hint:** the current hint is kept while it still names the inode (two point
  reads). Otherwise it is the observed path (one link) or the bytewise-smallest
  visible name. Listing every name reads whole directories up to the root, so it
  happens only for hard links or a hint that moved, and not above 64 links.
- **Unknown is not gone.** JuiceFS's `GetPaths` skips directories it fails to read;
  `jfsmeta.GetPaths` returns EIO when it finds fewer names (trash ones included)
  than the link count. Any failed volume read leaves the records unchanged; the
  event is retried twice (200 ms, 1 s), then counted as failed.
- **Rename** keeps the asset and records; only the hint changes. A directory rename
  publishes one event, so its files' hints stay stale until their next event; the
  API resolves the current path. A rename to a hidden name (Finder's `.Trashes`)
  tombstones; moving the file back revives the same records.
- **Delete** events can carry a client's synthetic inode (bit 63) or a stale one.
  The event's inode and the inode last seen at the path are both synced; a delete
  that matches no bound inode is counted as unresolved.
- **Rename over a file** (the usual save) publishes no delete for the replaced
  file. A least-recently-used map of the last inode seen at each path (65,536
  paths, in memory) retires it; evictions are counted.
- Not catalogued: directories, symlinks, files with only hidden names (a dot
  component such as `.trash`, `.Trashes`, `.juicemount`, `.DS_Store`, or `Icon\r`),
  and non-canonical paths or paths over 4,096 bytes.

## Bounds and gaps

Events go through a bounded queue (4,096). The subscriber never blocks: a full
queue drops and counts. Changes are missed when:

- the Manager is down or not yet subscribed (`observed_since`);
- go-redis re-subscribes after a dropped connection (`resubscribes`);
- the queue is full or stopped (`dropped`), or still queued at stop (`discarded`);
- volume reads keep failing or the catalog refuses (`failed`);
- a delete names no inode the binder knows (`unresolved`), or the replaced file's
  path was evicted (`path_cache_evictions`);
- no client publishes them: writes by Manager sync, the farm or the WebDAV/S3
  gateway, and renames of files a client had not mirrored.

There is no rescan in this slice. `GET /api/continuity/status` reports these
counters and `complete: false`; a later rescan starts from them.

## API (admin key)

`GET /api/continuity/assets?path=<volume path>` or `?id=<asset id>` returns the
asset ID and label, the path hint, active locations and the file's current path,
resolved from the volume. `GET /api/continuity/status` returns the counters.

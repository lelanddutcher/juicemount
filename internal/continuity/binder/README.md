# Continuity volume binder

Experimental, behind `JM_CONTINUITY=on` (off by default). The Manager keeps one
continuity catalog in `<state dir>/continuity/catalog.sqlite` (a directory of its
own, mode 0700, owned by the Manager, never inside the volume) and binds every
visible regular file of its JuiceFS volume to a catalog asset. It reads volume
metadata only (`internal/smartfolders/jfsmeta`, no session); it never writes to
the volume, reads file contents or hashes anything.

## Identity

A file is its inode. JuiceFS never reuses an inode number: `baseMeta.nextInode`
hands out numbers from batches that `allocateInodes` reserves with `INCRBY` on the
volume's `nextinode` counter (`third_party/juicefs/pkg/meta/base.go`,
`nextInode`/`allocateInodes`; `redis.go`, `incrCounter`), and a freed inode is
never returned to it. The exception is a metadata restore (`juicefs load` of an
older dump), which can roll the counter back; the binder cannot detect that.

For volume UUID `U` and inode `I`:

| Record | Value |
| --- | --- |
| Asset ID | `asset_` + first 16 bytes of SHA-256(`juicemount.continuity.jfs-asset` NUL `U` NUL decimal `I`) |
| Location ID | `jfs-U-I`, value `{source: "jfs:U", locator: "ino:I", revision: ""}` |
| Asset label | the path hint: the volume-relative path last observed |

Both IDs are derived, so a replayed bind is the identical request.

## Operations

Every event re-reads the volume (the published inode is used only for deletes)
and the catalog, then plans at most one operation from the current state:

| Kind | When | Changes |
| --- | --- | --- |
| `bind` | no location yet | create asset (label = hint), put location |
| `hint` | active, label differs | rename asset |
| `revive` | tombstoned but live again (restored from the trash) | put location (expecting the tombstone) [+ rename] |
| `unlink` | file gone or only in the trash | remove location (tombstone) |

The operation ID is `jfs-U-I-<kind>-<location seq>-<asset seq>`, the sequences
being the `UpdatedSequence` of the records the plan starts from (0 when absent).
A replay or an uncertain-commit retry from the same state is the identical
request, which the catalog answers with the original receipt; once applied, the
state moved, so a replayed event plans nothing. A conflict re-plans (3 rounds).

- **Rename** keeps the asset and location; only the hint changes. A directory
  rename publishes one event, so the hints of its files stay stale until their
  next event. The API resolves paths live.
- **Delete** tombstones the location when the inode is gone or its only parent is
  a trash directory. The asset stays (assets are never deleted).
- **Rename over a file** (the usual save) publishes no delete for the replaced
  file. The binder remembers the last inode seen at each path (in memory, 65,536
  paths at most) and retires the replaced inode the same way.
- **Hard links:** the hint is the bytewise-smallest visible path (else the
  smallest hidden one) among the inode's names outside the trash, so it does not
  depend on event order. Losing the hinted name moves the hint; losing the last
  name tombstones the location. Reading every name lists whole directories, so
  it is done only for files with more than one link.
- Not catalogued: directories, symlinks, paths with a dot component (`.trash`,
  `.juicemount`, `.DS_Store`, temp files) or `Icon\r`, and non-canonical paths
  or paths over 4,096 bytes.

## Bounds and gaps

Events go through a bounded queue (4,096). The subscriber never blocks: a full
queue drops and counts. Changes are missed when:

- the Manager is down or not yet subscribed (`observed_since`);
- go-redis re-subscribes after a dropped connection (`resubscribes`);
- the queue is full (`dropped`), or a volume/catalog error stops one (`failed`);
- a delete names no inode the binder knows (`unresolved`);
- no client publishes them: writes by Manager sync, the farm or the WebDAV/S3
  gateway, and renames of files a client had not mirrored.

There is no rescan in this slice. `GET /api/continuity/status` reports these
counters and `complete: false`; a later rescan starts from them.

## API (admin key)

`GET /api/continuity/assets?path=<volume path>` or `?id=<asset id>` returns the
asset ID, path hint, active locations and the file's current path(s), resolved
from the volume. `GET /api/continuity/status` returns the counters.

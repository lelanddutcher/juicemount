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
| Later pair `jfs-U-I-g<n>`, `jfs-U-I-g<n>-hint` (n = 1…8) | as above, for an inode that must carry another asset than its current pair holds: a merge, or a restore (below) |
| Relationship `jfs-U-I-g<n>-merge` | `{source asset: retired provisional asset, target: continued asset, predicate: "continuity:merged-into", provenance: observed, source: "jfs:U"}` |
| Relationship `jfs-U-I-g<n>-restored` | `{source asset: the restored file's new asset, target: the asset it had, predicate: "continuity:restored-from", …}` |

A file is its inode until it is saved by replacement: then the new inode
continues the old one's asset (below), so an asset's ID is derived from the
first inode it was bound to, and an inode can carry another inode's asset.

The IDs are derived, so a replayed bind is the identical request. The hint is a
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
| `continue` | a replaced file's asset moves to its unbound replacement | remove the old inode's pair, put the new inode's pair with the old asset |
| `merge` | as `continue`, the replacement having a provisional asset | also remove the replacement's own pair, put its next pair, put the merge relationship |
| `restore` | tombstoned but visible again while its asset is live on another inode | create an asset of its own (`AssetID` of U, I and n), put its next pair, put the restore relationship |

A location's asset never changes, so an inode that must carry another asset
gets its next pair. An inode's binding is its highest pair; `bind`, `hint`,
`revive` and `unlink` act on that pair (kinds prefixed `g<n>-` on pair n).
Two live inodes never share an asset; `CheckAccess` requires every live
inode of an asset to pass, should that ever not hold.

The operation ID is `jfs-U-I-<kind>-<inode seq>-<hint seq>`, the sequences being
the `UpdatedSequence` of the two records the plan starts from (0 when absent). A
replay starts from the moved state and plans nothing. A conflict re-plans (3
rounds). Any other `Apply` error may be an uncertain commit, so the identical
request is sent once more; if it had committed, the catalog answers with the
original receipt.

- **Hint:** the current hint is kept while it still names the inode (two point
  reads). Otherwise it is the observed path (one link) or the bytewise-smallest
  visible name. Listing every name reads whole directories up to the root, so it
  happens only for hard links or a hint that moved. Above 64 links the records
  are left as they are and the event counts as `too_many_links` (no retry).
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
  paths, in memory) notices it; evictions are counted. See "Saves that replace
  the file".
- Not catalogued: directories, symlinks, files with only hidden names (a dot
  component such as `.trash`, `.Trashes`, `.juicemount`, `.DS_Store`, or `Icon\r`),
  and non-canonical paths or paths over 4,096 bytes.

## Saves that replace the file

Editors save by writing a temp file and renaming it over the document;
Finder's replace does the same. The document gets a new inode, which
continues the replaced inode's asset: same asset ID, label, collections and
history. Tested in `continue_test.go`.

- **When.** A rename event onto path `p` finds `p` naming inode `Y` while the
  path cache says `p` last named a bound `X`, `X` has no visible name left,
  and the rename looks like a save: it was received (by `Offer`) within
  `SaveWindow` (2 min) of `X`'s ctime, which JuiceFS sets when the rename over
  `p` (or an unlink) sends `X` to the trash. `Y`'s own ctime is not used:
  writes committed after the rename (client write-back, a spool drain) move
  it. Not continued, counted as `not_continued`: a create or update showing a
  new file where `X` vanished (a delete unseen and a new file of the same
  name, as camera clips have; also `not_continued_no_rename`), a rename long
  after `X` left, and an `X` whose attributes are gone (deleted for good: so
  a volume without a trash never continues a save). One
  operation (`continue`, ID `jfs-U-Y-continue-<X's seq>-0`) retires `X`'s
  pair and puts `Y`'s with `X`'s asset and hint `p`. No asset is created for `Y`.
- **The temp file was bound first** (its create event came before the rename;
  also a temp file written in another folder): `merge`. A location's asset
  never changes, so `Y`'s own pair is retired and its next pair
  carries `X`'s asset; the relationship records which asset was retired into
  which. Only a *provisional* asset is merged: `Y`'s own derived asset, bound
  within `ProvisionalAge` (1 h, by its bind's receipt time). An older file
  moved over another (Finder's replace of one document by another) keeps its
  own asset and the replaced one is retired, as before: a heuristic never
  ends an established identity.
- **A replaced file restored** from the volume trash (or moved back from a
  hidden name) while its asset lives on in the document: `restore`, not
  `revive`. It gets an asset of its own, related to the one it had; the
  document keeps the asset, its access check and its collections.
- **Safe save** (some Mac apps: write a temp file, rename the original to a
  backup name, rename the temp file to the original name, delete the backup)
  has no rename over. A rename of a known file to a sibling name is
  remembered (in memory, up to 1,024, for `SafeSaveWindow`, 30 s); when that
  file's delete comes within the window and another regular file now holds
  the name it left, that file continues its asset (`continue`, usually
  `merge`). A visible or hidden backup name both work, and so does a client's
  synthetic inode on the delete (the window runs from the backup's unlink
  to the delete event's receipt). False positive: rename `clip.mov` to
  `clip_old.mov`, copy a new `clip.mov` in, delete `clip_old.mov` within 30 s:
  the new clip continues the old one's asset.
- **Restart.** At start the path cache is seeded from the catalog's active
  hints (`ListSourceLocations`, up to the cache size, no evictions), so a save
  replaced across a restart is still continued. Hints can be stale: a stale
  entry only costs the check that its inode is gone; the rename and ctime
  checks keep a file deleted long ago from being continued by a new one at
  its path.
- **Falls back** (the old behaviour: `X` retired, `Y` bound on its own, counted
  as `not_continued`): no evidence of a save (above), `Y` has other names
  (hard links), `Y` has an established asset or no pair left, or `X`'s asset
  is bound to another live inode. Not a replacement at all, so not counted:
  `X` still has a visible name (a hard link elsewhere keeps its asset and hint), or `X` was
  never bound.
- **Residual.** Missed, with `Y` getting a new asset and `X`'s records staying
  active until a rescan: the path was evicted from the cache
  (`path_cache_evictions`), or the rename event was lost. A safe save is
  missed across a restart (the remembered renames are not persisted), when
  the backup goes to another folder (NSDocument's "(A Document Being Saved
  By …)" folder), or when the backup is kept (an Emacs-style `doc~`): the
  backup keeps the asset. A save whose rename event comes from another
  client than an update of the same path handled first is not continued
  (one client's events arrive in order). A delete unseen and a new file
  *renamed* onto the path within 2 minutes (a copier writing a temp name)
  is continued. The window compares the client's clock (ctime) with the
  Manager's; skew eats into it. Each save
  leaves one retired pair on the asset (and, for a merge, a retired asset);
  `CheckAccess` finds the live inode first and pages the retired ones only
  when none is live.

## Bounds and gaps

Events go through a bounded queue (4,096). The subscriber never blocks: a full
queue drops and counts. Changes are missed when:

- the Manager is down or not yet subscribed (`observed_since`);
- go-redis re-subscribes after a dropped connection (`resubscribes`);
- the queue is full or stopped (`dropped`), or still queued at stop (`discarded`);
- volume reads keep failing or the catalog refuses (`failed`);
- a delete names no inode the binder knows (`unresolved`), the replaced file's
  path was evicted (`path_cache_evictions`), or the file has over 64 links
  (`too_many_links`);
- a save by replacement was not continued (`not_continued`; above);
- no client publishes them: writes by Manager sync, the farm or the WebDAV/S3
  gateway, and renames of files a client had not mirrored.

There is no rescan in this slice. `GET /api/continuity/status` reports these
counters and `complete: false`; a later rescan starts from them.

## For later slices

- `jfs-path:` hint locations are hints only: stale after a folder rename, and two
  assets can hold the same path (a file replaced before its old record was
  retired). Never use them as identity or to deduplicate; resolve the current
  path from the volume (`AssetView.Path`).
- Tell the records apart by source: `Source(uuid)` for the inode, `HintSource(uuid)`
  for the hint.
- The catalog has no index on locators, so path → asset goes through the volume
  (`LookupIno`, then the inode's derived location IDs: the continuation pair
  first), or needs an index in a new schema version.
- An asset's inode is its active `Source(uuid)` location, not the one derived
  from the asset ID: after a save by replacement they differ.

## API (admin key)

`GET /api/continuity/assets?path=<volume path>` or `?id=<asset id>` returns the
asset ID and label, the path hint, active locations and the file's current path,
resolved from the volume. `GET /api/continuity/status` returns the counters, including `continued`,
`merged`, `not_continued`, `not_continued_no_rename` and `pairs_exhausted`
(a restored file left out because its inode used all 8 later pairs).

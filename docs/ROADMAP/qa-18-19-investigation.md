# QA-18 + QA-19 — investigation, proposed fixes, and next-loop plan

**Date:** 2026-05-17
**Found in:** QA suite run #3 (run-id `20260517-173656`)
**Status:** investigation complete; fixes proposed; ready for implementation loop

---

## QA-19 — Stale NFS file handle during sustained sequential write

### Symptom

`scripts/qa-suite/04-fio.sh` profile `seqwrite-1m` (1 MiB blocks via psync
into a 512 MiB file, single thread, sequential). After ~52 seconds of
writing (~412 MiB through), fio aborts with:

```
fio: io_u error on file /Volumes/zpool-dev/.jmqa-fio-39706/seqwrite-1m.0.0:
     Stale NFS file handle: write offset=412090368, buflen=1048576
fio: pid=39722, err=70/file:io_u.c:2018, func=io_u error,
     error=Stale NFS file handle
```

errno=70 = `ESTALE` = `NFS3ERR_STALE` returned by our server.

### Code path that returns NFS3ERR_STALE

Two sites in our code can return `NFSStatusStale`:

1. **`nfs/handler.go:414` — `FromHandle`**:
   ```go
   inode := binary.BigEndian.Uint64(handle)
   e := h.store.LookupByInode(inode)
   if e == nil {
       return nil, nil, &nfslib.NFSStatusError{NFSStatus: nfslib.NFSStatusStale}
   }
   ```
2. **`internal/nfs/nfs_onwrite.go:39`** — wraps a FromHandle failure:
   ```go
   fs, path, err := userHandle.FromHandle(req.Handle)
   if err != nil {
       return &NFSStatusError{NFSStatusStale, err}
   }
   ```

The trigger therefore reduces to: **the NFS file handle the kernel client
holds (which is just the inode number, big-endian, 8 bytes — see
`ToHandle` at `nfs/handler.go:362`) resolved to nothing in
`store.LookupByInode()` mid-write.**

For that lookup to suddenly return nil, *something must have removed
that inode entry from `metadata.Store.inodeCache` between the time the
handle was issued (at file create / first lookup) and the failing WRITE
RPC ~52 seconds later.*

### What can remove a live inode from `inodeCache`

`grep -rn 'DeleteFromCache\|delete.*inodeCache'` finds 5 callers:

| # | Site                            | Trigger                                            |
|---|---------------------------------|----------------------------------------------------|
| 1 | `nfs/handler.go:575`            | **phantom-purge in `Stat`** — `lstatNotExistWithTimeout` returned ENOENT for the path |
| 2 | `nfs/handler.go:816`            | phantom-purge in **read-only OpenFile** when `fdPool.Get` returns ENOENT |
| 3 | `nfs/handler.go:915`            | Rename: deletes the OLD path entry |
| 4 | `metadata/redis.go:369`         | SUBSCRIBE event: `op="delete"` from another writer |
| 5 | `metadata/redis.go:376`         | SUBSCRIBE event: `op="rename"` deletes OldPath |
| † | `metadata/store.go:668` (`DeletePaths`) | Bulk prune from `syncMetadata` after `PruneThreshold` consecutive absences |

For fio's pattern (single writer, single file, no rename, no concurrent
delete), the only mechanisms that can fire are:

- **(1) phantom-purge in Stat** — *if* the kernel NFS client issues a
  refresh GETATTR mid-write AND our `lstatNotExistWithTimeout` against
  FUSE momentarily returns ENOENT.
- **(†) prune via `DeletePaths`** — *if* the file is absent from the
  Lua scan results for `PruneThreshold` (2) consecutive sync cycles
  (~60 s minimum at 30 s interval).

Both are plausible during a write that lasts 54 s with a writeback
backend.

### Why the kernel NFSv3 client refreshes GETATTR mid-write

NFSv3 has no leases; clients refresh attributes opportunistically. macOS
`mount_nfs` aggressively re-stats the file roughly every 1-3 seconds
under sustained I/O to keep the size attribute current for callers like
`fstat()`. That GETATTR routes through `Stat()` in our handler, which
runs the phantom-purge gate.

### Why FUSE momentarily returns ENOENT during a writeback write

JuiceFS is mounted with `--writeback --buffer-size 1024`. In writeback
mode, writes land in a local buffer first; metadata commits to Redis
asynchronously. During the commit window, JuiceFS's own FUSE layer can
serve stat requests against a path that has been re-allocated internally
(e.g., chunk rotation), causing a transient ENOENT visible to our
`os.Lstat(fusePath)` call.

Combined with the existing phantom-purge logic at `nfs/handler.go:572`:

```go
fusePath := jfs.fullPath(filename)
isNotExist, ok := lstatNotExistWithTimeout(fusePath, 2*time.Second)
if ok && isNotExist {
    jmlog.Warn("purging phantom file (stale cache)", "path", filename)
    jfs.handler.store.DeleteFromCache(filename)   // ← deletes inodeCache[inode]
    go jfs.handler.store.Delete(filename)
    return nil, os.ErrNotExist
}
```

A single transient ENOENT during a writeback sync = full cache eviction =
all in-flight handles for that file invalidated = next WRITE RPC gets
NFS3ERR_STALE.

### Reproduction

`scripts/qa-suite/04-fio.sh` profile `seqwrite-1m` reproduces it
deterministically on the current binary against the LAN backend. ~52 s
into the write, ESTALE fires.

### Proposed fix (preferred — surgical)

**Gate the phantom-purge on the presence of an active write tracker for
the same path.** `writeSizes[path]` is set when a `writeFile.WriteAt`
runs and not deleted on Close (QA-16 fix). If `hasWriteSize` is true, we
know a writer recently exercised the path; trust the cache entry and
skip the purge.

In `nfs/handler.go` around line 548:

```go
if !e.IsDir {
    // QA-19 fix: NEVER purge a file with in-flight writes. A
    // concurrent NFS GETATTR racing a sustained WRITE can observe a
    // transient ENOENT from FUSE (juicefs writeback hasn't synced
    // metadata yet) and incorrectly purge the cache entry, which
    // invalidates the writer's NFS handle and surfaces as ESTALE.
    // writeSizes is the authoritative signal that the path has an
    // active writer; trust it over a single Lstat observation.
    if hasWriteSize {
        // skip purge; treat cache entry as authoritative
    } else if jfs.handler.redisClient != nil &&
              jfs.handler.redisClient.RecentlyDegraded(60*time.Second) {
        // existing Redis-flap gate (unchanged)
    } else {
        // existing phantom-purge logic (unchanged)
    }
}
```

**Defense in depth — also gate the prune path.** In
`metadata/redis.go:syncMetadata`, when computing absent paths, skip any
path with an active `writeSizes[path]` so the prune-absent counter never
increments for a file currently being written.

### Alternative considered

`FromHandle` could fall back to an emergency Stat-via-FUSE before
declaring STALE. Rejected because: it adds latency to the hot READ/WRITE
path, masks the underlying cache-eviction race, and would still race
with juicefs's own ENOENT window.

### Acceptance test

`scripts/qa-suite/04-fio.sh` must complete all 8 fio profiles with
zero ESTALE errors. Additional asserting test: write a 1 GiB file via
single-threaded fio + simultaneously issue 10 GETATTR RPCs per second
against the same path (separate workload generator); verify zero ESTALE.

### Severity

**HIGH.** Any sustained sequential write past ~30 s on writeback-mode
JuiceFS can hit this. Premiere Pro renders, ffmpeg encodes, dd, cp of
large media files, rsync's transfer phase — all real-world workloads.
This is the C.1 code reviewer's HIGH finding ("FDPool eviction's
refCount window is sound-in-practice but not formally race-free") — but
the actual offender turned out to be the phantom-purge path, not FDPool
eviction. Now we have a deterministic reproducer.

---

## QA-18 — "Not a directory" race during rapid small-file creation

### Symptom

`scripts/qa-suite/02-finder.sh`, "1000 × 1 KiB" test. The script does:

```bash
mkdir -p "$SMALL_DIR"
for i in $(seq 1 1000); do
    head -c 1024 /dev/urandom > "$SMALL_DIR/file-${i}.txt"
done
```

After 389 successful creates, file-390 errors with:

```
/Volumes/zpool-dev/.jmqa-finder-37615/small-files/file-390.txt:
    Not a directory
```

`set -u` in the phase script then kills the run. The same phase passes
cleanly when invoked standalone — the failure is timing/load-dependent
and only fires when small-files creation follows the prior phases'
I/O burst.

### Code path that returns ENOTDIR

ENOTDIR (errno 20) for `open(O_CREAT)` means a path component that
should be a directory wasn't — kernel resolved one of
`.jmqa-finder-37615` or `small-files` as a regular file when looking up
parents for the create.

In our NFS server's flow, that means a LOOKUP RPC against the parent
returned an `fattr3` with `type = NF3REG` rather than `NF3DIR`. The
`fattr3.type` comes from our `Entry.IsDir` via `entry.FileInfo()`.

So **`Entry.IsDir` was `false` for one of the parent path components at
the moment the kernel issued the LOOKUP.**

### Where IsDir gets set on a directory entry

| Path | Action on dir entry |
|------|---------------------|
| `Create` (file create RPC) | inserts new entry for the FILE; IsDir=false. Doesn't touch parent. |
| `MkdirAll` | inserts the dir with IsDir=true and `LocalOnly=true`. |
| `applyEvent` (SUBSCRIBE) | takes `evt.IsDir` from juicefs's event. |
| `syncMetadata` BulkInsert | takes `IsDir = ft == 2` from Lua. |
| `Rename` | reuses `oldEntry.IsDir`. |

### Hypothesis 1 — sync flips a child entry to share the dir's path key

Unlikely. Each entry is keyed by its full path. Children share the parent's
ParentPath but have distinct Paths. Insert can't overwrite the parent
unless `e.Path == parentPath`.

### Hypothesis 2 — children index churn corrupts ListChildren

When `BulkInsert` runs `removeFromChildrenIdx(old)` followed by
`addToChildrenIdx(e)`, there is a window inside `s.mu.Lock` where the
children slice for the parent path has been partially rebuilt. If a
concurrent reader took `s.mu.RLock` between the two operations... but
RLock and Lock are mutually exclusive, so no concurrent reader can
observe the intermediate state. Not this.

### Hypothesis 3 — pathCache LRU evicts the parent dir, then a fallback
**re-creates it as a file** via `ToHandle`'s synthetic-inode path

This is the strong hypothesis. `metadata.Store.evictOldest()` runs
inside `BulkInsert` (`store.go:306`). If the LRU evicts the parent
directory entry (because the rapid file creates pushed many child entries
to the top of LRU and the dir hadn't been re-touched recently), then a
subsequent `ToHandle(parentPath)` falls through to:

```go
// nfs/handler.go:380
// Fallback: entry not in cache yet (just created, or FUSE-only).
hash := fnv.New64a()
hash.Write([]byte(fullPath))
inode := hash.Sum64() | (1 << 63)
entry := metadata.MakeEntry(fullPath, false, 0, time.Now(), inode)
//                                   ^^^^^ HARDCODED false ← BUG
h.store.InsertToCache(entry)
```

**`MakeEntry(fullPath, false, ...)`** hardcodes `IsDir=false`. If a
directory's entry is evicted from the cache (LRU) and a later operation
on that directory needs a handle, our fallback creates an entry that
claims the path is a *file*. Subsequent NFS LOOKUPs against the path see
`fattr3.type=NF3REG` → kernel returns ENOTDIR for any child operation.

This matches the QA-18 symptom precisely.

### How rapid create triggers LRU eviction of the parent

`evictOldest` runs inside `BulkInsert`. Background sync at 30 s
intervals could BulkInsert 100s of new entries. If the parent dir's
entry isn't in the "recently used" tier when the LRU prunes, it gets
evicted. The parent dir was last touched at `mkdir -p` time, which by
the time we're at file-390 is already 10+ seconds old and may not be
"recent" enough.

### Reproduction

Hard to reproduce standalone — needs the prior-phase I/O burst to push
the LRU pressure that evicts the parent dir. Test 02-finder + 1000 small
files inside the full suite reproduces it; running 02-finder alone does
not.

### Proposed fix (preferred — surgical)

In `nfs/handler.go:386` (`ToHandle` fallback path), do an `os.Lstat`
against the FUSE path BEFORE creating the synthetic-inode entry, and use
the real `IsDir` value:

```go
// Fallback: entry not in cache. Stat FUSE to determine the real
// IsDir before fabricating a synthetic entry. Without this, the
// fallback hardcoded IsDir=false, which corrupted any directory
// that got LRU-evicted under load (QA-18: rapid child creates
// pushed the parent dir out, and the next LOOKUP for the parent
// got NF3REG instead of NF3DIR → kernel returned ENOTDIR for
// subsequent child operations).
hash := fnv.New64a()
hash.Write([]byte(fullPath))
inode := hash.Sum64() | (1 << 63)

isDir := false
fusePath := h.fusePath + "/" + fullPath
if fi, err := os.Lstat(fusePath); err == nil {
    isDir = fi.IsDir()
}
entry := metadata.MakeEntry(fullPath, isDir, 0, time.Now(), inode)
h.store.InsertToCache(entry)
```

The Lstat adds a syscall to the fallback path, but the fallback only
fires when the cache misses — this is already a slow path.

### Defense in depth — make `evictOldest` skip directories

Directories are far fewer in count than files (typical creator: O(1000)
dirs, O(1M) files). The cost of pinning all dirs in the cache is small.
And losing a directory entry to LRU is high-impact (every child operation
on that dir fails). Modify `evictOldest` to skip entries where `IsDir`.

### Acceptance test

The 1000-small-files test in 02-finder must pass cleanly under suite
load. Additionally: a stress test that creates 10,000 files in 100
different new directories, exercising the LRU pressure.

### Severity

**MEDIUM-HIGH.** Affects any rapid-create workflow: `git clone` of a
large repo, downloading a Premiere Pro project bundle, importing a media
library, untarring assets. Stochastic — only fires under cache pressure.

---

## Adjacent observations from run #3

These aren't fixes for QA-18/19 but are surfaced by the same investigation
and worth noting:

1. **JM RSS grew 1.5 GB → 2.4 GB over 54 min (60%).** Endurance phase
   showed no drift over its 20-min window, so the growth concentrates
   in the fio + 02-finder phases. Possibly leaked entries in `inodeCache`
   (Insert never removes the old inode when replacing a path's entry —
   `metadata/store.go:175`). Worth tier-2 investigation.

2. **07-failure scenario B (toggle user-offline mid-write) crashed.**
   Harness uses `curl -X POST /offline -d '{"offline":true}'` but the
   `/offline` endpoint contract may take query params. To fix in 07-failure.sh:
   ```bash
   curl -X POST "http://127.0.0.1:11050/offline?offline=true"
   ```
   (verify against `bridge/cbridge.go:handleOfflineHTTP` first).

3. **02-finder `Quick Look-equivalent` regression: 4-8/50 vs 50/50 in
   run #1.** Strongly correlates with the freshly-restarted-mount state
   — the mount was 5-30 s old when 02-finder ran. The 50 files picked by
   `find -maxdepth 3 -type f` include `.juicemount-selftest.tmp` and
   AppleDouble sidecars that may not have backing data yet. To fix in
   02-finder.sh: filter the find to `! -name '.*'` to skip dotfiles, and
   add a settling sleep at phase start.

4. **05-metadata 1/341 ls failures during concurrent writes** — same
   class of timing issue as QA-18; the in-flight churn during writes
   produced a transient inconsistency. The fix for QA-18 (Lstat fallback
   in ToHandle) likely also resolves this.

---

## Plan for the next fix-and-test loop

### Slice 1 — QA-19 fix (phantom-purge gated on active writes)

**File:** `nfs/handler.go` around line 548
**Change:** add `if hasWriteSize { /* skip purge */ }` branch as the
first check in the existing if-tree.
**Defense in depth:** `metadata/redis.go:syncMetadata` — when bumping
pruneAbsent counters, skip paths with active writeSizes.

**Code-reviewer prompt (Rule 4 applies):**
> Review the QA-19 fix in nfs/handler.go: adding a writeSizes check
> before the existing phantom-purge logic. The change makes the
> phantom-purge skip cache eviction when a file has an in-flight
> writer. Audit: (a) is the writeSizes read under the right lock
> ordering relative to phantom-purge? (b) can a writer terminate
> between hasWriteSize=true and the purge decision being made?
> (c) does the defense-in-depth change in syncMetadata interact
> correctly with the pruneAbsent counter ladder?

**Acceptance:** `04-fio.sh` completes 8/8 profiles with zero ESTALE.
Add a new test: 1 GB sustained write + parallel GETATTR storm; zero
ESTALE.

### Slice 2 — QA-18 fix (ToHandle fallback Lstats real IsDir)

**File:** `nfs/handler.go:380` (ToHandle fallback)
**Change:** Lstat the FUSE path before fabricating the entry; pass the
real IsDir to MakeEntry.
**Defense in depth:** `metadata/store.go:evictOldest` — skip entries
where IsDir=true.

**Code-reviewer prompt:**
> Review the QA-18 fix in nfs/handler.go: ToHandle fallback now Lstats
> FUSE for the real IsDir. Audit: (a) Lstat timeout — should it use
> lstatNotExistWithTimeout pattern? What if FUSE is wedged? (b) the
> ToHandle path is on the hot LOOKUP path under heavy traffic — does
> adding a syscall here introduce a hot-path regression? (c) the
> defense-in-depth evictOldest skip — what's the memory cost vs.
> the cost of mis-cached directories?

**Acceptance:** 02-finder 1000-small-files test passes cleanly under
suite load (run inside the full sequence, not standalone).

### Slice 3 — harness corrections

**02-finder.sh:** add `sleep 2` at phase start; filter Quick Look find
to exclude dotfiles; wrap small-file create loop in `set +e` so a single
ENOTDIR doesn't kill the phase.
**07-failure.sh:** verify and fix `/offline` POST contract; add a
fallback `curl -G --data` form if POST-with-body isn't accepted.

### Slice 4 — re-run suite, verify

Build app, restart JM, launch `scripts/qa-suite/run-all.sh`. Expectations:
- 04-fio: 8/0 (was 0/8) — confirms QA-19 fix
- 02-finder: full 12+ pass — confirms QA-18 fix + harness updates
- 07-failure: full 9+ pass — confirms /offline contract fix
- 09-endurance: still clean (no leak regression)

### Slice 5 — write final STATE.md entries with ✓ closure

If all green, mark QA-18 and QA-19 ✓ CLOSED with adjacent-scenarios
matrix per Rule 3. If QA-18 still flakes (LRU pressure tests don't
fully exercise it), document the matrix and move it to "soak test
required" rather than full close.

### Loop prompt (paste back to spawn the work)

```
/loop QA-18/19 fix loop — implement and validate per
docs/ROADMAP/qa-18-19-investigation.md. Order: slice 1 (QA-19), slice 2
(QA-18), slice 3 (harness), slice 4 (suite re-run), slice 5 (STATE.md
closure). Read the investigation doc at session start.

Per-iteration checklist:
  1. Pick the next slice from the plan.
  2. RULE 4 if touching concurrency / cache state: spawn code-reviewer
     with the explicit concurrency-audit prompt from the doc.
  3. Build: `go vet ./... && go build ./... && scripts/build-app.sh`.
  4. Restart JM via `open -n /path/to/JuiceMount.app`; wait for
     /health green via scripts/qa-suite/lib.sh helpers.
  5. Run validation per the slice's acceptance criterion.
  6. Update docs/STATE.md per Rule 3.
  7. Commit with specific paths (NEVER `git add -A`).

STOP conditions:
  - All 5 slices done with green tests.
  - OR three consecutive iterations produce no shippable progress.
  - OR user explicitly halts.

On stop: PushNotification with QA-18/19 status + final suite run
verdict.
```

---

## References

- `nfs/handler.go:362-419` — ToHandle / FromHandle / inode-as-handle scheme
- `nfs/handler.go:475-635` — Stat path with phantom-purge logic
- `metadata/store.go:153-220` — Insert / InsertToCache / DeleteFromCache
- `metadata/store.go:279-320` — BulkInsert (drives evictOldest)
- `metadata/redis.go:682-810` — syncMetadata (drives pruneAbsent)
- `scripts/qa-suite/04-fio.sh` — QA-19 reproducer
- `scripts/qa-suite/02-finder.sh:155-165` — QA-18 reproducer
- Run #3 artifacts: `/tmp/jm-qa-artifacts/20260517-173656/`

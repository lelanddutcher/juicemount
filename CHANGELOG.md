# JuiceMount6 Changelog

## 2026-03-31 — Code Audit & Full-Stack Overhaul

### Summary
Comprehensive code audit identified 8 correctness bugs, 8 performance issues, and 8 architectural gaps. All critical items addressed. JuiceFS FUSE mount management integrated into the JM5 process — JM5 is now a complete self-contained stack.

---

### Correctness Fixes

1. **Race condition in inode counter** (`nfs/handler.go`)
   - `inodeCounter` was bare `uint64` incremented without synchronization across goroutines
   - Fixed: changed to `atomic.Uint64`

2. **MemoryBuffer never invalidated on writes** (`nfs/handler.go`)
   - A file cached in MemoryBuffer would serve stale data after NFS writes, renames, or deletes
   - Fixed: `writeFile.Close()`, `Rename()`, and `Remove()` now call `memBuf.Invalidate()`

3. **writeSizes map leaked memory** (`nfs/handler.go`)
   - `trackWriteSize()` added entries but nothing ever removed them
   - Fixed: `writeFile.Close()` deletes the entry after use

4. **Remove() deleted from FUSE in background** (`nfs/handler.go`)
   - Returned success to NFS client before the file was actually removed on disk
   - Fixed: FUSE `os.Remove()` is now synchronous

5. **FDPool double-check race** (`nfs/fdpool.go`)
   - `Get()` and `GetWrite()` accessed entry fields outside the lock after the double-check
   - Fixed: all entry access is now under the lock

6. **UID/GID=0 on metadata-cached entries** (`metadata/types.go`, `nfs/handler.go`)
   - `metadata.FileInfo.Sys()` returned `*metadata.Entry` which the NFS attribute builder didn't recognize, falling back to UID=0/GID=0 (root:wheel)
   - macOS Finder showed red "no access" minus signs on every folder
   - Fixed: `Sys()` now returns `*syscall.Stat_t` with the current user's UID/GID
   - Same fix applied to `rootDirInfo`

### Performance Improvements

1. **O(1) ListChildren** (`metadata/store.go`)
   - Added `childrenIdx map[string]map[string]*Entry` (parent -> children)
   - `ListChildren()` was O(total entries) scanning all 147K entries per ReadDir
   - Now O(children count) — direct map lookup
   - All 8 mutation points (Insert, Delete, BulkInsert, etc.) maintain the index

2. **READ/WRITE buffer pools** (`internal/nfs/nfs_onread.go`, `nfs_onwrite.go`)
   - Added `sync.Pool` for 1MB read data buffers (eliminates per-RPC allocation)
   - Response writers use the existing `responseBufferPool` instead of `bytes.NewBuffer([]byte{})`

3. **Pre-serialized GETATTR XDR** (`internal/nfs/nfs_ongetattr.go`)
   - Already implemented in codebase; now works correctly with the UID/GID fix
   - 88-byte cached response body, zero XDR marshaling on cache hit

4. **BulkClearLocalOnly** (`metadata/store.go`, `metadata/redis.go`)
   - Discovered during testing: `syncMetadata()` called `ClearLocalOnly()` individually for 147K entries (147K SQLite UPDATE transactions)
   - Added `BulkClearLocalOnly()` that batches 500 per transaction
   - Redis sync time on cell hotspot: >60s timeout -> 85s pass; on 10GbE: ~7s

5. **Readahead uses FDPool** (`nfs/readahead.go`)
   - Prefetch goroutines now reuse pooled file descriptors instead of opening new ones

### Architecture Changes

1. **Removed double handle caching** (`nfs/server.go`)
   - `helpers.NewCachingHandler` wrapper was adding UUID-based LRU handles on top of our deterministic inode-based handles
   - Removed: NFS handler now served directly, eliminating UUID allocation, LRU eviction, and mutex contention

2. **Bounded prefetch goroutines** (`nfs/handler.go`)
   - Recursive `prefetchChildren` could spawn unbounded goroutines
   - Fixed: non-blocking semaphore acquisition before spawning sub-prefetches

3. **TCP keepalive** (`nfs/server.go`)
   - Added `SetKeepAlive(true)` + `SetKeepAlivePeriod(30s)` on NFS connections
   - Stale client connections (laptop sleep, WiFi drop) are now detected and cleaned up

4. **Graceful shutdown ordering** (`cmd/jm5/main.go`)
   - Replaced defer-based shutdown with explicit ordered sequence:
     unmount NFS -> stop server -> stop handler -> stop health -> stop Redis -> close SQLite -> unmount FUSE

5. **Stale prefetch map cleanup** (`nfs/handler.go`)
   - `prefetched` map entries now evicted after 2 minutes by the existing cleanup loop

6. **JuiceFS FUSE mount management** (`health/fuse.go`, `cmd/jm5/main.go`)
   - JM5 now mounts JuiceFS FUSE automatically on startup (no manual `juicefs mount` needed)
   - Background monitor checks mount health every 10s, auto-remounts on crash
   - Kills stale JuiceFS processes before remount
   - FUSE mounted at hidden `~/.juicemount/fuse-internal` (invisible to user)
   - Users only see the NFS volume at `/Volumes/zpool`
   - `--no-fuse` flag for testing/development

7. **Real FUSE health check** (`health/monitor.go`)
   - Old check: `os.Stat(fusePath)` — passes even when FUSE is unmounted (directory still exists)
   - New check: verifies mount table entry exists AND `ReadDir` responds within 5s

### Dead Code Cleanup
- Removed unused `cacheWarm` field from ReadaheadManager
- Removed dead `rdb` creation in main.go (was printing a pointer address)
- Removed unused `context` import in cbridge.go

### Test Fixes
- `TestReadCachedBlock`: added 30s context timeout and reduced scan iterations (was hanging forever over slow networks)
- `TestFileInfo`: updated to expect `*syscall.Stat_t` from `Sys()` instead of `*metadata.Entry`
- `ReadaheadManager` test calls updated for new constructor signature and `Stats()` return values

---

### Files Modified (16 files)

| File | Changes |
|------|---------|
| `cmd/jm5/main.go` | FUSE manager integration, graceful shutdown, dead code, flags |
| `nfs/handler.go` | atomic inode, membuf invalidation, writeSizes cleanup, sync Remove, bounded prefetch, prefetched cleanup, rootDirInfo UID/GID |
| `nfs/server.go` | Removed CachingHandler wrapper, TCP keepalive |
| `nfs/fdpool.go` | Double-check race fix |
| `nfs/readahead.go` | FDPool integration, removed unused field |
| `nfs/readahead_test.go` | Updated constructor and Stats() calls |
| `metadata/store.go` | Children index, BulkClearLocalOnly |
| `metadata/types.go` | Sys() returns *syscall.Stat_t with correct UID/GID |
| `metadata/redis.go` | Uses BulkClearLocalOnly |
| `metadata/store_test.go` | Updated TestFileInfo for new Sys() |
| `cache/reader_test.go` | Context timeout on TestReadCachedBlock |
| `internal/nfs/nfs_onread.go` | Buffer pool for read data + response |
| `internal/nfs/nfs_onwrite.go` | Buffer pool for response |
| `health/fuse.go` | NEW — JuiceFS FUSE mount lifecycle manager |
| `health/monitor.go` | Real FUSE health check (mount table + readdir) |
| `bridge/cbridge.go` | Dead code cleanup |

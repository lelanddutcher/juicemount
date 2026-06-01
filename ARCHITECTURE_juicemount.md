# JuiceMount Architecture Document

**Last updated:** 2026-05-28 (production-hardening branch; write spool added — see § 15)
**Repo:** `github.com/lelanddutcher/juicemount` (private)
**Module:** `github.com/lelanddutcher/juicemount`
**Go version:** 1.26.1
**Active branch:** `production-hardening`

---

## 1. System Overview

JuiceMount5 (JM5) is a userspace NFS v3 loopback server for macOS that presents a
JuiceFS FUSE volume to Finder with near-local performance. The core problem it
solves: macOS Finder issues tens of thousands of `stat()` / `LOOKUP` / `GETATTR`
calls when opening a directory, and each one through JuiceFS FUSE costs 1-60 ms
due to the kernel/userspace boundary crossings. JM5 interposes a Go-based NFS
server on `127.0.0.1:11049` that serves metadata from a local SQLite cache and
proxies file I/O to the hidden JuiceFS FUSE mount only when necessary.

The result: directory opens that took 3-10 seconds through raw FUSE now complete
in 15-120 ms via the NFS loopback.

### What it is

- A Go NFS v3 server (fork of `willscott/go-nfs`) listening on localhost
- macOS `mount_nfs` mounts it at `/Volumes/zpool`
- All metadata served from SQLite + in-memory maps (sub-millisecond)
- File data flows through JuiceFS FUSE, optionally bypassed by direct SSD cache reads
- Redis pub/sub keeps metadata in sync across machines

### What it is NOT

- Not a full NFS server -- no AUTH, no locking (uses `nolocks,locallocks`)
- Not a replacement for JuiceFS -- JuiceFS does the actual cloud storage
- Not multi-tenant -- single user, single mount, localhost only

---

## 2. Architecture Diagram

```
+--------------------------------------------------------------------+
|  macOS Finder / Applications (Final Cut, Premiere, DaVinci, etc.)  |
+----------------------------+---------------------------------------+
                             |
                     NFS v3 over TCP
                     127.0.0.1:11049
                     (TCP_NODELAY, 4MB socket buffers)
                             |
+----------------------------v---------------------------------------+
|                     JuiceMount5 NFS Server                         |
|                     (cmd/jm5/main.go)                              |
|                                                                    |
|  +------------------+   +-------------------+   +---------------+  |
|  | NFS Handler      |   | Metadata Store    |   | SSD Cache     |  |
|  | (nfs/handler.go) |   | (metadata/)       |   | Reader        |  |
|  |                  |   |                   |   | (cache/)      |  |
|  | - Stat/Lookup    |   | SQLite DB (WAL)   |   |               |  |
|  | - ReadDir        |   | +pathCache (map)  |   | Direct pread  |  |
|  | - Read/Write     |   | +inodeCache (map) |   | on SSD blocks |  |
|  | - Create/Rename  |   |                   |   | (bypasses     |  |
|  | - FD Pool        |   | ~147K entries     |   |  FUSE)        |  |
|  | - Readahead      |   +--------+----------+   +-------+-------+  |
|  | - MemoryBuffer   |            |                       |          |
|  +--------+---------+            |                       |          |
|           |                      |                       |          |
|           |  FUSE stat/read      |  Redis sync           |  Redis   |
|           |  (fallback only)     |                       |  chunk   |
|           v                      v                       v  mapping |
|  +--------+---------+   +-------+----------+   +--------+-------+  |
|  | JuiceFS FUSE     |   | Redis Client     |   | Redis          |  |
|  | mount            |   | (metadata/       |   | (chunk->slice  |  |
|  | ~/.juicemount/   |   |  redis.go)       |   |  lookups for   |  |
|  |  fuse-internal   |   |                  |   |  cache reader) |  |
|  +--------+---------+   | - SUBSCRIBE loop |   +----------------+  |
|           |              | - Reconcile loop |                       |
|           |              | - Lua tree pull  |                       |
|           v              +--------+---------+                       |
|  +--------+---------+            |                                  |
|  | Health Monitor   |            |                                  |
|  | + NetWatcher     |            |                                  |
|  | (health/)        |            |                                  |
|  +------------------+            |                                  |
+----------------------------------+----------------------------------+
                                   |
                    +--------------+--------------+
                    |                             |
           +-------v--------+          +---------v--------+
           | Redis Server   |          | MinIO Server     |
           | 127.0.0.1  |          | 127.0.0.1    |
           | :6379 (DB 1)   |          | :9000            |
           |                |          |                  |
           | JuiceFS meta   |          | JuiceFS data     |
           | d* keys (dirs) |          | bucket: "zpool"  |
           | i* keys (attrs)|          |                  |
           | c* keys (chunks|          |                  |
           +----------------+          +------------------+
```

---

## 3. Go Packages and Responsibilities

### `cmd/jm5/` -- CLI entry point
**File:** `cmd/jm5/main.go`

Startup sequence:
0. Mounts JuiceFS FUSE at `~/.juicemount/fuse-internal` (hidden from user) via `health.FUSEManager`; starts background FUSE health monitor (10s tick, auto-remount on crash)
1. Opens SQLite metadata store (`metadata.Open`)
2. Connects to Redis, runs initial Lua-based metadata sync (`rc.SyncOnce`)
3. Starts background Redis SUBSCRIBE + reconciliation loops (`rc.Start`)
4. Detects and initializes SSD cache reader (`cache.DetectCacheDir`)
5. Creates and starts the NFS server (`nfs.NewServer`, `srv.Start`)
6. Attaches cache reader and Redis client to the NFS handler
7. Starts NetWatcher (1s poll interval) with Redis reconnect callback
8. Starts HealthMonitor (10s tick) for Redis, MinIO, FUSE checks
9. Mounts NFS via `sudo mount_nfs` at `/Volumes/zpool` (the only user-visible volume)
10. Waits for SIGINT/SIGTERM

Shutdown sequence (explicit, ordered):
1. Unmount NFS (`/Volumes/zpool`)
2. Stop NFS server (close listener)
3. Stop handler resources (fd pool, membuf, readahead)
4. Stop network watcher + health monitor
5. Stop cache reader
6. Stop Redis client
7. Close SQLite
8. Unmount JuiceFS FUSE

**Mount options:**
```
port=11049,mountport=11049,soft,intr,timeo=300,retrans=5,
nolocks,locallocks,rsize=1048576,wsize=1048576,readahead=128,
actimeo=3600,vers=3,tcp
```

Key flags: `rsize/wsize=1MB` for throughput, `actimeo=3600` to maximize macOS
attribute caching (1 hour), `soft,intr` so hung NFS doesn't freeze Finder.

### `nfs/` -- NFS handler and file I/O
**Files:** `handler.go`, `server.go`, `fdpool.go`, `readahead.go`, `membuf.go`,
plus the write-spool files (`spool.go`, `spool_index.go`, `spool_writefile.go`,
`spool_readfile.go`, `spool_status.go`, `spool_manifest.go`, `drainer.go`) — see § 15.

This is the core package. It implements the `go-nfs` Handler interface:

- **`JuiceMountHandler`** -- Top-level handler. Implements `Mount()`,
  `FromHandle()`, `ToHandle()`, `FSStat()`, `Change()`, `VerifierFor()`,
  `DataForVerifier()`, `HandleLimit()`.

- **`juiceFS`** -- Implements `billy.Filesystem`. All NFS file operations
  (Stat, ReadDir, Open, Create, Rename, Remove, MkdirAll) go through this.
  Metadata operations hit SQLite/pathCache first; FUSE is a fallback only.

- **`juiceChange`** -- Implements `billy.Change`. All attribute-change ops
  (Chmod, Chown, Chtimes) are no-ops since JuiceFS handles permissions.

- **`cachedFile`** -- Read-path file wrapper with three-tier priority:
  1. MemoryBuffer (zero-syscall, heap-resident for files <128MB)
  2. SSD cache reader (direct pread, bypasses FUSE)
  3. JuiceFS FUSE pread (fallback, populates SSD cache)

- **`writeFile`** -- Write-path file wrapper. Tracks `writtenEnd` for size
  reporting, publishes metadata events on close, skips `Sync()` to rely on
  JuiceFS writeback. The default path when the spool is disabled.

- **`spoolWriteFile` / `spoolReadFile`** -- Spool-routed file wrappers used
  when the write spool is enabled (`JM_SPOOL_ENABLE=1`). `O_CREATE` writes land
  on a local-SSD spool file and ack immediately; reads of a not-yet-drained
  file are served from the spool. nil-spool falls back to the `writeFile` /
  `cachedFile` paths above. See § 15.

- **`FDPool`** -- Pools open file descriptors to avoid the 60ms cost of
  opening files on JuiceFS FUSE. Entries auto-evict after 2 minutes idle.
  Supports both read (`Get`) and write (`GetWrite`) fd acquisition.

- **`ReadaheadManager`** -- Detects sequential read patterns (3 consecutive
  sequential reads on the same inode) and prefetches 32MB ahead (8 x 4MB
  blocks) in background goroutines (max 4 concurrent). Warms the JuiceFS
  SSD cache for upcoming reads.

- **`MemoryBuffer`** -- Caches entire files <128MB in Go heap for
  zero-syscall reads. 2GB total budget. Good for .prproj, .cube, .xmp,
  .fcpxml files. Async loading on first access; evicts after 2 min idle.

- **`Server`** -- TCP listener wrapper. Sets TCP_NODELAY + TCP keepalive
  (30s) on accept, sets 4MB SO_SNDBUF/SO_RCVBUF. Handler is served
  directly (no CachingHandler wrapper -- JuiceMountHandler implements
  deterministic inode-based handles and READDIRPLUS verifiers natively).

### `metadata/` -- SQLite store and Redis sync
**Files:** `store.go`, `redis.go`, `types.go`

- **`Store`** -- SQLite-backed metadata store with triple in-memory caches:
  `pathCache map[string]*Entry`, `inodeCache map[uint64]*Entry`, and
  `childrenIdx map[string]map[string]*Entry` (parent -> children).
  All lookups (`LookupByPath`, `LookupByInode`, `ListChildren`) hit the
  in-memory maps -- never blocked by SQLite transactions.
  `ListChildren` is O(children) via the childrenIdx, not O(total entries).

  SQLite schema:
  ```sql
  CREATE TABLE entries (
      path        TEXT PRIMARY KEY,
      name        TEXT NOT NULL,
      parent_path TEXT NOT NULL,
      is_dir      INTEGER NOT NULL,
      size        INTEGER NOT NULL,
      mtime       INTEGER NOT NULL,
      inode       INTEGER NOT NULL,
      mode        INTEGER NOT NULL,
      local_only  INTEGER NOT NULL DEFAULT 0
  );
  CREATE INDEX idx_parent ON entries(parent_path);
  CREATE INDEX idx_inode ON entries(inode);
  CREATE INDEX idx_name ON entries(name COLLATE NOCASE);
  ```

  Pragmas: `journal_mode=WAL`, `synchronous=NORMAL`, `cache_size=-8000`,
  `busy_timeout=30000`. MaxOpenConns=8.

  Key methods:
  - `InsertToCache` / `DeleteFromCache` -- in-memory only, instant, never
    blocked. Used by rename/mkdir/SUBSCRIBE to provide immediate NFS visibility.
  - `BulkInsert(entries, batchSize)` -- batched SQLite writes. Uses 500-entry
    batches to limit write-lock hold time.
  - `ListChildren` -- reads from pathCache (iterates map), not SQLite.

- **`RedisClient`** -- Two-mechanism sync:
  1. **SUBSCRIBE** (`juicemount:metadata` channel) -- <100ms propagation for
     real-time create/update/delete/rename events between JuiceMount clients.
  2. **Batch reconciliation** -- Periodic (30s default) full tree pull via Lua
     script, catches anything SUBSCRIBE missed.

  The Lua script (`luaScript`) SCANs all `d*` keys (JuiceFS directory entries),
  builds an inode-to-path reverse map, resolves full paths, and fetches inode
  attributes (`i{inode}`) for mtime and size. Returns entries as
  `"fileType:mtime:fileSize:inode:relative/path"` strings.

  Reconciliation includes: bulk insert of all Redis entries (500/batch),
  clearing `local_only` flags, pruning entries that no longer exist in Redis.

  Reconnect: exponential backoff on failure (30s -> 1m -> 2m -> 5m max).
  `TriggerSync()` for immediate sync after network changes.

- **`Entry`** -- Metadata struct with `PreSerializedGetAttr []byte` field for
  caching the XDR-encoded GETATTR response (88 bytes). Invalidated when size
  or mtime changes.

- **`MetadataEvent`** -- JSON struct for Redis pub/sub: `{op, path, size, mtime, inode, is_dir, old_path}`.

### `cache/` -- SSD direct cache reader
**File:** `reader.go`

Reads JuiceFS cache blocks directly from the local SSD, bypassing the FUSE
mount entirely for cached data. Flow:

1. Given an inode and file offset, compute chunk index and offset within chunk
2. Fetch chunk-to-slice mapping from Redis (`c{inode}_{chunkIndex}` list) --
   cached locally in `sliceCache`
3. Find which slice covers the offset, compute block index within slice
4. Build cache file path: `~/.juicefs/cache/{uuid}/raw/chunks/0/{sliceID/1000}/{sliceID}_{blockIndex}_{blockSize}`
5. Direct `pread()` from the cache block file

Block size: 4MB (matches JuiceFS default). Chunk size: 64MB.
FDs are pooled with 5-minute TTL. Slice cache is invalidated on file modification.

Auto-detection: `DetectCacheDir()` scans `~/.juicefs/cache/*/raw/chunks/` for
valid cache directories. Requires JuiceFS 1.3.x.

### `health/` -- Health monitoring and network detection
**Files:** `monitor.go`, `netwatch.go`

- **`HealthMonitor`** -- Checks Redis (PING), MinIO (`/minio/health/live`),
  FUSE (stat), NFS (stat mount point) every 10 seconds. Tracks connection
  state transitions (connected/disconnected/reconnecting). Suppresses FUSE
  errors during network grace period (5s after interface change).

- **`NetWatcher`** -- Polls system network interfaces every 1 second. Detects
  when the "active" interface changes (e.g., WiFi to 10GbE to Tailscale).
  Priority: en1-en9 (Ethernet) > en0 (WiFi) > utun* (Tailscale) > bridge > other.
  On change, fires callbacks -- the main callback triggers Redis reconnect
  and immediate metadata sync.

### `bridge/` -- C bridge for Swift integration
**File:** `bridge/cbridge.go`

CGo-based bridge that compiles to a C archive (`.a`) for embedding in a macOS
Swift app. Exports:

- `NFSServerStart(configJSON)` -- starts the full stack from a JSON config
- `NFSServerStop()` -- graceful shutdown
- `NFSServerIsRunning()` -- health check
- `NFSServerStats()` -- JSON stats (entry count, sync timing, health)
- `NFSServerSyncNow()` -- triggers immediate reconciliation
- `NFSServerFreeString(s)` -- frees C strings allocated by Go

The Swift app passes config as JSON: `{redis_url, fuse_path, mount_point, listen_addr, db_path}`.

### `internal/nfs/` -- Forked go-nfs library
**Files:** `conn.go`, `file.go`, `nfs_onwrite.go`, `nfs_ongetattr.go`, `nfs_oncreate.go`, `nfs_onreaddirplus.go`

This is a vendored fork of `willscott/go-nfs` with JM5-specific optimizations
(all marked with `[JM5]` comments):

- **`conn.go`** -- Connection handling:
  - `responseBufferPool` (sync.Pool) eliminates per-RPC buffer allocations
  - TCP_NODELAY set on each connection
  - 64-slot write serializer channel (up from 1) for response batching
  - 1MB buffered writer matching NFS rsize
  - Batch-drain: collects all queued responses before TCP flush
  - Sequential RPC dispatch (macOS NFS client doesn't pipeline)
  - RPC performance counters (total, slow >5ms, very slow >50ms)

- **`nfs_ongetattr.go`** -- GETATTR handler:
  - Fast path: if Entry has `PreSerializedGetAttr` bytes, writes them directly
    (skips all XDR reflection/encoding)
  - Slow path: computes FileAttribute, encodes via XDR using pooled buffer,
    then caches the 88-byte result on the Entry for next time

- **`nfs_onwrite.go`** -- WRITE handler:
  - Skips pre-op stat (WCC pre_op_attr is optional per RFC 1813 section 2.6)
  - Always reports `unstable` stability regardless of client request
  - Post-op stat uses only post_op_attr (no full WCC)

- **`nfs_oncreate.go`** -- CREATE handler:
  - Handles exclusive create mode (used by macOS for atomic creates) by treating
    it as unchecked create. Reads and discards the createverf3 verifier.
    This eliminates the "exclusive mode not supported" error that caused macOS
    to retry with unchecked mode, adding latency.

- **`nfs_onreaddirplus.go`** -- READDIRPLUS handler:
  - Standard implementation with cookie-based pagination
  - Entity limit capped at `HandleLimit() / 2` entries per response

- **`file.go`** -- FileAttribute and helper types:
  - `PreSerializeGetAttrBody()` -- Hand-coded big-endian encoding of the
    88-byte GETATTR response (NFSStatusOk + fattr3) without any XDR library.
  - `ToFileAttribute()` -- Converts `os.FileInfo` to NFS fattr3.
  - WCC helpers (`WriteWcc`, `WritePostOpAttrs`)

---

## 4. Key Data Flows

### Read Path (NFS READ RPC)

```
Finder READ(handle, offset, count)
  |
  v
FromHandle(handle) --> LookupByInode(inode) --> Entry with path
  |
  v
OpenFile(path, O_RDONLY)
  |
  +--> FDPool.Get(fusePath)        // reuse pooled fd (avoids 60ms open)
  |
  v
cachedFile.ReadAt(buf, offset)
  |
  +--> [1] MemoryBuffer.ReadAt()   // zero-syscall if file is buffered
  |    (files < 128MB, in Go heap)
  |
  +--> [2] CacheReader.ReadBlock() // direct SSD pread (bypasses FUSE)
  |    (chunk/slice lookup via Redis, then pread on cache block file)
  |
  +--> [3] fuseFD.ReadAt()         // JuiceFS FUSE read (populates SSD cache)
  |
  v
ReadaheadManager.OnRead()          // track sequential pattern, may trigger prefetch
  |
  v
NFS response (data + post_op_attr)
```

### Write Path (NFS WRITE RPC)

> **Note (2026-05-28):** with the write spool enabled (`JM_SPOOL_ENABLE=1`),
> `O_CREATE` writes are intercepted *before* this path and routed to a
> local-SSD spool that acks immediately; a background drainer later performs
> the FUSE write shown below. See § 15. The flow below is the spool-disabled
> (default) path — and is also exactly what the drainer replays into FUSE.

```
Finder WRITE(handle, offset, data, stability)
  |
  v
FromHandle(handle) --> Entry with path
  |
  v
OpenFile(path, O_RDWR) via FDPool.GetWrite()  // reuse pooled write fd
  |
  v
Seek to offset, Write data
  |
  v
writeFile.Write() --> trackWriteSize(path, newEnd)  // immediate size tracking
  |
  v
writeFile.Close()
  +--> FDPool.Release() (fd stays open for reuse, NO Sync)
  +--> store.UpdateSize(path, finalSize, now)
  +--> publishEvent({op:"create", path, size, mtime, inode})
  |
  v
NFS response:
  - status = OK
  - WCC: pre_op_attr=nil, post_op_attr from Stat
  - stability = unstable (always, regardless of client request)
  - verifier = server ID
```

### Stat / GETATTR Path

```
Finder GETATTR(handle)
  |
  v
FromHandle(handle) --> LookupByInode(inode) --> Entry
  |
  v
Lstat(path) --> juiceFS.Stat(path)
  |
  +--> Fast rejection: ._*, .DS_Store, .Spotlight-V100, etc. --> ErrNotExist
  |
  +--> Check writeSizes map for in-flight write size override
  |
  +--> LookupByPath(path) --> pathCache hit?
  |    YES: return Entry.FileInfo() (with write size override if larger)
  |    NO:  fall through to FUSE
  |
  +--> os.Stat(fusePath)  // FUSE fallback
  |    Cache result into SQLite (async)
  |
  v
onGetAttr():
  +--> Fast path: Entry.PreSerializedGetAttr != nil?
  |    YES: w.Write(preSerializedBytes) -- 88 bytes, zero XDR encoding
  |    NO:  Encode FileAttribute via XDR, cache bytes on Entry
```

### LOOKUP Path

```
Finder LOOKUP(dir_handle, name)
  |
  v
FromHandle(dir_handle) --> parent path
  |
  v
Stat(parent + "/" + name)  --> same flow as GETATTR above
  |
  v
ToHandle(fs, childPath) --> 8-byte handle from inode
  |
  v
NFS response (handle + post_op_attr)
```

### Directory Listing (READDIRPLUS)

```
Finder READDIRPLUS(dir_handle, cookie, verifier, dirCount, maxCount)
  |
  v
FromHandle(dir_handle) --> dir path
  |
  v
ReadDir(dirname) on juiceFS:
  |
  +--> ListChildren(dirname) from pathCache
  |    Has entries? YES: return cached + trigger background prefetch
  |    NO: fall through to FUSE
  |
  +--> os.ReadDir(fusePath)  // FUSE fallback
  |    BulkInsert all entries into SQLite (synchronous, 500/batch)
  |    Return entries
  |
  v
prefetchChildren(dirname) -- background goroutine:
  +--> os.ReadDir(fusePath) for this directory
  +--> BulkInsert new entries (skip already-cached)
  +--> For each subdirectory, recursively prefetch (one level deep)
  +--> Max 4 concurrent prefetch goroutines via semaphore
  +--> Debounce: skip if prefetched within 30 seconds
  |
  v
READDIRPLUS response:
  - "." and ".." entries with attributes
  - Each child: fileID, name, cookie, fattr3, NFS handle
  - EOF flag
  - Paginated by cookie + maxCount
```

### Rename / Create

```
Rename(oldpath, newpath):
  1. os.Rename(fusePath/old, fusePath/new)     // FUSE rename
  2. DeleteFromCache(oldpath)                   // immediate in-memory removal
  3. InsertToCache(newEntry)                    // immediate in-memory insert
  4. async: store.Delete(old) + store.Insert(new)  // SQLite (may be slow)
  5. publishEvent({op:"rename", path:new, old_path:old})

Create(filename):
  1. os.Create(fusePath/filename)               // FUSE create
  2. store.Insert(entry with local_only=true)    // SQLite + cache immediately
  3. Return writeFile wrapper

MkdirAll(dirname):
  1. os.MkdirAll(fusePath/dirname)              // FUSE mkdir
  2. InsertToCache(entry with local_only=true)   // immediate in-memory
  3. async: store.Insert(entry)                  // SQLite write
  4. publishEvent({op:"create", path:dirname, is_dir:true})
```

---

## 5. Performance Optimizations

### 5.1 SQLite Metadata Cache (~147K entries, in-memory pathCache)

All NFS metadata operations (GETATTR, LOOKUP, READDIR) read from in-memory
Go maps (`pathCache`, `inodeCache`) -- never from SQLite queries. SQLite is
only used for persistence across restarts and as the write-behind store.

Dual cache architecture:
- `pathCache map[string]*Entry` -- keyed by relative path (e.g., "Film Projects/GMTM/clip.mov")
- `inodeCache map[uint64]*Entry` -- keyed by JuiceFS inode number

On startup, `rebuildCaches()` loads all SQLite rows into both maps. All
subsequent operations update both maps in-memory first, then write to SQLite
(often asynchronously).

### 5.2 SSD Direct Cache Reader (bypasses FUSE for cached blocks)

The `cache.Reader` reads JuiceFS's local SSD cache blocks directly via
`pread()` system calls, completely bypassing the FUSE kernel/userspace
boundary. This eliminates two context switches per read for cached data.

Flow: inode + offset -> chunk index -> Redis lookup for slice mapping ->
compute block file path -> direct `pread()` on SSD.

Slice mapping is cached locally (`sliceCache map[string][]SliceInfo`).
File descriptors are pooled with 5-minute TTL.

### 5.3 FD Pool (reuses file descriptors)

Opening a file on JuiceFS FUSE costs ~60ms. The `FDPool` keeps file
descriptors open and reuses them across NFS RPCs. This is critical because
`go-nfs` calls `OpenFile -> Write -> Close` on every WRITE RPC.

- 2-minute idle timeout before eviction
- 30-second eviction check interval
- Separate `Get` (read) and `GetWrite` (write with flags) methods
- Reference counting prevents closing fds with active users

### 5.4 Pre-serialized GETATTR Responses

GETATTR is the hottest NFS RPC (Finder calls it constantly). The first
GETATTR for an entry goes through normal XDR encoding. The resulting 88-byte
response body (`NFSStatusOk` + `fattr3`) is cached on the `Entry` struct as
`PreSerializedGetAttr`. Subsequent GETATTRs for the same entry just copy
these 88 bytes -- zero reflection, zero XDR encoding.

Invalidation: `PreSerializedGetAttr` is set to nil when `UpdateSize()` is
called (size or mtime changed).

### 5.5 Response Buffer Pool

`sync.Pool` of `bytes.Buffer` objects (initial cap 4KB) used for every NFS
RPC response. Eliminates one allocation + GC pressure per RPC. Buffers
larger than 64KB are not returned to the pool to avoid memory bloat from
large READ responses.

### 5.6 TCP_NODELAY

Set on both the listener (via `noDelayListener`) and each accepted connection.
Without it, Nagle's algorithm adds ~40ms delay on small NFS responses like
GETATTR. Socket buffers set to 4MB (`SO_SNDBUF`, `SO_RCVBUF`).

The `serializeWrites` goroutine handles coalescing: it batch-drains all
queued responses before flushing the 1MB buffered writer, reducing the
number of TCP write syscalls.

### 5.7 Fast `._*` Rejection

macOS Finder probes for resource forks (`._filename`), `.DS_Store`,
`.Spotlight-V100`, `.Trashes`, `.fseventsd`, `.TemporaryItems`,
`.VolumeIcon.icns`, and `Icon\r` on every directory open. These never exist
on JuiceFS. The Stat handler returns `os.ErrNotExist` immediately for these
patterns, eliminating ~1/3 of all LOOKUPs from hitting FUSE.

### 5.8 READDIR-Triggered Prefetch

When `ReadDir` returns entries from cache, a background goroutine
(`prefetchChildren`) scans the directory on FUSE and inserts any uncached
children into SQLite. It also recursively prefetches one level of
subdirectories for Finder's "expanding disclosure triangle" navigation
pattern.

- Max 4 concurrent prefetch goroutines (semaphore)
- 30-second debounce per directory
- 500-entry BulkInsert batches

### 5.9 Write Optimizations

- **No Sync on Close:** `writeFile.Close()` does NOT call `fsync()`. The
  go-nfs library calls `OpenFile+Write+Close` on every WRITE RPC, so syncing
  would flush to MinIO on every RPC. Instead, relies on JuiceFS's writeback
  buffer.
- **Unstable stability:** Always reports `unstable` write stability regardless
  of client request, avoiding forced flushes.
- **WCC skip:** WRITE handler skips pre-op stat (only sends post_op_attr).
  WCC pre_op_attr is optional per RFC 1813 section 2.6.
- **FD pool for writes:** Same file descriptor reused across WRITE RPCs for
  the same file.
- **Size tracking:** `writeSizes map[string]int64` tracks the latest written
  position so that GETATTR/STAT during a write returns the correct file size
  without waiting for FUSE to propagate.

### 5.10 Readahead for Sequential Reads

`ReadaheadManager` detects sequential read patterns (3 consecutive sequential
reads on the same inode) and triggers background prefetch:
- Prefetches 32MB ahead (8 x 4MB blocks)
- Reads go through JuiceFS FUSE, which populates the SSD cache
- Max 4 concurrent readahead goroutines
- Tracker entries expire after 30 seconds

### 5.11 Memory Buffer for Small Files

`MemoryBuffer` caches entire files in Go heap for zero-syscall reads:
- Threshold: files up to 128MB
- Total budget: 2GB
- Async loading on first access (caller uses FUSE for that read)
- Eviction: 2-minute idle TTL, 30-second eviction interval
- Good for: .prproj, .cube, .3dl, .xmp, .fcpxml
- Bad for: media files (too large, sequential one-pass reads)

### 5.12 Connection-Level Optimizations

- **Sequential RPC dispatch:** macOS NFS client does not pipeline RPCs on a
  single TCP connection, so concurrent dispatch adds overhead with no benefit.
- **64-slot write channel:** Response channel buffered to 64 (up from 1) for
  batching during burst traffic.
- **1MB write buffer:** Buffered writer size matches NFS rsize for optimal
  TCP segment coalescing.

---

## 6. Connection Resilience

### NetWatcher

Polls system network interfaces every 1 second. Detects when the "active"
interface changes (e.g., switching from WiFi to 10GbE Ethernet, or losing
connectivity). Priority ordering: Ethernet (en1-9) > WiFi (en0) >
Tailscale (utun*) > Bridge > Other.

On interface change:
1. Fires registered callbacks
2. Main callback: `rc.Reconnect()` -> closes old Redis client, creates new one
3. On successful reconnect: `rc.TriggerSync()` -> immediate reconciliation
4. On failed reconnect: reconcile loop retries with exponential backoff

### Redis Reconnect

`RedisClient.Reconnect()`:
1. Closes old `redis.Client` (ignores errors -- may already be broken)
2. Creates new client with same address
3. PINGs to verify connectivity
4. Updates connection state tracking

### Grace Periods

`HealthMonitor` has a 5-second grace period after network changes. During
this period, FUSE stat failures are suppressed (marked healthy with
"suppressed (network grace period)" message) to avoid false alarms while
connections re-establish.

### Reconciliation Backoff

On failed reconciliation:
- Base interval: 30 seconds
- Exponential backoff: 30s, 60s, 2m, 4m, 5m (max)
- On recovery: logs "reconciliation recovered after N failures", resets
  interval to 30s
- `syncNowCh` allows immediate sync triggering (e.g., after network change)

---

## 7. The SQLITE_BUSY Fix

### Problem

During batch reconciliation, `BulkInsert` held a write transaction that could
block for seconds (inserting ~147K entries). During that time, any NFS
operation that triggered a SQLite write (Rename, MkdirAll, SUBSCRIBE event
insert) would get `SQLITE_BUSY` errors, causing Finder operations to fail.

### Solution: Cache-First Writes

All mutating NFS operations (Rename, MkdirAll, Create) now follow the pattern:
1. **In-memory cache first** (`InsertToCache` / `DeleteFromCache`) -- instant,
   never blocked by SQLite
2. **FUSE operation** (the actual filesystem mutation)
3. **SQLite write async** (`go store.Insert(...)`) -- may be delayed if
   BulkInsert is running, but NFS operations work because pathCache is updated

### Smaller BulkInsert Batches

`BulkInsert` batch size reduced from unlimited to **500 entries per
transaction**. This means the write lock is held for ~10-50ms per batch
instead of seconds for the full 147K-entry insert. Between batches, other
SQLite writers can execute.

### Retry Logic

`RedisClient.retryInsert()` retries with exponential backoff:
- 5 attempts max
- Delays: 50ms, 100ms, 200ms, 400ms, 800ms
- Only retries on `SQLITE_BUSY` errors

### ListChildren from Cache

`Store.ListChildren()` iterates `pathCache` (in-memory map) instead of
querying SQLite. This means directory listings are never blocked by
concurrent BulkInsert transactions.

---

## 8. Redis Pub/Sub Architecture

### SUBSCRIBE for Real-Time Events

Channel: `juicemount:metadata`

Event format (JSON):
```json
{
  "op": "create|update|rename|delete",
  "path": "relative/path/to/file",
  "size": 12345,
  "mtime": 1711300000,
  "inode": 67890,
  "is_dir": false,
  "old_path": "old/path"
}
```

The `subscribeLoop` runs continuously, reconnecting with 2-second delay on
connection loss. Each received event is applied via `applyEvent()`:
- `create`/`update`: InsertToCache (immediate) + retryInsert to SQLite
- `delete`: DeleteFromCache (immediate) + Delete from SQLite
- `rename`: Delete old + InsertToCache new (immediate) + retryInsert

Events are published by JuiceMount NFS operations:
- `writeFile.Close()` -> create event
- `juiceFS.Rename()` -> rename event
- `juiceFS.Remove()` -> delete event
- `juiceFS.MkdirAll()` -> create event (is_dir=true)

### Lua Script for Batch Reconciliation

The Lua script runs server-side on Redis (atomic, no round-trips):

1. `SCAN` all `d*` keys (JuiceFS directory entries) with `COUNT 1000`
2. For each directory entry, extract child inode (9-byte value: 1 byte file
   type + 4 bytes padding + 4 bytes inode)
3. Build reverse map: `inode -> parent_inode + name`
4. Resolve full paths by walking the reverse map up to inode 1 (root)
5. For each resolved entry, `GET i{inode}` for attributes (mtime at offset 24,
   size at offset 52, both uint64 big-endian)
6. Return array of `"fileType:mtime:size:inode:path"` strings

The reconciliation then:
1. Parses all entries from Lua output
2. BulkInserts into SQLite (500/batch)
3. Clears `local_only` flags for entries now confirmed in Redis
4. Prunes entries that exist in SQLite but not in Redis (and aren't local_only)

---

## 9. Benchmark Suite

**File:** `test/benchmark_suite_test.go`
**Baselines:** `test/benchmark_baselines.json`

### What It Tests

| # | Benchmark | Description | Metric |
|---|-----------|-------------|--------|
| 1 | Finder Directory Open | ReadDir + Stat all entries for dirs of ~10, ~100, ~1000+ entries | ms (avg of 5 iterations) |
| 2 | Stat Latency | p50/p95/p99 of 200 stat calls on warm cache | ms |
| 3 | Sequential Read | Read a 10MB file start to finish (256KB buffer) | MB/s |
| 4 | Random Read / Scrubbing | 50 random 64KB reads at random offsets (simulates video scrubbing) | ms/read |
| 5 | Write Small Files | 50 x 1KB files (project file save pattern) | total ms |
| 6 | Write Large File | Single 10MB file (256KB write chunks) | total ms |
| 7 | Create + Rename | 20 mkdir+rename cycles with concurrent background FS activity (SQLITE_BUSY stress test) | ms/cycle |
| 8 | Deep Tree Walk | `filepath.Walk` to depth 4 under "Film Projects/GMTM" | total ms |
| 9 | Concurrent Reads | 4 goroutines each reading a 1MB file | total ms |
| 10 | Cold vs Warm | Same ReadDir+Stat twice (first = cold, second = warm) | ms each |

### Regression Detection

Threshold: 20% slower than baseline (ratio > 1.20 for latency metrics, or
ratio > 1.20 for throughput). Test fails if any benchmark regresses.

### How to Run

```bash
# Run the benchmark suite (requires NFS mount at /Volumes/zpool)
cd /Users/USER/Developer/JuiceMount5
go test -v -run TestBenchmarkSuite ./test/ -timeout 10m

# Update baselines after a performance improvement
UPDATE_BASELINES=1 go test -v -run TestBenchmarkSuite_UpdateBaselines ./test/ -timeout 10m
```

### Current Baselines (2026-03-25)

```json
{
  "cold_run_ms": 600,
  "concurrent_reads_4x_ms": 1000,
  "deep_tree_walk_ms": 2806.4,
  "finder_dir_open_10": 18.23,
  "finder_dir_open_100": 120.46,
  "finder_dir_open_1000": 2115.93,
  "random_read_64k_ms": 3.63,
  "sequential_read_mbps": 10.1,
  "stat_p50_ms": 0.393,
  "stat_p95_ms": 0.86,
  "stat_p99_ms": 1.299,
  "warm_run_ms": 550,
  "write_large_10m_ms": 39.4,
  "write_small_50x1k_ms": 452
}
```

---

## 10. Server Infrastructure

| Component | Address | Purpose |
|-----------|---------|---------|
| Redis | `127.0.0.1:6379` (DB 1) | JuiceFS metadata (d*, i*, c* keys) + pub/sub |
| MinIO | `127.0.0.1:9000` | JuiceFS data storage (S3-compatible) |
| JuiceFS volume | "zpool" | The JuiceFS filesystem |
| FUSE mount | `~/.juicemount/fuse-internal` | Hidden JuiceFS FUSE mount (not user-facing) |
| NFS server | `127.0.0.1:11049` | Loopback NFS server |
| NFS mount | `/Volumes/zpool` | User-facing mount point |
| SQLite DB | `~/.juicemount/metadata.db` | Local metadata cache |
| SSD cache | `~/.juicefs/cache/{uuid}/raw/chunks/` | JuiceFS local block cache |

---

## 11. Pin Store, Prefetcher, and Offline Mode (added 2026-05-08)

This section documents the offline-pinning subsystem layered on top of the original
JM5 architecture in §1–§10. It's the difference between "JuiceMount works on a wired
LAN" and "JuiceMount works on cellular while you're flying" — and it's where the
production-hardening branch concentrated its effort.

### 11.1 Components

```
                          User toggles offline
                                    │
                                    ▼
                          ┌─────────────────────┐
                          │  pin.SetOffline(b)  │ atomic.Int32, lock-free
                          └─────────────────────┘
                                    ▲
                                    │ read on every NFS OpenFile / ReadAt
                                    │
┌──────────────────────────┐   ┌────┴───────┐   ┌──────────────────┐
│  pin.Store (SQLite)      │←──│ NFS handler│   │  pin.Prefetcher  │
│  - 1 row per pinned path │   │ open-time  │   │  - 4 workers     │
│  - status: Pending /     │   │ + read-time│   │  - 1 MB chunks   │
│    Prefetching / Ready / │   │ gates      │   │  - PullPending   │
│    Failed / Unpinned     │   │            │   │  - ReWarmupLoop  │
│  - bytes_cached          │   │            │   │  - VerifyAndRepair│
│  - last_prefetched       │   │            │   │                  │
│  - pin_root              │   │            │   │                  │
└──────────────────────────┘   └────────────┘   └──────────────────┘
        ▲                                                  │
        │ UpdateStatus on each prefetch completion         │
        └──────────────────────────────────────────────────┘
                            ▲                 │
                            │                 │ os.Open + Read in 1 MB chunks
                            │                 ▼
                  ┌─────────┴──────────────────────────┐
                  │ JuiceFS FUSE (~/.juicemount/       │
                  │ fuse-internal/) — JuiceFS LRU      │
                  │ caches blocks at                   │
                  │ ~/.juicefs/cache/{uuid}/raw/chunks │
                  └────────────────────────────────────┘
```

### 11.2 Pin store (`internal/cache/pin/store.go`)

SQLite-backed registry, separate DB from the main metadata store (avoids WAL
contention).

| Status | Int | Meaning |
|--------|-----|---------|
| Unknown | 0 | Sentinel for invalid reads |
| Pending | 1 | User pinned, prefetcher hasn't picked it up |
| Prefetching | 2 | Worker is reading the file through FUSE |
| Ready | 3 | Bytes confirmed cached |
| Failed | 4 | Last prefetch attempt errored (last_error column has details) |
| Unpinned | 5 | User removed; eligible for eviction (currently unused — Unpin DELETEs the row instead) |

Key operations:
- `Pin(path, size, root)` — INSERT OR IGNORE; idempotent.
- `Unpin(path)` / `UnpinByRoot(root)` — DELETE.
- `IsPinnedReady(path)` — hot-path indexed lookup; returns true for `Ready` OR `Prefetching` with `bytes_cached >= size` (the "late-Ready window"). Called on every NFS OpenFile when offline mode is on.
- `Pending(limit)` — entries waiting for prefetch.
- `Stale(ttl, limit)` — Ready entries with `last_prefetched < now - ttl`. Used by ReWarmupLoop.
- `AllPinnedForRepair(limit)` — Ready + Prefetching + Failed entries. Used by VerifyAndRepair to recover from transient errors.

### 11.3 Prefetcher (`internal/cache/pin/prefetcher.go`)

Bounded worker pool; 4 workers by default. Each worker pulls from `chan jobReq`, opens the file via the FUSE mount, reads in 1 MB chunks discarding the bytes. The side-effect we want is the read traveling through JuiceFS's download + cache pipeline.

Three long-running daemons run in goroutines:
- `PullPending(ctx, batchSize)` — every 5 s, pulls up to `batchSize` Pending rows and Enqueues them. This is what drains the queue when VerifyAndRepair marks 300+ files Pending at once (avoids overflowing the 256-slot ring buffer).
- `ReWarmupLoop(ctx, ttl, batchSize)` — every 15 minutes, re-reads files with `last_prefetched > 6 hours` ago. Prevents JuiceFS LRU eviction from rotating pinned content out.
- (The 4 worker goroutines.)

Path translation: `Prefetcher.stripMountPrefix(canonicalPath)` turns canonical pin keys (e.g. `/Volumes/zpool/foo/bar.mov`) into FUSE-relative paths (`foo/bar.mov`) so the worker can `os.Open(filepath.Join(p.fusePath, relative))`.

### 11.4 NFS handler integration (`nfs/handler.go`)

Two gates layered on top of the existing read path:

**Open-time gate** (in `OpenFile`):
```go
if !isWrite && pin.IsOffline() && jfs.handler.pinStore != nil {
    canonical := jfs.handler.canonicalize(filename)
    if !jfs.handler.isPinnedReady(canonical) {
        return nil, syscall.EIO
    }
}
```
Fails un-pinned reads in ~6 ms. Without this, small reads hit the kernel page
cache or memory buffer first, and a 137 MB read could take 14 s to fail at the
read-time gate (NFS retry timeout).

**Read-time gate** (in `cachedFile.ReadAt`):
```go
if pin.IsOffline() && !f.pinned {
    return 0, syscall.EIO
}
```
The `f.pinned` bool is captured at OpenFile time from the same `IsPinnedReady`
check. Critical: pinned files must NOT be EIO'd here, because Priority 2 (SSD
cache reader) and Priority 3 (FUSE/JuiceFS LRU) are separate cache layers; a
miss at Priority 2 doesn't mean the bytes are gone — they may still be in
JuiceFS's local LRU and serve at multi-GB/s.

The same gate is replicated on `billyFile` (the read path for files not yet in
the SQLite metadata cache — happens routinely during fresh directory traversal).

`canonicalize(filename)` handles:
- plain relative (`Film Projects/foo.mov` → `/Volumes/zpool/Film Projects/foo.mov`)
- leading-slash relative (`/Film Projects/foo.mov` → same)
- already-absolute under mount (`/Volumes/zpool/...` → unchanged)
- trailing slash on mount point (tolerated)
- empty mount point falls back to legacy default

Tests in `nfs/canonicalize_test.go`.

### 11.5 JuiceFS daemon configuration (`health/fuse.go`)

What the menu-bar app launches:
```
juicefs mount redis://127.0.0.1:6379/1 ~/.juicemount/fuse-internal \
  -d --no-usage-report --writeback --buffer-size 4096 --prefetch 3 \
  -o nobrowse --cache-size <auto> --free-space-ratio 0.01 \
  [--max-uploads 64]   # only when JM_WAN_MODE=1 (or JM_MAX_UPLOADS=<n>); default 20
```

`--cache-size` is **auto-expanded** at mount time:
```
target_gb = max(user_configured_gb, 0.85 * total_disk_gb)
```
So a 100 GiB user preference on a 1 TiB disk becomes 850 GiB. The user-configured value is treated as a floor, not a ceiling. The real upper bound is enforced by `--free-space-ratio 0.01` at write time — JuiceFS stops caching when free disk drops below 1%.

Why default `--free-space-ratio` (0.1) was a problem: video editors fill their disks. Below 10% free, JuiceFS silently disables cache writes and uploads every block straight to S3. This was the user's reported "tons of network traffic on a file that should be cached" symptom.

### 11.6 APFS purgeable-space reclamation

macOS APFS hides hundreds of GB behind "purgeable" — Time Machine local snapshots, iCloud cached content, system caches. `df` doesn't show it; `URLResourceKey.volumeAvailableCapacityForImportantUsageKey` does.

`health.ReclaimPurgeableSpace(volume, targetBytes)` shells out to `tmutil thinlocalsnapshots <vol> <amount> 4` and returns freed bytes. Auto-called at mount time when free < 50 GB. Manual trigger via popover Reclaim button or `POST /reclaim`.

Real-world test: freed 210 GB on the user's machine in one shot by thinning a single Time Machine local snapshot.

### 11.7 HTTP control plane (`bridge/cbridge.go`)

Routes registered on the metrics server (port 11050):

| Route | Method | Purpose |
|-------|--------|---------|
| `/metrics` | GET | Prometheus-style RPC counters + latencies |
| `/cache-status` | GET | Pin aggregate + per-root stats + live prefetch + offline flag |
| `/pin?path=...` | POST | Register path/folder for pinning |
| `/unpin?path=...` | POST | Remove from registry |
| `/offline?on=1\|0` | GET | Toggle offline mode |
| `/reclaim` | POST | Thin Time Machine local snapshots |
| `/verify-pins` | POST | Re-verify all pinned coverage by marking everything Pending |

Used by:
- The Swift menu-bar app (popover refresh polling, button actions)
- `cmd/juicemount` control-client CLI (for scripting)
- Any external orchestration

### 11.8 Logging (`internal/jmlog/jmlog.go`)

Structured JSON via stdlib `log/slog`. Output fanned to stderr AND a size-rotated file:
- Path: `~/Library/Logs/JuiceMount/juicemount.log`
- Rotation: 16 MB per file × 5 generations = 80 MB cap
- Per-instance rotation flag (lock-free atomic)

Plus a goroutine that tails `~/.juicefs/juicefs.log` and promotes WARNING/ERROR records into our log. The chatty "space not enough on device" pattern is aggregated (one summary per 60 s with a count).

---

## 12. Known Issues and Future Work

### go-nfs Fork Opportunities

The `internal/nfs/` package is a vendored fork of `willscott/go-nfs`. Current
modifications are marked with `[JM5]` comments. Potential upstream contributions
or deeper fork changes:

- **READDIRPLUS maxcount estimation** -- Current estimation uses a fixed 512
  bytes per entry. Could be more accurate to avoid under/over-filling responses.
- **Fragment reconstruction** -- `conn.go` logs a warning for multi-fragment
  requests but doesn't implement reassembly. Not needed for macOS client but
  would improve protocol compliance.

### Finder Cold Directory Timing

First access to a directory not in SQLite cache requires FUSE ReadDir + sync
BulkInsert. For directories with 1000+ entries, this can take 2+ seconds.
The prefetch system mitigates this for navigated-to directories but doesn't
help with the initial cold access from a completely fresh start.

Possible improvements:
- Background full-tree prefetch on startup (after initial Redis sync)
- Lazy READDIRPLUS that returns partial results while prefetch completes

### Exclusive Create Cosmetic Errors

The `onCreate` handler treats exclusive creates as unchecked creates. While
functionally correct (the file is created successfully), it means:
- The createverf3 verifier is read and discarded
- If the file already exists, we don't check the verifier to determine if
  this is a retry of the same create

This is cosmetically incorrect per RFC 1813 but has no practical impact since
macOS retries handle this case.

### Write Durability Window

Since `writeFile.Close()` does not call `fsync()` and always reports
`unstable` stability, there is a durability window between when the NFS
client considers a write "done" and when JuiceFS actually flushes to MinIO.
This is acceptable for the video editing workload (projects auto-save
frequently, and the JuiceFS writeback buffer handles durability) but would
not be appropriate for a database workload.

### Memory Pressure

With the MemoryBuffer (2GB budget), FDPool, and SQLite in-memory caches
(147K entries x ~200 bytes each = ~30MB), JM5 can use 2-3GB of RAM. The
MemoryBuffer budget should be configurable and perhaps auto-adjusted based
on system memory pressure.

---

## 13. Testing

### Test Packages

| Package | File | What it tests |
|---------|------|---------------|
| `metadata` | `store_test.go` | SQLite CRUD, BulkInsert, cache coherence, concurrent access |
| `metadata` | `redis_test.go` | Redis connect, Lua sync, SUBSCRIBE event application, reconnect |
| `cache` | `reader_test.go` | SSD cache block reading, slice mapping, cache miss handling |
| `nfs` | `server_test.go` | Server start/stop, listener setup, handler initialization |
| `nfs` | `connect_test.go` | NFS mount/unmount, basic file operations through NFS |
| `nfs` | `read_test.go` | Read path: cached reads, FUSE fallback, readahead triggering |
| `nfs` | `write_test.go` | Write path: size tracking, fd pool, metadata updates |
| `nfs` | `readahead_test.go` | Sequential detection, prefetch triggering, concurrent limits |
| `nfs` | `membuf_test.go` | Memory buffer: load, evict, concurrent access, budget limits |
| `nfs` | `canonicalize_test.go` | Pin path translation: 7 cases (relative, absolute, trailing slash, custom mount, fallback) |
| `nfs` | `billyfile_offline_test.go` | Offline gate on the no-metadata read path: 4 states |
| `health` | `monitor_test.go` | Health check cycle, state transitions, grace period |
| `health` | `netwatch_test.go` | Interface detection, change callbacks, grace period |
| `internal/cache/pin` | `store_test.go` | Pin registry CRUD, status transitions, IsPinnedReady late-window |
| `internal/jmlog` | `jmlog_test.go` | Log rotation cap, level parse, structured attr handling |
| `internal/nle` | `parser_test.go` | NLE project parsers: Premiere, Resolve, FCPX media-reference extraction |
| `test` | `benchmark_suite_test.go` | Full performance regression suite (10 benchmarks) — dated 2026-03-31, predates pin/offline subsystems |
| `test` | `e2e_test.go` | End-to-end: start server, mount, file ops, unmount — same |
| `test` | `workflow_test.go` | Real-world workflows: Finder operations, concurrent access — same |
| `test` | `finder_perf_test.go` | Finder-specific performance patterns — same |

### How to Run

```bash
# Unit tests (no network or mount required)
go test ./metadata/ ./cache/ ./nfs/ ./health/

# Integration tests (requires Redis at 127.0.0.1:6379)
go test -v ./metadata/ -run TestRedis

# NFS mount tests (requires running JuiceMount5 + sudo access)
go test -v ./nfs/ -run TestConnect

# End-to-end tests (starts server, mounts, runs operations)
go test -v ./test/ -run TestE2E -timeout 5m

# Full benchmark suite
go test -v ./test/ -run TestBenchmarkSuite -timeout 10m
```

### Sudoers Setup for Mount Tests

The NFS mount/unmount commands require `sudo`. For CI or automated testing:

```
# /etc/sudoers.d/juicemount
%staff ALL=(ALL) NOPASSWD: /sbin/mount_nfs
%staff ALL=(ALL) NOPASSWD: /sbin/umount
%staff ALL=(ALL) NOPASSWD: /bin/mkdir -p /Volumes/zpool
```

---

## 14. Build and Run Instructions

### Prerequisites

- Go 1.26+
- JuiceFS 1.3.x (`/opt/homebrew/bin/juicefs`)
- Redis server at 127.0.0.1:6379
- MinIO server at 127.0.0.1:9000
- JuiceFS volume "zpool" already created and mountable
- macOS (for NFS mount_nfs and Finder integration)

### Build

```bash
cd /Users/USER/Developer/JuiceMount5

# Build the CLI
go build -o jm5 ./cmd/jm5/

# Build the C bridge (for Swift app)
cd bridge
go build -buildmode=c-archive -o libjm5.a ./cbridge.go
```

### Run

```bash
# Start JuiceFS FUSE mount first (hidden, internal)
juicefs mount redis://127.0.0.1:6379/1 ~/.juicemount/fuse-internal \
  --cache-dir ~/.juicefs/cache \
  --cache-size 102400 \
  --buffer-size 1024 \
  --prefetch 3 \
  --writeback

# Start JuiceMount5
./jm5 \
  --redis redis://127.0.0.1:6379/1 \
  --fuse-path ~/.juicemount/fuse-internal \
  --mount /Volumes/zpool \
  --listen 127.0.0.1:11049

# Or with custom paths
./jm5 \
  --redis redis://127.0.0.1:6379/1 \
  --fuse-path /path/to/fuse \
  --mount /Volumes/myvolume \
  --listen 127.0.0.1:12345 \
  --db /path/to/metadata.db

# Start without mounting (for testing)
./jm5 --no-mount
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--redis` | `redis://127.0.0.1:6379/1` | Redis URL |
| `--fuse-path` | `~/.juicemount/fuse-internal` | JuiceFS FUSE mount path |
| `--mount` | `/Volumes/zpool` | NFS mount point |
| `--listen` | `127.0.0.1:11049` | NFS server listen address |
| `--db` | `~/.juicemount/metadata.db` | SQLite database path |
| `--no-mount` | `false` | Start server without mounting |

### Verify

```bash
# Check mount
mount | grep zpool

# List files
ls /Volumes/zpool/

# Check metadata count
sqlite3 ~/.juicemount/metadata.db "SELECT COUNT(*) FROM entries;"
```

---

## 15. Write Spool (Option 2) — the local-SSD write intermediary (added 2026-05-28)

This section documents the write-spool subsystem layered on top of §1–§14. It
is the write-path analogue of §11's pin/offline read subsystem: the difference
between "writes feel fast on a wired LAN" and "writes feel like local SSD even
over Tailscale or cellular." It is gated by `JM_SPOOL_ENABLE=1`; when disabled,
the handler's `spool` field is nil and §4's direct-to-FUSE write path runs
unchanged. Slice-by-slice design history lives in `docs/ROADMAP/option-2-spool.md`.

### 15.1 The problem and the durability boundary

JuiceFS gates sustained writes on a RAM-tracked uploader budget
(`--buffer-size`). Once the in-flight upload queue exceeds that budget the
`WriteAt` syscall blocks — so a 2 GB Finder copy over a slow link drains at
upload speed (observed ~280 KB/s on Tailscale → ~2-hour ETAs) even though
`rawstaging/` has hundreds of GB of disk headroom. Raising `--buffer-size`
steals RAM from the editor's NLE project caches.

The spool moves the **durability checkpoint one step earlier**:

- **Without spool:** durable when JuiceFS's `rawstaging/` write completes —
  still inside JuiceFS's domain, subject to its flow control.
- **With spool:** durable when our spool file's `fsync()` completes — inside
  JuiceMount's domain, on local SSD.

Both are local-SSD-resident; the spool boundary lets us ACK to Finder *before*
JuiceFS sees the bytes. A background drainer then feeds JuiceFS at MinIO's pace.
This is the model Dropbox Smart Sync, LucidLink, and Suite use under the hood —
achieved here without forking JuiceFS's flow control.

### 15.2 Components

```
   NFS WRITE RPC (O_CREATE)                         NFS READ RPC
            │                                            │
            ▼                                            ▼
   handler.OpenFile(write)                     handler.OpenFile(read)
            │                                            │
            ▼                                  spool.LookupActive(path)?
   spool.OpenWrite(path)  ──────────┐          ├─ hit  → spoolReadFile (local SSD)
            │                       │          └─ miss → cacheReader/readahead/
            ▼                       │                     memBuf/FUSE  (§4 path)
   spoolWriteFile.WriteAt ── streaming SHA-256
            │  (local SSD file under <spool>/files/)
            ▼
   Close → fsync → meta.MarkReady → signalReady ──────────┐
                                                          ▼
                                          ┌──────────── Drainer ───────────┐
                                          │ dispatcher + N workers (def 4)  │
                                          │ ListReady → MarkDraining        │
                                          │ copy spool→FUSE (1 MiB buf)     │
                                          │ verify SHA-256 (stream + at-rest)│
                                          │ MarkDrainComplete → rm spool file│
                                          └────────────────┬────────────────┘
                                                           ▼
                                              FUSE → rawstaging → MinIO
                                                       (JuiceFS, at its pace)
```

- **`SpoolStore`** (`nfs/spool.go`) — owns the spool root on local SSD
  (`<root>/files/` for spool files, `<root>/quarantine/` for SHA-mismatched
  files, `<root>/manifest.log` for the audit trail), the in-memory index, the
  SQLite-backed durable index, and the capacity budget. Capacity is reserved
  with an atomic compare-and-swap loop so concurrent writers on different
  entries can never both pass the cap check and over-fill.

- **`SpoolEntry`** (`nfs/spool.go`) — one in-flight or pending-upload file.
  Holds the write fd plus a streaming `sha256.Hash`. Refcounted so a single
  Finder copy's multi-RPC `OpenWrite → WriteAt… → Close` lifecycle reuses one
  spool file (mirrors FDPool same-path dedupe). Detects out-of-order `WriteAt`
  (offset < current `writtenEnd`): when seen, the streaming hash is marked
  invalid and the drainer re-hashes from disk instead of trusting it. Carries a
  synthetic inode (set once by the handler) so Stat/Lstat report a stable value
  during the file's spool lifetime.

- **`SpoolIndex`** (`nfs/spool_index.go`) — `map[path]*SpoolEntry` under an
  `RWMutex`. The read hot path takes a read-lock O(1) lookup; QA-35-disciplined
  (empty-spool lookup benchmarked ~8 ns, well under the 100 ns budget).

- **`metadata.SpoolStore` + `spool_entries` table** (`metadata/spool_store.go`,
  `metadata/spool_schema.go`) — the durable index. Lives in the *same* SQLite
  database as the `entries` cache, sharing its WAL and `writeMu`. One row per
  in-flight/pending file (see schema in §15.8). This is what survives a process
  restart and drives crash recovery.

- **`Drainer`** (`nfs/drainer.go`) — a single dispatcher goroutine + a bounded
  worker pool (default 4). Sleeps on a wake signal (`signalReady` on each
  `Close`), with a 30 s poll fallback for missed signals. Per row: atomically
  claims via `MarkDraining`, copies the spool file into the FUSE mount with a
  1 MiB buffer, SHA-verifies, then `MarkDrainComplete` (which deletes the spool
  file only after the SQL row is `done`).

- **`manifestWriter`** (`nfs/spool_manifest.go`) — append-only JSONL audit log
  at `<root>/manifest.log`; records every drain-done and quarantine event with
  the path, size, and SHA-256. Non-fatal if it fails to open (audit-only).

### 15.3 Write flow

```
Finder/Premiere WRITE → handler.OpenFile(O_CREATE)
  │  (spool != nil && O_CREATE set)
  ▼
spool.OpenWrite(filename)
  ├─ index hit → refcount++ and reuse (same-path reopen)
  ├─ capacity exhausted → ErrSpoolFull → NFS3ERR_NOSPC
  └─ else: meta.Insert(row, state=writing) + O_EXCL create <root>/files/<hash>-<ts>
  ▼
return spoolWriteFile (handler also SetInode + tracks active-writer)
  │
  ▼  per WRITE RPC:
spoolWriteFile.WriteAt → entry.WriteAt(p, off)
  ├─ CAS-reserve capacity for any new bytes (ErrSpoolFull if over cap)
  ├─ file.WriteAt(p, off)
  └─ fold p into streaming SHA-256 (unless an out-of-order write invalidated it)
  │
  ▼  on Close (last refcount):
entry.Close → fsync + close fd → finalize SHA → meta.MarkReady(size, sha) → signalReady()
  ▼
NFS response: OK (durable on local SSD; Finder considers the write complete)
```

The spool file basename is `hex(SHA-256(nfs_path))[:8] + "-" + unixMicros` — the
hash prefix avoids filesystem-character issues with the NFS path (slashes,
spaces, unicode), the timestamp suffix avoids collisions on rapid re-opens.

### 15.4 Read flow (3-tier + Stat/Lstat shadow)

The read path gains one tier *in front of* the existing §4 read path:

```
handler.OpenFile(read) → spool.LookupActive(filename)
  ├─ hit  → spoolReadFile.ReadAt (pread on the local-SSD spool file)
  └─ miss → existing cachedFile path: memBuf → cache.Reader → FUSE
```

This closes slice C's "just-written file is briefly invisible until drained"
limitation: between `Close` and the drainer's copy-into-FUSE, the bytes live
only in the spool, so reads are served from there. `Stat` and `Lstat` apply the
same shadow — when a path is in the spool index they report the entry's live
`writtenEnd` and `lastWrite` mtime (same shadowing rule §5.9's `writeSizes`
established), so a file being written reports a growing size in real time rather
than 0 / 1970-epoch. The miss case is the unchanged hot path — the index lookup
is the ~8 ns QA-35-gated cost noted above.

### 15.5 Drainer dispositions

For each claimed row the worker (`drainOne`) ends in exactly one disposition:

- **Success:** copy completes, size matches, SHA-256 matches (both the
  spool-read stream and the at-rest re-read through FUSE) → `MarkDrainComplete`
  marks the row `done`, removes the spool file, releases capacity, evicts the
  index entry, appends a manifest record.
- **Transient failure** (mkdir/open/create/copy/close error, size mismatch, or
  a SQL error from `MarkDrainComplete`): `MarkDrainRetry` bumps `drain_attempts`
  + `last_error` and resets the row to `ready`. Exponential backoff
  (`base 1 s << (attempts-1)`, capped 30 s) is scheduled via a **separate timer
  goroutine** — the worker frees its semaphore slot immediately rather than
  sleeping on it. After `MaxAttempts` (default 5) → permanent `failed`.
- **SHA mismatch** (a bit flip on the spool SSD, or corruption through FUSE):
  `QuarantineDrain` marks the row `failed`, moves the spool file to
  `<root>/quarantine/`, releases capacity, and logs a manifest quarantine event.
  Retrying a bit flip would only re-detect it, so the file is preserved for
  forensics and never deleted.

`MarkDrainComplete` is ordered so a transient SQL failure can never convert a
successfully-drained file into data loss: the SQL `MarkDone` runs first, and
only on its success is the spool file removed and capacity released.

### 15.6 Crash recovery (`SpoolStore.RecoverOnBoot`)

The boot scrubber runs once at startup, after `NewSpoolStore`/`InitSpoolSchema`
and **before** `drainer.Start` (it asserts the no-concurrent-drainer invariant
and panics if violated). It reconciles on-disk spool files against the SQL rows:

| Situation | Action |
|-----------|--------|
| File on disk, no SQL row | delete the orphan file |
| Row `writing` (crash mid-write) | `MarkFailed` + delete the partial file — the NFS client doesn't know its in-flight WRITEs weren't durable, so resume is unsafe |
| Row `ready`, file present | re-account bytes against the capacity counter; leave state (drainer picks it up) |
| Row `ready`/`draining`, file missing | `MarkFailed` (no data to retry) |
| Row `draining`, file present | `ResetToReady` so the drainer re-attempts; re-account bytes |
| Row `done`/`failed`, stale file present | remove the stale file (terminal rows keep no file) |

The capacity counter is reset to 0 and rebuilt from the surviving `ready`/
`draining` rows, so recovery is idempotent. The in-memory index is *not*
repopulated — recovered paths haven't reached FUSE yet, so reads briefly return
ENOENT until the drainer copies them in (same window as slice C).

### 15.7 Integrity discipline

SHA-256 is enforced at every hop:

1. **On write:** computed streaming as bytes land in the spool file. Invalidated
   (and re-derived from disk by the drainer) if any out-of-order `WriteAt`
   arrives.
2. **On drain-read:** the bytes read back out of the spool file are re-hashed
   and compared to the recorded SHA → catches a spool-SSD bit flip.
3. **At rest in FUSE:** after the copy, the destination is re-read *through the
   FUSE mount* and re-hashed → catches corruption introduced by the FUSE/JuiceFS
   writeback in flight. (Performed only for sequential writes, where a trusted
   reference SHA exists; out-of-order writes rely on the manifest log instead.)

Cost is ~1 GB/s on Apple Silicon NEON — negligible against 80–500 MB/s NVMe and
the one extra sequential read per drained file. The append-only `manifest.log`
records the SHA + timestamp for every drain-done and quarantine, as a
tamper-evident audit trail.

### 15.8 Capacity and SQLite schema

Capacity defaults to 50 GiB (`JM_SPOOL_SIZE_GB`), 0 = unlimited. Reservation is
incremental per `WriteAt` (we don't know a file's final size in advance); an
overflowing write returns `ErrSpoolFull`, which the handler maps to
`NFS3ERR_NOSPC` so Finder shows a clean "disk full."

```sql
CREATE TABLE IF NOT EXISTS spool_entries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    nfs_path        TEXT NOT NULL,
    spool_file      TEXT NOT NULL UNIQUE,
    size            INTEGER NOT NULL DEFAULT 0,
    sha256          BLOB,
    drain_state     TEXT NOT NULL CHECK(drain_state IN
                      ('writing','ready','draining','done','failed')),
    drain_attempts  INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_spool_drain_state ON spool_entries(drain_state);
CREATE INDEX IF NOT EXISTS idx_spool_path        ON spool_entries(nfs_path);
```

Canonical `drain_state` transitions: `writing→ready` (clean close),
`writing→failed` (crash, via scrubber), `ready→draining` (claimed),
`draining→done` (drained + verified), `draining→ready` (interrupted, via
scrubber), `draining→failed` (attempts exhausted), `failed→ready` (operator
manual retry).

### 15.9 Configuration and JuiceFS interaction

All env-gated (parsed identically by the Swift bridge in `bridge/cbridge.go` and
the CLI in `cmd/jm5/main.go`):

| Env var | Default | Effect |
|---------|---------|--------|
| `JM_SPOOL_ENABLE` | `0` (off) | `1` enables the spool; otherwise the §4 direct-to-FUSE write path runs |
| `JM_SPOOL_DIR` | `~/Library/Application Support/JuiceMount/spool/` (Darwin; `$TMPDIR/…` fallback) | spool root directory |
| `JM_SPOOL_SIZE_GB` | `50` | capacity cap in GiB; values < 1 are ignored |
| `JM_WAN_MODE` | off | `1` raises JuiceFS `--max-uploads` 20 → 64 (more parallel PUTs to cover bandwidth-delay product on a fat WAN pipe) |
| `JM_MAX_UPLOADS` | unset | direct `--max-uploads <n>` override; wins over `JM_WAN_MODE` |

Note the spool does **not** require bumping JuiceFS `--buffer-size` (it stays at
the QA-33 value of 4096); the spool absorbs the write burst, and the drainer
feeds JuiceFS at a steady pace. `JM_WAN_MODE` / `JM_MAX_UPLOADS` tune the
*drain-to-MinIO* side, not the Finder-facing write side.

### 15.10 HTTP control plane

`GET /spool` on the metrics/control server (port 11050) returns the live spool
state for the menu bar + Manager UI. `503` when the spool is disabled, `500`
(with a partial body) on SQL error, `200` on the happy path:

```json
{ "enabled": true, "pending_files": 12, "pending_bytes": 3400000000,
  "in_progress": 4, "succeeded": 880, "failed": 0, "quarantined": 0,
  "capacity_used": 3400000000, "capacity_total": 53687091200,
  "entries": [ { "path": "...", "size": ..., "drain_state": "draining",
                 "drain_attempts": 0, "updated_at_unix": ... } ] }
```

The entry list is capped at 200 newest rows to keep the 1 Hz menu-bar poll cheap.

### 15.11 What's deferred (not yet shipped)

The data contract above is stable, but these consumer surfaces are not yet built:
the Swift menu-bar "Pending uploads" section + icon badge, the Manager web-UI
tile, the `App.swift` graceful-quit-with-pending-uploads dialog, the Preferences
"Sync & Upload" pane, and the 24-hour live soak test.

### 15.12 Rollback

`JM_SPOOL_ENABLE=0` (the default) reverts to the existing `writeFile` path; the
spool directory is unused and no behavior changes. Spool files persisted from a
prior enabled run remain on disk and are drained on the next enabled start (the
SQL index drives the drainer). After a clean soak the flag is intended to flip
to default-on, with the env var retained as the rollback lever.

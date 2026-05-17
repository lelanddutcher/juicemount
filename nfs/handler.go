package nfs

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"net"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/metadata"
)

// JuiceMountHandler implements the go-nfs Handler interface, serving
// metadata from SQLite and proxying file I/O to JuiceFS FUSE.
type JuiceMountHandler struct {
	store       *metadata.Store
	fusePath    string // path to hidden JuiceFS FUSE mount
	mountPoint  string // user-facing NFS mount (e.g. /Volumes/zpool); used to canonicalize pin keys
	cacheReader *cache.Reader
	fdPool      *FDPool
	readahead   *ReadaheadManager
	memBuf      *MemoryBuffer
	redisClient *metadata.RedisClient // for publishing events
	pinStore    *pin.Store            // optional; gates reads when offline mode is on

	// Synthetic inode counter for locally-created entries (atomic)
	inodeCounter atomic.Uint64

	// Write size tracking: path → latest known size (for WCC accuracy).
	// Sticky — entries persist after writers close (QA-16: concurrent
	// closes must not lose the high-water mark).
	writeSizeMu sync.Mutex
	writeSizes  map[string]int64

	// Active writer refcount: path → number of in-flight write handles.
	// Used by phantom-purge gate to distinguish "active writer right now"
	// from "was ever written" (writeSizes is sticky so it can't answer
	// the first question). Incremented when OpenFile returns a writeFile;
	// decremented on writeFile.Close; entry deleted when count reaches 0.
	// (QA-19 fix, 2026-05-17)
	activeWritersMu sync.Mutex
	activeWriters   map[string]int

	// READDIRPLUS cookie tracking
	verifierMu sync.Mutex
	verifiers  map[string]verifierData

	// Directory prefetch tracking — prevents redundant prefetches
	prefetchMu   sync.Mutex
	prefetched   map[string]time.Time
	prefetchSem  chan struct{} // limits concurrent prefetch goroutines

	// Verifier cleanup lifecycle
	verifierStop chan struct{}
}

type verifierData struct {
	verifier   uint64
	entries    []os.FileInfo
	lastAccess time.Time
}

// lstatNotExistWithTimeout runs os.Lstat in a goroutine bounded by the
// given timeout. Returns (isNotExist, ok) where ok=false means the Lstat
// took longer than the timeout and the caller should fall back to a
// conservative path. On timeout the spawned goroutine is leaked; it
// will terminate naturally when the underlying FUSE syscall returns.
// That's preferable to blocking the request goroutine forever.
func lstatNotExistWithTimeout(p string, timeout time.Duration) (isNotExist, ok bool) {
	type result struct {
		err error
	}
	ch := make(chan result, 1)
	go func() {
		_, err := os.Lstat(p)
		ch <- result{err: err}
	}()
	select {
	case r := <-ch:
		return os.IsNotExist(r.err), true
	case <-time.After(timeout):
		return false, false
	}
}

// lstatWithTimeout is the FileInfo-returning sibling of
// lstatNotExistWithTimeout. ok=false means the underlying Lstat didn't
// complete within the timeout; callers should fall back to a safe default
// (typically: treat the entry as unknown rather than blocking the request
// goroutine on a wedged FUSE daemon).
func lstatWithTimeout(p string, timeout time.Duration) (fi os.FileInfo, ok bool) {
	type result struct {
		fi  os.FileInfo
		err error
	}
	ch := make(chan result, 1)
	go func() {
		fi, err := os.Lstat(p)
		ch <- result{fi: fi, err: err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			return nil, true // call completed but failed (e.g., ENOENT); caller decides
		}
		return r.fi, true
	case <-time.After(timeout):
		return nil, false
	}
}

func NewHandler(store *metadata.Store, fusePath string) *JuiceMountHandler {
	fdPool := NewFDPool()
	h := &JuiceMountHandler{
		store:        store,
		fusePath:     fusePath,
		fdPool:       fdPool,
		readahead:    NewReadaheadManager(fusePath, fdPool),
		memBuf:       NewMemoryBuffer(DefaultMemBufThreshold, DefaultMemBufBudget),
		writeSizes:    make(map[string]int64),
		activeWriters: make(map[string]int),
		verifiers:     make(map[string]verifierData),
		prefetched:   make(map[string]time.Time),
		prefetchSem:  make(chan struct{}, 4), // max 4 concurrent prefetches
		verifierStop: make(chan struct{}),
	}
	go h.verifierCleanupLoop(60*time.Second, 5*time.Minute)
	return h
}

// SetCacheReader attaches a direct SSD cache reader for bypassing FUSE on cached reads.
func (h *JuiceMountHandler) SetCacheReader(cr *cache.Reader) {
	h.cacheReader = cr
}

// SetPinStore attaches the pin registry and the user-facing mount point that
// the pin keys are anchored to. When offline mode is on, the read path
// consults this store to fail-fast on un-pinned/un-cached files.
//
// mountPoint is the path the user mounts to (e.g. "/Volumes/zpool"). It is
// used as the prefix when canonicalizing in-mount filenames into the
// absolute paths the pin store keys on.
func (h *JuiceMountHandler) SetPinStore(ps *pin.Store, mountPoint string) {
	h.pinStore = ps
	h.mountPoint = mountPoint
}

// canonicalize converts an in-mount relative path (the form go-nfs hands us
// in OpenFile) into the absolute path that the pin store keys on. It is
// tolerant of the various shapes filenames arrive in:
//
//   - "Film Projects/foo.mov"     → "/Volumes/zpool/Film Projects/foo.mov"
//   - "/Film Projects/foo.mov"    → "/Volumes/zpool/Film Projects/foo.mov"
//   - "/Volumes/zpool/foo.mov"    → "/Volumes/zpool/foo.mov" (already absolute)
//
// Falls back to the legacy hardcoded prefix when no mount point is set.
func (h *JuiceMountHandler) canonicalize(filename string) string {
	mp := h.mountPoint
	if mp == "" {
		mp = "/Volumes/zpool"
	}
	// Strip any trailing slash on the mount point for clean concat.
	for len(mp) > 1 && mp[len(mp)-1] == '/' {
		mp = mp[:len(mp)-1]
	}
	// If the filename is already absolute and under the mount, it's already canonical.
	if len(filename) > 0 && filename[0] == '/' {
		// already-absolute under mount?
		if len(filename) >= len(mp) && filename[:len(mp)] == mp {
			return filename
		}
		// leading-slash relative — strip it and concat
		return mp + filename
	}
	return mp + "/" + filename
}

// isPinnedReady reports whether the canonical-form path is in the pin
// store with status=Ready. Used by the offline-mode open gate.
// Indexed lookup — safe to call on every OpenFile.
func (h *JuiceMountHandler) isPinnedReady(canonicalPath string) bool {
	if h.pinStore == nil {
		return false
	}
	return h.pinStore.IsPinnedReady(canonicalPath)
}

// SetRedisClient attaches a Redis client for publishing metadata events.
func (h *JuiceMountHandler) SetRedisClient(rc *metadata.RedisClient) {
	h.redisClient = rc
}

// nextSyntheticInode returns a unique inode for locally-created entries.
// Uses high bit set to distinguish from JuiceFS inodes.
func (h *JuiceMountHandler) nextSyntheticInode() uint64 {
	return h.inodeCounter.Add(1) | (1 << 63)
}

// incActiveWriter records that a new write handle is in flight for this path.
// Pairs with decActiveWriter on writeFile.Close. The Stat phantom-purge gate
// reads this count to decide whether the cache entry must be preserved (any
// nonzero count means the writer's NFS handle is in active use; deleting
// the inode cache entry would surface as ESTALE on the writer's next RPC).
func (h *JuiceMountHandler) incActiveWriter(path string) {
	h.activeWritersMu.Lock()
	h.activeWriters[path]++
	h.activeWritersMu.Unlock()
}

// decActiveWriter releases one in-flight handle reference. Deletes the map
// entry when the count returns to zero so the map doesn't grow unbounded
// across the process lifetime.
func (h *JuiceMountHandler) decActiveWriter(path string) {
	h.activeWritersMu.Lock()
	if c, ok := h.activeWriters[path]; ok {
		if c <= 1 {
			delete(h.activeWriters, path)
		} else {
			h.activeWriters[path] = c - 1
		}
	}
	h.activeWritersMu.Unlock()
}

// hasActiveWriter reports whether any write handle is currently in flight
// for the given path. The phantom-purge gate consults this to avoid
// invalidating an active writer's NFS handle. (QA-19 fix.)
func (h *JuiceMountHandler) hasActiveWriter(path string) bool {
	h.activeWritersMu.Lock()
	defer h.activeWritersMu.Unlock()
	return h.activeWriters[path] > 0
}

// trackWriteSize records the high-water mark of written size for a path.
// QA-16 fix (2026-05-17): uses MAX semantics, not absolute set. Under
// concurrent WRITE RPCs each writeFile instance reports the size after
// ITS write. Without MAX, a late RPC writing at a low offset would
// shrink the tracked size, even though earlier RPCs already wrote past
// it. The tracked value is the file's logical size, not any individual
// RPC's contribution — so it must only grow.
func (h *JuiceMountHandler) trackWriteSize(path string, size int64) {
	h.writeSizeMu.Lock()
	if cur, ok := h.writeSizes[path]; !ok || size > cur {
		h.writeSizes[path] = size
	}
	h.writeSizeMu.Unlock()
}

// prefetchChildren scans a directory on FUSE and caches all children into SQLite.
// This runs in the background so that subsequent Stat() calls are instant.
// Also spawns bounded sub-prefetches for immediate subdirectories (one level)
// for Finder's "expanding disclosure triangle" pattern.
func (h *JuiceMountHandler) prefetchChildren(dirname string) {
	h.prefetchMu.Lock()
	if t, ok := h.prefetched[dirname]; ok && time.Since(t) < 30*time.Second {
		h.prefetchMu.Unlock()
		return // recently prefetched
	}
	h.prefetched[dirname] = time.Now()
	h.prefetchMu.Unlock()

	fusePath := path.Join(h.fusePath, dirname)
	if dirname == "." {
		fusePath = h.fusePath
	}

	dirEntries, err := os.ReadDir(fusePath)
	if err != nil {
		return
	}

	toInsert := make([]*metadata.Entry, 0, len(dirEntries))
	var subdirs []string
	for _, de := range dirEntries {
		info, err := de.Info()
		if err != nil {
			continue
		}
		var inode uint64
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			inode = st.Ino
		} else {
			inode = h.nextSyntheticInode()
		}
		childPath := path.Join(dirname, info.Name())
		if dirname == "." {
			childPath = info.Name()
		}

		// Only insert if not already cached
		if h.store.LookupByPath(childPath) == nil {
			entry := metadata.MakeEntry(childPath, info.IsDir(), info.Size(), info.ModTime(), inode)
			toInsert = append(toInsert, entry)
		}

		// Track subdirectories for one-level-deep prefetch
		if info.IsDir() {
			subdirs = append(subdirs, childPath)
		}
	}

	if len(toInsert) > 0 {
		h.store.BulkInsert(toInsert, 500)
	}

	// Prefetch one level of subdirectories, bounded by the semaphore.
	// Non-blocking acquire — if all workers are busy, skip that subdir.
	for _, sub := range subdirs {
		h.prefetchMu.Lock()
		_, already := h.prefetched[sub]
		h.prefetchMu.Unlock()
		if already {
			continue
		}
		select {
		case h.prefetchSem <- struct{}{}:
			go func(dir string) {
				defer func() { <-h.prefetchSem }()
				h.prefetchChildren(dir)
			}(sub)
		default:
			// all workers busy, skip
		}
	}
}

// publishEvent sends a metadata event via Redis SUBSCRIBE if a client is configured.
func (h *JuiceMountHandler) publishEvent(evt metadata.MetadataEvent) {
	if h.redisClient == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h.redisClient.PublishEvent(ctx, evt)
	}()
}

// verifierCleanupLoop periodically removes stale verifier and prefetch entries.
func (h *JuiceMountHandler) verifierCleanupLoop(interval, ttl time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			h.evictStaleVerifiers(ttl)
			h.evictStalePrefetched(2 * time.Minute)
		case <-h.verifierStop:
			return
		}
	}
}

// evictStalePrefetched removes prefetch tracking entries older than ttl.
func (h *JuiceMountHandler) evictStalePrefetched(ttl time.Duration) {
	cutoff := time.Now().Add(-ttl)
	h.prefetchMu.Lock()
	for k, t := range h.prefetched {
		if t.Before(cutoff) {
			delete(h.prefetched, k)
		}
	}
	h.prefetchMu.Unlock()
}

// evictStaleVerifiers removes verifier entries whose lastAccess is older than ttl.
func (h *JuiceMountHandler) evictStaleVerifiers(ttl time.Duration) {
	cutoff := time.Now().Add(-ttl)
	h.verifierMu.Lock()
	for k, vd := range h.verifiers {
		if vd.lastAccess.Before(cutoff) {
			delete(h.verifiers, k)
		}
	}
	h.verifierMu.Unlock()
}

// Stop cleans up handler resources.
func (h *JuiceMountHandler) StopHandler() {
	if h.verifierStop != nil {
		close(h.verifierStop)
	}
	if h.readahead != nil {
		h.readahead.Stop()
	}
	if h.memBuf != nil {
		h.memBuf.Stop()
	}
	if h.fdPool != nil {
		h.fdPool.Stop()
	}
}

// Mount handles the NFS MOUNT RPC.
func (h *JuiceMountHandler) Mount(ctx context.Context, conn net.Conn, req nfslib.MountRequest) (status nfslib.MountStatus, hndl billy.Filesystem, auths []nfslib.AuthFlavor) {
	return nfslib.MountStatusOk, &juiceFS{handler: h}, []nfslib.AuthFlavor{nfslib.AuthFlavorNull}
}

// Change returns the change interface (for write ops).
func (h *JuiceMountHandler) Change(fs billy.Filesystem) billy.Change {
	return &juiceChange{handler: h}
}

// FSStat fills in filesystem statistics.
func (h *JuiceMountHandler) FSStat(ctx context.Context, f billy.Filesystem, stat *nfslib.FSStat) error {
	stat.TotalSize = 1 << 40  // 1TB
	stat.FreeSize = 1 << 39   // 512GB
	stat.AvailableSize = 1 << 39
	stat.TotalFiles = 1 << 20
	stat.FreeFiles = 1 << 19
	stat.AvailableFiles = 1 << 19
	stat.CacheHint = 0
	return nil
}

// ToHandle converts a path to an NFS file handle.
// Uses deterministic 8-byte inode-based handles.
func (h *JuiceMountHandler) ToHandle(f billy.Filesystem, path []string) []byte {
	fullPath := strings.Join(path, "/")

	// Root directory
	if fullPath == "" || fullPath == "." {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, 1)
		return buf
	}

	// Look up inode from metadata store
	e := h.store.LookupByPath(fullPath)
	if e != nil {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, e.Inode)
		return buf
	}

	// Fallback: entry not in cache yet (just created, or FUSE-only).
	// Generate a deterministic handle AND insert a cache entry so that
	// FromHandle can resolve it. Without this, the handle is unresolvable
	// and the client gets NFSStatusStale on subsequent operations.
	hash := fnv.New64a()
	hash.Write([]byte(fullPath))
	inode := hash.Sum64() | (1 << 63) // high bit set = synthetic

	// QA-18 fix (2026-05-17): determine the REAL IsDir from FUSE before
	// fabricating the cache entry. Previously this hardcoded IsDir=false,
	// which under cache pressure (BulkInsert evictOldest pushing a parent
	// directory out of the cache after rapid sibling creates) re-created
	// the directory's cache entry as a regular file. Subsequent NFS
	// LOOKUPs for that path saw type=NF3REG, and any child operation
	// (create/open/mkdir under it) returned ENOTDIR to the client.
	// Reproducer: scripts/qa-suite/02-finder.sh "1000 × 1 KiB" test at
	// file-390 ("Not a directory").
	//
	// Lstat against FUSE adds one syscall, but this path only fires on
	// a cache miss — already a slow path. Guard with the same 2-second
	// FUSE-wedge timeout the phantom-purge uses so a stalled juicefs
	// daemon can't block every NFS LOOKUP that lands in this fallback.
	// On Lstat failure / timeout / nil-fi, we fall back to the original
	// hardcoded false; the entry will be corrected on the next sync or
	// LOOKUP that succeeds.
	isDir := false
	// (parameter `path` shadows the package here; use string concat.)
	if fi, ok := lstatWithTimeout(h.fusePath+"/"+fullPath, 2*time.Second); ok && fi != nil {
		isDir = fi.IsDir()
	}
	entry := metadata.MakeEntry(fullPath, isDir, 0, time.Now(), inode)
	h.store.InsertToCache(entry)
	go h.store.Insert(entry) // persist async

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, inode)
	return buf
}

// FromHandle resolves an NFS file handle back to a filesystem and path.
func (h *JuiceMountHandler) FromHandle(handle []byte) (billy.Filesystem, []string, error) {
	if len(handle) != 8 {
		return nil, nil, fmt.Errorf("invalid handle length: %d", len(handle))
	}

	inode := binary.BigEndian.Uint64(handle)

	// Root directory
	if inode == 1 {
		return &juiceFS{handler: h}, []string{}, nil
	}

	// Look up by inode
	e := h.store.LookupByInode(inode)
	if e == nil {
		return nil, nil, &nfslib.NFSStatusError{NFSStatus: nfslib.NFSStatusStale}
	}

	parts := splitPath(e.Path)
	return &juiceFS{handler: h}, parts, nil
}

func (h *JuiceMountHandler) InvalidateHandle(f billy.Filesystem, handle []byte) error {
	return nil // deterministic handles never go stale
}

func (h *JuiceMountHandler) HandleLimit() int {
	return 10000 // deterministic handles have no real limit; large value
	// ensures READDIRPLUS batches many entries per response
}

// CachingHandler methods for READDIRPLUS
func (h *JuiceMountHandler) VerifierFor(path string, contents []os.FileInfo) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(path))
	for _, fi := range contents {
		hash.Write([]byte(fi.Name()))
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(fi.ModTime().Unix()))
		hash.Write(b)
	}
	v := hash.Sum64()

	h.verifierMu.Lock()
	h.verifiers[path] = verifierData{verifier: v, entries: contents, lastAccess: time.Now()}
	h.verifierMu.Unlock()

	return v
}

func (h *JuiceMountHandler) DataForVerifier(path string, verifier uint64) []os.FileInfo {
	h.verifierMu.Lock()
	defer h.verifierMu.Unlock()
	if vd, ok := h.verifiers[path]; ok && vd.verifier == verifier {
		vd.lastAccess = time.Now()
		h.verifiers[path] = vd
		return vd.entries
	}
	return nil
}

// splitPath splits "a/b/c" into ["a", "b", "c"]
func splitPath(p string) []string {
	p = strings.TrimPrefix(p, "/")
	if p == "" || p == "." {
		return []string{}
	}
	return strings.Split(p, "/")
}

// juiceFS implements billy.Filesystem, serving metadata from SQLite
// and proxying file I/O to the JuiceFS FUSE mount.
type juiceFS struct {
	handler *JuiceMountHandler
}

func (jfs *juiceFS) fullPath(filename string) string {
	return path.Join(jfs.handler.fusePath, filename)
}

// cacheProbeHit returns true when the SSD cache reader can serve the
// first block of `e`. Returns false on any miss, timeout, or absent
// cacheReader. Used by OpenFile's C.2/QA-12 path to allow opens of
// "recently cached" files in offline mode.
//
// 200ms is the timeout — see the call site for the rationale.
func (jfs *juiceFS) cacheProbeHit(e *metadata.Entry) bool {
	if e == nil || jfs.handler.cacheReader == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var scratch [4096]byte
	n, err := jfs.handler.cacheReader.ReadBlock(ctx, e.Inode, 0, scratch[:])
	return err == nil && n > 0
}

func (jfs *juiceFS) entryToFileInfo(e *metadata.Entry) os.FileInfo {
	return e.FileInfo()
}

func (jfs *juiceFS) Stat(filename string) (os.FileInfo, error) {
	if filename == "" || filename == "." || filename == "/" {
		return &rootDirInfo{}, nil
	}
	filename = strings.TrimPrefix(filename, "/")

	// Fast rejection for macOS metadata patterns that never exist on
	// JuiceFS. Eliminates ~1/3 of LOOKUPs from Finder.
	//
	// QA-13 (2026-05-17): `._*` AppleDouble sidecars REMOVED from
	// this filter. The kernel's setxattr fallback creates `._filename`
	// then immediately stat's it to confirm. With the filter, the
	// post-create stat returned ENOENT — the kernel concluded the
	// create had failed and reported EPERM/ENOENT back to userspace,
	// which crashed copyfile(3) into a 0-byte truncation. cp and
	// Finder both broke as a result. Let `._*` files round-trip
	// normally; ReadDir keeps suppressing them from listings so they
	// don't clutter Finder's view of user-facing files.
	base := path.Base(filename)
	if base == ".DS_Store" ||
		base == ".Spotlight-V100" ||
		base == ".Trashes" ||
		base == ".fseventsd" ||
		base == ".TemporaryItems" ||
		base == ".VolumeIcon.icns" ||
		base == "Icon\r" {
		return nil, os.ErrNotExist
	}

	// Check if there's a tracked write size (in-flight write)
	jfs.handler.writeSizeMu.Lock()
	writeSize, hasWriteSize := jfs.handler.writeSizes[filename]
	jfs.handler.writeSizeMu.Unlock()

	e := jfs.handler.store.LookupByPath(filename)
	if e != nil {
		// For non-directory entries, verify the file still exists on FUSE.
		// Stale cache entries (from failed copies or deleted files) cause
		// Finder to report "already exists" for files that don't exist.
		//
		// Bounded: a wedged FUSE daemon (MinIO unreachable, JuiceFS hung,
		// SSD I/O stalled) would otherwise park this syscall forever and,
		// under the current per-connection sequential RPC dispatch, freeze
		// every other RPC on that connection — which is Finder's only NFS
		// connection. 2 s is well above a healthy FUSE stat (~µs) and well
		// below the threshold where Finder starts showing a beachball.
		// On timeout we conservatively keep the cache entry (assume the
		// file still exists) rather than block.
		if !e.IsDir {
			// Gate the phantom-file purge on metadata-authority health.
			// 2026-05-16 incident: Redis at 127.0.0.1 was unreachable
			// for ~30 min overnight. During the outage JuiceFS couldn't
			// resolve its own metadata for some paths and FUSE returned
			// ENOENT for files that genuinely exist. The phantom-purge
			// path then deleted those cache entries; when Redis came
			// back, the SQLite store was inconsistent with Redis, and
			// NFS handles that had been issued before the purge pointed
			// at a different inode — producing "Stale NFS file handle"
			// errors on user attempts to write into affected directories.
			//
			// Fix: if Redis was disconnected or reconnected within the
			// cooldown window, skip the purge. A single FUSE-says-gone
			// observation isn't trustworthy when the metadata authority
			// just blipped. The file may genuinely have been deleted —
			// in which case the next stat after the cooldown will catch
			// it — but the cost of being wrong here (stale handle that
			// requires a remount to recover) is much higher than the
			// cost of a few extra seconds of "this stale entry exists."
			if jfs.handler.hasActiveWriter(filename) {
				// QA-19 fix (2026-05-17): NEVER purge a file with an
				// in-flight write handle. The NFS file handle is the
				// inode (see ToHandle/FromHandle); deleting the cache
				// entry removes inodeCache[inode] and the writer's next
				// WRITE RPC gets NFS3ERR_STALE. A concurrent GETATTR
				// (NFSv3 kernel clients periodically refresh attrs under
				// sustained I/O) racing a writeback-mode juicefs sync
				// can momentarily observe ENOENT from FUSE even though
				// the writer's data is in the writeback buffer.
				//
				// Uses the activeWriters refcount (not the sticky
				// writeSizes map) so the gate self-clears the moment
				// the last writer closes. A file that's stale AND has
				// no active writer still gets purged on the next stat.
				// Reproducer: scripts/qa-suite/04-fio.sh seqwrite-1m
				// firing at ~52s into a 512 MiB write.
			} else if jfs.handler.redisClient != nil && jfs.handler.redisClient.RecentlyDegraded(60*time.Second) {
				// Treat the cache entry as authoritative. Skip purge.
			} else {
				fusePath := jfs.fullPath(filename)
				isNotExist, ok := lstatNotExistWithTimeout(fusePath, 2*time.Second)
				if ok && isNotExist {
					jmlog.Warn("purging phantom file (stale cache)", "path", filename)
					jfs.handler.store.DeleteFromCache(filename)
					go jfs.handler.store.Delete(filename)
					return nil, os.ErrNotExist
				}
				// !ok means the Lstat timed out — FUSE is degraded. Treat the
				// cache entry as authoritative for now; the stale entry (if
				// any) will be cleaned up on a future stat once FUSE recovers.
			}
		}

		// If we have a tracked write size that's larger, use it
		if hasWriteSize && writeSize > e.Size {
			clone := *e
			clone.Size = writeSize
			clone.Mtime = time.Now()
			return clone.FileInfo(), nil
		}
		return e.FileInfo(), nil
	}

	// [JM6 tier-1.7] Offline fail-fast for un-pinned, un-cached files.
	//
	// We reach this fallback when SQLite metadata didn't know about
	// the path. Falling through to os.Stat() on FUSE would query
	// JuiceFS → Redis (the metadata authority). When we're offline,
	// that query hangs or times out — bad UX for Finder.
	//
	// Refuse fast with pin.ErrOfflineNotAvailable. The NFS protocol
	// layer (nfs_ongetattr.go / nfs_onlookup.go) maps the sentinel
	// to NFSStatusNXIO. Why NXIO rather than NOENT: NOENT causes
	// macOS to invalidate its file handle cache for the path; after
	// recovery the file would NOT reappear without a remount. NXIO
	// surfaces as "I/O error" to apps but preserves the handle cache,
	// so post-recovery the next Stat succeeds and Finder shows the
	// file again automatically.
	//
	// Pinned-and-ready files bypass this: by construction the pin
	// store knows about them, the FUSE path serves from local cache
	// without touching Redis.
	if pin.IsOffline() && jfs.handler.pinStore != nil {
		canonical := jfs.handler.canonicalize(filename)
		if !jfs.handler.isPinnedReady(canonical) {
			jmlog.Debug("offline: refusing stat of un-pinned, un-cached file",
				"path", filename, "canonical", canonical)
			return nil, pin.ErrOfflineNotAvailable
		}
	}

	// Fallback: stat the file on FUSE directly and cache it
	fusePath := jfs.fullPath(filename)
	info, err := os.Stat(fusePath)
	if err != nil {
		return nil, os.ErrNotExist
	}

	// Cache this stat result into SQLite so we don't fall back again
	var inode uint64
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		inode = st.Ino
	} else {
		inode = jfs.handler.nextSyntheticInode()
	}
	entry := metadata.MakeEntry(filename, info.IsDir(), info.Size(), info.ModTime(), inode)
	go jfs.handler.store.Insert(entry)

	// Override with tracked write size if available
	if hasWriteSize && writeSize > info.Size() {
		return &writeSizeInfo{FileInfo: info, size: writeSize}, nil
	}
	return info, nil
}

// writeSizeInfo wraps FileInfo to override the size with tracked write data.
type writeSizeInfo struct {
	os.FileInfo
	size int64
}

func (w *writeSizeInfo) Size() int64 { return w.size }

func (jfs *juiceFS) Lstat(filename string) (os.FileInfo, error) {
	return jfs.Stat(filename)
}

func (jfs *juiceFS) ReadDir(dirname string) ([]os.FileInfo, error) {
	if dirname == "" || dirname == "." || dirname == "/" {
		dirname = "."
	} else {
		dirname = strings.TrimPrefix(dirname, "/")
	}

	children, err := jfs.handler.store.ListChildren(dirname)
	if err != nil {
		return nil, err
	}

	if len(children) > 0 {
		infos := make([]os.FileInfo, 0, len(children))
		for _, e := range children {
			// Skip macOS metadata files that Stat() would reject
			base := e.Name
			if strings.HasPrefix(base, "._") ||
				base == ".DS_Store" ||
				base == ".Spotlight-V100" ||
				base == ".Trashes" ||
				base == ".fseventsd" ||
				base == ".TemporaryItems" {
				continue
			}
			infos = append(infos, e.FileInfo())
		}
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].Name() < infos[j].Name()
		})
		// Proactively prefetch subdirectories' children in background
		// so Finder's subsequent navigation is instant
		go jfs.handler.prefetchChildren(dirname)
		return infos, nil
	}

	// [JM6 tier-1.7] Offline fail-fast: an empty SQLite ListChildren
	// result while offline likely means we just haven't synced this
	// directory yet — Redis is unreachable. Falling through to
	// os.ReadDir(fusePath) would query JuiceFS → Redis and hang or
	// time out. Return empty fast. The next ReadDir after the network
	// returns will populate via sync. Pinned content above this
	// directory is unaffected (those entries are in SQLite already).
	if pin.IsOffline() {
		jmlog.Debug("offline: empty readdir (no synced entries available)",
			"dirname", dirname)
		return []os.FileInfo{}, nil
	}

	// Fallback: read directly from FUSE and cache into SQLite
	fusePath := jfs.fullPath(dirname)
	dirEntries, err := os.ReadDir(fusePath)
	if err != nil {
		return nil, err
	}
	infos := make([]os.FileInfo, 0, len(dirEntries))
	toInsert := make([]*metadata.Entry, 0, len(dirEntries))
	for _, de := range dirEntries {
		info, err := de.Info()
		if err != nil {
			continue
		}
		infos = append(infos, info)

		// Extract real inode from FUSE stat
		var inode uint64
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			inode = st.Ino
		} else {
			inode = jfs.handler.nextSyntheticInode()
		}
		childPath := path.Join(dirname, info.Name())
		if dirname == "." {
			childPath = info.Name()
		}
		entry := metadata.MakeEntry(childPath, info.IsDir(), info.Size(), info.ModTime(), inode)
		toInsert = append(toInsert, entry)
	}

	// Bulk-insert into SQLite synchronously so subsequent Stat() calls
	// from Finder (which follow immediately after READDIR) hit the cache
	if len(toInsert) > 0 {
		jfs.handler.store.BulkInsert(toInsert, 500)
	}

	return infos, nil
}

// File operations — proxy to JuiceFS FUSE
func (jfs *juiceFS) Open(filename string) (billy.File, error) {
	return jfs.OpenFile(filename, os.O_RDONLY, 0)
}

func (jfs *juiceFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	filename = strings.TrimPrefix(filename, "/")

	// Look up the entry to get inode and size for cache reads
	e := jfs.handler.store.LookupByPath(filename)

	// Detect write intent
	isWrite := flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0

	// Offline-mode gate: refuse OPEN of un-pinned files when offline.
	// This is the strict guarantee — we don't let kernel page cache or
	// FUSE buffering accidentally serve un-pinned bytes when the user
	// is on cellular and explicitly asked us to fail fast.
	//
	// Pinned-and-ready files are allowed through. Writes are always
	// allowed (the user explicitly created them; they go to FUSE cache).
	// Compute pinned-ness once per open. We use this both for the offline
	// gate below AND to skip the read-time offline EIO on pinned files
	// (JuiceFS LRU serves from local SSD; no backend round-trip needed).
	var isPinned bool
	var canonical string
	if !isWrite && jfs.handler.pinStore != nil {
		canonical = jfs.handler.canonicalize(filename)
		isPinned = jfs.handler.isPinnedReady(canonical)
	}

	if !isWrite && pin.IsOffline() && jfs.handler.pinStore != nil && !isPinned {
		// C.2 fix (QA-12, 2026-05-17): before refusing the open,
		// probe whether the SSD cache can serve the first block of
		// this file. If it can, the file is "recently cached" and
		// the user-intent of offline mode (don't trigger backend
		// traffic on un-pinned reads) is still respected — the read
		// path is cache-priority-correct downstream (cachedFile.ReadAt
		// runs the cache reader BEFORE the per-read offline gate at
		// handler.go:1052). Only refuse if the cache can't help.
		//
		// 200ms timeout: covers Redis localhost LRange (sub-ms warm,
		// ≤50ms under load) + first-open APFS dir-cache miss
		// (~10-40ms) + ReadAt of one 4 KiB block. Tight enough that
		// a hanging Redis can't stall OpenFile but loose enough that
		// a legitimately-cached file on a sleepy cold system doesn't
		// get false-refused.
		if !jfs.cacheProbeHit(e) {
			jmlog.Debug("offline: refusing open of un-pinned file",
				"in_mount", filename,
				"canonical", canonical)
			// pin.ErrOfflineNotAvailable → NFS protocol layer maps to
			// NFSStatusNXIO. See Stat fallback comment for the cache-
			// preservation rationale (NXIO doesn't invalidate kernel
			// file handles; NoEnt does).
			return nil, pin.ErrOfflineNotAvailable
		}
		jmlog.Debug("offline: allowing open of cached un-pinned file (probe hit)",
			"in_mount", filename,
			"inode", func() uint64 { if e != nil { return e.Inode }; return 0 }())
	}

	// For read-only opens, try to use fd pool + cache reader
	if !isWrite && e != nil {
		fusePath := jfs.fullPath(filename)
		fd, err := jfs.handler.fdPool.Get(fusePath)
		if err != nil {
			if os.IsNotExist(err) {
				jmlog.Warn("purging phantom entry on open", "path", filename)
				jfs.handler.store.DeleteFromCache(filename)
				go jfs.handler.store.Delete(filename)
			}
			return nil, err
		}
		return &cachedFile{
			name:        filename,
			fuseFD:      fd,
			fusePath:    fusePath,
			fdPool:      jfs.handler.fdPool,
			cacheReader: jfs.handler.cacheReader,
			readahead:   jfs.handler.readahead,
			memBuf:      jfs.handler.memBuf,
			inode:       e.Inode,
			fileSize:    e.Size,
			pinned:      isPinned,
		}, nil
	}

	// For writes, use fd pool to avoid re-opening on every NFS WRITE RPC.
	// The go-nfs library calls OpenFile→Write→Close on every WRITE RPC,
	// so pooling the fd saves an open() + close() syscall per RPC.
	fullPath := jfs.fullPath(filename)

	if isWrite {
		fd, err := jfs.handler.fdPool.GetWrite(fullPath, flag, perm)
		if err != nil {
			return nil, err
		}
		inode := uint64(0)
		if e != nil {
			inode = e.Inode
		} else {
			inode = jfs.handler.nextSyntheticInode()
		}
		entry := &metadata.Entry{Path: filename, Inode: inode}
		// QA-19: register the active writer BEFORE returning. Pair
		// with decActiveWriter in writeFile.Close so the phantom-purge
		// gate in Stat() can see this writer is in flight.
		jfs.handler.incActiveWriter(filename)
		return &writeFile{
			File:    fd,
			name:    filename,
			handler: jfs.handler,
			entry:   entry,
			fdPool:  jfs.handler.fdPool,
			fusePath: fullPath,
		}, nil
	}

	f, err := os.OpenFile(fullPath, flag, perm)
	if err != nil {
		return nil, err
	}
	// Carry forward the same pin snapshot we computed at the top of OpenFile.
	// Writes don't gate (writes are always allowed); reads through this
	// branch (e == nil, no metadata cache) pick up the same offline policy
	// as the cachedFile branch.
	return &billyFile{File: f, name: filename, pinned: isPinned}, nil
}

func (jfs *juiceFS) Create(filename string) (billy.File, error) {
	filename = strings.TrimPrefix(filename, "/")

	// Create on FUSE
	fullPath := jfs.fullPath(filename)
	f, err := os.Create(fullPath)
	if err != nil {
		return nil, err
	}

	// Insert into SQLite immediately with local_only flag
	now := time.Now()
	e := metadata.MakeEntry(filename, false, 0, now, jfs.handler.nextSyntheticInode())
	e.LocalOnly = true
	jfs.handler.store.Insert(e)

	// QA-19: Create returns a writeFile whose Close calls decActiveWriter.
	// Match it with an inc here so the phantom-purge gate sees the writer.
	// Without this, the NFS CREATE → first-write window (which is exactly
	// fio seqwrite's startup path) gets no protection.
	jfs.handler.incActiveWriter(filename)

	return &writeFile{
		File:    f,
		name:    filename,
		handler: jfs.handler,
		entry:   e,
	}, nil
}

func (jfs *juiceFS) Rename(oldpath, newpath string) error {
	oldpath = strings.TrimPrefix(oldpath, "/")
	newpath = strings.TrimPrefix(newpath, "/")

	// Execute on FUSE
	if err := os.Rename(jfs.fullPath(oldpath), jfs.fullPath(newpath)); err != nil {
		return err
	}

	// Invalidate memory buffer for old path
	if jfs.handler.memBuf != nil {
		jfs.handler.memBuf.Invalidate(oldpath)
	}

	// Update in-memory cache FIRST (instant visibility for NFS stats).
	// SQLite writes happen async — they may be blocked by BulkInsert,
	// but the in-memory cache ensures NFS LOOKUP/GETATTR work immediately.
	oldEntry := jfs.handler.store.LookupByPath(oldpath)
	jfs.handler.store.DeleteFromCache(oldpath)
	if oldEntry != nil {
		newEntry := metadata.MakeEntry(newpath, oldEntry.IsDir, oldEntry.Size, oldEntry.Mtime, oldEntry.Inode)
		jfs.handler.store.InsertToCache(newEntry)

		// SQLite update async (won't block NFS)
		go func() {
			jfs.handler.store.Delete(oldpath)
			jfs.handler.store.Insert(newEntry)
		}()

		// Publish rename event
		jfs.handler.publishEvent(metadata.MetadataEvent{
			Op: "rename", Path: newpath, OldPath: oldpath,
			Size: oldEntry.Size, Mtime: oldEntry.Mtime.Unix(),
			Inode: oldEntry.Inode, IsDir: oldEntry.IsDir,
		})
	} else {
		// No cached entry — still do the SQLite ops async
		go func() {
			jfs.handler.store.Delete(oldpath)
			// Stat from FUSE to get the new entry's info
			info, err := os.Stat(jfs.fullPath(newpath))
			if err == nil {
				var inode uint64
				if st, ok := info.Sys().(*syscall.Stat_t); ok {
					inode = st.Ino
				} else {
					inode = jfs.handler.nextSyntheticInode()
				}
				e := metadata.MakeEntry(newpath, info.IsDir(), info.Size(), info.ModTime(), inode)
				jfs.handler.store.Insert(e)
			}
		}()
	}
	return nil
}

func (jfs *juiceFS) Remove(filename string) error {
	filename = strings.TrimPrefix(filename, "/")

	// Invalidate memory buffer
	if jfs.handler.memBuf != nil {
		jfs.handler.memBuf.Invalidate(filename)
	}

	// Delete from SQLite first (immediate visibility)
	e := jfs.handler.store.LookupByPath(filename)
	jfs.handler.store.Delete(filename)

	// Delete on FUSE synchronously — returning success before the file is
	// actually removed causes stale-handle confusion on subsequent operations.
	os.Remove(jfs.fullPath(filename))

	// Publish delete event
	if e != nil {
		jfs.handler.publishEvent(metadata.MetadataEvent{
			Op: "delete", Path: filename, Inode: e.Inode,
		})
	}
	return nil
}

func (jfs *juiceFS) MkdirAll(dirname string, perm os.FileMode) error {
	dirname = strings.TrimPrefix(dirname, "/")

	// Create on FUSE
	if err := os.MkdirAll(jfs.fullPath(dirname), perm); err != nil {
		return err
	}

	// Insert into in-memory cache FIRST (instant visibility for NFS stats)
	now := time.Now()
	e := metadata.MakeEntry(dirname, true, 0, now, jfs.handler.nextSyntheticInode())
	e.LocalOnly = true
	jfs.handler.store.InsertToCache(e)

	// SQLite write async (won't block NFS even if BulkInsert holds the lock)
	go jfs.handler.store.Insert(e)

	jfs.handler.publishEvent(metadata.MetadataEvent{
		Op: "create", Path: dirname, Mtime: now.Unix(),
		Inode: e.Inode, IsDir: true,
	})
	return nil
}

// Remaining billy.Filesystem stubs
func (jfs *juiceFS) Join(elem ...string) string          { return path.Join(elem...) }
func (jfs *juiceFS) TempFile(dir, prefix string) (billy.File, error) { return nil, fmt.Errorf("not implemented") }
func (jfs *juiceFS) Symlink(target, link string) error     { return fmt.Errorf("not supported") }
func (jfs *juiceFS) Readlink(link string) (string, error) { return "", fmt.Errorf("not supported") }
func (jfs *juiceFS) Chroot(p string) (billy.Filesystem, error) { return nil, fmt.Errorf("not supported") }
func (jfs *juiceFS) Root() string { return "/" }

// juiceChange implements billy.Change for write operations.
type juiceChange struct {
	handler *JuiceMountHandler
}

func (jc *juiceChange) Chmod(name string, mode os.FileMode) error              { return nil }
func (jc *juiceChange) Chown(name string, uid, gid int) error                  { return nil }
func (jc *juiceChange) Lchown(name string, uid, gid int) error                 { return nil }
func (jc *juiceChange) Chtimes(name string, atime, mtime time.Time) error      { return nil }

// cachedFile implements billy.File with two-tier read path:
// 1. Direct SSD pread (bypasses FUSE) — if cache reader is available and block is cached
// 2. JuiceFS FUSE pread (fallback) — populates SSD cache for future reads
type cachedFile struct {
	name        string
	fuseFD      *os.File
	fusePath    string
	fdPool      *FDPool
	cacheReader *cache.Reader
	readahead   *ReadaheadManager
	memBuf      *MemoryBuffer
	inode       uint64
	fileSize    int64
	closed      bool

	// Decided at OpenFile time: whether this file passed the pin check.
	// Pinned files are allowed to fall through to FUSE during offline mode
	// (JuiceFS serves from its local LRU; no backend round-trip).
	// Un-pinned files never reach this struct in offline mode (open-time
	// gate refuses them before we construct the cachedFile).
	pinned bool
}

func (f *cachedFile) Name() string { return f.name }

func (f *cachedFile) ReadAt(p []byte, off int64) (int, error) {
	// Priority 1: Memory buffer (zero-syscall, for small files like .prproj, LUTs)
	if f.memBuf != nil {
		n, hit := f.memBuf.ReadAt(f.name, p, off, f.fileSize, f.fusePath)
		if hit {
			if f.readahead != nil {
				f.readahead.OnRead(f.inode, off, n, f.name)
			}
			metrics.Default().AddBytesRead(int64(n))
			// [JM6] Surface EOF when the buffered file ran out under us.
			// memBuf returns (0, true) when off is past the buffered
			// length; callers iterating in a short-read loop (e.g. the
			// new subdivided onRead path) would otherwise re-issue at
			// the same offset forever because they can't distinguish
			// "end of file" from "transient zero-byte response". This
			// path is reached for small files <= 32 KiB that bypass the
			// upstream size-clamp in nfs_onread.go.
			if n == 0 {
				return 0, io.EOF
			}
			return n, nil
		}
	}

	// Priority 2: Direct SSD cache read (bypasses FUSE)
	if f.cacheReader != nil {
		n, err := f.cacheReader.ReadBlock(context.Background(), f.inode, off, p)
		if err == nil && n > 0 {
			if f.readahead != nil {
				f.readahead.OnRead(f.inode, off, n, f.name)
			}
			metrics.Default().AddBytesRead(int64(n))
			return n, nil
		}
	}

	// Offline mode short-circuit: if the user has flipped to offline, we
	// don't fall through to FUSE for un-pinned files. JuiceFS would
	// otherwise try to GET the missing block from S3, which on cellular
	// can take 30+ seconds and will tarpit the NLE waiting for the read.
	// Return EIO immediately so the NLE shows "media offline" instead of
	// beachballing.
	//
	// PINNED files are exempt: JuiceFS already has their bytes in its
	// local LRU cache (the prefetcher pulled them at pin time), so a FUSE
	// read is fully local and safe even on cellular. Without this exemption
	// the read-time gate would refuse pinned files whose blocks happen to
	// not be in our SSD cache reader (different cache layer than JuiceFS),
	// re-introducing the very "media offline" UX cliff the offline-pin
	// feature exists to prevent.
	if pin.IsOffline() && !f.pinned {
		return 0, pin.ErrOfflineNotAvailable
	}

	// Priority 3: JuiceFS FUSE read (populates SSD cache for next time)
	n, err := f.fuseFD.ReadAt(p, off)
	if n > 0 && f.readahead != nil {
		f.readahead.OnRead(f.inode, off, n, f.name)
	}
	if n > 0 {
		metrics.Default().AddBytesRead(int64(n))
	}
	return n, err
}

func (f *cachedFile) Read(p []byte) (int, error)    { return f.fuseFD.Read(p) }
func (f *cachedFile) Write(p []byte) (int, error)   { return f.fuseFD.Write(p) }
func (f *cachedFile) Seek(offset int64, whence int) (int64, error) { return f.fuseFD.Seek(offset, whence) }
func (f *cachedFile) Lock() error                    { return nil }
func (f *cachedFile) Unlock() error                  { return nil }
func (f *cachedFile) Truncate(size int64) error      { return f.fuseFD.Truncate(size) }

func (f *cachedFile) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true
	f.fdPool.Release(f.fusePath)
	return nil
}

// writeFile wraps os.File for write operations, tracking written size
// and updating SQLite on close. Uses fd pool to avoid re-opening on every RPC.
type writeFile struct {
	*os.File
	name       string
	handler    *JuiceMountHandler
	entry      *metadata.Entry
	writtenEnd int64 // highest byte position written
	fdPool     *FDPool
	fusePath   string
}

func (f *writeFile) Name() string  { return f.name }
func (f *writeFile) Lock() error   { return nil }
func (f *writeFile) Unlock() error { return nil }

func (f *writeFile) Write(p []byte) (int, error) {
	n, err := f.File.Write(p)
	if n > 0 {
		pos, _ := f.File.Seek(0, io.SeekCurrent)
		if pos > f.writtenEnd {
			f.writtenEnd = pos
			f.handler.trackWriteSize(f.name, pos)
		}
		metrics.Default().AddBytesWritten(int64(n))
	}
	return n, err
}

func (f *writeFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(p, off)
	if n > 0 {
		end := off + int64(n)
		if end > f.writtenEnd {
			f.writtenEnd = end
			f.handler.trackWriteSize(f.name, end)
		}
		metrics.Default().AddBytesWritten(int64(n))
	}
	return n, err
}

func (f *writeFile) Close() error {
	// QA-19: release the active-writer refcount paired with the
	// incActiveWriter at OpenFile time. Done first thing so a panic
	// in any of the downstream cache/SQLite work below doesn't leak
	// the refcount (would permanently block phantom-purge for the path).
	f.handler.decActiveWriter(f.name)

	// Do NOT Sync() here — the go-nfs library calls OpenFile+Write+Close on
	// every WRITE RPC, so syncing here would flush to MinIO on every RPC.
	// Instead, rely on JuiceFS's writeback buffer and its own flush timing.
	// Release fd back to pool instead of closing (avoids reopen on next RPC).
	if f.fdPool != nil {
		f.fdPool.Release(f.fusePath)
	} else {
		f.File.Close()
	}

	// Invalidate memory buffer so subsequent reads see fresh data
	if f.handler.memBuf != nil {
		f.handler.memBuf.Invalidate(f.name)
	}

	// QA-16 fix (2026-05-17): use the HIGH-WATER mark from the shared
	// writeSizes accumulator, NOT this RPC's per-instance writtenEnd.
	// Under concurrent dispatch, each WRITE RPC has its own writeFile
	// with its own writtenEnd that only reflects ITS write. A low-offset
	// RPC closing last would have used a small writtenEnd here,
	// truncating the SQLite-recorded size even though earlier RPCs
	// wrote past it. The shared writeSizes map (updated MAX-wise by
	// trackWriteSize) is the true logical size.
	//
	// We also do NOT delete the writeSizes entry on Close — under
	// concurrent dispatch, another RPC may still be writing past this
	// one's position. Stale entries are cleaned up lazily by the next
	// Stat() comparing against SQLite, or could be aged out by a future
	// sweep. The previous delete-on-close created a window where Stat()
	// would briefly see no in-flight tracking and fall back to the old
	// SQLite size.
	f.handler.writeSizeMu.Lock()
	finalSize, ok := f.handler.writeSizes[f.name]
	if !ok || f.writtenEnd > finalSize {
		finalSize = f.writtenEnd
		f.handler.writeSizes[f.name] = finalSize
	}
	f.handler.writeSizeMu.Unlock()

	// Update SQLite with the high-water size. UpdateSize itself uses
	// MAX semantics (see metadata/store.go) so the order of concurrent
	// Close() calls no longer matters — values monotonically increase.
	now := time.Now()
	if finalSize > 0 {
		f.handler.store.UpdateSize(f.name, finalSize, now)
	}

	// Publish create/update event (async)
	f.handler.publishEvent(metadata.MetadataEvent{
		Op: "create", Path: f.name,
		Size: finalSize, Mtime: now.Unix(),
		Inode: f.entry.Inode,
	})
	return nil
}

// billyFile wraps os.File to implement billy.File (for writes / non-cached opens).
//
// pinned is captured at OpenFile time, same semantics as cachedFile.pinned —
// a per-open snapshot used to gate the read-time offline check. Without it,
// a file opened during a brief online window and then read after offline
// flips on would bypass the gate and stall on FUSE → backend.
type billyFile struct {
	*os.File
	name   string
	pinned bool
}

func (f *billyFile) Name() string { return f.name }
func (f *billyFile) Lock() error  { return nil }
func (f *billyFile) Unlock() error { return nil }
func (f *billyFile) Truncate(size int64) error { return f.File.Truncate(size) }

// Read overrides *os.File.Read to enforce the read-time offline gate on
// un-pinned files. Pinned files fall through to FUSE/JuiceFS LRU as usual.
func (f *billyFile) Read(p []byte) (int, error) {
	if pin.IsOffline() && !f.pinned {
		return 0, pin.ErrOfflineNotAvailable
	}
	return f.File.Read(p)
}

// ReadAt is the hot path for NFS READ RPCs (which always carry an offset).
func (f *billyFile) ReadAt(p []byte, off int64) (int, error) {
	if pin.IsOffline() && !f.pinned {
		return 0, pin.ErrOfflineNotAvailable
	}
	return f.File.ReadAt(p, off)
}

// rootDirInfo is the FileInfo for the root directory.
// Sys() returns a *syscall.Stat_t with the current user's UID/GID so that
// Finder doesn't show the red "no access" badge on the mount root.
type rootDirInfo struct{}

func (r *rootDirInfo) Name() string      { return "" }
func (r *rootDirInfo) Size() int64       { return 0 }
func (r *rootDirInfo) Mode() fs.FileMode { return fs.ModeDir | 0755 }
func (r *rootDirInfo) ModTime() time.Time { return time.Now() }
func (r *rootDirInfo) IsDir() bool       { return true }
func (r *rootDirInfo) Sys() any {
	return &syscall.Stat_t{
		Ino:   1,
		Uid:   uint32(os.Getuid()),
		Gid:   uint32(os.Getgid()),
		Nlink: 2,
	}
}

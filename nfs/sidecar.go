package nfs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/metadata"
)

// Sidecar cache (nav-latency crux, 2026-07-10). A Finder listing of a folder
// full of `._` AppleDouble sidecars takes MINUTES on a slow/cellular link:
// macOS reads every `._Foo` to merge the resource-fork / Finder-info when it
// displays `Foo`, and each is a ~700ms backend round-trip over the tunnel.
// (Measured live: a 398-child / 199-`._` folder = READDIR 42ms but `ls -la`
// 4-min timeout with +248 cold backend reads; a single `._` read = 777ms.)
// The directory LISTING is already RAM-served from the mirror; the killer is
// the per-entry sidecar CONTENT read.
//
// Fix: a bounded in-RAM cache of complete `._` sidecar bodies, so the second
// (and warmer-front-run first) read of a folder is RAM-served.
//
// CORRECTNESS (the membuf-stale-partial lesson, [[project_membuf_stale_image]]):
//   - Cache ONLY a COMPLETE file body (never assemble from partial reads) —
//     populated by a single read that covers [0,size) or by the warmer's
//     dedicated full read.
//   - Validate every serve against the MIRROR's live (mtime,size) for the path
//     (a RAM lookup, ns): a changed sidecar has a new mtime/size, so its stale
//     cache entry can never match → miss → fresh read. Resource-fork edits
//     (Finder tags/comments) rewrite the `._` file and bump mtime.
//   - BYPASS entirely while a writer is active for the path (a copy in flight):
//     never serve a half-written sidecar.
//
// Kill switch: JM_SIDECAR_CACHE=0. Only `._`-prefixed files ≤ sidecarMaxFile
// are eligible — real media never enters this cache.
const (
	sidecarMaxFile   = 128 << 10 // only cache `._` files this small (they are ~4KB)
	sidecarCacheCap  = 128 << 20 // unique body bytes; identical AppleDouble bodies are interned
	sidecarEntryCap  = 262144    // bound path/metadata overhead even when every body deduplicates
	sidecarNamePfx   = "._"
	sidecarNamePfxLn = 2
)

type sidecarEntry struct {
	proof      *sidecarReadProof // current-process, authoritative EOF; never persisted
	snapshot   *sidecarSnapshot  // read-only durable last-observed image; never a write seed
	data       []byte
	contentKey string
	mtime      int64
	size       int64
	lastUsed   int64
}

type sidecarBlob struct {
	key  string
	data []byte
	refs int
}

type sidecarCacheState struct {
	present bool
	mtime   int64
	size    int64
	length  int
}

// sidecarCache is a bounded, mtime-validated LRU of complete `._` bodies.
type sidecarCache struct {
	enabled  bool
	mu       sync.Mutex
	m        map[string]*sidecarEntry
	blobs    map[string]*sidecarBlob
	bytes    int64 // unique body bytes, not logical bytes across paths
	maxBytes int64
	maxFiles int
	tick     int64

	// Disk persistence (sidecar_persist.go). persistPath is set once by
	// enablePersist; dirty counts puts/invalidates since the last snapshot
	// (atomic — bumped on the serve-adjacent put path without the saver
	// needing the cache lock).
	persistPath     string
	persistStop     chan struct{}
	persistDone     chan struct{}
	persistStopOnce sync.Once
	persistWriteMu  sync.Mutex
	dirty           int64
}

func newSidecarCache() *sidecarCache {
	max := int64(sidecarCacheCap)
	if v := os.Getenv("JM_SIDECAR_CACHE_MB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 4 && n <= 4096 {
			max = int64(n) << 20
		}
	}
	return &sidecarCache{
		enabled:  os.Getenv("JM_SIDECAR_CACHE") != "0",
		m:        make(map[string]*sidecarEntry),
		blobs:    make(map[string]*sidecarBlob),
		maxBytes: max,
		maxFiles: sidecarEntryCap,
	}
}

// isSidecarName reports whether base is a `._` AppleDouble sidecar name.
func isSidecarName(base string) bool {
	return len(base) > sidecarNamePfxLn && base[0] == '.' && base[1] == '_'
}

// cacheableMetaName reports whether base is a Finder-metadata file the
// sidecar cache may serve: `._` AppleDouble sidecars AND `.DS_Store`
// (2026-07-13, cellular): Finder reads a folder's .DS_Store on every open —
// one more tunnel round-trip chain per navigation. It gets the exact same
// correctness treatment as `._` bodies: complete-read-only populate,
// per-serve (mtime,size) mirror validation, and write-open invalidation
// (Finder rewrites .DS_Store constantly — the invalidate hook plus the
// mtime bump make a stale serve structurally impossible). The WARM pass
// stays `._`-only: .DS_Store is one file per dir and demand-populates on
// Finder's own first read.
func cacheableMetaName(base string) bool {
	return isSidecarName(base) || base == ".DS_Store"
}

// get returns the cached complete body for path IFF it is present and its
// (mtime,size) still match the caller-supplied current values (from the
// mirror). Touches LRU recency on a hit.
func (c *sidecarCache) get(path string, curMtime, curSize int64) ([]byte, bool) {
	if c == nil || !c.enabled {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[path]
	if !ok || e.snapshot != nil || e.mtime != curMtime || e.size != curSize {
		return nil, false
	}
	c.tick++
	e.lastUsed = c.tick
	return e.data, true
}

// state returns validation metadata only, never the cached Finder metadata
// body. It is used exclusively to explain a fail-closed offline rewrite in
// logs; without this distinction an absent entry and a stale tag are
// operationally indistinguishable.
func (c *sidecarCache) state(path string) sidecarCacheState {
	if c == nil || !c.enabled {
		return sidecarCacheState{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.m[path]
	if e == nil {
		return sidecarCacheState{}
	}
	return sidecarCacheState{present: true, mtime: e.mtime, size: e.size, length: len(e.data)}
}

// put stores a COMPLETE body (len(data)==size) tagged with (mtime,size),
// evicting LRU entries to stay under the byte cap. A partial (len!=size) is
// refused — the anti-membuf-stale guard.
func (c *sidecarCache) put(path string, data []byte, mtime, size int64) {
	c.putWithProof(path, data, mtime, size, nil, nil)
}

func (c *sidecarCache) putWithProof(path string, data []byte, mtime, size int64, proof *sidecarReadProof, snapshot *sidecarSnapshot) {
	if c == nil || !c.enabled || size <= 0 || size > sidecarMaxFile || int64(len(data)) != size {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.putLocked(path, data, mtime, size)
	if proof != nil {
		owned := *proof
		owned.data = c.m[path].data
		c.m[path].proof = &owned
		c.m[path].snapshot = snapshot
	}
	for (c.bytes > c.maxBytes || len(c.m) > c.fileLimitLocked()) && len(c.m) > 1 {
		c.evictOneLRULocked()
	}
	atomic.AddInt64(&c.dirty, 1)
	metrics.Default().IncSidecarCachePut()
}

// putIfEntry replaces only the object observed before a delayed descriptor
// operation. Identical tags are insufficient: a successor may have different
// bytes with the same size and mtime.
func (c *sidecarCache) putIfEntry(path string, observed *sidecarEntry, data []byte, mtime, size int64) bool {
	if c == nil || !c.enabled || size <= 0 || size > sidecarMaxFile || int64(len(data)) != size {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m[path] != observed {
		return false
	}
	c.putLocked(path, data, mtime, size)
	for (c.bytes > c.maxBytes || len(c.m) > c.fileLimitLocked()) && len(c.m) > 1 {
		c.evictOneLRULocked()
	}
	atomic.AddInt64(&c.dirty, 1)
	metrics.Default().IncSidecarCachePut()
	return true
}

// ensureInternLocked upgrades test fixtures and legacy persistence entries that
// were constructed before body interning existed. Exact body strings are the
// map key, so deduplication never relies on a probabilistic hash for bytes that
// may contain real Finder tags or resource forks.
func (c *sidecarCache) ensureInternLocked() {
	if c.blobs != nil {
		return
	}
	c.blobs = make(map[string]*sidecarBlob)
	c.bytes = 0
	for _, e := range c.m {
		key := string(e.data)
		if blob := c.blobs[key]; blob != nil {
			blob.refs++
			e.data = blob.data
			e.contentKey = blob.key
			continue
		}
		blob := &sidecarBlob{key: key, data: e.data, refs: 1}
		c.blobs[key] = blob
		e.contentKey = blob.key
		c.bytes += int64(len(blob.data))
	}
}

func (c *sidecarCache) fileLimitLocked() int {
	if c.maxFiles > 0 {
		return c.maxFiles
	}
	return sidecarEntryCap
}

// putLocked stores one validated complete body and owns/canonicalizes its
// bytes. The caller holds c.mu. Dirty/metrics accounting belongs to the caller
// so persistence restore can remain observationally quiet.
func (c *sidecarCache) putLocked(path string, data []byte, mtime, size int64) {
	c.ensureInternLocked()
	if old := c.m[path]; old != nil {
		c.releaseEntryLocked(old)
	}
	key := string(data)
	blob := c.blobs[key]
	if blob == nil {
		buf := append([]byte(nil), data...) // caller's read buffer is pooled/reused
		blob = &sidecarBlob{key: key, data: buf, refs: 1}
		c.blobs[key] = blob
		c.bytes += int64(len(buf))
	} else {
		blob.refs++
	}
	c.tick++
	c.m[path] = &sidecarEntry{
		data: blob.data, contentKey: blob.key,
		mtime: mtime, size: size, lastUsed: c.tick,
	}
}

func (c *sidecarCache) releaseEntryLocked(e *sidecarEntry) {
	if e == nil {
		return
	}
	c.ensureInternLocked()
	blob := c.blobs[e.contentKey]
	if blob == nil {
		return
	}
	blob.refs--
	if blob.refs <= 0 {
		delete(c.blobs, blob.key)
		c.bytes -= int64(len(blob.data))
	}
}

// invalidate drops a path (used when a writer opens the sidecar for write) and
// returns the validation tag that was removed. Callers can distinguish a
// meaningful cache-generation transition from the common cold-cache no-op
// without ever logging or exposing the Finder metadata body itself.
func (c *sidecarCache) invalidate(path string) (sidecarCacheState, bool) {
	if c == nil || !c.enabled {
		return sidecarCacheState{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.m[path]; ok {
		state := sidecarCacheState{present: true, mtime: e.mtime, size: e.size, length: len(e.data)}
		c.releaseEntryLocked(e)
		delete(c.m, path)
		atomic.AddInt64(&c.dirty, 1)
		return state, true
	}
	return sidecarCacheState{}, false
}

func (c *sidecarCache) captureEntry(path string) *sidecarEntry {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[path]
}

// A delayed predecessor effect never deletes a newer same-path sidecar body.
func (c *sidecarCache) invalidateIfEntry(path string, observed *sidecarEntry) bool {
	if c == nil || observed == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m[path] != observed {
		return false
	}
	c.releaseEntryLocked(observed)
	delete(c.m, path)
	atomic.AddInt64(&c.dirty, 1)
	return true
}

// retag advances the validation metadata for an already-verified complete
// body without changing its bytes. The drainer captures the whole AppleDouble
// image before deleting the spool file; onSpoolDrained then publishes the
// final mirror mtime. Without carrying that new tag onto the saved body, the
// immediately-following offline Finder continuation rejects its own freshly
// drained seed as stale and surfaces NFS3ERR_ACCES.
func (c *sidecarCache) retag(path string, mtime, size int64) bool {
	if c == nil || !c.enabled {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.m[path]
	if e == nil || e.snapshot != nil || e.size != size || int64(len(e.data)) != size {
		return false // path-only retag cannot downgrade a read snapshot to a weak write seed
	}
	e.proof = nil // path-only drain callbacks cannot renew a strong content proof
	e.mtime = mtime
	atomic.AddInt64(&c.dirty, 1)
	return true
}

func (c *sidecarCache) evictOneLRULocked() {
	var victim string
	var oldest int64 = 1<<63 - 1
	for k, e := range c.m {
		if e.lastUsed < oldest {
			oldest, victim = e.lastUsed, k
		}
	}
	if victim != "" {
		c.releaseEntryLocked(c.m[victim])
		delete(c.m, victim)
	}
}

func (c *sidecarCache) stats() (files int, bytes int64) {
	if c == nil {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.m), c.bytes
}

// ---- handler integration ----

// A known wrong or stopped selected volume cannot fall through to a weak
// metadata body. Offline transport loss alone leaves last-observed reads live.
func (h *JuiceMountHandler) sidecarScopeRefused() bool {
	if h == nil {
		return false
	}
	b := h.cacheBinding.Load()
	return b != nil && (b.identityRefused.Load() || b.session != nil && b.session.SnapshotRefused())
}

// sidecarCurrentMeta returns the mirror's live (mtime unix, size) for a path,
// ok=false when the mirror has no row (never serve/populate a cache entry for a
// path the mirror can't vouch for). RAM lookup, ns.
func (h *JuiceMountHandler) sidecarCurrentMeta(name string) (mtime, size int64, ok bool) {
	if h == nil || h.store == nil {
		return 0, 0, false
	}
	e := h.store.LookupByPath(name)
	if e == nil {
		return 0, 0, false
	}
	size = e.Size
	// A locally-spooled writer publishes its growing size through writeSizes
	// before the final drain updates the durable mirror row. Stat/Lstat already
	// use that high-water mark; use the same effective size when validating a
	// complete metadata snapshot so a just-written 152-byte AppleDouble header
	// can seed the next offline range write safely.
	h.writeSizeMu.Lock()
	if n := h.writeSizes[name]; n > size {
		size = n
	}
	h.writeSizeMu.Unlock()
	return e.Mtime.Unix(), size, true
}

// metadataSnapshotStart returns the current write generation only when no
// write handle is active for path. A later write-open always advances the
// generation before invalidating the old sidecar body.
func (h *JuiceMountHandler) metadataSnapshotStart(name string) (uint64, bool) {
	if h == nil || h.sidecar == nil || !h.sidecar.enabled || !cacheableMetaName(path.Base(name)) {
		return 0, false
	}
	h.activeWritersMu.Lock()
	defer h.activeWritersMu.Unlock()
	if h.activeWriters[name] != 0 {
		return 0, false
	}
	return h.activeWriterGen[name], true
}

// publishMetadataSnapshot inserts body only if no writer started while it was
// being read and the mirror still validates the same complete image. Holding
// activeWritersMu across the tiny RAM-cache put makes the check-and-publish
// atomic with incActiveWriter; the next writer cannot miss invalidating it.
func (h *JuiceMountHandler) publishMetadataSnapshot(name string, generation uint64, body []byte, mtime, size int64) {
	if h == nil || h.sidecar == nil || int64(len(body)) != size {
		return
	}
	curMtime, curSize, ok := h.sidecarCurrentMeta(name)
	if !ok || curMtime != mtime || curSize != size {
		return
	}
	h.activeWritersMu.Lock()
	defer h.activeWritersMu.Unlock()
	if h.activeWriters[name] != 0 || h.activeWriterGen[name] != generation {
		return
	}
	h.sidecar.put(name, body, mtime, size)
}

// cacheWrittenMetadataFromSpool snapshots a complete contiguous AppleDouble
// or .DS_Store body from the local SSD. Partial/preallocated images fail
// closed: without every byte [0,size), they are not safe offline pre-images.
func (h *JuiceMountHandler) cacheWrittenMetadataFromSpool(name string, entry *SpoolEntry) {
	if entry == nil {
		return
	}
	generation, ok := h.metadataSnapshotStart(name)
	if !ok {
		return
	}
	size := entry.WrittenEnd()
	if size <= 0 || size > sidecarMaxFile || entry.ContiguousEnd() != size {
		return
	}
	mtime, mirrorSize, ok := h.sidecarCurrentMeta(name)
	if !ok || mirrorSize != size {
		return
	}
	fd, err := entry.OpenForRead()
	if err != nil {
		return
	}
	defer fd.Close()
	body := make([]byte, size)
	n, err := fd.ReadAt(body, 0)
	if n != len(body) || (err != nil && !errors.Is(err, io.EOF)) {
		return
	}
	h.publishMetadataSnapshot(name, generation, body, mtime, size)
}

// cacheWrittenMetadataFromFUSE is the direct-write twin. Reads are bounded by
// the foreground FUSE deadline and data ceiling so cache maintenance can never
// reintroduce an unbounded Finder close. The mirror supplies the exact complete
// size; a timeout/short read simply leaves the cache cold and preserves the
// existing fail-closed offline behavior.
func (h *JuiceMountHandler) cacheWrittenMetadataFromFUSE(name string, fd *os.File) {
	// This is optional post-write cache maintenance. Under the cellular
	// acceptance guard, leave the just-written metadata cache cold instead of
	// issuing a content read through FUSE. The write itself has already
	// completed and is deliberately unaffected.
	if fd == nil || h.testRefuseFUSEContentReads {
		return
	}
	generation, ok := h.metadataSnapshotStart(name)
	if !ok {
		return
	}
	mtime, size, ok := h.sidecarCurrentMeta(name)
	if !ok || size <= 0 || size > sidecarMaxFile {
		return
	}
	proof := h.beginStrongSidecarRead(name)
	if !sidecarSourceMatches(proof, fd, metrics.FUSESrcForeground) {
		proof = nil
	}
	body := make([]byte, size+1)
	n, err := boundedFUSEFileReadAt(fd, body, 0, fuseReadTimeout())
	if int64(n) != size || !errors.Is(err, io.EOF) {
		return
	}
	h.publishMetadataSnapshot(name, generation, body[:n], mtime, size)
	if sidecarSourceMatches(proof, fd, metrics.FUSESrcForeground) {
		h.publishStrongSidecarRead(proof, body[:n])
	}
}

// sidecarServe attempts to serve a read of a `._` file from the RAM cache.
// Returns (n, true) when served; (0, false) to fall through to the FUSE read.
// Validated against the live mirror meta and bypassed while a writer is active.
func (h *JuiceMountHandler) sidecarServe(name string, p []byte, off int64) (int, bool) {
	if h == nil || h.sidecar == nil || !h.sidecar.enabled || h.sidecarScopeRefused() {
		return 0, false
	}
	if !cacheableMetaName(path.Base(name)) {
		return 0, false
	}
	if h.hasActiveWriter(name) {
		return 0, false // never serve a half-written sidecar
	}
	mtime, size, ok := h.sidecarCurrentMeta(name)
	if !ok || size <= 0 || size > sidecarMaxFile {
		return 0, false
	}
	data, hit := h.sidecar.get(name, mtime, size)
	if !hit {
		metrics.Default().IncSidecarCacheMiss()
		return 0, false
	}
	if off >= int64(len(data)) {
		metrics.Default().IncSidecarCacheHit()
		return 0, true // past EOF — 0 bytes, served (caller maps to io.EOF)
	}
	n := copy(p, data[off:])
	metrics.Default().IncSidecarCacheHit()
	metrics.Default().AddBytesRead(int64(n))
	return n, true
}

// boundSidecarServe is the exact-generation dormant serve path for the
// future bound-producer activation. It never falls back to a weak RAM tag.
func (h *JuiceMountHandler) boundSidecarServe(name string, p []byte, off int64) (int, bool) {
	if h == nil || h.store == nil || !cacheableMetaName(path.Base(name)) || h.hasActiveWriter(name) || off < 0 {
		return 0, false
	}
	image, bound, valid, err := h.store.BoundSidecarBody(name)
	if err != nil || !bound || !valid {
		return 0, false
	}
	n := 0
	if off < int64(len(image.Body)) {
		n = copy(p, image.Body[off:])
	}
	check, _, current, err := h.store.BoundSidecarBody(name)
	if err != nil || !current || check.Token != image.Token || check.SourceSHA256 != image.SourceSHA256 || h.hasActiveWriter(name) {
		clear(p[:n])
		return 0, false
	}
	return n, true
}

// sidecarMaybePopulate caches a `._` body after a COMPLETE read (off==0 and the
// read returned the whole file). Called on the FUSE read fall-through path.
func (h *JuiceMountHandler) sidecarMaybePopulate(name string, off int64, data []byte, fileSize int64) {
	if h == nil || h.sidecar == nil || !h.sidecar.enabled {
		return
	}
	if off != 0 || fileSize <= 0 || fileSize > sidecarMaxFile || int64(len(data)) != fileSize {
		return // only a complete single-read [0,size) populates
	}
	if !cacheableMetaName(path.Base(name)) || h.hasActiveWriter(name) {
		return
	}
	mtime, size, ok := h.sidecarCurrentMeta(name)
	if !ok || size != fileSize {
		return // mirror disagrees on size — don't cache a maybe-stale body
	}
	h.sidecar.put(name, data, mtime, size)
}

// offlineMetadataSeed returns a private copy of a complete, mirror-validated
// Finder metadata body. It is the only safe basis for an offline in-place
// rewrite: without every untouched byte locally available, the write must keep
// failing closed just like an in-place edit of ordinary media.
func (h *JuiceMountHandler) offlineMetadataSeed(name string) ([]byte, bool) {
	if h == nil || h.sidecar == nil || h.sidecarScopeRefused() || !cacheableMetaName(path.Base(name)) {
		return nil, false
	}
	// A live source proof retains the pre-existing write-seed continuity.
	// A restored read snapshot has no such proof and is never a write seed.
	if proof := h.strongSidecarRead(name); proof != nil {
		private := append([]byte(nil), proof.data...)
		if h.strongSidecarRead(name) == proof {
			return private, true
		}
		clear(private)
		return nil, false
	}
	mtime, size, ok := h.sidecarCurrentMeta(name)
	if !ok || size <= 0 || size > sidecarMaxFile {
		return nil, false
	}
	data, ok := h.sidecar.get(name, mtime, size)
	if !ok || int64(len(data)) != size {
		return nil, false
	}
	return append([]byte(nil), data...), true
}

// offlineBoundMetadataSeed returns the exact durable generation for a future
// producer birth CAS. It does not itself authorize a write or mint an FD proof.
func (h *JuiceMountHandler) offlineBoundMetadataSeed(name string) ([]byte, metadata.MirrorToken, bool) {
	if h == nil || h.store == nil || !cacheableMetaName(path.Base(name)) || h.hasActiveWriter(name) {
		return nil, metadata.MirrorToken{}, false
	}
	image, bound, valid, err := h.store.BoundSidecarBody(name)
	if err != nil || !bound || !valid || h.hasActiveWriter(name) {
		return nil, metadata.MirrorToken{}, false
	}
	return append([]byte(nil), image.Body...), image.Token, true
}

// cacheDrainedMetadata captures the authoritative whole spool image before the
// drainer evicts it and tags it with the timestamp actually stored on the
// backend. Finder commonly revisits a directory's `._` sidecar after its first
// row has already drained; retaining this bounded verified body lets that
// continuation be copied up safely if connectivity changes in between.
func (h *JuiceMountHandler) cacheDrainedMetadata(nfsPath string, size int64, mtime time.Time) {
	if h == nil || h.sidecar == nil || size <= 0 || size > sidecarMaxFile {
		return
	}
	nfsPath = strings.TrimPrefix(nfsPath, "/")
	if !cacheableMetaName(path.Base(nfsPath)) {
		return
	}
	spool := h.spool.Load()
	if spool == nil {
		jmlog.Warn("sidecar snapshot rejected after drain", "path", nfsPath, "reason", "spool-unavailable")
		return
	}
	entry, ok := spool.LookupActive(nfsPath)
	if !ok {
		jmlog.Warn("sidecar snapshot rejected after drain", "path", nfsPath, "reason", "active-entry-missing", "drained_size", size)
		return
	}
	if entry.WrittenEnd() != size {
		jmlog.Warn("sidecar snapshot rejected after drain", "path", nfsPath, "reason", "spool-size-mismatch",
			"drained_size", size, "written_end", entry.WrittenEnd(), "contiguous_end", entry.ContiguousEnd())
		return
	}
	body, err := os.ReadFile(entry.SpoolFilePath())
	if err != nil {
		jmlog.Warn("sidecar snapshot rejected after drain", "path", nfsPath, "reason", "spool-read-failed", "error", err.Error())
		return
	}
	if int64(len(body)) != size {
		jmlog.Warn("sidecar snapshot rejected after drain", "path", nfsPath, "reason", "spool-read-size-mismatch",
			"drained_size", size, "read_size", len(body))
		return
	}
	currentMtime, currentSize, ok := h.sidecarCurrentMeta(nfsPath)
	if !ok {
		jmlog.Warn("sidecar snapshot rejected after drain", "path", nfsPath, "reason", "mirror-entry-missing")
		return
	}
	if currentSize != size || currentMtime != mtime.Unix() {
		jmlog.Warn("sidecar snapshot rejected after drain", "path", nfsPath, "reason", "mirror-tag-mismatch",
			"drained_size", size, "drained_mtime", mtime.Unix(), "mirror_size", currentSize, "mirror_mtime", currentMtime)
		return
	}
	h.sidecar.put(nfsPath, body, currentMtime, size)
}

// warmSidecar reads a `._` file fully from FUSE and caches it (the warmer's
// per-file worker). Bounded open; complete-read-only.
func (h *JuiceMountHandler) warmSidecar(name, fusePath string) bool {
	if h == nil || h.sidecar == nil || !h.sidecar.enabled || h.testRefuseFUSEContentReads {
		return false
	}
	mtime, size, ok := h.sidecarCurrentMeta(name)
	if !ok || size <= 0 || size > sidecarMaxFile {
		return false
	}
	if h.strongSidecarRead(name) != nil {
		return false // already warm
	}
	if h.hasActiveWriter(name) {
		return false
	}
	proof := h.beginStrongSidecarRead(name)
	// BACKGROUND work — up to sidecarWarmSem(3) × sidecarWarmParallel(16) = 48
	// concurrent warms. The FUSESrcSidecarWarm label is what routes this to
	// warmGate instead of the 24-slot FOREGROUND nfsLstatGate
	// (fuseGateForSource, audit P0): 48-way opportunistic warming could
	// otherwise hold every foreground slot on a high-latency link, so a user
	// navigating right then queued behind work nobody was waiting for.
	f, err, opened := openFileWithTimeout(metrics.FUSESrcSidecarWarm, fusePath, os.O_RDONLY, 0, warmOpTimeout())
	if !opened || err != nil {
		return false
	}
	defer f.Close()
	if !sidecarSourceMatches(proof, f, metrics.FUSESrcSidecarWarm) {
		proof = nil
	}
	// The extra byte distinguishes a whole body from a stale-low mirror prefix.
	buf := make([]byte, size+1)
	release, acquired := tryAcquireFUSEDataBackground()
	if !acquired {
		noteFUSEDataRefused()
		return false
	}
	n, readErr, done := readAtCallBounded(f.ReadAt, buf, 0, warmOpTimeout(), release)
	if !done || int64(n) != size || !errors.Is(readErr, io.EOF) {
		return false
	}
	m2, s2, ok2 := h.sidecarCurrentMeta(name)
	if !ok2 || m2 != mtime || s2 != size {
		return false
	}
	h.sidecar.put(name, buf[:n], mtime, size)
	if sidecarSourceMatches(proof, f, metrics.FUSESrcSidecarWarm) {
		h.publishStrongSidecarRead(proof, buf[:n])
	}
	metrics.Default().IncSidecarWarmPopulated()
	return true
}

// sidecarWarmDirAsync front-runs Finder: on a readdir, background-warm the
// dir's `._` bodies into the cache so the per-entry AppleDouble reads that
// follow are RAM-served. Non-blocking (readdir hot path): TTL-deduped + a
// non-blocking semaphore acquire — a busy pool or a recently-warmed dir is
// silently skipped. Nil/disabled/offline-safe.
const (
	sidecarWarmDedupeTTL = 5 * time.Minute
	sidecarWarmMaxPerDir = 512 // cap `._` reads per warm pass (bound tunnel work)
)

func (h *JuiceMountHandler) sidecarWarmDirAsync(dir string) {
	if h == nil || h.sidecar == nil || !h.sidecar.enabled || pin.IsOffline() ||
		h.testRefuseFUSEContentReads || !allowSpeculativeDirectoryWarm(netprofile.Default().Class()) {
		return
	}
	// FUTILITY BREAKER — stop warming when warming is not working.
	//
	// Measured on a real cellular link 2026-07-29, and again with the mount
	// OFFLINE, where the warmer should have had nothing to reach for:
	//
	//	sidecar_warm  calls=264  timeouts=258 (98%)  mean=791ms
	//	sidecar_warm_populated = 2
	//
	// 264 bounded FUSE opens, 258 of them spending the full 800ms
	// fuseStatTimeout, to populate TWO entries. Each one holds a warmGate slot
	// and, online, costs a metered round trip. This is the "latency spikes
	// hidden to the user ... even when offline" in the field report: invisible
	// work that never completes and never stops trying, because nothing in the
	// warmer ever consulted its own outcome.
	//
	// A warmer exists to make the NEXT access cheap. When it is failing, it is
	// doing the opposite — burning the very resource it is trying to save. So
	// give it a memory: after enough consecutive failures, back off for a
	// cooldown and let the foreground path serve. Any single success resets it,
	// so a link that recovers resumes warming immediately without a restart.
	if !h.sidecarWarmAllowed() {
		return
	}
	now := time.Now()
	h.sidecarWarmMu.Lock()
	if t, ok := h.sidecarWarmed[dir]; ok && now.Sub(t) < sidecarWarmDedupeTTL {
		h.sidecarWarmMu.Unlock()
		return
	}
	h.sidecarWarmed[dir] = now
	if len(h.sidecarWarmed) > 4096 {
		for d, t := range h.sidecarWarmed {
			if now.Sub(t) >= sidecarWarmDedupeTTL {
				delete(h.sidecarWarmed, d)
			}
		}
	}
	h.sidecarWarmMu.Unlock()

	select {
	case h.sidecarWarmSem <- struct{}{}:
		go func() {
			defer func() { <-h.sidecarWarmSem }()
			h.sidecarWarmDir(dir)
		}()
	default:
		// warm pool busy — the next readdir past the TTL re-triggers
	}
}

// sidecarWarmParallel bounds concurrent `._` reads WITHIN a warm pass. A `._`
// body is ~4KB, so the read is LATENCY-bound (one tunnel RTT), not bandwidth-
// bound — reading them in parallel collapses a serial 199×700ms≈2min crawl to
// ~(199/N)×700ms while adding negligible bytes-in-flight. This is what lets the
// warmer front-run Finder's foreground per-entry reads on the FIRST visit.
const sidecarWarmParallel = 16

// sidecarWarmDir warms a dir's `._` children (from the mirror listing) into the
// sidecar cache, in parallel. Each fetch passes the read-QoS bulk lane at
// prefetch class (sheds under contention — interactive reads always win).
func (h *JuiceMountHandler) sidecarWarmDir(dir string) {
	if h.store == nil || h.testRefuseFUSEContentReads {
		return
	}
	kids, err := h.store.ListChildren(dir)
	if err != nil {
		return
	}
	// Collect the eligible `._` children first.
	work := make([]string, 0, len(kids))
	for _, e := range kids {
		if len(work) >= sidecarWarmMaxPerDir {
			break
		}
		if !e.IsDir && isSidecarName(e.Name) && e.Size > 0 && e.Size <= sidecarMaxFile {
			work = append(work, e.Path)
		}
	}
	if len(work) == 0 {
		return
	}

	jobs := make(chan string, len(work))
	for _, p := range work {
		jobs <- p
	}
	close(jobs)

	var wg sync.WaitGroup
	var warmed int64
	workers := sidecarWarmParallel
	if workers > len(work) {
		workers = len(work)
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rel := range jobs {
				if pin.IsOffline() {
					return
				}
				// Deliberately NOT QoS-shed: a `._` sidecar is the exact byte
				// Finder is about to read to display this folder — it is the
				// critical path, not speculative prefetch. And it is ~4KB
				// (latency-bound), so 16 in flight is ~64KB, no bandwidth
				// threat to a concurrent media read. Shedding here (the v1 bug)
				// let the serial foreground reads win the race → warm=0.
				if h.warmSidecar(rel, h.fusePath+"/"+rel) {
					atomic.AddInt64(&warmed, 1)
				}
			}
		}()
	}
	wg.Wait()
	// Feed the futility breaker: a pass that ATTEMPTED work and populated
	// nothing is the signal that warming is currently pointless. See
	// sidecarWarmDirAsync.
	h.noteSidecarWarmResult(len(work), int(warmed))
	if warmed > 0 {
		jmlog.Debug("sidecar warm: dir warmed", "dir", dir, "sidecars", warmed)
	}
}

// Futility breaker state for the `._` sidecar warmer. See sidecarWarmDirAsync.
const (
	// sidecarWarmFailStreakMax is how many consecutive failed warm passes trip
	// the breaker. Small on purpose: at 800ms per timed-out read and up to 512
	// reads per pass, a handful of failing passes is already minutes of wasted
	// FUSE work.
	sidecarWarmFailStreakMax = 3
	// sidecarWarmCooldown is how long the warmer stays parked once tripped. Long
	// enough that a genuinely bad link stops paying, short enough that a
	// recovered one resumes without a restart. Any success resets immediately.
	sidecarWarmCooldown = 2 * time.Minute
)

// sidecarWarmAllowed reports whether warming should run right now, and is the
// breaker's read side.
func (h *JuiceMountHandler) sidecarWarmAllowed() bool {
	if sidecarWarmBreakerDisabled() {
		return true
	}
	h.sidecarWarmMu.Lock()
	defer h.sidecarWarmMu.Unlock()
	if h.sidecarWarmFailStreak < sidecarWarmFailStreakMax {
		return true
	}
	if time.Since(h.sidecarWarmTrippedAt) < sidecarWarmCooldown {
		return false
	}
	// Cooldown elapsed: allow ONE probe pass through. If it fails the streak is
	// still at the cap and we park for another cooldown; if it succeeds,
	// noteSidecarWarmResult clears everything.
	h.sidecarWarmTrippedAt = time.Now()
	return true
}

// noteSidecarWarmResult is the breaker's write side. populated is how many `._`
// bodies the pass actually landed in the RAM cache; attempted is how many it
// tried. A pass that populated nothing while attempting work is a failure.
func (h *JuiceMountHandler) noteSidecarWarmResult(attempted, populated int) {
	if attempted == 0 {
		return // nothing to learn from an empty pass
	}
	h.sidecarWarmMu.Lock()
	defer h.sidecarWarmMu.Unlock()
	if populated > 0 {
		h.sidecarWarmFailStreak = 0
		return
	}
	h.sidecarWarmFailStreak++
	if h.sidecarWarmFailStreak == sidecarWarmFailStreakMax {
		h.sidecarWarmTrippedAt = time.Now()
		jmlog.Info("sidecar warm: parking — consecutive passes populated nothing",
			"fail_streak", h.sidecarWarmFailStreak,
			"cooldown", sidecarWarmCooldown.String(),
			"note", "warming a link this slow costs more than it saves; foreground still serves")
	}
}

// sidecarWarmBreakerDisabled restores the pre-fix always-warm behavior.
func sidecarWarmBreakerDisabled() bool {
	return os.Getenv("JM_SIDECAR_WARM_BREAKER") == "0"
}

// ---- drain-time empty-sidecar elision (2026-08-18) ----
//
// WHAT AN AppleDouble SIDECAR IS. macOS stores a file's extended attributes
// (Finder tags, colour labels, resource forks, Spotlight comments) in a
// SEPARATE companion file named `._<name>`, in the same directory, whenever the
// underlying filesystem cannot hold xattrs itself. NFS is such a filesystem, so
// every file a Mac writes to /Volumes/zpool arrives as TWO files: `Foo` and
// `._Foo`. Both are spooled to local disk and both are drained to the backend.
//
// WHY THAT IS EXPENSIVE. The drain is METADATA-bound, not bandwidth-bound.
// Measured 2026-08-18: one os.Create through the FUSE mount costs 116 ms (the
// same create on local disk costs 0.07 ms), creates saturate at ~14.8/s no
// matter the concurrency, and the ceiling is PER-DIRECTORY. Of the ~369 ms a
// drained row costs, the MinIO upload is 32 ms (9%) and the create is 204 ms.
// A 30-file Finder copy produces 61 spool rows. So the `._` sidecars consume
// HALF of a hard, per-directory create ceiling.
//
// WHAT THIS DOES. At drain time the spooled bytes already sit on local disk and
// are free to inspect (0.07 ms). If a `._` body carries no metadata, the file
// is not created on the backend at all — the row is completed and its mirror
// entry removed, so the path simply does not exist.
//
// WHY THAT IS CORRECT. An ABSENT `._Foo` means "Foo has no extended
// attributes", which is precisely what an empty AppleDouble body encodes. The
// macOS NFS client treats ENOENT on `._Foo` as "no xattrs" — the same answer,
// minus the round trips. A sidecar that carries anything real is drained
// normally, so no user metadata is ever dropped.
//
// REFUTED, do not retry: storing the AppleDouble payload as an xattr on the
// real file instead. Measured setxattr = 75.0 ms vs create-the-sidecar = 61.0
// ms — 123% of the cost. juicefs xattrs are Redis metadata writes.

const (
	appleDoubleMagic   = 0x00051607
	appleDoubleVersion = 0x00020000
	adEntryFinderInfo  = 9
	adEntryResourceFrk = 2
	adFinderInfoSize   = 32
	// adEmptyResourceForkSize is the size of the canonical macOS "empty"
	// resource fork — a 286-byte map whose body reads "This resource fork
	// intentionally left blank". Every `._` file macOS writes carries one.
	adEmptyResourceForkSize = 286
	// attrHeaderSize is the size of the ATTR (com.apple.FinderInfo xattr
	// region) header that follows the 32-byte Finder info inside the Finder
	// info entry: magic, debug tag, total size, data start, data length,
	// 3 reserved words, flags, and the attribute count.
	attrHeaderSize = 36
)

var (
	adAttrMagic          = []byte("ATTR")
	adEmptyResourceForkK = []byte("This resource fork intentionally left blank")
)

// adIgnorableAttrs are xattr names whose presence still counts as "this
// sidecar carries nothing worth a backend file".
//
// MEASURED, and it CORRECTS the premise this work started from. A survey of 60
// real `._` files on the live volume found that 56 of them are NOT byte-empty:
// every single one carries exactly one xattr, `com.apple.provenance`, inside an
// otherwise all-zero Finder info block with the canonical blank resource fork.
// A strict "no attributes at all" predicate would therefore have skipped
// NOTHING and this change would have measured zero.
//
// `com.apple.provenance` is macOS 13+ kernel bookkeeping that records which
// application created a file, so TCC can decide whether app X may reach files
// made by app Y. It is per-Mac, per-volume, and identical across every file a
// given app writes (the same 11-byte value appeared on all 56). It is not user
// metadata, it is meaningless on a shared network volume, and it is already
// lost by most copy tools. Dropping it is benign.
//
// Nothing else is ignorable. The same survey found one file carrying
// `com.blackmagicdesign.thumbnail` and one with a non-zero Finder info block —
// both are refused by the predicate below and drain normally.
var adIgnorableAttrs = map[string]bool{
	"com.apple.provenance": true,
}

// appleDoubleIsDefaultEmpty reports whether body is a well-formed AppleDouble
// sidecar that carries no metadata worth materialising on the backend.
//
// FAILS CLOSED. Anything it does not fully understand — a bad magic, a
// truncated header, an out-of-range entry, an unknown entry type, a non-zero
// Finder info block, a resource fork that is not the canonical empty one, an
// xattr outside adIgnorableAttrs — returns false, and the caller drains the row
// normally. A false negative costs one create; a false positive would silently
// lose a user's Finder tags.
func appleDoubleIsDefaultEmpty(body []byte) bool {
	if len(body) < 26 {
		return false
	}
	if be32(body[0:4]) != appleDoubleMagic || be32(body[4:8]) != appleDoubleVersion {
		return false
	}
	n := int(be16(body[24:26]))
	// A default sidecar has exactly the two standard entries (Finder info and
	// resource fork). More than that means macOS attached something extra —
	// a comment, an icon, a real name — so materialise it.
	if n < 1 || n > 2 || len(body) < 26+12*n {
		return false
	}
	sawFinderInfo := false
	for i := 0; i < n; i++ {
		off := 26 + 12*i
		id := be32(body[off : off+4])
		start := int64(be32(body[off+4 : off+8]))
		length := int64(be32(body[off+8 : off+12]))
		if start < 0 || length < 0 || start+length > int64(len(body)) {
			return false // entry points outside the body: truncated or garbage
		}
		blk := body[start : start+length]
		switch id {
		case adEntryFinderInfo:
			sawFinderInfo = true
			if !finderInfoBlockIsEmpty(blk) {
				return false
			}
		case adEntryResourceFrk:
			if !resourceForkIsEmpty(blk) {
				return false
			}
		default:
			return false // an entry type we do not model — never assume empty
		}
	}
	return sawFinderInfo || n == 1
}

// finderInfoBlockIsEmpty reports whether an AppleDouble Finder info entry holds
// no user metadata: 32 zero bytes of Finder info, and either nothing after them
// or an ATTR xattr region containing only ignorable attribute names.
func finderInfoBlockIsEmpty(blk []byte) bool {
	if len(blk) < adFinderInfoSize {
		return false
	}
	for _, b := range blk[:adFinderInfoSize] {
		if b != 0 {
			return false // Finder flags, colour label, type/creator: real metadata
		}
	}
	rest := blk[adFinderInfoSize:]
	if len(rest) == 0 {
		return true
	}
	// The ATTR header follows the Finder info, in practice after 2 bytes of
	// padding (measured: offset +34, not +32). Accept either.
	hdr := []byte(nil)
	for _, pad := range []int{0, 2} {
		if len(rest) >= pad+len(adAttrMagic) && bytes.Equal(rest[pad:pad+len(adAttrMagic)], adAttrMagic) {
			hdr = rest[pad:]
			break
		}
	}
	if hdr == nil {
		// No xattr region. Only all-zero padding is acceptable here; any other
		// content is something we do not model, so fail closed.
		for _, b := range rest {
			if b != 0 {
				return false
			}
		}
		return true
	}
	if len(hdr) < attrHeaderSize {
		return false // truncated ATTR header
	}
	num := int(be16(hdr[34:36]))
	if num == 0 {
		return true
	}
	p := attrHeaderSize
	for i := 0; i < num; i++ {
		// Each attribute entry: offset(4) length(4) flags(2) namelen(1)
		// name(namelen, NUL-terminated), then padded to a 4-byte boundary.
		if p+11 > len(hdr) {
			return false
		}
		nameLen := int(hdr[p+10])
		if p+11+nameLen > len(hdr) {
			return false
		}
		name := string(bytes.TrimRight(hdr[p+11:p+11+nameLen], "\x00"))
		if !adIgnorableAttrs[name] {
			return false // a real xattr: this sidecar must reach the backend
		}
		p = (p + 11 + nameLen + 3) &^ 3
	}
	return true
}

// resourceForkIsEmpty reports whether an AppleDouble resource-fork entry is
// absent or is the canonical 286-byte macOS empty fork.
func resourceForkIsEmpty(blk []byte) bool {
	if len(blk) == 0 {
		return true
	}
	if len(blk) != adEmptyResourceForkSize {
		return false
	}
	return bytes.Contains(blk, adEmptyResourceForkK)
}

func be16(b []byte) uint16 { return uint16(b[0])<<8 | uint16(b[1]) }
func be32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

// drainSkipEmptySidecarsEnabled gates the elision. DEFAULTS **OFF** as of
// 2026-08-19, because measurement showed it costs work and saves none.
//
// WHAT WAS MEASURED. With the elision ON, copying real files onto the volume:
//
//	10 real files copied
//	11 sidecars elided (sidecars_skipped delta)
//	backend afterwards: 10 real files AND 10 ._ sidecars — every one present
//	spool COMMITs per sidecar: median 3, max 3 (n=11)
//
// Eliding a sidecar removes its mirror entry, so the macOS NFS client's next
// LOOKUP finds the ._ missing and writes it AGAIN. The sidecar is spooled and
// drained roughly three times instead of once and still lands on the backend.
// The create is not eliminated, or even displaced — it is multiplied.
//
// WHY THIS WAS NOT CAUGHT EARLIER. The first test used dd-created files whose
// sidecars carry a 286-byte resource fork, so appleDoubleIsDefaultEmpty
// correctly REFUSED them and the elision never fired at all. A test in which
// the feature does not run cannot show what the feature does. It also could not
// be seen from outside the process: SidecarsSkipped was incremented and never
// surfaced until cebcbf5 — which is what made this measurable at all.
//
// The predicate, the AppleDouble parser and their neuter-verified tests are kept
// (they are correct, and the parser is reusable); only the default changes. Set
// JM_DRAIN_SKIP_EMPTY_SIDECARS=1 to re-enable for further investigation.
func drainSkipEmptySidecarsEnabled() bool {
	return os.Getenv("JM_DRAIN_SKIP_EMPTY_SIDECARS") == "1"
}

// onSidecarSkipped is the drainer's handler-side hook for a `._` row that is
// being completed WITHOUT a backend file. It returns false to VETO the skip —
// the drainer then drains the row normally.
//
// The mirror still holds an entry for this path (the NFS CREATE inserted it so
// Finder could see the file it had just written). That entry must go, or a
// readdir would list `._Foo`, a LOOKUP would succeed, and the read behind it
// would fall through to a FUSE file that does not exist. Removing the entry is
// what turns the elision into the honest answer "this path does not exist".
//
// The veto guards mirror the phantom-purge ones: never drop an entry out from
// under a live NFS handle.
func (h *JuiceMountHandler) onSidecarSkipped(nfsPath string) bool {
	if h == nil || h.store == nil {
		return false
	}
	// Mirror keys are stored without a leading slash (same normalisation
	// juiceFS.Remove does); spool rows may carry one.
	nfsPath = strings.TrimPrefix(nfsPath, "/")
	if h.hasActiveWriter(nfsPath) {
		return false
	}
	if h.fdPool != nil && h.fdPool.HasOpenRefs(h.fusePath+"/"+nfsPath) {
		return false
	}
	h.sidecar.invalidate(nfsPath)
	if h.memBuf != nil {
		h.memBuf.Invalidate(nfsPath)
	}
	// Cache first, then the durable row: a reader between the two resolves the
	// spool shadow (still present — the drainer removes it after this returns)
	// and gets the real bytes. The reverse order would leave a window where the
	// cache says the path exists but nothing can serve it.
	h.store.DeleteFromCache(nfsPath)
	go func() {
		if err := h.store.Delete(nfsPath); err != nil {
			jmlog.Warn("sidecar skip: delete mirror row failed", "path", nfsPath, "error", err.Error())
		}
	}()
	return true
}

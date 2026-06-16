package nfs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"net"
	"os"
	"path"
	"sort"
	"strconv"
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
	"github.com/lelanddutcher/juicemount/internal/netprofile"

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
	prefetchMu  sync.Mutex
	prefetched  map[string]time.Time
	prefetchSem chan struct{} // limits concurrent prefetch goroutines

	// Verifier cleanup lifecycle
	verifierStop chan struct{}

	// QA-30 Layer B: singleflight per inode for FromHandle recovery so a
	// burst of identical stale-handle retries (DaVinci can fire 50+/sec
	// on a single inode) collapses to one Lstat + one re-insert. The
	// negative side caches recently-failed recoveries so we don't re-Lstat
	// for a genuinely-gone inode on every retry within the same burst.
	recoveryMu       sync.Mutex
	recoveryInFlight map[uint64]chan struct{} // inode → done-when-closed
	recoveryNegative map[uint64]time.Time     // inode → expiry; if now < expiry, skip

	// Spool routing (Option 2, slice C). When non-nil, O_CREATE writes
	// are routed through the spool instead of going directly to FUSE,
	// decoupling Finder write ack from MinIO upload completion. Gated
	// by JM_SPOOL_ENABLE at startup; nil means the existing fdPool
	// writeFile path is used (the pre-spool behavior). Read path is
	// NOT consulted here in slice C — slice D adds the 3-tier read
	// lookup. Files written via spool are temporarily invisible to
	// reads until the drainer copies them to FUSE (documented
	// limitation in docs/ROADMAP/option-2-spool.md).
	spool            *SpoolStore
	drainer          *Drainer
	spoolSweeperStop func() // stops the idle-finalize sweeper; set by SetSpool
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
// nfsLstatGate caps concurrent in-flight Lstats spawned by lstatWithTimeout
// and lstatNotExistWithTimeout. Under a FUSE wedge (the scenario these
// timeouts exist to handle), each call leaks a goroutine until the
// underlying Lstat returns — which under a sustained wedge is "never."
// Bounded gate prevents thousands of leaked goroutines during a stale-
// handle storm (QA-30 Layer B code review HIGH-1). When saturated, callers
// bail on the gate wait within their own deadline (treated as "FUSE
// degraded, fail conservative").
//
// 24 slots (up from 8): these bounded helpers are now on the per-RPC HOT
// path (juiceFS.Stat/Lstat/ReadDir/OpenFile fall-throughs), not just the
// rare phantom-purge path. Under a JuiceFS wedge the NFS server's 128
// concurrent-RPC budget (rpcSem) was being fully consumed by handlers stuck
// in unbounded os.Lstat/os.Stat, which stopped the server reading new
// requests and staled the whole mount (Finder "error 100060"). Bounding the
// FUSE syscalls converts that into fast per-RPC errors that free the RPC
// slot immediately; the gate caps how many goroutines can be parked in a
// stuck syscall at once (≤24) while healthy cold-cache concurrency flows.
var nfsLstatGate = make(chan struct{}, 24)

// errFUSETimeout is returned by the bounded FUSE helpers when a JuiceFS
// syscall doesn't complete within its deadline (the mount is wedged/slow).
// It is the shared nfslib.ErrFUSETimeout sentinel so the RPC boundary
// (internal/nfs/conn.go) can map it to NFS3ERR_JUKEBOX — the client then
// treats it as "server busy, retry" instead of a permanent error that would
// abort the Finder operation.
var errFUSETimeout = nfslib.ErrFUSETimeout

// fuseStatTimeout bounds a single hot-path FUSE Stat/Lstat/ReadDir/OpenFile.
// Long enough that a healthy cold-cache fetch (Redis + a MinIO range get on a
// slow link) completes, short enough that a wedge fails the RPC well within
// the client's mount timeout.
//
// Default is WAN-aware: 800ms on LAN, 2s when JM_WAN_MODE=1. On a LAN the
// hot-path stats are existence checks served from the SQLite mirror first
// (FUSE is fallback-only), and a genuine cold fetch is Redis (~1ms) + a
// MinIO range GET (tens of ms) — 800ms is ample headroom while still
// failing a wedge fast. 800ms was validated against a real recursive
// CFexpress-card Finder copy (2026-06-14): with the cache-only CREATE/LOOKUP
// path it kept CREATE/LOOKUP tail latency ~10x under the soft-mount timeout
// and the deep-tree copy cleared the metadata danger zone that used to trip
// "error 100060". Baking it in as the LAN default makes that durable (the
// prior launchctl setenv was reboot-only). On WAN (high-RTT MinIO over
// Tailscale/cellular) a cold range GET legitimately takes longer, so the
// default stays 2s there to avoid spurious JUKEBOX retries.
//
// An explicit JM_FUSE_OP_TIMEOUT_MS always wins over the WAN-aware default.
var fuseStatTimeout = func() time.Duration {
	if v := os.Getenv("JM_FUSE_OP_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	if os.Getenv("JM_WAN_MODE") == "1" {
		return 2 * time.Second
	}
	return 800 * time.Millisecond
}()

// statWithTimeout is the os.Stat sibling of lstatWithTimeout. ok=false means
// the underlying Stat didn't complete within the timeout (FUSE wedged).
func statWithTimeout(p string, timeout time.Duration) (fi os.FileInfo, err error, ok bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case nfsLstatGate <- struct{}{}:
	case <-timer.C:
		return nil, nil, false
	}
	type result struct {
		fi  os.FileInfo
		err error
	}
	ch := make(chan result, 1)
	go func() {
		fi, err := os.Stat(p)
		ch <- result{fi: fi, err: err}
		<-nfsLstatGate // release only after Stat actually returns
	}()
	select {
	case r := <-ch:
		return r.fi, r.err, true
	case <-timer.C:
		return nil, nil, false
	}
}

// readDirWithTimeout is the os.ReadDir sibling. ok=false → FUSE wedged.
func readDirWithTimeout(p string, timeout time.Duration) (ents []os.DirEntry, err error, ok bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case nfsLstatGate <- struct{}{}:
	case <-timer.C:
		return nil, nil, false
	}
	type result struct {
		ents []os.DirEntry
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		ents, err := os.ReadDir(p)
		ch <- result{ents: ents, err: err}
		<-nfsLstatGate
	}()
	select {
	case r := <-ch:
		return r.ents, r.err, true
	case <-timer.C:
		return nil, nil, false
	}
}

// openFileWithTimeout is the os.OpenFile sibling. ok=false → FUSE wedged.
// On timeout the leaked goroutine's *os.File (if the open eventually
// succeeds) is closed so we don't leak an fd.
func openFileWithTimeout(p string, flag int, perm os.FileMode, timeout time.Duration) (f *os.File, err error, ok bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case nfsLstatGate <- struct{}{}:
	case <-timer.C:
		return nil, nil, false
	}
	type result struct {
		f   *os.File
		err error
	}
	ch := make(chan result, 1)
	go func() {
		f, err := os.OpenFile(p, flag, perm)
		ch <- result{f: f, err: err}
		<-nfsLstatGate
	}()
	select {
	case r := <-ch:
		return r.f, r.err, true
	case <-timer.C:
		// Close the fd if the open completes after we've bailed.
		go func() {
			if r := <-ch; r.f != nil {
				_ = r.f.Close()
			}
		}()
		return nil, nil, false
	}
}

func lstatNotExistWithTimeout(p string, timeout time.Duration) (isNotExist, ok bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case nfsLstatGate <- struct{}{}:
	case <-timer.C:
		return false, false
	}
	type result struct {
		err error
	}
	ch := make(chan result, 1)
	go func() {
		_, err := os.Lstat(p)
		ch <- result{err: err}
		<-nfsLstatGate // release slot only after Lstat actually returns
	}()
	select {
	case r := <-ch:
		return os.IsNotExist(r.err), true
	case <-timer.C:
		// Worker still holds gate until its Lstat returns; bounded leak.
		return false, false
	}
}

// lstatWithTimeout is the FileInfo-returning sibling of
// lstatNotExistWithTimeout. ok=false means the underlying Lstat didn't
// complete within the timeout; callers should fall back to a safe default
// (typically: treat the entry as unknown rather than blocking the request
// goroutine on a wedged FUSE daemon).
func lstatWithTimeout(p string, timeout time.Duration) (fi os.FileInfo, ok bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	// QA-30 Layer B HIGH-1: bounded gate so a FUSE wedge can't leak
	// unbounded goroutines. Shared with lstatNotExistWithTimeout.
	select {
	case nfsLstatGate <- struct{}{}:
	case <-timer.C:
		return nil, false
	}
	type result struct {
		fi  os.FileInfo
		err error
	}
	ch := make(chan result, 1)
	go func() {
		fi, err := os.Lstat(p)
		ch <- result{fi: fi, err: err}
		<-nfsLstatGate // release slot only after Lstat actually returns
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			return nil, true // call completed but failed (e.g., ENOENT); caller decides
		}
		return r.fi, true
	case <-timer.C:
		return nil, false
	}
}

// HandlerOption customizes construction-time tuning of a handler.
// Applied once in NewHandler — none of these mutate a live handler.
type HandlerOption func(*handlerOptions)

type handlerOptions struct {
	memBufThreshold int64 // bytes; <= 0 → DefaultMemBufThreshold
	memBufBudget    int64 // bytes; <= 0 → DefaultMemBufBudget
}

// WithMemBufLimits sets the memory-buffer file-size threshold and total
// heap budget, in bytes. Values <= 0 keep the package defaults — callers
// can pass an unset (zero) config value straight through (LB-4 back-compat
// with config JSON written before these knobs existed).
func WithMemBufLimits(thresholdBytes, budgetBytes int64) HandlerOption {
	return func(o *handlerOptions) {
		o.memBufThreshold = thresholdBytes
		o.memBufBudget = budgetBytes
	}
}

func NewHandler(store *metadata.Store, fusePath string, opts ...HandlerOption) *JuiceMountHandler {
	var ho handlerOptions
	for _, opt := range opts {
		opt(&ho)
	}
	fdPool := NewFDPool()
	h := &JuiceMountHandler{
		store:     store,
		fusePath:  fusePath,
		fdPool:    fdPool,
		readahead: NewReadaheadManager(fusePath, fdPool, netprofile.Default()),
		// NewMemoryBuffer maps <= 0 to the package defaults.
		memBuf:        NewMemoryBuffer(ho.memBufThreshold, ho.memBufBudget),
		writeSizes:    make(map[string]int64),
		activeWriters: make(map[string]int),
		verifiers:     make(map[string]verifierData),
		prefetched:    make(map[string]time.Time),
		prefetchSem:   make(chan struct{}, 4), // max 4 concurrent prefetches
		verifierStop:  make(chan struct{}),
	}
	go h.verifierCleanupLoop(60*time.Second, 5*time.Minute)
	return h
}

// SetCacheReader attaches a direct SSD cache reader for bypassing FUSE on cached reads.
func (h *JuiceMountHandler) SetCacheReader(cr *cache.Reader) {
	h.cacheReader = cr
}

// invalidateReadCaches drops any cached bytes for path from BOTH the memory
// buffer and the direct-SSD slice cache, so a read after an overwrite / rename
// / delete / re-drain never serves the PREVIOUS generation's content. The
// slice cache maps inode -> JuiceFS slice IDs and was otherwise NEVER
// invalidated in production, so an in-place overwrite (same inode, new slice
// IDs) kept returning the old blocks (silent stale-content corruption on the
// re-export / NLE-relink round-trip). Invalidating is cheap and idempotent —
// a spurious invalidation just forces a correct FUSE/MinIO re-fetch — so we
// call it broadly. The inode is resolved from the live metadata cache; if the
// entry is already evicted (delete/rename source) there is nothing cached to
// serve under it, so a miss is fine.
func (h *JuiceMountHandler) invalidateReadCaches(path string) {
	if h.memBuf != nil {
		h.memBuf.Invalidate(path)
	}
	if h.cacheReader != nil {
		if e := h.store.LookupByPath(path); e != nil {
			h.cacheReader.InvalidateSliceCache(e.Inode)
		}
	}
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

// SetSpool attaches the JuiceMount-side write spool and its drainer, and
// starts the idle-finalize sweeper.
//
// When set (non-nil): juiceFS.Create routes new files through the spool, and
// juiceFS.OpenFile routes subsequent WRITE RPCs to the spool for any path
// with an active spool entry — decoupling Finder write ack from MinIO upload
// completion (writes land on local SSD; the drainer copies to FUSE in the
// background). Pre-spool behavior is preserved when nil: the legacy
// Create/writeFile/fdPool path runs unchanged. Gated by JM_SPOOL_ENABLE at
// the call sites (cmd/jm5/main.go, bridge/cbridge.go).
//
// Must be called before drainer.Start (it registers the drain-complete
// callback, which worker goroutines read).
func (h *JuiceMountHandler) SetSpool(spool *SpoolStore, drainer *Drainer) {
	h.spool = spool
	h.drainer = drainer
	if drainer != nil {
		// Post-drain hook: once a spooled file lands in FUSE, sync its real
		// size into the metadata cache and publish a create event — the
		// spool analogue of writeFile.Close's UpdateSize+publishEvent, which
		// the spool path can't do at write time because the bytes aren't in
		// FUSE yet. Without this, Stat reports the Create-time size 0 until
		// the next Redis reconcile.
		drainer.SetOnDrainComplete(h.onSpoolDrained)
	}
	if spool != nil {
		// NFS closes the file after every WRITE RPC, so finalize is driven
		// by quiescence (idle sweeper), not by Close. Stopped in StopHandler.
		//
		// The idle window must be LONGER than the realistic gap between a
		// single file's consecutive WRITE RPCs under concurrent load. With the
		// old 3s default, a large-offload Finder copy (many files in flight)
		// would interleave RPCs so that one file's writes were >3s apart; the
		// sweeper then finalized it MID-COPY, and the next WRITE for that path
		// hit OpenWrite's 30s reopen-wait (waiting for the backed-up drainer to
		// evict the just-finalized entry) — which exceeds the 40s soft-mount
		// timeout and aborts the whole copy with "error 100060" (ETIMEDOUT).
		// A generous window keeps actively-copied files in `writing` so they
		// are never finalized out from under an in-progress copy. Drain
		// throughput is unaffected (the drainers run continuously on the
		// backlog); only the per-file drain START is delayed by the window.
		// Tunable via JM_SPOOL_SWEEP_IDLE_SEC for unusual workloads.
		idle := 30 * time.Second
		if v := os.Getenv("JM_SPOOL_SWEEP_IDLE_SEC"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				idle = time.Duration(n) * time.Second
			}
		}
		h.spoolSweeperStop = spool.StartSweeper(idle, 0)
	}
}

// onSpoolDrained runs after the drainer copies a spooled file into FUSE. It
// updates the metadata cache with the real size and publishes a create event
// so other JuiceMount clients see the file. Inode/local_only are left to the
// normal Redis reconcile (identical to the legacy create lifecycle).
func (h *JuiceMountHandler) onSpoolDrained(nfsPath string, size int64) {
	now := time.Now()
	if size > 0 {
		_ = h.store.UpdateSize(nfsPath, size, now)
	}
	inode := uint64(0)
	if e := h.store.LookupByPath(nfsPath); e != nil {
		inode = e.Inode
	}
	// The drainer just (re)wrote this path's bytes into FUSE. If this drain was
	// an OVERWRITE of a previously-read file (re-export / NLE relink over the
	// same name, which routes through the spool), the direct-SSD slice cache
	// still maps this inode to the OLD slice IDs and the memory buffer holds
	// the old content; drop both so reads see the freshly-drained data.
	if h.cacheReader != nil && inode != 0 {
		h.cacheReader.InvalidateSliceCache(inode)
	}
	if h.memBuf != nil {
		h.memBuf.Invalidate(nfsPath)
	}
	h.publishEvent(metadata.MetadataEvent{
		Op: "create", Path: nfsPath, Size: size, Mtime: now.Unix(), Inode: inode,
	})
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

// clampWriteSize forces the tracked size DOWN to size if the current
// high-water mark exceeds it. Truncate is the one writer operation that is
// an authoritative size statement rather than a positional contribution —
// MAX semantics (trackWriteSize) would otherwise resurrect the stale
// pre-truncate size in Stat after the spool entry drains. Concurrent writes
// past the truncation point re-raise the mark via trackWriteSize as usual.
func (h *JuiceMountHandler) clampWriteSize(path string, size int64) {
	h.writeSizeMu.Lock()
	if cur, ok := h.writeSizes[path]; ok && cur > size {
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

	readStart := time.Now()
	// Bounded: this background scan must never park its goroutine (and its OS
	// thread) on a wedged/slow FUSE mount. readDirWithTimeout returns ok=false
	// past the deadline; we abandon the best-effort prefetch rather than leak
	// a blocked thread (crash 2026-06-14). The nfsLstatGate (cap 24) shared
	// with the hot path also caps how many of these can be in flight at once.
	dirEntries, err, ok := readDirWithTimeout(fusePath, fuseStatTimeout)
	if !ok {
		return
	}
	if err != nil {
		return
	}
	// A slow directory read is a self-calibrating signal that we're on a
	// high-latency link (cellular / Tailscale / distant backend). It gates the
	// recursive subdir fan-out below — see the comment at that loop.
	readElapsed := time.Since(readStart)

	toInsert := make([]*metadata.Entry, 0, len(dirEntries))
	var subdirs []string
	for _, de := range dirEntries {
		// Skip AppleDouble/._ sidecars in the scan: they're filtered out of
		// NFS listings anyway, and each one is a wasted FUSE round-trip
		// (lookup → ENOENT) on a remote link. NOTE: only skipped in this
		// directory-SCAN path; the per-path Stat the kernel uses after a
		// create still lets ._* through (QA-13).
		if strings.HasPrefix(de.Name(), "._") {
			continue
		}
		info, err := de.Info()
		if err != nil {
			// Don't fail the scan on one bad entry. This only skips ADDING a
			// new mirror row; a child already in the mirror is untouched (we
			// never delete here), so a transient stat failure can't erase a
			// known file from listings.
			jmlog.Debug("prefetch: stat failed for child, skipping insert",
				"dir", dirname, "name", de.Name(), "error", err.Error())
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

	// Gate the recursive subdir fan-out (remote folder-nav perf fix). The
	// directory the user actually navigated to was just refreshed above (one
	// round-trip). The recursion below PRE-WARMS subdirs they haven't opened
	// yet — a LAN-only optimization that turns into a link flood on
	// cellular/WAN: one folder open was measured triggering ~680 sequential
	// FUSE→Redis round-trips, ~100 ms each on cellular. Skip the fan-out when:
	//   - we're offline (user or auto): a backend scan can't/shouldn't run, or
	//   - the link is slow: readElapsed is well above warm-LAN readdir times
	//     (single-digit ms) and well below cellular RTT.
	const prefetchSlowLinkThreshold = 35 * time.Millisecond
	if pin.IsOffline() || readElapsed > prefetchSlowLinkThreshold {
		if readElapsed > prefetchSlowLinkThreshold {
			jmlog.Debug("prefetch: high-latency link — skipping subdir fan-out",
				"dir", dirname, "readdir_ms", readElapsed.Milliseconds())
		}
		return
	}

	// Fast link: still skip any subdir whose children are ALREADY mirrored —
	// re-reading a subtree we already hold is pure redundant round-trips even
	// on LAN. Bounded by the semaphore; non-blocking acquire.
	for _, sub := range subdirs {
		if kids, _ := h.store.ListChildren(sub); len(kids) > 0 {
			continue // already mirrored — nothing to warm
		}
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
//
// Shutdown order (slice C): drainer FIRST, then spool, then the older
// subsystems. The drainer holds in-flight goroutines that touch the
// spool, and the spool's index/db are still needed during the drainer
// drain-window. Old fdPool and verifierStop sweep after that.
func (h *JuiceMountHandler) StopHandler() {
	if h.spoolSweeperStop != nil {
		h.spoolSweeperStop() // stop finalizing new entries before draining down
	}
	if h.drainer != nil {
		h.drainer.Stop(30 * time.Second)
	}
	if h.spool != nil {
		h.spool.Stop()
	}
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
	stat.TotalSize = 1 << 40 // 1TB
	stat.FreeSize = 1 << 39  // 512GB
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
		// Record the synthetic→path mapping for handles handed out via THIS
		// branch too. The create/readdir path assigns COUNTER-based synthetic
		// inodes (nextSyntheticInode = counter|high-bit) that land in the cache,
		// so ToHandle returns them here — not only via the fnv64a fallback
		// below. Both kinds can lose their inodeCache entry when the reconcile
		// swaps in JuiceFS's real inode, stranding the client's handle → ESTALE.
		// RecordSyntheticHandle no-ops for real inodes.
		h.store.RecordSyntheticHandle(e.Inode, fullPath)
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

	// Remember this synthetic handle → path mapping PERMANENTLY (until FIFO
	// eviction), separate from the inodeCache. The 30s reconcile replaces this
	// path's synthetic inode with JuiceFS's real inode, dropping the synthetic
	// key from inodeCache — but the client keeps using this synthetic handle for
	// the file's lifetime. Without this record, its next op → FromHandle miss →
	// ESTALE (error 100070, and the retry-storm path to 100060) mid-copy.
	h.store.RecordSyntheticHandle(inode, fullPath)

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
		// QA-30 Layer B (2026-05-25): before returning STALE, try one-shot
		// recovery from the recently-evicted shadow map. If the entry was
		// removed by a prune/eviction within the last ShadowTTL (5 min)
		// AND FUSE confirms the path still exists, re-insert it and serve
		// normally. Singleflight by inode so DaVinci's scrub retries don't
		// cascade into N redundant Lstat calls.
		if recovered := h.tryRecoverEvicted(inode); recovered != nil {
			parts := splitPath(recovered.Path)
			return &juiceFS{handler: h}, parts, nil
		}

		// Synthetic-handle recovery (2026-06-14). A synthetic inode (high bit)
		// that ToHandle handed out for a not-yet-persisted path loses its
		// inodeCache entry when the reconcile swaps in JuiceFS's real inode —
		// but the client still holds the synthetic handle. Resolve it from the
		// persistent synthetic-handle map so the op proceeds on the path instead
		// of failing the client with ESTALE (error 100070 / retry-storm 100060
		// mid-copy). tryRecoverEvicted above deliberately skips synthetic inodes
		// (no Redis shadow); this is their recovery path.
		if inode&(1<<63) != 0 {
			if p, ok := h.store.SyntheticHandlePath(inode); ok {
				return &juiceFS{handler: h}, splitPath(p), nil
			}
		}

		// QA-25 diagnostic (2026-05-20): log every STALE so we can
		// see exactly which inode the kernel is presenting and
		// correlate against what we have in the cache. Remove after
		// QA-25 is closed; the cost of this Warn-level log is small.
		pathSample := "(no entry)"
		ps, is := h.store.CacheStats()
		jmlog.Warn("FromHandle STALE",
			"inode", fmt.Sprintf("%x", inode),
			"inode_synthetic", inode&(1<<63) != 0,
			"pathCache_size", ps,
			"inodeCache_size", is,
			"sample", pathSample,
		)
		return nil, nil, &nfslib.NFSStatusError{NFSStatus: nfslib.NFSStatusStale}
	}

	parts := splitPath(e.Path)
	return &juiceFS{handler: h}, parts, nil
}

// tryRecoverEvicted is QA-30 Layer B's recovery path. Called by FromHandle
// on cache-miss for a real (non-synthetic) inode. Looks up the recently-
// evicted shadow map; if present AND FUSE confirms the path still exists,
// re-inserts the entry into the cache and returns it. Otherwise returns
// nil (caller falls through to STALE).
//
// Singleflight by inode: a DaVinci scrub-storm produces 50+ identical
// FromHandle calls per second on the same stale inode. Without
// singleflight, each would race to Lstat + re-insert. The first call
// owns the recovery; later concurrent calls wait on the shared
// done-channel and then re-Lookup the cache (which the first call has
// either populated, or skipped via the negative cache).
//
// Negative cache (recoveryNegative) bounds the Lstat rate on confirmed-
// gone handles. Default TTL 5s — long enough to absorb a typical retry
// burst, short enough that a real "file came back" recovery isn't
// blocked forever.
func (h *JuiceMountHandler) tryRecoverEvicted(inode uint64) *metadata.Entry {
	// Synthetic inodes (high bit set) have no shadow entry — they're
	// counter-based ToHandle fallbacks, not real juicefs inodes.
	if inode&(1<<63) != 0 {
		return nil
	}
	// Negative cache check.
	h.recoveryMu.Lock()
	if exp, ok := h.recoveryNegative[inode]; ok {
		if time.Now().Before(exp) {
			h.recoveryMu.Unlock()
			return nil
		}
		delete(h.recoveryNegative, inode)
	}
	// Singleflight: if another goroutine is already recovering this
	// inode, wait on its done channel then retry the cache lookup.
	if done, ok := h.recoveryInFlight[inode]; ok {
		h.recoveryMu.Unlock()
		<-done
		// Whoever ran the recovery has either populated the cache or
		// installed a negative entry. Re-lookup.
		return h.store.LookupByInode(inode)
	}
	// Become the owner.
	if h.recoveryInFlight == nil {
		h.recoveryInFlight = make(map[uint64]chan struct{}, 8)
	}
	if h.recoveryNegative == nil {
		h.recoveryNegative = make(map[uint64]time.Time, 8)
	}
	done := make(chan struct{})
	h.recoveryInFlight[inode] = done
	h.recoveryMu.Unlock()

	// Cleanup when we return.
	defer func() {
		h.recoveryMu.Lock()
		delete(h.recoveryInFlight, inode)
		close(done)
		h.recoveryMu.Unlock()
	}()

	// Look up the shadow record.
	shadow, ok := h.store.LookupRecentlyEvicted(inode)
	if !ok {
		// Not in shadow → real STALE. Cache the negative briefly so
		// burst retries skip the lookup.
		h.recoveryMu.Lock()
		h.recoveryNegative[inode] = time.Now().Add(5 * time.Second)
		h.recoveryMu.Unlock()
		return nil
	}

	// Verify the path actually exists in FUSE before recovering.
	fusePath := h.fusePath + "/" + strings.TrimLeft(shadow.Path, "/")
	fi, fok := lstatWithTimeout(fusePath, 2*time.Second)
	if !fok {
		// Lstat timed out — FUSE is degraded. Don't recover, don't
		// cache negative (might succeed next time).
		return nil
	}
	if fi == nil {
		// File is genuinely gone. Cache negative so burst retries skip.
		h.recoveryMu.Lock()
		h.recoveryNegative[inode] = time.Now().Add(5 * time.Second)
		h.recoveryMu.Unlock()
		return nil
	}

	// File exists. Promote the shadow back to live cache.
	recovered := h.store.RecoverShadow(shadow, inode)
	jmlog.Info("FromHandle recovered evicted entry",
		"inode", fmt.Sprintf("%x", inode),
		"path", shadow.Path,
	)
	return recovered
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

	// Slice D: spool shadow. If a writer just landed bytes for this
	// path AND the drainer hasn't copied them to FUSE yet, the entry
	// lives only in the in-memory spool index (the metadata store
	// has nothing yet, FUSE has nothing yet). Without this short-
	// circuit, Stat would fall through to FUSE and return ENOENT
	// for an actively-being-written file.
	//
	// QA-35 perf-discipline: the empty-spool Lookup is 8.4 ns
	// (measured in slice A benchmarks). Adding it BEFORE the
	// writeSizes lock + metadata lookup keeps the hot path's
	// per-RPC tax at a no-op when no writes are in flight.
	if jfs.handler.spool != nil {
		if e, ok := jfs.handler.spool.LookupActive(filename); ok {
			return spoolFileInfoForEntry(path.Base(filename), e), nil
		}
	}

	// NO fast-reject of macOS metadata names here. QA-13 (2026-05-17)
	// already removed `._*` after the name-based ErrNotExist broke
	// copyfile(3): Finder/cp create the file then immediately stat it to
	// confirm, and a name-based ENOENT made the kernel conclude the create
	// failed → 0-byte truncation / error. The SAME bug bit the volume
	// system dirs (.fseventsd/.Spotlight-V100/.Trashes/.TemporaryItems/
	// .VolumeIcon.icns/Icon\r) when a user copied a whole volume/card ROOT:
	// Finder created .fseventsd, stat'd it, got our ErrNotExist, and aborted
	// the copy with -36 at the tail (2026-06-14). Anything onCreate lets
	// through MUST round-trip through Stat/Lstat/ReadDir or copying it fails.
	// The lost micro-optimization (skipping a LookupByPath for these probes)
	// is now negligible — LookupByPath is a cache-only map read, and absent
	// names still return ErrNotExist from the lookup below, identically.

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
			} else if jfs.handler.fdPool != nil && jfs.handler.fdPool.HasOpenRefs(jfs.fullPath(filename)) {
				// QA-35 (2026-05-26): NEVER purge a file with an active
				// reader OR writer. fdPool.HasOpenRefs returns true when
				// at least one outstanding Get OR GetWrite holds a FD on
				// this path (read AND write slots checked post-QA-37).
				// Either is proof the file is not a phantom — if anything
				// holds a FD, the kernel keeps the inode alive and ENOENT
				// from Lstat must be transient. The typical phantom-purge
				// trigger is ENOENT from Lstat during a transient juicefs
				// staging-block upload spike; an active holder rules that
				// out as the explanation.
				//
				// Trade-off: this is a heuristic, NOT a proof of dentry
				// existence. The kernel keeps the inode alive while a
				// FD is open, so a remote `juicefs rmr` from another
				// client could leave us with an open FD whose dentry is
				// genuinely gone. In that narrow case we serve stale
				// FileInfo until the reader closes — acceptable for the
				// playback workload (operator controls remote deletes
				// during active sessions). Worst-case staleness clears
				// when the FD is released and the next Stat reverifies.
				//
				// This eliminates one FUSE Lstat per GETATTR/LOOKUP on
				// any file Resolve, Finder Quick Look, or any other
				// long-open reader has open — the dominant per-metadata-
				// RPC tax in the playback workload.
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

	// Fallback: stat the file on FUSE directly and cache it. BOUNDED: a
	// wedged JuiceFS makes os.Stat hang forever; on the per-RPC hot path
	// that exhausts the NFS server's concurrency budget and stales the whole
	// mount. statWithTimeout returns ok=false on a wedge so we fail this RPC
	// fast (errFUSETimeout → JUKEBOX) and free the slot instead of blocking.
	fusePath := jfs.fullPath(filename)
	info, err, ok := statWithTimeout(fusePath, fuseStatTimeout)
	if !ok {
		return nil, errFUSETimeout
	}
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

// Lstat is the fast-path for NFS GETATTR. It is called from
// internal/nfs/nfs_ongetattr.go which then uses Entry.PreSerializedGetAttr to
// skip XDR encoding entirely.
//
// QA-35 (2026-05-26): GETATTR runs at high frequency (every kernel attr
// cache refresh — typically every 3 s per open file, more under sustained
// I/O). Routing it through Stat's phantom-purge gate burned one FUSE
// Lstat round-trip per GETATTR for every cache-miss file (the open-FD and
// hasActiveWriter gates only cover held files). On 100 files that is 100
// FUSE syscalls per attr-cache cycle.
//
// Bypass rationale: GETATTR operates on an NFS handle the client already
// owns, i.e. the cache must have known about this inode at handle-issue
// time. If the metadata cache still has the entry, that is sufficient
// truth for GETATTR — no need to revalidate against FUSE on every refresh.
// On cache miss we fall through to Stat, which does the full phantom-purge
// dance (the FUSE fallback path) — this is rare for GETATTR because the
// existence-of-handle implies existence-of-entry in the common case.
func (jfs *juiceFS) Lstat(filename string) (os.FileInfo, error) {
	if filename == "" || filename == "." || filename == "/" {
		return &rootDirInfo{}, nil
	}
	filename = strings.TrimPrefix(filename, "/")

	// Slice D: spool shadow. Same QA-35-disciplined pre-check Stat does
	// — empty-spool lookup is ~8 ns and gates the lookup before the
	// macOS-metadata filter so an in-flight file with one of those
	// base names is still served from spool. (Unlikely but valid.)
	if jfs.handler.spool != nil {
		if e, ok := jfs.handler.spool.LookupActive(filename); ok {
			return spoolFileInfoForEntry(path.Base(filename), e), nil
		}
	}

	// Lstat mirrors Stat: NO name-based fast-reject of macOS metadata names.
	// Rejecting them by name broke copying a volume/card root (the .fseventsd
	// -36, 2026-06-14) — see the Stat comment above. Round-trip them; an
	// absent name still returns ErrNotExist from the cache lookup below.

	// Tracked write size for in-flight writes (sticky map — see Stat).
	jfs.handler.writeSizeMu.Lock()
	writeSize, hasWriteSize := jfs.handler.writeSizes[filename]
	jfs.handler.writeSizeMu.Unlock()

	if e := jfs.handler.store.LookupByPath(filename); e != nil {
		if hasWriteSize && writeSize > e.Size {
			clone := *e
			clone.Size = writeSize
			clone.Mtime = time.Now()
			return clone.FileInfo(), nil
		}
		return e.FileInfo(), nil
	}

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
			base := e.Name
			// Do NOT hide `._*` AppleDouble sidecars: they are created,
			// stored, and stat/open/read-accessible (QA-13 removed them from
			// the Stat/Open filter at handler.go ~1167 so they round-trip).
			// Hiding them ONLY from readdir made them exist-but-unlisted —
			// confirmed 2026-06-14 by a real Finder copy of EOS_DIGITAL/DCIM
			// where `._<dir>` sidecars stat'd fine over NFS but never appeared
			// in `ls`. That breaks rsync / Carbon Copy Cloner / Finder-compare
			// (they enumerate via readdir, see the sidecar "missing", and
			// re-copy it every run) and loses the apparent resource-fork /
			// Finder-info metadata on a round-trip. List them like any entry.
			//
			// Still hide the macOS VOLUME-level system dirs: these are managed
			// by macOS at the mount root, not user data, and surfacing them can
			// make Finder try to manage them on our backend.
			if base == ".Spotlight-V100" ||
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
		// Proactively prefetch subdirectories' children in background so
		// Finder's subsequent navigation is instant. Two guards make this
		// safe under a bulk recursive walk (crash 2026-06-14, threads→5200+
		// toward the 8192 cap):
		//
		//   1. Skip when offline. prefetchChildren's os.ReadDir hits
		//      JuiceFS→Redis, which is unreachable offline; the read blocks
		//      until JuiceFS gives up. The synchronous offline path below
		//      (line ~1416) returns empty precisely to avoid that hang —
		//      spawning it here in the background would reintroduce it, one
		//      unbounded goroutine per directory.
		//   2. Bound the fan-out. A bare `go prefetchChildren` per READDIR
		//      turned a deep walk into thousands of concurrent ungated FUSE
		//      reads, each pinning an OS thread. Acquire a prefetchSem slot
		//      non-blocking and skip if the pool is busy — prefetch is a
		//      best-effort nav-latency optimization, not correctness, so
		//      shedding it under load is the right trade.
		if !pin.IsOffline() {
			select {
			case jfs.handler.prefetchSem <- struct{}{}:
				go func() {
					defer func() { <-jfs.handler.prefetchSem }()
					jfs.handler.prefetchChildren(dirname)
				}()
			default:
				// prefetch pool busy — shed this one
			}
		}
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

	// Fallback: read directly from FUSE and cache into SQLite. BOUNDED so a
	// wedged JuiceFS can't hang the READDIR RPC and exhaust the server's
	// concurrency budget (see statWithTimeout rationale).
	fusePath := jfs.fullPath(dirname)
	dirEntries, err, ok := readDirWithTimeout(fusePath, fuseStatTimeout)
	if !ok {
		return nil, errFUSETimeout
	}
	if err != nil {
		return nil, err
	}
	infos := make([]os.FileInfo, 0, len(dirEntries))
	toInsert := make([]*metadata.Entry, 0, len(dirEntries))
	for _, de := range dirEntries {
		info, err := de.Info()
		if err != nil {
			// Log rather than silently dropping. A stat failure here (e.g. a
			// transient hiccup on a high-latency link) omits the entry from the
			// returned listing, which presents to the user as a folder that
			// "didn't fully load." macOS then caches that partial result for up
			// to acdirmax, so it persists until the cache expires or a refresh.
			jmlog.Debug("readdir: stat failed for child — omitting from cold listing",
				"dir", dirname, "name", de.Name(), "error", err.Error())
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

// StatCacheOnly returns the FileInfo for filename if the metadata cache knows
// it, WITHOUT any FUSE round-trip. found=false means the path is not in our
// local view. The guarded-CREATE existence checks use this instead of fs.Stat
// so a brand-new file (definitionally absent from the cache) doesn't pay an
// 800ms FUSE existence-stat each — the dominant cost that made a recursive
// deep-tree Finder copy (tens of thousands of new files → tens of thousands of
// cache-miss stats, bottlenecked through nfsLstatGate) saturate the metadata
// hot path and trip the kernel soft-mount timeout ("error 100060"), while a
// flat copy of large files (few new files) sailed through (2026-06-14, root-
// caused via a faithful Finder reproduction). It does NOT prove backend
// non-existence; use only where an optimistic "not present locally → proceed"
// is correct (a CREATE whose fs.Create is the real arbiter).
func (jfs *juiceFS) StatCacheOnly(filename string) (os.FileInfo, bool) {
	filename = strings.TrimPrefix(filename, "/")
	if e := jfs.handler.store.LookupByPath(filename); e != nil {
		return e.FileInfo(), true
	}
	return nil, false
}

// File operations — proxy to JuiceFS FUSE
func (jfs *juiceFS) Open(filename string) (billy.File, error) {
	return jfs.OpenFile(filename, os.O_RDONLY, 0)
}

func (jfs *juiceFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	filename = strings.TrimPrefix(filename, "/")

	// Detect write intent
	isWrite := flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0

	// Slice D: spool shadow for the READ path. If the requested file
	// is currently in the spool index (a writer's bytes are durable on
	// our local SSD but haven't yet been copied to FUSE by the drainer),
	// serve reads from the spool file directly. This closes the slice-C
	// "briefly invisible" gap.
	//
	// Write opens (O_CREATE / O_RDWR / O_WRONLY) bypass this and fall
	// through to the existing write-path branches below — the write
	// integration in slice C already routes O_CREATE through the spool.
	//
	// QA-35 perf-discipline: the empty-spool Lookup is 8.4 ns
	// (benchmarked in slice A). Adding it here BEFORE the metadata
	// lookup keeps the read-OpenFile hot path's per-RPC overhead at
	// a no-op when no writes are in flight.
	if !isWrite && jfs.handler.spool != nil {
		if sentry, ok := jfs.handler.spool.LookupActive(filename); ok {
			return &spoolReadFile{
				name:  filename,
				entry: sentry,
			}, nil
		}
	}

	// Look up the entry to get inode and size for cache reads
	e := jfs.handler.store.LookupByPath(filename)

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
			"inode", func() uint64 {
				if e != nil {
					return e.Inode
				}
				return 0
			}())
	}

	// For read-only opens, try to use fd pool + cache reader
	if !isWrite && e != nil {
		fusePath := jfs.fullPath(filename)
		fd, err := jfs.handler.fdPool.Get(fusePath)
		if err != nil {
			if os.IsNotExist(err) {
				// QA-32 (2026-05-25): the phantom-purge here used to fire
				// unconditionally. That destroyed pinned-file cache entries
				// during write-upload windows when FUSE returns ENOENT for
				// legitimate files because juicefs is busy uploading
				// staging blocks. Layer C protects the prune path; this
				// open path bypassed it. Three guards now match Stat()'s
				// phantom-purge in spirit:
				//   1. NEVER purge a pinned file. Pinning is the user's
				//      explicit contract. If FUSE says ENOENT on a pinned
				//      path, FUSE is wrong, not the cache.
				//   2. NEVER purge while there's an active writer for
				//      this path — concurrent OpenFile races with writes.
				//   3. Verify via a 2-second-budgeted Lstat that the file
				//      really doesn't exist before destroying the entry
				//      (FUSE's fdPool.Get ENOENT can be a fast lie under
				//      load; an explicit Lstat re-probes deterministically).
				canonical := jfs.handler.canonicalize(filename)
				if jfs.handler.isPinnedReady(canonical) {
					jmlog.Debug("open ENOENT on pinned file — NOT purging (FUSE likely busy)",
						"path", filename)
					return nil, err
				}
				if jfs.handler.hasActiveWriter(filename) {
					jmlog.Debug("open ENOENT but active writer present — NOT purging",
						"path", filename)
					return nil, err
				}
				isNotExist, ok := lstatNotExistWithTimeout(fusePath, 2*time.Second)
				if !ok {
					jmlog.Debug("open ENOENT but Lstat-verify timed out — NOT purging (FUSE degraded)",
						"path", filename)
					return nil, err
				}
				if !isNotExist {
					jmlog.Debug("open ENOENT but Lstat-verify says file exists — NOT purging (FUSE racy)",
						"path", filename)
					return nil, err
				}
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
			handler:     jfs.handler,
			cacheReader: jfs.handler.cacheReader,
			readahead:   jfs.handler.readahead,
			memBuf:      jfs.handler.memBuf,
			inode:       e.Inode,
			fileSize:    e.Size,
			pinned:      isPinned,
			// QA-31 + HIGH-1 fix: value-copy snapshot, NOT a pointer
			// to the live Entry. The Entry can be mutated in-place by
			// concurrent UpdateSize on writeback paths.
			cachedInfo: &snapshotFileInfo{
				name:  e.Name,
				size:  e.Size,
				mode:  e.Mode,
				mtime: e.Mtime,
				isDir: e.IsDir,
				inode: e.Inode,
			},
		}, nil
	}

	// For writes, use fd pool to avoid re-opening on every NFS WRITE RPC.
	// The go-nfs library calls OpenFile→Write→Close on every WRITE RPC,
	// so pooling the fd saves an open() + close() syscall per RPC.
	fullPath := jfs.fullPath(filename)

	if isWrite {
		// Spool-routed write (Option 2): route to the spool ONLY when this
		// path already has an active spool entry (created via juiceFS.Create
		// and not yet drained). NFS WRITE RPCs arrive as OpenFile(O_RDWR)
		// with NO O_CREATE (internal/nfs/nfs_onwrite.go), so the spool keys
		// off the index — keying off O_CREATE made the spool unreachable
		// over NFS entirely (Finding 1). A path with NO spool entry is an
		// in-place modify of a file that already lives in FUSE; that MUST
		// stay on the legacy fdPool path so the drainer never truncates it
		// via os.Create.
		if jfs.handler.spool != nil {
			if _, active := jfs.handler.spool.LookupActive(filename); active {
				sentry, err := jfs.handler.spool.OpenWrite(filename)
				if err != nil {
					return nil, err
				}
				// Keep the synthetic inode stable for the entry's lifetime
				// (Stat/Lstat shadow + NFS handle). SetInode is idempotent;
				// the inode was already assigned at Create time, so this is
				// normally a no-op — the fallback only fires for the unusual
				// case of an indexed entry with no inode yet.
				if sentry.Inode() == 0 {
					inode := jfs.handler.nextSyntheticInode()
					if e != nil {
						inode = e.Inode
					}
					sentry.SetInode(inode)
				}
				// QA-19 phantom-purge gate; released in spoolWriteFile.Close.
				jfs.handler.incActiveWriter(filename)
				return &spoolWriteFile{
					name:    filename,
					entry:   sentry,
					handler: jfs.handler,
				}, nil
			}
		}

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
			File:     fd,
			name:     filename,
			handler:  jfs.handler,
			entry:    entry,
			fdPool:   jfs.handler.fdPool,
			fusePath: fullPath,
		}, nil
	}

	// BOUNDED open: a wedged JuiceFS can't hang this OPEN/READ RPC path.
	f, err, ok := openFileWithTimeout(fullPath, flag, perm, fuseStatTimeout)
	if !ok {
		return nil, errFUSETimeout
	}
	if err != nil {
		return nil, err
	}
	// Carry forward the same pin snapshot we computed at the top of OpenFile.
	// Writes don't gate (writes are always allowed); reads through this
	// branch (e == nil, no metadata cache) pick up the same offline policy
	// as the cachedFile branch.
	return &billyFile{File: f, name: filename, pinned: isPinned, handler: jfs.handler}, nil
}

// CommitFile satisfies the internal/nfs Committer interface: it fsyncs any
// buffered spool data for path to stable storage (the NFS COMMIT / FILE_SYNC
// durability barrier) WITHOUT finalizing the entry, so a power loss after the
// client's fsync/commit can't lose acknowledged bytes. No-op when the path is
// not currently spooled (already drained, or spool disabled — those bytes are
// durable via their own path).
func (jfs *juiceFS) CommitFile(path string) error {
	if jfs.handler.spool == nil {
		return nil
	}
	path = strings.TrimPrefix(path, "/")
	if e, ok := jfs.handler.spool.Index().Lookup(path); ok {
		return e.Sync()
	}
	return nil
}

func (jfs *juiceFS) Create(filename string) (billy.File, error) {
	filename = strings.TrimPrefix(filename, "/")
	now := time.Now()

	// Spool-routed create (Option 2). The CREATE RPC is the new-file entry
	// point (internal/nfs/nfs_oncreate.go calls fs.Create). When the spool
	// is enabled we open a spool entry instead of creating the file on FUSE;
	// the drainer materializes it in FUSE later. We STILL insert the
	// metadata entry (same synthetic inode as the legacy path) so the NFS
	// handle resolves via ToHandle/FromHandle and the file appears in
	// directory listings immediately. Stat/Lstat/READ are shadowed by the
	// spool index (real growing size) until the drainer lands the file, at
	// which point onSpoolDrained syncs the final size into this entry.
	if jfs.handler.spool != nil {
		inode := jfs.handler.nextSyntheticInode()
		e := metadata.MakeEntry(filename, false, 0, now, inode)
		e.LocalOnly = true
		// Persist the entries row ASYNC (matching MkdirAll at :679 and prefetch
		// at :1745): InsertToCache gives the NFS handle + directory listing
		// immediate visibility, while the SQLite write — a writeMu-serialized
		// FTS-upsert transaction — moves OFF the CREATE RPC hot path. A
		// synchronous Insert here was the dominant per-file serialization in a
		// many-file offload: every CREATE blocked behind every other entries
		// write (reconcile BulkInsert, the echoed event flood), pushing CREATE
		// latency toward the soft-mount timeout at tens of thousands of files.
		// Crash-safe: the entry is LocalOnly + size 0; if the async Insert is
		// lost to a crash, the spool file + spool_entries row survive and the
		// boot scrubber + onSpoolDrained re-materialize the entry.
		jfs.handler.store.InsertToCache(e)
		go jfs.handler.store.Insert(e)

		sentry, err := jfs.handler.spool.OpenWrite(filename)
		if err != nil {
			return nil, err
		}
		sentry.SetInode(inode)
		jfs.handler.incActiveWriter(filename)
		return &spoolWriteFile{name: filename, entry: sentry, handler: jfs.handler}, nil
	}

	// Legacy path: create directly on FUSE.
	fullPath := jfs.fullPath(filename)
	f, err := os.Create(fullPath)
	if err != nil {
		return nil, err
	}

	// Cache immediately for NFS visibility; persist to SQLite async (same
	// off-hot-path pattern as the spool branch above and MkdirAll at :679).
	e := metadata.MakeEntry(filename, false, 0, now, jfs.handler.nextSyntheticInode())
	e.LocalOnly = true
	jfs.handler.store.InsertToCache(e)
	go jfs.handler.store.Insert(e)

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

	// Spool-aware rename (Phase-1 BUG 1). Order matters:
	//
	//  1. Cancel any active spool entry at the DESTINATION. POSIX rename
	//     replaces dst; without the cancel, dst's old entry keeps shadowing
	//     reads and its queued drain later overwrites the renamed file with
	//     the replaced file's bytes (same hazard class as QA-37 deletes).
	//  2. Migrate the SOURCE's active entry (and, for directory renames,
	//     every entry under oldpath+"/") to the new path — index key, SQL
	//     row, and drain target. This MUST happen before the FUSE rename:
	//     once the migration commits, an in-flight drain that already
	//     claimed the old target observes done=false at MarkDrainComplete
	//     and undoes its FUSE write instead of resurrecting the old path.
	//
	// Migration failure fails the RPC: proceeding with a FUSE rename while
	// the spool still targets the old path is exactly the silent-resurrect
	// bug this exists to fix. The client retries cleanly.
	migrated := 0
	if jfs.handler.spool != nil {
		// Cancel any active destination entry UNCONDITIONALLY (adversarial-
		// review BUG C) — gating on LookupActive missed rows recovered at
		// boot, which are deliberately NOT re-indexed (RecoverOnBoot): a
		// rename over such a row left its stale drain queued, able to land
		// after ours and overwrite the renamed file with the replaced file's
		// bytes. CancelForDelete is a no-op when nothing exists (same
		// unconditional pattern as Remove).
		jfs.handler.spool.CancelForDelete(newpath)
		n, err := jfs.handler.spool.MigrateForRename(oldpath, newpath)
		if err != nil {
			jmlog.Warn("rename: spool migration failed — failing RPC",
				"old", oldpath, "new", newpath, "error", err.Error())
			return err
		}
		migrated = n
	}

	// Execute on FUSE. A purely-spooled file hasn't been drained yet, so it
	// does not exist on FUSE — ENOENT here is the EXPECTED case when the
	// spool migration moved entries, not a failure (the drain materializes
	// the new path). Any other combination keeps the legacy contract:
	// errors propagate (onRename maps them to NFS statuses).
	if err := os.Rename(jfs.fullPath(oldpath), jfs.fullPath(newpath)); err != nil {
		if !(migrated > 0 && os.IsNotExist(err)) {
			return err
		}
		// migrated>0 && ENOENT: the SOURCE was purely spooled (not yet on FUSE),
		// so the migration re-keyed its spool entry to newpath and the rename
		// itself is a no-op on FUSE. But if newpath ALREADY had drained content
		// on FUSE, that OLD content still sits there and would be served if the
		// migrated entry's drain later FAILS (quarantine / retry-exhaust) —
		// silent stale-content corruption (a relink/atomic-save reading the
		// pre-rename bytes). Remove the stale FUSE dest now so a failed drain
		// yields a clean ENOENT instead. In-flight readers keep their open fd
		// (open-then-unlink); new reads hit the migrated spool entry's fresh
		// content during the drain, then FUSE after it lands.
		if rmErr := os.Remove(jfs.fullPath(newpath)); rmErr != nil && !os.IsNotExist(rmErr) {
			jmlog.Warn("rename: remove stale FUSE dest after spooled-source migration",
				"newpath", newpath, "error", rmErr.Error())
		}
	}

	// Invalidate both read caches for both ends: the old path is gone, and a
	// replaced destination must not serve the pre-rename bytes from the memory
	// buffer OR the direct-SSD slice cache.
	jfs.handler.invalidateReadCaches(oldpath)
	jfs.handler.invalidateReadCaches(newpath)

	// Carry the in-flight write-size high-water mark across the rename so
	// (a) Stat at the new path stays accurate for a file still being
	// written, and (b) a FUTURE file created at the old path doesn't
	// inherit a stale inflated size from the sticky map (QA-16 MAX
	// semantics never shrink).
	jfs.handler.writeSizeMu.Lock()
	if sz, ok := jfs.handler.writeSizes[oldpath]; ok {
		delete(jfs.handler.writeSizes, oldpath)
		if cur, ok := jfs.handler.writeSizes[newpath]; !ok || sz > cur {
			jfs.handler.writeSizes[newpath] = sz
		}
	}
	jfs.handler.writeSizeMu.Unlock()

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

	// Invalidate both read caches BEFORE the entry is evicted below (so the
	// inode is still resolvable for the slice-cache drop).
	jfs.handler.invalidateReadCaches(filename)

	// QA-37: cancel any in-flight spool entry FIRST, so a pending or
	// mid-flight drain can't resurrect the file we're about to delete. A
	// drain already copying to FUSE undoes its write when it finds the row
	// gone at MarkDrainComplete.
	if jfs.handler.spool != nil {
		jfs.handler.spool.CancelForDelete(filename)
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

	// Create on FUSE — but NOT when offline. A FUSE mkdir needs JuiceFS→Redis,
	// which is unreachable offline; the call hangs or fails and an offline
	// Finder folder copy aborts with "the device disappeared" (-36). Lazy
	// dir-creation (2026-06-14): offline we ONLY record the dir in the metadata
	// cache below (LocalOnly, so it's browsable immediately AND reconcile won't
	// prune it), and the drainer materializes it on FUSE — os.MkdirAll the
	// parent of each spooled file — when the files inside it drain after
	// reconnect. So an offline copy spools cleanly and the tree appears for the
	// user; the backend catches up online.
	if !pin.IsOffline() {
		if err := os.MkdirAll(jfs.fullPath(dirname), perm); err != nil {
			return err
		}
	}

	// Insert into in-memory cache FIRST (instant visibility for NFS stats).
	// LocalOnly=true both flags "not yet on the backend" (the drainer/reconcile
	// clear it once it lands in Redis) and protects it from the reconcile prune
	// while it's only local — essential for offline-created dirs.
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
func (jfs *juiceFS) Join(elem ...string) string { return path.Join(elem...) }
func (jfs *juiceFS) TempFile(dir, prefix string) (billy.File, error) {
	return nil, fmt.Errorf("not implemented")
}
func (jfs *juiceFS) Symlink(target, link string) error    { return fmt.Errorf("not supported") }
func (jfs *juiceFS) Readlink(link string) (string, error) { return "", fmt.Errorf("not supported") }
func (jfs *juiceFS) Chroot(p string) (billy.Filesystem, error) {
	return nil, fmt.Errorf("not supported")
}
func (jfs *juiceFS) Root() string { return "/" }

// juiceChange implements billy.Change for write operations.
type juiceChange struct {
	handler *JuiceMountHandler
}

func (jc *juiceChange) Chmod(name string, mode os.FileMode) error         { return nil }
func (jc *juiceChange) Chown(name string, uid, gid int) error             { return nil }
func (jc *juiceChange) Lchown(name string, uid, gid int) error            { return nil }
func (jc *juiceChange) Chtimes(name string, atime, mtime time.Time) error { return nil }

// cachedFile implements billy.File with two-tier read path:
// 1. Direct SSD pread (bypasses FUSE) — if cache reader is available and block is cached
// 2. JuiceFS FUSE pread (fallback) — populates SSD cache for future reads
const (
	// fuseReadMaxRetries bounds how many times cachedFile.ReadAt re-attempts a
	// zero-progress transient FUSE/MinIO read error before surfacing it. 4
	// attempts with the staged backoff below totals ~0.5s of sleeps — tiny
	// against the ~40s soft-mount per-RPC budget (timeo=400) — and converted
	// the observed ~2% concurrent-cold-read failure rate to zero in validation.
	fuseReadMaxRetries = 4
	// fuseReadRetryBackoff is multiplied by the (1-based) attempt number, so
	// the sleeps are 50,100,150,200 ms. Short enough to stay well inside the
	// kernel per-RPC window, long enough to let a contended MinIO fetch / FUSE
	// loader slot free up between tries.
	fuseReadRetryBackoff = 50 * time.Millisecond

	// offlineLocalReadTimeout bounds an OFFLINE un-pinned FUSE read so a
	// locally-cached block is served while a read that would need an S3/MinIO
	// fetch is refused fast (the offline tarpit guarantee). A JuiceFS LRU hit
	// returns in well under this; an S3 GET on a slow link is 30s+, so this
	// cleanly separates "served from local cache" from "needs the backend".
	offlineLocalReadTimeout = 1500 * time.Millisecond
)

// readAtBounded runs fd.ReadAt in a goroutine and returns (n, err, true) if it
// finishes within timeout, or (0, nil, false) on timeout. MUST be given a
// PRIVATE buffer: on timeout the goroutine keeps running and writes to buf, so
// buf must NOT be the caller's reusable/pooled slice (else a late write
// corrupts a subsequent RPC). Used by the offline read path to probe whether
// an un-pinned block is locally servable without tarpitting on a backend fetch.
func readAtBounded(fd *os.File, buf []byte, off int64, timeout time.Duration) (int, error, bool) {
	type res struct {
		n   int
		err error
	}
	ch := make(chan res, 1)
	go func() {
		n, err := fd.ReadAt(buf, off)
		ch <- res{n, err}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r := <-ch:
		return r.n, r.err, true
	case <-timer.C:
		return 0, nil, false
	}
}

type cachedFile struct {
	name        string
	fuseFD      *os.File
	fusePath    string
	fdPool      *FDPool
	handler     *JuiceMountHandler // for clampWriteSize on Truncate
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

	// QA-31 (2026-05-25): VALUE snapshot of the file's metadata at Open
	// time. Exposed via CachedInfo() to satisfy
	// internal/nfs.CachedInfoProvider — onRead uses this for the size-
	// clamp and the post-op attrs of every READ RPC, eliminating two
	// FUSE Stat round-trips per RPC.
	//
	// IMPORTANT (QA-31 code-review HIGH-1): this is a VALUE-COPY of the
	// metadata.Entry's scalars, NOT a pointer wrapper around the live
	// Entry. The live Entry can be mutated in-place by concurrent paths
	// (e.g. UpdateSize on writeback) under the Store's mu.Lock with no
	// synchronization between the writer and a reader going through
	// FileInfo.Size(). Holding a value snapshot eliminates the race
	// entirely on the cachedFile side; staleness was already acceptable
	// per NFS post-op-attrs semantics (advisory; clients revalidate via
	// GETATTR).
	cachedInfo *snapshotFileInfo
}

// snapshotFileInfo is a frozen-at-construction os.FileInfo for cachedFile.
// All fields are copied by value at Open time; no aliasing of the live
// metadata.Entry.
type snapshotFileInfo struct {
	name  string
	size  int64
	mode  os.FileMode
	mtime time.Time
	isDir bool
	inode uint64
}

func (s *snapshotFileInfo) Name() string       { return s.name }
func (s *snapshotFileInfo) Size() int64        { return s.size }
func (s *snapshotFileInfo) Mode() os.FileMode  { return s.mode }
func (s *snapshotFileInfo) ModTime() time.Time { return s.mtime }
func (s *snapshotFileInfo) IsDir() bool        { return s.isDir }
func (s *snapshotFileInfo) Sys() any {
	return &syscall.Stat_t{
		Ino:   s.inode,
		Uid:   snapshotUID,
		Gid:   snapshotGID,
		Nlink: 1,
	}
}

// snapshotUID/snapshotGID mirror metadata/types.go's currentUID/currentGID
// so the Sys() result matches what GETATTR returns through the metadata
// path. Initialized once at process start.
var (
	snapshotUID = uint32(os.Getuid())
	snapshotGID = uint32(os.Getgid())
)

// CachedInfo implements internal/nfs.CachedInfoProvider. Returns the
// file's metadata as observed at Open time (immutable value snapshot;
// safe to read concurrently with mutations on the source Entry).
func (f *cachedFile) CachedInfo() os.FileInfo {
	if f.cachedInfo == nil {
		return nil
	}
	return f.cachedInfo
}

func (f *cachedFile) Name() string { return f.name }

// cacheReaderServeEnabled gates the Priority-2 direct-SSD-cache serving read.
// DEFAULT OFF. The direct read of JuiceFS's PRIVATE SSD block files is
// fundamentally incoherent and was proven to SILENTLY corrupt reads:
//
//   - No length clamp: cache.Reader.readFromCache ReadAt's the caller's FULL
//     buffer at the slice offset but never clamps to the slice's valid Len nor
//     the block's real data length. JuiceFS block files are variable-length
//     with a 4-byte CRC trailer and can pack multiple compacted slices, so an
//     overrunning read returns correct head bytes + FOREIGN tail (CRC /
//     adjacent slice) — a torn read within one file. The short read is even
//     swallowed (err=nil when n>0), so onRead accepts it and never retries
//     through coherent FUSE.
//   - Incoherent + stale fd: the 5-min-cached block fd can read a block file
//     mid-rewrite/evict by JuiceFS.
//
// Measured 2026-06-15: ~0.5-1.3% of files torn under 12-16-way concurrent NFS
// reads of a freshly-drained 692-file/22.76GB set; SERIAL and raw-FUSE reads
// were 100% correct and server read_fails/rpc_errors stayed 0 (silent). The
// coherent FUSE path (Priority 3) is correct AND fast — JuiceFS serves its own
// warm cache — and the buggy blockPath made P2 miss (→ FUSE) the vast majority
// of the time anyway, so disabling it costs almost nothing. Re-enable ONLY
// after cache.Reader is repaired (clamp to min(len, sliceLen, blockDataLen) +
// correct on-disk blockPath layout + CRC validation + no-stale-fd). See task
// "silent torn-read on concurrent NFS reads".
var cacheReaderServeEnabled = os.Getenv("JM_ENABLE_CACHE_READER") == "1"

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

	// Priority 2: Direct SSD cache read (bypasses FUSE). DISABLED by default —
	// incoherent + unclamped → silent torn reads under concurrency (see
	// cacheReaderServeEnabled). Falls through to the coherent FUSE path below.
	if f.cacheReader != nil && cacheReaderServeEnabled {
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
		// Offline + un-pinned: the bytes MIGHT be local — JuiceFS's own LRU has
		// them (recently read/written/just-copied), or the user toggled offline
		// while the network is actually up (Redis/JuiceFS reachable, just don't
		// want S3 traffic). Don't refuse blindly (the old behavior, which made a
		// just-copied-then-drained file unreadable offline — 2026-06-14
		// offline-ingest sprint). Attempt the FUSE read with a SHORT bound: a
		// local hit returns within it → serve it; a read that would need an
		// S3/MinIO fetch blocks past it → THEN refuse with
		// ErrOfflineNotAvailable, preserving the tarpit-avoidance the offline
		// mode exists for. A PRIVATE buffer is used so a bound-exceeded read's
		// still-running goroutine can never scribble into the caller's pooled p.
		tmp := make([]byte, len(p))
		bn, berr, done := readAtBounded(f.fuseFD, tmp, off, offlineLocalReadTimeout)
		if !done || (berr != nil && bn == 0 && !errors.Is(berr, io.EOF)) {
			return 0, pin.ErrOfflineNotAvailable
		}
		if bn > 0 {
			copy(p, tmp[:bn])
			if f.readahead != nil {
				f.readahead.OnRead(f.inode, off, bn, f.name)
			}
			metrics.Default().AddBytesRead(int64(bn))
		}
		return bn, berr
	}

	// Priority 3: JuiceFS FUSE read (populates SSD cache for next time).
	n, err := f.fuseFD.ReadAt(p, off)
	// [JM6 readback-resilience, 2026-06-14 / 2026-06-15] Two JuiceFS-under-
	// concurrent-load transients corrupt a read even though the bytes at rest
	// are intact (a sequential or retried re-read always succeeds):
	//
	//  (1) ZERO-progress EIO — surfaces as NFS3ERR_IO → EIO/SIGBUS at the
	//      client (NLEs mmap media and CRASH). Root-caused via a 10-way readback
	//      of a 119GB/5387-file shoot: ~1-2.5% of cold reads failed, 0 serially.
	//
	//  (2) PREMATURE EOF — io.EOF returned at an offset BEFORE the real end of
	//      file. Propagating it sets NFS EOF=1 and TRUNCATES the file at the
	//      client. Root-caused 2026-06-15: a 65MB file delivered as exactly its
	//      first 1MB under 14-way reads; the server's own per-read log proved it
	//      served the WHOLE file — the client stopped on the false EOF. SILENT:
	//      read_fails/rpc_errors stayed 0, and a per-read DATA verify caught
	//      nothing because every read's bytes were correct — only the EOF lied.
	//
	// Both retry via the coherent FUSE path. f.fileSize is the Open-time cached
	// size; if 0/unknown we can't judge prematurity so EOF is trusted (no
	// behavior change). A premature EOF arriving WITH data (n>0) has its EOF
	// dropped (return n,nil) so the client reissues for the tail; a premature
	// EOF with no data is retried, and if it persists we surface
	// io.ErrUnexpectedEOF → NFS3ERR_IO so the client ERRORS rather than silently
	// truncating. Offline errors stay fail-fast (the offline UX contract).
	prematureEOF := func(nn int, e error) bool {
		return f.fileSize > 0 && errors.Is(e, io.EOF) && off+int64(nn) < f.fileSize
	}
	zeroEIO := err != nil && n == 0 && !errors.Is(err, io.EOF) && !pin.IsOfflineNotAvailable(err)
	if zeroEIO || (n == 0 && prematureEOF(n, err)) {
		for attempt := 1; attempt <= fuseReadMaxRetries; attempt++ {
			time.Sleep(fuseReadRetryBackoff * time.Duration(attempt))
			metrics.Default().IncReadRetry()
			n, err = f.fuseFD.ReadAt(p, off)
			if n > 0 || err == nil {
				jmlog.Debug("FUSE read recovered on retry",
					"path", f.name, "off", off, "attempt", attempt)
				break
			}
			if errors.Is(err, io.EOF) && !prematureEOF(n, err) {
				break // genuine EOF at/after fileSize
			}
		}
		if err != nil && n == 0 && !errors.Is(err, io.EOF) {
			metrics.Default().IncReadFail()
			jmlog.Warn("cold FUSE read failed after retries; surfacing transient EIO to client",
				"path", f.name, "off", off, "attempts", fuseReadMaxRetries, "err", err)
		}
	}
	// Drop a premature EOF that came with data — there is more file beyond
	// off+n, so an EOF here would truncate. Client reissues at off+n.
	if n > 0 && prematureEOF(n, err) {
		err = nil
	}
	// Still a premature zero+EOF after retries: surface a hard error, never a
	// truncating EOF. io.ErrUnexpectedEOF maps to NFS3ERR_IO in onRead.
	if n == 0 && prematureEOF(n, err) {
		metrics.Default().IncReadFail()
		jmlog.Warn("premature EOF before fileSize after retries; surfacing EIO not truncating EOF",
			"path", f.name, "off", off, "fileSize", f.fileSize)
		err = io.ErrUnexpectedEOF
	}
	// A PARTIAL read with a transient error (n>0, non-EOF, non-offline): the n
	// bytes are valid (pread filled them before the shortfall); surfacing the
	// error maps to NFS3ERR_IO, which the kernel turns into SIGBUS for an MMAP
	// reader — and NLEs mmap their media, so they CRASH mid-playback. Deliver
	// the partial bytes and DROP the error; the client reissues at off+n for the
	// remainder (re-fetching the cold chunk). mmap-safe analogue of the n==0
	// cold-EIO retry. Found 2026-06-15: 20/692 cold CONCURRENT mmap reads SIGBUS'd
	// here while read_retries stayed 0 — the a3ba369 retry only covers n==0.
	if n > 0 && err != nil && !errors.Is(err, io.EOF) && !pin.IsOfflineNotAvailable(err) {
		metrics.Default().IncReadRetry()
		jmlog.Debug("partial FUSE read salvaged (dropped transient EIO; client reissues)",
			"path", f.name, "off", off, "n", n, "err", err)
		err = nil
	}
	if n > 0 && f.readahead != nil {
		f.readahead.OnRead(f.inode, off, n, f.name)
	}
	if n > 0 {
		metrics.Default().AddBytesRead(int64(n))
	}
	return n, err
}

func (f *cachedFile) Read(p []byte) (int, error)  { return f.fuseFD.Read(p) }
func (f *cachedFile) Write(p []byte) (int, error) { return f.fuseFD.Write(p) }
func (f *cachedFile) Seek(offset int64, whence int) (int64, error) {
	return f.fuseFD.Seek(offset, whence)
}
func (f *cachedFile) Lock() error   { return nil }
func (f *cachedFile) Unlock() error { return nil }
func (f *cachedFile) Truncate(size int64) error {
	// Clamp the sticky write-size high-water DOWN to the truncation point.
	// Without this, a SHRINKING overwrite (truncate to a smaller size, then
	// write less) leaves the old larger high-water in writeSizes — trackWriteSize
	// only RAISES it — and Stat then over-reports the old size, so reads return
	// the truncated tail as ZEROS (corrupt content on a shrink-overwrite).
	if f.handler != nil {
		f.handler.clampWriteSize(f.name, size)
	}
	return f.fuseFD.Truncate(size)
}

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

// Truncate overrides the embedded *os.File.Truncate to ALSO clamp the sticky
// write-size high-water down to the truncation point. writeFile.Close persists
// that high-water as the SQLite size (MAX-wise, to survive concurrent
// out-of-order writes), so without clamping, a SHRINKING overwrite (truncate to
// a smaller size, then write less) would leave the OLD larger size in the
// metadata while the FUSE file is smaller — reads then return the truncated
// tail as ZEROS (silent corrupt content on shrink-overwrite / re-export over a
// smaller file). Subsequent writes past the new size re-raise the mark normally.
func (f *writeFile) Truncate(size int64) error {
	f.handler.clampWriteSize(f.name, size)
	return f.File.Truncate(size)
}

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
	//
	// QA-37: writeFile lives in the write-side keyspace slot, so it MUST
	// call ReleaseWrite (not Release). The latter drops refCount on the
	// read slot and would (a) leak the write slot's refcount, blocking
	// eviction of the write fd, and (b) under-count read-slot refs if a
	// reader is concurrently active — corrupting the active-reader gate.
	if f.fdPool != nil {
		f.fdPool.ReleaseWrite(f.fusePath)
	} else {
		f.File.Close()
	}

	// Invalidate BOTH read caches so subsequent reads see the freshly-written
	// data, not the previous generation's bytes from the slice cache (in-place
	// overwrite keeps the same inode with new JuiceFS slice IDs).
	f.handler.invalidateReadCaches(f.name)

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
	name    string
	pinned  bool
	handler *JuiceMountHandler // for clampWriteSize on Truncate
}

func (f *billyFile) Name() string  { return f.name }
func (f *billyFile) Lock() error   { return nil }
func (f *billyFile) Unlock() error { return nil }
func (f *billyFile) Truncate(size int64) error {
	// Clamp the write-size high-water down to the truncation point — see
	// cachedFile.Truncate; a shrink-overwrite would otherwise over-report size.
	if f.handler != nil {
		f.handler.clampWriteSize(f.name, size)
	}
	return f.File.Truncate(size)
}

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

func (r *rootDirInfo) Name() string       { return "" }
func (r *rootDirInfo) Size() int64        { return 0 }
func (r *rootDirInfo) Mode() fs.FileMode  { return fs.ModeDir | 0755 }
func (r *rootDirInfo) ModTime() time.Time { return time.Now() }
func (r *rootDirInfo) IsDir() bool        { return true }
func (r *rootDirInfo) Sys() any {
	return &syscall.Stat_t{
		Ino:   1,
		Uid:   uint32(os.Getuid()),
		Gid:   uint32(os.Getgid()),
		Nlink: 2,
	}
}

package nfs

import (
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/metrics"
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
	sidecarCacheCap  = 128 << 20 // total RAM cap for the sidecar cache
	sidecarNamePfx   = "._"
	sidecarNamePfxLn = 2
)

type sidecarEntry struct {
	data     []byte
	mtime    int64
	size     int64
	lastUsed int64
}

// sidecarCache is a bounded, mtime-validated LRU of complete `._` bodies.
type sidecarCache struct {
	enabled  bool
	mu       sync.Mutex
	m        map[string]*sidecarEntry
	bytes    int64
	maxBytes int64
	tick     int64

	// Disk persistence (sidecar_persist.go). persistPath is set once by
	// enablePersist; dirty counts puts/invalidates since the last snapshot
	// (atomic — bumped on the serve-adjacent put path without the saver
	// needing the cache lock).
	persistPath string
	persistStop chan struct{}
	dirty       int64
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
		maxBytes: max,
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
	if !ok || e.mtime != curMtime || e.size != curSize {
		return nil, false
	}
	c.tick++
	e.lastUsed = c.tick
	return e.data, true
}

// put stores a COMPLETE body (len(data)==size) tagged with (mtime,size),
// evicting LRU entries to stay under the byte cap. A partial (len!=size) is
// refused — the anti-membuf-stale guard.
func (c *sidecarCache) put(path string, data []byte, mtime, size int64) {
	if c == nil || !c.enabled || size <= 0 || size > sidecarMaxFile || int64(len(data)) != size {
		return
	}
	buf := make([]byte, len(data)) // own the bytes (caller's p is pooled/reused)
	copy(buf, data)
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.m[path]; ok {
		c.bytes -= int64(len(old.data))
	}
	c.tick++
	c.m[path] = &sidecarEntry{data: buf, mtime: mtime, size: size, lastUsed: c.tick}
	c.bytes += int64(len(buf))
	for c.bytes > c.maxBytes && len(c.m) > 1 {
		c.evictOneLRULocked()
	}
	atomic.AddInt64(&c.dirty, 1)
	metrics.Default().IncSidecarCachePut()
}

// invalidate drops a path (used when a writer opens the sidecar for write).
func (c *sidecarCache) invalidate(path string) {
	if c == nil || !c.enabled {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.m[path]; ok {
		c.bytes -= int64(len(e.data))
		delete(c.m, path)
		atomic.AddInt64(&c.dirty, 1)
	}
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
		c.bytes -= int64(len(c.m[victim].data))
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
	return e.Mtime.Unix(), e.Size, true
}

// sidecarServe attempts to serve a read of a `._` file from the RAM cache.
// Returns (n, true) when served; (0, false) to fall through to the FUSE read.
// Validated against the live mirror meta and bypassed while a writer is active.
func (h *JuiceMountHandler) sidecarServe(name string, p []byte, off int64) (int, bool) {
	if h == nil || h.sidecar == nil || !h.sidecar.enabled {
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

// warmSidecar reads a `._` file fully from FUSE and caches it (the warmer's
// per-file worker). Bounded open; complete-read-only.
func (h *JuiceMountHandler) warmSidecar(name, fusePath string) bool {
	if h == nil || h.sidecar == nil || !h.sidecar.enabled {
		return false
	}
	mtime, size, ok := h.sidecarCurrentMeta(name)
	if !ok || size <= 0 || size > sidecarMaxFile {
		return false
	}
	if _, hit := h.sidecar.get(name, mtime, size); hit {
		return false // already warm
	}
	if h.hasActiveWriter(name) {
		return false
	}
	// BACKGROUND work — up to sidecarWarmSem(3) × sidecarWarmParallel(16) = 48
	// concurrent warms. The FUSESrcSidecarWarm label is what routes this to
	// warmGate instead of the 24-slot FOREGROUND nfsLstatGate
	// (fuseGateForSource, audit P0): 48-way opportunistic warming could
	// otherwise hold every foreground slot on a high-latency link, so a user
	// navigating right then queued behind work nobody was waiting for.
	f, err, opened := openFileWithTimeout(metrics.FUSESrcSidecarWarm, fusePath, os.O_RDONLY, 0, fuseStatTimeout)
	if !opened || err != nil {
		return false
	}
	defer f.Close()
	buf := make([]byte, size)
	total := 0
	for total < int(size) {
		n, rerr := f.ReadAt(buf[total:], int64(total))
		total += n
		if rerr != nil {
			break
		}
	}
	if int64(total) != size {
		return false // incomplete — never cache a partial
	}
	// Re-check meta didn't shift under us mid-read.
	m2, s2, ok2 := h.sidecarCurrentMeta(name)
	if !ok2 || m2 != mtime || s2 != size {
		return false
	}
	h.sidecar.put(name, buf, mtime, size)
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
	if h == nil || h.sidecar == nil || !h.sidecar.enabled || pin.IsOffline() {
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
	if h.store == nil {
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

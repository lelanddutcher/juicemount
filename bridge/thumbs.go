package main

import (
	"encoding/json"
	"net/http"
	"path"
	"path/filepath"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/internal/thumbcache"
	"github.com/lelanddutcher/juicemount/metadata"
)

// #1 (INSTANT-NAV) hydration pack — bridge side. The nfs.ThumbWarmer is
// deliberately decoupled (interfaces + closures); this file supplies its real
// dependencies and the observability endpoint. See nfs/thumbwarm.go for the
// warmer's discipline and internal/thumbcache for the bounded LRU substrate.

// thumbReadThroughCap bounds /blob's read-through populate (and mirrors the
// warmer's own thumbWarmBlobCap): a "small kind" blob larger than this is
// served straight from FUSE without caching.
const thumbReadThroughCap = 2 << 20

// thumbKind is the derivative kind the poster/QuickLook surfaces serve. Kept
// here (rather than reaching into nfs) so the gate below and the /thumb-local
// wiring agree on the cache key.
const thumbKind = "thumbnail"

// ---------------------------------------------------------------------------
// C5 — stale-derivative serve gate
//
// Derivatives are keyed by INODE. An inode is a location, not a content
// identity: overwrite a media file in place (re-export, re-transcode, a
// round-trip through an NLE) and the inode is unchanged, or delete a file and
// let JuiceFS recycle its inode, and every derivative row + cached blob under
// that key now depicts bytes that are gone. Before this gate the serve paths
// short-circuited on ds.Known(inode) and served whatever was indexed, so
// QuickLook and GET /blob returned the OLD — or a FOREIGN file's — poster or
// proxy, indefinitely, surviving app restarts via the persistent thumb cache.
//
// The freshness mechanism already existed and was simply not applied here:
// every derivative row carries source_size/source_mtime, the source's size and
// mtime at generation time (internal/farm stampSource), and the AI
// contribute-back POST path already refuses a row whose vouch disagrees with
// the live source (409 "AI computed against old bytes", cbridge.go). The gate
// below applies that comparison on the way OUT — on SIZE only. The POST path
// compares the vouch against a real os.Stat of the source (backend value vs
// backend value, so its mtime check is sound); this read path compares against
// the RAM mirror, whose mtime for a client-written file is local wall-clock
// time, not the backend's. See derivRowStale.
// ---------------------------------------------------------------------------

// liveSource is a source asset's CURRENT size + mtime. ok=false means "nobody
// cheaply knows" — not "the file is gone".
type liveSource struct {
	size int64
	// mtime is carried for the rejection LOG only — it is deliberately not part
	// of the staleness decision, because the mirror's mtime and the row's vouched
	// mtime come from opposite sides of the client/backend boundary and disagree
	// by construction for any file this client wrote. See derivRowStale.
	mtime int64 // unix seconds, to match DerivRow.SourceMtime
	ok    bool
}

// liveSourceFor returns an inode's live size/mtime from the in-RAM metadata
// mirror.
//
// PERFORMANCE CONTRACT — this is the whole reason the gate is affordable.
// LookupByInode is a map read under an RLock: ~µs, backend-INDEPENDENT, no FUSE
// syscall, no NFS self-loop, no network. Users run this over cellular at ~500ms
// RTT, where a real stat() of the source on the serve path would put a full
// round-trip on every thumbnail and on every byte-range request of a proxy —
// orders of magnitude worse than the wrong-image bug it would be fixing. If you
// ever change this function, it must stay a local lookup.
//
// It is a package var solely so tests can inject a counting fake and prove the
// happy path performs zero backend stats (TestBlobServeHappyPathZeroExtraSourceReads;
// TestBlobServeReadsMirrorNotTheBackend is the companion guard that fails if this
// is ever reimplemented as a stat of the source file).
var liveSourceFor = func(inode uint64) liveSource {
	globalMu.Lock()
	store := globalStore
	globalMu.Unlock()
	return mirrorSource(store, inode)
}

// mirrorSource is liveSourceFor's production body, split out so it can be
// tested against a real metadata.Store without touching the global.
func mirrorSource(store *metadata.Store, inode uint64) liveSource {
	if store == nil {
		return liveSource{}
	}
	e := store.LookupByInode(inode)
	if e == nil {
		return liveSource{}
	}
	return liveSource{size: e.Size, mtime: e.Mtime.Unix(), ok: true}
}

// derivRowStale reports whether a derivative row was produced from DIFFERENT
// source bytes than the ones live now — i.e. whether serving it would show the
// user the wrong file.
//
// SIZE IS THE ONLY AUTHORITATIVE SIGNAL HERE. MTIME IS NOT COMPARABLE ACROSS THE
// CLIENT/BACKEND BOUNDARY IN THIS SYSTEM, so an mtime-only disagreement is
// treated as FRESH:
//
//	the row's vouch is stamped from an os.Stat of the file ON THE BACKEND
//	(internal/farm stampSource), but the mirror's mtime for anything THIS client
//	wrote is local wall-clock time.Now() — nfs/handler.go onSpoolDrained and
//	writeFile.Close both publish through metadata.Store.UpdateSize with `now`,
//	while the drainer deliberately restores the file's REAL mtime onto the
//	backend inode (#103). The two therefore disagree by construction for every
//	client-written file until a reconcile heals the mirror.
//
// Comparing them made the gate fire on a PERFECTLY VALID derivative in exactly
// C5's own target workflow — re-export in place → drain → farm re-derives → the
// FRESH row is judged stale — and rejectStaleDeriv does not merely 404, it
// DELETES the local blob. The user-visible result is a QuickLook 404, macOS
// falling back to its own generator, and a full-source read over a 500 ms link:
// precisely the latency regression the thumbnail plane exists to prevent, in
// return for catching a re-encode that lands on a byte-identical length.
//
// Two further deliberate "not stale" answers, both chosen because this is a
// wrong-image bug and not a data-loss one, so a false negative (serve a stale
// poster) is a much better trade than a network stall or a mass cache wipe:
//
//   - UNVOUCHED ROW (no SourceSize). Rows written before the columns existed
//     carry no vouch. There is nothing to compare against, and treating them as
//     stale would blank every legacy derivative on the volume. Serve them. The
//     farm stamps the field on every row it writes today, so this only shrinks
//     over time.
//
//   - MIRROR CAN'T ANSWER (live.ok false: the inode isn't mirrored yet, or the
//     store is nil). The only way to get an authoritative answer here is a
//     backend stat, which is exactly the ~500ms round-trip the serve path must
//     never take. Serve.
func derivRowStale(d derivatives.DerivRow, live liveSource) bool {
	if d.SourceSize == nil {
		return false
	}
	if !live.ok {
		return false
	}
	return *d.SourceSize != live.size
}

// rejectStaleDeriv is the shared action on a stale row: drop the persistent
// thumb-cache blob for (inode, kind) so the stale bytes cannot be served from
// local disk on the next call OR after a restart (thumbcache.Open rebuilds its
// index by walking the directory, so an index-only drop would resurrect it),
// and log it. The caller then returns whatever it returns for an ABSENT
// derivative, so the surface above regenerates.
//
// Only the thumb cache is dropped, not the manifest row: the row is the farm's
// to correct (it re-derives and upserts with a fresh vouch), and deleting it
// here would race the JM-15 reconcile that may be about to rewrite it.
func rejectStaleDeriv(tc *thumbcache.Cache, inode uint64, kind string, d derivatives.DerivRow, live liveSource) {
	if tc != nil {
		tc.Invalidate(inode, kind)
	}
	jmlog.Info("stale derivative withheld — source changed since generation",
		"inode", inode, "kind", kind,
		"row_source_size", derefI64(d.SourceSize), "row_source_mtime", derefI64(d.SourceMtime),
		"live_size", live.size, "live_mtime", live.mtime)
}

// derefI64 renders a nullable vouch field for logging (-1 == absent).
func derefI64(p *int64) int64 {
	if p == nil {
		return -1
	}
	return *p
}

// freshThumbCachePath is /thumb-local's cache-hit lookup with the C5 gate
// applied. The QuickLook appex path is the one that hits the persistent cache
// BEFORE any manifest read, so without this a stale poster is served straight
// off local disk and never reaches resolveThumbBlobPath at all.
//
// Cost discipline: the manifest read (a local SQLite PK point query — no
// network) happens ONLY on a cache hit. A miss returns before touching the
// index, exactly as before. tc.Path is still called first so the cache's
// hit/miss accounting (surfaced by /thumbs) keeps its existing meaning.
//
// A cache entry with no thumbnail row in the index is served: the cache
// outliving its manifest row is a normal state (reconcile pending, index
// rebuilt), and there is no vouch to judge it by.
func freshThumbCachePath(ds *derivatives.Store, tc *thumbcache.Cache, inode uint64) (root, rel string, ok bool) {
	if tc == nil {
		return "", "", false
	}
	croot, crel, ok := tc.PathParts(inode, thumbKind)
	if !ok {
		return "", "", false
	}
	if ds == nil {
		return croot, crel, true
	}
	rows, err := ds.Manifest(inode)
	if err != nil {
		return croot, crel, true // index unreadable — no basis to reject; serve.
	}
	for _, d := range rows {
		// N1: match resolveThumbBlobPath's READY filter. Judging the first
		// thumbnail row of ANY status and then breaking meant a PENDING
		// re-derive row — which already carries the NEW source vouch — read as
		// fresh, so the OLD cached blob was served as if it had been validated.
		// Only a ready row vouches for bytes that actually exist.
		if d.Kind != thumbKind || d.Status != "ready" {
			continue
		}
		live := liveSourceFor(inode)
		if derivRowStale(d, live) {
			rejectStaleDeriv(tc, inode, thumbKind, d, live)
			return "", "", false
		}
		break
	}
	return croot, crel, true
}

// resolveThumbBlobPath resolves an inode's READY "thumbnail" derivative to
// its absolute on-FUSE blob path — the warmer's manifest bridge. Same
// resolution chain as handleBlobHTTP: local Tier-B index first, then the
// JM-15 one-sidecar reconcile for a farm-derived asset this app hasn't
// ingested yet (1-2 FUSE round-trips, which is exactly why the warmer
// budgets calls to this). Globals are read at call time so a mount-path
// change mid-session can't serve a stale root.
func resolveThumbBlobPath(inode uint64) (mountOut, blobRel string, ok bool) {
	globalMu.Lock()
	ds := globalDerivStore
	tc := globalThumbCache
	mount := globalFUSEPath
	if mount == "" {
		mount = globalMountPath
	}
	globalMu.Unlock()
	if ds == nil || mount == "" {
		return "", "", false
	}
	if known, _ := ds.Known(inode); !known {
		if found, ferr := farm.ReconcileOneSidecar(ds, mount, inode); ferr != nil || !found {
			return "", "", false
		}
	}
	rows, err := ds.Manifest(inode)
	if err != nil {
		return "", "", false
	}
	for _, d := range rows {
		if d.Kind != thumbKind || d.Status != "ready" || d.BlobRelPath == nil || *d.BlobRelPath == "" {
			continue
		}
		// C5: the row is READY, but ready-for-WHICH-bytes? Reject it if the
		// source has changed under the inode since generation, and drop the
		// cached copy on the way out. Returning !ok is exactly the
		// no-derivative answer both callers already handle — the warmer skips
		// the inode, /thumb-local 404s and lets macOS draw its own thumbnail.
		live := liveSourceFor(inode)
		if derivRowStale(d, live) {
			rejectStaleDeriv(tc, inode, thumbKind, d, live)
			return "", "", false
		}
		// Root and relative path stay APART so the open can walk every
		// component — a joined absolute path can only guard its last one.
		return mount, derivatives.DerivBlobRel(inode, *d.BlobRelPath), true
	}
	return "", "", false
}

// thumbLocalDeps is the injected surface of serveThumbLocal — the Wave-3
// QuickLook appex endpoint. Everything the handler needs is passed as data
// or closures so the decision core is unit-testable without the bridge
// globals (mount, mirror, cache, warmer).
type thumbLocalDeps struct {
	mount     string                                          // configured user-facing mount point ("/Volumes/zpool")
	lookup    func(rel string) (inode uint64, isDir, ok bool) // RAM mirror path→inode
	cachePath func(inode uint64) (root, rel string, ok bool)  // local thumb-cache blob (root+rel: anchored serve)
	populate  func(inode uint64) (root, rel string, ok bool)  // read-through: resolve+cache, then the same
	warmDir   func(relDir string)                             // async dir warm (TTL-deduped by the warmer)
}

// serveThumbLocal answers GET /thumb-local?path=<abs>&size=N for the
// QuickLook thumbnail appex (Wave-3). Contract with the appex:
//   - 200 image/jpeg  → the farm poster (appex draws it; Apple's generator
//     never touches the source file — the whole point).
//   - 404 (fast)      → appex errors out and macOS FALLS BACK to its own
//     generator, so a miss is never worse than today. We kick an async dir
//     warm so the next visit hits.
//
// Path discipline: the appex sends the user-facing absolute path; the mirror
// is keyed volume-relative, so translate via the configured mount prefix and
// refuse anything outside it. `size` is accepted but ignored — the cache
// holds the single 640px farm poster and the appex aspect-fits it.
//
// The miss path attempts a bounded read-through populate (the /blob
// Priority-2 pattern): posters are ~30KB, so even over a cellular tunnel the
// one-time fetch lands well inside the appex's 1.5s client timeout — the
// user sees the CORRECT thumbnail slightly late rather than a generic icon
// until the next folder visit. If resolution fails (no derivative yet), warm
// the dir and 404.
func serveThumbLocal(w http.ResponseWriter, r *http.Request, d thumbLocalDeps) {
	p := r.URL.Query().Get("path")
	if p == "" || !filepath.IsAbs(p) {
		http.Error(w, "path (absolute) required", http.StatusBadRequest)
		return
	}
	p = filepath.Clean(p)
	if d.mount == "" || d.lookup == nil || d.cachePath == nil ||
		(p != d.mount && !strings.HasPrefix(p, d.mount+"/")) {
		w.Header().Set("X-JM-Thumb", "outside-mount")
		http.NotFound(w, r)
		return
	}
	rel := strings.TrimPrefix(strings.TrimPrefix(p, d.mount), "/")
	if rel == "" || metadata.ScanFilteredPath(rel) {
		w.Header().Set("X-JM-Thumb", "filtered")
		http.NotFound(w, r)
		return
	}
	inode, isDir, ok := d.lookup(rel)
	if !ok || isDir {
		w.Header().Set("X-JM-Thumb", "unknown-path")
		http.NotFound(w, r)
		return
	}
	// ANCHORED, like /blob's cache branch. These two http.ServeFile calls were
	// the siblings the previous round's fix did not name: same cache, same
	// bytes, different endpoint — and ServeFile re-resolves the joined path.
	serve := func(root, rel, label string) bool {
		f, err := derivatives.OpenRegularUnder(root, rel)
		if err != nil {
			return false
		}
		defer f.Close()
		fi, serr := f.Stat()
		if serr != nil {
			return false
		}
		w.Header().Set("Content-Type", "image/jpeg")
		w.Header().Set("X-JM-Thumb", label)
		http.ServeContent(w, r, "", fi.ModTime(), f)
		return true
	}
	if root, rel, ok := d.cachePath(inode); ok {
		if serve(root, rel, "hit") {
			return
		}
	}
	if d.populate != nil {
		if root, rel, ok := d.populate(inode); ok {
			if serve(root, rel, "populated") {
				return
			}
		}
	}
	if d.warmDir != nil {
		d.warmDir(path.Dir(rel))
	}
	w.Header().Set("X-JM-Thumb", "miss")
	http.NotFound(w, r)
}

// populateThumbFromFUSE is serveThumbLocal's read-through: resolve the READY
// thumbnail blob on FUSE (local index → JM-15 one-sidecar reconcile), copy it
// into the local thumb cache (size-capped — the same 2MB bound the warmer
// uses), and return the CACHE path to serve from. Serving from the cache, not
// the FUSE path, keeps repeat requests off FUSE entirely.
func populateThumbFromFUSE(tc *thumbcache.Cache, inode uint64) (root, rel string, ok bool) {
	if tc == nil {
		return "", "", false
	}
	mount, blobRel, ok := resolveThumbBlobPath(inode)
	if !ok {
		return "", "", false
	}
	// ANCHORED symlink guard. The derivative tree is consumer-writable, and this
	// writes into the PERSISTENT thumb cache — after which the serve path hands
	// the bytes out happily, because by then they really are a regular file. So
	// poisoning here survives restarts and bypasses /blob's own guard entirely.
	// Guarding only the filename was not enough: the per-inode DIRECTORY could
	// be swapped for a symlink. Size is checked on the DESCRIPTOR, not by a
	// second stat of the name, so there is nothing to swap in between.
	f, err := derivatives.OpenRegularUnder(mount, blobRel)
	if err != nil {
		return "", "", false
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || fi.Size() <= 0 || fi.Size() > thumbReadThroughCap {
		return "", "", false
	}
	if _, err := tc.Put(inode, "thumbnail", f); err != nil {
		return "", "", false
	}
	return tc.PathParts(inode, "thumbnail")
}

// handleThumbLocalHTTP wires serveThumbLocal to the live bridge globals.
func handleThumbLocalHTTP(w http.ResponseWriter, r *http.Request) {
	globalMu.Lock()
	rc := globalRC
	tc := globalThumbCache
	ds := globalDerivStore
	warmer := globalThumbWarmer
	mount := globalWantMountPoint
	globalMu.Unlock()
	if rc == nil || tc == nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	serveThumbLocal(w, r, thumbLocalDeps{
		mount: mount,
		lookup: func(rel string) (uint64, bool, bool) {
			e := rc.Store().LookupByPath(rel)
			if e == nil {
				return 0, false, false
			}
			return e.Inode, e.IsDir, true
		},
		// C5: cache hits are freshness-gated. This is the surface that reads
		// the persistent cache BEFORE any manifest lookup, so it is where a
		// stale poster would otherwise be served straight off local disk.
		cachePath: func(ino uint64) (string, string, bool) { return freshThumbCachePath(ds, tc, ino) },
		populate:  func(ino uint64) (string, string, bool) { return populateThumbFromFUSE(tc, ino) },
		warmDir:   func(relDir string) { warmer.WarmDirAsync(relDir) },
	})
}

// handleThumbsHTTP serves GET /thumbs: the hydration pack's control-plane
// grader — thumb-cache residency/hit stats + the warmer counters, so
// coverage is measurable before any Finder surface exists (the P2-first
// discipline from INSTANT_BROWSE_DESIGN.md).
func handleThumbsHTTP(w http.ResponseWriter, r *http.Request) {
	globalMu.Lock()
	tc := globalThumbCache
	warmerWired := globalThumbWarmer != nil
	globalMu.Unlock()
	if tc == nil {
		http.Error(w, "thumb cache not enabled", http.StatusServiceUnavailable)
		return
	}
	st := tc.Stats()
	m := metrics.Default().Snapshot()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"enabled":      true,
		"warmer_wired": warmerWired,
		"bytes":        st.Bytes,
		"max_bytes":    st.MaxBytes,
		"files":        st.Files,
		"hits":         st.Hits,
		"misses":       st.Misses,
		"puts":         st.Puts,
		"evictions":    st.Evictions,
		// C5 observability: blobs dropped because their source changed under
		// the inode (correctness), as opposed to `evictions` (capacity).
		"invalidations": st.Invalidations,
		"warm_hydrated": m.ThumbWarmHydrated,
		"warm_negative": m.ThumbWarmNegative,
		"warm_shed":     m.ThumbWarmShed,
	})
}

package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/farm"
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

// resolveThumbBlobPath resolves an inode's READY "thumbnail" derivative to
// its absolute on-FUSE blob path — the warmer's manifest bridge. Same
// resolution chain as handleBlobHTTP: local Tier-B index first, then the
// JM-15 one-sidecar reconcile for a farm-derived asset this app hasn't
// ingested yet (1-2 FUSE round-trips, which is exactly why the warmer
// budgets calls to this). Globals are read at call time so a mount-path
// change mid-session can't serve a stale root.
func resolveThumbBlobPath(inode uint64) (string, bool) {
	globalMu.Lock()
	ds := globalDerivStore
	mount := globalFUSEPath
	if mount == "" {
		mount = globalMountPath
	}
	globalMu.Unlock()
	if ds == nil || mount == "" {
		return "", false
	}
	if known, _ := ds.Known(inode); !known {
		if found, ferr := farm.ReconcileOneSidecar(ds, mount, inode); ferr != nil || !found {
			return "", false
		}
	}
	rows, err := ds.Manifest(inode)
	if err != nil {
		return "", false
	}
	for _, d := range rows {
		if d.Kind != "thumbnail" || d.Status != "ready" || d.BlobRelPath == nil || *d.BlobRelPath == "" {
			continue
		}
		return filepath.Join(farm.DerivBlobDir(mount, inode), filepath.Clean("/"+*d.BlobRelPath)), true
	}
	return "", false
}

// thumbLocalDeps is the injected surface of serveThumbLocal — the Wave-3
// QuickLook appex endpoint. Everything the handler needs is passed as data
// or closures so the decision core is unit-testable without the bridge
// globals (mount, mirror, cache, warmer).
type thumbLocalDeps struct {
	mount     string                                          // configured user-facing mount point ("/Volumes/zpool")
	lookup    func(rel string) (inode uint64, isDir, ok bool) // RAM mirror path→inode
	cachePath func(inode uint64) (string, bool)               // local thumb-cache blob path
	populate  func(inode uint64) (string, bool)               // read-through: resolve+cache, return cache path
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
	if bp, ok := d.cachePath(inode); ok {
		w.Header().Set("Content-Type", "image/jpeg")
		w.Header().Set("X-JM-Thumb", "hit")
		http.ServeFile(w, r, bp)
		return
	}
	if d.populate != nil {
		if bp, ok := d.populate(inode); ok {
			w.Header().Set("Content-Type", "image/jpeg")
			w.Header().Set("X-JM-Thumb", "populated")
			http.ServeFile(w, r, bp)
			return
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
func populateThumbFromFUSE(tc *thumbcache.Cache, inode uint64) (string, bool) {
	if tc == nil {
		return "", false
	}
	src, ok := resolveThumbBlobPath(inode)
	if !ok {
		return "", false
	}
	fi, err := os.Stat(src)
	if err != nil || fi.Size() <= 0 || fi.Size() > thumbReadThroughCap {
		return "", false
	}
	f, err := os.Open(src)
	if err != nil {
		return "", false
	}
	defer f.Close()
	if _, err := tc.Put(inode, "thumbnail", f); err != nil {
		return "", false
	}
	return tc.Path(inode, "thumbnail")
}

// handleThumbLocalHTTP wires serveThumbLocal to the live bridge globals.
func handleThumbLocalHTTP(w http.ResponseWriter, r *http.Request) {
	globalMu.Lock()
	rc := globalRC
	tc := globalThumbCache
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
		cachePath: func(ino uint64) (string, bool) { return tc.Path(ino, "thumbnail") },
		populate:  func(ino uint64) (string, bool) { return populateThumbFromFUSE(tc, ino) },
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
		"enabled":       true,
		"warmer_wired":  warmerWired,
		"bytes":         st.Bytes,
		"max_bytes":     st.MaxBytes,
		"files":         st.Files,
		"hits":          st.Hits,
		"misses":        st.Misses,
		"puts":          st.Puts,
		"evictions":     st.Evictions,
		"warm_hydrated": m.ThumbWarmHydrated,
		"warm_negative": m.ThumbWarmNegative,
		"warm_shed":     m.ThumbWarmShed,
	})
}

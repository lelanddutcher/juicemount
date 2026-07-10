package main

import (
	"encoding/json"
	"net/http"
	"path/filepath"

	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/metrics"
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

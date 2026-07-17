package main

import (
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// Warm-up state surface (2026-07-14, release UX). After a boot — and
// especially after a cache clear or on a slow link — the mount serves
// correctly but SLOWER while (a) the metadata index rebuilds and (b) folder
// metadata (`._` sidecars, .DS_Store, posters) warms on first visits. Users
// read that window as "the app is broken" unless the UI says otherwise
// (field reports: "index rebuilding every so often", "nav not faster").
// GET /warmup gives the popover ONE consolidated, honest phase machine:
//
//	starting → indexing → warming → steady
//
//	starting: NFS not serving yet (mount sequence in flight)
//	indexing: the full metadata SCAN is running (progress = scanned/~total,
//	          capped at 99% because the total is the PREVIOUS sync's count)
//	warming:  serving, index quiet, but folder-metadata warm activity was
//	          seen within warmupActivityWindow AND uptime < warmupMaxWindow —
//	          first visits are still populating caches
//	steady:   everything above settled; repeat navigation is cache-served
//
// The progress number is phase-scoped and deliberately conservative:
// indexing reports real scan progress; warming reports elapsed-time toward
// warmupMaxWindow floored at 50% (it cannot regress below indexing's end);
// steady is 100. The popover renders the bar + the per-phase hint verbatim.
const (
	warmupSampleInterval = 15 * time.Second
	warmupActivityWindow = 90 * time.Second
	warmupMaxWindowSec   = 10 * 60 // uptime bound on the "warming" phase
	warmupServeGraceSec  = 5       // ignore warm blips in the first seconds
)

var (
	warmupServingSince atomic.Int64 // unix sec when NFS began serving (0 = not yet)
	warmupLastActivity atomic.Int64 // unix sec of last observed warm-cache growth
	warmupLastPuts     atomic.Uint64
	warmupSamplerOn    atomic.Bool
)

// warmupMarkServing records the serving-start instant (idempotent) and starts
// the activity sampler once. Called from the start path when the NFS server
// is up.
func warmupMarkServing() {
	now := time.Now().Unix()
	warmupServingSince.CompareAndSwap(0, now)
	if warmupSamplerOn.CompareAndSwap(false, true) {
		go warmupSampler()
	}
}

// warmupReset clears the serving marker on a deliberate stop so a later
// start re-enters the phase machine from "starting".
func warmupReset() {
	warmupServingSince.Store(0)
}

// warmupSampler watches the warm-cache counters; any growth stamps
// warmupLastActivity. Cheap: one metrics snapshot per interval.
func warmupSampler() {
	t := time.NewTicker(warmupSampleInterval)
	defer t.Stop()
	for range t.C {
		m := metrics.Default().Snapshot()
		puts := m.SidecarCachePut + m.ThumbWarmHydrated
		if last := warmupLastPuts.Load(); puts > last {
			warmupLastPuts.Store(puts)
			warmupLastActivity.Store(time.Now().Unix())
		}
	}
}

type warmupResponse struct {
	Phase      string `json:"phase"` // starting | indexing | warming | steady
	Progress   int    `json:"progress_pct"`
	Hint       string `json:"hint"`
	UptimeSec  int64  `json:"uptime_sec"`
	Serving    bool   `json:"serving"`
	IndexPct   int    `json:"index_pct"`
	IndexDone  int64  `json:"index_scanned"`
	IndexTotal int64  `json:"index_total_est"`
	SidecarPut uint64 `json:"sidecar_cache_put"`
	ThumbWarm  uint64 `json:"thumb_warm_hydrated"`
}

// handleWarmupHTTP serves GET /warmup — the popover's warm-up card source.
func handleWarmupHTTP(w http.ResponseWriter, r *http.Request) {
	globalMu.Lock()
	rc := globalRC
	running := globalServer != nil
	globalMu.Unlock()

	m := metrics.Default().Snapshot()
	resp := warmupResponse{
		SidecarPut: m.SidecarCachePut,
		ThumbWarm:  m.ThumbWarmHydrated,
		IndexPct:   100,
	}

	since := warmupServingSince.Load()
	now := time.Now().Unix()
	if since > 0 {
		resp.UptimeSec = now - since
		resp.Serving = true
	}

	syncing := false
	if rc != nil && rc.IsSyncing() {
		syncing = true
		scanned, est := rc.SyncProgress()
		resp.IndexDone, resp.IndexTotal = scanned, est
		if est > 0 && scanned > 0 {
			if pct := int(scanned * 100 / est); pct < 99 {
				resp.IndexPct = pct
			} else {
				resp.IndexPct = 99
			}
		} else {
			resp.IndexPct = 0
		}
	}

	maxWindow := int64(warmupMaxWindowSec)
	if raw := os.Getenv("JM_WARMUP_WINDOW_SEC"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			maxWindow = int64(n)
		}
	}

	switch {
	case !running || !resp.Serving:
		resp.Phase, resp.Progress = "starting", 5
		resp.Hint = "Starting up — connecting to the volume."
	case syncing:
		resp.Phase = "indexing"
		resp.Progress = resp.IndexPct / 2 // indexing occupies the first half of the bar
		if resp.Progress < 5 {
			resp.Progress = 5
		}
		resp.Hint = "Rebuilding the file index — browsing works, but may be slower until this completes."
	case resp.UptimeSec < maxWindow &&
		resp.UptimeSec > warmupServeGraceSec &&
		now-warmupLastActivity.Load() < int64(warmupActivityWindow/time.Second):
		resp.Phase = "warming"
		// Second half of the bar: elapsed-time toward the window, floored
		// so it never regresses below the indexing handoff.
		p := 50 + int(resp.UptimeSec*50/maxWindow)
		if p > 99 {
			p = 99
		}
		resp.Progress = p
		resp.Hint = "Warming folder metadata — first visits to a folder may be slower; repeat visits are instant."
	default:
		resp.Phase, resp.Progress = "steady", 100
		resp.Hint = "Fully warmed — navigation is served from local caches."
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

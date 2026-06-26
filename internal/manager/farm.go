package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

// farmQueueProbeTimeout caps any single Redis round-trip the Farm tab makes
// (ActiveWorkers SCAN, QueueDepth LLEN, ListJobs ZREVRANGE+HGETALL) so a wedged
// metadata Redis can't hang the handler. Mirrors overview.go's bounded-probe
// discipline (overviewBackendTimeout) — the GET is polled, so it must stay
// snappy even when the backend is sick.
const farmQueueProbeTimeout = 3 * time.Second

// farmRecentJobsLimit caps how many recent job-status records GET /api/farm/jobs
// returns. Matches the contract's documented default (ListJobs(50)).
const farmRecentJobsLimit = 50

// validFarmKinds is the set of job kinds a producer may request, mirroring the
// farmqueue.Kind* constants. Used to reject typo'd / unknown kinds at the HTTP
// boundary before anything reaches the queue.
var validFarmKinds = map[string]bool{
	farmqueue.KindDerivatives: true,
	farmqueue.KindProxy:       true,
	farmqueue.KindTranscript:  true,
	farmqueue.KindAll:         true,
}

// handleFarm serves GET /api/farm — the juicefarm operator rollup. The farm
// pre-aggregates its index into farm-status.json (coverage per kind + last
// sweep), so the manager (CGO-free, no sqlite, standalone) just relays the file
// verbatim. Read-only, no backend probe. Returns {available:false} when the path
// isn't configured or no sweep has produced the file yet — the Farm tab renders an
// empty-state hint rather than erroring.
func (a *API) handleFarm(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)

	if a.farmStatusPath == "" {
		_ = enc.Encode(map[string]any{
			"available": false,
			"reason":    "farm status path not configured (set JM_FARM_STATUS / mount the juicefarm-state volume)",
		})
		return
	}
	raw, err := os.ReadFile(a.farmStatusPath)
	if err != nil {
		_ = enc.Encode(map[string]any{
			"available": false,
			"reason":    "no farm sweep has run yet",
		})
		return
	}
	// Relay the farm's JSON verbatim under `status` so the schema stays owned by
	// the producer (internal/farm FarmStatus) — the manager doesn't re-model it.
	_ = enc.Encode(map[string]any{
		"available": true,
		"status":    json.RawMessage(raw),
	})
}

// farmSweepOptions are the optional per-job overrides a producer may set. Every
// field is a pointer so "absent" (nil) is distinguishable from a zero value —
// an omitted field leaves the corresponding Job field at its zero value, which
// the worker reads as "use my container env/flag default" (see
// FARM_QUEUE_PROTOCOL.md). Only fields the operator actually set get forwarded.
type farmSweepOptions struct {
	CRF          *int    `json:"crf,omitempty"`
	Preset       *string `json:"preset,omitempty"`
	Model        *string `json:"model,omitempty"`
	VCodec       *string `json:"vcodec,omitempty"`
	Workers      *int    `json:"workers,omitempty"`
	ProxyWorkers *int    `json:"proxy_workers,omitempty"`
}

// farmSweepRequest is the body of POST /api/farm/sweep. `path` is a path UNDER
// the volume mount (a directory → recursive, or a single file); `kinds` is a
// subset of {derivatives,proxy,transcript} or ["all"].
type farmSweepRequest struct {
	Path    string            `json:"path"`
	Kinds   []string          `json:"kinds"`
	Options *farmSweepOptions `json:"options,omitempty"`
}

// handleFarmSweep is POST /api/farm/sweep — the JM-16 producer entry point. It
// validates the request at the HTTP boundary, builds a farmqueue.Job stamped
// producer="manager", applies any options overrides, and LPUSHes it onto the
// shared juicefarm: queue. Returns {id, status:"queued"} on success.
//
// Failure modes: 400 on bad input (empty path, empty/unknown kinds, malformed
// JSON); 503 when the manager has no meta/redis URL (farmQ == nil) so the queue
// is unreachable; 500 when the enqueue round-trip itself fails.
func (a *API) handleFarmSweep(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req farmSweepRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Path == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	if len(req.Kinds) == 0 {
		http.Error(w, "kinds is required (one or more of: derivatives, proxy, transcript, all)", http.StatusBadRequest)
		return
	}
	for _, k := range req.Kinds {
		if !validFarmKinds[k] {
			http.Error(w, "unknown kind "+strconv.Quote(k)+" (valid: derivatives, proxy, transcript, all)", http.StatusBadRequest)
			return
		}
	}

	// Validation passed — now we need a live queue. Checked AFTER input
	// validation so a malformed request gets a clear 400 even when the queue
	// is down (the client's bug shouldn't be masked by the 503).
	if a.farmQ == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "farm queue unavailable (manager has no meta/redis URL)",
		})
		return
	}

	job := farmqueue.NewJob(req.Path, req.Kinds, "manager")
	applyFarmSweepOptions(&job, req.Options)

	ctx, cancel := context.WithTimeout(r.Context(), farmQueueProbeTimeout)
	defer cancel()
	if err := a.farmQ.Enqueue(ctx, job); err != nil {
		http.Error(w, "enqueue failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":     job.ID,
		"status": "queued",
	})
}

// applyFarmSweepOptions copies the set (non-nil) option overrides onto the job.
// An unset option leaves the Job field at its zero value so the worker applies
// its own default (the contract's "omit ⇒ use env/flag default" rule).
func applyFarmSweepOptions(job *farmqueue.Job, opts *farmSweepOptions) {
	if opts == nil {
		return
	}
	if opts.CRF != nil {
		job.CRF = *opts.CRF
	}
	if opts.Preset != nil {
		job.Preset = *opts.Preset
	}
	if opts.Model != nil {
		job.Model = *opts.Model
	}
	if opts.VCodec != nil {
		job.VCodec = *opts.VCodec
	}
	if opts.Workers != nil {
		job.Workers = *opts.Workers
	}
	if opts.ProxyWorkers != nil {
		job.ProxyWorkers = *opts.ProxyWorkers
	}
}

// handleFarmJobs is GET /api/farm/jobs — the Farm tab's read surface. It reports
// whether the farm is actually draining (≥1 heartbeating worker), the current
// queue depth, and the most-recent job-status records.
//
// Every Redis call is bounded by farmQueueProbeTimeout (mirroring overview.go's
// per-probe cap) so a wedged metadata Redis degrades the response rather than
// hanging the handler. Per-call errors are swallowed — a partial response
// (e.g. depth available but worker SCAN timed out) is more useful to the
// operator than a 5xx, matching the Overview tab's "never break the dashboard"
// contract.
func (a *API) handleFarmJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	if a.farmQ == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"available": false,
			"reason":    "farm queue unavailable (manager has no meta/redis URL)",
		})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), farmQueueProbeTimeout)
	defer cancel()
	workers, _ := a.farmQ.ActiveWorkers(ctx)
	depth, _ := a.farmQ.QueueDepth(ctx)
	jobs, _ := a.farmQ.ListJobs(ctx, farmRecentJobsLimit)
	// Normalize nil slices to non-nil so the JSON shape is stable
	// (`[]` not `null`) — a hard rule in this codebase (Go↔Swift null
	// discipline) and friendlier for the JS consumer.
	if workers == nil {
		workers = []farmqueue.Worker{}
	}
	if jobs == nil {
		jobs = []farmqueue.JobStatus{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"available":   len(workers) > 0,
		"workers":     workers,
		"queue_depth": depth,
		"jobs":        jobs,
	})
}

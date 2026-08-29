package manager

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

// farmQueue is the slice of *farmqueue.Client the manager's producer surface
// (POST /api/farm/sweep + GET /api/farm/jobs) actually uses. Factored as an
// interface so handler tests can exercise the SUCCESS paths against a fake
// without a live Redis, and so the compiler pins exactly which queue operations
// the manager depends on. *farmqueue.Client satisfies it; api.go assigns one
// when a meta/redis URL is configured.
type farmQueue interface {
	Enqueue(ctx context.Context, j farmqueue.Job) error
	ActiveWorkers(ctx context.Context) ([]farmqueue.Worker, error)
	QueueDepth(ctx context.Context) (int64, error)
	ListJobs(ctx context.Context, n int) ([]farmqueue.JobStatus, error)
	ClearFinished(ctx context.Context) (int, error)
	GetControl(ctx context.Context) (farmqueue.FarmControl, error)
	StoreControl(ctx context.Context, ctl farmqueue.FarmControl, forceRevision int64) (int64, error)
}

// Compile-time proof the real client implements the manager's queue slice.
var _ farmQueue = (*farmqueue.Client)(nil)

// farmQueueProbeTimeout caps any single Redis round-trip the Farm tab makes
// (ActiveWorkers SCAN, QueueDepth LLEN, ListJobs ZREVRANGE+HGETALL) so a wedged
// metadata Redis can't hang the handler. Mirrors overview.go's bounded-probe
// discipline (overviewBackendTimeout) — the GET is polled, so it must stay
// snappy even when the backend is sick.
const farmQueueProbeTimeout = 3 * time.Second

// farmProgressStaleAfter is deliberately much larger than jmfarm's three-second
// progress-write cadence. If an in_progress record has not been refreshed for
// this long, the producing process is no longer a trustworthy source of live
// activity (hard stop, container restart, lost mount, etc.). Last-sweep and
// coverage data remain useful indefinitely; only the live-progress assertion is
// time-sensitive.
const farmProgressStaleAfter = 30 * time.Second

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
// sweep), so the manager (CGO-free, no sqlite, standalone) relays that JSON.
// The one exception is live progress: a hard-killed worker cannot perform its
// final write that removes in_progress. We therefore suppress an in_progress
// record older than farmProgressStaleAfter and return it as stale_progress for
// truthful interrupted-work UI. Read-only, no backend probe. Returns
// {available:false} when the path isn't configured, absent, or unreadable.
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
	status, staleProgress, err := prepareFarmStatus(raw, time.Now())
	if err != nil {
		_ = enc.Encode(map[string]any{
			"available": false,
			"reason":    "farm status is unreadable",
		})
		return
	}
	response := map[string]any{
		"available": true,
		// farm-status.json is written beside this Manager by the server/NAS
		// worker. Remote workers keep their own derivative indexes and publish
		// portable sidecar manifests, so this rollup must never be presented as
		// a farm-wide aggregate until the Manager actually reconciles those
		// indexes. Keep the scope explicit in the wire contract.
		"scope":  "server_local",
		"status": status,
	}
	if staleProgress != nil {
		response["stale_progress"] = staleProgress
	}
	_ = enc.Encode(response)
}

// staleFarmProgress preserves the producer's last live-progress record without
// presenting it as current. The age is useful to operators and makes the
// Manager's inference explicit rather than silently rewriting history.
type staleFarmProgress struct {
	Progress   json.RawMessage `json:"progress"`
	LastSeenAt int64           `json:"last_seen_at,omitempty"`
	AgeSeconds int64           `json:"age_seconds,omitempty"`
	Reason     string          `json:"reason"`
}

// prepareFarmStatus keeps the producer-owned schema as raw JSON fields. This
// avoids importing internal/farm (and its sqlite dependency) into the CGO-free
// Manager while still allowing the time-sensitive in_progress field to be
// validated. Stable coverage and last-sweep fields are preserved byte-for-byte
// at the field-value level.
func prepareFarmStatus(raw []byte, now time.Time) (map[string]json.RawMessage, *staleFarmProgress, error) {
	var status map[string]json.RawMessage
	if err := json.Unmarshal(raw, &status); err != nil {
		return nil, nil, err
	}
	progress, ok := status["in_progress"]
	if !ok || len(progress) == 0 || string(progress) == "null" {
		return status, nil, nil
	}

	var writtenAt int64
	if rawWrittenAt, ok := status["written_at"]; ok {
		_ = json.Unmarshal(rawWrittenAt, &writtenAt)
	}
	age := int64(0)
	if writtenAt > 0 && now.Unix() > writtenAt {
		age = now.Unix() - writtenAt
	}
	// A missing/invalid timestamp cannot substantiate a live assertion. Future
	// timestamps are allowed for bounded clock skew and will age normally.
	if writtenAt > 0 && now.Sub(time.Unix(writtenAt, 0)) <= farmProgressStaleAfter {
		return status, nil, nil
	}

	delete(status, "in_progress")
	return status, &staleFarmProgress{
		Progress:   progress,
		LastSeenAt: writtenAt,
		AgeSeconds: age,
		Reason:     "worker stopped refreshing live progress",
	}, nil
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

	ctx, cancel := context.WithTimeout(r.Context(), farmQueueProbeTimeout)
	defer cancel()
	storage := a.enforceFarmStorageGuard(ctx)
	if storage.Configured && !storage.Safe {
		writeJSON(w, http.StatusInsufficientStorage, map[string]any{
			"error":   storage.Reason,
			"storage": storage,
		})
		return
	}
	kinds := req.Kinds
	for _, kind := range req.Kinds {
		if kind == farmqueue.KindAll {
			kinds = farmqueue.AllKinds()
			break
		}
	}
	var firstID string
	for _, kind := range kinds {
		job := farmqueue.NewJob(req.Path, []string{kind}, "manager")
		applyFarmSweepOptions(&job, req.Options)
		if err := a.farmQ.Enqueue(ctx, job); err != nil {
			// Contract (FARM_QUEUE_PROTOCOL.md, Manager HTTP surface): "503 if
			// Redis/meta unavailable" — an enqueue that can't reach Redis is the
			// unavailable case, not a manager bug, so 503 (retryable) not 500.
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"error": "enqueue failed (Redis/meta unreachable): " + err.Error(),
			})
			return
		}
		if firstID == "" {
			firstID = job.ID
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":     firstID,
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
	storage := a.enforceFarmStorageGuard(ctx)
	workers, _ := a.farmQ.ActiveWorkers(ctx)
	depth, _ := a.farmQ.QueueDepth(ctx)
	jobs, _ := a.farmQ.ListJobs(ctx, farmRecentJobsLimit)
	control, controlErr := a.farmQ.GetControl(ctx)
	if controlErr != nil {
		control = farmqueue.DefaultFarmControl()
	}
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
		"control":     control,
		"storage":     storage,
	})
}

// handleFarmJobsClear is POST /api/farm/jobs/clear. Clears terminal
// (done/failed) records from the Recent-jobs list — and prunes any
// leaked index entries — via farmqueue.ClearFinished. queued/running
// jobs are preserved so an in-flight sweep is never orphaned. Returns
// {"cleared": N}. 503 when the queue is unconfigured (no meta/redis URL).
func (a *API) handleFarmJobsClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if a.farmQ == nil {
		http.Error(w, "farm queue unavailable (manager has no meta/redis URL)", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), farmQueueProbeTimeout)
	defer cancel()
	n, err := a.farmQ.ClearFinished(ctx)
	if err != nil {
		log.Printf("manager: farm jobs clear failed: %v", err)
		http.Error(w, "clear failed", http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cleared": n})
}

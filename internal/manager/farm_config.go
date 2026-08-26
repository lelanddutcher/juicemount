package manager

// Farm node config (FARM-NODE-CONFIG spec) — the manager is the single source
// of truth for how every farm worker behaves. The desired state lives as a
// farmqueue.FarmConfig document at the juicefarm:config key in the shared meta
// Redis; workers poll it each drain-loop tick and report applied revision +
// drift via their heartbeats.
//
// Routes (registered in api.go alongside /api/farm/*):
//
//	GET /api/farm/config   → {config, workers} — current doc + per-node live view
//	PUT /api/farm/config   → validate + atomically store a new revision
//	DELETE /api/farm/config → remove the doc (workers keep last-good, badge shows)
//	GET /api/farm/workers  → enriched ActiveWorkers list (config_revision,
//	                         pending_restart, capabilities, effective settings)
//
// Auth = the same admin-key wrapper as every other /api route. Validation =
// farmqueue.ValidateFarmConfig whitelist; secrets are rejected by omission
// from the whitelist (this Redis doubles as volume metadata storage).

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// farmConfigRequest is the PUT body. Defaults and/or per-worker overrides;
// both are flat patches validated against the whitelist. A nil section means
// "leave that section untouched" — the manager merges with the EXISTING doc so
// the UI can save one worker's patch without resending global defaults.
type farmConfigRequest struct {
	Defaults  *map[string]any           `json:"defaults,omitempty"`
	Overrides map[string]map[string]any `json:"overrides,omitempty"` // full replacement of the named worker's patch
}

// handleFarmConfig serves GET+PUT+DELETE /api/farm/config.
func (a *API) handleFarmConfig(w http.ResponseWriter, r *http.Request) {
	if a.farmQ == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"available": false,
			"error":     "farm queue not configured (no meta/redis URL)",
		})
		return
	}
	ctx, cancel := contextWithTimeout(r, 5*time.Second)
	defer cancel()

	switch r.Method {
	case http.MethodGet:
		cfg, err := a.farmQGet(ctx)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		workers, _ := a.farmQ.ActiveWorkers(ctx)
		writeJSON(w, http.StatusOK, map[string]any{
			"available": true,
			"config":    cfg, // null = unmanaged cold-start state
			"workers":   workers,
			"whitelist": farmqueueValidKeys(),
		})

	case http.MethodPut:
		var req farmConfigRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad JSON body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.Defaults == nil && req.Overrides == nil {
			http.Error(w, "nothing to save: provide defaults and/or overrides", http.StatusBadRequest)
			return
		}
		cur, err := a.farmQGet(ctx)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		next := &farmQueueFarmConfig{Defaults: map[string]any{}, Overrides: map[string]map[string]any{}}
		if cur != nil {
			next.Defaults = cur.Defaults
			next.Overrides = cur.Overrides
		}
		if req.Defaults != nil {
			if err := farmqueueValidate(*req.Defaults); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			next.Defaults = mergePatch(next.Defaults, *req.Defaults)
		}
		for name, patch := range req.Overrides {
			if !validWorkerName(name) {
				http.Error(w, fmt.Sprintf("invalid worker name %q", name), http.StatusBadRequest)
				return
			}
			if len(patch) == 0 {
				delete(next.Overrides, name) // empty patch = clear that node's override
				continue
			}
			if err := farmqueueValidate(patch); err != nil {
				http.Error(w, fmt.Sprintf("overrides[%s]: %s", name, err.Error()), http.StatusBadRequest)
				return
			}
			next.Overrides[name] = patch
		}
		rev, err := a.farmQStore(ctx, next)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "revision": rev, "config": next, "config_revision": rev})

	case http.MethodDelete:
		if err := a.farmQDeleteConfig(ctx); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})

	default:
		http.Error(w, "GET, PUT or DELETE", http.StatusMethodNotAllowed)
	}
}

// handleFarmWorkers serves GET /api/farm/workers — the enriched heartbeat view
// (capabilities, config revision, pending-restart badges, effective settings).
func (a *API) handleFarmWorkers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	if a.farmQ == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"available": false})
		return
	}
	ctx, cancel := contextWithTimeout(r, 5*time.Second)
	defer cancel()
	workers, err := a.farmQ.ActiveWorkers(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if workers == nil {
		workers = []farmQueueWorker{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"available": true, "workers": workers})
}

// ---- small indirection helpers -------------------------------------------
// The manager binary imports internal/farmqueue already (farm.go), so these
// just forward to it; they exist to keep handler tests fakeable at one seam.

func (a *API) farmQGet(ctx stdCtx) (*farmQueueFarmConfig, error) {
	type getter interface {
		GetConfig(c stdCtx) (*farmQueueFarmConfig, error)
	}
	g, ok := a.farmQ.(getter)
	if !ok {
		return nil, fmt.Errorf("farm queue does not support config")
	}
	return g.GetConfig(ctx)
}

func (a *API) farmQStore(ctx stdCtx, cfg *farmQueueFarmConfig) (int64, error) {
	type storer interface {
		StoreConfig(c stdCtx, c2 *farmQueueFarmConfig, force int64) (int64, error)
	}
	s, ok := a.farmQ.(storer)
	if !ok {
		return 0, fmt.Errorf("farm queue does not support config store")
	}
	return s.StoreConfig(ctx, cfg, 0)
}

func (a *API) farmQDeleteConfig(ctx stdCtx) error {
	type deleter interface {
		DeleteConfig(c stdCtx) error
	}
	d, ok := a.farmQ.(deleter)
	if !ok {
		return fmt.Errorf("farm queue does not support config delete")
	}
	return d.DeleteConfig(ctx)
}

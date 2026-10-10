package manager

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

type farmJobController interface {
	RequestJobCancel(ctx stdCtx, id, requestedBy string) error
	RequeueFailed(ctx stdCtx, id, requestedBy string) (farmqueue.Job, bool, error)
}

type farmNodeObserver interface {
	WorkerLog(ctx stdCtx, workerID string, limit int64) ([]farmqueue.WorkerLogLine, error)
}

func farmActionParts(path, marker string) []string {
	i := strings.Index(path, marker)
	if i < 0 {
		return nil
	}
	rest := strings.Trim(path[i+len(marker):], "/")
	if rest == "" {
		return nil
	}
	return strings.Split(rest, "/")
}

// handleFarmJobAction serves cooperative cancel and exact-payload failed retry.
// Both are POST-only and remain under the Manager admin-key wrapper.
func (a *API) handleFarmJobAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	parts := farmActionParts(r.URL.Path, "/api/farm/job/")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
		http.Error(w, "expected /api/farm/job/{id}/cancel or /requeue", http.StatusNotFound)
		return
	}
	ctl, ok := a.farmQ.(farmJobController)
	if a.farmQ == nil || !ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "error": "farm job control unavailable"})
		return
	}
	ctx, cancel := contextWithTimeout(r, 5*time.Second)
	defer cancel()
	switch parts[1] {
	case "cancel":
		err := ctl.RequestJobCancel(ctx, parts[0], "manager")
		if errors.Is(err, farmqueue.ErrJobNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		if errors.Is(err, farmqueue.ErrJobTerminal) {
			writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "id": parts[0], "status": "cancel_requested"})
	case "requeue":
		job, created, err := ctl.RequeueFailed(ctx, parts[0], "manager")
		if errors.Is(err, farmqueue.ErrJobNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		if errors.Is(err, farmqueue.ErrJobNotFailed) {
			writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "id": job.ID, "status": "queued", "created": created, "requeue_of": parts[0]})
	default:
		http.Error(w, "action must be cancel or requeue", http.StatusNotFound)
	}
}

// handleFarmNodeAction writes pause/drain/resume as hot per-node config and
// serves the bounded Redis log tail for the one live process with that name.
func (a *API) handleFarmNodeAction(w http.ResponseWriter, r *http.Request) {
	parts := farmActionParts(r.URL.Path, "/api/farm/node/")
	if len(parts) != 2 || !validWorkerName(parts[0]) {
		http.Error(w, "expected /api/farm/node/{name}/{pause|drain|resume|log}", http.StatusNotFound)
		return
	}
	name, action := parts[0], parts[1]
	if action == "log" {
		if r.Method != http.MethodGet {
			http.Error(w, "GET only", http.StatusMethodNotAllowed)
			return
		}
		observer, ok := a.farmQ.(farmNodeObserver)
		if a.farmQ == nil || !ok {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"available": false, "error": "farm node log unavailable"})
			return
		}
		ctx, cancel := contextWithTimeout(r, 5*time.Second)
		defer cancel()
		workers, err := a.farmQ.ActiveWorkers(ctx)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"available": false, "error": err.Error()})
			return
		}
		var matches []farmqueue.Worker
		for _, worker := range workers {
			if worker.Name == name {
				matches = append(matches, worker)
			}
		}
		if len(matches) != 1 {
			writeJSON(w, http.StatusConflict, map[string]any{"available": false, "error": "node must have exactly one live process"})
			return
		}
		lines, err := observer.WorkerLog(ctx, matches[0].ID, 200)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"available": false, "error": err.Error()})
			return
		}
		if lines == nil {
			lines = []farmqueue.WorkerLogLine{}
		}
		writeJSON(w, http.StatusOK, map[string]any{"available": true, "name": name, "worker_id": matches[0].ID, "lines": lines})
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if action != "pause" && action != "drain" && action != "resume" {
		http.Error(w, "action must be pause, drain, or resume", http.StatusNotFound)
		return
	}
	if a.farmQ == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "error": "farm queue unavailable"})
		return
	}
	ctx, cancel := contextWithTimeout(r, 5*time.Second)
	defer cancel()
	cfg, err := a.farmQGet(ctx)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if cfg == nil {
		cfg = &farmqueue.FarmConfig{}
	}
	if cfg.Defaults == nil {
		cfg.Defaults = map[string]any{}
	}
	if cfg.Overrides == nil {
		cfg.Overrides = map[string]map[string]any{}
	}
	patch := map[string]any{}
	for key, value := range cfg.Overrides[name] {
		patch[key] = value
	}
	patch["paused"] = action == "pause"
	patch["drain"] = action == "drain"
	if err := farmqueue.ValidateFarmConfig(patch); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	cfg.Overrides[name] = patch
	cfg.UpdatedBy = "manager"
	rev, err := a.farmQStore(ctx, cfg)
	if err != nil {
		if errors.Is(err, farmqueue.ErrFarmConfigConflict) {
			writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "name": name, "action": action, "revision": rev, "paused": patch["paused"], "drain": patch["drain"]})
}

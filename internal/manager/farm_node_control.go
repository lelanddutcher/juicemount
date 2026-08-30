package manager

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

// farmNodeController is intentionally narrower than farmQueue so queue-only
// test doubles and alternate producers do not accidentally gain lifecycle
// authority. The concrete *farmqueue.Client implements both interfaces.
type farmNodeController interface {
	RequestWorkerRestart(ctx stdCtx, name, requestedBy string) (farmqueue.WorkerCommand, error)
	WorkerCommandStatusByID(ctx stdCtx, id string) (farmqueue.WorkerCommandStatus, bool, error)
	SetWorkerDisabled(ctx stdCtx, name string, disabled bool) error
	WorkerDisabled(ctx stdCtx, name string) (bool, error)
}

type farmWorkerControlRequest struct {
	Name   string `json:"name"`
	Action string `json:"action"` // restart|disable|enable
}

// handleFarmWorkerControl provides truthful operational lifecycle controls.
// Disable is a durable admission gate; restart is a one-shot acknowledged
// command and requires exactly one live process with the stable name.
func (a *API) handleFarmWorkerControl(w http.ResponseWriter, r *http.Request) {
	if a.farmQ == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ok": false, "error": "farm queue not configured",
		})
		return
	}
	ctl, ok := a.farmQ.(farmNodeController)
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ok": false, "error": "farm node control is unavailable",
		})
		return
	}
	ctx, cancel := contextWithTimeout(r, 5*time.Second)
	defer cancel()

	if r.Method == http.MethodGet {
		id := strings.TrimSpace(r.URL.Query().Get("command_id"))
		if id == "" {
			http.Error(w, "missing ?command_id=", http.StatusBadRequest)
			return
		}
		status, found, err := ctl.WorkerCommandStatusByID(ctx, id)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		if !found {
			writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "error": "command not found"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "command": status})
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "GET or POST", http.StatusMethodNotAllowed)
		return
	}

	var req farmWorkerControlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad JSON body: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.Name = strings.ToLower(strings.TrimSpace(req.Name))
	req.Action = strings.ToLower(strings.TrimSpace(req.Action))
	if !validWorkerName(req.Name) {
		http.Error(w, "invalid stable worker name", http.StatusBadRequest)
		return
	}
	workers, err := a.farmQ.ActiveWorkers(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	matches := 0
	for _, worker := range workers {
		if worker.Name == req.Name {
			matches++
		}
	}
	// A stable name is the lifecycle authority boundary. Refuse every action
	// while two live processes share it; a name-wide disable would otherwise
	// stop both nodes and a restart command could be consumed nondeterministically.
	if matches > 1 {
		writeJSON(w, http.StatusConflict, map[string]any{
			"ok": false, "error": "multiple live workers share this name; rename them before lifecycle control",
		})
		return
	}

	switch req.Action {
	case "disable", "enable":
		disabled := req.Action == "disable"
		if err := ctl.SetWorkerDisabled(ctx, req.Name, disabled); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true, "name": req.Name, "disabled": disabled,
			"note": map[bool]string{true: "node disabled immediately; active work is returned to its durable lane", false: "node is enabled and may claim compatible work"}[disabled],
		})

	case "restart":
		disabled, err := ctl.WorkerDisabled(ctx, req.Name)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		if disabled {
			writeJSON(w, http.StatusConflict, map[string]any{
				"ok": false, "error": "node is disabled; enable it before restarting",
			})
			return
		}
		if matches == 0 {
			writeJSON(w, http.StatusConflict, map[string]any{
				"ok": false, "error": "node is not online; no process can acknowledge a restart",
			})
			return
		}
		cmd, err := ctl.RequestWorkerRestart(ctx, req.Name, "manager")
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{
			"ok": true, "name": req.Name, "command": cmd,
			"note": "restart requested; the worker must acknowledge before its durable claim is released",
		})

	default:
		http.Error(w, "action must be restart, disable, or enable", http.StatusBadRequest)
	}
}

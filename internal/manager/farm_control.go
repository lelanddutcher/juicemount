package manager

import (
	"context"
	"encoding/json"
	"net/http"
)

type farmControlPatch struct {
	Paused       *bool `json:"paused,omitempty"`
	WatchEnabled *bool `json:"watch_enabled,omitempty"`
}

// handleFarmControl exposes the whole-farm transport controls used by the
// Manager's play/pause surface. PUT is a merge patch so toggling queue state
// cannot accidentally disable automatic discovery (or vice versa).
func (a *API) handleFarmControl(w http.ResponseWriter, r *http.Request) {
	if a.farmQ == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "farm queue unavailable"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), farmQueueProbeTimeout)
	defer cancel()

	switch r.Method {
	case http.MethodGet:
		ctl, err := a.farmQ.GetControl(ctx)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, ctl)
	case http.MethodPut:
		var patch farmControlPatch
		if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		if patch.Paused == nil && patch.WatchEnabled == nil {
			http.Error(w, "paused or watch_enabled is required", http.StatusBadRequest)
			return
		}
		if patch.Paused != nil && !*patch.Paused {
			storage := a.enforceFarmStorageGuard(ctx)
			if storage.Configured && !storage.Safe {
				writeJSON(w, http.StatusInsufficientStorage, map[string]any{
					"error":   storage.Reason,
					"storage": storage,
				})
				return
			}
		}
		ctl, err := a.farmQ.GetControl(ctx)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		if patch.Paused != nil {
			ctl.Paused = *patch.Paused
			// Any explicit operator transport action takes ownership of the
			// state. Resume is admitted only after the storage guard above, so
			// clearing the interlock fields cannot mask an unsafe backend.
			ctl.AutoPaused = false
			ctl.PauseCode = ""
			ctl.PauseReason = ""
		}
		if patch.WatchEnabled != nil {
			ctl.WatchEnabled = *patch.WatchEnabled
		}
		ctl.UpdatedBy = "manager"
		rev, err := a.farmQ.StoreControl(ctx, ctl, 0)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		stored, err := a.farmQ.GetControl(ctx)
		if err != nil {
			// The write succeeded. Preserve that success even if the immediate
			// readback fails, while still returning the authoritative revision.
			ctl.Revision = rev
			writeJSON(w, http.StatusOK, ctl)
			return
		}
		writeJSON(w, http.StatusOK, stored)
	default:
		http.Error(w, "GET or PUT only", http.StatusMethodNotAllowed)
	}
}

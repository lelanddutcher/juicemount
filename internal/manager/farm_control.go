package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

type farmNodeHold struct {
	Name   string `json:"name"`
	Paused bool   `json:"paused"`
	Drain  bool   `json:"drain"`
}

type farmNodeHolds struct {
	Known          bool           `json:"known"`
	ConfigRevision int64          `json:"config_revision,omitempty"`
	DefaultPaused  bool           `json:"default_paused"`
	DefaultDrain   bool           `json:"default_drain"`
	Nodes          []farmNodeHold `json:"nodes"`
}

type farmControlStatus struct {
	farmqueue.FarmControl
	Known     bool          `json:"known"`
	NodeHolds farmNodeHolds `json:"node_holds"`
}

func (a *API) farmControlStatus(ctx context.Context, ctl farmqueue.FarmControl, known bool) farmControlStatus {
	status := farmControlStatus{
		FarmControl: ctl,
		Known:       known,
		NodeHolds:   farmNodeHolds{Nodes: []farmNodeHold{}},
	}
	if !known {
		return status
	}
	cfg, err := a.farmQGet(ctx)
	if err != nil || cfg == nil {
		// A missing document can leave workers on their last applied config.
		return status
	}
	status.NodeHolds.Known = true
	status.NodeHolds.ConfigRevision = cfg.Revision
	status.NodeHolds.DefaultPaused, _ = cfg.Defaults["paused"].(bool)
	status.NodeHolds.DefaultDrain, _ = cfg.Defaults["drain"].(bool)
	for name := range cfg.Overrides {
		effective, _ := cfg.ResolveFor(name)
		paused, _ := effective["paused"].(bool)
		drain, _ := effective["drain"].(bool)
		if paused || drain {
			status.NodeHolds.Nodes = append(status.NodeHolds.Nodes, farmNodeHold{Name: name, Paused: paused, Drain: drain})
		}
	}
	sort.Slice(status.NodeHolds.Nodes, func(i, j int) bool {
		return status.NodeHolds.Nodes[i].Name < status.NodeHolds.Nodes[j].Name
	})
	return status
}

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
		writeJSON(w, http.StatusOK, a.farmControlStatus(ctx, ctl, true))
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
			writeJSON(w, http.StatusOK, a.farmControlStatus(ctx, ctl, true))
			return
		}
		writeJSON(w, http.StatusOK, a.farmControlStatus(ctx, stored, true))
	default:
		http.Error(w, "GET or PUT only", http.StatusMethodNotAllowed)
	}
}

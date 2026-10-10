package manager

// activation.go: the H-8 option B mode switch (H8_OPTION_B_PLAN.md §3).
//
// The proxy reads jmlease:v1:cfg "mode" (passthrough | observe | enforce)
// and lets it override its configured start mode. The Manager writes that
// field through the proxy's `admin` principal (clients cannot write
// jmlease:* otherwise). Founder decision 2026-10-06: there is no anonymous
// principal and no anon gate; unauthenticated clients are refused in every
// mode, so enforce is gated only by an explicit confirmation. Stepping back
// (observe, passthrough) is never gated: it is the first rollback step.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"time"
)

// LeaseModeStore reads and writes jmlease:v1:cfg "mode".
type LeaseModeStore interface {
	GetMode(ctx context.Context) (string, error)
	SetMode(ctx context.Context, mode string) error
}

var leaseModes = map[string]bool{"passthrough": true, "observe": true, "enforce": true}

type activation struct {
	store LeaseModeStore
}

type modeResponse struct {
	Mode string `json:"mode"`
	// Source: "cfg" when jmlease:v1:cfg sets it, "proxy-start" when unset
	// (the proxy then runs the mode its compose environment started with).
	Source string `json:"source"`
}

// handleMode is GET/POST /api/leases/mode.
func (a *activation) handleMode(w http.ResponseWriter, r *http.Request) {
	if a == nil || a.store == nil {
		http.Error(w, "mode switch is not configured (set JM_META_ADMIN_PASSWORD to the proxy admin principal's secret)", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	switch r.Method {
	case http.MethodGet:
		m, err := a.store.GetMode(ctx)
		if err != nil {
			http.Error(w, "reading the mode failed: "+err.Error(), http.StatusBadGateway)
			return
		}
		resp := modeResponse{Mode: m, Source: "cfg"}
		if m == "" {
			resp.Source = "proxy-start"
		}
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, http.StatusOK, resp)
	case http.MethodPost:
		var body struct {
			Mode    string `json:"mode"`
			Confirm string `json:"confirm"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body); err != nil || !leaseModes[body.Mode] {
			http.Error(w, "mode must be passthrough, observe or enforce", http.StatusBadRequest)
			return
		}
		if body.Mode == "enforce" && body.Confirm != "enforce" {
			http.Error(w, "enforce refuses every write that conflicts with a lease; repeat confirm:\"enforce\" to switch", http.StatusBadRequest)
			return
		}
		if err := a.store.SetMode(ctx, body.Mode); err != nil {
			status := http.StatusBadGateway
			if errors.Is(err, context.DeadlineExceeded) {
				status = http.StatusGatewayTimeout
			}
			http.Error(w, "writing the mode failed: "+err.Error(), status)
			return
		}
		log.Printf("manager: proxy lease mode set to %s", body.Mode)
		writeJSON(w, http.StatusOK, modeResponse{Mode: body.Mode, Source: "cfg"})
	default:
		http.Error(w, "GET or POST", http.StatusMethodNotAllowed)
	}
}

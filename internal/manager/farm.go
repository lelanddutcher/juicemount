package manager

import (
	"encoding/json"
	"net/http"
	"os"
)

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

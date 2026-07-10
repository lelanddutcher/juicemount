package manager

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

// farmChangesBasename is the pre-aggregated changes feed the farm publishes
// next to farm-status.json (internal/farm.ChangesFeedBasename — kept as a
// string literal here so the manager binary does not import internal/farm,
// which would drag the sqlite driver in; the parity test pins the two).
const farmChangesBasename = "derivatives-changes.json"

// farmChangesMaxLimit mirrors derivatives.Store.ListChangedSince's clamp so
// the manager-relayed feed and the Mac control plane's /derivatives/changes
// behave identically: limit ≤ 0 or > 10000 ⇒ 10000.
const farmChangesMaxLimit = 10000

// farmChangeRow is the contract's derivatives-changes row
// (spec/schema/derivatives-changes.schema.json), duplicated from
// derivatives.ChangeRow so the sqlite-free manager binary never imports the
// derivatives package. A test asserts the two marshal identically — drift
// fails CI, not production.
type farmChangeRow struct {
	Inode     uint64  `json:"inode"`
	Kind      string  `json:"kind"`
	Status    string  `json:"status"`
	Hash      *string `json:"hash"`
	UpdatedAt int64   `json:"updated_at"`
}

// deriveFarmChangesPath resolves where the farm's changes feed lives:
// explicit config wins, then the JM_FARM_CHANGES-style env value, then the
// sibling of the farm-status file (the farm always writes them side by side).
// Empty when none apply — the route then 503s as unconfigured.
func deriveFarmChangesPath(explicit, env, statusPath string) string {
	if explicit != "" {
		return explicit
	}
	if env != "" {
		return env
	}
	if statusPath != "" {
		return filepath.Join(filepath.Dir(statusPath), farmChangesBasename)
	}
	return ""
}

// handleFarmDerivativesChanges serves GET /api/farm/derivatives/changes
// ?since=<unix>&limit=N — the JM-15 #56 server half. It relays the farm's
// pre-aggregated feed file, filtered to rows with updated_at STRICTLY > since,
// ascending by (updated_at, inode, kind), capped at limit. The response is the
// contract's bare array (derivatives-changes.schema.json) — the same shape the
// Mac control plane serves for ITS index — so the Mac-side reconcile can learn
// farm-generated derivatives without a sidecar re-sweep.
//
// Parameter semantics mirror the Mac handler exactly: a missing or unparseable
// `since` means 0 (full history — cold start has no cursor); a missing/garbage
// `limit` gets the ListChangedSince default clamp (10000).
//
// Failure modes (fail closed, but never lie about emptiness):
//   - feed path unconfigured        → 503 (surface unavailable)
//   - feed file absent              → 200 [] (farm hasn't published yet; the
//     consumer's cursor doesn't advance on an empty array, so nothing is lost)
//   - feed unreadable / corrupt     → 503 (do NOT serve [] for "unknown" —
//     that's indistinguishable from "no changes" only because the cursor
//     stays put, but a corrupt feed deserves a retryable error, not silence)
func (a *API) handleFarmDerivativesChanges(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	if a.farmChangesPath == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "farm changes feed not configured (set JM_FARM_STATUS / JM_FARM_CHANGES or mount the juicefarm-state volume)",
		})
		return
	}

	var since int64
	if v := r.URL.Query().Get("since"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			since = n
		}
	}
	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	if limit <= 0 || limit > farmChangesMaxLimit {
		limit = farmChangesMaxLimit
	}

	raw, err := os.ReadFile(a.farmChangesPath)
	if err != nil {
		if os.IsNotExist(err) {
			writeJSON(w, http.StatusOK, []farmChangeRow{})
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "farm changes feed unreadable: " + err.Error(),
		})
		return
	}
	var rows []farmChangeRow
	if err := json.Unmarshal(raw, &rows); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "farm changes feed corrupt: " + err.Error(),
		})
		return
	}

	// The farm writes rows ascending already; re-assert the contract order
	// defensively (a hand-edited or partially-migrated file must not leak an
	// out-of-order feed to the cursor-advancing consumer).
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].UpdatedAt != rows[j].UpdatedAt {
			return rows[i].UpdatedAt < rows[j].UpdatedAt
		}
		if rows[i].Inode != rows[j].Inode {
			return rows[i].Inode < rows[j].Inode
		}
		return rows[i].Kind < rows[j].Kind
	})

	out := make([]farmChangeRow, 0, len(rows))
	for _, row := range rows {
		if row.UpdatedAt > since {
			out = append(out, row)
			if len(out) >= limit {
				break
			}
		}
	}
	writeJSON(w, http.StatusOK, out)
}

package nfs

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/lelanddutcher/juicemount/metadata"
)

// SpoolStatusResponse is the JSON shape served at /spool and consumed
// by the menu bar app + Manager UI. Exported so the Swift JSON decoder
// can reference the same key names via tags here.
type SpoolStatusResponse struct {
	Enabled       bool             `json:"enabled"`
	Error         string           `json:"error,omitempty"`
	PendingFiles  int              `json:"pending_files"`
	PendingBytes  int64            `json:"pending_bytes"`
	InProgress    int64            `json:"in_progress"`
	Succeeded     int64            `json:"succeeded"`
	Failed        int64            `json:"failed"`
	Quarantined   int64            `json:"quarantined"`
	CapacityUsed  int64            `json:"capacity_used"`
	CapacityTotal int64            `json:"capacity_total"`
	Entries       []SpoolEntryView `json:"entries"`
}

// SpoolEntryView is a single row's worth of UI-facing state.
type SpoolEntryView struct {
	Path          string `json:"path"`
	Size          int64  `json:"size"`
	DrainState    string `json:"drain_state"`
	DrainAttempts int    `json:"drain_attempts"`
	LastError     string `json:"last_error,omitempty"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

// SpoolStatusEntryCap caps the per-response entry list so menu-bar
// payloads stay small on a 1 Hz poll. Older rows live in SQLite for
// audit but are not returned via this endpoint.
const SpoolStatusEntryCap = 200

// BuildSpoolStatus assembles the response struct from a live spool
// store + drainer. Either may be nil — that combination is the
// "spool not enabled" response. Returned error is non-nil only on
// SQL failure; the caller should still serialize the partial response
// in that case so the UI can render a degraded view.
func BuildSpoolStatus(spool *SpoolStore, drainer *Drainer) (SpoolStatusResponse, error) {
	if spool == nil {
		return SpoolStatusResponse{
			Enabled: false,
			Error:   "spool not enabled (set JM_SPOOL_ENABLE=1 to opt in)",
		}, nil
	}

	resp := SpoolStatusResponse{Enabled: true}

	pendingFiles, pendingBytes, err := spool.Meta().PendingStats()
	if err != nil {
		resp.Error = "pending stats: " + err.Error()
		return resp, err
	}
	resp.PendingFiles = pendingFiles
	resp.PendingBytes = pendingBytes

	used, total := spool.Capacity()
	resp.CapacityUsed = used
	resp.CapacityTotal = total

	rows, listErr := spool.Meta().ListAll()
	if listErr != nil {
		resp.Error = "list entries: " + listErr.Error()
		// Continue — counters above are still useful for the UI.
	}
	views := make([]SpoolEntryView, 0)
	for _, r := range rows {
		switch r.DrainState {
		case metadata.DrainWriting, metadata.DrainReady, metadata.DrainDraining, metadata.DrainFailed:
			views = append(views, SpoolEntryView{
				Path:          r.NFSPath,
				Size:          r.Size,
				DrainState:    string(r.DrainState),
				DrainAttempts: r.DrainAttempts,
				LastError:     r.LastError,
				UpdatedAtUnix: r.UpdatedAt.Unix(),
			})
		}
	}
	// Newest-first by SQL id (ListAll preserves insert order; reverse).
	for i, j := 0, len(views)-1; i < j; i, j = i+1, j-1 {
		views[i], views[j] = views[j], views[i]
	}
	if len(views) > SpoolStatusEntryCap {
		views = views[:SpoolStatusEntryCap]
	}
	resp.Entries = views

	if drainer != nil {
		m := drainer.Metrics()
		resp.InProgress = m.InFlight.Load()
		resp.Succeeded = m.DrainsSucceeded.Load()
		resp.Failed = m.DrainsFailed.Load()
		resp.Quarantined = m.Quarantined.Load()
	}

	return resp, listErr
}

// WriteSpoolStatusJSON is the HTTP-handler-friendly wrapper: serializes
// BuildSpoolStatus to w with the right Content-Type and status code.
// 503 when the spool is disabled (spool == nil), 500 on SQL failure
// (still emits the partial body so the UI can show degraded state),
// 200 on the happy path.
func WriteSpoolStatusJSON(w http.ResponseWriter, spool *SpoolStore, drainer *Drainer) {
	w.Header().Set("Content-Type", "application/json")
	resp, err := BuildSpoolStatus(spool, drainer)
	switch {
	case !resp.Enabled:
		w.WriteHeader(http.StatusServiceUnavailable)
	case err != nil:
		w.WriteHeader(http.StatusInternalServerError)
	}
	if encErr := json.NewEncoder(w).Encode(resp); encErr != nil {
		// Headers already flushed; best-effort. Tests assert payload
		// via direct call to BuildSpoolStatus, not via this writer.
		_ = encErr
	}
}

// Compile-time check: json.NewEncoder wants an io.Writer.
var _ io.Writer = (http.ResponseWriter)(nil)

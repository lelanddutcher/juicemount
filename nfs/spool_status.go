package nfs

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

// SpoolStatusResponse is the JSON shape served at /spool and consumed
// by the menu bar app + Manager UI. Exported so the Swift JSON decoder
// can reference the same key names via tags here.
type SpoolStatusResponse struct {
	Enabled       bool   `json:"enabled"`
	Error         string `json:"error,omitempty"`
	PendingFiles  int    `json:"pending_files"`
	PendingBytes  int64  `json:"pending_bytes"`
	InProgress    int64  `json:"in_progress"`
	Succeeded     int64  `json:"succeeded"`
	Failed        int64  `json:"failed"`
	Quarantined   int64  `json:"quarantined"`
	CapacityUsed  int64  `json:"capacity_used"`
	CapacityTotal int64  `json:"capacity_total"`
	// StalledFiles counts listed entries flagged Stalled (see
	// SpoolEntryView.Stalled). FailedFiles counts the listed failed
	// rows — i.e. failed rows still relevant to the user (recoverable
	// data on disk, or failed recently), not all-time history.
	// OldestPendingAgeSec is the age of the oldest writing/ready/
	// draining row, so the UI can say "queued · 2h" without timestamp
	// math. All three are LB-5 stuck-spool affordance signals.
	StalledFiles        int              `json:"stalled_files"`
	FailedFiles         int              `json:"failed_files"`
	OldestPendingAgeSec int64            `json:"oldest_pending_age_sec"`
	// StallWaiters is the number of writes currently PARKED in the capacity
	// stall (spool full, blocking for headroom). Offline reports the current
	// offline state. OfflineBufferFull is the derived UI trigger: while offline
	// and the spool is full with writes waiting, the app should surface
	// "Offline buffer full — N copies paused, reconnect to drain" instead of
	// letting the bare NFS NOSPC read as "your disk is full". (With the graceful
	// stall the copy no longer FAILS — it pauses — so this is the signal that
	// tells the user WHY it paused and what to do.)
	StallWaiters      int  `json:"stall_waiters"`
	Offline           bool `json:"offline"`
	OfflineBufferFull bool `json:"offline_buffer_full"`
	Entries           []SpoolEntryView `json:"entries"`
}

// SpoolEntryView is a single row's worth of UI-facing state.
type SpoolEntryView struct {
	Path          string `json:"path"`
	Size          int64  `json:"size"`
	DrainState    string `json:"drain_state"`
	DrainAttempts int    `json:"drain_attempts"`
	LastError     string `json:"last_error,omitempty"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
	// AgeSec is now - updated_at, floored at 0.
	AgeSec int64 `json:"age_sec"`
	// Stalled means this entry is making no progress and won't without
	// intervention: a `writing` row quiescent beyond the sweeper's
	// escalation window (leaked handle — the sweeper will force-finalize
	// it, and /spool-recover?action=clear-stalled does so immediately),
	// or a ready/draining row whose drain_attempts already hit the
	// drainer's retry ceiling (it will only fail-permanent on its next
	// claim). Failed rows are NOT stalled — they get FailedFiles and the
	// retry-failed action instead.
	Stalled bool `json:"stalled"`
}

// SpoolStatusEntryCap caps the per-response entry list so menu-bar
// payloads stay small on a 1 Hz poll. Older rows live in SQLite for
// audit but are not returned via this endpoint.
const SpoolStatusEntryCap = 200

// SpoolStatusDoneTailWindow / SpoolStatusDoneTailCap bound the
// "recently finished" tail appended after the active rows. Done rows
// older than the window (or beyond the cap) are history — they live in
// SQLite until DeleteDone GC, but listing them in /spool made the UI
// show 46 rows with pending=0 (Phase-0 observation).
const (
	SpoolStatusDoneTailWindow = 5 * time.Minute
	SpoolStatusDoneTailCap    = 10
)

// SpoolStatusFailedRetention is how long a failed row with NO
// recoverable spool file stays listed (recent-error feedback). Failed
// rows whose spool file still exists are always listed — they are
// actionable via /spool-recover?action=retry-failed.
const SpoolStatusFailedRetention = time.Hour

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
			// Non-nil empty slice so the JSON is `"entries": []`, never null.
			// A nil Go slice marshals to JSON null, which makes Swift's
			// synthesized Codable throw valueNotFound and abort the whole
			// decode (the CacheStatus roots:null root cause). This disabled
			// branch is the COMMON case — the spool is opt-in.
			Entries: []SpoolEntryView{},
		}, nil
	}

	// Initialize Entries to a non-nil empty slice up front so even the early
	// error returns below (pending-stats / list failures) emit `"entries": []`
	// rather than null — same roots:null decode-abort guard as the branch above.
	resp := SpoolStatusResponse{Enabled: true, Entries: []SpoolEntryView{}}

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

	// Offline-buffer-full signal (2026-06-15). With the graceful capacity stall
	// a full spool PAUSES writes instead of failing them, so the app needs an
	// explicit reason to show the user. OfflineBufferFull fires while offline
	// with writes parked in the stall — the cue to reconnect to drain.
	resp.StallWaiters = int(spool.StallWaiters())
	resp.Offline = pin.IsOffline()
	resp.OfflineBufferFull = resp.Offline && resp.StallWaiters > 0

	// QA-38: only fetch the rows the view actually renders (active + the
	// recent-done tail). The old ListAll() scanned the entire spool table —
	// which had grown to 44k+ rows — on every poll, burning CPU + GC and
	// wedging the mount. The loop below already discards done rows older than
	// SpoolStatusDoneTailWindow, so this is output-identical.
	rows, listErr := spool.Meta().ListForStatus(time.Now().Add(-SpoolStatusDoneTailWindow))
	if listErr != nil {
		resp.Error = "list entries: " + listErr.Error()
		// Continue — counters above are still useful for the UI.
	}

	// maxAttempts mirrors the drainer's retry ceiling for the
	// attempts-exhausted stalled signal. With no drainer wired (status
	// built between Stop and Start) fall back to the DrainerConfig
	// default so the signal stays meaningful.
	maxAttempts := 5
	if drainer != nil {
		maxAttempts = drainer.maxAttempts
	}
	// escalateAfter is read the same way the sweeper reads it: set once
	// at construction (or by tests before concurrency starts).
	escalate := spool.escalateAfter
	now := time.Now()
	ageOf := func(updatedAt time.Time) int64 {
		sec := int64(now.Sub(updatedAt) / time.Second)
		if sec < 0 {
			sec = 0
		}
		return sec
	}

	views := make([]SpoolEntryView, 0)
	doneTail := make([]SpoolEntryView, 0)
	for _, r := range rows {
		v := SpoolEntryView{
			Path:          r.NFSPath,
			Size:          r.Size,
			DrainState:    string(r.DrainState),
			DrainAttempts: r.DrainAttempts,
			LastError:     r.LastError,
			UpdatedAtUnix: r.UpdatedAt.Unix(),
			AgeSec:        ageOf(r.UpdatedAt),
		}
		switch r.DrainState {
		case metadata.DrainWriting, metadata.DrainReady, metadata.DrainDraining:
			if r.DrainState == metadata.DrainWriting {
				// `writing` rows persist size=0 until finalize; the live
				// index entry knows the real high-water mark. Overlay it
				// (the CancelForDelete pattern) so the UI never renders
				// "Zero KB" for an entry holding real bytes, and fold the
				// live bytes into pending_bytes so pending_files>0 with
				// pending_bytes=0 can't recur. In-memory lookup only — no
				// per-RPC cost, and /spool runs on the cold control plane.
				if e, ok := spool.index.Lookup(r.NFSPath); ok && e.ID() == r.ID {
					if we := e.WrittenEnd(); we > v.Size {
						resp.PendingBytes += we - v.Size
						v.Size = we
					}
					// Stalled: quiescent beyond the sweeper's escalation
					// window — only a leaked handle looks like this (the
					// same predicate escalateIfStuck enforces).
					if escalate > 0 && now.Sub(e.LastWrite()) >= escalate {
						v.Stalled = true
					}
				} else if escalate > 0 && now.Sub(r.UpdatedAt) >= escalate {
					// A writing row with NO live index entry can never
					// finalize (nothing holds it); flag once it's old.
					v.Stalled = true
				}
			} else if r.DrainAttempts >= maxAttempts {
				// Retry budget exhausted — the row will only transition
				// to failed on its next claim; the UI should already be
				// offering recovery.
				v.Stalled = true
			}
			if v.Stalled {
				resp.StalledFiles++
			}
			if v.AgeSec > resp.OldestPendingAgeSec {
				resp.OldestPendingAgeSec = v.AgeSec
			}
			views = append(views, v)

		case metadata.DrainFailed:
			// Failed rows are listed only while RELEVANT: spool file
			// still on disk (recoverable via retry-failed), or failed
			// recently (error feedback). Ancient failures with no data
			// are history, not UI rows. The os.Stat per failed row is
			// fine here — cold control-plane path, capped row count.
			fileExists := false
			if r.SpoolFile != "" {
				_, statErr := os.Stat(r.SpoolFile)
				fileExists = statErr == nil
			}
			if fileExists || now.Sub(r.UpdatedAt) <= SpoolStatusFailedRetention {
				resp.FailedFiles++
				views = append(views, v)
			}

		case metadata.DrainDone:
			// Recently-finished tail only (Phase-0 observation: 46
			// historical done rows listed with pending=0). `succeeded`
			// remains the all-time counter.
			if now.Sub(r.UpdatedAt) <= SpoolStatusDoneTailWindow {
				doneTail = append(doneTail, v)
			}
		}
	}
	// Newest-first by SQL id (ListAll preserves insert order; reverse),
	// active rows first, then the recently-done tail.
	reverseViews := func(s []SpoolEntryView) {
		for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
			s[i], s[j] = s[j], s[i]
		}
	}
	reverseViews(views)
	reverseViews(doneTail)
	if len(doneTail) > SpoolStatusDoneTailCap {
		doneTail = doneTail[:SpoolStatusDoneTailCap]
	}
	views = append(views, doneTail...)
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

package manager

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHandleFarmSweepValidation locks in the input-validation contract of
// POST /api/farm/sweep with a nil farmQ (no Redis). The handler must reject
// bad input with a 400 BEFORE it ever needs the queue, and only fall through
// to a 503 ("farm queue unavailable") once the request itself is well-formed.
//
// No Redis is touched: every case here either short-circuits on validation or
// hits the nil-farmQ guard, so the test stays a pure unit test.
func TestHandleFarmSweepValidation(t *testing.T) {
	// a.farmQ is left nil — Register isn't run, so the handler exercises the
	// validation path and the unavailable-queue guard only.
	a := &API{}

	cases := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "empty path",
			body:     `{"path":"","kinds":["derivatives"]}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing path field",
			body:     `{"kinds":["all"]}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty kinds",
			body:     `{"path":"/jfs/SPARQ","kinds":[]}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing kinds field",
			body:     `{"path":"/jfs/SPARQ"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "unknown kind",
			body:     `{"path":"/jfs/SPARQ","kinds":["derivatives","bogus"]}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "malformed JSON",
			body:     `{"path":"/jfs/SPARQ",`,
			wantCode: http.StatusBadRequest,
		},
		{
			// Valid input, but nil farmQ → the queue is unavailable.
			name:     "valid input, no queue",
			body:     `{"path":"/jfs/SPARQ","kinds":["derivatives","proxy","transcript"]}`,
			wantCode: http.StatusServiceUnavailable,
		},
		{
			// "all" is a valid single kind; still 503 with no queue.
			name:     "valid all kind, no queue",
			body:     `{"path":"/jfs/SPARQ","kinds":["all"],"options":{"crf":21,"preset":"slow"}}`,
			wantCode: http.StatusServiceUnavailable,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/farm/sweep", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			a.handleFarmSweep(rec, req)
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d (body: %q)", rec.Code, tc.wantCode, rec.Body.String())
			}
		})
	}
}

// TestHandleFarmSweepMethodGuard confirms a non-POST verb is rejected before
// any body decode / queue access.
func TestHandleFarmSweepMethodGuard(t *testing.T) {
	a := &API{}
	req := httptest.NewRequest(http.MethodGet, "/api/farm/sweep", nil)
	rec := httptest.NewRecorder()
	a.handleFarmSweep(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

// TestHandleFarmJobsNoQueue confirms GET /api/farm/jobs degrades gracefully to
// {available:false} (HTTP 200) when the manager has no Redis queue wired, rather
// than NPE'ing on a nil farmQ.
func TestHandleFarmJobsNoQueue(t *testing.T) {
	a := &API{}
	req := httptest.NewRequest(http.MethodGet, "/api/farm/jobs", nil)
	rec := httptest.NewRecorder()
	a.handleFarmJobs(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if body := rec.Body.String(); !strings.Contains(body, `"available": false`) {
		t.Fatalf("expected available:false in response, got: %q", body)
	}
}

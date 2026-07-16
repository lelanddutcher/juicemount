package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
	"github.com/santhosh-tekuri/jsonschema/v5"
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

// ---- success-path + contract conformance (fake queue, no Redis) ------------

// fakeFarmQueue is an in-memory farmQueue so the handlers' SUCCESS paths are
// testable without a live Redis (the production implementation is
// *farmqueue.Client; the handlers only ever see the interface).
type fakeFarmQueue struct {
	enqueueErr error
	enqueued   []farmqueue.Job
	workers    []farmqueue.Worker
	depth      int64
	jobs       []farmqueue.JobStatus
	clearErr   error
}

func (f *fakeFarmQueue) Enqueue(_ context.Context, j farmqueue.Job) error {
	if f.enqueueErr != nil {
		return f.enqueueErr
	}
	f.enqueued = append(f.enqueued, j)
	return nil
}
func (f *fakeFarmQueue) ActiveWorkers(context.Context) ([]farmqueue.Worker, error) {
	return f.workers, nil
}
func (f *fakeFarmQueue) QueueDepth(context.Context) (int64, error) { return f.depth, nil }
func (f *fakeFarmQueue) ListJobs(context.Context, int) ([]farmqueue.JobStatus, error) {
	return f.jobs, nil
}
func (f *fakeFarmQueue) ClearFinished(context.Context) (int, error) {
	if f.clearErr != nil {
		return 0, f.clearErr
	}
	kept := f.jobs[:0]
	removed := 0
	for _, j := range f.jobs {
		if j.Status == farmqueue.StatusDone || j.Status == farmqueue.StatusFailed {
			removed++
			continue
		}
		kept = append(kept, j)
	}
	f.jobs = kept
	return removed, nil
}

// compileContractSchema loads + compiles one vendored contract schema. loc may
// carry a JSON-pointer fragment (e.g. "farm-queue.schema.json#/definitions/response")
// to validate against a sub-schema. Draft is auto-detected from each file's $schema.
func compileContractSchema(t *testing.T, loc string) *jsonschema.Schema {
	t.Helper()
	file, frag, _ := strings.Cut(loc, "#")
	path := filepath.Join("..", "..", "contract", "spec", "schema", file)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read contract schema %s: %v", file, err)
	}
	var meta struct {
		ID string `json:"$id"`
	}
	_ = json.Unmarshal(b, &meta)
	id := meta.ID
	if id == "" {
		id = file
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource(id, bytes.NewReader(b)); err != nil {
		t.Fatalf("add contract schema %s: %v", file, err)
	}
	target := id
	if frag != "" {
		target += "#" + frag
	}
	sch, err := c.Compile(target)
	if err != nil {
		t.Fatalf("compile contract schema %s: %v", loc, err)
	}
	return sch
}

// decodeJSON unmarshals raw JSON into the interface{} tree jsonschema validates.
func decodeJSON(t *testing.T, raw []byte) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode JSON: %v (body: %q)", err, raw)
	}
	return v
}

// readContractFixture loads a golden response from the vendored contract.
func readContractFixture(t *testing.T, rel string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "contract", "fixtures", rel))
	if err != nil {
		t.Fatalf("read contract fixture %s: %v", rel, err)
	}
	return b
}

// subsetMatch asserts every key/value present in fixture also appears (equal)
// in got. got MAY carry additive keys the fixture omits (e.g. processed:0 on a
// queued job — the contract schemas leave additionalProperties open); the
// fixture is the floor, not the ceiling. Arrays must match element-wise.
func subsetMatch(t *testing.T, path string, fixture, got any) {
	t.Helper()
	switch fx := fixture.(type) {
	case map[string]any:
		gm, ok := got.(map[string]any)
		if !ok {
			t.Errorf("%s: fixture is object, got %T", path, got)
			return
		}
		for k, fv := range fx {
			gv, ok := gm[k]
			if !ok {
				t.Errorf("%s.%s: missing in response", path, k)
				continue
			}
			subsetMatch(t, path+"."+k, fv, gv)
		}
	case []any:
		ga, ok := got.([]any)
		if !ok {
			t.Errorf("%s: fixture is array, got %T", path, got)
			return
		}
		if len(ga) != len(fx) {
			t.Errorf("%s: array length %d, want %d", path, len(ga), len(fx))
			return
		}
		for i := range fx {
			subsetMatch(t, fmt.Sprintf("%s[%d]", path, i), fx[i], ga[i])
		}
	default:
		if fixture != got {
			t.Errorf("%s: got %v (%T), want %v (%T)", path, got, got, fixture, fixture)
		}
	}
}

// TestHandleFarmSweepEnqueueConformance drives the POST /api/farm/sweep success
// path end-to-end against a fake queue and holds the wire shapes to the
// contract: the accepted request body validates against
// farm-queue.schema.json#/definitions/request, the response against
// #/definitions/response (and carries the enqueue-ok.json fixture's exact key
// set), and the Job that lands on the queue is stamped producer="manager" with
// the caller's path/kinds/options applied.
func TestHandleFarmSweepEnqueueConformance(t *testing.T) {
	body := `{
	  "path": "/jfs/Film Projects/SPARQ",
	  "kinds": ["derivatives", "proxy"],
	  "options": {"crf": 18, "preset": "medium", "model": "medium.en",
	              "vcodec": "h264_nvenc", "workers": 6, "proxy_workers": 3}
	}`
	// The manager accepts exactly the contract's request shape.
	reqSchema := compileContractSchema(t, "farm-queue.schema.json#/definitions/request")
	if err := reqSchema.Validate(decodeJSON(t, []byte(body))); err != nil {
		t.Fatalf("test request does not match contract request schema: %v", err)
	}

	fake := &fakeFarmQueue{}
	a := &API{farmQ: fake}
	req := httptest.NewRequest(http.MethodPost, "/api/farm/sweep", strings.NewReader(body))
	rec := httptest.NewRecorder()
	a.handleFarmSweep(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %q)", rec.Code, rec.Body.String())
	}

	// Response validates against the contract response schema.
	respSchema := compileContractSchema(t, "farm-queue.schema.json#/definitions/response")
	respTree := decodeJSON(t, rec.Body.Bytes())
	if err := respSchema.Validate(respTree); err != nil {
		t.Fatalf("response violates contract schema: %v\nbody: %s", err, rec.Body.String())
	}
	// Same key set as the golden enqueue-ok fixture ({id, status}) — additive
	// drift here would surprise the OpenLoupe/Farm-tab consumers.
	var fixtureKeys, gotKeys map[string]any
	if err := json.Unmarshal(readContractFixture(t, "farm/enqueue-ok.json"), &fixtureKeys); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &gotKeys); err != nil {
		t.Fatalf("response: %v", err)
	}
	if len(gotKeys) != len(fixtureKeys) {
		t.Errorf("response keys = %v, want the fixture's key set %v", gotKeys, fixtureKeys)
	}
	for k := range fixtureKeys {
		if _, ok := gotKeys[k]; !ok {
			t.Errorf("response missing fixture key %q", k)
		}
	}
	if s, _ := gotKeys["status"].(string); s != "queued" {
		t.Errorf("status = %q, want %q", s, "queued")
	}
	if id, _ := gotKeys["id"].(string); !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(id) {
		t.Errorf("id = %q, want 16-hex per contract", id)
	}

	// The enqueued Job is stamped + carries the overrides.
	if len(fake.enqueued) != 1 {
		t.Fatalf("enqueued %d jobs, want 1", len(fake.enqueued))
	}
	j := fake.enqueued[0]
	if j.Producer != "manager" {
		t.Errorf("producer = %q, want manager", j.Producer)
	}
	if j.Path != "/jfs/Film Projects/SPARQ" {
		t.Errorf("path = %q", j.Path)
	}
	if len(j.Kinds) != 2 || j.Kinds[0] != "derivatives" || j.Kinds[1] != "proxy" {
		t.Errorf("kinds = %v", j.Kinds)
	}
	if j.CRF != 18 || j.Preset != "medium" || j.Model != "medium.en" ||
		j.VCodec != "h264_nvenc" || j.Workers != 6 || j.ProxyWorkers != 3 {
		t.Errorf("options not applied: %+v", j)
	}
	if _, err := time.Parse(time.RFC3339, j.EnqueuedAt); err != nil {
		t.Errorf("enqueued_at %q is not RFC3339: %v", j.EnqueuedAt, err)
	}
}

// TestHandleFarmSweepRedisUnreachable pins the contract's failure semantics:
// "503 if Redis/meta unavailable". A queue that is CONFIGURED but unreachable
// at enqueue time must be a 503 (retryable, with a JSON error body), not a 500.
func TestHandleFarmSweepRedisUnreachable(t *testing.T) {
	fake := &fakeFarmQueue{enqueueErr: errors.New("dial tcp 192.168.0.197:30179: connect: connection refused")}
	a := &API{farmQ: fake}
	req := httptest.NewRequest(http.MethodPost, "/api/farm/sweep",
		strings.NewReader(`{"path":"/jfs/SPARQ","kinds":["all"]}`))
	rec := httptest.NewRecorder()
	a.handleFarmSweep(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body: %q)", rec.Code, rec.Body.String())
	}
	var errBody map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &errBody); err != nil {
		t.Fatalf("503 body is not JSON: %v (%q)", err, rec.Body.String())
	}
	if _, ok := errBody["error"]; !ok {
		t.Fatalf("503 body lacks an error field: %q", rec.Body.String())
	}
}

// TestHandleFarmJobsFixtureConformance seeds the fake queue with the contract's
// jobs.json golden data and asserts GET /api/farm/jobs (a) validates against
// farm-jobs.schema.json and (b) reproduces every fixture field verbatim
// (fixture ⊆ response — the handler may add schema-legal zero-count fields the
// fixture omits, e.g. processed:0 on a still-queued job).
func TestHandleFarmJobsFixtureConformance(t *testing.T) {
	raw := readContractFixture(t, "farm/jobs.json")
	// The fixture parses through the SAME wire structs the worker maintains —
	// field-name drift in farmqueue would fail here, not in production.
	var fx struct {
		Available  bool                  `json:"available"`
		Workers    []farmqueue.Worker    `json:"workers"`
		QueueDepth int64                 `json:"queue_depth"`
		Jobs       []farmqueue.JobStatus `json:"jobs"`
	}
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatalf("fixture does not fit the farmqueue wire structs: %v", err)
	}

	fake := &fakeFarmQueue{workers: fx.Workers, depth: fx.QueueDepth, jobs: fx.Jobs}
	a := &API{farmQ: fake}
	req := httptest.NewRequest(http.MethodGet, "/api/farm/jobs", nil)
	rec := httptest.NewRecorder()
	a.handleFarmJobs(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	sch := compileContractSchema(t, "farm-jobs.schema.json")
	respTree := decodeJSON(t, rec.Body.Bytes())
	if err := sch.Validate(respTree); err != nil {
		t.Fatalf("response violates farm-jobs contract schema: %v\nbody: %s", err, rec.Body.String())
	}
	// The golden fixture itself must also satisfy the schema (guards against a
	// stale vendored pair) …
	if err := sch.Validate(decodeJSON(t, raw)); err != nil {
		t.Fatalf("contract fixture no longer matches its own schema: %v", err)
	}
	// … and the response must carry the fixture verbatim.
	subsetMatch(t, "$", decodeJSON(t, raw), respTree)
}

// TestHandleFarmJobsEmptyNeverNull pins the Go↔Swift/JS null discipline on the
// idle-queue shape: workers/jobs marshal as [] (never null), available=false
// with zero heartbeating workers, and the whole body still schema-validates.
func TestHandleFarmJobsEmptyNeverNull(t *testing.T) {
	a := &API{farmQ: &fakeFarmQueue{}}
	req := httptest.NewRequest(http.MethodGet, "/api/farm/jobs", nil)
	rec := httptest.NewRecorder()
	a.handleFarmJobs(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	sch := compileContractSchema(t, "farm-jobs.schema.json")
	tree := decodeJSON(t, rec.Body.Bytes())
	if err := sch.Validate(tree); err != nil {
		t.Fatalf("empty response violates schema: %v\nbody: %s", err, rec.Body.String())
	}
	m := tree.(map[string]any)
	if avail, _ := m["available"].(bool); avail {
		t.Errorf("available = true with zero workers")
	}
	if _, ok := m["workers"].([]any); !ok {
		t.Errorf("workers is %T, want [] (never null)", m["workers"])
	}
	if _, ok := m["jobs"].([]any); !ok {
		t.Errorf("jobs is %T, want [] (never null)", m["jobs"])
	}
}

// TestHandleFarmJobsClear verifies POST /api/farm/jobs/clear removes ONLY
// terminal (done/failed) records and returns the count — queued/running
// jobs must survive so an in-flight sweep is never orphaned. 503 when the
// queue is unconfigured.
func TestHandleFarmJobsClear(t *testing.T) {
	fake := &fakeFarmQueue{jobs: []farmqueue.JobStatus{
		{ID: "a", Status: farmqueue.StatusRunning},
		{ID: "b", Status: farmqueue.StatusDone},
		{ID: "c", Status: farmqueue.StatusQueued},
		{ID: "d", Status: farmqueue.StatusFailed},
	}}
	a := &API{farmQ: fake}
	rec := httptest.NewRecorder()
	a.handleFarmJobsClear(rec, httptest.NewRequest(http.MethodPost, "/api/farm/jobs/clear", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	var body struct {
		Cleared int `json:"cleared"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}
	if body.Cleared != 2 {
		t.Fatalf("cleared = %d, want 2 (done+failed)", body.Cleared)
	}
	if len(fake.jobs) != 2 {
		t.Fatalf("remaining jobs = %d, want 2 (running+queued kept)", len(fake.jobs))
	}
	for _, j := range fake.jobs {
		if j.Status == farmqueue.StatusDone || j.Status == farmqueue.StatusFailed {
			t.Fatalf("terminal job %q survived clear", j.ID)
		}
	}

	a2 := &API{}
	rec2 := httptest.NewRecorder()
	a2.handleFarmJobsClear(rec2, httptest.NewRequest(http.MethodPost, "/api/farm/jobs/clear", nil))
	if rec2.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured status = %d, want 503", rec2.Code)
	}
}

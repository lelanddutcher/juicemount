package manager

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

func runFarmNodeControl(t *testing.T, a *API, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	a.handleFarmWorkerControl(rec, req)
	return rec
}

func TestFarmWorkerControlRestartRequiresOneLiveStableName(t *testing.T) {
	fake := &fakeFarmQ{}
	a := &API{farmQ: fake}
	body := `{"name":"render-a","action":"restart"}`

	rec := runFarmNodeControl(t, a, http.MethodPost, "/api/farm/workers/control", body)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "not online") {
		t.Fatalf("offline restart = %d %s", rec.Code, rec.Body.String())
	}

	fake.workers = []farmqueue.Worker{
		{ID: "runtime-a", Name: "render-a", State: "working"},
		{ID: "runtime-b", Name: "render-a", State: "idle"},
	}
	rec = runFarmNodeControl(t, a, http.MethodPost, "/api/farm/workers/control", body)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "multiple live workers") {
		t.Fatalf("duplicate restart = %d %s", rec.Code, rec.Body.String())
	}
	rec = runFarmNodeControl(t, a, http.MethodPost, "/api/farm/workers/control", `{"name":"render-a","action":"disable"}`)
	if rec.Code != http.StatusConflict || fake.disabled["render-a"] {
		t.Fatalf("duplicate disable = %d %s state=%v", rec.Code, rec.Body.String(), fake.disabled)
	}

	fake.workers = fake.workers[:1]
	rec = runFarmNodeControl(t, a, http.MethodPost, "/api/farm/workers/control", body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("live restart = %d %s", rec.Code, rec.Body.String())
	}
	var response struct {
		OK      bool                    `json:"ok"`
		Command farmqueue.WorkerCommand `json:"command"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !response.OK || response.Command.ID != "cmd-test" || response.Command.Name != "render-a" {
		t.Fatalf("restart response = %+v", response)
	}

	rec = runFarmNodeControl(t, a, http.MethodGet, "/api/farm/workers/control?command_id=cmd-test", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"state": "requested"`) {
		t.Fatalf("command status = %d %s", rec.Code, rec.Body.String())
	}
}

func TestFarmWorkerControlDisableEnableAndRestartGate(t *testing.T) {
	fake := &fakeFarmQ{workers: []farmqueue.Worker{{ID: "runtime-a", Name: "render-a"}}}
	a := &API{farmQ: fake}

	rec := runFarmNodeControl(t, a, http.MethodPost, "/api/farm/workers/control", `{"name":"render-a","action":"disable"}`)
	if rec.Code != http.StatusOK || !fake.disabled["render-a"] {
		t.Fatalf("disable = %d %s state=%v", rec.Code, rec.Body.String(), fake.disabled)
	}
	rec = runFarmNodeControl(t, a, http.MethodPost, "/api/farm/workers/control", `{"name":"render-a","action":"restart"}`)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "enable it before restarting") {
		t.Fatalf("disabled restart = %d %s", rec.Code, rec.Body.String())
	}
	rec = runFarmNodeControl(t, a, http.MethodPost, "/api/farm/workers/control", `{"name":"render-a","action":"enable"}`)
	if rec.Code != http.StatusOK || fake.disabled["render-a"] {
		t.Fatalf("enable = %d %s state=%v", rec.Code, rec.Body.String(), fake.disabled)
	}
}

func TestFarmWorkerControlValidationAndAvailability(t *testing.T) {
	rec := runFarmNodeControl(t, &API{}, http.MethodPost, "/api/farm/workers/control", `{"name":"render-a","action":"disable"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil queue status = %d", rec.Code)
	}

	a := &API{farmQ: &fakeFarmQ{}}
	for _, body := range []string{
		`{"name":"../render","action":"disable"}`,
		`{"name":"render-a","action":"destroy"}`,
		`not-json`,
	} {
		rec = runFarmNodeControl(t, a, http.MethodPost, "/api/farm/workers/control", body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %q status = %d: %s", body, rec.Code, rec.Body.String())
		}
	}
}

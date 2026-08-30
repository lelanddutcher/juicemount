package manager

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

func TestFarmNodePauseDrainResumeAreExplicitConfig(t *testing.T) {
	fake := &fakeFarmQ{}
	a := &API{farmQ: fake}
	for _, tc := range []struct {
		action        string
		paused, drain bool
	}{
		{"pause", true, false},
		{"drain", false, true},
		{"resume", false, false},
	} {
		req := httptest.NewRequest(http.MethodPost, "/api/farm/node/render-a/"+tc.action, nil)
		rec := httptest.NewRecorder()
		a.handleFarmNodeAction(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s = %d %s", tc.action, rec.Code, rec.Body.String())
		}
		patch := fake.cfg.Overrides["render-a"]
		if patch["paused"] != tc.paused || patch["drain"] != tc.drain {
			t.Fatalf("%s patch = %#v", tc.action, patch)
		}
	}
	if fake.cfg.Revision != 3 {
		t.Fatalf("config revision = %d, want 3", fake.cfg.Revision)
	}
}

func TestFarmJobActionsAndLogTail(t *testing.T) {
	fake := &fakeFarmQ{
		workers: []farmqueue.Worker{{ID: "runtime-a", Name: "render-a"}},
		logs: map[string][]farmqueue.WorkerLogLine{
			"runtime-a": {{At: "2026-08-30T00:00:00Z", Line: "claimed job abc"}},
		},
	}
	a := &API{farmQ: fake}

	rec := httptest.NewRecorder()
	a.handleFarmJobAction(rec, httptest.NewRequest(http.MethodPost, "/api/farm/job/abc/cancel", nil))
	if rec.Code != http.StatusAccepted || len(fake.canceled) != 1 || fake.canceled[0] != "abc" {
		t.Fatalf("cancel = %d %s calls=%v", rec.Code, rec.Body.String(), fake.canceled)
	}

	rec = httptest.NewRecorder()
	a.handleFarmJobAction(rec, httptest.NewRequest(http.MethodPost, "/api/farm/job/abc/requeue", nil))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("requeue = %d %s", rec.Code, rec.Body.String())
	}
	var retry struct {
		ID      string `json:"id"`
		Created bool   `json:"created"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &retry); err != nil || retry.ID != "retry-abc" || !retry.Created {
		t.Fatalf("requeue response = %+v err=%v", retry, err)
	}

	rec = httptest.NewRecorder()
	a.handleFarmNodeAction(rec, httptest.NewRequest(http.MethodGet, "/api/farm/node/render-a/log", nil))
	if rec.Code != http.StatusOK || !json.Valid(rec.Body.Bytes()) || rec.Body.String() == "" {
		t.Fatalf("log = %d %s", rec.Code, rec.Body.String())
	}
}

func TestFarmQueuesReturnsNamedLanes(t *testing.T) {
	fake := &fakeFarmQueue{depth: 7}
	a := &API{farmQ: fake}
	rec := httptest.NewRecorder()
	a.handleFarmQueues(rec, httptest.NewRequest(http.MethodGet, "/api/farm/queues", nil))
	if rec.Code != http.StatusOK || !json.Valid(rec.Body.Bytes()) {
		t.Fatalf("queues = %d %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Queues map[string]int64 `json:"queues"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.Queues["catch_all"] != 7 {
		t.Fatalf("queues body = %+v err=%v", body, err)
	}
}

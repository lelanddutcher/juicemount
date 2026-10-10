package manager

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

func TestFarmGlobalPlayReportsRemainingNodeHolds(t *testing.T) {
	fake := &fakeFarmQ{
		control: farmqueue.FarmControl{Revision: 2, Paused: true, WatchEnabled: true},
		cfg: &farmqueue.FarmConfig{
			Revision: 7,
			Defaults: map[string]any{"paused": false},
			Overrides: map[string]map[string]any{
				"render-b": {"drain": true},
				"render-a": {"paused": true},
			},
		},
	}
	a := &API{farmQ: fake}
	rec := httptest.NewRecorder()
	a.handleFarmControl(rec, httptest.NewRequest(http.MethodPut, "/api/farm/control", strings.NewReader(`{"paused":false}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("play status = %d body=%s", rec.Code, rec.Body.String())
	}
	var got farmControlStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !got.Known || got.Paused || !got.NodeHolds.Known || got.NodeHolds.ConfigRevision != 7 {
		t.Fatalf("control status = %+v", got)
	}
	if len(got.NodeHolds.Nodes) != 2 || got.NodeHolds.Nodes[0].Name != "render-a" ||
		!got.NodeHolds.Nodes[0].Paused || got.NodeHolds.Nodes[1].Name != "render-b" ||
		!got.NodeHolds.Nodes[1].Drain {
		t.Fatalf("node holds = %+v", got.NodeHolds)
	}
	if fake.cfg.Overrides["render-a"]["paused"] != true || fake.cfg.Overrides["render-b"]["drain"] != true {
		t.Fatalf("global play changed node overrides: %+v", fake.cfg.Overrides)
	}
}

func TestFarmControlMissingConfigLeavesNodeHoldsUnknown(t *testing.T) {
	fake := &fakeFarmQ{control: farmqueue.FarmControl{Revision: 3, WatchEnabled: true}}
	a := &API{farmQ: fake}
	rec := httptest.NewRecorder()
	a.handleFarmControl(rec, httptest.NewRequest(http.MethodGet, "/api/farm/control", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("get status = %d body=%s", rec.Code, rec.Body.String())
	}
	var got farmControlStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !got.Known || got.NodeHolds.Known || got.NodeHolds.Nodes == nil {
		t.Fatalf("missing config reported known holds: %+v", got)
	}
}

type controlReadFailureQueue struct{ fakeFarmQ }

func (f *controlReadFailureQueue) GetControl(context.Context) (farmqueue.FarmControl, error) {
	return farmqueue.FarmControl{}, errors.New("control unavailable")
}

func TestFarmJobsControlReadFailureIsUnknown(t *testing.T) {
	a := &API{farmQ: &controlReadFailureQueue{}}
	rec := httptest.NewRecorder()
	a.handleFarmJobs(rec, httptest.NewRequest(http.MethodGet, "/api/farm/jobs", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("jobs status = %d body=%s", rec.Code, rec.Body.String())
	}
	var got struct {
		Control farmControlStatus `json:"control"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Control.Known || got.Control.NodeHolds.Known {
		t.Fatalf("failed control read reported known state: %+v", got.Control)
	}
}

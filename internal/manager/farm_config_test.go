package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

// fakeFarmQ provides the config-method surface handleFarmConfig uses, without
// a live Redis.
type fakeFarmQ struct {
	cfg     *farmqueue.FarmConfig
	stored  int
	deleted int
	workers []farmqueue.Worker
	control farmqueue.FarmControl
}

func (f *fakeFarmQ) Enqueue(ctx context.Context, j farmqueue.Job) error { return nil }
func (f *fakeFarmQ) ActiveWorkers(ctx context.Context) ([]farmqueue.Worker, error) {
	return f.workers, nil
}
func (f *fakeFarmQ) QueueDepth(ctx context.Context) (int64, error) { return 0, nil }
func (f *fakeFarmQ) ListJobs(ctx context.Context, n int) ([]farmqueue.JobStatus, error) {
	return nil, nil
}
func (f *fakeFarmQ) ClearFinished(ctx context.Context) (int, error) { return 0, nil }
func (f *fakeFarmQ) GetControl(ctx context.Context) (farmqueue.FarmControl, error) {
	if f.control.Revision == 0 && !f.control.WatchEnabled {
		return farmqueue.DefaultFarmControl(), nil
	}
	return f.control, nil
}
func (f *fakeFarmQ) StoreControl(ctx context.Context, ctl farmqueue.FarmControl, force int64) (int64, error) {
	if force > 0 {
		ctl.Revision = force
	} else {
		ctl.Revision = f.control.Revision + 1
	}
	f.control = ctl
	return ctl.Revision, nil
}

func (f *fakeFarmQ) GetConfig(ctx context.Context) (*farmqueue.FarmConfig, error) {
	return f.cfg, nil
}
func (f *fakeFarmQ) StoreConfig(ctx context.Context, cfg *farmqueue.FarmConfig, force int64) (int64, error) {
	f.stored++
	if force >= 1 {
		cfg.Revision = force
	} else if f.cfg != nil {
		cfg.Revision = f.cfg.Revision + 1
	} else {
		cfg.Revision = 1
	}
	f.cfg = cfg
	return cfg.Revision, nil
}
func (f *fakeFarmQ) DeleteConfig(ctx context.Context) error {
	f.deleted++
	f.cfg = nil
	return nil
}

// Compile-time proof the fake satisfies the API's farmQueue seam.
var _ farmQueue = (*fakeFarmQ)(nil)

func TestValidateFarmConfigRejectsBad(t *testing.T) {
	cases := []struct {
		name    string
		patch   map[string]any
		wantErr bool
	}{
		{"good crf", map[string]any{"crf": 21}, false},
		{"crf out of range", map[string]any{"crf": 99}, true},
		{"unknown key", map[string]any{"password": "hunter2"}, true},
		{"bad vcodec", map[string]any{"vcodec": "h264_mpeg999"}, true},
		{"good HEVC VAAPI codec", map[string]any{"vcodec": "hevc_vaapi"}, false},
		{"good HEVC QSV codec", map[string]any{"vcodec": "hevc_qsv"}, false},
		{"good HEVC NVENC codec", map[string]any{"vcodec": "hevc_nvenc"}, false},
		{"good HEVC software codec", map[string]any{"vcodec": "libx265"}, false},
		{"good device", map[string]any{"transcript_device": "vulkan"}, false},
		{"bad device", map[string]any{"transcript_device": "toaster"}, true},
		{"good preset", map[string]any{"preset": "slow"}, false},
		{"model injection", map[string]any{"model": "/etc/passwd; rm -rf /"}, true},
	}
	for _, tc := range cases {
		err := farmqueue.ValidateFarmConfig(tc.patch)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err=%v wantErr=%v", tc.name, err, tc.wantErr)
		}
	}
}

func TestResolveForMergesOverride(t *testing.T) {
	fc := &farmqueue.FarmConfig{
		Revision: 3,
		Defaults: map[string]any{"crf": float64(21), "transcript_device": "cpu", "nice": float64(10)},
		Overrides: map[string]map[string]any{
			"b70-gpu": {"transcript_device": "vulkan", "workers": float64(2)},
		},
	}
	eff, restart := fc.ResolveFor("b70-gpu")
	if eff["transcript_device"] != "vulkan" {
		t.Errorf("override lost: %v", eff)
	}
	if eff["crf"] != float64(21) {
		t.Errorf("default lost: %v", eff)
	}
	if len(restart) != 1 || restart[0] != "nice" {
		t.Errorf("restart keys = %v, want [nice]", restart)
	}
}

func TestHandleFarmConfigGetPutDelete(t *testing.T) {
	fake := &fakeFarmQ{}
	a := &API{adminKey: "", farmQ: fake}

	// GET with no config → available:true, config:null (cold start).
	rec := httptest.NewRecorder()
	a.handleFarmConfig(rec, httptest.NewRequest(http.MethodGet, "/api/farm/config", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET cold: %d %s", rec.Code, rec.Body.String())
	}

	// PUT valid defaults+override.
	body := map[string]any{
		"defaults":  map[string]any{"crf": 21},
		"overrides": map[string]map[string]any{"b70-gpu": {"transcript_device": "vulkan"}},
	}
	raw, _ := json.Marshal(body)
	rec = httptest.NewRecorder()
	a.handleFarmConfig(rec, httptest.NewRequest(http.MethodPut, "/api/farm/config", bytesReader(raw)))
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT: %d %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Revision int64                 `json:"revision"`
		Config   *farmqueue.FarmConfig `json:"config"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Revision != 1 || resp.Config == nil ||
		resp.Config.Overrides["b70-gpu"]["transcript_device"] != "vulkan" {
		t.Fatalf("PUT result wrong: %+v", rec.Body.String())
	}

	// PUT invalid key → 400 and NOTHING stored (second store call count stays).
	rec = httptest.NewRecorder()
	bad, _ := json.Marshal(map[string]any{"defaults": map[string]any{"secret_key": "x"}})
	a.handleFarmConfig(rec, httptest.NewRequest(http.MethodPut, "/api/farm/config", bytesReader(bad)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("PUT bad key: %d, want 400", rec.Code)
	}
	if fake.stored != 1 {
		t.Fatalf("rejected PUT stored anyway (%d)", fake.stored)
	}

	// DELETE clears.
	rec = httptest.NewRecorder()
	a.handleFarmConfig(rec, httptest.NewRequest(http.MethodDelete, "/api/farm/config", nil))
	if rec.Code != http.StatusOK || fake.cfg != nil {
		t.Fatalf("DELETE failed: %d cfg=%v", rec.Code, fake.cfg)
	}
}

func TestHandleFarmWorkersEnriched(t *testing.T) {
	fake := &fakeFarmQ{workers: []farmqueue.Worker{{
		ID: "w1", Name: "b70-gpu", ConfigRevision: 2,
		PendingRestart: []string{"nice"},
		Capabilities:   []string{"cpu", "vulkan", "vaapi"},
	}}}
	a := &API{farmQ: fake}
	rec := httptest.NewRecorder()
	a.handleFarmWorkers(rec, httptest.NewRequest(http.MethodGet, "/api/farm/workers", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("code %d", rec.Code)
	}
	var out struct {
		Workers []farmqueue.Worker `json:"workers"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if len(out.Workers) != 1 || out.Workers[0].Name != "b70-gpu" ||
		len(out.Workers[0].PendingRestart) != 1 {
		t.Fatalf("enriched workers lost: %+v", out)
	}
}

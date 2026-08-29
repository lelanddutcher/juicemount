package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

type storageGuardFarmQueue struct {
	fakeFarmQueue
	pauses int
}

func (f *storageGuardFarmQueue) PauseForSafety(_ context.Context, code, reason string) (farmqueue.FarmControl, error) {
	f.pauses++
	ctl, _ := f.GetControl(context.Background())
	ctl.Revision++
	ctl.Paused = true
	ctl.AutoPaused = true
	ctl.PauseCode = code
	ctl.PauseReason = reason
	ctl.UpdatedBy = "safety-interlock"
	f.control = ctl
	return ctl, nil
}

func unsafeStorageAPI(q farmQueue) *API {
	return &API{
		farmQ: q, farmStoragePath: "/backend", farmMinFree: 100,
		farmStorageStat: func(string) (farmStorageSnapshot, error) {
			return farmStorageSnapshot{TotalBytes: 1000, AvailableBytes: 99}, nil
		},
	}
}

func TestFarmStorageGuardAutoPausesSharedControl(t *testing.T) {
	q := &storageGuardFarmQueue{}
	a := unsafeStorageAPI(q)
	gate := a.enforceFarmStorageGuard(context.Background())
	if gate.Safe || gate.AvailableBytes != 99 || gate.RequiredBytes != 100 {
		t.Fatalf("gate = %+v", gate)
	}
	if q.pauses != 1 || !q.control.Paused || !q.control.AutoPaused || q.control.PauseCode != "storage-pressure" {
		t.Fatalf("control = %+v pauses=%d", q.control, q.pauses)
	}
}

func TestFarmSweepRejectsUnsafeStorageBeforeEnqueue(t *testing.T) {
	q := &storageGuardFarmQueue{}
	a := unsafeStorageAPI(q)
	req := httptest.NewRequest(http.MethodPost, "/api/farm/sweep",
		strings.NewReader(`{"path":"/jfs/incoming","kinds":["proxy"]}`))
	rec := httptest.NewRecorder()
	a.handleFarmSweep(rec, req)
	if rec.Code != http.StatusInsufficientStorage {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if len(q.enqueued) != 0 || q.pauses != 1 {
		t.Fatalf("enqueued=%d pauses=%d", len(q.enqueued), q.pauses)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	storage, ok := body["storage"].(map[string]any)
	if !ok || storage["safe"] != false {
		t.Fatalf("storage response = %#v", body["storage"])
	}
}

func TestFarmControlCannotResumeBelowStorageReserve(t *testing.T) {
	q := &storageGuardFarmQueue{fakeFarmQueue: fakeFarmQueue{control: farmqueue.FarmControl{
		Revision: 3, Paused: true, AutoPaused: true, WatchEnabled: true,
	}}}
	a := unsafeStorageAPI(q)
	req := httptest.NewRequest(http.MethodPut, "/api/farm/control", strings.NewReader(`{"paused":false}`))
	rec := httptest.NewRecorder()
	a.handleFarmControl(rec, req)
	if rec.Code != http.StatusInsufficientStorage {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if !q.control.Paused || !q.control.AutoPaused {
		t.Fatalf("unsafe resume cleared interlock: %+v", q.control)
	}
}

func TestFarmRequiredHeadroomScalesWithPool(t *testing.T) {
	if got := farmRequiredHeadroom(1<<40, 0); got != defaultFarmMinFree {
		t.Fatalf("1 TiB required = %d, want %d", got, defaultFarmMinFree)
	}
	if got := farmRequiredHeadroom(100<<40, 0); got != (100<<40)/100 {
		t.Fatalf("100 TiB required = %d, want one percent", got)
	}
	if got := farmRequiredHeadroom(100<<40, 1234); got != 1234 {
		t.Fatalf("configured required = %d, want 1234", got)
	}
}

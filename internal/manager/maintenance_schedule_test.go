package manager

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestScheduledArgv(t *testing.T) {
	mm := newMaintenanceManager("juicefs", "", "redis://x:6379/1", "/jfs")
	cases := map[MaintenanceKind]string{
		MaintenanceGC:          "juicefs gc redis://x:6379/1 --delete",
		MaintenanceFSCK:        "juicefs fsck redis://x:6379/1",
		MaintenanceCompactMeta: "juicefs gc --compact redis://x:6379/1",
	}
	for k, want := range cases {
		got, ok := mm.scheduledArgv(k)
		if !ok {
			t.Fatalf("%s: ok=false, want true", k)
		}
		if strings.Join(got, " ") != want {
			t.Fatalf("%s argv = %q, want %q", k, strings.Join(got, " "), want)
		}
	}
	if _, ok := mm.scheduledArgv(MaintenanceWarmup); ok {
		t.Fatal("warmup should not be schedulable")
	}
	// empty metaURL → not runnable.
	mm2 := newMaintenanceManager("juicefs", "/jfs", "", "/jfs")
	if _, ok := mm2.scheduledArgv(MaintenanceGC); ok {
		t.Fatal("gc with empty metaURL should be ok=false")
	}
}

func TestMaintenanceSchedulerSetPersist(t *testing.T) {
	mm := newMaintenanceManager("juicefs", "", "redis://x:6379/1", "/jfs")
	s := newMaintenanceScheduler(mm)
	list := s.list()
	if len(list) != 3 {
		t.Fatalf("list len = %d, want 3 (gc/fsck/compact)", len(list))
	}
	for _, info := range list {
		if info.Enabled {
			t.Fatalf("%s enabled by default, want off", info.Kind)
		}
		if info.RecommendedCron == "" || info.Advice == "" {
			t.Fatalf("%s missing recommended cron/advice", info.Kind)
		}
		if !info.Runnable {
			t.Fatalf("%s not runnable though metaURL is set", info.Kind)
		}
	}
	// enable gc monthly, round-trip through snapshot/load.
	if err := s.Set(MaintenanceGC, "0 3 1 * *", true); err != nil {
		t.Fatalf("Set: %v", err)
	}
	s2 := newMaintenanceScheduler(mm)
	s2.load(s.snapshot())
	found := false
	for _, info := range s2.list() {
		if info.Kind == "gc" {
			found = true
			if !info.Enabled || info.Cron != "0 3 1 * *" {
				t.Fatalf("gc after reload = %+v", info)
			}
		}
	}
	if !found {
		t.Fatal("gc missing after reload")
	}
	if err := s.Set(MaintenanceGC, "not a cron", true); err == nil {
		t.Fatal("expected bad cron to be rejected")
	}
	if err := s.Set(MaintenanceWarmup, "0 3 1 * *", true); err == nil {
		t.Fatal("expected non-schedulable kind to be rejected")
	}
}

func TestHandleMaintenanceSchedules(t *testing.T) {
	mm := newMaintenanceManager("juicefs", "", "redis://x:6379/1", "/jfs")
	a := &API{maintenance: mm, maintenanceSched: newMaintenanceScheduler(mm)}

	rec := httptest.NewRecorder()
	a.handleMaintenanceSchedules(rec, httptest.NewRequest(http.MethodGet, "/api/maintenance/schedules", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status %d", rec.Code)
	}

	body := `{"kind":"fsck","cron":"0 4 1 * *","enabled":true}`
	rec2 := httptest.NewRecorder()
	a.handleMaintenanceSchedules(rec2, httptest.NewRequest(http.MethodPut, "/api/maintenance/schedules", strings.NewReader(body)))
	if rec2.Code != http.StatusOK {
		t.Fatalf("PUT status %d (%s)", rec2.Code, rec2.Body.String())
	}

	rec3 := httptest.NewRecorder()
	a.handleMaintenanceSchedules(rec3, httptest.NewRequest(http.MethodGet, "/api/maintenance/schedules", nil))
	var resp struct {
		Schedules []maintenanceScheduleInfo `json:"schedules"`
	}
	if err := json.Unmarshal(rec3.Body.Bytes(), &resp); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}
	ok := false
	for _, s := range resp.Schedules {
		if s.Kind == "fsck" && s.Enabled {
			ok = true
		}
	}
	if !ok {
		t.Fatal("fsck not enabled after PUT")
	}

	// non-schedulable kind → 400.
	rec4 := httptest.NewRecorder()
	a.handleMaintenanceSchedules(rec4, httptest.NewRequest(http.MethodPut, "/api/maintenance/schedules", strings.NewReader(`{"kind":"warmup","cron":"0 4 1 * *","enabled":true}`)))
	if rec4.Code != http.StatusBadRequest {
		t.Fatalf("bad-kind PUT status %d, want 400", rec4.Code)
	}
}

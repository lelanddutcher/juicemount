package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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
		wantOn := info.Kind == string(MaintenanceCompactMeta)
		if info.Enabled != wantOn || !info.Default {
			t.Fatalf("%s default: enabled=%v default=%v, want enabled=%v default=true", info.Kind, info.Enabled, info.Default, wantOn)
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

// compact-meta is on by default (weekly, Sunday 03:00) for a new install and
// for an existing install that never saved a choice; an explicit Off, or any
// saved cadence, survives the default.
func TestCompactMetaDefaultOnRespectsExplicitChoice(t *testing.T) {
	mm := newMaintenanceManager("juicefs", "", "redis://x:6379/1", "/jfs")
	get := func(s *maintenanceScheduler) maintenanceScheduleInfo {
		for _, info := range s.list() {
			if info.Kind == string(MaintenanceCompactMeta) {
				return info
			}
		}
		t.Fatal("compact-meta missing")
		return maintenanceScheduleInfo{}
	}

	// New install / old state file without a compact-meta row.
	fresh := newMaintenanceScheduler(mm)
	fresh.load([]maintenanceScheduleRow{{Kind: "gc", Cron: "0 3 1 * *", Enabled: true}})
	if info := get(fresh); !info.Enabled || !info.Default || info.Cron != "0 3 * * 0" || info.NextRun == 0 {
		t.Fatalf("no saved choice: %+v, want on, default, weekly Sunday 03:00, next run set", info)
	}
	if next := time.UnixMilli(get(fresh).NextRun); next.Weekday() != time.Sunday || next.Hour() != 3 || next.Minute() != 0 {
		t.Fatalf("next run %s, want a Sunday at 03:00", next)
	}
	fresh.Start(context.Background())
	if _, ok := fresh.entries[MaintenanceCompactMeta]; !ok {
		t.Fatal("default compact-meta schedule not registered at Start")
	}
	if _, ok := fresh.entries[MaintenanceFSCK]; ok {
		t.Fatal("fsck registered though it is off by default")
	}
	fresh.Stop()

	// Existing install that explicitly turned it off (a pre-change row).
	off := newMaintenanceScheduler(mm)
	off.load([]maintenanceScheduleRow{{Kind: "compact-meta", Cron: "0 5 1 1,4,7,10 *", Enabled: false}})
	if info := get(off); info.Enabled || info.Default || info.NextRun != 0 {
		t.Fatalf("explicit off: %+v, want off and not default", info)
	}
	off.Start(context.Background())
	if _, ok := off.entries[MaintenanceCompactMeta]; ok {
		t.Fatal("explicitly-off compact-meta registered at Start")
	}
	off.Stop()

	// Existing install with its own cadence keeps it.
	quarterly := newMaintenanceScheduler(mm)
	quarterly.load([]maintenanceScheduleRow{{Kind: "compact-meta", Cron: "0 5 1 1,4,7,10 *", Enabled: true}})
	if info := get(quarterly); !info.Enabled || info.Default || info.Cron != "0 5 1 1,4,7,10 *" {
		t.Fatalf("explicit quarterly: %+v", info)
	}

	// Turning it off through the API is saved as a choice and survives a reload.
	s := newMaintenanceScheduler(mm)
	if err := s.Set(MaintenanceCompactMeta, "0 3 * * 0", false); err != nil {
		t.Fatal(err)
	}
	s2 := newMaintenanceScheduler(mm)
	s2.load(s.snapshot())
	if info := get(s2); info.Enabled || info.Default {
		t.Fatalf("off after reload: %+v", info)
	}
}

// A default run writes a row only to keep last_run; that row keeps following
// the default (it is not mistaken for an operator choice after a reload).
func TestCompactMetaDefaultRunKeepsFollowingDefault(t *testing.T) {
	mm := newMaintenanceManager("juicefs", "", "redis://x:6379/1", "/jfs")
	done := make(chan struct{})
	mm.SetRunner(func(_ context.Context, argv []string, _ *MaintenanceOp) error {
		if got := strings.Join(argv, " "); got != "juicefs gc --compact redis://x:6379/1" {
			t.Errorf("argv %q", got)
		}
		close(done)
		return nil
	})
	s := newMaintenanceScheduler(mm)
	s.fire(MaintenanceCompactMeta)
	<-done
	rows := s.snapshot()
	if len(rows) != 1 || !rows[0].Default || rows[0].LastRun == 0 {
		t.Fatalf("rows after a default run = %+v, want one Default row with last_run", rows)
	}
	s2 := newMaintenanceScheduler(mm)
	s2.load(rows)
	for _, info := range s2.list() {
		if info.Kind == string(MaintenanceCompactMeta) && (!info.Default || !info.Enabled || info.LastRun == 0) {
			t.Fatalf("after reload: %+v, want default, on, last_run kept", info)
		}
	}
}

// A scheduled run waits while another scheduled kind runs, then starts; it
// skips after waitMax.
func TestScheduledRunWaitsForOtherKind(t *testing.T) {
	mm := newMaintenanceManager("juicefs", "", "redis://x:6379/1", "/jfs")
	release := make(chan struct{})
	var compactStarted atomic.Int64
	mm.SetRunner(func(_ context.Context, argv []string, _ *MaintenanceOp) error {
		if strings.Contains(strings.Join(argv, " "), "--compact") {
			compactStarted.Store(time.Now().UnixNano())
			return nil
		}
		<-release // the gc sweep
		return nil
	})
	if _, err := mm.tryStart(MaintenanceGC, []string{"juicefs", "gc", "x", "--delete"}); err != nil {
		t.Fatal(err)
	}
	s := newMaintenanceScheduler(mm)
	s.waitPoll = 10 * time.Millisecond
	s.waitMax = 5 * time.Second
	fired := make(chan struct{})
	go func() { s.fire(MaintenanceCompactMeta); close(fired) }()
	time.Sleep(100 * time.Millisecond)
	if compactStarted.Load() != 0 {
		t.Fatal("compact started while gc was running")
	}
	gcEnd := time.Now().UnixNano()
	close(release)
	<-fired
	deadline := time.Now().Add(2 * time.Second)
	for compactStarted.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if c := compactStarted.Load(); c == 0 || c < gcEnd {
		t.Fatalf("compact start %d, want after gc end %d", c, gcEnd)
	}

	// Skip after waitMax while the other kind is still running.
	block := make(chan struct{})
	mm2 := newMaintenanceManager("juicefs", "", "redis://x:6379/1", "/jfs")
	var compacts atomic.Int32
	mm2.SetRunner(func(_ context.Context, argv []string, _ *MaintenanceOp) error {
		if strings.Contains(strings.Join(argv, " "), "--compact") {
			compacts.Add(1)
			return nil
		}
		<-block
		return nil
	})
	if _, err := mm2.tryStart(MaintenanceFSCK, []string{"juicefs", "fsck", "x"}); err != nil {
		t.Fatal(err)
	}
	s2 := newMaintenanceScheduler(mm2)
	s2.waitPoll = 10 * time.Millisecond
	s2.waitMax = 50 * time.Millisecond
	s2.fire(MaintenanceCompactMeta)
	close(block)
	time.Sleep(50 * time.Millisecond)
	if compacts.Load() != 0 {
		t.Fatal("compact ran though fsck outlasted waitMax")
	}
}

package manager

// Per-lever maintenance scheduling — GC / FSCK / compact-meta on a cron.
//
// JuiceFS auto-handles the ROUTINE cleanup: trash expiry (driven by
// --trash-days) and slice compaction both run in client background jobs.
// The three levers scheduled here are NOT automatic — the upstream
// "Status Check & Maintenance" docs call them "routine checks as needed"
// and note object leaks "almost never occur":
//
//   - gc  (juicefs gc --delete)     — reclaim leaked objects
//   - fsck (juicefs fsck)           — metadata↔object integrity check
//   - compact-meta (gc --compact)   — force slice compaction
//
// gc and fsck are OFF by default; the UI pre-fills a conservative
// recommended cadence + the advice text below and the operator opts in.
// compact-meta is ON by default (weekly, Sunday 03:00 Manager time): since
// juicefs patch 0036 remote and tunnel Macs no longer compact on read, so
// the NAS repacks fragmented chunks next to the data, with no WAN traffic.
//
// "Default" means: no operator choice is stored for that kind. A row the
// operator saved (PUT) always wins, including enabled=false, so an explicit
// "Off" survives this default. Rows the scheduler writes itself (to keep
// last_run for a default schedule) carry Default=true and keep following
// the default.
//
// Scheduled runs of different kinds never overlap: a run that finds another
// scheduled kind running waits for it (see fire). A run missed while the
// Manager was down is not caught up; the next one fires on schedule.
// Reuses robfig/cron (already a Backups dep) but keeps its own engine +
// persisted rows (one per kind) so it stays independent of the backup
// scheduler. Sources: juicefs.com/docs Status Check & Maintenance.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

// maintenanceRecommendation is the pre-fill + educational copy the UI
// shows per lever.
type maintenanceRecommendation struct {
	Cron   string
	Advice string
	// DefaultOn: the schedule runs at Cron until the operator saves a
	// choice for this kind.
	DefaultOn bool
}

// maintenanceRecommendations — conservative cadences because these sweeps
// are optional insurance, not required upkeep (JuiceFS auto-cleans the
// routine equivalents).
var maintenanceRecommendations = map[MaintenanceKind]maintenanceRecommendation{
	MaintenanceGC: {
		Cron:   "0 3 1 * *", // monthly, 1st @ 03:00
		Advice: "Reclaims leaked objects left by interrupted writes or crashes. JuiceFS auto-cleans routine garbage and leaks almost never happen, so this is optional insurance — and it scans the whole object store, so keep it infrequent. Recommended: monthly.",
	},
	MaintenanceFSCK: {
		Cron:   "0 4 1 * *", // monthly, 1st @ 04:00
		Advice: "Integrity check comparing metadata against object storage. Optional; a monthly run surfaces rare inconsistencies early. Recommended: monthly.",
	},
	MaintenanceCompactMeta: {
		// Weekly, Sunday @ 03:00 Manager time. 03:00 exists on every
		// daylight-saving change day (02:xx is skipped in spring in the US
		// and the EU), Sunday is the quietest day for a studio, and the slot
		// is clear of the monthly gc (1st @ 03:00) and fsck (1st @ 04:00)
		// recommendations except when the 1st is a Sunday; then the later
		// run waits for the earlier one (fire).
		Cron:      "0 3 * * 0",
		DefaultOn: true,
		Advice:    "Repacks every file part saved in more than one piece into whole blocks, on the NAS, next to the data: the sweep itself sends nothing over the internet. JuiceFS repacks when writing and when Macs near the server read; remote Macs and Macs on a tunnel do not repack on read, so this weekly run does it for them. Cost: after a sweep, each Mac away from the NAS downloads again, once, the repacked parts of files it had cached or pinned. The first sweep after an upgrade repacks everything saved in pieces so far (captures, renders, files edited in place) and can take hours; for a big pinned folder that re-download can be large. On by default since this version: if Compact showed Off before and you want it off, choose Off again. Recommended: weekly.",
	},
}

// schedulableMaintenanceKinds is the set the scheduler accepts. warmup /
// cache-flush are excluded — they need a path / aren't periodic hygiene.
var schedulableMaintenanceKinds = map[MaintenanceKind]bool{
	MaintenanceGC:          true,
	MaintenanceFSCK:        true,
	MaintenanceCompactMeta: true,
}

// orderedMaintenanceKinds gives the API/UI a deterministic order.
var orderedMaintenanceKinds = []MaintenanceKind{MaintenanceGC, MaintenanceFSCK, MaintenanceCompactMeta}

// maintenanceScheduler owns a cron engine + one persisted row per
// configured kind, and fires the matching maintenance op via the shared
// MaintenanceManager (same tryStart path as the manual buttons).
type maintenanceScheduler struct {
	mu       sync.Mutex
	cron     *cron.Cron
	rows     map[MaintenanceKind]*maintenanceScheduleRow
	entries  map[MaintenanceKind]cron.EntryID
	started  bool
	mm       *MaintenanceManager
	onChange func()

	// waitPoll / waitMax bound how long a scheduled run waits for another
	// scheduled kind to finish before it skips (fire). Fields for tests.
	waitPoll time.Duration
	waitMax  time.Duration

	// stop is closed by Stop, so a run waiting for another kind gives up
	// at shutdown instead of starting later (fire). Start makes a new one.
	stop chan struct{}

	// noDefaults: no state file, so an operator's choice would not
	// survive a restart; then no schedule runs by default (the embedded
	// jm5 Manager). Set by the API wiring.
	noDefaults bool
}

func newMaintenanceScheduler(mm *MaintenanceManager) *maintenanceScheduler {
	return &maintenanceScheduler{
		cron:     cron.New(cron.WithParser(cronParser)),
		rows:     make(map[MaintenanceKind]*maintenanceScheduleRow),
		entries:  make(map[MaintenanceKind]cron.EntryID),
		mm:       mm,
		waitPoll: time.Minute,
		waitMax:  6 * time.Hour,
		stop:     make(chan struct{}),
	}
}

// effectiveLocked returns the cadence and on/off state in force for kind:
// the operator's saved row, or the recommendation when no choice is saved.
// Caller holds mu.
func (s *maintenanceScheduler) effectiveLocked(kind MaintenanceKind) (cronExpr string, enabled bool, isDefault bool) {
	rec := maintenanceRecommendations[kind]
	if r, ok := s.rows[kind]; ok && !r.Default {
		return r.Cron, r.Enabled, false
	}
	return rec.Cron, rec.DefaultOn && !s.noDefaults, true
}

// SetOnChange wires the persistence callback (saveState).
func (s *maintenanceScheduler) SetOnChange(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onChange = fn
}

// snapshot / load satisfy the JobManager persistence hook.
func (s *maintenanceScheduler) snapshot() []maintenanceScheduleRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]maintenanceScheduleRow, 0, len(s.rows))
	for _, r := range s.rows {
		out = append(out, *r)
	}
	return out
}

func (s *maintenanceScheduler) load(rows []maintenanceScheduleRow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = make(map[MaintenanceKind]*maintenanceScheduleRow, len(rows))
	for i := range rows {
		k := MaintenanceKind(rows[i].Kind)
		if !schedulableMaintenanceKinds[k] {
			continue // drop rows for kinds we no longer schedule
		}
		cp := rows[i]
		s.rows[k] = &cp
	}
}

// Start spins up the cron engine and registers every enabled row.
// Idempotent. cron.Start is called outside the lock (its run loop calls
// back into fire()).
func (s *maintenanceScheduler) Start(_ context.Context) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	select {
	case <-s.stop: // stopped before: a fresh signal for this run
		s.stop = make(chan struct{})
	default:
	}
	logScheduleClock()
	for _, k := range orderedMaintenanceKinds {
		cronExpr, enabled, isDefault := s.effectiveLocked(k)
		if enabled {
			if isDefault {
				log.Printf("manager: maintenance schedule %q on by default (%s)", k, cronExpr)
			}
			if err := s.registerLocked(k, cronExpr); err != nil {
				log.Printf("manager: maintenance schedule %q failed to register: %v", k, err)
			}
		}
	}
	s.mu.Unlock()
	s.cron.Start()
}

// Stop drains the cron engine. Called on shutdown.
func (s *maintenanceScheduler) Stop() {
	s.mu.Lock()
	started := s.started
	s.started = false
	if started {
		close(s.stop) // releases runs waiting for another kind
	}
	s.mu.Unlock()
	if !started {
		return
	}
	ctxDone := s.cron.Stop()
	select {
	case <-ctxDone.Done():
	case <-time.After(5 * time.Second):
		log.Printf("manager: maintenanceScheduler.Stop timed out after 5s")
	}
}

// registerLocked adds/replaces the cron entry for kind. Caller holds mu.
func (s *maintenanceScheduler) registerLocked(kind MaintenanceKind, cronExpr string) error {
	_, sched, err := resolveCronExpr(cronExpr)
	if err != nil {
		return err
	}
	if prev, ok := s.entries[kind]; ok {
		s.cron.Remove(prev)
		delete(s.entries, kind)
	}
	k := kind
	s.entries[kind] = s.cron.Schedule(sched, cron.FuncJob(func() { s.fire(k) }))
	return nil
}

// Set upserts the schedule for kind. Enabled=false keeps the row (so the
// chosen cadence is remembered) but removes the live cron entry.
func (s *maintenanceScheduler) Set(kind MaintenanceKind, cronExpr string, enabled bool) error {
	if !schedulableMaintenanceKinds[kind] {
		return fmt.Errorf("maintenance kind %q is not schedulable", kind)
	}
	if _, _, err := resolveCronExpr(cronExpr); err != nil {
		return err
	}
	s.mu.Lock()
	r, ok := s.rows[kind]
	if !ok {
		r = &maintenanceScheduleRow{Kind: string(kind)}
		s.rows[kind] = r
	}
	r.Cron = cronExpr
	r.Enabled = enabled
	r.Default = false // the operator's choice; the default no longer applies
	if s.started {
		if enabled {
			_ = s.registerLocked(kind, cronExpr)
		} else if prev, ok := s.entries[kind]; ok {
			s.cron.Remove(prev)
			delete(s.entries, kind)
		}
	}
	cb := s.onChange
	s.mu.Unlock()
	if cb != nil {
		cb()
	}
	return nil
}

// fire is the cron callback — builds the op's argv and hands it to the
// shared MaintenanceManager (same one-per-kind mutex as the manual
// buttons, so a manual gc already running makes the scheduled one skip).
//
// Scheduled runs never overlap: the start is tryStartExclusive, which
// refuses, in the same locked step that claims the slot, while any other
// schedulable kind is running (gc and compact-meta both list every object).
// A refused run retries every waitPoll, at most waitMax, then skips; it also
// gives up when the scheduler stops or the operator turns the kind off.
// last_run records only a run that started; a skip goes to last_skipped.
func (s *maintenanceScheduler) fire(kind MaintenanceKind) {
	s.mu.Lock()
	mm := s.mm
	poll, maxWait, stop := s.waitPoll, s.waitMax, s.stop
	s.mu.Unlock()
	if mm == nil {
		return
	}
	argv, ok := mm.scheduledArgv(kind)
	if !ok {
		s.recordSkip(kind, "metadata URL not configured")
		log.Printf("manager: maintenance schedule %q not runnable (metaURL unset)", kind)
		return
	}
	for waited := time.Duration(0); ; waited += poll {
		_, err := mm.tryStartExclusive(kind, argv)
		if err == nil {
			s.recordRun(kind)
			return
		}
		var other *errOtherKindBusy
		if !errors.As(err, &other) {
			s.recordSkip(kind, err.Error())
			log.Printf("manager: maintenance schedule %q skipped: %v", kind, err)
			return
		}
		if waited >= maxWait {
			s.recordSkip(kind, fmt.Sprintf("%s still running after %s", other.Kind, maxWait))
			log.Printf("manager: maintenance schedule %q skipped: %q still running after %s", kind, other.Kind, maxWait)
			return
		}
		if waited == 0 {
			log.Printf("manager: maintenance schedule %q waits for %q to finish", kind, other.Kind)
		}
		timer := time.NewTimer(poll)
		select {
		case <-stop:
			timer.Stop()
			log.Printf("manager: maintenance schedule %q: scheduler stopped while waiting; skipped", kind)
			return
		case <-timer.C:
		}
		// The operator may have turned this kind off while it waited.
		s.mu.Lock()
		_, enabled, _ := s.effectiveLocked(kind)
		s.mu.Unlock()
		if !enabled {
			s.recordSkip(kind, "turned off while waiting")
			log.Printf("manager: maintenance schedule %q turned off while waiting; skipped", kind)
			return
		}
	}
}

// rowForRecordLocked returns kind's row, creating one marked Default for a
// default schedule so the kind keeps following the default. Caller holds mu.
func (s *maintenanceScheduler) rowForRecordLocked(kind MaintenanceKind) *maintenanceScheduleRow {
	r, ok := s.rows[kind]
	if !ok {
		cronExpr, enabled, _ := s.effectiveLocked(kind)
		r = &maintenanceScheduleRow{Kind: string(kind), Cron: cronExpr, Enabled: enabled, Default: true}
		s.rows[kind] = r
	}
	return r
}

func (s *maintenanceScheduler) recordRun(kind MaintenanceKind) {
	s.mu.Lock()
	r := s.rowForRecordLocked(kind)
	r.LastRun = time.Now().UnixMilli()
	r.LastSkipped, r.LastSkipReason = 0, ""
	cb := s.onChange
	s.mu.Unlock()
	if cb != nil {
		cb()
	}
}

func (s *maintenanceScheduler) recordSkip(kind MaintenanceKind, reason string) {
	s.mu.Lock()
	r := s.rowForRecordLocked(kind)
	r.LastSkipped = time.Now().UnixMilli()
	r.LastSkipReason = reason
	cb := s.onChange
	s.mu.Unlock()
	if cb != nil {
		cb()
	}
}

// logScheduleClock logs, once per Start, the zone the schedules run in, and
// a TZ value the runtime could not load (Go then falls back to UTC
// silently, so a typo like Europe/Berln would move every run).
func logScheduleClock() {
	tz := strings.TrimPrefix(os.Getenv("TZ"), ":")
	if tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			log.Printf("manager: TZ=%q is not a known time zone; schedules run in UTC (%v)", tz, err)
		}
	}
	log.Printf("manager: maintenance and backup schedules run in the Manager's zone %s", time.Now().Format("MST -0700"))
}

// maintenanceScheduleInfo is the API-boundary shape: the current setting
// plus the recommended cadence + advice the UI renders as guidance.
type maintenanceScheduleInfo struct {
	Kind    string `json:"kind"`
	Cron    string `json:"cron"`
	Enabled bool   `json:"enabled"`
	// Default is true when no operator choice is saved: Cron/Enabled are
	// the recommendation (compact-meta: on, weekly).
	Default         bool   `json:"default"`
	DefaultOn       bool   `json:"default_on"`
	LastRun         int64  `json:"last_run,omitempty"`
	LastSkipped     int64  `json:"last_skipped,omitempty"`
	LastSkipReason  string `json:"last_skip_reason,omitempty"`
	NextRun         int64  `json:"next_run,omitempty"`
	RecommendedCron string `json:"recommended_cron"`
	Advice          string `json:"advice"`
	// Runnable is false when the op would 501 (metaURL not configured) —
	// the UI can then explain scheduling won't fire until that's fixed.
	Runnable bool `json:"runnable"`
}

// list returns one info row per schedulable kind, in deterministic order,
// with the recommended cadence pre-filled when the operator hasn't set one.
func (s *maintenanceScheduler) list() []maintenanceScheduleInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	runnable := s.mm != nil && s.mm.metaURL != ""
	out := make([]maintenanceScheduleInfo, 0, len(orderedMaintenanceKinds))
	for _, k := range orderedMaintenanceKinds {
		rec := maintenanceRecommendations[k]
		cronExpr, enabled, isDefault := s.effectiveLocked(k)
		info := maintenanceScheduleInfo{
			Kind:            string(k),
			Cron:            cronExpr,
			Enabled:         enabled,
			Default:         isDefault,
			DefaultOn:       rec.DefaultOn,
			RecommendedCron: rec.Cron,
			Advice:          rec.Advice,
			Runnable:        runnable,
		}
		if r, ok := s.rows[k]; ok {
			info.LastRun = r.LastRun
			info.LastSkipped = r.LastSkipped
			info.LastSkipReason = r.LastSkipReason
		}
		if enabled {
			if _, sched, err := resolveCronExpr(cronExpr); err == nil {
				info.NextRun = sched.Next(time.Now()).UnixMilli()
			}
		}
		out = append(out, info)
	}
	return out
}

// handleMaintenanceSchedules is GET/PUT /api/maintenance/schedules.
// GET returns one row per schedulable lever (GC/FSCK/compact-meta) with
// its current setting + the recommended cadence and advice the UI renders
// as guidance. PUT sets one lever: body {kind, cron, enabled}.
func (a *API) handleMaintenanceSchedules(w http.ResponseWriter, r *http.Request) {
	if a.maintenanceSched == nil {
		http.Error(w, "maintenance scheduler not configured", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"schedules": a.maintenanceSched.list()})
	case http.MethodPut:
		var req struct {
			Kind    string `json:"kind"`
			Cron    string `json:"cron"`
			Enabled bool   `json:"enabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := a.maintenanceSched.Set(MaintenanceKind(req.Kind), req.Cron, req.Enabled); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"kind": req.Kind, "cron": req.Cron, "enabled": req.Enabled})
	default:
		http.Error(w, "GET or PUT", http.StatusMethodNotAllowed)
	}
}

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
	"fmt"
	"log"
	"net/http"
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
		Advice:    "Repacks files saved in many small pieces into whole blocks, on the NAS, next to the data: no internet traffic. JuiceFS repacks when writing and when Macs near the server read; remote Macs and Macs on a tunnel do not repack on read, so this weekly run does it for them. A Mac away from the NAS downloads a repacked part of a file again the next time it reads it. On by default. Recommended: weekly.",
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
}

func newMaintenanceScheduler(mm *MaintenanceManager) *maintenanceScheduler {
	return &maintenanceScheduler{
		cron:     cron.New(cron.WithParser(cronParser)),
		rows:     make(map[MaintenanceKind]*maintenanceScheduleRow),
		entries:  make(map[MaintenanceKind]cron.EntryID),
		mm:       mm,
		waitPoll: time.Minute,
		waitMax:  6 * time.Hour,
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
	return rec.Cron, rec.DefaultOn, true
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
// Before it starts, it waits (poll every waitPoll, at most waitMax) while
// another scheduled kind is running, so two whole-volume sweeps never load
// the NAS at once (gc and compact-meta both list every object). After
// waitMax it skips this run; the next one fires on schedule.
func (s *maintenanceScheduler) fire(kind MaintenanceKind) {
	now := time.Now().UnixMilli()
	s.mu.Lock()
	r, ok := s.rows[kind]
	if !ok {
		// A default schedule: keep last_run in a row marked Default, so the
		// kind still follows the default (effectiveLocked).
		cronExpr, enabled, _ := s.effectiveLocked(kind)
		r = &maintenanceScheduleRow{Kind: string(kind), Cron: cronExpr, Enabled: enabled, Default: true}
		s.rows[kind] = r
	}
	r.LastRun = now
	mm := s.mm
	cb := s.onChange
	poll, maxWait := s.waitPoll, s.waitMax
	s.mu.Unlock()
	if mm == nil {
		return
	}
	argv, ok := mm.scheduledArgv(kind)
	if !ok {
		log.Printf("manager: maintenance schedule %q not runnable (metaURL unset)", kind)
		return
	}
	for waited := time.Duration(0); ; waited += poll {
		other, busy := mm.otherScheduledKindRunning(kind)
		if !busy {
			break
		}
		if waited >= maxWait {
			log.Printf("manager: maintenance schedule %q skipped: %q still running after %s", kind, other, maxWait)
			if cb != nil {
				cb()
			}
			return
		}
		if waited == 0 {
			log.Printf("manager: maintenance schedule %q waits for %q to finish", kind, other)
		}
		time.Sleep(poll)
		// The operator may have turned this kind off while it waited.
		s.mu.Lock()
		_, enabled, _ := s.effectiveLocked(kind)
		s.mu.Unlock()
		if !enabled {
			log.Printf("manager: maintenance schedule %q turned off while waiting; skipped", kind)
			return
		}
	}
	if _, err := mm.tryStart(kind, argv); err != nil {
		log.Printf("manager: maintenance schedule %q skipped: %v", kind, err)
	}
	if cb != nil {
		cb()
	}
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

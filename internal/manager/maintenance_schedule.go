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
// So every schedule is OFF by default; the UI pre-fills a conservative
// recommended cadence + the advice text below and the operator opts in.
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
		Cron:   "0 5 1 1,4,7,10 *", // quarterly, 1st @ 05:00
		Advice: "Forces slice compaction to densify fragmented chunks. JuiceFS already compacts automatically during normal reads/writes, so only schedule this if you actually notice read fragmentation. Recommended: off, or quarterly.",
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
}

func newMaintenanceScheduler(mm *MaintenanceManager) *maintenanceScheduler {
	return &maintenanceScheduler{
		cron:    cron.New(cron.WithParser(cronParser)),
		rows:    make(map[MaintenanceKind]*maintenanceScheduleRow),
		entries: make(map[MaintenanceKind]cron.EntryID),
		mm:      mm,
	}
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
	for k, r := range s.rows {
		if r.Enabled {
			if err := s.registerLocked(k, r.Cron); err != nil {
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
func (s *maintenanceScheduler) fire(kind MaintenanceKind) {
	now := time.Now().UnixMilli()
	s.mu.Lock()
	if r, ok := s.rows[kind]; ok {
		r.LastRun = now
	}
	mm := s.mm
	cb := s.onChange
	s.mu.Unlock()
	if mm == nil {
		return
	}
	argv, ok := mm.scheduledArgv(kind)
	if !ok {
		log.Printf("manager: maintenance schedule %q not runnable (metaURL unset)", kind)
		return
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
	Kind            string `json:"kind"`
	Cron            string `json:"cron"`
	Enabled         bool   `json:"enabled"`
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
		info := maintenanceScheduleInfo{
			Kind:            string(k),
			Cron:            rec.Cron, // default to recommended when unset
			RecommendedCron: rec.Cron,
			Advice:          rec.Advice,
			Runnable:        runnable,
		}
		if r, ok := s.rows[k]; ok {
			info.Cron = r.Cron
			info.Enabled = r.Enabled
			info.LastRun = r.LastRun
			if r.Enabled {
				if _, sched, err := resolveCronExpr(r.Cron); err == nil {
					info.NextRun = sched.Next(time.Now()).UnixMilli()
				}
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

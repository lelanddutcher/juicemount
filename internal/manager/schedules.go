// Package manager — SLICE 5: cron-scheduled Backups.
//
// A Schedule is a stored cron expression that fires Submit() on the
// JobManager at each tick. Each fire produces a normal Job that flows
// through the existing migration pipeline (same SSE, same state-file
// row, same Migrations-tab job list) — the only differences are:
//
//   - the Job carries a ScheduleName annotation so the UI can group
//     scheduled runs together, and
//   - the Job's destination is resolved at submit time from a saved
//     Destination profile (slice-4), so credential plumbing flows via
//     cmd.Env (NOT argv) into the juicefs sync subprocess.
//
// The scheduler is a single robfig/cron Cron instance owned by
// scheduleStoreImpl. Start() spins it up; Stop() drains it gracefully
// on shutdown. Add/Update/Remove keep the cron entries map in sync
// with the persisted schedules slice.
//
// Persistence:  schedules live in persistedState.Schedules (state.go).
// Schema stays at v2 — extending within v2 (the json decoder leaves
// unknown fields at zero values, so older readers degrade cleanly).
package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

// SourceSpec describes a schedule's source. For ad-hoc Migrations-tab
// jobs the source is a raw path string; for schedules we want enough
// structure to support both host-path Out backups (Direction=in default)
// and future Direction=out exports. Direction empty defaults to "in"
// just like the migration request.
type SourceSpec struct {
	Path      string    `json:"path"`
	Direction Direction `json:"direction,omitempty"`
}

// DestinationRef refers to a saved destination by name. Resolved at
// fire time (NOT at schedule-creation time) so credential rotations
// in the Destinations tab take effect immediately on the next run.
type DestinationRef struct {
	Name string `json:"name"`
	// Path is an optional subdirectory inside the saved destination.
	// Useful for, e.g., a single S3 bucket profile re-used across many
	// schedules — each schedule appends a different prefix.
	Path string `json:"path,omitempty"`
}

// Schedule is the API-boundary form of a scheduleState row. JSON-
// identical apart from the History array (returned by GET /api/schedules
// only when the per-schedule view asks for it — list responses omit
// it so the payload stays small).
type Schedule struct {
	Name          string         `json:"name"`
	Source        SourceSpec     `json:"source"`
	Destination   DestinationRef `json:"destination"`
	Options       SyncOptions    `json:"options"`
	Cron          string         `json:"cron"`
	Paused        bool           `json:"paused"`
	RetainHistory int            `json:"retain_history"`
	LastRun       int64          `json:"last_run,omitempty"`
	NextRun       int64          `json:"next_run,omitempty"`
}

// cronPresets maps friendly names to standard 5-field cron expressions.
// Locked in the SLICE-5 spec — adding a new preset is a doc-and-code
// change so the UI dropdown stays in lockstep.
var cronPresets = map[string]string{
	"nightly-2am":    "0 2 * * *",
	"weekly-sun-3am": "0 3 * * 0",
	"hourly":         "0 * * * *",
	"every-6-hours":  "0 */6 * * *",
}

// cronParser is the SLICE-5 cron-expression parser. We use the
// standard (Minute Hour Dom Month Dow) 5-field grammar — the
// robfig/cron default — and explicitly enable DescriptorOption so
// "@hourly", "@daily" etc. also parse without surprise. Seconds are
// intentionally NOT enabled: a 1-second tick would clobber the
// JobManager's single-active-job serialization.
var cronParser = cron.NewParser(
	cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

// resolveCronExpr accepts either a preset name from cronPresets or a
// raw cron expression, and returns the canonical form along with the
// parsed schedule (used by the scheduler to compute NextRun). Empty
// input is rejected up front.
func resolveCronExpr(input string) (string, cron.Schedule, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", nil, errors.New("cron expression is empty")
	}
	expr := input
	if v, ok := cronPresets[input]; ok {
		expr = v
	}
	sched, err := cronParser.Parse(expr)
	if err != nil {
		return "", nil, fmt.Errorf("invalid cron expression %q: %w", expr, err)
	}
	return expr, sched, nil
}

// validateScheduleName uses the same character class as destinations —
// URL-segment-safe and short enough for clean log output. Kept as a
// separate function so a future divergence (e.g. allowing colons in
// schedule names) doesn't accidentally loosen destination naming.
const maxScheduleNameLen = 64

func validateScheduleName(name string) error {
	if name == "" {
		return errors.New("schedule name is required")
	}
	if len(name) > maxScheduleNameLen {
		return fmt.Errorf("name too long (%d > %d)", len(name), maxScheduleNameLen)
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_':
		default:
			return fmt.Errorf("name contains invalid character %q at position %d (allowed: a-z 0-9 - _)", r, i)
		}
	}
	return nil
}

// defaultRetainHistory is the per-schedule cap on stored run history
// when the caller leaves Schedule.RetainHistory at zero. Spec default.
const defaultRetainHistory = 20

// maxRetainHistory caps the per-schedule history at a sane bound so a
// misconfigured value can't unbounded-grow the state file. 1000 is
// generous — a daily schedule retaining 1000 runs covers ~3 years.
const maxRetainHistory = 1000

// scheduleStoreImpl owns schedules in memory, the cron engine, and the
// persistence callback. Mirrors destinationStoreImpl's structure so the
// JobManager can treat both as opaque snapshot/load providers.
type scheduleStoreImpl struct {
	mu       sync.RWMutex
	rows     []scheduleState
	entries  map[string]cron.EntryID
	cron     *cron.Cron
	started  bool
	mgr      *JobManager
	dests    *destinationStoreImpl
	fuseRoot string // for /jfs path rewriting on schedule fire
	onChange func()
}

// newScheduleStore constructs an empty store and the underlying cron
// engine. The engine isn't started here — Start() does that after the
// JobManager has finished loading persisted state.
//
// dests may be nil at construction time (Register wires it in
// post-construction); the scheduler refuses to fire schedules whose
// destination resolution fails, with a clear error logged + recorded
// in history.
func newScheduleStore(mgr *JobManager, dests *destinationStoreImpl, fuseRoot string) *scheduleStoreImpl {
	return &scheduleStoreImpl{
		entries:  make(map[string]cron.EntryID),
		cron:     cron.New(cron.WithParser(cronParser)),
		mgr:      mgr,
		dests:    dests,
		fuseRoot: fuseRoot,
	}
}

// SetOnChange wires the persistence callback (saveState).
func (s *scheduleStoreImpl) SetOnChange(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onChange = fn
}

// snapshot satisfies the scheduleStore interface — returns the
// persisted-form rows for the JobManager to flush to disk. Holds only
// the read lock so it can be called from saveState under JobManager.mu
// without re-entering scheduler mutations.
func (s *scheduleStoreImpl) snapshot() []scheduleState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]scheduleState, len(s.rows))
	copy(out, s.rows)
	return out
}

// load installs schedules read from disk on startup. Replaces any
// in-memory rows. Called once during Register before Start().
func (s *scheduleStoreImpl) load(rows []scheduleState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append([]scheduleState(nil), rows...)
}

// Start spins up the cron engine and registers every loaded schedule.
// Idempotent — repeated calls are a no-op after the first. Called by
// Register once after both the JobManager and the destinations store
// are wired up.
func (s *scheduleStoreImpl) Start(ctx context.Context) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	// Hold the write lock across the registration loop — registerLocked
	// mutates s.entries and reads s.rows, both of which would race a
	// concurrent upsert/remove if we'd dropped the lock. cron.Start
	// itself spins a goroutine that calls back into fire() which
	// re-acquires s.mu, so we MUST start the cron engine outside the
	// lock to avoid self-deadlock on the first immediate-fire tick.
	registerErrors := make(map[string]error, len(s.rows))
	for _, r := range s.rows {
		if err := s.registerLocked(r); err != nil {
			registerErrors[r.Name] = err
		}
	}
	s.mu.Unlock()
	for name, err := range registerErrors {
		log.Printf("manager: schedule %q failed to register at startup: %v (skipping; fix via PUT /api/schedules/%s)", name, err, name)
	}
	s.cron.Start()
}

// Stop drains the cron engine. Called by JobManager.StopAll() on
// shutdown. Blocks until the last in-flight Submit returns (typically
// instant — Submit just appends to a queue).
func (s *scheduleStoreImpl) Stop() {
	s.mu.Lock()
	started := s.started
	s.started = false
	s.mu.Unlock()
	if !started {
		return
	}
	ctxDone := s.cron.Stop()
	// Cron.Stop returns a context that closes when in-flight jobs
	// finish. Wait up to 5s so a slow Submit() doesn't strand the
	// process; in practice the Submit is non-blocking so this returns
	// immediately.
	select {
	case <-ctxDone.Done():
	case <-time.After(5 * time.Second):
		log.Printf("manager: scheduler.Stop timed out after 5s; abandoning in-flight schedule ticks")
	}
}

// registerLocked adds (or replaces) the cron entry for the given
// schedule. Caller must hold s.mu (write lock) OR call before Start
// — both paths are used: Update for live changes, Start for the
// startup loop.
//
// Wraps the underlying AddFunc call with a critical-section that
// also updates s.entries so a subsequent Remove can find the entry id.
func (s *scheduleStoreImpl) registerLocked(r scheduleState) error {
	if r.Paused {
		return nil
	}
	_, sched, err := resolveCronExpr(r.Cron)
	if err != nil {
		return err
	}
	name := r.Name
	id := s.cron.Schedule(sched, cron.FuncJob(func() {
		s.fire(name)
	}))
	// s.mu is held by the caller; safe to mutate the entries map.
	if prev, ok := s.entries[name]; ok {
		// Defensive: a stale entry id from a prior register would leak
		// — remove it so the engine isn't double-firing.
		s.cron.Remove(prev)
	}
	s.entries[name] = id
	// Compute NextRun from the parsed schedule against time.Now so the
	// list endpoint shows a fresh value immediately after Update.
	if i := s.findRowLocked(name); i >= 0 {
		s.rows[i].NextRun = sched.Next(time.Now()).UnixMilli()
	}
	return nil
}

// findRowLocked returns the index of the named row in s.rows, or -1.
// Caller must hold s.mu (any level).
func (s *scheduleStoreImpl) findRowLocked(name string) int {
	for i, r := range s.rows {
		if r.Name == name {
			return i
		}
	}
	return -1
}

// fire is the cron-engine callback that resolves a schedule into a
// concrete Job and submits it. Runs on a goroutine the cron engine
// owns; we do all the heavy lifting here so the engine's run loop
// stays responsive.
//
// Failure modes (each captured as a history row so the operator can
// see why a tick didn't produce a job):
//   - destination not found
//   - destination decryption failed (admin key rotation)
//   - destination ToSyncURI error
//   - JobManager.Submit error (currently always returns nil)
//
// Successful submissions hand off the live job id to the history; the
// terminal state (done/error/canceled) is reconciled into the row by
// the JobManager when SaveState fires.
func (s *scheduleStoreImpl) fire(name string) {
	now := time.Now().UnixMilli()
	s.mu.Lock()
	idx := s.findRowLocked(name)
	if idx < 0 {
		s.mu.Unlock()
		log.Printf("manager: scheduler fired for %q but no row found (race with delete)", name)
		return
	}
	r := s.rows[idx]
	s.rows[idx].LastRun = now
	// Recompute NextRun for the API/UI. Best-effort — if the cron
	// expression somehow stopped parsing (shouldn't, we validated on
	// upsert), leave the old value.
	if _, sched, err := resolveCronExpr(r.Cron); err == nil {
		s.rows[idx].NextRun = sched.Next(time.Now()).UnixMilli()
	}
	dests := s.dests
	mgr := s.mgr
	fuseRoot := s.fuseRoot
	onChange := s.onChange
	s.mu.Unlock()

	if r.Paused {
		// Cron entries for paused schedules are removed by Update; a
		// fire here means a tight race between Update and the engine's
		// run loop. Treat as a no-op so we don't accidentally submit a
		// just-paused schedule.
		return
	}

	source, destURI, env, errMsg := resolveScheduleSpec(r, dests, fuseRoot)
	if errMsg != "" {
		s.recordHistory(name, scheduleHistoryRow{
			State:      "error",
			StartedAt:  now,
			FinishedAt: time.Now().UnixMilli(),
			Error:      errMsg,
		})
		log.Printf("manager: schedule %q fire failed to resolve: %s", name, errMsg)
		if onChange != nil {
			onChange()
		}
		return
	}
	if mgr == nil {
		return
	}
	job, err := mgr.SubmitWithEnv(source, destURI, r.Options, 0, r.Source.Direction, name, env)
	if err != nil {
		s.recordHistory(name, scheduleHistoryRow{
			State:      "error",
			StartedAt:  now,
			FinishedAt: time.Now().UnixMilli(),
			Error:      err.Error(),
		})
		log.Printf("manager: schedule %q submit failed: %v", name, err)
		if onChange != nil {
			onChange()
		}
		return
	}
	s.recordHistory(name, scheduleHistoryRow{
		JobID:     job.ID,
		State:     "running",
		StartedAt: now,
	})
	if onChange != nil {
		onChange()
	}
}

// recordHistory appends a row to the schedule's history, trimming to
// RetainHistory (or defaultRetainHistory when zero). Newest-first.
func (s *scheduleStoreImpl) recordHistory(name string, row scheduleHistoryRow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := s.findRowLocked(name)
	if idx < 0 {
		return
	}
	cap := s.rows[idx].RetainHistory
	if cap <= 0 {
		cap = defaultRetainHistory
	}
	if cap > maxRetainHistory {
		cap = maxRetainHistory
	}
	// Prepend newest-first; trim tail to cap.
	hist := append([]scheduleHistoryRow{row}, s.rows[idx].History...)
	if len(hist) > cap {
		hist = hist[:cap]
	}
	s.rows[idx].History = hist
}

// resolveScheduleSpec turns a persisted schedule row into the concrete
// (source, destination URI, env) tuple Submit needs. Pure function —
// no side effects, no log spam, easy to unit-test.
//
// Returns errMsg (non-empty) on failure; the caller logs and records
// the failure in history. We use a string error for the history
// payload (it round-trips through JSON cleanly) rather than wrapping
// in an error type the persistence layer would have to special-case.
func resolveScheduleSpec(r scheduleState, dests *destinationStoreImpl, fuseRoot string) (source, destURI string, env []string, errMsg string) {
	if dests == nil {
		return "", "", nil, "destinations store unavailable (set JM_ADMIN_KEY)"
	}
	d, err := dests.getPlaintext(r.Destination.Name)
	if err != nil {
		return "", "", nil, fmt.Sprintf("resolve destination %q: %v", r.Destination.Name, err)
	}
	baseURI, env, err := d.ToSyncURI(r.Options.PreserveStructure)
	if err != nil {
		return "", "", nil, fmt.Sprintf("destination ToSyncURI: %v", err)
	}
	// Append the optional sub-path from DestinationRef.Path. We're
	// careful with trailing slashes — matchSlash already applied the
	// preserveStructure convention to baseURI, so we keep that suffix
	// intact and stitch the path in BEFORE the trailing slash.
	destURI = appendDestPath(baseURI, r.Destination.Path, r.Options.PreserveStructure)
	// Source is passed through raw; RunSync calls normalizeAnyURI on
	// it (sync.go line 138) using spec.FUSEMount, so /jfs/... sources
	// get rewritten to file:///<fuseRoot>/... at exec time. Code
	// reviewer thought fuseRoot needed to be used here — confirmed
	// the downstream normalization handles all directions including
	// Out, so this layer stays a pure resolution step.
	_ = fuseRoot
	source = r.Source.Path
	return source, destURI, env, ""
}

// appendDestPath splices a destination sub-path into a sync URI while
// preserving the trailing-slash convention applied by matchSlash. We
// can't just concatenate — the URI already has a final "/" (or doesn't)
// that juicefs sync FATALs on if it disagrees with the source.
func appendDestPath(uri, sub string, preserveStructure bool) string {
	sub = strings.TrimSpace(sub)
	if sub == "" {
		return uri
	}
	sub = strings.TrimPrefix(sub, "/")
	sub = strings.TrimSuffix(sub, "/")
	if sub == "" {
		return uri
	}
	base := strings.TrimSuffix(uri, "/")
	out := base + "/" + sub
	return matchSlash(out, preserveStructure)
}

// validateSchedule performs the syntactic + reference checks that all
// CRUD paths need. Cron expression is resolved here (so the API can
// surface a clear 400 instead of waiting until first-fire to discover
// the typo); destination existence is checked when a destinations
// store is provided.
func (s *scheduleStoreImpl) validateSchedule(sched Schedule) error {
	if err := validateScheduleName(sched.Name); err != nil {
		return err
	}
	if sched.Source.Path == "" {
		return errors.New("source.path is required")
	}
	if !strings.HasPrefix(sched.Source.Path, "/") {
		return errors.New("source.path must be an absolute path")
	}
	if sched.Destination.Name == "" {
		return errors.New("destination.name is required")
	}
	if _, _, err := resolveCronExpr(sched.Cron); err != nil {
		return err
	}
	if s.dests != nil && !s.dests.exists(sched.Destination.Name) {
		return fmt.Errorf("destination %q does not exist", sched.Destination.Name)
	}
	if sched.RetainHistory < 0 {
		return errors.New("retain_history must be >= 0")
	}
	if sched.RetainHistory > maxRetainHistory {
		return fmt.Errorf("retain_history %d exceeds cap %d", sched.RetainHistory, maxRetainHistory)
	}
	return nil
}

// upsert writes (creating or replacing) a schedule and re-registers it
// with the cron engine. The cron entry for an existing name is removed
// before re-adding so the new expression takes effect immediately.
func (s *scheduleStoreImpl) upsert(sched Schedule, allowReplace bool) error {
	if err := s.validateSchedule(sched); err != nil {
		return err
	}
	expr, parsed, err := resolveCronExpr(sched.Cron)
	if err != nil {
		return err
	}
	retain := sched.RetainHistory
	if retain <= 0 {
		retain = defaultRetainHistory
	}
	s.mu.Lock()
	defer func() {
		// Defer onChange notification until AFTER s.mu is released so
		// the saveState callback can re-enter snapshot() without
		// deadlock. We capture the callback here before the unlock.
		cb := s.onChange
		s.mu.Unlock()
		if cb != nil {
			cb()
		}
	}()
	idx := s.findRowLocked(sched.Name)
	if idx >= 0 && !allowReplace {
		return fmt.Errorf("schedule %q already exists", sched.Name)
	}
	row := scheduleState{
		Name:          sched.Name,
		Source:        sched.Source,
		Destination:   sched.Destination,
		Options:       sched.Options,
		Cron:          expr,
		Paused:        sched.Paused,
		RetainHistory: retain,
	}
	if idx >= 0 {
		// Preserve LastRun + History across updates so a re-save doesn't
		// blow away the run log.
		row.LastRun = s.rows[idx].LastRun
		row.History = s.rows[idx].History
		s.rows[idx] = row
	} else {
		s.rows = append(s.rows, row)
	}
	// Recompute NextRun off the parsed schedule.
	if i := s.findRowLocked(sched.Name); i >= 0 {
		s.rows[i].NextRun = parsed.Next(time.Now()).UnixMilli()
	}
	// Re-register against the cron engine. Remove the old entry first
	// (if any) so a re-add doesn't double-fire. Paused schedules are
	// removed from the engine but kept in s.rows.
	if prev, ok := s.entries[sched.Name]; ok {
		s.cron.Remove(prev)
		delete(s.entries, sched.Name)
	}
	if s.started && !sched.Paused {
		// Re-register inline; ignore error because validateSchedule
		// already proved the cron expression parses.
		if i := s.findRowLocked(sched.Name); i >= 0 {
			_ = s.registerLocked(s.rows[i])
		}
	}
	return nil
}

// remove deletes a schedule by name. Idempotent — returns nil when the
// name doesn't exist.
func (s *scheduleStoreImpl) remove(name string) {
	s.mu.Lock()
	defer func() {
		cb := s.onChange
		s.mu.Unlock()
		if cb != nil {
			cb()
		}
	}()
	idx := s.findRowLocked(name)
	if idx < 0 {
		return
	}
	if prev, ok := s.entries[name]; ok {
		s.cron.Remove(prev)
		delete(s.entries, name)
	}
	s.rows = append(s.rows[:idx], s.rows[idx+1:]...)
}

// list returns a snapshot of every schedule as the API-boundary form.
// History is included so the Backups tab can render last-N-runs
// without a follow-up request — kept compact via RetainHistory.
func (s *scheduleStoreImpl) list() []scheduleListItem {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]scheduleListItem, 0, len(s.rows))
	for _, r := range s.rows {
		out = append(out, scheduleListItem{
			Schedule: Schedule{
				Name:          r.Name,
				Source:        r.Source,
				Destination:   r.Destination,
				Options:       r.Options,
				Cron:          r.Cron,
				Paused:        r.Paused,
				RetainHistory: r.RetainHistory,
				LastRun:       r.LastRun,
				NextRun:       r.NextRun,
			},
			History: append([]scheduleHistoryRow(nil), r.History...),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// scheduleListItem is the API response shape — the Schedule plus its
// history. Separated from Schedule so we can later trim the list
// endpoint to omit history (for very large run logs) without changing
// the per-item GET response shape.
type scheduleListItem struct {
	Schedule
	History []scheduleHistoryRow `json:"history,omitempty"`
}

// runNow fires a schedule immediately, regardless of its next-tick
// time. Returns the freshly-submitted Job's id (or an error from the
// resolve/submit path). Used by POST /api/schedules/{name}/run.
func (s *scheduleStoreImpl) runNow(name string) (string, error) {
	s.mu.RLock()
	idx := s.findRowLocked(name)
	if idx < 0 {
		s.mu.RUnlock()
		return "", errScheduleNotFound
	}
	r := s.rows[idx]
	dests := s.dests
	mgr := s.mgr
	fuseRoot := s.fuseRoot
	s.mu.RUnlock()

	source, destURI, env, errMsg := resolveScheduleSpec(r, dests, fuseRoot)
	if errMsg != "" {
		return "", errors.New(errMsg)
	}
	if mgr == nil {
		return "", errors.New("job manager unavailable")
	}
	job, err := mgr.SubmitWithEnv(source, destURI, r.Options, 0, r.Source.Direction, name, env)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	idx = s.findRowLocked(name)
	if idx >= 0 {
		s.rows[idx].LastRun = time.Now().UnixMilli()
	}
	cb := s.onChange
	s.mu.Unlock()
	s.recordHistory(name, scheduleHistoryRow{
		JobID:     job.ID,
		State:     "running",
		StartedAt: time.Now().UnixMilli(),
	})
	if cb != nil {
		cb()
	}
	return job.ID, nil
}

var errScheduleNotFound = errors.New("schedule not found")

// ===========================================================================
// HTTP handlers
// ===========================================================================

// handleSchedules dispatches GET (list) and POST (create) on
// /api/schedules.
func (a *API) handleSchedules(w http.ResponseWriter, r *http.Request) {
	if a.schedules == nil {
		http.Error(w, "schedules subsystem not initialized", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{
			"schedules": a.schedules.list(),
			"presets":   cronPresets,
		})
	case http.MethodPost:
		a.handleScheduleCreate(w, r)
	default:
		http.Error(w, "GET or POST only", http.StatusMethodNotAllowed)
	}
}

// handleScheduleItem dispatches PUT (update), DELETE (remove), and
// POST .../run on /api/schedules/{name}.
func (a *API) handleScheduleItem(w http.ResponseWriter, r *http.Request) {
	if a.schedules == nil {
		http.Error(w, "schedules subsystem not initialized", http.StatusServiceUnavailable)
		return
	}
	tail := strings.TrimPrefix(r.URL.Path, a.prefix+"/api/schedules/")
	if tail == r.URL.Path {
		tail = strings.TrimPrefix(r.URL.Path, "/api/schedules/")
	}
	if tail == "" {
		http.Error(w, "schedule name required", http.StatusBadRequest)
		return
	}
	parts := strings.SplitN(tail, "/", 2)
	name := parts[0]
	sub := ""
	if len(parts) > 1 {
		sub = parts[1]
	}
	if err := validateScheduleName(name); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch {
	case sub == "run" && r.Method == http.MethodPost:
		a.handleScheduleRunNow(w, r, name)
	case sub == "" && r.Method == http.MethodPut:
		a.handleScheduleUpdate(w, r, name)
	case sub == "" && r.Method == http.MethodDelete:
		a.handleScheduleDelete(w, r, name)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (a *API) handleScheduleCreate(w http.ResponseWriter, r *http.Request) {
	var s Schedule
	if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	s.Name = strings.TrimSpace(s.Name)
	if err := a.schedules.upsert(s, false); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"name": s.Name})
}

func (a *API) handleScheduleUpdate(w http.ResponseWriter, r *http.Request, name string) {
	var s Schedule
	if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if s.Name != "" && s.Name != name {
		http.Error(w, "name in URL and body must match (or omit body.name)", http.StatusBadRequest)
		return
	}
	s.Name = name
	if err := a.schedules.upsert(s, true); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"name": s.Name})
}

func (a *API) handleScheduleDelete(w http.ResponseWriter, r *http.Request, name string) {
	a.schedules.remove(name)
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) handleScheduleRunNow(w http.ResponseWriter, r *http.Request, name string) {
	jobID, err := a.schedules.runNow(name)
	if err != nil {
		if errors.Is(err, errScheduleNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"job_id": jobID, "schedule": name})
}

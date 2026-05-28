// Package manager implements the JuiceMount control plane. SLICE 0
// of the manager roadmap covers the migrator's existing functionality
// — copy-into-JuiceFS via `juicefs sync` exposed as an HTTP API with
// SSE progress. Subsequent slices add overview, trash, destinations,
// backups (schedules), maintenance, and settings tabs.
//
// Consumed by juicemount-server which registers the routes on its
// existing metrics listener — single process, no cross-network plumbing.
package manager

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// JobState is the public state of a sync job exposed via /api/jobs/...
type JobState string

const (
	JobPending  JobState = "pending"
	JobRunning  JobState = "running"
	JobDone     JobState = "done"
	JobError    JobState = "error"
	JobCanceled JobState = "canceled"
)

// ProgressEvent is one tick of a running job's progress. Emitted via
// SSE to subscribers and stored as the job's "last known state" so a
// late subscriber can fetch a snapshot via /api/jobs/{id}.
type ProgressEvent struct {
	Files     int64   `json:"files"`      // copied file count
	Bytes     int64   `json:"bytes"`      // copied byte count
	Errors    int64   `json:"errors"`     // failed entries
	Current   string  `json:"current"`    // last-seen current path
	ETASec    int64   `json:"eta_sec"`    // juicefs's ETA in seconds (-1 = unknown)
	BPS       float64 `json:"bps"`        // throughput, bytes per second
	UpdatedAt int64   `json:"updated_at"` // unix-ms
}

// Job tracks the lifecycle of one sync invocation.
type Job struct {
	ID          string        `json:"id"`
	Source      string        `json:"source"`
	Destination string        `json:"destination"`
	Options     SyncOptions   `json:"options"`
	State       JobState      `json:"state"`
	CreatedAt   int64         `json:"created_at"` // unix-ms
	StartedAt   int64         `json:"started_at"`
	FinishedAt  int64         `json:"finished_at"`
	// TotalBytes is the pre-computed source size from the UI's preview
	// pane, passed through on job creation. Used by the frontend to
	// render a real % progress bar (vs an indeterminate placeholder).
	// 0 means "unknown" — frontend falls back to indeterminate display.
	TotalBytes int64 `json:"total_bytes"`
	// Direction the job was created in (in/out/between). Persisted so
	// resume after a restart re-submits with the correct direction
	// rather than relying on path-based inference (which breaks for
	// Between once slice-4 lands). Empty/missing on records written
	// before slice-1 — caller treats empty as DirectionIn.
	Direction Direction     `json:"direction,omitempty"`
	Last      ProgressEvent `json:"last"`
	Error     string        `json:"error,omitempty"`

	// runtime-only — not serialized
	cancel    context.CancelFunc `json:"-"`
	listeners []chan ProgressEvent
	mu        sync.Mutex
}

// SyncFunc is the signature of a "run one sync" implementation. The
// default is RunSync (which invokes `juicefs sync` via exec). Tests
// override this to mock the subprocess.
type SyncFunc func(ctx context.Context, juicefsBin string, spec RunSyncSpec, source, destination string, opts SyncOptions, progress chan<- ProgressEvent) error

// JobManager owns all jobs in this process. Single-worker for v1.
type JobManager struct {
	juicefsBin string
	spec       RunSyncSpec // destination-resolution config

	// runner is the underlying sync implementation. Defaults to
	// RunSync; set via SetRunner() for tests.
	runner SyncFunc

	// stateFileLoaded guards SetStateFile against repeated loads.
	// Without this, a second SetStateFile call (defensive or
	// accidental) would re-read the on-disk snapshot and clobber any
	// jobs submitted since the first call. Set once in SetStateFile.
	stateFileLoaded bool

	// stateFile is an optional JSON file the manager loads on startup
	// and writes after every job state transition. Bind-mount the
	// containing dir to make job history survive container restart.
	// Empty = no persistence (jobs vanish on process exit).
	stateFile string

	mu     sync.RWMutex
	jobs   map[string]*Job
	order  []string
	active *Job
}

// persistedState is what we serialize to disk. Mirrors the in-memory
// shape but only the JSON-tagged fields of Job (no runtime mutex /
// listener slices). order preserves submission order across restarts.
type persistedState struct {
	Jobs  map[string]*Job `json:"jobs"`
	Order []string        `json:"order"`
}

// NewJobManager constructs a JobManager. juicefsBin is the path to
// the juicefs CLI (or just "juicefs" for PATH lookup). spec controls
// destination URL resolution — see RunSyncSpec doc.
func NewJobManager(juicefsBin string, spec RunSyncSpec) *JobManager {
	return &JobManager{
		juicefsBin: juicefsBin,
		spec:       spec,
		runner:     RunSync,
		jobs:       make(map[string]*Job),
	}
}

// legacyStateFile is the SLICE 0 fallback path: if the configured
// state file doesn't exist yet, we also check the pre-rename location
// (/var/lib/migrator/jobs.json from the migrator era) so a one-release
// in-place upgrade carries job history forward without manual copying.
// Going forward, writes always go to the new path (state file as
// configured, typically /var/lib/manager/state.json). The fallback
// path is read-only; we never write back to it.
const legacyStateFile = "/var/lib/migrator/jobs.json"

// SetStateFile enables JSON persistence at the given path and loads
// any existing history immediately. Idempotent — repeated calls after
// the first are no-ops (guarded by m.stateFileLoaded) so a defensive
// or accidental second call cannot clobber the in-memory state with a
// stale on-disk snapshot. Empty path is a no-op — useful for the
// default "ephemeral" mode.
//
// SLICE 0 backward-compat: if the configured path doesn't exist but
// the legacy migrator-era path (/var/lib/migrator/jobs.json) does,
// we load from the legacy path and write going forward to the new
// path. Logs a one-shot migration line so the operator sees it
// happened.
//
// Jobs that were Running or Pending at the moment the previous process
// died are marked as JobError on load with the reason "interrupted by
// container restart" — they're not auto-resumed (the user can hit the
// Resume button in the UI if they want to).
func (m *JobManager) SetStateFile(path string) {
	if path == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stateFileLoaded {
		// Already loaded once — refuse to re-read from disk (would
		// clobber any jobs submitted since the first call). Allow the
		// path to be updated for the write side only if it matches
		// what we already set; ignore mismatches.
		return
	}
	m.stateFileLoaded = true
	m.stateFile = path
	data, err := os.ReadFile(path)
	if err != nil && os.IsNotExist(err) && path != legacyStateFile {
		// Fall back to the pre-rename location for one-release compat.
		legacyData, legacyErr := os.ReadFile(legacyStateFile)
		if legacyErr == nil {
			log.Printf("manager: state file %s missing; migrating job history from legacy path %s (will write to new path going forward)", path, legacyStateFile)
			data = legacyData
			err = nil
		}
	}
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("manager: read state file %s: %v", path, err)
		}
		return
	}
	var s persistedState
	if err := json.Unmarshal(data, &s); err != nil {
		log.Printf("manager: parse state file %s: %v (starting fresh)", path, err)
		return
	}
	for _, j := range s.Jobs {
		if j == nil {
			continue
		}
		// Crash recovery: any job that was running/pending pre-restart
		// is orphaned. Promote to error so the UI surfaces it (with
		// the Resume button enabled).
		if j.State == JobRunning || j.State == JobPending {
			j.State = JobError
			if j.Error == "" {
				j.Error = "interrupted by container restart"
			}
			if j.FinishedAt == 0 {
				j.FinishedAt = time.Now().UnixMilli()
			}
		}
		m.jobs[j.ID] = j
	}
	m.order = s.Order
	log.Printf("manager: loaded %d jobs from %s", len(m.jobs), path)
}

// saveStateLocked atomically writes the current jobs map + order to
// stateFile. Caller must hold m.mu (any level). Best-effort — logs
// errors but never blocks the caller; persistence is convenience, not
// correctness-critical.
func (m *JobManager) saveStateLocked() {
	if m.stateFile == "" {
		return
	}
	// Snapshot under read of each job's mutex so we don't catch a
	// half-written field. The outer m.mu (caller-held) already
	// excludes concurrent map writes.
	out := persistedState{
		Jobs:  make(map[string]*Job, len(m.jobs)),
		Order: append([]string(nil), m.order...),
	}
	for id, j := range m.jobs {
		j.mu.Lock()
		// Shallow copy by value so we don't serialize the mutex.
		snap := Job{
			ID:          j.ID,
			Source:      j.Source,
			Destination: j.Destination,
			Options:     j.Options,
			State:       j.State,
			CreatedAt:   j.CreatedAt,
			StartedAt:   j.StartedAt,
			FinishedAt:  j.FinishedAt,
			TotalBytes:  j.TotalBytes,
			Direction:   j.Direction,
			Last:        j.Last,
			Error:       j.Error,
		}
		j.mu.Unlock()
		out.Jobs[id] = &snap
	}
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		log.Printf("manager: marshal state: %v", err)
		return
	}
	tmp := m.stateFile + ".tmp"
	if err := os.MkdirAll(filepath.Dir(m.stateFile), 0o755); err != nil {
		log.Printf("manager: mkdir state dir: %v", err)
		return
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		log.Printf("manager: write state tmp: %v", err)
		return
	}
	if err := os.Rename(tmp, m.stateFile); err != nil {
		log.Printf("manager: rename state: %v", err)
	}
}

// SetRunner swaps the underlying sync implementation. Test-only.
func (m *JobManager) SetRunner(fn SyncFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runner = fn
}

// Submit queues a job. Returns the assigned ID. If the manager has
// no active job, kicks it off immediately on a background goroutine.
// totalBytes is the pre-computed source size (from the UI's preview
// scan); pass 0 for unknown.
func (m *JobManager) Submit(source, destination string, opts SyncOptions, totalBytes int64, direction Direction) (*Job, error) {
	if direction == "" {
		direction = DirectionIn
	}
	id := newJobID()
	j := &Job{
		ID:          id,
		Source:      source,
		Destination: destination,
		Options:     opts,
		State:       JobPending,
		CreatedAt:   time.Now().UnixMilli(),
		TotalBytes:  totalBytes,
		Direction:   direction,
	}
	m.mu.Lock()
	m.jobs[id] = j
	m.order = append(m.order, id)
	canStart := m.active == nil
	if canStart {
		m.active = j
	}
	m.saveStateLocked()
	m.mu.Unlock()
	if canStart {
		go m.run(j)
	}
	return j, nil
}

// Get returns a snapshot of a job by ID, or nil if not found.
// The returned *Job has its own mutex; use GetState/GetLast to read
// fields that may be mutated by the runner goroutine.
func (m *JobManager) Get(id string) *Job {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.jobs[id]
}

// GetState returns the current state of a job, acquired under the
// job's own lock. Safe to call from any goroutine.
func (j *Job) GetState() JobState {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.State
}

// GetSnapshot returns a copy of the job's current externally-visible
// state for safe serialization. Holds j.mu briefly.
func (j *Job) GetSnapshot() Job {
	j.mu.Lock()
	defer j.mu.Unlock()
	// Copy by value; listeners + cancel + mu are zero-valued in the
	// returned struct.
	return Job{
		ID:          j.ID,
		Source:      j.Source,
		Destination: j.Destination,
		Options:     j.Options,
		State:       j.State,
		CreatedAt:   j.CreatedAt,
		StartedAt:   j.StartedAt,
		FinishedAt:  j.FinishedAt,
		Last:        j.Last,
		Error:       j.Error,
	}
}

// List returns all jobs in insertion order.
func (m *JobManager) List() []*Job {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Job, 0, len(m.order))
	for _, id := range m.order {
		if j, ok := m.jobs[id]; ok {
			out = append(out, j)
		}
	}
	return out
}

// Cancel stops a running or pending job by ID. Returns true if the
// job existed and was cancellable.
func (m *JobManager) Cancel(id string) bool {
	m.mu.Lock()
	j, ok := m.jobs[id]
	m.mu.Unlock()
	if !ok {
		return false
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	switch j.State {
	case JobPending, JobRunning:
		if j.cancel != nil {
			j.cancel()
		}
		j.State = JobCanceled
		j.FinishedAt = time.Now().UnixMilli()
		j.notifyListenersLocked(ProgressEvent{
			Files:     j.Last.Files,
			Bytes:     j.Last.Bytes,
			Errors:    j.Last.Errors,
			Current:   j.Last.Current,
			ETASec:    -1,
			UpdatedAt: j.FinishedAt,
		})
		j.closeListenersLocked()
		return true
	default:
		return false
	}
}

// Subscribe returns a channel that receives every subsequent
// ProgressEvent for the job, and a cleanup func the caller must
// invoke when done. Buffered with a small backlog so a slow consumer
// doesn't block the runner; events drop on overflow.
func (m *JobManager) Subscribe(id string) (<-chan ProgressEvent, func(), bool) {
	j := m.Get(id)
	if j == nil {
		return nil, nil, false
	}
	ch := make(chan ProgressEvent, 32)
	j.mu.Lock()
	j.listeners = append(j.listeners, ch)
	last := j.Last
	state := j.State
	j.mu.Unlock()

	// Emit the current snapshot immediately so the subscriber gets
	// the latest state without waiting for the next tick.
	if last.UpdatedAt > 0 {
		ch <- last
	}
	if state == JobDone || state == JobError || state == JobCanceled {
		close(ch)
	}

	cleanup := func() {
		j.mu.Lock()
		defer j.mu.Unlock()
		for i, c := range j.listeners {
			if c == ch {
				j.listeners = append(j.listeners[:i], j.listeners[i+1:]...)
				break
			}
		}
	}
	return ch, cleanup, true
}

// StopAll cancels every active and pending job. Called on shutdown.
func (m *JobManager) StopAll() {
	m.mu.RLock()
	ids := make([]string, 0, len(m.jobs))
	for id := range m.jobs {
		ids = append(ids, id)
	}
	m.mu.RUnlock()
	for _, id := range ids {
		m.Cancel(id)
	}
}

// run executes a sync job and updates state as it progresses.
// Called on a goroutine; blocks until the job is in a terminal state.
func (m *JobManager) run(j *Job) {
	ctx, cancel := context.WithCancel(context.Background())
	j.mu.Lock()
	j.cancel = cancel
	j.State = JobRunning
	j.StartedAt = time.Now().UnixMilli()
	j.mu.Unlock()
	m.mu.Lock()
	m.saveStateLocked()
	m.mu.Unlock()

	// Wire progress callbacks into the job's listener fan-out. Cap 64
	// because two concurrent producers (parseSyncProgress + the
	// pollJuicefsMetrics goroutine) both emit here, and bursty stretches
	// — startup, large-file boundaries — would saturate a smaller buffer
	// and silently drop events via the non-blocking sends both producers use.
	progress := make(chan ProgressEvent, 64)
	done := make(chan error, 1)

	m.mu.RLock()
	runner := m.runner
	spec := m.spec
	m.mu.RUnlock()
	go func() {
		done <- runner(ctx, m.juicefsBin, spec, j.Source, j.Destination, j.Options, progress)
		close(progress)
	}()

	for ev := range progress {
		j.mu.Lock()
		// Merge monotonically rather than overwrite. Two concurrent
		// producers feed `progress`: the regex stderr parser and the
		// Prometheus metrics poller. Each may emit events that carry
		// only a SUBSET of the counters (e.g. the regex parser's final
		// "copied 14589" line has Files=14589 but Bytes=0 because the
		// line has no byte unit). Without this merge, that final event
		// clobbers the accurate Bytes value the metrics poller already
		// stored. Files/Bytes/Errors are monotonic in juicefs so max
		// is always the right choice; Current/ETASec/UpdatedAt take
		// the latest value.
		j.Last = mergeProgress(j.Last, ev)
		j.notifyListenersLocked(j.Last)
		j.mu.Unlock()
	}

	err := <-done

	j.mu.Lock()
	j.FinishedAt = time.Now().UnixMilli()
	switch {
	case ctx.Err() != nil:
		j.State = JobCanceled
	case err != nil:
		j.State = JobError
		j.Error = err.Error()
	default:
		j.State = JobDone
	}
	j.notifyListenersLocked(j.Last)
	j.closeListenersLocked()
	j.mu.Unlock()

	// Dequeue and kick the next pending job if any.
	m.mu.Lock()
	m.active = nil
	var nextToRun *Job
	for _, id := range m.order {
		next := m.jobs[id]
		if next.State == JobPending {
			m.active = next
			nextToRun = next
			break
		}
	}
	m.saveStateLocked()
	m.mu.Unlock()
	if nextToRun != nil {
		go m.run(nextToRun)
	}
}

// mergeProgress combines a sparsely-populated incoming event with the
// already-stored last-known state. juicefs counters are monotonic so
// max is correct for Files/Bytes/Errors; Current/ETASec/UpdatedAt
// always take the latest non-zero value (Current "" means "unchanged",
// not "cleared"). See the run() loop comment for the producer
// asymmetry this guards against.
func mergeProgress(prev, next ProgressEvent) ProgressEvent {
	out := prev
	if next.Files > out.Files {
		out.Files = next.Files
	}
	if next.Bytes > out.Bytes {
		out.Bytes = next.Bytes
	}
	if next.Errors > out.Errors {
		out.Errors = next.Errors
	}
	if next.Current != "" {
		out.Current = next.Current
	}
	if next.ETASec != 0 {
		out.ETASec = next.ETASec
	}
	if next.BPS != 0 {
		out.BPS = next.BPS
	}
	if next.UpdatedAt > out.UpdatedAt {
		out.UpdatedAt = next.UpdatedAt
	}
	return out
}

// notifyListenersLocked broadcasts an event to all subscribers.
// Drops on a full buffer rather than blocking the runner.
// Caller MUST hold j.mu.
func (j *Job) notifyListenersLocked(ev ProgressEvent) {
	for _, c := range j.listeners {
		select {
		case c <- ev:
		default:
			// listener too slow; drop this event
		}
	}
}

// closeListenersLocked closes every subscriber channel. Called at
// job termination. Caller MUST hold j.mu.
func (j *Job) closeListenersLocked() {
	for _, c := range j.listeners {
		close(c)
	}
	j.listeners = nil
}

// newJobID returns a short, sortable, URL-safe ID. Format:
// j<unix-ms>-<rand4>. Insertion order naturally sorts by time.
func newJobID() string {
	return "j" + time.Now().UTC().Format("20060102T150405.000") + "-" + randHex(4)
}

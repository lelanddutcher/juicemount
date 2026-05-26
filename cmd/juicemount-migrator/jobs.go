//go:build migrator_wip
// +build migrator_wip

package main

import (
	"context"
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
	State       JobState      `json:"state"`
	CreatedAt   int64         `json:"created_at"` // unix-ms
	StartedAt   int64         `json:"started_at"`
	FinishedAt  int64         `json:"finished_at"`
	Last        ProgressEvent `json:"last"`
	Error       string        `json:"error,omitempty"`

	// runtime-only — not serialized
	cancel    context.CancelFunc `json:"-"`
	listeners []chan ProgressEvent
	mu        sync.Mutex
}

// SyncFunc is the signature of a "run one sync" implementation. The
// default is RunSync (which invokes `juicefs sync` via exec). Tests
// override this to mock the subprocess.
type SyncFunc func(ctx context.Context, juicefsBin, metaURL, source, destination string, progress chan<- ProgressEvent) error

// JobManager owns all jobs in this process. Single-worker for v1
// (sync runs one at a time); a tiny queue could be added later if
// concurrent migrations turn out to be valuable.
type JobManager struct {
	juicefsBin string
	metaURL    string

	// runner is the underlying sync implementation. Defaults to
	// RunSync; set via SetRunner() for tests that need deterministic
	// long-running / canceled / errored behavior.
	runner SyncFunc

	mu     sync.RWMutex
	jobs   map[string]*Job
	order  []string // insertion order for stable listing
	active *Job
}

func NewJobManager(juicefsBin, metaURL string) *JobManager {
	return &JobManager{
		juicefsBin: juicefsBin,
		metaURL:    metaURL,
		runner:     RunSync,
		jobs:       make(map[string]*Job),
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
func (m *JobManager) Submit(source, destination string) (*Job, error) {
	id := newJobID()
	j := &Job{
		ID:          id,
		Source:      source,
		Destination: destination,
		State:       JobPending,
		CreatedAt:   time.Now().UnixMilli(),
	}
	m.mu.Lock()
	m.jobs[id] = j
	m.order = append(m.order, id)
	canStart := m.active == nil
	if canStart {
		m.active = j
	}
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

	// Wire progress callbacks into the job's listener fan-out.
	progress := make(chan ProgressEvent, 8)
	done := make(chan error, 1)

	m.mu.RLock()
	runner := m.runner
	m.mu.RUnlock()
	go func() {
		done <- runner(ctx, m.juicefsBin, m.metaURL, j.Source, j.Destination, progress)
		close(progress)
	}()

	for ev := range progress {
		j.mu.Lock()
		j.Last = ev
		j.notifyListenersLocked(ev)
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
	for _, id := range m.order {
		next := m.jobs[id]
		if next.State == JobPending {
			m.active = next
			go m.run(next)
			break
		}
	}
	m.mu.Unlock()
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

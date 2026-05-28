package manager

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Mock SyncFuncs for deterministic test paths.
func runnerForeverUntilCanceled(ctx context.Context, _ string, _ RunSyncSpec, _, _ string, _ SyncOptions, _ chan<- ProgressEvent) error {
	<-ctx.Done()
	return context.Canceled
}

func runnerErrorImmediately(_ context.Context, _ string, _ RunSyncSpec, _, _ string, _ SyncOptions, _ chan<- ProgressEvent) error {
	return errors.New("simulated sync failure")
}

func runnerSucceedImmediately(_ context.Context, _ string, _ RunSyncSpec, _, _ string, _ SyncOptions, _ chan<- ProgressEvent) error {
	return nil
}

func TestJobIDFormat(t *testing.T) {
	id := newJobID()
	if len(id) < 20 {
		t.Fatalf("job ID too short: %q", id)
	}
	if id[0] != 'j' {
		t.Errorf("job ID should start with 'j', got %q", id[0:1])
	}
	// Two consecutive IDs should differ (monotonic ms + random suffix)
	id2 := newJobID()
	if id == id2 {
		t.Errorf("two consecutive newJobID() returned the same value: %q", id)
	}
}

func TestJobManagerSubmitListGet(t *testing.T) {
	m := NewJobManager("/dev/null", RunSyncSpec{Mode: ModeEmbedded, FUSEMount: "/mnt/juicefs"})
	m.SetRunner(runnerForeverUntilCanceled)
	defer m.StopAll()

	// Submit one job — should kick off immediately (active==nil).
	j1, err := m.Submit("/tmp/src1", "/jfs/dst1", DefaultSyncOptions(), 0, "")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if j1.ID == "" {
		t.Errorf("job ID is empty")
	}
	if j1.Source != "/tmp/src1" || j1.Destination != "/jfs/dst1" {
		t.Errorf("source/dest not preserved: %+v", j1)
	}

	// Get by ID round-trip.
	got := m.Get(j1.ID)
	if got == nil || got.ID != j1.ID {
		t.Errorf("Get round-trip failed: %v", got)
	}

	// Submit a second job — should queue (active is the first).
	j2, _ := m.Submit("/tmp/src2", "/jfs/dst2", DefaultSyncOptions(), 0, "")
	if j2.ID == j1.ID {
		t.Errorf("two jobs got same ID")
	}

	// List should return both in insertion order.
	jobs := m.List()
	if len(jobs) != 2 {
		t.Fatalf("List returned %d jobs, want 2", len(jobs))
	}
	if jobs[0].ID != j1.ID || jobs[1].ID != j2.ID {
		t.Errorf("List order broken: got %s,%s want %s,%s",
			jobs[0].ID, jobs[1].ID, j1.ID, j2.ID)
	}
}

func TestJobManagerCancel(t *testing.T) {
	m := NewJobManager("/dev/null", RunSyncSpec{Mode: ModeEmbedded, FUSEMount: "/mnt/juicefs"})
	m.SetRunner(runnerForeverUntilCanceled)

	j, _ := m.Submit("/tmp/x", "/jfs/y", DefaultSyncOptions(), 0, "")
	// Wait for the job to actually transition to Running.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if m.Get(j.ID).GetState() == JobRunning {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if m.Get(j.ID).GetState() != JobRunning {
		t.Fatalf("job did not enter Running state, got %s", m.Get(j.ID).GetState())
	}

	if !m.Cancel(j.ID) {
		t.Fatalf("Cancel returned false for running job")
	}

	// Job state should be Canceled within a short window.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m.Get(j.ID).GetState() == JobCanceled {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("job did not reach Canceled state in 2s, last state=%s",
		m.Get(j.ID).GetState())
}

func TestJobManagerErrorPropagates(t *testing.T) {
	m := NewJobManager("/dev/null", RunSyncSpec{Mode: ModeEmbedded, FUSEMount: "/mnt/juicefs"})
	m.SetRunner(runnerErrorImmediately)

	j, _ := m.Submit("/tmp/src", "/jfs/dst", DefaultSyncOptions(), 0, "")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m.Get(j.ID).GetState() == JobError {
			snap := m.Get(j.ID).GetSnapshot()
			if snap.Error == "" {
				t.Errorf("job error field empty after errored runner")
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("job did not reach Error state in 2s, got %s", m.Get(j.ID).GetState())
}

func TestJobManagerSuccessReachesDone(t *testing.T) {
	m := NewJobManager("/dev/null", RunSyncSpec{Mode: ModeEmbedded, FUSEMount: "/mnt/juicefs"})
	m.SetRunner(runnerSucceedImmediately)

	j, _ := m.Submit("/tmp/src", "/jfs/dst", DefaultSyncOptions(), 0, "")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m.Get(j.ID).GetState() == JobDone {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("job did not reach Done state in 2s, got %s", m.Get(j.ID).GetState())
}

func TestJobManagerSubscribe(t *testing.T) {
	m := NewJobManager("/dev/null", RunSyncSpec{Mode: ModeEmbedded, FUSEMount: "/mnt/juicefs"})
	m.SetRunner(runnerSucceedImmediately)

	j, _ := m.Submit("/tmp/src", "/jfs/dst", DefaultSyncOptions(), 0, "")
	ch, cleanup, ok := m.Subscribe(j.ID)
	if !ok {
		t.Fatalf("Subscribe returned ok=false")
	}
	defer cleanup()

	// Drain the channel until close. Time-bounded.
	deadline := time.After(3 * time.Second)
	got := 0
	for {
		select {
		case _, more := <-ch:
			if !more {
				if got == 0 {
					t.Errorf("subscribe channel closed without emitting any events")
				}
				return
			}
			got++
		case <-deadline:
			t.Errorf("subscribe channel did not close in 3s")
			return
		}
	}
}

func TestJobManagerStopAll(t *testing.T) {
	m := NewJobManager("/dev/null", RunSyncSpec{Mode: ModeEmbedded, FUSEMount: "/mnt/juicefs"})
	m.SetRunner(runnerForeverUntilCanceled)

	_, _ = m.Submit("/tmp/a", "/jfs/a", DefaultSyncOptions(), 0, "")
	_, _ = m.Submit("/tmp/b", "/jfs/b", DefaultSyncOptions(), 0, "")
	time.Sleep(100 * time.Millisecond)

	m.StopAll()

	// All jobs should be Canceled within a short window.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		all := m.List()
		allCanceled := true
		for _, j := range all {
			s := j.GetState()
			if s != JobCanceled && s != JobDone && s != JobError {
				allCanceled = false
				break
			}
		}
		if allCanceled {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, j := range m.List() {
		t.Errorf("job %s in state %s after StopAll", j.ID, j.GetState())
	}
}

func TestJobManagerGetMissing(t *testing.T) {
	m := NewJobManager("/dev/null", RunSyncSpec{Mode: ModeEmbedded, FUSEMount: "/mnt/juicefs"})
	if got := m.Get("nope"); got != nil {
		t.Errorf("Get for missing ID should return nil, got %v", got)
	}
	if m.Cancel("nope") {
		t.Errorf("Cancel for missing ID should return false")
	}
	if _, _, ok := m.Subscribe("nope"); ok {
		t.Errorf("Subscribe for missing ID should return ok=false")
	}
}

func TestSetStateFileIdempotent(t *testing.T) {
	// Guards against the rename-era regression: a defensive second
	// SetStateFile call MUST NOT re-read the on-disk snapshot, or
	// jobs submitted between the two calls would be clobbered by
	// the stale serialized state.
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte(`{"schema_version":1,"jobs":{"j1":{"id":"j1","source":"/s","destination":"/d","state":"done","created_at":1}},"order":["j1"]}`), 0o600); err != nil {
		t.Fatalf("write seed state: %v", err)
	}

	m := NewJobManager("/dev/null", RunSyncSpec{Mode: ModeEmbedded, FUSEMount: "/mnt/juicefs"})
	m.SetStateFile(path)
	if got := m.Get("j1"); got == nil {
		t.Fatalf("first SetStateFile didn't load seed job")
	}

	// Add a NEW job in-memory that doesn't exist on disk yet.
	live, err := m.Submit("/new-src", "/new-dst", DefaultSyncOptions(), 0, "")
	if err != nil {
		t.Fatalf("Submit live job: %v", err)
	}

	// Truncate the state file on disk (simulate stale snapshot) then
	// call SetStateFile again. If the guard is broken, the live job
	// would disappear from m.jobs.
	if err := os.WriteFile(path, []byte(`{"schema_version":1,"jobs":{},"order":[]}`), 0o600); err != nil {
		t.Fatalf("truncate state: %v", err)
	}
	m.SetStateFile(path)

	if got := m.Get(live.ID); got == nil {
		t.Errorf("second SetStateFile clobbered live job %s (idempotency guard regressed)", live.ID)
	}
	if got := m.Get("j1"); got == nil {
		t.Errorf("second SetStateFile dropped originally-loaded job j1")
	}
}

func TestMergeProgress(t *testing.T) {
	// Locks in the merge semantic: monotonic max for counters; latest
	// non-zero for Current/ETA/BPS/UpdatedAt. Prevents the regex
	// parser's Bytes=0 final flush from clobbering the metrics
	// poller's accurate Bytes total at end-of-job.
	prev := ProgressEvent{Files: 1000, Bytes: 500_000_000, Current: "old.mov", ETASec: 60, UpdatedAt: 100}

	t.Run("regex-final-flush does not clobber metrics bytes", func(t *testing.T) {
		flush := ProgressEvent{Files: 1234, Bytes: 0, UpdatedAt: 200}
		got := mergeProgress(prev, flush)
		if got.Files != 1234 {
			t.Errorf("Files: got %d want 1234", got.Files)
		}
		if got.Bytes != 500_000_000 {
			t.Errorf("Bytes clobbered: got %d want 500000000", got.Bytes)
		}
		if got.UpdatedAt != 200 {
			t.Errorf("UpdatedAt: got %d want 200", got.UpdatedAt)
		}
	})

	t.Run("monotonic max on counters", func(t *testing.T) {
		next := ProgressEvent{Files: 999, Bytes: 600_000_000, Errors: 5, UpdatedAt: 200}
		got := mergeProgress(prev, next)
		if got.Files != 1000 {
			t.Errorf("Files should stay max: got %d want 1000", got.Files)
		}
		if got.Bytes != 600_000_000 {
			t.Errorf("Bytes should advance: got %d", got.Bytes)
		}
		if got.Errors != 5 {
			t.Errorf("Errors should advance: got %d", got.Errors)
		}
	})

	t.Run("empty Current preserves previous", func(t *testing.T) {
		next := ProgressEvent{Current: "", UpdatedAt: 200}
		got := mergeProgress(prev, next)
		if got.Current != "old.mov" {
			t.Errorf("Current cleared: got %q want %q", got.Current, "old.mov")
		}
	})

	t.Run("new Current replaces previous", func(t *testing.T) {
		next := ProgressEvent{Current: "new.mov", UpdatedAt: 200}
		got := mergeProgress(prev, next)
		if got.Current != "new.mov" {
			t.Errorf("Current: got %q want %q", got.Current, "new.mov")
		}
	})
}

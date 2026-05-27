package migrator

import (
	"context"
	"errors"
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
	j1, err := m.Submit("/tmp/src1", "/jfs/dst1", DefaultSyncOptions())
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
	j2, _ := m.Submit("/tmp/src2", "/jfs/dst2", DefaultSyncOptions())
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

	j, _ := m.Submit("/tmp/x", "/jfs/y", DefaultSyncOptions())
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

	j, _ := m.Submit("/tmp/src", "/jfs/dst", DefaultSyncOptions())
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

	j, _ := m.Submit("/tmp/src", "/jfs/dst", DefaultSyncOptions())
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

	j, _ := m.Submit("/tmp/src", "/jfs/dst", DefaultSyncOptions())
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

	_, _ = m.Submit("/tmp/a", "/jfs/a", DefaultSyncOptions())
	_, _ = m.Submit("/tmp/b", "/jfs/b", DefaultSyncOptions())
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

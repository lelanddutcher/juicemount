package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

// TestManagerDefaultFarmJobDrainsOnGenericWorker exercises the production Redis
// path that regressed when per-kind queues were introduced: the Manager's
// default [derivatives] job must be consumable by a generic worker whose kinds
// are empty (the configuration older deployments already have).
//
// Set JM_TEST_REDIS to an isolated redis:// URL. The test creates one terminal
// job record with the normal seven-day TTL, so it must never target production
// metadata Redis.
func TestManagerDefaultFarmJobDrainsOnGenericWorker(t *testing.T) {
	metaURL := os.Getenv("JM_TEST_REDIS")
	if metaURL == "" {
		t.Skip("set JM_TEST_REDIS to run the Manager → Redis → generic-worker integration test")
	}

	q, err := farmqueue.Open(metaURL)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	defer q.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := q.Ping(ctx); err != nil {
		t.Fatalf("ping JM_TEST_REDIS: %v", err)
	}

	a := &API{farmQ: q}
	req := httptest.NewRequest(http.MethodPost, "/api/farm/sweep",
		strings.NewReader(`{"path":"/jfs/jm-test-farm-queue","kinds":["derivatives"]}`))
	rec := httptest.NewRecorder()
	a.handleFarmSweep(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("enqueue = %d: %s", rec.Code, rec.Body.String())
	}
	var response struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode enqueue response: %v", err)
	}
	if response.ID == "" {
		t.Fatal("enqueue response has no job id")
	}

	job, ok, err := q.DequeueKinds(ctx, time.Second, nil)
	if err != nil {
		t.Fatalf("generic worker dequeue: %v", err)
	}
	if !ok {
		t.Fatal("generic worker did not receive Manager's default derivatives job")
	}
	if job.ID != response.ID || len(job.Kinds) != 1 || job.Kinds[0] != farmqueue.KindDerivatives {
		t.Fatalf("dequeued job = %+v, want Manager derivatives job %q", job, response.ID)
	}

	if err := q.MarkRunning(ctx, job.ID); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if err := q.MarkDone(ctx, job.ID, 1, 0); err != nil {
		t.Fatalf("mark done: %v", err)
	}

	jobs, err := q.ListJobs(ctx, 10)
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	for _, status := range jobs {
		if status.ID == job.ID {
			if status.Status != farmqueue.StatusDone {
				t.Fatalf("status = %q, want done", status.Status)
			}
			return
		}
	}
	t.Fatalf("job %q missing from recent jobs", job.ID)
}

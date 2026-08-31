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

// TestManagerDefaultFarmJobReachesServerDispatcher exercises the current
// production Redis path end to end: the Manager's directory-sized derivatives
// request is routed to the server planning lane, capability-checked, durably
// claimed, completed, and acknowledged. cmd/jmfarm's separate plan tests prove
// this short parent expands into bounded executable children.
//
// Set JM_TEST_MANAGER_REDIS to an isolated redis:// URL. The test creates one
// terminal job record with the normal seven-day TTL, so it must never target
// production metadata Redis.
func TestManagerDefaultFarmJobReachesServerDispatcher(t *testing.T) {
	metaURL := os.Getenv("JM_TEST_MANAGER_REDIS")
	if metaURL == "" {
		t.Skip("set JM_TEST_MANAGER_REDIS to run the Manager → Redis → server-dispatch integration test")
	}

	q, err := farmqueue.Open(metaURL)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	defer q.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := q.Ping(ctx); err != nil {
		t.Fatalf("ping JM_TEST_MANAGER_REDIS: %v", err)
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

	worker := farmqueue.Worker{
		ID:           "manager-live-server-" + farmqueue.NewID(),
		Name:         "manager-live-server",
		Role:         farmqueue.QueueClassServer,
		Capabilities: []string{"cpu", "metadata"},
	}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, worker)
	if err != nil {
		t.Fatalf("server worker claim: %v", err)
	}
	if !ok {
		t.Fatal("server worker did not receive Manager's default derivatives job")
	}
	job := claim.Job
	if job.ID != response.ID || len(job.Kinds) != 1 || job.Kinds[0] != farmqueue.KindDerivatives {
		t.Fatalf("dequeued job = %+v, want Manager derivatives job %q", job, response.ID)
	}
	if job.QueueClass != farmqueue.QueueClassServer || job.SelectedBackend != "server-dispatch" || !job.PlanOnly ||
		len(job.RequiredCapabilities) != 1 || job.RequiredCapabilities[0] != "metadata" {
		t.Fatalf("routing = class %q backend %q plan_only=%v capabilities=%v, want server/server-dispatch planning parent",
			job.QueueClass, job.SelectedBackend, job.PlanOnly, job.RequiredCapabilities)
	}

	if err := q.MarkClaimRunning(ctx, claim, worker); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if err := q.MarkDone(ctx, job.ID, 1, 0); err != nil {
		t.Fatalf("mark done: %v", err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatalf("ack claim: %v", err)
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

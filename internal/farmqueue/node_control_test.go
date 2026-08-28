package farmqueue

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestValidWorkerControlName(t *testing.T) {
	for _, good := range []string{"render-a", "b70-render-rc0.5", "nas_server"} {
		if !ValidWorkerControlName(good) {
			t.Errorf("valid name %q rejected", good)
		}
	}
	for _, bad := range []string{"", "Render-A", "../render", "render a", "render/a"} {
		if ValidWorkerControlName(bad) {
			t.Errorf("invalid name %q accepted", bad)
		}
	}
}

// TestRedisWorkerLifecycleControl exercises the one-shot restart + durable
// disable protocol against the same isolated Redis gate as claim recovery.
func TestRedisWorkerLifecycleControl(t *testing.T) {
	metaURL := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if metaURL == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_REDIS_URL to an isolated Redis and JM_FARM_TEST_REDIS_FLUSH=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	q, err := Open(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	first, err := q.RequestWorkerRestart(ctx, "render-a", "manager")
	if err != nil {
		t.Fatal(err)
	}
	second, err := q.RequestWorkerRestart(ctx, "render-a", "manager")
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := q.WaitWorkerCommand(ctx, "render-a", 0)
	if err != nil || !ok || got.ID != first.ID {
		t.Fatalf("first FIFO command = %+v ok=%v err=%v", got, ok, err)
	}
	if err := q.AcknowledgeWorkerCommand(ctx, got, "runtime-1"); err != nil {
		t.Fatal(err)
	}
	status, found, err := q.WorkerCommandStatusByID(ctx, first.ID)
	if err != nil || !found || status.State != "acknowledged" || status.WorkerID != "runtime-1" {
		t.Fatalf("ack status = %+v found=%v err=%v", status, found, err)
	}
	got, ok, err = q.WaitWorkerCommand(ctx, "render-a", 0)
	if err != nil || !ok || got.ID != second.ID {
		t.Fatalf("second FIFO command = %+v ok=%v err=%v", got, ok, err)
	}
	if _, ok, err := q.WaitWorkerCommand(ctx, "render-a", 0); err != nil || ok {
		t.Fatalf("empty command queue ok=%v err=%v", ok, err)
	}

	if err := q.SetWorkerDisabled(ctx, "render-a", true); err != nil {
		t.Fatal(err)
	}
	if disabled, err := q.WorkerDisabled(ctx, "render-a"); err != nil || !disabled {
		t.Fatalf("disabled=%v err=%v", disabled, err)
	}
	if err := q.SetWorkerDisabled(ctx, "render-a", false); err != nil {
		t.Fatal(err)
	}
	if disabled, err := q.WorkerDisabled(ctx, "render-a"); err != nil || disabled {
		t.Fatalf("after enable disabled=%v err=%v", disabled, err)
	}
}

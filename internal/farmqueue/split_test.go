package farmqueue

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestNewCPUFallbackSubsetIsExactAndIndependent(t *testing.T) {
	parent := Job{
		ID: "render-parent", Path: "/jfs/incoming", Kinds: []string{KindProxy}, Producer: "manager",
		CRF: 19, Preset: "slow", VCodec: "hevc_vaapi", QueueClass: QueueClassRender,
		RequiredCapabilities: []string{"encoder:hevc_vaapi", "worker:gpu-1"},
		SelectedBackend:      "hevc_vaapi", SelectedWorker: "gpu-1", Attempts: 1,
		ProcessedOffset: 27, RetryTargets: []string{"old-retry.mov"},
	}
	targets := []string{"/jfs/incoming/prores-a.mov", "/jfs/incoming/prores-b.mov"}

	child, err := newCPUFallbackSubset(parent, targets)
	if err != nil {
		t.Fatal(err)
	}
	if child.ID != "render-parent-cpu" || child.ParentID != parent.ID {
		t.Fatalf("child identity = %q parent=%q", child.ID, child.ParentID)
	}
	if child.QueueClass != QueueClassCPU || child.VCodec != "libx264" || child.SelectedBackend != "libx264" {
		t.Fatalf("child route = class=%q codec=%q backend=%q", child.QueueClass, child.VCodec, child.SelectedBackend)
	}
	if child.SelectedWorker != "" || !reflect.DeepEqual(child.RequiredCapabilities, []string{"cpu"}) {
		t.Fatalf("child worker/capabilities = %q %v", child.SelectedWorker, child.RequiredCapabilities)
	}
	if child.Attempts != 0 || child.ProcessedOffset != 0 {
		t.Fatalf("child retry state = attempts=%d offset=%d", child.Attempts, child.ProcessedOffset)
	}
	if !reflect.DeepEqual(child.RetryTargets, targets) {
		t.Fatalf("child targets = %v, want %v", child.RetryTargets, targets)
	}
	child.RetryTargets[0] = "mutated"
	if targets[0] == "mutated" {
		t.Fatal("child aliases caller target slice")
	}
	if parent.VCodec != "hevc_vaapi" || parent.QueueClass != QueueClassRender || parent.ProcessedOffset != 27 {
		t.Fatalf("parent was mutated: %+v", parent)
	}
}

func TestRedisCPUFallbackSplitIsIdempotent(t *testing.T) {
	metaURL := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if metaURL == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_REDIS_URL to an isolated Redis and JM_FARM_TEST_REDIS_FLUSH=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	q, err := Open(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	render := Worker{ID: "render", Role: QueueClassRender,
		Capabilities: []string{"encoder:hevc_vaapi"}, Encoders: []string{"hevc_vaapi"}}
	parent := Job{
		ID: "mixed-batch", Path: "/jfs/incoming", Kinds: []string{KindProxy}, Producer: "manager",
		VCodec: "hevc_vaapi", QueueClass: QueueClassRender, SelectedBackend: "hevc_vaapi",
		SelectedWorker: render.ID, RequiredCapabilities: []string{"encoder:hevc_vaapi", "worker:" + render.ID},
	}
	if err := q.Enqueue(ctx, parent); err != nil {
		t.Fatal(err)
	}
	parentClaim, ok, err := q.ClaimForWorker(ctx, time.Second, render)
	if err != nil || !ok {
		t.Fatalf("render parent claim: ok=%v err=%v", ok, err)
	}
	targets := []string{"/jfs/incoming/prores.mov"}
	first, created, err := q.EnqueueCPUFallbackSubset(ctx, parentClaim.Job, targets, "ProRes decoder unavailable")
	if err != nil || !created {
		t.Fatalf("first split: created=%v err=%v", created, err)
	}
	second, created, err := q.EnqueueCPUFallbackSubset(ctx, parentClaim.Job, targets, "duplicate recovery")
	if err != nil || created || second.ID != first.ID {
		t.Fatalf("replayed split: child=%q created=%v err=%v", second.ID, created, err)
	}
	if depth, err := q.rdb.LLen(ctx, classQueue(KindProxy, QueueClassCPU)).Result(); err != nil || depth != 1 {
		t.Fatalf("CPU queue depth = %d, %v; want 1", depth, err)
	}

	server := Worker{ID: "server", Role: QueueClassServer, Capabilities: []string{"cpu", "metadata"}}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, server)
	if err != nil || !ok {
		t.Fatalf("server claim: ok=%v err=%v", ok, err)
	}
	if claim.Job.ParentID != parent.ID || !reflect.DeepEqual(claim.Job.RetryTargets, targets) {
		t.Fatalf("claimed CPU child = %+v", claim.Job)
	}
	if n, err := q.rdb.LLen(ctx, ProcessingPrefix+render.ID).Result(); err != nil || n != 1 {
		t.Fatalf("render parent processing receipts = %d, %v; want 1 while CPU child runs", n, err)
	}
	if n, err := q.rdb.LLen(ctx, ProcessingPrefix+server.ID).Result(); err != nil || n != 1 {
		t.Fatalf("CPU child processing receipts = %d, %v; want 1", n, err)
	}
}

func TestNewCPUFallbackSubsetRejectsInvalidParent(t *testing.T) {
	if _, err := newCPUFallbackSubset(Job{}, []string{"clip.mov"}); err == nil {
		t.Fatal("missing parent ID accepted")
	}
	if _, err := newCPUFallbackSubset(Job{ID: "x", Kinds: []string{KindTranscript}}, []string{"clip.mov"}); err == nil {
		t.Fatal("non-proxy parent accepted")
	}
	if _, err := newCPUFallbackSubset(Job{ID: "x", Kinds: []string{KindProxy}}, nil); err == nil {
		t.Fatal("empty fallback subset accepted")
	}
}

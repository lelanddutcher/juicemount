package farmqueue

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNewTargetShardsBoundsAndUnpinsRenderWorker(t *testing.T) {
	parent := Job{
		ID: "directory-proxy", Path: "/jfs/incoming", Kinds: []string{KindProxy}, Producer: "manager",
		VCodec: "hevc_qsv", QueueClass: QueueClassRender, SelectedBackend: "hevc_qsv", SelectedWorker: "gpu-a",
		RequiredCapabilities: []string{"encoder:hevc_qsv", "worker:gpu-a"}, Attempts: 2, ProcessedOffset: 99,
	}
	targets := []string{"a.mov", "b.mov", "c.mov", "d.mov", "e.mov"}
	children, err := newTargetShards(parent, targets, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 3 {
		t.Fatalf("children = %d, want 3", len(children))
	}
	for i, child := range children {
		if child.ID == parent.ID || child.ParentID != parent.ID || child.ShardIndex != i+1 || child.ShardCount != 3 {
			t.Fatalf("child %d identity = %+v", i, child)
		}
		if child.SelectedWorker != "" || !reflect.DeepEqual(child.RequiredCapabilities, []string{"encoder:hevc_qsv"}) {
			t.Fatalf("child %d retained exact worker pin: %q %v", i, child.SelectedWorker, child.RequiredCapabilities)
		}
		if child.QueueClass != QueueClassRender || child.SelectedBackend != "hevc_qsv" || child.VCodec != "hevc_qsv" {
			t.Fatalf("child %d lost verified render route: %+v", i, child)
		}
		if child.Attempts != parent.Attempts || child.ProcessedOffset != 0 || len(child.RetryTargets) > 2 {
			t.Fatalf("child %d retry state/size = %+v", i, child)
		}
	}
	got := append(append([]string{}, children[0].RetryTargets...), children[1].RetryTargets...)
	got = append(got, children[2].RetryTargets...)
	if !reflect.DeepEqual(got, targets) {
		t.Fatalf("shard targets = %v, want %v", got, targets)
	}
}

func TestRedisTargetShardsAreIdempotentAndClaimableByCompatibleWorker(t *testing.T) {
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

	parent := Job{
		ID: "directory-proxy", Path: "/jfs/incoming", Kinds: []string{KindProxy}, Producer: "manager",
		VCodec: "hevc_vaapi", QueueClass: QueueClassRender, SelectedBackend: "hevc_vaapi",
		SelectedWorker: "render-a", RequiredCapabilities: []string{"encoder:hevc_vaapi", "worker:render-a"},
		Attempts: 1,
	}
	if err := q.Enqueue(ctx, parent); err != nil {
		t.Fatal(err)
	}
	renderA := Worker{ID: "render-a", Role: QueueClassRender, Capabilities: []string{"encoder:hevc_vaapi"}, Encoders: []string{"hevc_vaapi"}}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, renderA)
	if err != nil || !ok {
		t.Fatalf("parent claim: ok=%v err=%v", ok, err)
	}
	if err := q.RequeueClaimSameRoute(ctx, claim, "transient shard transaction failure"); err != nil {
		t.Fatal(err)
	}
	claim, ok, err = q.ClaimForWorker(ctx, time.Second, renderA)
	if err != nil || !ok {
		t.Fatalf("same-route parent reclaim: ok=%v err=%v", ok, err)
	}
	if claim.Job.Attempts != parent.Attempts || claim.Job.SelectedBackend != parent.SelectedBackend || claim.Job.SelectedWorker != parent.SelectedWorker {
		t.Fatalf("same-route retry changed admission: %+v", claim.Job)
	}
	targets := []string{"a.mov", "b.mov", "c.mov", "d.mov", "e.mov"}
	children, created, err := q.EnqueueTargetShards(ctx, claim.Job, targets, 2)
	if err != nil || created != 3 || len(children) != 3 {
		t.Fatalf("first split: children=%d created=%d err=%v", len(children), created, err)
	}
	_, created, err = q.EnqueueTargetShards(ctx, claim.Job, targets, 2)
	if err != nil || created != 0 {
		t.Fatalf("replayed split: created=%d err=%v", created, err)
	}
	if depth, err := q.rdb.LLen(ctx, classQueue(KindProxy, QueueClassRender)).Result(); err != nil || depth != 3 {
		t.Fatalf("render queue depth = %d, %v; want 3", depth, err)
	}
	if err := q.MarkDispatched(ctx, parent.ID); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if removed, err := q.ClearFinished(ctx); err != nil || removed != 0 {
		t.Fatalf("clear finished removed dispatched parent: removed=%d err=%v", removed, err)
	}
	parentStatus, err := q.rdb.HGetAll(ctx, JobHashPrefix+parent.ID).Result()
	if err != nil || parentStatus["status"] != StatusDispatched || parentStatus["lease_owner"] != "" {
		t.Fatalf("dispatched parent status = %v, %v", parentStatus, err)
	}

	// The exact render-a pin was intentionally released, so a different node
	// with the same verified encoder can take a bounded child immediately.
	renderB := Worker{ID: "render-b", Role: QueueClassRender, Capabilities: []string{"encoder:hevc_vaapi"}, Encoders: []string{"hevc_vaapi"}}
	childClaim, ok, err := q.ClaimForWorker(ctx, time.Second, renderB)
	if err != nil || !ok {
		t.Fatalf("compatible worker child claim: ok=%v err=%v", ok, err)
	}
	if childClaim.Job.ParentID != parent.ID || childClaim.Job.SelectedWorker != "" || childClaim.Job.Attempts != parent.Attempts || len(childClaim.Job.RetryTargets) > 2 {
		t.Fatalf("claimed child = %+v", childClaim.Job)
	}
}

func TestNewTargetShardsPreservesExplicitCPUFallback(t *testing.T) {
	parent := Job{
		ID: "mixed-cpu", ParentID: "mixed", Path: "/jfs/incoming", Kinds: []string{KindProxy},
		VCodec: "libx264", QueueClass: QueueClassCPU, SelectedBackend: "libx264",
		RequiredCapabilities: []string{"cpu"},
	}
	children, err := newTargetShards(parent, []string{"a.mov", "b.mov", "c.mov"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, child := range children {
		if child.QueueClass != QueueClassCPU || child.VCodec != "libx264" || !reflect.DeepEqual(child.RequiredCapabilities, []string{"cpu"}) {
			t.Fatalf("CPU child was promoted or rerouted: %+v", child)
		}
	}
}

func TestRedisPlannedDerivativeChildrenAreAtomicVisibleAndClaimable(t *testing.T) {
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
	children := []Job{
		{ID: "p-metadata", ParentID: "p", Path: "/jfs", Kinds: []string{KindDerivatives}, Producer: "manager",
			DerivativePass: DerivativePassMetadata, QueueClass: QueueClassServer, SelectedBackend: "server-metadata",
			RequiredCapabilities: []string{"metadata"}, RetryTargets: []string{"/jfs/a.mp4"}, ShardIndex: 1, ShardCount: 3},
		{ID: "p-render", ParentID: "p", Path: "/jfs", Kinds: []string{KindDerivatives}, Producer: "manager",
			DerivativePass: DerivativePassPreviews, QueueClass: QueueClassRender, SelectedBackend: "h264_vaapi",
			RequiredCapabilities: []string{"decoder:h264_vaapi"}, RetryTargets: []string{"/jfs/a.mp4"}, ShardIndex: 2, ShardCount: 3},
		{ID: "p-cpu", ParentID: "p", Path: "/jfs", Kinds: []string{KindDerivatives}, Producer: "manager",
			DerivativePass: DerivativePassPreviews, QueueClass: QueueClassCPU, SelectedBackend: "cpu-decode",
			RequiredCapabilities: []string{"cpu"}, RetryTargets: []string{"/jfs/b.mov"}, ShardIndex: 3, ShardCount: 3,
			RoutingReason: "ProRes has no verified hardware decoder"},
	}
	if created, err := q.EnqueuePlannedChildren(ctx, children); err != nil || created != 3 {
		t.Fatalf("first plan publish=%d, %v", created, err)
	}
	if created, err := q.EnqueuePlannedChildren(ctx, children); err != nil || created != 0 {
		t.Fatalf("replayed plan publish=%d, %v", created, err)
	}
	server := Worker{ID: "server", Role: QueueClassServer, Capabilities: []string{"metadata"}}
	render := Worker{ID: "render", Role: QueueClassRender, Decoders: []string{"h264_vaapi"}}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, render)
	if err != nil || !ok || claim.Job.ID != "p-render" || claim.Job.SelectedBackend != "h264_vaapi" {
		t.Fatalf("render derivative claim=%+v ok=%v err=%v", claim.Job, ok, err)
	}
	if err := q.MarkClaimRunning(ctx, claim, render); err != nil {
		t.Fatal(err)
	}
	if err := q.RequeueClaim(ctx, claim, true, "verified decoder failed; explicit CPU fallback"); err != nil {
		t.Fatal(err)
	}
	claimed := map[string]bool{}
	for len(claimed) < 3 {
		claim, ok, err := q.ClaimForWorker(ctx, time.Second, server)
		if err != nil || !ok {
			t.Fatalf("server derivative claim=%+v ok=%v err=%v", claim.Job, ok, err)
		}
		claimed[claim.Job.ID] = true
		if err := q.AckClaim(ctx, claim); err != nil {
			t.Fatal(err)
		}
	}
	if !claimed["p-metadata"] || !claimed["p-cpu"] || !claimed["p-render"] {
		t.Fatalf("server claims=%v", claimed)
	}
	status, err := q.rdb.HGetAll(ctx, JobHashPrefix+"p-cpu").Result()
	if err != nil || status["backend"] != "cpu-decode" || status["error"] == "" || status["derivative_pass"] != DerivativePassPreviews {
		t.Fatalf("visible CPU fallback status=%v err=%v", status, err)
	}
	requeued, err := q.rdb.HGetAll(ctx, JobHashPrefix+"p-render").Result()
	if err != nil || requeued["backend"] != "cpu-decode" || requeued["queue_class"] != QueueClassCPU ||
		!strings.Contains(requeued["error"], "explicit CPU fallback") {
		t.Fatalf("failed render preview fallback status=%v err=%v", requeued, err)
	}
}

func TestNewTargetShardsRejectsNestedOrInvalidSplit(t *testing.T) {
	if _, err := newTargetShards(Job{}, []string{"a", "b"}, 1); err == nil {
		t.Fatal("missing parent ID accepted")
	}
	if _, err := newTargetShards(Job{ID: "x", ShardIndex: 1, ShardCount: 2}, []string{"a", "b"}, 1); err == nil {
		t.Fatal("nested shard accepted")
	}
	if _, err := newTargetShards(Job{ID: "x"}, []string{"a"}, 1); err == nil {
		t.Fatal("unnecessary split accepted")
	}
}

func TestPlanOnlyParentCreatesOneBoundedChildForOneFile(t *testing.T) {
	parent := Job{
		ID: "directory-transcript", Path: "/jfs/incoming", Kinds: []string{KindTranscript},
		QueueClass: QueueClassServer, SelectedBackend: "server-dispatch",
		RequiredCapabilities: []string{"metadata"}, PlanOnly: true,
	}
	children, err := newTargetShards(parent, []string{"/jfs/incoming/one.mov"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 {
		t.Fatalf("children=%d, want one execution child", len(children))
	}
	child := children[0]
	if child.PlanOnly || child.QueueClass != "" || child.SelectedBackend != "" || len(child.RequiredCapabilities) != 0 {
		t.Fatalf("planner admission leaked into execution child: %+v", child)
	}
	if child.ShardIndex != 1 || child.ShardCount != 1 || !reflect.DeepEqual(child.RetryTargets, []string{"/jfs/incoming/one.mov"}) {
		t.Fatalf("one-file child is not bounded and exact: %+v", child)
	}
}

func TestRedisServerPlannerPublishesClaimableRenderChildren(t *testing.T) {
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

	server := Worker{ID: "server", Name: "server", Role: QueueClassServer,
		Capabilities: []string{"cpu", "metadata"}}
	render := Worker{ID: "render", Name: "render", Role: QueueClassRender,
		Capabilities: []string{"encoder:hevc_vaapi"}, Encoders: []string{"hevc_vaapi"}}
	if err := q.Heartbeat(ctx, render); err != nil {
		t.Fatal(err)
	}
	parent := NewJob("/jfs/incoming", []string{KindProxy}, "manager")
	if err := q.Enqueue(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := q.ClaimForWorker(ctx, 100*time.Millisecond, render); err != nil || ok {
		t.Fatalf("render claimed unplanned directory parent: ok=%v err=%v", ok, err)
	}
	plan, ok, err := q.ClaimForWorker(ctx, time.Second, server)
	if err != nil || !ok || !plan.Job.PlanOnly {
		t.Fatalf("server planner claim = %+v ok=%v err=%v", plan.Job, ok, err)
	}
	targets := []string{"/jfs/incoming/a.mov", "/jfs/incoming/b.mov", "/jfs/incoming/c.mov"}
	children, created, err := q.EnqueueTargetShards(ctx, plan.Job, targets, 2)
	if err != nil || created != 2 || len(children) != 2 {
		t.Fatalf("plan split: children=%d created=%d err=%v", len(children), created, err)
	}
	if err := q.MarkDispatched(ctx, parent.ID); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, plan); err != nil {
		t.Fatal(err)
	}
	for _, child := range children {
		if child.PlanOnly || child.QueueClass != QueueClassRender || child.SelectedBackend != "hevc_vaapi" ||
			!reflect.DeepEqual(child.RequiredCapabilities, []string{"encoder:hevc_vaapi"}) {
			t.Fatalf("planned execution child = %+v", child)
		}
	}
	claim, ok, err := q.ClaimForWorker(ctx, time.Second, render)
	if err != nil || !ok {
		t.Fatalf("render did not claim planned child: ok=%v err=%v", ok, err)
	}
	if claim.Job.ParentID != parent.ID || claim.Job.PlanOnly || len(claim.Job.RetryTargets) > 2 {
		t.Fatalf("render child claim = %+v", claim.Job)
	}
}

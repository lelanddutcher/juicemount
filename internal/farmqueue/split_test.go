package farmqueue

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
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
	if !child.CPUFallbackLocked {
		t.Fatal("explicit CPU fallback child remained eligible for hardware re-promotion")
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
		Capabilities: []string{"encoder:hevc_vaapi"}, Encoders: []string{"hevc_vaapi"},
		Benchmarks: WorkerBenchmarks{EncodeFPS: 120, ProbedAt: nowISO()}}
	if err := q.Heartbeat(ctx, render); err != nil {
		t.Fatal(err)
	}
	if err := q.PublishStoragePermit(ctx, StoragePermit{Source: "test", AvailableBytes: 2, RequiredBytes: 1}); err != nil {
		t.Fatal(err)
	}
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
	firstRaw, err := q.rdb.HGet(ctx, JobHashPrefix+first.ID, "payload").Result()
	if err != nil {
		t.Fatalf("read persisted fallback payload: %v", err)
	}
	if receipt, err := q.rdb.HGet(ctx, JobHashPrefix+first.ID, "cpu_fallback_published").Result(); err != nil || receipt != "1" {
		t.Fatalf("publication receipt = %q, %v; want 1", receipt, err)
	}
	second, created, err := q.EnqueueCPUFallbackSubset(ctx, parentClaim.Job, targets, "duplicate recovery")
	if err != nil || created || second.ID != first.ID {
		t.Fatalf("replayed split: child=%q created=%v err=%v", second.ID, created, err)
	}
	secondRaw, err := q.rdb.HGet(ctx, JobHashPrefix+first.ID, "payload").Result()
	if err != nil || secondRaw != firstRaw || second.RoutingReason != first.RoutingReason ||
		second.EnqueuedAt != first.EnqueuedAt || second.SelectedBackend != first.SelectedBackend ||
		second.QueueClass != first.QueueClass || !reflect.DeepEqual(second.RetryTargets, first.RetryTargets) {
		t.Fatalf("replay changed immutable fallback payload: first=%+v second=%+v rawEqual=%v err=%v",
			first, second, secondRaw == firstRaw, err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, _, err := q.EnqueueCPUFallbackSubset(canceled, parentClaim.Job, targets, "transport cancellation"); !errors.Is(err, context.Canceled) || errors.Is(err, ErrCPUFallbackAmbiguous) {
		t.Fatalf("canceled transport: got %v, want untyped context cancellation", err)
	}
	if _, _, err := q.EnqueueCPUFallbackSubset(ctx, parentClaim.Job,
		[]string{"/jfs/incoming/different-target.mov"}, "changed subset"); !errors.Is(err, ErrCPUFallbackAmbiguous) {
		t.Fatalf("changed immutable subset: got %v, want ErrCPUFallbackAmbiguous", err)
	}
	unchangedRaw, err := q.rdb.HGet(ctx, JobHashPrefix+first.ID, "payload").Result()
	if err != nil || unchangedRaw != firstRaw {
		t.Fatalf("rejected changed-target replay mutated payload: equal=%v err=%v", unchangedRaw == firstRaw, err)
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
	if claim.Job.RoutingReason != "ProRes decoder unavailable" {
		t.Fatalf("CPU child lost immutable fallback provenance: %q", claim.Job.RoutingReason)
	}
	if n, err := q.rdb.LLen(ctx, ProcessingPrefix+render.ID).Result(); err != nil || n != 1 {
		t.Fatalf("render parent processing receipts = %d, %v; want 1 while CPU child runs", n, err)
	}
	if n, err := q.rdb.LLen(ctx, ProcessingPrefix+server.ID).Result(); err != nil || n != 1 {
		t.Fatalf("CPU child processing receipts = %d, %v; want 1", n, err)
	}
	if n, err := q.rdb.LLen(ctx, classQueue(KindProxy, QueueClassCPU)).Result(); err != nil || n != 0 {
		t.Fatalf("CPU queue depth after sole claim = %d, %v; want 0", n, err)
	}
	if extra, ok, err := q.ClaimForWorker(ctx, 50*time.Millisecond, server); err != nil || ok {
		t.Fatalf("duplicate CPU fallback remained claimable: ok=%v claim=%+v err=%v", ok, extra, err)
	}
}

func TestRedisCPUFallbackSplitRejectsPayloadWithoutPublicationReceipt(t *testing.T) {
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

	parent := Job{ID: "unpublished-parent", Path: "/jfs/incoming", Kinds: []string{KindProxy}, Producer: "manager",
		VCodec: "hevc_vaapi", QueueClass: QueueClassRender, SelectedBackend: "hevc_vaapi"}
	targets := []string{"/jfs/incoming/prores.mov"}
	reason := "ProRes decoder unavailable"
	child, err := newCPUFallbackSubset(parent, targets)
	if err != nil {
		t.Fatal(err)
	}
	child.RoutingReason = reason
	raw, err := json.Marshal(child)
	if err != nil {
		t.Fatal(err)
	}
	status := JobStatus{
		ID: child.ID, Status: StatusQueued, Path: child.Path, Kinds: strings.Join(child.Kinds, ","),
		Producer: child.Producer, EnqueuedAt: child.EnqueuedAt, Backend: child.SelectedBackend,
		QueueClass: child.QueueClass, ParentID: child.ParentID, CoverageParentID: child.CoverageParentID, Error: reason,
	}
	statusJSON, err := json.Marshal(status.toMap())
	if err != nil {
		t.Fatal(err)
	}
	// Redis Lua errors do not roll back commands already executed. This variant
	// fails after persisting the payload but before LPUSH, modeling that partial
	// runtime-error window against the real isolated server.
	faultScript := strings.Replace(enqueueCPUFallbackSubsetScript,
		"redis.call('HSET', KEYS[1], 'payload', ARGV[1])\nif ARGV[4] ~= '' then",
		"redis.call('HSET', KEYS[1], 'payload', ARGV[1])\nredis.call('ZADD', KEYS[3], 'not-a-score', statusFields.id)\nif ARGV[4] ~= '' then", 1)
	if faultScript == enqueueCPUFallbackSubsetScript {
		t.Fatal("failed to install injected Lua fault")
	}
	keys := []string{JobHashPrefix + child.ID, QueueKeyFor(&child), JobIndexKey}
	if _, err := q.rdb.Eval(ctx, faultScript, keys,
		raw, statusJSON, time.Now().UnixMicro(), child.TrackedEpoch, int64(JobTTL/time.Second),
		"", "", "", "", child.ID, "", "").Result(); err == nil {
		t.Fatal("injected Lua failure unexpectedly succeeded")
	}
	if saved, err := q.rdb.HGet(ctx, keys[0], "payload").Result(); err != nil || saved != string(raw) {
		t.Fatalf("injected payload state = %q, %v; want persisted payload", saved, err)
	}
	if _, err := q.rdb.HGet(ctx, keys[0], "cpu_fallback_published").Result(); err != redis.Nil {
		t.Fatalf("injected failure left a publication receipt: %v", err)
	}
	if depth, err := q.rdb.LLen(ctx, keys[1]).Result(); err != nil || depth != 0 {
		t.Fatalf("injected failure queue depth=%d err=%v; want 0", depth, err)
	}

	if _, _, err := q.EnqueueCPUFallbackSubset(ctx, parent, targets, reason); !errors.Is(err, ErrCPUFallbackAmbiguous) {
		t.Fatalf("unreceipted payload: got %v, want ErrCPUFallbackAmbiguous", err)
	}
	if depth, err := q.rdb.LLen(ctx, keys[1]).Result(); err != nil || depth != 0 {
		t.Fatalf("replay queued unpublished payload: depth=%d err=%v", depth, err)
	}
	if _, err := q.rdb.ZScore(ctx, JobIndexKey, child.ID).Result(); err != redis.Nil {
		t.Fatalf("unpublished payload appeared in index: %v", err)
	}

	// Also model LPUSH succeeding before a later command fails. The payload is
	// now queued, but without the final receipt a retry must still fail closed
	// and must not publish a second copy.
	if err := q.rdb.LPush(ctx, keys[1], raw).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := q.EnqueueCPUFallbackSubset(ctx, parent, targets, reason); !errors.Is(err, ErrCPUFallbackAmbiguous) {
		t.Fatalf("queued unreceipted payload: got %v, want ErrCPUFallbackAmbiguous", err)
	}
	if depth, err := q.rdb.LLen(ctx, keys[1]).Result(); err != nil || depth != 1 {
		t.Fatalf("queued unreceipted replay changed queue depth=%d err=%v; want 1", depth, err)
	}
}

func TestRedisCPUFallbackSplitFailsClosedOnWrongTypeHashOnlyRemnant(t *testing.T) {
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

	parent := Job{ID: "wrong-type-parent", Path: "/jfs/incoming", Kinds: []string{KindProxy}, Producer: "manager",
		VCodec: "hevc_vaapi", QueueClass: QueueClassRender, SelectedBackend: "hevc_vaapi"}
	targets := []string{"/jfs/incoming/prores.mov"}
	reason := "ProRes decoder unavailable"
	child, err := newCPUFallbackSubset(parent, targets)
	if err != nil {
		t.Fatal(err)
	}
	child.RoutingReason = reason
	raw, err := json.Marshal(child)
	if err != nil {
		t.Fatal(err)
	}
	status := JobStatus{
		ID: child.ID, Status: StatusQueued, Path: child.Path, Kinds: strings.Join(child.Kinds, ","),
		Producer: child.Producer, EnqueuedAt: child.EnqueuedAt, Backend: child.SelectedBackend,
		QueueClass: child.QueueClass, ParentID: child.ParentID, CoverageParentID: child.CoverageParentID, Error: reason,
	}
	queueKey := QueueKeyFor(&child)
	statusKey := JobHashPrefix + child.ID
	if err := q.rdb.Set(ctx, queueKey, "wrong Redis type", 0).Err(); err != nil {
		t.Fatal(err)
	}

	// Reproduce Redis EXEC behavior: LPUSH reports WRONGTYPE, but HSET and
	// ZADD still commit, leaving a hash-only remnant that must fail closed.
	pipe := q.rdb.TxPipeline()
	pipe.LPush(ctx, queueKey, raw)
	pipe.HSet(ctx, statusKey, status.toMap())
	pipe.ZAdd(ctx, JobIndexKey, redis.Z{Score: float64(time.Now().UnixMicro()), Member: child.ID})
	if _, err := pipe.Exec(ctx); err == nil {
		t.Fatal("wrong-type MULTI/EXEC unexpectedly succeeded")
	}
	if exists, err := q.rdb.Exists(ctx, statusKey).Result(); err != nil || exists != 1 {
		t.Fatalf("legacy status remnant exists=%d err=%v", exists, err)
	}
	if _, err := q.rdb.HGet(ctx, statusKey, "payload").Result(); err != redis.Nil {
		t.Fatalf("legacy remnant unexpectedly has payload: err=%v", err)
	}

	if _, _, err := q.EnqueueCPUFallbackSubset(ctx, parent, targets, reason); err == nil || errors.Is(err, ErrCPUFallbackAmbiguous) {
		t.Fatalf("wrong-type queue: got %v, want untyped refusal", err)
	}
	if _, err := q.rdb.HGet(ctx, statusKey, "payload").Result(); err != redis.Nil {
		t.Fatalf("failed type validation mutated hash-only remnant: err=%v", err)
	}
	if err := q.rdb.Del(ctx, queueKey).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := q.EnqueueCPUFallbackSubset(ctx, parent, targets, "recovery retry"); !errors.Is(err, ErrCPUFallbackAmbiguous) {
		t.Fatalf("payload-less hash remnant: got %v, want ErrCPUFallbackAmbiguous", err)
	}
	if _, err := q.rdb.HGet(ctx, statusKey, "payload").Result(); err != redis.Nil {
		t.Fatalf("failed hash-only replay unexpectedly added payload: err=%v", err)
	}
	if depth, err := q.rdb.LLen(ctx, queueKey).Result(); err != nil || depth != 0 {
		t.Fatalf("payload-less remnant queued fallback: depth=%d err=%v", depth, err)
	}
}

func TestRedisCPUFallbackSplitPrevalidatesAllKeyTypes(t *testing.T) {
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

	for _, wrongKey := range []string{"queue", "status", "index"} {
		t.Run(wrongKey, func(t *testing.T) {
			parent := Job{ID: "wrong-type-" + wrongKey, Path: "/jfs/incoming", Kinds: []string{KindProxy}, Producer: "manager",
				VCodec: "hevc_vaapi", QueueClass: QueueClassRender, SelectedBackend: "hevc_vaapi"}
			child, err := newCPUFallbackSubset(parent, []string{"/jfs/incoming/prores.mov"})
			if err != nil {
				t.Fatal(err)
			}
			queueKey := QueueKeyFor(&child)
			statusKey := JobHashPrefix + child.ID
			wrongRedisKey := map[string]string{"queue": queueKey, "status": statusKey, "index": JobIndexKey}[wrongKey]
			if err := q.rdb.Set(ctx, wrongRedisKey, "wrong Redis type", 0).Err(); err != nil {
				t.Fatal(err)
			}
			if _, _, err := q.EnqueueCPUFallbackSubset(ctx, parent, []string{"/jfs/incoming/prores.mov"}, "test"); err == nil || errors.Is(err, ErrCPUFallbackAmbiguous) {
				t.Fatalf("wrong-type destination: got %v, want untyped refusal", err)
			}
			if wrongKey != "status" {
				if exists, err := q.rdb.Exists(ctx, statusKey).Result(); err != nil || exists != 0 {
					t.Fatalf("status mutated before type error: exists=%d err=%v", exists, err)
				}
			}
			if wrongKey != "queue" {
				if exists, err := q.rdb.Exists(ctx, queueKey).Result(); err != nil || exists != 0 {
					t.Fatalf("queue mutated before type error: exists=%d err=%v", exists, err)
				}
			}
			if wrongKey != "index" {
				if exists, err := q.rdb.Exists(ctx, JobIndexKey).Result(); err != nil || exists != 0 {
					t.Fatalf("index mutated before type error: exists=%d err=%v", exists, err)
				}
			}
			if value, err := q.rdb.Get(ctx, wrongRedisKey).Result(); err != nil || value != "wrong Redis type" {
				t.Fatalf("wrong-type key mutated: value=%q err=%v", value, err)
			}
			if err := q.rdb.Del(ctx, wrongRedisKey).Err(); err != nil {
				t.Fatal(err)
			}
		})
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

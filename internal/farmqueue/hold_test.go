package farmqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRedisAmbiguousFallbackHoldsOriginalClaims uses the real claim and
// recovery scripts. The selected Redis DB must be disposable: it is flushed.
func TestRedisAmbiguousFallbackHoldsOriginalClaims(t *testing.T) {
	metaURL := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if metaURL == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_REDIS_URL to an isolated Redis and JM_FARM_TEST_REDIS_FLUSH=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	q, err := Open(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	if err := q.rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	worker := Worker{ID: "hold-render", Name: "hold-render", Role: QueueClassRender,
		Capabilities: []string{"encoder:hevc_vaapi", "transcript:vulkan"},
		Encoders:     []string{"hevc_vaapi"}, TranscriptBackends: []string{"vulkan"},
		Benchmarks: WorkerBenchmarks{EncodeFPS: 120, TranscriptXReal: 2, ProbedAt: nowISO()}}
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	if err := q.PublishStoragePermit(ctx, StoragePermit{Source: "hold-test", AvailableBytes: 2, RequiredBytes: 1}); err != nil {
		t.Fatal(err)
	}
	parent := func(id string) Job {
		return Job{ID: id, Path: "/jfs/incoming", Kinds: []string{KindProxy}, Producer: "hold-test",
			VCodec: "hevc_vaapi", QueueClass: QueueClassRender, SelectedBackend: "hevc_vaapi",
			SelectedWorker: worker.ID, RequiredCapabilities: []string{"encoder:hevc_vaapi", "worker:" + worker.ID}}
	}
	claimParent := func(id string) Claim {
		t.Helper()
		claim, ok, err := q.ClaimForWorker(ctx, 0, worker)
		if err != nil || !ok || claim.Job.ID != id || claim.Token == "" || claim.Raw == "" {
			t.Fatalf("claim %s: got=%s ok=%v token=%q err=%v", id, claim.Job.ID, ok, claim.Token, err)
		}
		if err := q.MarkClaimRunning(ctx, claim, worker); err != nil {
			t.Fatal(err)
		}
		return claim
	}
	targets := []string{"/jfs/incoming/prores.mov"}
	const reason = "ProRes source requires a CPU fallback"
	seedRemnant := func(parent Job, withPayload bool) (string, map[string]string) {
		t.Helper()
		child, err := newCPUFallbackSubset(parent, targets)
		if err != nil {
			t.Fatal(err)
		}
		key := JobHashPrefix + child.ID
		fields := map[string]any{"id": child.ID, "status": StatusQueued}
		if withPayload {
			// An observed render child can be done with different targets while
			// sharing this deterministic ID. Its immutable bytes are evidence,
			// not a license to overwrite it or infer CPU publication.
			child.RoutingReason = reason
			child.RetryTargets = []string{"/jfs/incoming/different.mov"}
			child.QueueClass = QueueClassRender
			child.SelectedBackend = "hevc_vaapi"
			child.CPUFallbackLocked = false
			raw, err := json.Marshal(child)
			if err != nil {
				t.Fatal(err)
			}
			fields["payload"] = string(raw)
			fields["status"] = StatusDone
			fields["backend"] = "hevc_vaapi"
		}
		if err := q.rdb.HSet(ctx, key, fields).Err(); err != nil {
			t.Fatal(err)
		}
		saved, err := q.rdb.HGetAll(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		return key, saved
	}
	checkChild := func(key string, saved map[string]string) {
		t.Helper()
		got, err := q.rdb.HGetAll(ctx, key).Result()
		if err != nil || !reflect.DeepEqual(got, saved) {
			t.Fatalf("ambiguous child mutated: key=%s got=%v err=%v", key, got, err)
		}
		if depth, err := q.rdb.LLen(ctx, classQueue(KindProxy, QueueClassCPU)).Result(); err != nil || depth != 0 {
			t.Fatalf("fallback CPU queue depth=%d err=%v", depth, err)
		}
	}
	checkHold := func(claim Claim, childKey string, childBefore map[string]string) map[string]string {
		t.Helper()
		key := JobHashPrefix + claim.Job.ID
		m, err := q.rdb.HGetAll(ctx, key).Result()
		if err != nil || m["status"] != StatusBlocked || m["payload"] != claim.Raw || m["held_raw"] != claim.Raw ||
			m["held_token"] != claim.Token || m["held_owner"] != claim.WorkerID || m["held_generation"] != fmt.Sprint(claim.Generation) {
			t.Fatalf("held parent %s custody: status=%q payloadEqual=%v tokenEqual=%v owner=%q generation=%q err=%v",
				claim.Job.ID, m["status"], m["payload"] == claim.Raw, m["held_token"] == claim.Token,
				m["held_owner"], m["held_generation"], err)
		}
		if m["id"] != claim.Job.ID || m["path"] != claim.Job.Path || m["kinds"] != KindProxy || m["producer"] != claim.Job.Producer {
			t.Fatalf("held parent display identity not restored: id=%q path=%q kinds=%q producer=%q",
				m["id"], m["path"], m["kinds"], m["producer"])
		}
		if m["attempts"] != fmt.Sprint(claim.Job.Attempts) || m["hardware_failures"] != fmt.Sprint(claim.Job.HardwareFailures) {
			t.Fatalf("hold changed retry counters: attempts=%q hardware=%q", m["attempts"], m["hardware_failures"])
		}
		if ttl, err := q.rdb.PTTL(ctx, key).Result(); err != nil || ttl != -1 {
			t.Fatalf("held parent TTL=%v err=%v; want persistent", ttl, err)
		}
		if _, err := q.rdb.ZScore(ctx, JobIndexKey, claim.Job.ID).Result(); err != nil {
			t.Fatalf("held parent missing from visible index: %v", err)
		}
		if _, err := q.rdb.ZScore(ctx, LeaseIndexKey, claim.Job.ID).Result(); err != redis.Nil {
			t.Fatalf("held parent remains leased: %v", err)
		}
		if n, err := q.rdb.LPos(ctx, ProcessingPrefix+worker.ID, claim.Raw, redis.LPosArgs{}).Result(); err != redis.Nil {
			t.Fatalf("held parent remains in processing: pos=%d err=%v", n, err)
		}
		if n, err := q.rdb.LPos(ctx, claim.QueueKey, claim.Raw, redis.LPosArgs{}).Result(); err != redis.Nil {
			t.Fatalf("held parent is still claimable: pos=%d err=%v", n, err)
		}
		checkChild(childKey, childBefore)
		return m
	}
	sparsify := func(id string) {
		t.Helper()
		if err := q.rdb.HDel(ctx, JobHashPrefix+id,
			"id", "path", "kinds", "producer", "enqueued_at", "backend", "target_worker", "queue_class").Err(); err != nil {
			t.Fatal(err)
		}
	}

	first := parent("held-proxy-one")
	if err := q.Enqueue(ctx, first); err != nil {
		t.Fatal(err)
	}
	stale := claimParent(first.ID)
	if err := q.RequeueClaimSameRoute(ctx, stale, "delivery retry before hold"); err != nil {
		t.Fatal(err)
	}
	current := claimParent(first.ID)
	if current.Token == stale.Token || current.Raw != stale.Raw {
		t.Fatalf("redelivery did not refresh token for same raw: old=%q new=%q rawEqual=%v", stale.Token, current.Token, stale.Raw == current.Raw)
	}
	if err := q.HoldClaim(ctx, stale, "stale ambiguous reply"); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("old token held successor: %v", err)
	}
	sparsify(first.ID)
	firstChildKey, firstChild := seedRemnant(current.Job, false)
	if _, _, err := q.EnqueueCPUFallbackSubset(ctx, current.Job, targets, reason); !errors.Is(err, ErrCPUFallbackAmbiguous) {
		t.Fatalf("first missing immutable payload: %v", err)
	}
	if err := q.HoldClaim(ctx, current, "fallback publication ambiguous"); err != nil {
		t.Fatal(err)
	}
	firstHeld := checkHold(current, firstChildKey, firstChild)
	if err := q.HoldClaim(ctx, current, "lost reply retry"); err != nil {
		t.Fatalf("idempotent hold replay: %v", err)
	}
	if again := checkHold(current, firstChildKey, firstChild); !reflect.DeepEqual(again, firstHeld) {
		t.Fatal("idempotent hold changed original receipt")
	}

	second := parent("held-proxy-two")
	if err := q.Enqueue(ctx, second); err != nil {
		t.Fatal(err)
	}
	secondClaim := claimParent(second.ID)
	sparsify(second.ID)
	secondChildKey, secondChild := seedRemnant(secondClaim.Job, true)
	if _, _, err := q.EnqueueCPUFallbackSubset(ctx, secondClaim.Job, targets, reason); !errors.Is(err, ErrCPUFallbackAmbiguous) {
		t.Fatalf("second missing publication receipt: %v", err)
	}
	// Redis Lua errors do not roll back prior writes. Inject a bad score
	// immediately after the durable hold receipt, before processing custody
	// is removed, and let restart recovery finish that exact hold.
	faultScript := strings.Replace(holdClaimScript,
		"redis.call('HSET', KEYS[1], unpack(fields))",
		"redis.call('HSET', KEYS[1], unpack(fields))\nredis.call('ZADD', KEYS[3], 'not-a-score', ARGV[6])", 1)
	if faultScript == holdClaimScript {
		t.Fatal("hold fault point not found")
	}
	display, err := json.Marshal(map[string]string{
		"id": second.ID, "path": secondClaim.Job.Path, "kinds": KindProxy,
		"producer": secondClaim.Job.Producer, "enqueued_at": secondClaim.Job.EnqueuedAt,
		"backend": secondClaim.Job.SelectedBackend, "target_worker": secondClaim.Job.SelectedWorker,
		"queue_class": secondClaim.Job.QueueClass, "parent_id": secondClaim.Job.ParentID,
		"coverage_parent_id": secondClaim.Job.CoverageParentID,
		"derivative_pass":    secondClaim.Job.DerivativePass, "requeue_of": secondClaim.Job.RequeueOf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.PExpire(ctx, JobHashPrefix+second.ID, 2*time.Second).Err(); err != nil {
		t.Fatal(err)
	}
	if ttl, err := q.rdb.PTTL(ctx, JobHashPrefix+second.ID).Result(); err != nil || ttl <= 0 {
		t.Fatalf("missing prior legacy TTL: %v %v", ttl, err)
	}
	if _, err := q.rdb.Eval(ctx, faultScript, []string{JobHashPrefix + second.ID,
		claimProcessingKey(secondClaim), LeaseIndexKey, claimProcessingIndex(secondClaim), JobIndexKey, trackedCustodyIndexKey},
		secondClaim.Token, secondClaim.WorkerID, secondClaim.Raw, secondClaim.Generation,
		"fallback publication ambiguous", second.ID, secondClaim.Job.TrackedEpoch, nowISO(), time.Now().UnixMicro(), display).Result(); err == nil {
		t.Fatal("injected post-receipt hold fault unexpectedly completed")
	}
	if ttl, err := q.rdb.PTTL(ctx, JobHashPrefix+second.ID).Result(); err != nil || ttl != -1 {
		t.Fatalf("partial hold can expire before recovery: %v %v", ttl, err)
	}
	partial, err := q.rdb.HGetAll(ctx, JobHashPrefix+second.ID).Result()
	if err != nil || partial["status"] != StatusBlocked || partial["held_token"] != secondClaim.Token || partial["held_raw"] != secondClaim.Raw {
		t.Fatalf("post-fault hold receipt missing: status=%q tokenEqual=%v rawEqual=%v err=%v",
			partial["status"], partial["held_token"] == secondClaim.Token, partial["held_raw"] == secondClaim.Raw, err)
	}
	if _, err := q.rdb.LPos(ctx, ProcessingPrefix+worker.ID, secondClaim.Raw, redis.LPosArgs{}).Result(); err != nil {
		t.Fatalf("post-fault processing receipt disappeared: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.Del(ctx, WorkerPrefix+worker.ID).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 1 {
		t.Fatalf("recover partial held processing receipt: recovered=%d err=%v", n, err)
	}
	secondHeld := checkHold(secondClaim, secondChildKey, secondChild)
	if got := checkHold(current, firstChildKey, firstChild); !reflect.DeepEqual(got, firstHeld) {
		t.Fatal("partial hold recovery changed other held parent")
	}
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	checkVisible := func() {
		t.Helper()
		jobs, err := q.ListJobs(ctx, 20)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for _, job := range jobs {
			if (job.ID == first.ID || job.ID == second.ID) && job.Status == StatusBlocked &&
				job.Path == "/jfs/incoming" && job.Kinds == KindProxy {
				seen[job.ID] = true
			}
		}
		if !seen[first.ID] || !seen[second.ID] {
			t.Fatalf("held parents missing from visible blocked jobs: %v", seen)
		}
	}
	checkVisible()
	if n, err := q.pruneMissingJobIDs(ctx, []string{first.ID, second.ID}); err != nil || n != 0 {
		t.Fatalf("stale index prune removed held parents: removed=%d err=%v", n, err)
	}
	checkVisible()

	transcript := func(id string) Job {
		return Job{ID: id, Path: "/jfs/incoming/interview.wav", Kinds: []string{KindTranscript}, Producer: "hold-test",
			QueueClass: QueueClassRender, SelectedBackend: "vulkan", SelectedWorker: worker.ID,
			RequiredCapabilities: []string{"transcript:vulkan", "worker:" + worker.ID}}
	}
	progressTranscript := func(id string) {
		t.Helper()
		if err := q.Enqueue(ctx, transcript(id)); err != nil {
			t.Fatal(err)
		}
		claim := claimParent(id)
		if err := q.MarkDone(ctx, claim.Job.ID, 1, 0); err != nil {
			t.Fatal(err)
		}
		if err := q.AckClaim(ctx, claim); err != nil {
			t.Fatal(err)
		}
		if st, err := q.rdb.HGet(ctx, JobHashPrefix+id, "status").Result(); err != nil || st != StatusDone {
			t.Fatalf("transcript did not progress: status=%q err=%v", st, err)
		}
	}
	progressTranscript("eligible-transcript-one")

	guards := []struct {
		name string
		call func() error
	}{
		{"MarkRunning", func() error { return q.MarkRunning(ctx, first.ID) }},
		{"MarkClaimRunning", func() error { return q.MarkClaimRunning(ctx, current, worker) }},
		{"RenewClaim", func() error { return q.RenewClaim(ctx, current) }},
		{"MarkDone", func() error { return q.MarkDone(ctx, first.ID, 1, 0) }},
		{"MarkFailed", func() error { return q.MarkFailed(ctx, first.ID, "unsafe retry") }},
		{"MarkCanceled", func() error { return q.MarkCanceled(ctx, first.ID, 0, 0) }},
		{"RequeueSameRoute", func() error { return q.RequeueClaimSameRoute(ctx, current, "unsafe retry") }},
		{"RequeueFailed", func() error { _, _, err := q.RequeueFailed(ctx, first.ID, "operator"); return err }},
		{"Enqueue", func() error { return q.Enqueue(ctx, first) }},
		{"RequestCancel", func() error { return q.RequestJobCancel(ctx, first.ID, "operator") }},
	}
	for _, guard := range guards {
		if err := guard.call(); err == nil {
			t.Fatalf("%s admitted a conflicting transition", guard.name)
		}
		if got := checkHold(current, firstChildKey, firstChild); !reflect.DeepEqual(got, firstHeld) {
			t.Fatalf("%s mutated held original", guard.name)
		}
		if got := checkHold(secondClaim, secondChildKey, secondChild); !reflect.DeepEqual(got, secondHeld) {
			t.Fatalf("%s mutated other held parent", guard.name)
		}
	}
	if _, err := q.ClearFinished(ctx); err != nil {
		t.Fatal(err)
	}
	if got := checkHold(current, firstChildKey, firstChild); !reflect.DeepEqual(got, firstHeld) {
		t.Fatal("ClearFinished changed held parent")
	}
	if got := checkHold(secondClaim, secondChildKey, secondChild); !reflect.DeepEqual(got, secondHeld) {
		t.Fatal("ClearFinished changed second held parent")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.Del(ctx, WorkerPrefix+worker.ID).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := q.RecoverAbandonedWorkers(ctx); err != nil || n != 0 {
		t.Fatalf("recovery touched held parents: recovered=%d err=%v", n, err)
	}
	if got := checkHold(current, firstChildKey, firstChild); !reflect.DeepEqual(got, firstHeld) {
		t.Fatal("restart recovery changed first held parent")
	}
	if got := checkHold(secondClaim, secondChildKey, secondChild); !reflect.DeepEqual(got, secondHeld) {
		t.Fatal("restart recovery changed second held parent")
	}
	checkVisible()
	if err := q.Heartbeat(ctx, worker); err != nil {
		t.Fatal(err)
	}
	progressTranscript("eligible-transcript-after-reopen")
	if claim, ok, err := q.ClaimForWorker(ctx, 0, worker); err != nil || ok {
		t.Fatalf("held parent became claimable after restart: claim=%s ok=%v err=%v", claim.Job.ID, ok, err)
	}
}

func TestRedisTrackedAmbiguousFallbackHoldKeepsCoverage(t *testing.T) {
	q, ctx, _, _, planner, child := newCoverageReceiptLifecycleFixture(t)
	claim := claimCoverageLifecycleChild(t, q, ctx, child)
	if claim.Generation < 1 || claim.Job.TrackedEpoch == "" || claim.Job.ParentID != planner.Job.ID {
		t.Fatalf("not a tracked execution claim: generation=%d epoch=%q parent=%q",
			claim.Generation, claim.Job.TrackedEpoch, claim.Job.ParentID)
	}
	targets := append([]string(nil), claim.Job.RetryTargets...)
	if len(targets) == 0 {
		t.Fatal("tracked child has no fallback target")
	}
	fallback, err := newCPUFallbackSubset(claim.Job, targets)
	if err != nil {
		t.Fatal(err)
	}
	fallback.RoutingReason = "tracked fallback unavailable"
	raw, err := json.Marshal(fallback)
	if err != nil {
		t.Fatal(err)
	}
	childKey := JobHashPrefix + fallback.ID
	if err := q.rdb.HSet(ctx, childKey, "id", fallback.ID, "status", StatusQueued, "payload", raw).Err(); err != nil {
		t.Fatal(err)
	}
	// A partial publication can have already reserved an open coverage
	// membership before its final CPU publication receipt was written.
	membersKey, ledgerKey := coverageMembersKey(planner.Job.ID), coverageLedgerKey(planner.Job.ID)
	if err := q.rdb.ZAdd(ctx, membersKey, redis.Z{Score: 0, Member: fallback.ID}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.rdb.HSet(ctx, ledgerKey, "open_count", "2").Err(); err != nil {
		t.Fatal(err)
	}
	ledgerBefore, err := q.rdb.HGetAll(ctx, ledgerKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	parentScore, err := q.rdb.ZScore(ctx, membersKey, child.ID).Result()
	if err != nil || parentScore != 0 {
		t.Fatalf("tracked parent membership=%v err=%v", parentScore, err)
	}
	fallbackScore, err := q.rdb.ZScore(ctx, membersKey, fallback.ID).Result()
	if err != nil || fallbackScore != 0 {
		t.Fatalf("partial fallback membership=%v err=%v", fallbackScore, err)
	}
	childBefore, err := q.rdb.HGetAll(ctx, childKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := q.EnqueueCPUFallbackSubsetForClaim(ctx, claim, targets, "tracked fallback unavailable"); !errors.Is(err, ErrCPUFallbackAmbiguous) {
		t.Fatalf("tracked unreceipted fallback: %v", err)
	}
	if err := q.HoldClaim(ctx, claim, "tracked fallback publication ambiguous"); err != nil {
		t.Fatal(err)
	}
	if err := q.AckClaim(ctx, claim); err == nil {
		t.Fatal("held tracked execution was ACKed")
	}
	parentFields, err := q.rdb.HGetAll(ctx, JobHashPrefix+child.ID).Result()
	if err != nil || parentFields["status"] != StatusBlocked || parentFields["held_raw"] != claim.Raw || parentFields["held_token"] != claim.Token {
		t.Fatalf("tracked parent hold custody: status=%q rawEqual=%v tokenEqual=%v err=%v",
			parentFields["status"], parentFields["held_raw"] == claim.Raw, parentFields["held_token"] == claim.Token, err)
	}
	ledgerAfter, err := q.rdb.HGetAll(ctx, ledgerKey).Result()
	if err != nil || !reflect.DeepEqual(ledgerAfter, ledgerBefore) {
		t.Fatalf("hold retired tracked coverage: before=%v after=%v err=%v", ledgerBefore, ledgerAfter, err)
	}
	if score, err := q.rdb.ZScore(ctx, membersKey, child.ID).Result(); err != nil || score != 0 {
		t.Fatalf("held execution coverage closed: score=%v err=%v", score, err)
	}
	if score, err := q.rdb.ZScore(ctx, membersKey, fallback.ID).Result(); err != nil || score != 0 {
		t.Fatalf("ambiguous fallback coverage closed: score=%v err=%v", score, err)
	}
	if got, err := q.rdb.HGetAll(ctx, childKey).Result(); err != nil || !reflect.DeepEqual(got, childBefore) {
		t.Fatalf("tracked ambiguous child mutated: got=%v err=%v", got, err)
	}
	if depth, err := q.rdb.LLen(ctx, classQueue(KindProxy, QueueClassCPU)).Result(); err != nil || depth != 0 {
		t.Fatalf("tracked ambiguous fallback entered CPU queue: depth=%d err=%v", depth, err)
	}
}

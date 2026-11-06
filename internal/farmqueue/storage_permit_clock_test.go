package farmqueue

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisClockShift makes this client see Redis TIME moved by offset, which is
// what a host whose own clock is off by -offset sees.
type redisClockShift struct{ offset time.Duration }

func (h redisClockShift) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h redisClockShift) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		h.shift(cmd)
		return err
	}
}

func (h redisClockShift) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		for _, cmd := range cmds {
			h.shift(cmd)
		}
		return err
	}
}

func (h redisClockShift) shift(cmd redis.Cmder) {
	if tc, ok := cmd.(*redis.TimeCmd); ok && tc.Err() == nil {
		tc.SetVal(tc.Val().Add(h.offset))
	}
}

func storagePermitRenderSetup(t *testing.T, q *Client, ctx context.Context) Worker {
	t.Helper()
	render := Worker{ID: "permit-clock-render", Name: "render", Role: QueueClassRender,
		Capabilities: []string{"cpu", "vaapi", "encoder:hevc_vaapi"},
		Encoders:     []string{"hevc_vaapi"},
		Benchmarks:   WorkerBenchmarks{EncodeFPS: 180, ProbedAt: nowISO()}}
	render.RequireStoragePermit = true
	if err := q.Heartbeat(ctx, render); err != nil {
		t.Fatal(err)
	}
	server := Worker{ID: "permit-clock-server", Name: "server", Role: QueueClassServer, Capabilities: []string{"cpu", "metadata"}}
	if err := q.Heartbeat(ctx, server); err != nil {
		t.Fatal(err)
	}
	job := NewJob("/jfs/incoming/clip.mov", []string{KindProxy}, "manager")
	job.ShardIndex, job.ShardCount = 1, 1
	job.RetryTargets = []string{job.Path}
	q.RouteJob(ctx, &job)
	if job.QueueClass != QueueClassRender {
		t.Fatalf("fixture routed to %q, want render", job.QueueClass)
	}
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	return render
}

// The permit used to be stamped with the publisher's wall clock but judged in
// the claim script against Redis TIME (+2 s / -20 s), so a server host 5 s
// ahead of Redis blocked every render claim. It is now stamped with Redis TIME.
func TestStoragePermitIsStampedWithRedisTime(t *testing.T) {
	q, ctx := claimScanTestQueue(t)
	// This host's clock reads 30 s ahead of Redis.
	q.rdb.AddHook(redisClockShift{offset: -30 * time.Second})
	if err := q.PublishStoragePermit(ctx, StoragePermit{Source: "test", AvailableBytes: 2, RequiredBytes: 1}); err != nil {
		t.Fatal(err)
	}
	raw, err := q.rdb.Get(ctx, StoragePermitKey).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var permit StoragePermit
	if err := json.Unmarshal(raw, &permit); err != nil {
		t.Fatal(err)
	}
	redisNow, err := q.rdb.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if skew := permit.CheckedAtUnix - redisNow.Unix(); skew < -2 || skew > 0 {
		t.Fatalf("permit checked_at_unix=%d is %ds from Redis TIME %d; want the Redis clock", permit.CheckedAtUnix, skew, redisNow.Unix())
	}
	if permit.CheckedAt != time.Unix(permit.CheckedAtUnix, 0).UTC().Format(time.RFC3339) {
		t.Fatalf("checked_at %q disagrees with checked_at_unix %d", permit.CheckedAt, permit.CheckedAtUnix)
	}
}

// The render worker's own admission check must agree with the claim script, so
// it judges a permit's age by Redis TIME too.
func TestGetStoragePermitJudgesAgeByRedisTime(t *testing.T) {
	q, ctx := claimScanTestQueue(t)
	if err := q.PublishStoragePermit(ctx, StoragePermit{Source: "test", AvailableBytes: 2, RequiredBytes: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.GetStoragePermit(ctx); err != nil {
		t.Fatalf("fresh permit rejected: %v", err)
	}
	// Seen from a host 30 s behind Redis, the same permit is 30 s old by the
	// clock the claim script uses, past the 20 s window.
	q.rdb.AddHook(redisClockShift{offset: 30 * time.Second})
	if _, err := q.GetStoragePermit(ctx); err == nil {
		t.Fatal("GetStoragePermit accepted a permit that is stale by Redis TIME")
	}
}

// End to end against a Redis whose clock really differs from this host's. Set
// JM_FARM_TEST_SKEWED_REDIS_URL to such an isolated instance (for example one
// started under a clock-shifting LD_PRELOAD 5 s behind this host).
func TestRenderClaimsSurvivePublisherClockSkew(t *testing.T) {
	url := os.Getenv("JM_FARM_TEST_SKEWED_REDIS_URL")
	if url == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("set JM_FARM_TEST_SKEWED_REDIS_URL to an isolated Redis whose clock differs from this host by 3-15 s")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	q, err := Open(url)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	redisNow, err := q.rdb.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	skew := time.Since(redisNow).Round(time.Second)
	if skew.Abs() < 3*time.Second {
		t.Skipf("Redis clock is only %v from this host", skew)
	}
	render := storagePermitRenderSetup(t, q, ctx)
	if err := q.PublishStoragePermit(ctx, StoragePermit{Source: "server-worker", AvailableBytes: 2, RequiredBytes: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.GetStoragePermit(ctx); err != nil {
		t.Fatalf("host %v from Redis: GetStoragePermit: %v", skew, err)
	}
	if _, ok, err := q.ClaimForWorker(ctx, 0, render); err != nil || !ok {
		t.Fatalf("host %v from Redis: render claim ok=%v err=%v", skew, ok, err)
	}
}

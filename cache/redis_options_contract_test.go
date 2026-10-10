//go:build redisoptionsacceptance

package cache

import (
	"context"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/redistestfixture"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/redis/go-redis/v9"
)

func TestSavedRedisURLCacheResetPreservesAuth(t *testing.T) {
	raw := redistestfixture.Start(t)
	opts, err := redisurl.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	r := NewReader(t.TempDir(), DefaultBlockSize, redis.NewClient(opts))
	defer r.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.rdb.Load().Ping(ctx).Err(); err != nil {
		t.Fatalf("initial cache pool: %v", err)
	}
	if !r.ResetRedisConnections() {
		t.Fatal("cache pool reset was refused")
	}
	next := r.rdb.Load()
	if next == nil || next.Options().Username != "probe" || next.Options().DB != 3 {
		t.Fatal("cache pool reset lost ACL or DB")
	}
	if err := next.Ping(ctx).Err(); err != nil {
		t.Fatalf("reset cache pool: %v", err)
	}
}

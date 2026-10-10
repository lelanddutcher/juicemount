//go:build redisoptionsacceptance

package health

import (
	"context"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/redistestfixture"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
)

func TestSavedRedisURLHealthAuthenticatedProbe(t *testing.T) {
	raw := redistestfixture.Start(t)
	opts, err := redisurl.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	m := New(Config{RedisURL: opts.Addr, RedisOptions: opts})
	defer m.rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := m.rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("health authenticated PING: %v", err)
	}
	if m.rdb.Options().DB != 3 || m.rdb.Options().Username != "probe" {
		t.Fatal("health lost DB or ACL username")
	}
	if !redisMountBackendReady(raw) {
		t.Fatal("FUSE readiness rejected authenticated Redis")
	}
}

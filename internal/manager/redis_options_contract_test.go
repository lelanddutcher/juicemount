//go:build redisoptionsacceptance

package manager

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/redistestfixture"
)

func TestSavedRedisURLManagerAuthenticatedProbes(t *testing.T) {
	raw := redistestfixture.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	status := (&overviewSource{metaURL: raw}).realProbeRedis(ctx)
	if !status.Reachable || status.Error != "" {
		t.Fatalf("overview INFO: %+v", status)
	}
	if err := testConnectionJFS(ctx, map[string]string{"meta_url": raw}); err != nil {
		t.Fatalf("destination PING: %v", err)
	}
	if !probeLinkRedis(raw) {
		t.Fatal("Link PING rejected authenticated Redis")
	}
	bad := (&overviewSource{metaURL: "redis://probe:synthetic-secret@127.0.0.1:0/?bad=synthetic-secret"}).realProbeRedis(ctx)
	if strings.Contains(bad.Error, "synthetic-secret") {
		t.Fatal("overview error exposed saved credential")
	}
}

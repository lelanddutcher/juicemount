//go:build redisoptionsacceptance

package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/redistestfixture"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

func TestCLICacheSessionIntegrationQualificationAndStop(t *testing.T) {
	// Fixture refuses a non-private TMPDIR before creating the owned Redis.
	raw := redistestfixture.Start(t)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	wasOffline := pin.IsAutoOffline()
	t.Cleanup(func() { pin.SetAutoOffline(wasOffline, "acceptance cleanup") })
	opts, err := redisurl.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Set(ctx, "setting", `{"UUID":"22222222-2222-4222-8222-222222222222","BlockSize":4096}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	rc, err := metadata.NewRedisClientDeferred(raw, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rc.Stop)
	// Match CLI's constructor order: owner first, actual Session publishes
	// initial false through owner, then Session is installed on the owner.
	o := newCLICacheNetworkOwner(nil, rc)
	s, err := cache.NewSession(cache.SessionConfig{
		Store: store, LogicalURL: raw, Selection: []string{"", "", ""},
		Options: rc.RawDB().Options(), StateDir: t.TempDir(),
		VerifyReader: func(*cache.Reader) error { return cache.ErrReaderUnavailable },
		OnQualified:  o.qualificationChanged,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	o.session = s
	t.Cleanup(o.stop)
	if err := rc.SyncOnce(); !errors.Is(err, metadata.ErrBackendUnqualified) {
		t.Fatalf("constructor metadata admission = %v", err)
	}
	qualify := func() {
		t.Helper()
		s.SetOnline(true)
		qualifyCtx, qualifyCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer qualifyCancel()
		if err := s.Qualify(qualifyCtx); err != nil {
			t.Fatal(err)
		}
		if !s.Qualified() || s.Reader() != nil {
			t.Fatal("actual CLI Session did not qualify with detached acceleration")
		}
	}
	qualify()
	if !o.recoverIfSafe("") || pin.IsAutoOffline() {
		t.Fatal("actual CLI Session callback did not permit fresh recovery")
	}
	if err := rc.SyncOnce(); err != nil {
		t.Fatalf("qualified metadata sync: %v", err)
	}
	epoch, _ := o.recoveryCandidate()
	s.SetOnline(false)
	if o.clearAutoOfflineIfCurrent(epoch) || !pin.IsAutoOffline() {
		t.Fatal("actual Session loss did not reject retained CLI recovery candidate")
	}
	if err := rc.SyncOnce(); !errors.Is(err, metadata.ErrBackendUnqualified) {
		t.Fatalf("loss metadata admission = %v", err)
	}
	qualify()
	if !o.recoverIfSafe("") {
		t.Fatal("fresh CLI Session qualification failed to recover")
	}
	o.stop()
	// Even a real successful qualification delivered after owner shutdown
	// cannot reenable metadata or clear automatic offline during teardown.
	s.SetOnline(false)
	qualify()
	if o.recoverIfSafe("") || !pin.IsAutoOffline() {
		t.Fatal("late Session qualification reopened stopped CLI owner")
	}
	if err := rc.SyncOnce(); !errors.Is(err, metadata.ErrBackendUnqualified) {
		t.Fatalf("stopped owner metadata admission = %v", err)
	}
}
